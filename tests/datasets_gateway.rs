//! Verify Rust hooks can use datasets during one query, then lose that access.
mod support;

#[path = "../hooks/examples/dataset_selector.rs"]
mod dataset_selector;

use serde_json::{Map, json};
use std::{
    collections::BTreeMap,
    fs,
    net::TcpListener,
    process::{Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use support::{ChildGuard, RedisNamespace, TempDirectory, TestEnvironment, assert_success};
use yasp::{
    backend::PostgresBackends,
    cache::RedisCache,
    config::{ApplicationContextsSettings, CacheSettings, DatasetSettings, SelectorSettings},
    datasets::{DatasetError, DatasetLoader, DatasetReader, Datasets, bind_dataset_loader},
    hooks::{CacheEligibilityError, bind_cache_eligibility_async, bind_context},
};

fn redis_url() -> String {
    std::env::var("YASP_TEST_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".into())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rust_context_and_selector_dataset_access_expires_after_the_query() {
    let environment = TestEnvironment::load();
    let identity = environment.gateway();
    let route = environment.config.default_backend().unwrap();
    let backend = environment.source(Some(&route.backend_login));
    let source_user = backend.identity.user.clone();
    let redis = redis_url();
    let namespace = RedisNamespace::new(&redis, "yasp_dataset_gateway");
    let settings = ApplicationContextsSettings {
        namespace: namespace.name.clone(),
        datasets: BTreeMap::from([(
            "users".into(),
            DatasetSettings {
                refresh_interval_seconds: 1,
                loader: SelectorSettings::Rust("test.users".into()),
            },
        )]),
        ..ApplicationContextsSettings::default()
    };
    let department = Arc::new(Mutex::new("finance".to_owned()));
    let loader: DatasetLoader = bind_dataset_loader(department.clone(), |_, department| {
        let department = department.lock().unwrap().clone();
        Box::pin(async move {
            Ok(Map::from_iter([(
                "alice".into(),
                json!({"department": department}),
            )]))
        })
    })
    .into();
    let datasets = Datasets::start(settings, &redis, BTreeMap::from([("users".into(), loader)]))
        .await
        .expect("dataset loads before listener startup");
    assert_eq!(
        datasets.reader().get("users", "alice").await.unwrap(),
        Some(json!({"department": "finance"}))
    );
    let retained = Arc::new(Mutex::new(None::<DatasetReader>));
    let context = bind_context(retained.clone(), |request, retained| {
        Box::pin(async move {
            if let Some(reader) = request.datasets {
                *retained.lock().unwrap() = Some(reader.clone());
                reader
                    .get("users", "alice")
                    .await
                    .map_err(|_| yasp::hooks::ContextError::Failed)?
                    .ok_or(yasp::hooks::ContextError::Failed)?;
            } else {
                return Err(yasp::hooks::ContextError::Failed);
            }
            Ok(Map::from_iter([("user_id".into(), json!("alice"))]))
        })
    });
    let backends = Arc::new(
        PostgresBackends::new(
            BTreeMap::from([(route.clone(), backend.backend_config())]),
            environment.config.backend.clone(),
        )
        .expect("source route is valid"),
    );
    let eligibility_calls = Arc::new(AtomicUsize::new(0));
    let eligibility = bind_cache_eligibility_async(eligibility_calls.clone(), |request, calls| {
        Box::pin(async move {
            calls.fetch_add(1, Ordering::SeqCst);
            let reader = request.datasets.ok_or(CacheEligibilityError)?;
            let user = reader
                .get("users", "alice")
                .await
                .map_err(|_| CacheEligibilityError)?;
            Ok(user.is_some())
        })
    });
    let cache_settings = CacheSettings {
        enabled: true,
        namespace: format!("{}:cache", namespace.name),
        ..CacheSettings::default()
    };
    let cache = Arc::new(
        RedisCache::new(cache_settings, &redis, Some(eligibility.into()))
            .expect("result cache initializes"),
    );
    let gateway = identity
        .gateway_config()
        .with_context_hook(context)
        .with_cache(cache)
        .with_datasets(datasets.reader());
    let selector = dataset_selector::bind(dataset_selector::RoutingPolicy {
        target: route.target.clone(),
        department_logins: BTreeMap::from([("finance".into(), route.backend_login.clone())]),
    });
    let server = support::GatewayServer::routed(gateway, backends, selector);
    let client = identity.connection(server.address);
    let first = client.query("SELECT current_user, clock_timestamp()");
    assert_success(&first, "first query routed with Rust dataset lookup");
    tokio::time::sleep(Duration::from_millis(20)).await;
    let cached = client.query("SELECT current_user, clock_timestamp()");
    assert_success(&cached, "second query reads the cached result");
    assert_eq!(
        first.stdout, cached.stdout,
        "the second query used the cache"
    );
    assert_eq!(eligibility_calls.load(Ordering::SeqCst), 2);
    assert!(String::from_utf8_lossy(&cached.stdout).contains(&source_user));

    *department.lock().unwrap() = "unmapped".into();
    let reader = datasets.reader();
    let deadline = Instant::now() + Duration::from_secs(5);
    while reader
        .get("users", "alice")
        .await
        .ok()
        .flatten()
        .and_then(|user| user.get("department").cloned())
        != Some(json!("unmapped"))
    {
        assert!(
            Instant::now() < deadline,
            "dataset refresh completed before deadline"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let denied = client.query("SELECT current_user, clock_timestamp()");
    assert!(!denied.status.success());
    assert!(String::from_utf8_lossy(&denied.stderr).contains("42501"));
    assert_eq!(eligibility_calls.load(Ordering::SeqCst), 2);
    server.stop();

    let retained = retained
        .lock()
        .unwrap()
        .take()
        .expect("context kept its reader clone");
    assert_eq!(
        retained.get("users", "alice").await,
        Err(DatasetError::Closed)
    );
    datasets.shutdown().await;
}

#[test]
fn configured_host_publishes_compiled_dataset_before_accepting_queries() {
    let environment = TestEnvironment::load();
    let identity = environment.gateway();
    let route = environment.config.default_backend().unwrap();
    let redis = redis_url();
    let namespace = RedisNamespace::new(&redis, "yasp_dataset_host");
    let listener = TcpListener::bind("127.0.0.1:0").expect("ephemeral loopback port binds");
    let address = listener.local_addr().unwrap();
    drop(listener);
    let temporary = TempDirectory::new("yasp_dataset_host");
    let path = temporary.0.join("config.json");
    let mut config = serde_json::to_value(&environment.config).unwrap();
    config["gateway"]["listen"] = address.to_string().into();
    config["schema_discovery"] = serde_json::json!({
        "target": route.target.clone(),
        "backend_login": route.backend_login.clone(),
    });
    config["hooks"]["select_backend"] = serde_json::json!({
        "type": "rust",
        "callback": "builtin.select_backend.passthrough",
        "params": {"target": route.target.clone(), "backend_login": route.backend_login.clone()},
    });
    config["application_contexts"] = serde_json::json!({
        "url_env": "YASP_TEST_REDIS_URL",
        "namespace": namespace.name,
        "datasets": {
            "users": {
                "loader": {
                    "type": "rust",
                    "callback": "builtin.dataset.load_json",
                    "params": {"file": "hooks/examples/users.json"},
                }
            },
            "email_locations": {
                "loader": {
                    "type": "rust",
                    "callback": "example.datasets.email_locations",
                }
            }
        }
    });
    fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_yasp"));
    command
        .args(["--config", path.to_str().unwrap()])
        .env("YASP_TEST_REDIS_URL", &redis)
        .env("YASP_GATEWAY_USERNAME", &identity.user)
        .env("YASP_GATEWAY_PASSWORD", &identity.password)
        .env("YASP_GATEWAY_DATABASE", &identity.database)
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    for target in environment.config.targets.values() {
        for login in target.logins.values() {
            command.env(&login.password_env, environment.value(&login.password_env));
        }
    }
    let mut process = ChildGuard::new(command.spawn().expect("configured host starts"));

    let data_key = format!("{}::email_locations::data", namespace.name);
    let users_key = format!("{}::users::data", namespace.name);
    let expected = r#"["crm.contacts.email","billing.invoices.recipient_email"]"#;
    let started = Instant::now();
    loop {
        assert!(
            process.child().try_wait().unwrap().is_none(),
            "configured host exited before publishing its dataset"
        );
        let published = Command::new("redis-cli")
            .args(["-u", &redis, "HGET", &data_key, "columns"])
            .output()
            .expect("redis-cli is installed");
        let user = Command::new("redis-cli")
            .args(["-u", &redis, "HGET", &users_key, "alice"])
            .output()
            .expect("redis-cli is installed");
        if published.status.success()
            && String::from_utf8_lossy(&published.stdout).trim() == expected
            && user.status.success()
            && String::from_utf8_lossy(&user.stdout).contains("\"department\":\"finance\"")
        {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "host did not publish the compiled dataset before deadline"
        );
        thread::sleep(Duration::from_millis(20));
    }

    let query = identity.connection(address).query("SELECT 1");
    assert_success(&query, "query after configured dataset startup");
}
