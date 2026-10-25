#![cfg(feature = "python")]
//! Python dispatch, isolated invocation inputs, and the normal YAML host.
mod support;

use serde_json::{Map, json};
use std::{
    collections::BTreeMap,
    fs,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use support::{
    GatewayServer, RedisNamespace, TempDirectory, TestDatabase, TestEnvironment, assert_success,
};
use yasp::{
    backend::PostgresBackends,
    config::{
        ApplicationContextsSettings, BackendSettings, DatasetSettings, GatewaySettings,
        PythonSettings, SelectorSettings,
    },
    datasets::{Datasets, bind_dataset_loader},
    hooks::{
        BackendSelection, QueryModificationHook, bind_query_context, bind_select_backend,
        python::PythonRuntime,
    },
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn python_modifier_has_ast_context_and_scoped_datasets_but_no_lineage_or_id() {
    let env = TestEnvironment::load();
    let database = TestDatabase::create(&env.source(None), "yasp_modify_python");
    let temporary = TempDirectory::new("modify_python");
    let path = temporary.0.join("modifier.py");
    let completed = temporary.0.join("completed");
    fs::write(
        &path,
        r#"
import time
from pathlib import Path
retained = None

def modify(request, params):
    global retained
    assert 'query_id' not in request and 'data_contexts' not in request
    assert 'Query' in request['query_ast']
    assert request['dialect'] == 'postgresql'
    assert request['query_context'] == {'tenant_id': 42}
    if retained is not None:
        try:
            retained.get('mappings', 'answer')
        except RuntimeError:
            pass
        else:
            raise AssertionError('dataset reader outlived invocation')
    retained = request['datasets']
    answer = retained.get('mappings', 'answer')
    sql = request['sql']
    request['query_ast'].clear()
    request['query_context']['tenant_id'] = 999
    if 'rewrite' in sql:
        return f'SELECT {answer}'
    if 'invalid' in sql:
        return {'sql': 'SELECT 999'}
    if 'boolean' in sql:
        return True
    if 'failed' in sql:
        raise ValueError('private policy details')
    if 'slow' in sql:
        time.sleep(0.4)
        Path(params['completed']).write_text('done')
        return 'SELECT 999'
    return None
"#,
    )
    .unwrap();
    let url = std::env::var("YASP_TEST_REDIS_URL").unwrap();
    let namespace = RedisNamespace::new(&url, "modify_python");
    let datasets = Datasets::start(
        ApplicationContextsSettings {
            namespace: namespace.name.clone(),
            datasets: BTreeMap::from([(
                "mappings".into(),
                DatasetSettings {
                    refresh_interval_seconds: 3600,
                    loader: SelectorSettings::Rust("app.mappings".into()),
                },
            )]),
            ..Default::default()
        },
        &url,
        BTreeMap::from([(
            "mappings".into(),
            bind_dataset_loader((), |_, _| {
                Box::pin(async { Ok(Map::from_iter([("answer".into(), json!(42))])) })
            })
            .into(),
        )]),
    )
    .await
    .unwrap();
    let runtime = PythonRuntime::new(PythonSettings {
        execution_timeout_ms: 2000,
        queue_capacity: 1,
        ..Default::default()
    })
    .await
    .unwrap();
    let modifier: QueryModificationHook = runtime
        .register(
            &path,
            "modify",
            Map::from_iter([("completed".into(), json!(completed))]),
        )
        .await
        .unwrap()
        .into();
    let selected = Arc::new(AtomicUsize::new(0));
    let selector = bind_select_backend(selected.clone(), |request, selected| {
        selected.fetch_add(1, Ordering::SeqCst);
        assert_eq!(request.query_context["tenant_id"], 42);
        assert!(request.query.serialized_ast().get("Query").is_some());
        assert_eq!(request.query_id, request.query.query_id());
        Ok(BackendSelection {
            target: "primary".into(),
            backend_login: "reader".into(),
        })
    });
    let gateway = env
        .gateway()
        .gateway_config()
        .with_settings(GatewaySettings {
            async_hook_timeout_ms: 150,
            ..Default::default()
        })
        .unwrap()
        .with_datasets(datasets.reader())
        .with_query_context_hook(bind_query_context((), |_, _| {
            Box::pin(async { Ok(Map::from_iter([("tenant_id".into(), json!(42))]).into()) })
        }))
        .with_query_modification_hook(modifier);
    let backends = Arc::new(
        PostgresBackends::new(
            BTreeMap::from([(
                BackendSelection {
                    target: "primary".into(),
                    backend_login: "reader".into(),
                },
                database.connection.backend_config(),
            )]),
            BackendSettings::default(),
        )
        .unwrap(),
    );
    let server = GatewayServer::routed(gateway, backends, selector);
    let client = env.gateway().connection(server.address);
    assert_eq!(
        String::from_utf8_lossy(&client.execute("SELECT 'rewrite'").stdout).trim(),
        "42"
    );
    assert_eq!(
        String::from_utf8_lossy(&client.execute("SELECT 'unchanged'").stdout).trim(),
        "unchanged"
    );
    for name in ["invalid", "boolean", "failed", "slow"] {
        let result = client.query(&format!("SELECT '{name}'"));
        let error = String::from_utf8_lossy(&result.stderr);
        assert!(!result.status.success());
        assert!(error.contains("XX000"), "{error}");
        assert!(!error.contains("private policy details"));
    }
    assert_eq!(
        selected.load(Ordering::SeqCst),
        2,
        "invalid or late output never reaches routing"
    );
    let deadline = Instant::now() + Duration::from_secs(3);
    while !completed.exists() {
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    client.execute("SELECT 'rewrite'");
    server.stop();
    datasets.shutdown().await;
}

#[test]
fn yaml_host_starts_python_for_modification_only_and_stops_cleanly_in_both_profiles() {
    use std::{
        net::TcpListener,
        process::{Command, Stdio},
        thread,
    };
    let env = TestEnvironment::load();
    let tls = std::path::PathBuf::from(std::env::var_os("YASP_TEST_TLS_DIR").unwrap());
    for profile in ["postgresql", "redshift"] {
        let temporary = TempDirectory::new("modify_host");
        fs::create_dir(temporary.0.join("config")).unwrap();
        fs::write(
            temporary.0.join("modifier.py"),
            "def modify_query(request, params):\n    return params.get('sql_by_input', {}).get(request['sql'].strip())\n",
        )
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let mut config = serde_json::to_value(&env.config).unwrap();
        config["gateway"]["listen"] = json!(address.to_string());
        config["gateway"]["protocol"] = json!(profile);
        config["gateway"]["tls"] = json!({"mode":"enable","cert_file":tls.join("server.crt"),"key_file":tls.join("server.key")});
        config["hooks"]["modify_query"] = json!({"type":"python","path":"../modifier.py","callback":"modify_query","params":{"sql_by_input":{"SELECT 7":"SELECT 9"}}});
        let path = temporary.0.join("config/config.json");
        fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
        let identity = env.gateway();
        let mut command = Command::new(env!("CARGO_BIN_EXE_yasp"));
        command
            .args(["--config", path.to_str().unwrap()])
            .env("YASP_GATEWAY_USERNAME", &identity.user)
            .env("YASP_GATEWAY_PASSWORD", &identity.password)
            .env("YASP_GATEWAY_DATABASE", &identity.database)
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        for target in env.config.targets.values() {
            for login in target.logins.values() {
                command.env(&login.password_env, env.value(&login.password_env));
            }
        }
        let mut host = support::ChildGuard::new(command.spawn().unwrap());
        let query = |sql: &str| {
            identity
                .connection(address)
                .command()
                .env("PGSSLMODE", "verify-full")
                .env("PGSSLROOTCERT", tls.join("ca.crt"))
                .args(["-v", "ON_ERROR_STOP=1", "-c", sql])
                .output()
                .unwrap()
        };
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            assert!(
                host.child().try_wait().unwrap().is_none(),
                "configured host exited"
            );
            if query("SELECT 1").status.success() {
                break;
            }
            assert!(Instant::now() < deadline);
            thread::sleep(Duration::from_millis(20));
        }
        let result = query("SELECT 7");
        assert_success(&result, "configured Python modification");
        assert_eq!(String::from_utf8_lossy(&result.stdout).trim(), "9");
        assert!(
            Command::new("kill")
                .args(["-TERM", &host.id().to_string()])
                .status()
                .unwrap()
                .success()
        );
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(status) = host.child().try_wait().unwrap() {
                assert!(status.success());
                break;
            }
            assert!(Instant::now() < deadline);
            thread::sleep(Duration::from_millis(20));
        }
    }
}
