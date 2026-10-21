//! Runtime lineage is fresh before hooks; persistent metadata follows raw fills.
#[path = "support/decision_model.rs"]
mod mock;
mod support;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    process::Command,
    sync::{Arc, Mutex},
};
use support::{GatewayServer, TestDatabase, TestEnvironment, assert_success};
use yasp::{
    backend::{PostgresBackend, PostgresBackends},
    catalog::{Catalog, CatalogSettings},
    config::{BackendSettings, CacheSettings, GatewaySettings},
    hooks::{BackendSelection, bind_cache_eligibility, bind_context, bind_select_backend},
    lineage::AstSettings,
    query::{InputOptions, SqlDialect},
};

fn redis(url: &str, args: &[&str]) -> Vec<u8> {
    let result = Command::new("redis-cli")
        .args(["-u", url, "--raw"])
        .args(args)
        .output()
        .unwrap();
    assert_success(&result, "lineage Redis fixture");
    let mut bytes = result.stdout;
    assert_eq!(bytes.pop(), Some(b'\n'));
    bytes
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lineage_reaches_all_hooks_and_cache_publication_with_current_schema() {
    let env = TestEnvironment::load();
    let database = TestDatabase::create(&env.source(None), "yasp_lineage");
    assert_success(
        &database.connection.execute(
            "CREATE TABLE users(id int,email text); INSERT INTO users VALUES(1,'a@example.test')",
        ),
        "lineage source",
    );
    let backend = Arc::new(PostgresBackend::new(database.connection.backend_config()).unwrap());
    let catalog = Catalog::postgres(backend.clone(), CatalogSettings::new(vec!["public".into()]))
        .await
        .unwrap();
    catalog.stop_refreshing().await;
    let seen = Arc::new(Mutex::new(Vec::<(String, Value)>::new()));
    let context = bind_context(seen.clone(), |request, seen| {
        Box::pin(async move {
            seen.lock().unwrap().push((
                "context".into(),
                serde_json::to_value(request.data_contexts).unwrap(),
            ));
            assert!(request.query.serialized_ast().get("Query").is_some());
            assert_eq!(
                request.query.input_options().dialect,
                SqlDialect::Postgresql
            );
            assert!(request.query.input_options().native);
            Ok(Default::default())
        })
    });
    let eligible = bind_cache_eligibility(seen.clone(), |request, seen| {
        seen.lock().unwrap().push((
            "eligible".into(),
            serde_json::to_value(request.data_contexts).unwrap(),
        ));
        Ok(true)
    });
    let selector = bind_select_backend(seen.clone(), |request, seen| {
        seen.lock().unwrap().push((
            "select".into(),
            serde_json::to_value(request.data_contexts).unwrap(),
        ));
        Ok(BackendSelection {
            target: "primary".into(),
            backend_login: "reader".into(),
        })
    });
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
    // Catalog identity uses the same endpoint/principal regardless of pool ownership.
    let url = std::env::var("YASP_TEST_REDIS_URL").unwrap();
    let namespace = support::RedisNamespace::new(&url, "lineage_raw");
    let ast_namespace = support::RedisNamespace::new(&url, "lineage_ast");
    let settings = CacheSettings {
        enabled: true,
        namespace: namespace.name.clone(),
        ttl_seconds: 30,
        ..Default::default()
    };
    let cache =
        Arc::new(yasp::cache::RedisCache::new(settings, &url, Some(eligible.into())).unwrap());
    let gateway = env
        .gateway()
        .gateway_config()
        .with_settings(GatewaySettings {
            query_timeout_ms: 200,
            ..Default::default()
        })
        .unwrap()
        .with_catalog(catalog.clone())
        .with_ast_context(AstSettings {
            enabled: true,
            dialect: Some(SqlDialect::Postgresql),
            namespace: ast_namespace.name.clone(),
        })
        .with_context_hook(context)
        .with_cache(cache);
    let server = GatewayServer::routed(gateway, backends, selector);
    let connection = env.gateway().connection(server.address);
    let sql = format!(
        "SELECT id AS x,email AS x,random() AS \"{}\" FROM users",
        namespace.name
    );
    let id = yasp::query::ParsedQuery::parse(&sql)
        .unwrap()
        .query_id()
        .to_owned();
    let ast_key = format!("{}:primary_reader:{id}", ast_namespace.name);
    let first = connection.execute(&sql);
    assert_success(&first, "lineage cache fill");
    let stored: Value = serde_json::from_slice(&redis(&url, &["GET", &ast_key])).unwrap();
    assert!(
        stored["ast"]["columns"][0]["sources"][0]["column"]
            .as_str()
            .unwrap()
            .ends_with("\"users\".\"id\"")
    );
    assert!(
        stored["ast"]["columns"][1]["sources"][0]["column"]
            .as_str()
            .unwrap()
            .ends_with("\"users\".\"email\"")
    );
    assert_eq!(stored["ast"]["columns"][0]["name"], "x");
    assert_eq!(stored["ast"]["columns"][1]["ordinal"], 1);
    let raw_key = format!("{}:primary_reader:{id}", namespace.name);
    let raw = redis(&url, &["GET", &raw_key]);
    let association: String = raw[37..69].iter().map(|b| format!("{b:02x}")).collect();
    assert_eq!(stored["association"], association);
    let ttl = redis(&url, &["PTTL", &raw_key]);
    let ast_ttl = redis(&url, &["PTTL", &ast_key]);
    let ttl = String::from_utf8(ttl).unwrap().parse::<i64>().unwrap();
    let ast_ttl = String::from_utf8(ast_ttl).unwrap().parse::<i64>().unwrap();
    assert!((ttl - ast_ttl).abs() < 200 && ast_ttl > 0);
    redis(&url, &["SET", &ast_key, "malformed", "EX", "30"]);
    assert_eq!(
        first.stdout,
        connection.execute(&sql).stdout,
        "bad persisted context never replaces fresh analysis or prevents a raw hit"
    );
    let calls = seen.lock().unwrap().clone();
    assert_eq!(
        calls
            .iter()
            .map(|(phase, _)| phase.as_str())
            .collect::<Vec<_>>(),
        [
            "context", "select", "eligible", "context", "select", "eligible"
        ]
    );
    assert!(calls.iter().all(|(_, context)| context == &calls[0].1));
    seen.lock().unwrap().clear();
    assert_success(
        &database
            .connection
            .execute("ALTER TABLE users ADD COLUMN phone text DEFAULT '123'"),
        "schema change",
    );
    assert_success(
        &connection.execute("SELECT id,phone FROM users"),
        "missing catalog column executes",
    );
    assert_eq!(
        seen.lock().unwrap()[0].1["ast"]["columns"][1]["sources"],
        json!([{"unknown":"phone"}])
    );
    catalog.refresh().await.unwrap();
    seen.lock().unwrap().clear();
    assert_success(
        &connection.execute("SELECT id,phone FROM users"),
        "fresh lineage on raw hit",
    );
    assert!(
        seen.lock().unwrap()[0].1["ast"]["columns"][1]["sources"][0]
            .get("column")
            .is_some()
    );
    seen.lock().unwrap().clear();
    assert_success(
        &connection.queries(&["BEGIN", "SELECT id,email FROM users", "ROLLBACK"]),
        "transaction lineage",
    );
    let phases: Vec<_> = seen
        .lock()
        .unwrap()
        .iter()
        .map(|(phase, _)| phase.clone())
        .collect();
    assert_eq!(
        phases,
        ["context", "select"],
        "transactions bypass Redis eligibility"
    );
    let slow_sql = format!("SELECT pg_sleep(2) AS \"{}\"", namespace.name);
    let slow_id = yasp::query::ParsedQuery::parse(&slow_sql)
        .unwrap()
        .query_id()
        .to_owned();
    let timeout = connection.query(&slow_sql);
    assert!(
        !timeout.status.success(),
        "source deadline cancels an AST-enabled query"
    );
    assert!(
        redis(
            &url,
            &[
                "GET",
                &format!("{}:primary_reader:{slow_id}", ast_namespace.name)
            ]
        )
        .is_empty(),
        "cancelled fills publish no lineage"
    );
    assert_success(
        &connection.execute(&sql),
        "recovery after cancellation retains cache and hook behavior",
    );
    drop(server);
    let keys =
        String::from_utf8(redis(&url, &["KEYS", &format!("{}:*", ast_namespace.name)])).unwrap();
    for key in keys.lines().filter(|key| {
        key.ends_with(&id)
            || key.ends_with(
                yasp::query::ParsedQuery::parse("SELECT id,phone FROM users")
                    .unwrap()
                    .query_id(),
            )
    }) {
        redis(&url, &["DEL", key]);
    }
}

#[test]
fn lineage_and_decisions_share_one_raw_association() {
    let env = TestEnvironment::load();
    let database = TestDatabase::create(&env.source(None), "yasp_lineage_model");
    let url = std::env::var("YASP_TEST_REDIS_URL").unwrap();
    let namespace = support::RedisNamespace::new(&url, "lineage_model");
    let ast_namespace = support::RedisNamespace::new(&url, "mixed_ast");
    let model_namespace = support::RedisNamespace::new(&url, "mixed_decisions");
    let mock = mock::MockModel::new();
    let settings = CacheSettings {
        enabled: true,
        namespace: namespace.name.clone(),
        ttl_seconds: 30,
        ..Default::default()
    };
    let gateway = env
        .gateway()
        .gateway_config()
        .with_ast_context(AstSettings {
            enabled: true,
            dialect: Some(SqlDialect::Snowflake),
            namespace: ast_namespace.name.clone(),
        })
        // Explicit per-query options take precedence over the configured default.
        .with_input_options(|_| InputOptions::native(SqlDialect::Postgresql))
        .with_decision_model({
            let mut settings = mock.settings(true);
            settings.namespace = model_namespace.name.clone();
            Arc::new(yasp::decision_model::DecisionModel::new(settings, BTreeMap::new()).unwrap())
        })
        .with_cache(Arc::new(
            yasp::cache::RedisCache::new(settings, &url, None).unwrap(),
        ));
    let server = GatewayServer::fixed(
        gateway,
        Arc::new(PostgresBackend::new(database.connection.backend_config()).unwrap()),
    );
    let connection = env.gateway().connection(server.address);
    let sql = format!("SELECT random() AS \"{}\"", namespace.name);
    let id = yasp::query::ParsedQuery::parse(&sql)
        .unwrap()
        .query_id()
        .to_owned();
    let result = connection.execute(&sql);
    assert_success(&result, "lineage plus model");
    let ast_keys = String::from_utf8(redis(
        &url,
        &["KEYS", &format!("{}:*:{id}", ast_namespace.name)],
    ))
    .unwrap();
    let ast_key = ast_keys
        .lines()
        .next()
        .expect("lineage sidecar published alongside decisions")
        .to_owned();
    let ast: Value = serde_json::from_slice(&redis(&url, &["GET", &ast_key])).unwrap();
    let decision_keys = String::from_utf8(redis(
        &url,
        &["KEYS", &format!("{}:*:{id}:*", model_namespace.name)],
    ))
    .unwrap();
    let decision_key = decision_keys.lines().next().unwrap();
    let decision: Value = serde_json::from_slice(&redis(&url, &["GET", decision_key])).unwrap();
    assert_eq!(ast["association"], decision["association"]);
    assert!(redis(&url, &["KEYS", &format!("yasp:ast_cache:*:{id}")]).is_empty());
    assert!(redis(&url, &["KEYS", &format!("yasp:decision_cache:*:{id}:*")]).is_empty());
    let ast_ttl = String::from_utf8(redis(&url, &["PTTL", &ast_key]))
        .unwrap()
        .trim()
        .parse::<i64>()
        .unwrap();
    let decision_ttl = String::from_utf8(redis(&url, &["PTTL", decision_key]))
        .unwrap()
        .trim()
        .parse::<i64>()
        .unwrap();
    assert!(ast_ttl > 0 && (ast_ttl - decision_ttl).abs() < 200);
    assert_eq!(result.stdout, connection.execute(&sql).stdout);
    assert_eq!(mock.count(), 1);
    redis(&url, &["DEL", &ast_key, decision_key]);
}

#[test]
fn concurrent_dashboard_queries_all_publish_and_reuse_their_contexts() {
    let env = TestEnvironment::load();
    let database = TestDatabase::create(&env.source(None), "yasp_dashboard_cache");
    let url = std::env::var("YASP_TEST_REDIS_URL").unwrap();
    let raw = support::RedisNamespace::new(&url, "dashboard_raw");
    let ast = support::RedisNamespace::new(&url, "dashboard_ast");
    let decisions = support::RedisNamespace::new(&url, "dashboard_decisions");
    let mock = mock::MockModel::new();
    mock.delay_ms
        .store(300, std::sync::atomic::Ordering::SeqCst);
    let mut model = mock.settings(true);
    model.namespace = decisions.name.clone();
    let gateway = env
        .gateway()
        .gateway_config()
        .with_ast_context(AstSettings {
            enabled: true,
            namespace: ast.name.clone(),
            ..Default::default()
        })
        .with_decision_model(Arc::new(
            yasp::decision_model::DecisionModel::new(model, BTreeMap::new()).unwrap(),
        ))
        .with_cache(Arc::new(
            yasp::cache::RedisCache::new(
                CacheSettings {
                    enabled: true,
                    namespace: raw.name.clone(),
                    ttl_seconds: 30,
                    ..Default::default()
                },
                &url,
                None,
            )
            .unwrap(),
        ));
    let server = GatewayServer::fixed(
        gateway,
        Arc::new(PostgresBackend::new(database.connection.backend_config()).unwrap()),
    );
    let queries: Vec<_> = (0..5)
        .map(|i| {
            format!(
                "SELECT {}random()::text AS \"{}_{i}\" FROM (SELECT pg_sleep(0.3)) wait",
                if i == 0 { "repeat('x',20000)||" } else { "" },
                raw.name
            )
        })
        .collect();
    let run = || {
        std::thread::scope(|scope| {
            let barrier = Arc::new(std::sync::Barrier::new(5));
            let handles: Vec<_> = queries
                .iter()
                .map(|sql| {
                    let connection = env.gateway().connection(server.address);
                    let barrier = barrier.clone();
                    scope.spawn(move || {
                        barrier.wait();
                        connection.execute(sql)
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().unwrap().stdout)
                .collect::<Vec<_>>()
        })
    };
    let first = run();
    assert_eq!(mock.count(), 5);
    assert!(mock.peak_active.load(std::sync::atomic::Ordering::SeqCst) > 1);
    for sql in &queries {
        let id = yasp::query::ParsedQuery::parse(sql)
            .unwrap()
            .query_id()
            .to_owned();
        for namespace in [&raw.name, &ast.name, &decisions.name] {
            let suffix = if namespace == &decisions.name {
                ":*"
            } else {
                ""
            };
            assert!(
                !redis(&url, &["KEYS", &format!("{namespace}:*:{id}{suffix}")]).is_empty(),
                "all five queries publish {namespace}"
            );
        }
    }
    assert_eq!(
        first,
        run(),
        "all five concurrent refreshes replay their raw results"
    );
    assert_eq!(
        mock.count(),
        5,
        "refreshes reuse stored decisions without inference"
    );
}
