//! Effective SQL drives routing, lineage, cache identity, and backend execution.
#[path = "../hooks/examples/query_modifier.rs"]
mod query_modifier_example;
mod support;

use serde_json::{Map, json};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use support::{GatewayServer, RedisNamespace, TestDatabase, TestEnvironment, assert_success};
use yasp::{
    backend::{PostgresBackend, PostgresBackends},
    cache::RedisCache,
    config::{
        ApplicationContextsSettings, BackendSettings, CacheSettings, DatasetSettings,
        GatewaySettings, SelectorSettings,
    },
    datasets::{DatasetError, DatasetReader, Datasets, bind_dataset_loader},
    hooks::{
        BackendSelection, QueryModificationError, bind_cache_eligibility, bind_modify_query,
        bind_process_result, bind_query_context, bind_select_backend,
    },
    lineage::AstSettings,
    query::{ParsedQuery, SqlDialect},
};

const INPUT: &str = "SELECT SUM(o.amount), clock_timestamp() FROM public.orders AS o";

fn effective(tenant: u64) -> String {
    format!(
        "SELECT SUM(o.amount), clock_timestamp() FROM (SELECT * FROM public.orders_v2 WHERE tenant_id = {tenant}) AS o"
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn effective_query_reaches_all_stages_on_hits_misses_and_transactions() {
    let env = TestEnvironment::load();
    let database = TestDatabase::create(&env.source(None), "yasp_modify");
    database.connection.execute("CREATE TABLE orders(amount int); INSERT INTO orders VALUES(999); CREATE TABLE orders_v2(tenant_id int,amount int); INSERT INTO orders_v2 VALUES(42,10),(42,20),(73,70)");
    let url = std::env::var("YASP_TEST_REDIS_URL").unwrap();
    let namespace = RedisNamespace::new(&url, "modify");
    let datasets = Datasets::start(
        ApplicationContextsSettings {
            namespace: format!("{}:datasets", namespace.name),
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
                Box::pin(async {
                    Ok(Map::from_iter([(
                        "orders".into(),
                        json!("public.orders_v2"),
                    )]))
                })
            })
            .into(),
        )]),
    )
    .await
    .unwrap();
    let tenant = Arc::new(AtomicUsize::new(42));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let context = bind_query_context((tenant.clone(), seen.clone()), |request, state| {
        assert_eq!(request.sql, INPUT);
        state.1.lock().unwrap().push("context");
        let tenant = state.0.load(Ordering::SeqCst);
        Box::pin(async move { Ok(Map::from_iter([("tenant_id".into(), json!(tenant))]).into()) })
    });
    let retained = Arc::new(Mutex::new(None::<DatasetReader>));
    let modifier = bind_modify_query(
        (seen.clone(), retained.clone(), env.gateway().user),
        |request, state| {
            Box::pin(async move {
                state.0.lock().unwrap().push("modify");
                assert_eq!(request.sql, INPUT);
                assert_eq!(
                    request.statement.to_string(),
                    ParsedQuery::parse(INPUT).unwrap().statement().to_string()
                );
                assert_eq!(request.authenticated_user, state.2);
                assert_eq!(request.dialect, SqlDialect::Postgresql);
                assert_eq!(request.default_target_dialect, Some(SqlDialect::Postgresql));
                let reader = request.datasets.unwrap();
                assert_eq!(
                    reader.get("mappings", "orders").await.unwrap(),
                    Some(json!("public.orders_v2"))
                );
                *state.1.lock().unwrap() = Some(reader.clone());
                Ok(Some(effective(
                    request.query_context["tenant_id"].as_u64().unwrap(),
                )))
            })
        },
    );
    let route = BackendSelection {
        target: "primary".into(),
        backend_login: "reader".into(),
    };
    let selector = bind_select_backend((seen.clone(), route.clone()), |request, state| {
        state.0.lock().unwrap().push("select");
        let expected = effective(request.query_context["tenant_id"].as_u64().unwrap());
        let parsed = ParsedQuery::parse(&expected).unwrap();
        assert_eq!(request.query.sql(), expected);
        assert_eq!(
            request.query.statement().to_string(),
            parsed.statement().to_string()
        );
        assert_eq!(request.query_id, parsed.query_id());
        let lineage = serde_json::to_value(request.data_contexts).unwrap();
        assert!(
            lineage["ast"]["columns"][0]["sources"][0]["column"]
                .as_str()
                .unwrap()
                .ends_with("\"orders_v2\".\"amount\"")
        );
        Ok(state.1.clone())
    });
    let eligibility = bind_cache_eligibility(seen.clone(), |request, seen| {
        seen.lock().unwrap().push("eligible");
        assert_eq!(
            request.query.sql(),
            effective(request.query_context["tenant_id"].as_u64().unwrap())
        );
        Ok(true)
    });
    let result = bind_process_result(seen.clone(), |request, _, seen| {
        Box::pin(async move {
            seen.lock().unwrap().push("result");
            assert_eq!(
                request.query.sql(),
                effective(request.query_context["tenant_id"].as_u64().unwrap())
            );
            assert_eq!(request.query_id, request.query.query_id());
            assert!(
                serde_json::to_string(&request.data_contexts.ast)
                    .unwrap()
                    .contains("orders_v2")
            );
            Ok(())
        })
    });
    let cache = Arc::new(
        RedisCache::new(
            CacheSettings {
                enabled: true,
                namespace: namespace.name.clone(),
                ttl_seconds: 60,
                ..Default::default()
            },
            &url,
            Some(eligibility.into()),
        )
        .unwrap(),
    );
    let gateway = env
        .gateway()
        .gateway_config()
        .with_query_context_hook(context)
        .with_query_modification_hook(modifier)
        .with_datasets(datasets.reader())
        .with_cache(cache)
        .with_ast_context(AstSettings {
            enabled: true,
            namespace: format!("{}:ast", namespace.name),
        })
        .with_result_hook(result, Default::default())
        .unwrap();
    let backends = Arc::new(
        PostgresBackends::new(
            BTreeMap::from([(route, database.connection.backend_config())]),
            BackendSettings::default(),
        )
        .unwrap(),
    );
    let server = GatewayServer::routed(gateway, backends, selector);
    let client = env.gateway().connection(server.address);
    client.execute("SELECT current_catalog");
    client.execute("SET application_name = 'rewrite-test'");
    assert!(
        seen.lock().unwrap().is_empty(),
        "frontend requests bypass application hooks"
    );
    let first = client.execute(INPUT);
    let second = client.execute(INPUT);
    assert!(String::from_utf8_lossy(&first.stdout).starts_with("30|"));
    assert_eq!(
        first.stdout, second.stdout,
        "second query reuses the effective-query cache entry"
    );
    tenant.store(73, Ordering::SeqCst);
    let third = client.execute(INPUT);
    assert!(String::from_utf8_lossy(&third.stdout).starts_with("70|"));
    assert_eq!(
        *seen.lock().unwrap(),
        ["context", "modify", "select", "eligible", "result"].repeat(3)
    );
    let keys = std::process::Command::new("redis-cli")
        .args([
            "-u",
            &url,
            "--scan",
            "--pattern",
            &format!("{}:primary_reader:*", namespace.name),
        ])
        .output()
        .unwrap();
    assert_success(&keys, "read effective cache keys");
    let keys = String::from_utf8(keys.stdout).unwrap();
    for tenant in [42, 73] {
        assert!(keys.contains(ParsedQuery::parse(&effective(tenant)).unwrap().query_id()));
    }
    assert!(!keys.contains(ParsedQuery::parse(INPUT).unwrap().query_id()));
    let reader = retained.lock().unwrap().take().unwrap();
    assert_eq!(
        reader.get("mappings", "orders").await,
        Err(DatasetError::Closed)
    );
    seen.lock().unwrap().clear();
    let transaction = client.queries(&["BEGIN", INPUT, "COMMIT"]);
    assert_success(
        &transaction,
        "modified source query in a pinned transaction",
    );
    assert!(String::from_utf8_lossy(&transaction.stdout).contains("70|"));
    assert_eq!(
        *seen.lock().unwrap(),
        ["context", "modify", "select", "result"]
    );
    server.stop();
    datasets.shutdown().await;
}

#[test]
fn invalid_rewrites_fail_before_routing_and_leave_the_source_unchanged() {
    let env = TestEnvironment::load();
    let database = TestDatabase::create(&env.source(None), "yasp_modify_invalid");
    database
        .connection
        .execute("CREATE TABLE protected_rows(id int); INSERT INTO protected_rows VALUES(1)");
    let routes = Arc::new(AtomicUsize::new(0));
    let modifier = bind_modify_query((), |request, _| {
        Box::pin(async move {
            match request.sql {
                "SELECT 'empty'" => Ok(Some("/* empty */".into())),
                "SELECT 'syntax'" => Ok(Some("SELECT (".into())),
                "SELECT 'multiple'" => Ok(Some("SELECT 1; SELECT 2".into())),
                "SELECT 'write'" => Ok(Some("DELETE FROM protected_rows".into())),
                "SELECT 'control'" => Ok(Some("SET application_name = 'bypass'".into())),
                "SELECT 'large'" => Ok(Some(" ".repeat(8193))),
                "SELECT 'complex'" => Ok(Some(format!("SELECT {}", vec!["1"; 2100].join(",")))),
                "SELECT 'nul'" => Ok(Some("SELECT '\0'".into())),
                "SELECT 'denied'" => Err(QueryModificationError::Denied),
                "SELECT 'failed'" => Err(QueryModificationError::Failed),
                "SELECT 'timeout'" => std::future::pending().await,
                // This must reach the backend, rather than frontend PID recognition.
                "SELECT 'pid'" => Ok(Some("SELECT pg_backend_pid()".into())),
                _ => Ok(None),
            }
        })
    });
    let selector = bind_select_backend(routes.clone(), |request, routes| {
        if request.query.sql().contains("preserve bytes") {
            assert_eq!(request.query.sql(), "/* preserve bytes */ select 7 ;");
        }
        routes.fetch_add(1, Ordering::SeqCst);
        Ok(BackendSelection {
            target: "primary".into(),
            backend_login: "reader".into(),
        })
    });
    let gateway = env
        .gateway()
        .gateway_config()
        .with_settings(GatewaySettings {
            max_sql_bytes: 8192,
            async_hook_timeout_ms: 30,
            ..Default::default()
        })
        .unwrap()
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
    for (name, code) in [
        ("empty", "0A000"),
        ("syntax", "42601"),
        ("multiple", "0A000"),
        ("write", "0A000"),
        ("control", "0A000"),
        ("large", "54000"),
        ("complex", "54000"),
        ("nul", "XX000"),
        ("denied", "42501"),
        ("failed", "XX000"),
        ("timeout", "XX000"),
    ] {
        let output = client.query(&format!("SELECT '{name}'"));
        assert!(!output.status.success(), "{name} must fail");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains(code),
            "{name}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    assert_eq!(routes.load(Ordering::SeqCst), 0);
    let unchanged = "/* preserve bytes */ select 7 ;";
    assert_eq!(
        String::from_utf8_lossy(&client.execute(unchanged).stdout).trim(),
        "7"
    );
    client.execute("SELECT 'pid'");
    assert_eq!(
        routes.load(Ordering::SeqCst),
        2,
        "rewritten data never re-enters frontend recognition"
    );
    let transaction = client.queries(&[
        "BEGIN",
        "SELECT 'failed'",
        "SELECT 1",
        "ROLLBACK",
        "SELECT 2",
    ]);
    assert!(String::from_utf8_lossy(&transaction.stderr).contains("25P02"));
    assert!(String::from_utf8_lossy(&transaction.stdout).contains('2'));
    assert_eq!(
        String::from_utf8_lossy(
            &database
                .connection
                .execute("SELECT count(*) FROM protected_rows")
                .stdout
        )
        .trim(),
        "1"
    );
    server.stop();
}

#[tokio::test]
async fn cancellation_disconnect_and_shutdown_drop_pending_modifications() {
    use futures::SinkExt;
    use pgwire::{
        api::client::{
            ClientInfo, Config, auth::DefaultStartupHandler, query::DefaultSimpleQueryHandler,
        },
        error::PgWireClientError,
        messages::{PgWireFrontendMessage, cancel::CancelRequest},
        tokio::client::{PgWireClient, PgWireMessageClientCodec},
    };
    use tokio::{net::TcpStream, sync::Notify, time::timeout};
    use tokio_util::codec::Framed;
    struct Dropped(Arc<AtomicUsize>);
    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    async fn connect(address: std::net::SocketAddr, identity: support::Identity) -> PgWireClient {
        let mut config = Config::new();
        config
            .host(address.ip().to_string())
            .port(address.port())
            .user(identity.user)
            .password(identity.password)
            .dbname(identity.database);
        PgWireClient::connect(Arc::new(config), DefaultStartupHandler::new(), None)
            .await
            .unwrap()
    }
    let env = TestEnvironment::load();
    let database = TestDatabase::create(&env.source(None), "yasp_modify_cancel");
    let entered = Arc::new(Notify::new());
    let dropped = Arc::new(AtomicUsize::new(0));
    let hook = bind_modify_query((entered.clone(), dropped.clone()), |request, state| {
        Box::pin(async move {
            if request.sql.contains("wait") {
                let _guard = Dropped(state.1.clone());
                state.0.notify_one();
                std::future::pending::<()>().await;
            }
            Ok(None)
        })
    });
    let gateway = env
        .gateway()
        .gateway_config()
        .with_settings(GatewaySettings {
            async_hook_timeout_ms: 30_000,
            shutdown_timeout_ms: 100,
            ..Default::default()
        })
        .unwrap()
        .with_query_modification_hook(hook);
    let backend = Arc::new(PostgresBackend::new(database.connection.backend_config()).unwrap());
    let server = GatewayServer::fixed(gateway, backend);
    let address = server.address;
    let mut client = connect(address, env.gateway()).await;
    let pid = client.process_id();
    let secret = client.secret_key().clone();
    let mut query =
        Box::pin(client.simple_query(DefaultSimpleQueryHandler::new(), "SELECT 1 /* wait */"));
    tokio::select! {
        result = &mut query => panic!("modification finished before cancellation: {result:?}"),
        result = timeout(Duration::from_secs(3), entered.notified()) => result.unwrap(),
    }
    let socket = TcpStream::connect(address).await.unwrap();
    Framed::new(socket, PgWireMessageClientCodec::default())
        .send(PgWireFrontendMessage::CancelRequest(CancelRequest::new(
            pid, secret,
        )))
        .await
        .unwrap();
    match timeout(Duration::from_secs(3), query)
        .await
        .unwrap()
        .unwrap_err()
    {
        PgWireClientError::RemoteError(error) => assert_eq!(error.code, "57014"),
        error => panic!("unexpected cancellation error: {error:?}"),
    }
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    client
        .simple_query(DefaultSimpleQueryHandler::new(), "SELECT 1")
        .await
        .unwrap();
    let identity = env.gateway();
    let disconnected = tokio::spawn(async move {
        connect(address, identity)
            .await
            .simple_query(DefaultSimpleQueryHandler::new(), "SELECT 2 /* wait */")
            .await
    });
    timeout(Duration::from_secs(3), entered.notified())
        .await
        .unwrap();
    disconnected.abort();
    let _ = disconnected.await;
    timeout(Duration::from_secs(3), async {
        while dropped.load(Ordering::SeqCst) != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let identity = env.gateway();
    let stopping = tokio::spawn(async move {
        connect(address, identity)
            .await
            .simple_query(DefaultSimpleQueryHandler::new(), "SELECT 3 /* wait */")
            .await
    });
    timeout(Duration::from_secs(3), entered.notified())
        .await
        .unwrap();
    server.stop_with_timeout(Duration::from_secs(3));
    assert_eq!(dropped.load(Ordering::SeqCst), 3);
    stopping.abort();
    let _ = stopping.await;
}

#[tokio::test]
async fn compiled_example_renames_base_tables_and_preserves_cte_references() {
    use sqlparser::ast::{Ident, ObjectName};
    use yasp::hooks::{QueryModificationHook, QueryModificationRequest};
    let name =
        |schema: &str, table: &str| ObjectName::from(vec![Ident::new(schema), Ident::new(table)]);
    let hook: QueryModificationHook = bind_modify_query(
        query_modifier_example::TableRename {
            from: name("public", "orders"),
            to: name("reporting", "orders_v2"),
        },
        query_modifier_example::rename_table,
    )
    .into();
    for (sql, expected) in [
        (
            "SELECT o.id FROM public.orders AS o",
            Some("SELECT o.id FROM reporting.orders_v2 AS o"),
        ),
        (
            "WITH orders AS (SELECT 1 AS id) SELECT o.id FROM orders AS o JOIN public.orders AS p ON o.id = p.id",
            Some(
                "WITH orders AS (SELECT 1 AS id) SELECT o.id FROM orders AS o JOIN reporting.orders_v2 AS p ON o.id = p.id",
            ),
        ),
        ("SELECT 'public.orders' AS literal", None),
    ] {
        let parsed = ParsedQuery::parse(sql).unwrap();
        let context = Map::new();
        let output = hook
            .modify(
                QueryModificationRequest {
                    authenticated_user: "bi",
                    frontend_database: "analytics",
                    client_id: 1,
                    sql: parsed.sql(),
                    statement: parsed.statement(),
                    dialect: parsed.dialect(),
                    gateway_dialect: parsed.dialect(),
                    default_target_dialect: None,
                    query_context: &context,
                    datasets: None,
                },
                Duration::from_secs(1),
                4096,
            )
            .await
            .unwrap();
        assert_eq!(output.as_deref(), expected);
    }
}
