#![cfg(unix)]
#[path = "support/decision_model.rs"]
mod mock;
mod support;
use mock::MockModel;
use serde_json::{Value, json};
use std::{collections::BTreeMap, process::Command, sync::Arc};
use support::{GatewayServer, TestDatabase, TestEnvironment, assert_success};
use yasp::{
    backend::PostgresBackend,
    decision_model::{ClassificationOutcome, ColumnIdentity, ColumnRequest, DecisionModel},
};

#[tokio::test]
async fn http_contract_rejects_invalid_and_truncated_answers() {
    let mock = MockModel::new();
    let model = mock.model(false);
    let column = ColumnIdentity {
        ordinal: 0,
        name: "email".into(),
        data_type: "text".into(),
    };
    let samples = vec!["someone@example.com".into()];
    let result = model
        .classify(ColumnRequest {
            sql: "SELECT email FROM users",
            column: &column,
            samples: &samples,
        })
        .await;
    assert!(
        matches!(result, ClassificationOutcome::Available { answers } if answers["email"].value == 0.75 && answers["email"].confidence == 0.8)
    );
    for response in [
        "not json".to_owned(),
        json!({"answers":{}}).to_string(),
        json!({"answers":{"email":{"type":"choice","noul":0.9,"confidence":0.9}}}).to_string(),
        json!({"answers":{"email":{"type":"noul","noul":1.1,"confidence":0.9}}}).to_string(),
        json!({"answers":{"email":{"type":"noul","noul":0.9,"confidence":-0.1}}}).to_string(),
        json!({"answers":{"email":{"type":"noul","noul":0.9,"confidence":0.9}},"usage":{"truncated":true}}).to_string(),
        "x".repeat(65537),
    ] {
        *mock.response.lock().unwrap() = Some(response);
        assert_eq!(model.classify(ColumnRequest { sql: "SELECT email FROM users", column: &column, samples: &samples }).await, ClassificationOutcome::Unavailable);
    }
    let before = mock.count();
    assert_eq!(
        model
            .classify(ColumnRequest {
                sql: &"x".repeat(65536),
                column: &column,
                samples: &samples
            })
            .await,
        ClassificationOutcome::Unavailable
    );
    assert_eq!(
        before,
        mock.count(),
        "oversized input never reaches the endpoint"
    );
    *mock.response.lock().unwrap() = None;
    let mut settings = mock.settings(false);
    settings
        .headers_env
        .insert("Authorization".into(), "TEST_AUTH".into());
    assert!(DecisionModel::new(settings.clone(), BTreeMap::new()).is_err());
    let authenticated = DecisionModel::new(
        settings,
        BTreeMap::from([("Authorization".into(), "Bearer example-secret".into())]),
    )
    .unwrap();
    assert!(matches!(
        authenticated
            .classify(ColumnRequest {
                sql: "SELECT email",
                column: &column,
                samples: &samples
            })
            .await,
        ClassificationOutcome::Available { .. }
    ));
    assert!(
        mock.headers
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .to_ascii_lowercase()
            .contains("authorization: bearer example-secret")
    );
    for invalid in [
        json!({"sample_size":0}),
        json!({"timeout_ms":0}),
        json!({"questions":{}}),
        json!({"endpoint":"http://user:secret@127.0.0.1:8001"}),
        json!({"headers_env":{"Content-Length":"TOKEN"}}),
    ] {
        let mut settings = serde_json::to_value(mock.settings(false)).unwrap();
        for (key, value) in invalid.as_object().unwrap() {
            settings[key] = value.clone();
        }
        assert!(
            serde_json::from_value::<yasp::decision_model::DecisionModelSettings>(settings)
                .unwrap()
                .validate()
                .is_err()
        );
    }
    mock.delay_ms.store(20, std::sync::atomic::Ordering::SeqCst);
    let clients = [mock.model(false), mock.model(false)];
    let outcomes = futures::future::join_all((0..24).map(|n| {
        clients[n % 2].classify(ColumnRequest {
            sql: "SELECT email",
            column: &column,
            samples: &samples,
        })
    }))
    .await;
    assert!(
        outcomes
            .iter()
            .all(|outcome| matches!(outcome, ClassificationOutcome::Available { .. }))
    );
    assert!(
        mock.peak_active.load(std::sync::atomic::Ordering::SeqCst) <= 8,
        "concurrency is bounded across client instances"
    );
    let mut settings = mock.settings(false);
    settings.timeout_ms = 20;
    mock.delay_ms.store(80, std::sync::atomic::Ordering::SeqCst);
    let slow = DecisionModel::new(settings, BTreeMap::new()).unwrap();
    assert_eq!(
        slow.classify(ColumnRequest {
            sql: "SELECT email",
            column: &column,
            samples: &samples
        })
        .await,
        ClassificationOutcome::Unavailable
    );
}

#[test]
fn postgres_sampling_prefix_transactions_and_failure_policy() {
    let env = TestEnvironment::load();
    let database = TestDatabase::create(&env.source(None), "yasp_classification");
    let mock = MockModel::new();
    let server = GatewayServer::fixed(
        env.gateway()
            .gateway_config()
            .with_decision_model(mock.model(false)),
        Arc::new(PostgresBackend::new(database.connection.backend_config()).unwrap()),
    );
    let connection = env.gateway().connection(server.address);
    let sql = "/* diagnostic */ SELECT CASE WHEN i > 100 THEN 'late@example.com' END AS email, CASE WHEN i IN (2, 4) THEN 'a@example.com' END AS email FROM generate_series(1, 110) i";
    assert_success(&connection.execute(sql), "sparse prefix");
    let mut requests = mock.requests.lock().unwrap().clone();
    requests.sort_by_key(|r| r["state"]["column"]["ordinal"].as_u64());
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0]["state"]["samples"], json!([]));
    assert_eq!(
        requests[1]["state"]["samples"],
        json!(["a@example.com", "a@example.com"])
    );
    assert!(
        !requests[0]["state"]["sql"]
            .as_str()
            .unwrap()
            .contains("diagnostic")
    );
    assert_eq!(requests[0]["questions"]["email"]["type"], "noul");
    mock.clear();
    let dense = connection.execute("SELECT i AS number FROM generate_series(1, 20) i");
    assert_eq!(String::from_utf8(dense.stdout).unwrap().lines().count(), 20);
    assert_eq!(
        mock.requests.lock().unwrap()[0]["state"]["samples"],
        json!(["1", "2", "3", "4", "5"])
    );
    mock.clear();
    assert_success(
        &connection.execute("SELECT 'empty@example.com' AS email WHERE false"),
        "empty result",
    );
    assert_eq!(
        mock.requests.lock().unwrap()[0]["state"]["samples"],
        json!([])
    );
    mock.clear();
    assert_success(
        &connection.execute("SELECT repeat('x', 262144) AS large_value"),
        "oversized first row remains unchanged",
    );
    assert_eq!(
        mock.requests.lock().unwrap()[0]["state"]["samples"],
        json!([])
    );
    mock.clear();
    assert_success(
        &connection
            .command()
            .args([
                "-v",
                "ON_ERROR_STOP=1",
                "-c",
                "BEGIN READ ONLY",
                "-c",
                "SELECT 'a@example.com' AS email",
                "-c",
                "SELECT 'b@example.com' AS email",
                "-c",
                "ROLLBACK",
            ])
            .output()
            .unwrap(),
        "transactions classify each query",
    );
    assert_eq!(mock.count(), 2);
    *mock.response.lock().unwrap() = Some("{}".into());
    assert_success(
        &connection.execute("SELECT 'raw@example.com' AS email"),
        "advisory model failure",
    );
    drop(server);
    let strict = GatewayServer::fixed(
        env.gateway()
            .gateway_config()
            .with_decision_model(mock.model(true)),
        Arc::new(PostgresBackend::new(database.connection.backend_config()).unwrap()),
    );
    let connection = env.gateway().connection(strict.address);
    let result = connection.query("SELECT 'secret@example.com' AS email");
    assert!(!result.status.success());
    assert!(!String::from_utf8_lossy(&result.stdout).contains("secret@example.com"));
    *mock.response.lock().unwrap() = None;
    assert_success(
        &connection.execute("SELECT 'recovered@example.com' AS email"),
        "strict failure recovery",
    );
}

fn redis(url: &str, args: &[&str]) -> Vec<u8> {
    let result = Command::new("redis-cli")
        .args(["-u", url, "--raw"])
        .args(args)
        .output()
        .unwrap();
    assert_success(&result, "classification cache Redis");
    let mut bytes = result.stdout;
    assert_eq!(bytes.pop(), Some(b'\n'));
    bytes
}

#[test]
fn cached_decisions_reuse_raw_results_and_validate_sidecars() {
    let env = TestEnvironment::load();
    let database = TestDatabase::create(&env.source(None), "yasp_classified_cache");
    let mock = MockModel::new();
    mock.response_by_ordinal
        .lock()
        .unwrap()
        .insert(1, "{}".into());
    let url = std::env::var("YASP_TEST_REDIS_URL").unwrap();
    let namespace = support::RedisNamespace::new(&url, "classification_cache");
    let sql = format!(
        "SELECT random() AS \"{}\", 'unknown' AS unavailable",
        namespace.name
    );
    let id = yasp::query::ParsedQuery::parse(&sql)
        .unwrap()
        .query_id()
        .to_owned();
    // Fixed-backend routing uses this stable configured pair.
    let backend = Arc::new(PostgresBackend::new(database.connection.backend_config()).unwrap());
    let settings = yasp::config::CacheSettings {
        enabled: true,
        namespace: namespace.name.clone(),
        ttl_seconds: 30,
        ..Default::default()
    };
    let gateway = env
        .gateway()
        .gateway_config()
        .with_cache(Arc::new(
            yasp::cache::RedisCache::new(settings.clone(), &url, None).unwrap(),
        ))
        .with_decision_model(mock.model(false));
    let server = GatewayServer::fixed(gateway, backend.clone());
    let connection = env.gateway().connection(server.address);
    let first = connection.execute(&sql);
    assert_success(&first, "classified cache miss");
    assert_eq!(mock.count(), 2);
    let keys = String::from_utf8(redis(
        &url,
        &["KEYS", &format!("yasp:decision_cache:*:{id}:*")],
    ))
    .unwrap();
    assert!(!keys.is_empty(), "decision sidecar published");
    let sidecar = keys.lines().find(|key| key.contains(":0:")).unwrap();
    let unavailable = keys.lines().find(|key| key.contains(":1:")).unwrap();
    let failed: Value = serde_json::from_slice(&redis(&url, &["GET", unavailable])).unwrap();
    assert_eq!(failed["classification"]["outcome"]["status"], "unavailable");
    let stored: Value = serde_json::from_slice(&redis(&url, &["GET", sidecar])).unwrap();
    assert_eq!(
        stored["classification"]["outcome"]["answers"]["email"]["value"],
        0.75
    );
    let raw_keys =
        String::from_utf8(redis(&url, &["KEYS", &format!("{}:*", namespace.name)])).unwrap();
    let raw_key = raw_keys.lines().next().unwrap();
    assert_eq!(
        redis(&url, &["TTL", raw_key]),
        redis(&url, &["TTL", sidecar])
    );
    assert_eq!(first.stdout, connection.execute(&sql).stdout);
    assert_eq!(mock.count(), 2, "cache hit never calls the model");
    for field in ["association", "fingerprint"] {
        let mut invalid = stored.clone();
        invalid[field] = "wrong".into();
        redis(&url, &["SET", sidecar, &invalid.to_string(), "EX", "30"]);
        assert_eq!(first.stdout, connection.execute(&sql).stdout);
        assert_eq!(
            mock.count(),
            2,
            "mismatched metadata cannot trigger inference"
        );
    }
    redis(&url, &["SET", sidecar, "invalid json", "EX", "30"]);
    assert_eq!(first.stdout, connection.execute(&sql).stdout);
    let raw = redis(&url, &["GET", raw_key]);
    assert!(raw.starts_with(b"YASP\x02"));
    let mut legacy = raw[..37].to_vec();
    legacy[4] = 1;
    legacy.extend_from_slice(&raw[69..]);
    let mut redis_connection = redis::Client::open(url.as_str())
        .unwrap()
        .get_connection()
        .unwrap();
    redis::cmd("SET")
        .arg(raw_key)
        .arg(legacy)
        .arg("EX")
        .arg(30)
        .query::<()>(&mut redis_connection)
        .unwrap();
    assert_eq!(first.stdout, connection.execute(&sql).stdout);
    assert_eq!(
        mock.count(),
        2,
        "legacy entries remain hits without model calls"
    );
    redis(&url, &["DEL", sidecar]);
    assert_eq!(first.stdout, connection.execute(&sql).stdout);
    assert_eq!(mock.count(), 2, "missing metadata remains a raw cache hit");
    drop(server);
    let strict = GatewayServer::fixed(
        env.gateway()
            .gateway_config()
            .with_cache(Arc::new(
                yasp::cache::RedisCache::new(settings, &url, None).unwrap(),
            ))
            .with_decision_model(mock.model(true)),
        Arc::new(PostgresBackend::new(database.connection.backend_config()).unwrap()),
    );
    assert!(
        !env.gateway()
            .connection(strict.address)
            .query(&sql)
            .status
            .success()
    );
    assert_eq!(mock.count(), 2);
    mock.response_by_ordinal.lock().unwrap().clear();
    redis::cmd("SET")
        .arg(raw_key)
        .arg(&raw[..40])
        .arg("EX")
        .arg(30)
        .query::<()>(&mut redis_connection)
        .unwrap();
    let recovered = env.gateway().connection(strict.address).execute(&sql);
    assert_eq!(
        mock.count(),
        4,
        "truncated classified headers cause a fresh execution"
    );
    assert_eq!(
        recovered.stdout,
        env.gateway()
            .connection(strict.address)
            .execute(&sql)
            .stdout
    );
    assert_eq!(
        mock.count(),
        4,
        "a malformed header must not poison the new cache capture"
    );
    redis(&url, &["DEL", sidecar]);
    redis(&url, &["DEL", unavailable]);
}

#[tokio::test]
async fn cancellation_during_inference_releases_the_source_and_recovers() {
    use futures::SinkExt;
    use pgwire::{
        api::client::{
            ClientInfo, Config, auth::DefaultStartupHandler, query::DefaultSimpleQueryHandler,
        },
        error::PgWireClientError,
        messages::{PgWireFrontendMessage, cancel::CancelRequest},
        tokio::client::{PgWireClient, PgWireMessageClientCodec},
    };
    use std::{sync::atomic::Ordering, time::Duration};
    use tokio::{
        net::TcpStream,
        time::{sleep, timeout},
    };
    use tokio_util::codec::Framed;
    let env = TestEnvironment::load();
    let database = TestDatabase::create(&env.source(None), "yasp_classify_cancel");
    let url = std::env::var("YASP_TEST_REDIS_URL").unwrap();
    let namespace = support::RedisNamespace::new(&url, "classify_cancel");
    let cancel_sql = format!(
        "SELECT i AS \"{}\" FROM generate_series(1,10000) i",
        namespace.name
    );
    let cache = Arc::new(
        yasp::cache::RedisCache::new(
            yasp::config::CacheSettings {
                enabled: true,
                namespace: namespace.name.clone(),
                ..Default::default()
            },
            &url,
            None,
        )
        .unwrap(),
    );
    let mock = MockModel::new();
    mock.delay_ms.store(1000, Ordering::SeqCst);
    let server = GatewayServer::fixed(
        env.gateway()
            .gateway_config()
            .with_cache(cache)
            .with_decision_model(mock.model(false)),
        Arc::new(PostgresBackend::new(database.connection.backend_config()).unwrap()),
    );
    let identity = env.gateway();
    let mut config = Config::new();
    config
        .host(server.address.ip().to_string())
        .port(server.address.port())
        .user(&identity.user)
        .password(&identity.password)
        .dbname(&identity.database);
    let mut client = PgWireClient::connect(Arc::new(config), DefaultStartupHandler::new(), None)
        .await
        .unwrap();
    let pid = client.process_id();
    let key = client.secret_key().clone();
    {
        let query = client.simple_query(DefaultSimpleQueryHandler::new(), &cancel_sql);
        tokio::pin!(query);
        tokio::select! {
            result = &mut query => panic!("query returned before cancellation: {result:?}"),
            _ = timeout(Duration::from_secs(3), async { while mock.count() == 0 { sleep(Duration::from_millis(5)).await; } }) => {},
        }
        let socket = TcpStream::connect(server.address).await.unwrap();
        Framed::new(socket, PgWireMessageClientCodec::default())
            .send(PgWireFrontendMessage::CancelRequest(CancelRequest::new(
                pid, key,
            )))
            .await
            .unwrap();
        let result = timeout(Duration::from_millis(700), &mut query)
            .await
            .expect("cancel promptly interrupts inference");
        assert!(
            matches!(result, Err(PgWireClientError::RemoteError(info)) if info.code == "57014")
        );
    }
    let id = yasp::query::ParsedQuery::parse(&cancel_sql)
        .unwrap()
        .query_id()
        .to_owned();
    assert!(redis(&url, &["KEYS", &format!("{}:*", namespace.name)]).is_empty());
    assert!(redis(&url, &["KEYS", &format!("yasp:decision_cache:*:{id}:*")]).is_empty());
    // The source lease is cleaned up even though the mock's old request is still pending.
    mock.delay_ms.store(0, Ordering::SeqCst);
    let recovery_sql = format!("SELECT 42 AS \"{}_recovered\"", namespace.name);
    timeout(
        Duration::from_millis(700),
        client.simple_query(DefaultSimpleQueryHandler::new(), &recovery_sql),
    )
    .await
    .unwrap()
    .unwrap();
    let id = yasp::query::ParsedQuery::parse(&recovery_sql)
        .unwrap()
        .query_id()
        .to_owned();
    for key in String::from_utf8(redis(
        &url,
        &["KEYS", &format!("yasp:decision_cache:*:{id}:*")],
    ))
    .unwrap()
    .lines()
    {
        redis(&url, &["DEL", key]);
    }
}

#[test]
fn both_profiles_preserve_native_text_values_with_configured_sample_size() {
    let env = TestEnvironment::load();
    let database = TestDatabase::create(&env.source(None), "yasp_classify_profiles");
    let tls = std::path::PathBuf::from(std::env::var_os("YASP_TEST_TLS_DIR").unwrap());
    let sql = "SELECT i AS number, 12345678901234567890.123456789::numeric AS amount, decode('00ff','hex') AS data, '😀'::text AS label, ARRAY[1,2] AS items FROM generate_series(1,10) i";
    let expected = database.connection.execute(sql).stdout;
    for profile in ["postgresql", "redshift"] {
        let mock = MockModel::new();
        let mut settings = mock.settings(false);
        settings.sample_size = 2;
        let gateway = env
            .gateway()
            .gateway_config()
            .with_decision_model(Arc::new(
                DecisionModel::new(settings, BTreeMap::new()).unwrap(),
            ))
            .with_settings(yasp::config::GatewaySettings {
                protocol: profile.into(),
                tls: yasp::config::GatewayTls {
                    mode: "enable".into(),
                    cert_file: Some(tls.join("server.crt")),
                    key_file: Some(tls.join("server.key")),
                },
                ..Default::default()
            })
            .unwrap();
        let server = GatewayServer::fixed(
            gateway,
            Arc::new(PostgresBackend::new(database.connection.backend_config()).unwrap()),
        );
        let output = env
            .gateway()
            .connection(server.address)
            .command()
            .env("PGSSLMODE", "verify-full")
            .env("PGSSLROOTCERT", tls.join("ca.crt"))
            .args(["-v", "ON_ERROR_STOP=1", "-c", sql])
            .output()
            .unwrap();
        assert_success(&output, "classified native values through each profile");
        assert_eq!(expected, output.stdout);
        let mut requests = mock.requests.lock().unwrap().clone();
        requests.sort_by_key(|r| r["state"]["column"]["ordinal"].as_u64());
        assert_eq!(requests.len(), 5);
        assert_eq!(requests[0]["state"]["samples"], json!(["1", "2"]));
        assert_eq!(
            requests[1]["state"]["samples"][0],
            "12345678901234567890.123456789"
        );
        assert_eq!(requests[2]["state"]["samples"][0], "\\x00ff");
        assert_eq!(requests[3]["state"]["samples"][0], "😀");
        assert_eq!(requests[4]["state"]["samples"][0], "{1,2}");
    }
}

#[test]
fn advisory_inference_timeout_does_not_consume_the_frontend_write_deadline() {
    let env = TestEnvironment::load();
    let database = TestDatabase::create(&env.source(None), "yasp_classify_deadlines");
    let mock = MockModel::new();
    mock.delay_ms
        .store(250, std::sync::atomic::Ordering::SeqCst);
    let mut settings = mock.settings(false);
    settings.timeout_ms = 100;
    let gateway = env
        .gateway()
        .gateway_config()
        .with_decision_model(Arc::new(
            DecisionModel::new(settings, BTreeMap::new()).unwrap(),
        ))
        .with_settings(yasp::config::GatewaySettings {
            write_timeout_ms: 20,
            ..Default::default()
        })
        .unwrap();
    let server = GatewayServer::fixed(
        gateway,
        Arc::new(PostgresBackend::new(database.connection.backend_config()).unwrap()),
    );
    let result = env
        .gateway()
        .connection(server.address)
        .execute("SELECT i FROM generate_series(1,20) i");
    assert_eq!(
        String::from_utf8(result.stdout).unwrap().lines().count(),
        20
    );
    assert_eq!(mock.count(), 1);
}
