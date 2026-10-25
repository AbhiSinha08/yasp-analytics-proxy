//! Input grammar hints never authorize discovery or change PostgreSQL input validation.
mod support;

use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};
use yasp::{
    backend::PostgresBackend,
    config::{GatewaySettings, GatewayTls},
    hooks::{bind_query_context, builtin::query_context::metabase},
    query::{ParsedQuery, SqlDialect},
};

#[test]
fn dialect_grammar_and_read_only_inspection_use_the_same_dialect() {
    let sql = r"SELECT 'a\'b' AS value";
    assert!(ParsedQuery::parse(sql).is_err());
    let parsed = ParsedQuery::parse_with_dialect(sql, 4096, SqlDialect::Snowflake).unwrap();
    assert_eq!(parsed.dialect(), SqlDialect::Snowflake);
    parsed.validate_read_only().unwrap();
    for dialect in [
        SqlDialect::Postgresql,
        SqlDialect::Redshift,
        SqlDialect::Snowflake,
    ] {
        let query = ParsedQuery::parse_with_dialect(
            "WITH secret AS (SELECT * FROM users) DELETE FROM users",
            4096,
            dialect,
        )
        .unwrap();
        assert!(query.validate_read_only().is_err());
        assert!(ParsedQuery::parse_with_dialect("SELECT 1; SELECT 2", 4096, dialect).is_err());
        assert!(ParsedQuery::parse_with_dialect("SELECT 1", 2, dialect).is_err());
    }
}

#[test]
fn metadata_bypasses_context_and_ordinary_sql_uses_its_dialect() {
    let environment = support::TestEnvironment::load();
    let identity = environment.gateway();
    let database = support::TestDatabase::create(&environment.source(None), "yasp_dialects");
    let backend = Arc::new(PostgresBackend::new(database.connection.backend_config()).unwrap());
    let observed = Arc::new(Mutex::new(Vec::new()));
    let hook = bind_query_context(observed.clone(), |request, state| {
        Box::pin(async move {
            let output = metabase(request, None)?;
            state.lock().unwrap().push(output.dialect);
            Ok(output)
        })
    });
    let server = support::GatewayServer::fixed(
        identity
            .gateway_config()
            .with_settings(GatewaySettings {
                max_sql_bytes: 256,
                ..GatewaySettings::default()
            })
            .unwrap()
            .with_query_context_hook(hook),
        backend,
    );
    let connection = identity.connection(server.address);
    for kind in ["native", "MBQL", "unknown"] {
        support::assert_success(
            &connection.query(&format!(
                "-- Metabase:: userID: 7 queryType: {kind} queryHash: abc\nSELECT 7"
            )),
            "remark data query",
        );
    }
    support::assert_success(
        &connection.query("SELECT 8"),
        "unmarked defaults to frontend",
    );
    for remark in [
        "",
        "-- Metabase:: userID: 7 queryType: native queryHash: abc\n",
    ] {
        support::assert_success(
            &connection.query(&format!("{remark}SHOW server_version")),
            "metadata ignores native remark",
        );
    }
    let oversized = connection.query(&format!("SELECT 1 /* {} */", "x".repeat(512)));
    assert!(String::from_utf8_lossy(&oversized.stderr).contains("54000"));
    assert_eq!(
        *observed.lock().unwrap(),
        vec![Some(SqlDialect::Postgresql); 4]
    );
    server.stop();
}

#[test]
fn redshift_profile_authenticates_over_tls_and_shares_query_lifecycle() {
    let environment = support::TestEnvironment::load();
    let identity = environment.gateway();
    let database = support::TestDatabase::create(&environment.source(None), "yasp_redshift");
    let backend = Arc::new(PostgresBackend::new(database.connection.backend_config()).unwrap());
    let directory =
        PathBuf::from(std::env::var_os("YASP_TEST_TLS_DIR").expect("use scripts/test.py"));
    let settings = GatewaySettings {
        protocol: "redshift".into(),
        tls: GatewayTls {
            mode: "enable".into(),
            cert_file: Some(directory.join("server.crt")),
            key_file: Some(directory.join("server.key")),
        },
        ..GatewaySettings::default()
    };
    let server = support::GatewayServer::fixed(
        identity.gateway_config().with_settings(settings).unwrap(),
        backend,
    );
    let mut command = identity.connection(server.address).command();
    let output = command
        .env("PGSSLMODE", "require")
        .args([
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            "SHOW server_version",
            "-c",
            "SELECT version()",
            "-c",
            "BEGIN READ ONLY",
            "-c",
            "SELECT 42",
            "-c",
            "ROLLBACK",
            "-c",
            "SELECT 43",
        ])
        .output()
        .unwrap();
    support::assert_success(&output, "Redshift TLS password profile");
    let output = String::from_utf8(output.stdout).unwrap();
    assert!(
        output.starts_with("8.0.2\nPostgreSQL 8.0.2, Redshift 1.0"),
        "{output}"
    );
    assert!(output.contains("42\nROLLBACK\n43\n"), "{output}");
    let plaintext = identity.connection(server.address).query("SELECT 1");
    assert!(!plaintext.status.success());
    let mut wrong = identity.connection(server.address);
    wrong.identity.password = "wrong".into();
    let output = wrong
        .command()
        .env("PGSSLMODE", "require")
        .args(["-c", "SELECT 1"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let bounded = identity
        .connection(server.address)
        .command()
        .env("PGSSLMODE", "require")
        .args([
            "-c",
            "SET readonly=0",
            "-c",
            "CREATE TABLE forbidden_write (n integer)",
            "-c",
            "SELECT 44",
        ])
        .output()
        .unwrap();
    assert!(String::from_utf8_lossy(&bounded.stderr).contains("0A000"));
    assert_eq!(String::from_utf8_lossy(&bounded.stdout).trim(), "SET\n44");
    server.stop();
}

#[test]
fn session_pid_matches_startup_without_routing_or_intercepting_data_sql() {
    use pgwire::{
        api::client::{
            ClientInfo, Config, auth::DefaultStartupHandler, query::DefaultSimpleQueryHandler,
        },
        tokio::client::PgWireClient,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use yasp::hooks::QueryContextError;

    let environment = support::TestEnvironment::load();
    let identity = environment.gateway();
    let database = support::TestDatabase::create(&environment.source(None), "yasp_session_pid");
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let hook = bind_query_context(calls.clone(), |_, count| {
        count.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Err(QueryContextError::Failed) })
    });
    let server = support::GatewayServer::fixed(
        identity.gateway_config().with_query_context_hook(hook),
        Arc::new(PostgresBackend::new(database.connection.backend_config()).unwrap()),
    );
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
        let mut config = Config::new();
        config.host(server.address.ip().to_string()).port(server.address.port())
            .user(&identity.user).password(&identity.password).dbname(&identity.database);
        let mut first = PgWireClient::connect(Arc::new(config.clone()), DefaultStartupHandler::new(), None).await.unwrap();
        let mut second = PgWireClient::connect(Arc::new(config), DefaultStartupHandler::new(), None).await.unwrap();
        assert_ne!(first.process_id(), second.process_id());
        for client in [&mut first, &mut second] {
            for sql in ["SELECT pg_backend_pid()", "/* probe */ select pg_catalog.pg_backend_pid ( );",
                "-- Metabase:: userID: 1 queryType: MBQL queryHash: abc\nSELECT pg_backend_pid()",
                "-- Metabase:: userID: 1 queryType: native queryHash: abc\nSELECT pg_backend_pid()"] {
                let mut rows = client.simple_query(DefaultSimpleQueryHandler::new(), sql).await.unwrap()
                    .remove(0).into_data_rows_reader();
                assert_eq!(rows.next_row().unwrap().next_value::<i32>().unwrap(), Some(client.process_id()));
                assert!(rows.next_row().is_none());
            }
        }
    });
    assert_eq!(
        observed.load(Ordering::SeqCst),
        0,
        "session probes bypass source context/routing"
    );
    for sql in [
        "SELECT pg_backend_pid() FROM users",
        "WITH hidden AS (SELECT * FROM users) SELECT pg_backend_pid()",
        "SELECT pg_backend_pid() + 1",
    ] {
        let output = identity.connection(server.address).query(sql);
        assert!(
            !output.status.success(),
            "data/native SQL must retain ordinary policy: {sql}"
        );
        assert!(output.stdout.is_empty());
    }
    assert_eq!(observed.load(Ordering::SeqCst), 3);
    server.stop();
}
