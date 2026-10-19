//! Input grammar hints never authorize discovery or change PostgreSQL input validation.
mod support;

use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};
use yasp::{
    backend::PostgresBackend,
    config::{GatewaySettings, GatewayTls},
    hooks::{bind_context, builtin::input_options::metabase},
    query::{InputOptions, ParsedQuery, SqlDialect},
};

#[test]
fn native_grammar_and_read_only_inspection_use_the_same_dialect() {
    let sql = r"SELECT 'a\'b' AS value";
    assert!(ParsedQuery::parse(sql).is_err());
    let parsed =
        ParsedQuery::parse_with_options(sql, 4096, InputOptions::native(SqlDialect::Snowflake))
            .unwrap();
    assert_eq!(parsed.sql(), sql);
    assert_eq!(parsed.input_options().dialect, SqlDialect::Snowflake);
    parsed.validate_read_only().unwrap();
    let resolve = metabase(SqlDialect::Snowflake);
    for query_type in ["native", "MBQL", "unknown"] {
        let sql = format!(
            r#"-- /* partner: "metabase", {{"dashboard_id":null,"chart_id":57,"optional_user_id":1,"optional_account_id":null,"filter_values":{{}}}} */ Metabase:: userID: 1 queryType: {query_type} queryHash: abc
SELECT 1"#
        );
        let options = resolve(yasp::hooks::InputRequest {
            sql: &sql,
            frontend_dialect: SqlDialect::Redshift,
        });
        assert_eq!(options.native, query_type == "native");
        assert_eq!(
            options.dialect,
            if query_type == "native" {
                SqlDialect::Snowflake
            } else {
                SqlDialect::Redshift
            }
        );
    }
    for dialect in [
        SqlDialect::Postgresql,
        SqlDialect::Redshift,
        SqlDialect::Snowflake,
    ] {
        let parsed = ParsedQuery::parse_with_options(
            "WITH secret AS (SELECT * FROM users) DELETE FROM users",
            4096,
            InputOptions::native(dialect),
        )
        .unwrap();
        assert!(parsed.validate_read_only().is_err());
        assert!(
            ParsedQuery::parse_with_options(
                "SELECT 1; SELECT 2",
                4096,
                InputOptions::native(dialect)
            )
            .is_err()
        );
        assert!(
            ParsedQuery::parse_with_options("SELECT 1", 2, InputOptions::native(dialect)).is_err()
        );
    }
}

#[test]
fn remarks_choose_grammar_but_cannot_bypass_routing() {
    let environment = support::TestEnvironment::load();
    let identity = environment.gateway();
    let database = support::TestDatabase::create(&environment.source(None), "yasp_dialects");
    let backend = Arc::new(PostgresBackend::new(database.connection.backend_config()).unwrap());
    let observed = Arc::new(Mutex::new(Vec::new()));
    let hook = bind_context(observed.clone(), |request, state| {
        state.lock().unwrap().push(request.query.input_options());
        Box::pin(async { Ok(serde_json::Map::new()) })
    });
    let server = support::GatewayServer::fixed(
        identity
            .gateway_config()
            .with_settings(GatewaySettings {
                max_sql_bytes: 256,
                ..GatewaySettings::default()
            })
            .unwrap()
            .with_input_options(metabase(SqlDialect::Snowflake))
            .with_context_hook(hook),
        backend,
    );
    let connection = identity.connection(server.address);
    // Native Snowflake syntax parses before context, then fails engine affinity.
    let native = connection
        .query("-- Metabase:: userID: 7 queryType: native queryHash: abc\nSELECT 'a\\'b' AS value");
    assert!(String::from_utf8_lossy(&native.stderr).contains("does not match"));
    let mbql = connection.query("-- Metabase:: userID: 7 queryType: MBQL queryHash: abc\nSELECT 7");
    support::assert_success(&mbql, "MBQL defaults to PostgreSQL");
    assert_eq!(String::from_utf8_lossy(&mbql.stdout).trim(), "7");
    for remark in [
        "",
        "-- Metabase:: userID: 7 queryType: unknown queryHash: abc\n",
        "-- Metabase:: queryType: native\n",
    ] {
        support::assert_success(
            &connection.query(&format!("{remark}SELECT 8")),
            "unknown remarks default to frontend",
        );
    }
    // A native remark cannot borrow the discovery role, even for catalog SQL.
    let spoof = connection.query("-- Metabase:: userID: 7 queryType: native queryHash: abc\nSELECT relname FROM pg_catalog.pg_class");
    assert!(String::from_utf8_lossy(&spoof.stderr).contains("does not match"));
    support::assert_success(
        &connection.query("SELECT relname FROM pg_catalog.pg_class LIMIT 1"),
        "ordinary metadata bypass remains available",
    );
    let oversized = connection.query(&format!("SELECT 1 /* {} */", "x".repeat(512)));
    assert!(String::from_utf8_lossy(&oversized.stderr).contains("54000"));
    let requests = observed.lock().unwrap();
    assert_eq!(requests.len(), 6);
    assert!(requests[0].native && requests[0].dialect == SqlDialect::Snowflake);
    assert!(
        requests[1..5]
            .iter()
            .all(|options| !options.native && options.dialect == SqlDialect::Postgresql)
    );
    assert!(requests[5].native);
    drop(requests);
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
