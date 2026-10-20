#![cfg(feature = "snowflake")]
//! Explicit network acceptance gate; the launcher owns the Snowflake HTTP fixture.
mod support;

use std::{collections::BTreeMap, sync::Arc, time::Duration};
use yasp::{
    backend::{
        Backends, PostgresBackends,
        snowflake::{SnowflakeBackend, SnowflakeConfig},
    },
    config::{BackendSettings, GatewaySettings},
    hooks::{BackendSelection, bind_select_backend, builtin::input_options::metabase},
    query::SqlDialect,
};

const NATIVE: &str = "-- Metabase:: userID: 7 queryType: native queryHash: abc\n";

#[test]
fn native_snowflake_through_pgwire_routes_values_transactions_and_recovers() {
    let environment = support::TestEnvironment::load();
    let identity = environment.gateway();
    let database = support::TestDatabase::create(&environment.source(None), "yasp_snowflake");
    support::assert_success(
        &database
            .connection
            .execute("CREATE TABLE yasp_catalog_only_marker (n integer)"),
        "create discovery-only marker",
    );
    let pg_selection = BackendSelection {
        target: "catalog".into(),
        backend_login: "reader".into(),
    };
    let sf_selection = BackendSelection {
        target: "snowflake".into(),
        backend_login: "analyst".into(),
    };
    let postgres = Arc::new(
        PostgresBackends::new(
            BTreeMap::from([(pg_selection.clone(), database.connection.backend_config())]),
            BackendSettings::default(),
        )
        .unwrap(),
    );
    let snowflake = snowflake_backend();
    let backends = Arc::new(
        Backends::new(postgres)
            .with_snowflake(sf_selection.clone(), snowflake.clone())
            .unwrap()
            .with_snowflake(
                BackendSelection {
                    target: "snowflake_alias".into(),
                    backend_login: "analyst".into(),
                },
                snowflake,
            )
            .unwrap(),
    );
    let selector = bind_select_backend(sf_selection, |request, state| {
        if request.query.sql().contains("route_postgres") {
            Ok(BackendSelection {
                target: "catalog".into(),
                backend_login: "reader".into(),
            })
        } else if request.query.sql().contains("route_snowflake_alias") {
            Ok(BackendSelection {
                target: "snowflake_alias".into(),
                backend_login: "analyst".into(),
            })
        } else {
            Ok(state.clone())
        }
    });
    let config = identity
        .gateway_config()
        .with_schema_discovery(pg_selection)
        .with_settings(GatewaySettings {
            query_timeout_ms: 1_500,
            ..GatewaySettings::default()
        })
        .unwrap()
        .with_input_options(metabase(SqlDialect::Snowflake));
    let server = support::GatewayServer::start(move |listener, stopped| async move {
        yasp::gateway::serve_backends(listener, config, backends, selector, async {
            let _ = stopped.await;
        })
        .await
        .unwrap();
    });
    check_native_runtime(&identity, server);
}

fn snowflake_backend() -> Arc<SnowflakeBackend> {
    Arc::new(
        SnowflakeBackend::new(SnowflakeConfig {
            private_key_file: std::env::var("YASP_SNOWFLAKE_PRIVATE_KEY_FILE")
                .unwrap()
                .into(),
            private_key_passphrase: None,
            account: "local".into(),
            username: "fake".into(),
            database: "YASP_DEMO".into(),
            schema: "PUBLIC".into(),
            role: "ANALYST".into(),
            warehouse: "TEST".into(),
            emulator: Some(
                std::env::var("YASP_SNOWFLAKE_ENDPOINT")
                    .unwrap()
                    .parse()
                    .unwrap(),
            ),
            max_connections: 2,
            acquire_timeout: Duration::from_secs(6),
            max_response_bytes: 8 * 1024 * 1024,
        })
        .unwrap(),
    )
}

#[test]
fn snowflake_catalog_uses_network_metadata_and_serves_both_frontend_profiles() {
    let environment = support::TestEnvironment::load();
    let identity = environment.gateway();
    let database = support::TestDatabase::create(&environment.source(None), "yasp_sf_catalog");
    let tls = std::path::PathBuf::from(std::env::var_os("YASP_TEST_TLS_DIR").unwrap());
    for profile in ["postgresql", "redshift"] {
        let pg_selection = BackendSelection {
            target: "catalog".into(),
            backend_login: "reader".into(),
        };
        let sf_selection = BackendSelection {
            target: "snowflake".into(),
            backend_login: "analyst".into(),
        };
        let postgres = Arc::new(
            PostgresBackends::new(
                BTreeMap::from([(pg_selection.clone(), database.connection.backend_config())]),
                BackendSettings::default(),
            )
            .unwrap(),
        );
        let sf = snowflake_backend();
        let backends = Arc::new(
            Backends::new(postgres)
                .with_snowflake(sf_selection.clone(), sf.clone())
                .unwrap(),
        );
        let routed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed = routed.clone();
        let selector = bind_select_backend(sf_selection, move |_, state| {
            observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(state.clone())
        });
        let config = identity
            .gateway_config()
            .with_schema_discovery(pg_selection)
            .with_settings(GatewaySettings {
                protocol: profile.into(),
                tls: yasp::config::GatewayTls {
                    mode: "enable".into(),
                    cert_file: Some(tls.join("server.crt")),
                    key_file: Some(tls.join("server.key")),
                },
                ..GatewaySettings::default()
            })
            .unwrap()
            .with_input_options(metabase(SqlDialect::Snowflake));
        let server = support::GatewayServer::start(move |listener, shutdown| async move {
            let catalog = yasp::catalog::Catalog::snowflake(
                sf,
                yasp::catalog::CatalogSettings::new(vec!["PUBLIC".into(), "ANALYTICS".into()]),
            )
            .await
            .expect("network Snowflake schema provider");
            let snapshot = catalog.snapshot().unwrap();
            assert!(
                snapshot
                    .schemas
                    .contains(&("YASP_DEMO".into(), "ANALYTICS".into()))
            );
            let relation = snapshot
                .relations
                .values()
                .find(|r| r.id.name == "METADATA_TYPES" && r.id.schema == "ANALYTICS")
                .expect("Snowflake definition");
            assert_eq!(snapshot.source.role.as_deref(), Some("ANALYST"));
            assert_eq!(snapshot.source.warehouse.as_deref(), Some("TEST"));
            assert_eq!(relation.columns[0].native_type.precision, Some(18));
            assert_eq!(relation.columns[0].native_type.scale, Some(0));
            assert!(!relation.columns[0].nullable);
            assert_eq!(relation.columns[3].id.name, "MixedCase");
            yasp::gateway::serve_backends(
                listener,
                config.with_catalog(catalog.clone()),
                backends,
                selector,
                async {
                    let _ = shutdown.await;
                },
            )
            .await
            .unwrap();
            catalog.stop_refreshing().await;
        });
        let fixture: serde_json::Value = serde_json::from_str(if profile == "postgresql" {
            include_str!("fixtures/jdbc_postgresql.json")
        } else {
            include_str!("fixtures/jdbc_redshift.json")
        })
        .unwrap();
        let mut connection = identity.connection(server.address).command();
        connection
            .env("PGSSLMODE", "require")
            .args(["-v", "ON_ERROR_STOP=1"]);
        for index in [
            1,
            if profile == "postgresql" { 2 } else { 3 },
            if profile == "postgresql" { 4 } else { 5 },
        ] {
            let sql = fixture["queries"][index]
                .as_str()
                .unwrap()
                .replace("'public'", "'ANALYTICS'")
                .replace("'users'", "'METADATA_TYPES'");
            connection.args(["-c", &sql]);
        }
        let result = connection.output().unwrap();
        support::assert_success(&result, "Snowflake definitions rendered through pgwire");
        let result = String::from_utf8_lossy(&result.stdout);
        assert!(result.contains("YASP_DEMO|ANALYTICS|METADATA_TYPES"));
        assert!(result.contains("MixedCase"));
        let column = result
            .lines()
            .find(|row| row.starts_with("YASP_DEMO|ANALYTICS|METADATA_TYPES|MixedCase|"))
            .unwrap()
            .split('|')
            .collect::<Vec<_>>();
        if profile == "postgresql" {
            assert_eq!(column[4], "1043");
            assert_eq!(column[6], "-1");
        } else {
            assert_eq!(column[5], "varchar");
            assert!(
                column[6].is_empty(),
                "emulator does not retain VARCHAR length"
            );
        }
        assert_eq!(
            routed.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "metadata never invokes application routing"
        );
        let mut preview = identity.connection(server.address).command();
        let preview = preview
            .env("PGSSLMODE", "require")
            .args([
                "-c",
                &format!("{NATIVE}SELECT count(*) FROM ANALYTICS.METADATA_TYPES"),
            ])
            .output()
            .unwrap();
        support::assert_success(
            &preview,
            "native chart SQL shares the catalog-enabled listener",
        );
        assert_eq!(String::from_utf8_lossy(&preview.stdout).trim(), "1");
        assert_eq!(routed.load(std::sync::atomic::Ordering::SeqCst), 1);
        let mut denied = identity.connection(server.address).command();
        let result = denied
            .env("PGSSLMODE", "require")
            .args(["-c", "SELECT relname FROM pg_catalog.pg_class"])
            .output()
            .unwrap();
        assert!(String::from_utf8_lossy(&result.stderr).contains("0A000"));
        server.stop();
    }
}

fn check_native_runtime(identity: &support::Identity, server: support::GatewayServer) {
    let connection = identity.connection(server.address);
    let sql = format!(
        "{NATIVE}SELECT 12345678901234567890.123456789::NUMBER(38,9), NULL, TO_BINARY('00ff', 'HEX'), TRUE, DATE '2024-02-29', TO_TIMESTAMP_NTZ('2024-02-29 12:34:56.123456'), TO_TIME('12:34:56.123456')"
    );
    let values = connection.query(&sql);
    support::assert_success(
        &values,
        "Snowflake values cross the real network and pgwire adapters",
    );
    assert_eq!(
        String::from_utf8_lossy(&values.stdout).trim(),
        "12345678901234567890.123456789||\\x00ff|t|2024-02-29|2024-02-29 12:34:56.123456|12:34:56.123456"
    );
    let transaction = connection.queries(&[
        "BEGIN READ ONLY",
        &format!("{NATIVE}SELECT 7::NUMBER(38,0)"),
        "ROLLBACK",
        &format!("{NATIVE}SELECT 8"),
    ]);
    support::assert_success(
        &transaction,
        "read-only transaction and next autocommit query recover",
    );
    assert!(String::from_utf8_lossy(&transaction.stdout).contains("ROLLBACK\n8"));
    let affinity = connection.queries(&[
        "BEGIN",
        &format!("{NATIVE}SELECT 1"),
        &format!("{NATIVE}SELECT 2 /* route_snowflake_alias */"),
        "ROLLBACK",
        &format!("{NATIVE}SELECT 3"),
    ]);
    assert!(
        String::from_utf8_lossy(&affinity.stderr).contains("25000"),
        "transaction affinity includes the logical binding even when its physical adapter is shared"
    );
    assert!(String::from_utf8_lossy(&affinity.stdout).contains("ROLLBACK\n3"));
    let generated = connection.query("SELECT 1");
    support::assert_success(&generated, "frontend SQL passes through unchanged");
    assert_eq!(String::from_utf8_lossy(&generated.stdout).trim(), "1");
    let affinity = connection.query(&format!("{NATIVE}SELECT 1 /* route_postgres */"));
    assert!(String::from_utf8_lossy(&affinity.stderr).contains("does not match"));
    let catalog_sql =
        "SELECT relname FROM pg_catalog.pg_class WHERE relname = 'yasp_catalog_only_marker'";
    let spoof = connection.query(&format!("{NATIVE}{catalog_sql}"));
    support::assert_success(
        &spoof,
        "emulator has its own PostgreSQL compatibility views",
    );
    assert!(
        spoof.stdout.is_empty(),
        "native catalog hints cannot borrow PostgreSQL discovery"
    );
    support::assert_success(
        &connection.query(catalog_sql),
        "catalog definitions retain designated PostgreSQL authority",
    );
    let timeout = connection.query(&format!(
        "{NATIVE}SELECT SUM(RANDOM()) FROM range(1000000000)"
    ));
    assert!(String::from_utf8_lossy(&timeout.stderr).contains("57014"));
    support::assert_success(
        &connection.query(&format!("{NATIVE}SELECT 9")),
        "capacity and frontend recover after timeout",
    );
    let error_recovery = connection.queries(&[
        "BEGIN",
        &format!("{NATIVE}SELECT N FROM YASP_MISSING_FIXTURE"),
        &format!("{NATIVE}SELECT 1"),
        "ROLLBACK",
        &format!("{NATIVE}SELECT 10"),
    ]);
    let errors = String::from_utf8_lossy(&error_recovery.stderr);
    assert!(errors.contains("25P02"));
    assert!(String::from_utf8_lossy(&error_recovery.stdout).contains("ROLLBACK\n10"));
    cancellation_and_disconnect(server.address, identity);
    server.stop();
}

fn cancellation_and_disconnect(address: std::net::SocketAddr, identity: &support::Identity) {
    use futures::SinkExt;
    use pgwire::{
        api::client::{
            ClientInfo, Config, auth::DefaultStartupHandler, query::DefaultSimpleQueryHandler,
        },
        error::PgWireClientError,
        messages::{PgWireFrontendMessage, cancel::CancelRequest},
        tokio::client::{PgWireClient, PgWireMessageClientCodec},
    };
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
        let mut config = Config::new();
        config.host(address.ip().to_string()).port(address.port()).user(&identity.user).password(&identity.password).dbname(&identity.database);
        let mut client = PgWireClient::connect(Arc::new(config), DefaultStartupHandler::new(), None).await.unwrap();
        use futures::StreamExt;
        use pgwire::messages::{PgWireBackendMessage, simplequery::Query};
        client.send(PgWireFrontendMessage::Query(Query::new(format!("{NATIVE}SELECT 1.000000001::NUMBER(38,9) AS D, TO_BINARY('00ff', 'HEX') AS B, NULL::VARCHAR AS Z, TO_TIMESTAMP_NTZ('2024-02-29 12:34:56.123456') AS T")))).await.unwrap();
        let mut described = false;
        while let Some(message) = client.next().await {
            match message.unwrap() {
                PgWireBackendMessage::RowDescription(description) => {
                    assert_eq!(description.fields.iter().map(|f| (f.name.as_str(), f.type_id)).collect::<Vec<_>>(), vec![("D", 1700), ("B", 17), ("Z", 25), ("T", 1114)]);
                    assert_eq!(description.fields[0].type_modifier, ((38 << 16) | 9) + 4);
                    described = true;
                }
                PgWireBackendMessage::ErrorResponse(error) => panic!("typed result probe failed: {error:?}"),
                PgWireBackendMessage::ReadyForQuery(_) => break,
                _ => {}
            }
        }
        assert!(described, "client received actual column metadata");
        let pid = client.process_id();
        let key = client.secret_key().clone();
        let sql = format!("{NATIVE}SELECT SUM(RANDOM()) FROM range(1000000000)");
        let mut query = Box::pin(client.simple_query(DefaultSimpleQueryHandler::new(), &sql));
        tokio::select! {
            result = &mut query => panic!("long query completed before cancel: {result:?}"),
            _ = tokio::time::sleep(Duration::from_millis(250)) => {}
        }
        let socket = tokio::net::TcpStream::connect(address).await.unwrap();
        let mut cancel = tokio_util::codec::Framed::new(socket, PgWireMessageClientCodec::default());
        cancel.send(PgWireFrontendMessage::CancelRequest(CancelRequest::new(pid, key))).await.unwrap();
        assert!(matches!(query.await, Err(PgWireClientError::RemoteError(info)) if info.code == "57014"));
        client.simple_query(DefaultSimpleQueryHandler::new(), &format!("{NATIVE}SELECT 11")).await.expect("explicit cancellation recovers");
        let mut query = Box::pin(client.simple_query(DefaultSimpleQueryHandler::new(), &sql));
        tokio::select! {
            result = &mut query => panic!("long query completed before disconnect: {result:?}"),
            _ = tokio::time::sleep(Duration::from_millis(250)) => {}
        }
        drop(query);
        drop(client);
        tokio::time::sleep(Duration::from_millis(100)).await;
    });
    support::assert_success(
        &identity
            .connection(address)
            .query(&format!("{NATIVE}SELECT 12")),
        "disconnect returns source capacity",
    );
}

#[path = "support/snowflake_host.rs"]
mod snowflake_host;

#[test]
fn yaml_snowflake_only_host_routes_native_data_and_serves_bi_definitions() {
    for profile in ["postgresql", "redshift"] {
        snowflake_host::with_host(profile, |_, _, query| {
            let result =
                query("SELECT AMOUNT, \"MixedCase\", PAYLOAD FROM ANALYTICS.METADATA_TYPES");
            support::assert_success(&result, "unmarked native Snowflake data");
            assert_eq!(
                String::from_utf8(result.stdout).unwrap().trim(),
                "12345678901234567890.123456789||\\x00ff"
            );
            let result = query("SELECT version()");
            support::assert_success(&result, "frontend version");
            let result = query(
                "-- Metabase:: userID: 1 queryType: MBQL queryHash: abc\nSELECT * FROM PUBLIC.USERS",
            );
            support::assert_success(&result, "frontend-generated Snowflake data");
            assert_eq!(String::from_utf8_lossy(&result.stdout).lines().count(), 3);
            assert!(!query("DELETE FROM PUBLIC.USERS").status.success());
        });
    }
}
