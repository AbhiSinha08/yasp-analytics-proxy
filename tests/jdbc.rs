//! Bundled JDBC drivers against disposable PostgreSQL and Snowflake bindings.
#[path = "support/clients.rs"]
mod clients;
mod support;
use clients::run_client;
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};
use yasp::{
    backend::PostgresBackend,
    config::{GatewaySettings, GatewayTls},
};

#[test]
fn bundled_jdbc_setup_and_metadata_corpus() {
    yasp::logger::init_with_level("DEBUG").unwrap();
    let environment = support::TestEnvironment::load();
    let identity = environment.gateway();
    let database = support::TestDatabase::create(&environment.source(None), "yasp_jdbc");
    database.populate();
    let directory =
        PathBuf::from(std::env::var_os("YASP_TEST_TLS_DIR").expect("use scripts/test.py"));
    let output = support::root().join("target/compatibility");
    std::fs::create_dir_all(&output).unwrap();
    let image = std::env::var("YASP_JDBC_IMAGE").expect("launcher pins image");
    let mut connected = Vec::new();
    for profile in ["postgresql", "redshift"] {
        let observed = Arc::new(Mutex::new(Vec::<String>::new()));
        let capture = observed.clone();
        let config = identity
            .gateway_config()
            .with_settings(GatewaySettings {
                protocol: profile.into(),
                query_timeout_ms: 5_000,
                tls: GatewayTls {
                    mode: "enable".into(),
                    cert_file: Some(directory.join("server.crt")),
                    key_file: Some(directory.join("server.key")),
                },
                ..GatewaySettings::default()
            })
            .unwrap()
            .with_query_context_hook(yasp::hooks::bind_query_context((), move |request, _| {
                let mut queries = capture.lock().unwrap();
                if queries.len() < 1024 {
                    queries.push(request.sql.to_owned());
                }
                Box::pin(async { Ok(Default::default()) })
            }));
        let backend = Arc::new(PostgresBackend::new(database.connection.backend_config()).unwrap());
        let server = support::GatewayServer::start(move |listener, shutdown| async move {
            let catalog = yasp::catalog::Catalog::postgres(
                backend.clone(),
                yasp::catalog::CatalogSettings::new(vec!["public".into()]),
            )
            .await
            .unwrap();
            yasp::gateway::serve(
                listener,
                config.with_catalog(catalog.clone()),
                backend,
                async {
                    let _ = shutdown.await;
                },
            )
            .await
            .unwrap();
            catalog.stop_refreshing().await;
        });
        let report = run_client(profile, &identity, server.address, "public", "users", false);
        server.stop();
        let queries = observed.lock().unwrap();
        std::fs::write(output.join(format!("jdbc-{profile}.json")), serde_json::to_vec_pretty(&serde_json::json!({"profile":profile,"image":image,"client":report,"ordinary_queries":*queries})).unwrap()).unwrap();
        let mut required = vec![
            "schemas",
            "tables",
            "columns",
            "primary_keys",
            "foreign_keys",
            "indexes",
            "preview",
            "transaction",
        ];
        if profile == "redshift" {
            required.extend(["metabase_tables", "metabase_fields"]);
        }
        let supported = report["observations"]
            .as_array()
            .is_some_and(|observations| {
                required.iter().all(|name| {
                    observations
                        .iter()
                        .any(|o| o["operation"] == *name && o["passed"] == true)
                })
            });
        connected.push((profile, report["connected"] == true && supported));
        assert!(
            supported,
            "{profile} required JDBC operations failed: {report}"
        );
        let observations = report["observations"]
            .as_array()
            .expect("driver observations");
        for operation in ["tables", "columns"] {
            let observation = observations
                .iter()
                .find(|o| o["operation"] == operation)
                .unwrap();
            let rows = observation["rows"].as_array().expect("metadata rows");
            assert!(
                !rows.is_empty(),
                "{profile} {operation} returned definitions"
            );
            assert_eq!(rows[0][1], "public");
            assert_eq!(rows[0][2], "users");
            if operation == "columns" {
                assert_eq!(rows[0][3], "id");
                assert_eq!(rows[0][4], "4");
                assert_eq!(rows.len(), 6);
            }
        }
        if profile == "redshift" {
            let tables = observations
                .iter()
                .find(|o| o["operation"] == "metabase_tables")
                .unwrap();
            assert!(
                tables["rows"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|row| row[0] == "users" && row[1] == "public" && row[4] == "true")
            );
            let fields = observations
                .iter()
                .find(|o| o["operation"] == "metabase_fields")
                .unwrap();
            let rows = fields["rows"].as_array().unwrap();
            assert_eq!(rows[0][0], "id");
            assert_eq!(rows[0][2], "0");
            assert_eq!(rows[0][5], "true");
            assert_eq!(rows.len(), 6);
        }
        eprintln!(
            "{profile}: {} queries captured; connected={}",
            queries.len(),
            report["connected"]
        );
    }
    assert!(
        connected.iter().all(|(_, connected)| *connected),
        "JDBC connection gate failed: {connected:?}; see target/compatibility/jdbc-*.json"
    );
}

#[cfg(feature = "snowflake")]
#[path = "support/snowflake_host.rs"]
mod snowflake_host;

#[cfg(feature = "snowflake")]
#[test]
fn bundled_jdbc_metadata_uses_snowflake_only_yaml_host() {
    for profile in ["postgresql", "redshift"] {
        snowflake_host::with_host(profile, |identity, address, _| {
            let report = run_client(profile, identity, address, "PUBLIC", "USERS", false);
            assert_eq!(report["connected"], true, "{report}");
            for observation in report["observations"].as_array().unwrap() {
                assert_eq!(observation["passed"], true, "{profile}: {observation}");
                match observation["operation"].as_str().unwrap() {
                    "native_values" => {
                        assert_eq!(observation["decimal"], "12345678901234567890.123456789");
                        assert_eq!(observation["null"], true);
                        assert_eq!(observation["binary"], serde_json::json!([0, -1]));
                        assert_eq!(observation["timestamp"], "2026-01-01T12:34:56.123456");
                    }
                    "primary_keys" => {
                        assert_eq!(observation["rows"][0][3], "ID");
                        assert_eq!(observation["rows"][0][4], "1");
                    }
                    "composite_primary_keys" => {
                        let mut columns = observation["rows"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .map(|row| (row[4].as_str().unwrap(), row[3].as_str().unwrap()))
                            .collect::<Vec<_>>();
                        columns.sort();
                        assert_eq!(columns, [("1", "TENANT_ID"), ("2", "ID")]);
                    }
                    "tables" | "columns" | "foreign_keys" | "composite_foreign_keys" => assert!(
                        !observation["rows"].as_array().unwrap().is_empty(),
                        "{observation}"
                    ),
                    "indexes" => assert!(
                        observation["rows"].as_array().unwrap().is_empty(),
                        "Snowflake has no PostgreSQL indexes"
                    ),
                    _ => {}
                }
            }
        });
    }
}
