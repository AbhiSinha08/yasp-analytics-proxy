//! Capture the deployed JDBC drivers' SQL against disposable PostgreSQL data.
mod support;
use std::{
    io::Write,
    path::PathBuf,
    process::{Command, Stdio},
    sync::{Arc, Mutex},
};
use yasp::{
    backend::PostgresBackend,
    config::{GatewaySettings, GatewayTls},
    query::InputOptions,
};

#[test]
#[ignore = "requires the pinned Metabase Docker image; use scripts/compatibility/jdbc_probe.py"]
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
            .with_input_options(move |request| {
                let mut queries = capture.lock().unwrap();
                if queries.len() < 1024 {
                    queries.push(request.sql.to_owned());
                }
                InputOptions::frontend(request.frontend_dialect)
            });
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
        let name = support::unique_name("yasp-jdbc");
        let mut command = Command::new("docker");
        command.args([
            "run",
            "--rm",
            "--pull=never",
            "-i",
            "--name",
            &name,
            "--label",
            &format!(
                "yasp.compatibility={}",
                std::env::var("YASP_JDBC_RUN").unwrap()
            ),
        ]);
        for key in [
            "YASP_JDBC_PROFILE",
            "YASP_JDBC_USER",
            "YASP_JDBC_PASSWORD",
            "YASP_JDBC_HOST",
            "YASP_JDBC_PORT",
            "YASP_JDBC_DATABASE",
        ] {
            command.args(["--env", key]);
        }
        command.args([
            "--mount",
            &format!(
                "type=bind,src={},dst=/tmp/yasp-fixture-ca.crt,readonly",
                directory.join("ca.crt").display()
            ),
        ]);
        command
            .args([
                "--entrypoint",
                "java",
                &image,
                "-Xmx256m",
                "-cp",
                "/app/metabase.jar",
                "clojure.main",
                "-",
            ])
            .env("YASP_JDBC_PROFILE", profile)
            .env("YASP_JDBC_USER", &identity.user)
            .env("YASP_JDBC_PASSWORD", &identity.password)
            .env("YASP_JDBC_DATABASE", &identity.database)
            .env("YASP_JDBC_PORT", server.address.port().to_string())
            .env(
                "YASP_JDBC_HOST",
                std::env::var("YASP_JDBC_HOST").unwrap_or_else(|_| "host.docker.internal".into()),
            )
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command.spawn().expect("Docker client starts");
        child
            .stdin
            .take()
            .unwrap()
            .write_all(include_bytes!("../scripts/compatibility/jdbc_probe.clj"))
            .unwrap();
        let result = child.wait_with_output().unwrap();
        // The name belongs exclusively to this run; --rm normally removed it.
        let _ = Command::new("docker").args(["rm", "-f", &name]).output();
        server.stop();
        let stdout = String::from_utf8_lossy(&result.stdout);
        let report = stdout
            .lines()
            .find_map(|line| line.strip_prefix("RESULT "))
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap());
        let report = report.unwrap_or_else(|| serde_json::json!({"connected":false, "error":String::from_utf8_lossy(&result.stderr)}));
        let queries = observed.lock().unwrap();
        std::fs::write(output.join(format!("jdbc-{profile}.json")), serde_json::to_vec_pretty(&serde_json::json!({"profile":profile,"image":image,"client":report,"queries":*queries})).unwrap()).unwrap();
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
