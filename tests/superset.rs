//! Superset SQLAlchemy reflection through PostgreSQL and Snowflake host bindings.
#[path = "support/catalog_host.rs"]
mod catalog_host;
mod support;
use support::assert_success;

#[test]
fn redshift_superset_reflection_uses_discovery_and_preserves_source_facts() {
    catalog_host::with_catalog_host(None, |database, query| {
        database.connection.execute("CREATE TABLE public.purchases (id integer PRIMARY KEY, user_id integer REFERENCES public.users(id), amount numeric(30,9) DEFAULT 123.456789012); COMMENT ON COLUMN public.purchases.amount IS 'exact amount'; CREATE VIEW public.user_names AS SELECT id, full_name FROM public.users");
        let relations = include_str!("fixtures/superset_redshift.json");
        let captured: serde_json::Value = serde_json::from_str(relations).unwrap();
        let started = std::time::Instant::now();
        while !String::from_utf8_lossy(
            &query(captured["queries"][0]["sql"].as_str().unwrap()).stdout,
        )
        .contains("|user_names|")
        {
            assert!(
                started.elapsed() < std::time::Duration::from_secs(5),
                "catalog refresh publishes view"
            );
            std::thread::sleep(std::time::Duration::from_millis(30));
        }
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("fixtures/superset_redshift.json")).unwrap();
        for captured in fixture["queries"].as_array().unwrap() {
            let sql = captured["sql"].as_str().unwrap();
            let result = query(sql);
            assert_success(&result, captured["kind"].as_str().unwrap());
            let rows = String::from_utf8(result.stdout).unwrap();
            match captured["kind"].as_str().unwrap() {
                "relations" => assert!(
                    rows.contains("|public|") && rows.contains("|users|"),
                    "{rows}"
                ),
                "columns" => assert!(rows.contains("public|users|id||integer|"), "{rows}"),
                "constraints" => assert!(
                    rows.contains("public|users|p|users_pkey|{1}|1|id|PRIMARY KEY (id)|"),
                    "{rows}"
                ),
                _ => unreachable!(),
            }
            if captured["table"].is_null() {
                match captured["kind"].as_str().unwrap() {
                    "relations" => assert!(
                        rows.contains("|user_names|") && rows.contains("FROM users"),
                        "{rows}"
                    ),
                    "columns" => assert!(
                        rows.contains("numeric(30,9)")
                            && rows.contains("123.456789012")
                            && rows.contains("exact amount"),
                        "{rows}"
                    ),
                    "constraints" => assert!(
                        rows.contains("FOREIGN KEY (user_id) REFERENCES users(id)"),
                        "{rows}"
                    ),
                    _ => unreachable!(),
                }
            }
            if sql.contains("'public'") {
                let absent = query(&sql.replace("'public'", "'absent'"));
                assert_success(&absent, "absent schema");
                assert!(absent.stdout.is_empty());
                let injected =
                    query(&sql.replace("'public'", "'x''; SELECT full_name FROM public.users --'"));
                assert_success(&injected, "literal filter containing SQL");
                assert!(injected.stdout.is_empty());
            }
            for altered in [
                format!("{sql}; SELECT * FROM public.users"),
                sql.replace("pg_catalog.pg_class", "public.users"),
            ] {
                assert!(!query(&altered).status.success());
            }
        }
        assert!(!query("SELECT * FROM public.users").status.success());
    });
}

#[cfg(feature = "client-tests")]
#[path = "support/clients.rs"]
mod clients;
#[cfg(all(feature = "snowflake", feature = "client-tests"))]
#[path = "support/snowflake_host.rs"]
mod snowflake_host;

#[cfg(all(feature = "snowflake", feature = "client-tests"))]
#[test]
fn sqlalchemy_reflection_uses_snowflake_only_yaml_host() {
    for profile in ["postgresql", "redshift"] {
        snowflake_host::with_host(profile, |identity, address, _| {
            // sqlalchemy-redshift 0.8.14 returns quoted SQL identifier tokens
            // from condef; PostgreSQL's dialect decodes them into column names.
            let key_name = |name: &str| {
                if profile == "redshift" {
                    format!("\"{name}\"")
                } else {
                    name.to_owned()
                }
            };
            let report = clients::run_client(profile, identity, address, "PUBLIC", "USERS", true);
            std::fs::write(
                support::root().join(format!("target/compatibility/sqlalchemy-{profile}.json")),
                serde_json::to_vec_pretty(&report).unwrap(),
            )
            .unwrap();
            for observation in report["observations"]
                .as_array()
                .unwrap_or_else(|| panic!("{report}"))
            {
                assert_eq!(observation["passed"], true, "{profile}: {observation}");
                let result = &observation["result"];
                match observation["operation"].as_str().unwrap() {
                    "session_pid" => assert!(result.is_i64(), "frontend PID is an integer"),
                    "schemas" => assert!(result.as_array().unwrap().iter().any(|s| s == "PUBLIC")),
                    "tables" => assert!(result.as_array().unwrap().iter().any(|s| s == "USERS")),
                    "views" => assert_eq!(result[0], "DAILY_SALES"),
                    "columns" => {
                        assert_eq!(result[0]["name"], "ID");
                        assert_eq!(result[0]["comment"], "Customer identifier");
                    }
                    "primary_keys" => {
                        assert_eq!(
                            result["constrained_columns"],
                            serde_json::json!([key_name("ID")])
                        )
                    }
                    "foreign_keys" => assert_eq!(result.as_array().unwrap().len(), 2),
                    "composite_keys" => assert_eq!(
                        result[0]["constrained_columns"],
                        serde_json::json!([key_name("TENANT_ID"), key_name("TYPE_ID")])
                    ),
                    "indexes" => assert!(result.as_array().unwrap().is_empty()),
                    "uniques" => {
                        assert_eq!(result[0]["column_names"], serde_json::json!(["EMAIL"]))
                    }
                    "comments" => assert_eq!(result["text"], "Demo customers"),
                    _ => {}
                }
            }
        });
    }
}

#[cfg(feature = "client-tests")]
#[test]
fn sqlalchemy_reflection_uses_postgres_snapshot_in_both_profiles() {
    use std::{path::PathBuf, sync::Arc};
    use yasp::{
        backend::PostgresBackend,
        catalog::{Catalog, CatalogSettings},
        config::{GatewaySettings, GatewayTls},
    };
    let env = support::TestEnvironment::load();
    let identity = env.gateway();
    let database = support::TestDatabase::create(&env.source(None), "yasp_pg_sqlalchemy");
    database.connection.execute(r#"CREATE SCHEMA "PUBLIC"; CREATE SCHEMA "ANALYTICS";
        CREATE TYPE "PUBLIC".mood AS ENUM ('happy', 'quiet');
        CREATE DOMAIN "PUBLIC".positive AS integer CHECK (VALUE > 0);
        CREATE TABLE "PUBLIC"."USERS" ("ID" bigint GENERATED ALWAYS AS IDENTITY (START WITH 7 INCREMENT BY 3) PRIMARY KEY, "EMAIL" text UNIQUE, "STATE" "PUBLIC".mood, "SCORE" "PUBLIC".positive);
        COMMENT ON TABLE "PUBLIC"."USERS" IS 'source customers';
        CREATE INDEX users_email_idx ON "PUBLIC"."USERS" ("EMAIL" DESC) WHERE "EMAIL" IS NOT NULL;
        CREATE TABLE "PUBLIC"."ORDERS" ("ID" integer PRIMARY KEY, "USER_ID" bigint REFERENCES "PUBLIC"."USERS"("ID"));
        CREATE TABLE "ANALYTICS"."TYPES" ("TENANT_ID" integer, "ID" integer, PRIMARY KEY ("TENANT_ID", "ID"));
        CREATE TABLE "ANALYTICS"."TYPE_REFERENCES" ("TENANT_ID" integer, "TYPE_ID" integer, FOREIGN KEY ("TENANT_ID", "TYPE_ID") REFERENCES "ANALYTICS"."TYPES"("TENANT_ID", "ID"));
        CREATE VIEW "ANALYTICS"."DAILY_SALES" AS SELECT "ID" FROM "PUBLIC"."ORDERS";
        CREATE VIEW public.users AS SELECT "ID" AS id FROM "PUBLIC"."USERS";"#);
    let directory = PathBuf::from(std::env::var_os("YASP_TEST_TLS_DIR").unwrap());
    for profile in ["postgresql", "redshift"] {
        let backend = Arc::new(PostgresBackend::new(database.connection.backend_config()).unwrap());
        let config = identity
            .gateway_config()
            .with_settings(GatewaySettings {
                protocol: profile.into(),
                tls: GatewayTls {
                    mode: "enable".into(),
                    cert_file: Some(directory.join("server.crt")),
                    key_file: Some(directory.join("server.key")),
                },
                ..Default::default()
            })
            .unwrap();
        let server = support::GatewayServer::start(move |listener, shutdown| async move {
            let catalog = Catalog::postgres(
                backend.clone(),
                CatalogSettings::new(vec!["PUBLIC".into(), "ANALYTICS".into(), "public".into()]),
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
        let report =
            clients::run_client(profile, &identity, server.address, "PUBLIC", "USERS", true);
        std::fs::create_dir_all(support::root().join("target/compatibility")).unwrap();
        std::fs::write(
            support::root().join(format!(
                "target/compatibility/sqlalchemy-postgres-{profile}.json"
            )),
            serde_json::to_vec_pretty(&report).unwrap(),
        )
        .unwrap();
        for observation in report["observations"].as_array().unwrap() {
            assert_eq!(observation["passed"], true, "{profile}: {observation}");
            match observation["operation"].as_str().unwrap() {
                "columns" if profile == "postgresql" => assert_eq!(
                    observation["result"][0]["identity"]["start"], 7,
                    "{observation}"
                ),
                "indexes" if profile == "postgresql" => assert!(
                    observation["result"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|i| i["name"] == "users_email_idx"),
                    "{observation}"
                ),
                "foreign_keys" | "composite_keys" => {
                    assert_eq!(observation["result"].as_array().unwrap().len(), 1)
                }
                _ => {}
            }
        }
        server.stop();
    }
}
