//! Superset SQLAlchemy reflection through PostgreSQL and Snowflake host bindings.
#[path = "support/catalog_host.rs"]
mod catalog_host;
mod support;
use support::assert_success;

#[test]
fn redshift_superset_reflection_uses_discovery_and_preserves_source_facts() {
    catalog_host::with_catalog_host(None, |database, query| {
        database.connection.execute("CREATE TABLE public.purchases (id integer PRIMARY KEY, user_id integer REFERENCES public.users(id), amount numeric(30,9) DEFAULT 123.456789012); COMMENT ON COLUMN public.purchases.amount IS 'exact amount'; CREATE VIEW public.user_names AS SELECT id, full_name FROM public.users");
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

#[cfg(all(feature = "snowflake", feature = "client-tests"))]
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
