//! Metabase definition discovery through the configured gateway profiles.
#[path = "support/catalog_host.rs"]
mod catalog_host;
mod support;
use std::{
    thread,
    time::{Duration, Instant},
};
use support::assert_success;

#[test]
fn redshift_metabase_scan_uses_configured_catalog_without_application_access() {
    let query_context = serde_json::json!({
        "type": "rust",
        "callback": "builtin.query_context.metabase",
        "params": {"native_dialect": "postgresql"}
    });
    catalog_host::with_catalog_host(Some(query_context), |database, query| {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("fixtures/metabase_redshift.json")).unwrap();
        for captured in fixture["queries"].as_array().unwrap() {
            let sql = captured["sql"].as_str().unwrap();
            let result = query(sql);
            assert_success(&result, captured["operation"].as_str().unwrap());
            let rows = String::from_utf8(result.stdout).unwrap();
            if captured["operation"]
                .as_str()
                .unwrap()
                .starts_with("tables")
            {
                assert!(rows.contains("users|public|table|"), "{rows}");
                assert!(rows.contains("|t\n"), "selectable must be a boolean");
            } else {
                assert!(
                    rows.contains("id|int4|0|public|users|t|user identifier"),
                    "{rows}"
                );
                assert!(rows.contains("full_name|text|1|public|users|f|"), "{rows}");
            }
            assert!(
                !rows.contains("hidden"),
                "catalog must honor configured schema scope"
            );
            let list = query(
                &sql.replace("'public'", "'public', 'absent'")
                    .replace("'users'", "'users', 'absent'"),
            );
            assert_success(&list, "literal schema list");
            assert_eq!(String::from_utf8(list.stdout).unwrap(), rows);
            let absent = query(&sql.replace("'public'", "'absent'"));
            assert_success(&absent, "absent schema");
            if sql.contains("'public'") {
                assert!(absent.stdout.is_empty());
            }
            assert!(
                !query(&format!("{sql}; SELECT * FROM public.users"))
                    .status
                    .success()
            );
        }
        assert!(
            !query("SELECT * FROM public.users").status.success(),
            "ordinary reader has no application grants"
        );
        let tables = fixture["queries"][0]["sql"].as_str().unwrap();
        assert_success(
            &query(&format!(
                "-- Metabase:: userID: 1 queryType: native queryHash: abc\n{tables}"
            )),
            "recognized gateway metadata ignores native remarks",
        );
        let application_union = format!(
            "{tables} UNION ALL SELECT full_name, 'public', 'table', NULL, true FROM public.users"
        );
        assert!(!query(&application_union).status.success());
        database
            .connection
            .execute("ALTER TABLE public.users ADD COLUMN refreshed integer");
        let columns = fixture["queries"][2]["sql"].as_str().unwrap();
        let started = Instant::now();
        loop {
            let result = query(columns);
            assert_success(&result, "catalog refresh");
            if String::from_utf8(result.stdout)
                .unwrap()
                .contains("refreshed|int4|")
            {
                break;
            }
            assert!(
                started.elapsed() < Duration::from_secs(5),
                "configured periodic refresh did not publish DDL"
            );
            thread::sleep(Duration::from_millis(50));
        }
    });
}

#[cfg(feature = "snowflake")]
#[path = "support/snowflake_host.rs"]
mod snowflake_host;

#[cfg(feature = "snowflake")]
#[test]
fn metabase_schema_definitions_use_snowflake_in_both_profiles() {
    for profile in ["postgresql", "redshift"] {
        snowflake_host::with_host(profile, |_, _, query| {
            let mut queries: Vec<String> = if profile == "postgresql" {
                serde_json::from_str::<Vec<String>>(include_str!(
                    "fixtures/metabase_discovery.json"
                ))
                .unwrap()
                .into_iter()
                .map(|q| q.replace("IN (('public'))", "IN (('PUBLIC'))"))
                .collect()
            } else {
                let fixture: serde_json::Value =
                    serde_json::from_str(include_str!("fixtures/metabase_redshift.json")).unwrap();
                fixture["queries"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|q| {
                        let sql = q["sql"].as_str().unwrap();
                        if q["operation"].as_str().unwrap().starts_with("tables") {
                            sql.replace("'public'", "'PUBLIC'")
                                .replace("'users'", "'USERS'")
                        } else {
                            sql.to_owned() // The driver lowercases field filters.
                        }
                    })
                    .collect()
            };
            let foreign_keys =
                include_str!("../src/gateway/metadata/queries/metabase_foreign_keys.sql");
            queries.push(foreign_keys.into());
            for sql in queries {
                let output = query(&sql);
                support::assert_success(&output, "Metabase Snowflake metadata");
                if profile == "redshift" || sql == foreign_keys {
                    assert!(
                        !output.stdout.is_empty(),
                        "empty Metabase definitions: {sql}"
                    );
                }
                if sql == foreign_keys {
                    assert!(
                        String::from_utf8_lossy(&output.stdout)
                            .contains("PUBLIC|ORDERS|USER_ID|PUBLIC|USERS|ID")
                    );
                    assert!(
                        !query(&sql.replace("\"pg_class\"", "\"PUBLIC\".\"USERS\""))
                            .status
                            .success()
                    );
                }
                assert!(
                    !query(&format!("{sql}; SELECT * FROM PUBLIC.USERS"))
                        .status
                        .success()
                );
            }
            let filtered_keys =
                include_str!("../src/gateway/metadata/queries/metabase_foreign_keys_filtered.sql");
            let filtered = |schemas: &str| {
                let output = query(&filtered_keys.replace("'__YASP_SCHEMAS__'", schemas));
                support::assert_success(&output, "schema-filtered Metabase foreign keys");
                String::from_utf8(output.stdout).unwrap()
            };
            let public = filtered("'PUBLIC'");
            assert!(public.contains("PUBLIC|ORDERS|USER_ID|PUBLIC|USERS|ID"));
            assert!(public.lines().all(|row| row.starts_with("PUBLIC|")));
            let analytics = filtered("'ANALYTICS'");
            assert_eq!(
                analytics,
                "ANALYTICS|TYPE_REFERENCES|TENANT_ID|ANALYTICS|METADATA_TYPES|TENANT_ID\n\
                 ANALYTICS|TYPE_REFERENCES|TYPE_ID|ANALYTICS|METADATA_TYPES|ID\n"
            );
            assert_eq!(
                filtered("('PUBLIC'), ('ANALYTICS')"),
                format!("{analytics}{public}")
            );
            assert_eq!(filtered("('ANALYTICS'), ('ABSENT')"), analytics);
            assert!(filtered("'ABSENT'").is_empty());
            for (sql, expected) in [
                ("SHOW timezone", "UTC"),
                ("SHOW transaction isolation level", "read committed"),
            ] {
                let output = query(sql);
                support::assert_success(&output, "frontend setting probe");
                assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), expected);
            }
        });
    }
}

#[cfg(all(feature = "snowflake", feature = "client-tests"))]
#[path = "support/clients.rs"]
mod clients;

#[cfg(feature = "snowflake")]
#[test]
fn metabase_generated_queries_reach_snowflake_in_both_profiles() {
    const REMARK: &str = "-- Metabase:: userID: 1 queryType: MBQL queryHash: abc\n";
    const PROJECTION: &str = r#"SELECT "PUBLIC"."ORDERS"."ID" AS "ID", "PUBLIC"."ORDERS"."USER_ID" AS "USER_ID", "PUBLIC"."ORDERS"."PRODUCT_ID" AS "PRODUCT_ID", "PUBLIC"."ORDERS"."AMOUNT" AS "AMOUNT", "PUBLIC"."ORDERS"."ORDERED_AT" AS "ORDERED_AT" FROM "PUBLIC"."ORDERS" LIMIT 2000"#;
    for profile in ["postgresql", "redshift"] {
        snowflake_host::with_host(profile, |identity, address, query| {
            let sql = format!("{REMARK}{PROJECTION}");
            let output = query(&sql);
            support::assert_success(&output, "Metabase UI projection");
            let rows = String::from_utf8_lossy(&output.stdout);
            assert_eq!(rows.lines().count(), 2, "{rows}");
            assert!(rows.contains("|12.500000001|"), "{rows}");
            assert!(rows.contains("|80.000000002|"), "{rows}");
            for (sql, expected) in [
                (
                    r#"SELECT SUM("AMOUNT") FROM "PUBLIC"."ORDERS" WHERE "USER_ID" IN (1, 2)"#,
                    "92.500000003",
                ),
                (
                    r#"SELECT "u"."FULL_NAME" FROM "PUBLIC"."ORDERS" AS "o" JOIN "PUBLIC"."USERS" AS "u" ON "o"."USER_ID" = "u"."ID" ORDER BY "o"."ID""#,
                    "Ada Lovelace\nGrace Hopper",
                ),
                (
                    r#"WITH "orders" AS (SELECT "AMOUNT" FROM "PUBLIC"."ORDERS") SELECT MAX("AMOUNT") FROM "orders""#,
                    "80.000000002",
                ),
            ] {
                let output = query(&format!("{REMARK}{sql}"));
                support::assert_success(&output, "unchanged generated SQL");
                assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), expected);
            }
            let output = query(&format!("{REMARK}SELECT YASP_UNKNOWN_FUNCTION(1)"));
            assert!(!output.status.success());
            assert!(
                !String::from_utf8_lossy(&output.stderr).contains("translation is not enabled")
            );
            assert!(
                !query(&format!("{REMARK}DELETE FROM PUBLIC.ORDERS"))
                    .status
                    .success()
            );

            let tls = std::path::PathBuf::from(std::env::var_os("YASP_TEST_TLS_DIR").unwrap());
            let output = identity
                .connection(address)
                .command()
                .env("PGSSLMODE", "verify-full")
                .env("PGSSLROOTCERT", tls.join("ca.crt"))
                .args([
                    "-v",
                    "ON_ERROR_STOP=1",
                    "-c",
                    "BEGIN READ ONLY",
                    "-c",
                    &sql,
                    "-c",
                    "COMMIT",
                    "-c",
                    &sql,
                ])
                .output()
                .unwrap();
            support::assert_success(&output, "generated SQL transaction and autocommit recovery");
        });
    }
}

#[cfg(all(feature = "snowflake", feature = "client-tests"))]
#[test]
fn bundled_metabase_connection_and_metadata_accept_both_snowflake_frontends() {
    for profile in ["postgresql", "redshift"] {
        snowflake_host::with_host(profile, |identity, address, _| {
            let report = clients::run_metabase(profile, identity, address);
            assert_eq!(report["connected"], true, "{profile}: {report}");
            // Recognized connection probes retain frontend metadata types, even with native remarks.
            assert_eq!(report["native"]["jdbc_type"], 4, "{profile}: {report}");
            assert_eq!(report["native"]["value"], "1", "{profile}: {report}");
            let generated = report["generated"].as_array().unwrap();
            assert_eq!(generated.len(), 2, "{profile}: {report}");
            assert_eq!(
                generated[0]["amount"], "12.500000001",
                "{profile}: {report}"
            );
            assert_eq!(
                generated[1]["amount"], "80.000000002",
                "{profile}: {report}"
            );
            if profile == "redshift" {
                let metadata = &report["metadata"];
                assert_eq!(metadata["filtered_foreign_keys"], metadata["foreign_keys"]);
                let fields = metadata["public_fields"].as_array().unwrap();
                assert!(
                    fields.iter().any(|field| field["table-name"] == "ORDERS"
                        && field["name"] == "AMOUNT"
                        && field["database-position"] == 3),
                    "{metadata}"
                );
                assert!(
                    fields.iter().any(|field| field["table-name"] == "USERS"
                        && field["name"] == "ID"
                        && field["pk?"] == true
                        && field["field-comment"] == "Customer identifier"),
                    "{metadata}"
                );
                assert!(
                    metadata["analytics_fields"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|field| field["name"] == "MixedCase"),
                    "{metadata}"
                );
                let order_fields = metadata["order_fields"].as_array().unwrap();
                assert_eq!(order_fields.len(), 5, "{metadata}");
                assert!(
                    order_fields
                        .iter()
                        .all(|field| field["table-schema"] == "PUBLIC"
                            && field["table-name"] == "ORDERS"),
                    "{metadata}"
                );
                assert!(
                    metadata["foreign_keys"].as_array().unwrap().iter().any(
                        |key| key["fk-table-name"] == "ORDERS"
                            && key["fk-column-name"] == "USER_ID"
                            && key["pk-table-name"] == "USERS"
                            && key["pk-column-name"] == "ID"
                    ),
                    "{metadata}"
                );
            }
        });
    }
}
