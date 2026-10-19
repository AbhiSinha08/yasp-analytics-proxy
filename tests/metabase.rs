//! Replay Metabase's own Redshift sync SQL through the configured host.
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
    let input_options = serde_json::json!({
        "type": "rust",
        "callback": "builtin.input_options.metabase",
        "params": {"native_dialect": "postgresql"}
    });
    catalog_host::with_catalog_host(Some(input_options), |database, query| {
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
        assert!(
            !query(&format!(
                "-- Metabase:: userID: 1 queryType: native queryHash: abc\n{tables}"
            ))
            .status
            .success(),
            "native remarks must not borrow catalog access"
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
