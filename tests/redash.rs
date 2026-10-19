//! Replay Redash's Redshift schema SQL through the configured host.
#[path = "support/catalog_host.rs"]
mod catalog_host;
mod support;
use support::assert_success;

#[test]
fn redshift_redash_scan_uses_configured_catalog_without_application_access() {
    catalog_host::with_catalog_host(None, |_database, query| {
        let redash = include_str!("../src/gateway/metadata/queries/redash_redshift_columns.sql");
        let result = query(redash);
        assert_success(&result, "Redash Redshift schema scan");
        let rows = String::from_utf8(result.stdout).unwrap();
        assert!(rows.contains("users|public|id|int4\n"), "{rows}");
        assert!(rows.contains("users|public|full_name|text\n"), "{rows}");
        assert!(!rows.contains("hidden"));
        for altered in [
            format!("{redash}; SELECT * FROM public.users"),
            redash.replace("svv_columns", "public.users"),
            redash.replace("SVV_EXTERNAL_SCHEMAS", "public.users"),
        ] {
            assert!(!query(&altered).status.success());
        }
        assert!(
            !query("SELECT * FROM public.users").status.success(),
            "ordinary reader has no application grants"
        );
    });
}

#[cfg(feature = "snowflake")]
#[path = "support/snowflake_host.rs"]
mod snowflake_host;

#[cfg(feature = "snowflake")]
#[test]
fn redash_schema_definitions_use_snowflake_in_both_profiles() {
    for (profile, sql, expected) in [
        (
            "postgresql",
            include_str!("../src/gateway/metadata/queries/redash_postgresql_columns.sql"),
            "PUBLIC|USERS|ID|numeric(18,0)",
        ),
        (
            "redshift",
            include_str!("../src/gateway/metadata/queries/redash_redshift_columns.sql"),
            "USERS|PUBLIC|ID|numeric(18,0)",
        ),
    ] {
        snowflake_host::with_host(profile, |_, _, query| {
            let output = query(sql);
            support::assert_success(&output, "Redash Snowflake schema scan");
            assert!(String::from_utf8(output.stdout).unwrap().contains(expected));
            assert!(
                !query(&format!("{sql}; SELECT * FROM PUBLIC.USERS"))
                    .status
                    .success()
            );
        });
    }
}
