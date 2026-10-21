#![cfg(feature = "snowflake")]
//! Host/configuration acceptance through the disposable Snowflake API emulator.
#[path = "support/snowflake_host.rs"]
mod snowflake_host;
mod support;
use serde_json::{Value, json};
use std::process::Command;
use support::assert_success;

fn redis(url: &str, args: &[&str]) -> Vec<u8> {
    let output = Command::new("redis-cli")
        .args(["-u", url, "--raw"])
        .args(args)
        .output()
        .unwrap();
    assert_success(&output, "Snowflake lineage Redis");
    let mut bytes = output.stdout;
    assert_eq!(bytes.pop(), Some(b'\n'));
    bytes
}

#[test]
fn configured_snowflake_lineage_is_shared_by_both_frontend_profiles() {
    let url = std::env::var("YASP_TEST_REDIS_URL").unwrap();
    for profile in ["postgresql", "redshift"] {
        let namespace = support::RedisNamespace::new(&url, "lineage_sf");
        let ast_namespace = support::RedisNamespace::new(&url, "lineage_sf_ast");
        snowflake_host::with_config(
            profile,
            |config| {
                config["data_contexts"] = json!({"ast":{"enabled":true,"dialect":"snowflake","namespace":ast_namespace.name}});
                config["hooks"]
                    .as_object_mut()
                    .unwrap()
                    .remove("input_options");
                config["cache"] = json!({"enabled":true,"url_env":"YASP_TEST_REDIS_URL","namespace":namespace.name,"ttl_seconds":30,"max_rows_per_query":100,"max_buffered_bytes_total":196608});
            },
            |_, _, query| {
                assert_success(
                    &query("SELECT version()"),
                    "frontend version without input options",
                );
                assert_success(
                    &query("SHOW search_path"),
                    "frontend setup without input options",
                );
                assert_success(
                    &query(include_str!(
                        "../src/gateway/metadata/queries/metabase_foreign_keys.sql"
                    )),
                    "frontend catalog without input options",
                );
                let sql = format!(
                    "SELECT u.EMAIL AS \"contact\", SUM(o.AMOUNT) AS \"amount\", '{}' AS \"marker\" FROM USERS u JOIN ORDERS o ON u.ID=o.USER_ID GROUP BY u.EMAIL QUALIFY ROW_NUMBER() OVER(ORDER BY u.EMAIL)=1",
                    namespace.name
                );
                let first = query(&sql);
                assert_success(&first, "Snowflake lineage fill");
                let id = yasp::query::ParsedQuery::parse_with_options(
                    &sql,
                    1_048_576,
                    yasp::query::InputOptions::native(yasp::query::SqlDialect::Snowflake),
                )
                .unwrap()
                .query_id()
                .to_owned();
                let ast_key = format!("{}:snowflake_reader:{id}", ast_namespace.name);
                let stored: Value =
                    serde_json::from_slice(&redis(&url, &["GET", &ast_key])).unwrap();
                assert_eq!(
                    stored["ast"]["columns"][0]["sources"][0]["column"],
                    "\"YASP_DEMO\".\"PUBLIC\".\"USERS\".\"EMAIL\""
                );
                assert_eq!(
                    stored["ast"]["columns"][1]["sources"][0]["column"],
                    "\"YASP_DEMO\".\"PUBLIC\".\"ORDERS\".\"AMOUNT\""
                );
                assert_eq!(stored["ast"]["columns"][2]["sources"], json!([]));
                assert_eq!(first.stdout, query(&sql).stdout);
                redis(&url, &["DEL", &ast_key]);
            },
        );
    }
}
