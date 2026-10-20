#![cfg(feature = "snowflake")]
//! Snowflake cache behavior through the configured host and real network adapter.
#[path = "support/snowflake_host.rs"]
mod snowflake_host;
mod support;
use std::{
    io::Write,
    process::{Command, Stdio},
    thread,
    time::Duration,
};
use support::{RedisNamespace, assert_success};

const MBQL: &str = "-- Metabase:: userID: 1 queryType: MBQL queryHash: abc\n";
const NATIVE: &str = "-- Metabase:: userID: 1 queryType: native queryHash: abc\n";

fn redis(url: &str, args: &[&str]) -> Vec<u8> {
    let output = Command::new("redis-cli")
        .args(["-u", url, "--raw"])
        .args(args)
        .output()
        .unwrap();
    assert_success(&output, "test Redis command");
    let mut bytes = output.stdout;
    assert_eq!(bytes.pop(), Some(b'\n'));
    bytes
}
fn key(namespace: &str, sql: &str) -> String {
    format!(
        "{namespace}:snowflake_reader:{}",
        yasp::query::ParsedQuery::parse(sql).unwrap().query_id()
    )
}
fn configure(config: &mut serde_json::Value, namespace: &str, ttl: u32, rows: usize) {
    config["cache"] = serde_json::json!({"enabled":true,"url_env":"YASP_TEST_REDIS_URL",
        "namespace":namespace,"ttl_seconds":ttl,"max_rows_per_query":rows,"max_buffered_bytes_total":196608});
    config["schema_discovery"]["catalog"]["refresh_interval_seconds"] = 60.into();
}

#[test]
fn hits_preserve_values_and_recover_from_corruption_without_caching_transactions() {
    let url = std::env::var("YASP_TEST_REDIS_URL").unwrap();
    for profile in ["postgresql", "redshift"] {
        let namespace = RedisNamespace::new(&url, "yasp_sf_cache");
        snowflake_host::with_config(
            profile,
            |config| configure(config, &namespace.name, 60, 10),
            |identity, address, query| {
                let sql = format!("{MBQL}SELECT RANDOM() AS value");
                let cache_key = key(&namespace.name, &sql);
                let first = query(&sql);
                assert_success(&first, "Snowflake cache miss");
                let hit = query(&format!("{MBQL} select random() AS value ;"));
                assert_success(&hit, "Snowflake cache hit");
                assert_eq!(
                    first.stdout, hit.stdout,
                    "volatile value must come from Redis"
                );
                assert_eq!(redis(&url, &["EXISTS", &cache_key]), b"1");
                let ttl = String::from_utf8(redis(&url, &["TTL", &cache_key]))
                    .unwrap()
                    .parse::<i64>()
                    .unwrap();
                assert!((1..=60).contains(&ttl));

                let native = query(&format!("{NATIVE}SELECT RANDOM() AS value"));
                assert_success(&native, "native input has a separate cache scope");
                assert_ne!(first.stdout, native.stdout);
                let generated = query(&sql);
                assert_success(&generated, "generated input replaces a different scope");
                assert_ne!(generated.stdout, native.stdout);
                assert_eq!(query(&sql).stdout, generated.stdout);

                let exact = format!(
                    "{MBQL}SELECT AMOUNT, \"MixedCase\", PAYLOAD, CREATED, RANDOM() FROM ANALYTICS.METADATA_TYPES"
                );
                let first = query(&exact);
                assert_success(&first, "lossless Snowflake cache capture");
                assert!(String::from_utf8_lossy(&first.stdout).starts_with(
                    "12345678901234567890.123456789||\\x00ff|2026-01-01 12:34:56.123456|"
                ));
                let hit = query(&exact);
                assert_success(&hit, "lossless Snowflake cache replay");
                assert_eq!(first.stdout, hit.stdout);

                let tls = std::path::PathBuf::from(std::env::var_os("YASP_TEST_TLS_DIR").unwrap());
                let transaction = identity
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
                        &sql,
                        "-c",
                        "ROLLBACK",
                    ])
                    .output()
                    .unwrap();
                assert_success(&transaction, "Snowflake transactions bypass cache");
                let values: Vec<_> = String::from_utf8_lossy(&transaction.stdout)
                    .lines()
                    .filter(|line| line.parse::<f64>().is_ok())
                    .map(str::to_owned)
                    .collect();
                assert_eq!(values.len(), 2);
                assert_ne!(values[0], values[1]);
                assert_eq!(
                    query(&sql).stdout,
                    generated.stdout,
                    "transaction reads must not replace the cached value"
                );

                let entry = redis(&url, &["GET", &cache_key]);
                for broken in [entry[..entry.len() - 1].to_vec(), {
                    let mut wrong = entry.clone();
                    *wrong.last_mut().unwrap() ^= 1; // Invalid completion terminator.
                    wrong
                }] {
                    let mut child = Command::new("redis-cli")
                        .args(["-u", &url, "-x", "SET", &cache_key])
                        .stdin(Stdio::piped())
                        .stdout(Stdio::piped())
                        .spawn()
                        .unwrap();
                    child.stdin.take().unwrap().write_all(&broken).unwrap();
                    assert_success(
                        &child.wait_with_output().unwrap(),
                        "corrupt owned cache entry",
                    );
                    let recovered = query(&sql);
                    assert_success(&recovered, "malformed cache entry is a miss");
                    assert_eq!(
                        String::from_utf8_lossy(&recovered.stdout).lines().count(),
                        1
                    );
                    assert_eq!(query(&sql).stdout, recovered.stdout);
                }
                let failure = format!("{MBQL}SELECT YASP_NO_SUCH_FUNCTION(1)");
                assert!(!query(&failure).status.success());
                assert_eq!(
                    redis(&url, &["EXISTS", &key(&namespace.name, &failure)]),
                    b"0"
                );
            },
        );
    }
}

#[test]
fn limits_expiry_and_profile_role_warehouse_scopes_are_enforced() {
    let url = std::env::var("YASP_TEST_REDIS_URL").unwrap();
    let namespace = RedisNamespace::new(&url, "yasp_sf_scopes");
    let sql = format!("{MBQL}SELECT RANDOM() AS value");
    let mut previous = None;
    for (profile, role, warehouse) in [
        ("postgresql", "PUBLIC", "LOCAL"),
        ("redshift", "PUBLIC", "LOCAL"),
        ("redshift", "ANALYST", "LOCAL"),
        ("redshift", "ANALYST", "OTHER"),
    ] {
        snowflake_host::with_config(
            profile,
            |config| {
                configure(config, &namespace.name, 60, 10);
                config["targets"]["snowflake"]["logins"]["reader"]["role"] = role.into();
                config["targets"]["snowflake"]["snowflake"]["warehouse"] = warehouse.into();
            },
            |_, _, query| {
                let result = query(&sql);
                assert_success(&result, "scope cache miss");
                if let Some(previous) = previous.take() {
                    assert_ne!(result.stdout, previous);
                }
                assert_eq!(query(&sql).stdout, result.stdout);
                previous = Some(result.stdout);
            },
        );
    }
    snowflake_host::with_config(
        "redshift",
        |config| {
            configure(config, &namespace.name, 1, 1);
            config["gateway"]["query_timeout_ms"] = 1000.into();
        },
        |_, _, query| {
            for sql in [
                format!("{MBQL}SELECT ID FROM PUBLIC.USERS"),
                format!("{MBQL}SELECT REPEAT('x', 100000)"),
            ] {
                let result = query(&sql);
                assert_success(&result, "result exceeding cache bounds still streams");
                assert_eq!(redis(&url, &["EXISTS", &key(&namespace.name, &sql)]), b"0");
            }
            let slow = format!("{NATIVE}SELECT SUM(RANDOM()) FROM range(1000000000)");
            let timed_out = query(&slow);
            assert!(!timed_out.status.success());
            assert!(String::from_utf8_lossy(&timed_out.stderr).contains("57014"));
            assert_eq!(redis(&url, &["EXISTS", &key(&namespace.name, &slow)]), b"0");
            let sql = format!("{MBQL}SELECT RANDOM() AS expires");
            let first = query(&sql);
            assert_success(&first, "expiring Snowflake result");
            assert_eq!(query(&sql).stdout, first.stdout);
            thread::sleep(Duration::from_millis(1100));
            let expired = query(&sql);
            assert_success(&expired, "expired cache entry");
            assert_ne!(first.stdout, expired.stdout);
        },
    );
}

#[path = "support/decision_model.rs"]
mod decision_mock;

#[test]
fn classification_is_shared_with_snowflake_and_replayed_without_inference() {
    let mock = decision_mock::MockModel::new();
    let url = std::env::var("YASP_TEST_REDIS_URL").unwrap();
    for profile in ["postgresql", "redshift"] {
        let namespace = RedisNamespace::new(&url, "sf_classification");
        snowflake_host::with_config(
            profile,
            |config| {
                configure(config, &namespace.name, 30, 100);
                config["data_contexts"] =
                    serde_json::json!({"decision_model": mock.settings(false)});
            },
            |_, _, query| {
                mock.clear();
                let sql = format!(
                    "SELECT 'alice@example.com' AS \"{}\", NULL AS \"missing\"",
                    namespace.name
                );
                let first = query(&sql);
                assert_success(&first, "classified Snowflake result");
                assert_eq!(mock.count(), 2);
                let requests = mock.requests.lock().unwrap().clone();
                assert!(
                    requests
                        .iter()
                        .any(|r| r["state"]["samples"] == serde_json::json!(["alice@example.com"]))
                );
                assert!(
                    requests
                        .iter()
                        .any(|r| r["state"]["samples"] == serde_json::json!([]))
                );
                assert_eq!(first.stdout, query(&sql).stdout);
                assert_eq!(mock.count(), 2, "Snowflake hits reuse decisions");
                let id = yasp::query::ParsedQuery::parse(&sql)
                    .unwrap()
                    .query_id()
                    .to_owned();
                let keys = String::from_utf8(redis(
                    &url,
                    &["KEYS", &format!("yasp:decision_cache:*:{id}:*")],
                ))
                .unwrap();
                assert_eq!(keys.lines().count(), 2);
                for key in keys.lines() {
                    redis(&url, &["DEL", key]);
                }
            },
        );
    }
}
