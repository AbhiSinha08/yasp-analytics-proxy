#![cfg(feature = "redshift-tests")]
//! Native Redshift adapter acceptance against an unmodified local server.
#[path = "support/redshift_host.rs"]
mod redshift_host;
mod support;
use support::assert_success;

const NATIVE: &str = "-- Metabase:: userID: 1 queryType: native queryHash: redshift\n";

#[test]
fn configured_redshift_backend_serves_native_and_generated_queries_in_both_profiles() {
    for profile in ["postgresql", "redshift"] {
        redshift_host::with_host(profile, |fixture, identity, address, query| {
            for sql in [
                "SELECT id, full_name, email FROM public.users ORDER BY id",
                "SELECT full_name FROM public.users WHERE id >= 1 AND email IS NOT NULL ORDER BY full_name LIMIT 1",
                "SELECT u.full_name, count(o.id), sum(o.amount) FROM public.users u JOIN analytics.orders o ON o.user_id = u.id GROUP BY u.full_name ORDER BY u.full_name",
                "SELECT date_trunc('day', ordered_at), count(*) FROM analytics.orders GROUP BY 1 ORDER BY 1",
            ] {
                let expected = fixture.query(sql);
                assert_success(&expected, "direct source query");
                let actual = query(sql);
                assert_success(&actual, "generated query through Redshift backend");
                assert_eq!(actual.stdout, expected.stdout, "{profile}: {sql}");
            }
            for sql in [
                "SELECT DATEADD(day, 1, '2026-01-02'::timestamp)::date",
                "SELECT DATEDIFF(day, '2026-01-01'::timestamp, '2026-01-03'::timestamp)",
                "SELECT JSON_TYPEOF(JSON_PARSE('[1,2]'))",
                "SELECT JSON_SERIALIZE(JSON_PARSE('12345678901234567890.123456789'))",
                "SELECT payload.customer.name::varchar FROM analytics.events WHERE id = 1",
                "SELECT payload.items[0].sku::varchar FROM analytics.events WHERE id = 1",
            ] {
                let expected = fixture.query(sql);
                assert_success(&expected, "direct native query");
                let actual = query(&format!("{NATIVE}{sql}"));
                assert_success(&actual, "native Redshift query through YASP");
                assert_eq!(actual.stdout, expected.stdout, "{profile}: {sql}");
            }
            for sql in [
                "SELECT * FROM public.does_not_exist",
                "SELECT 1 / 0",
                "DELETE FROM public.users",
                "WITH bad AS (DELETE FROM public.users RETURNING *) SELECT * FROM bad",
                "SELECT 1; SELECT 2",
                "SELECT set_config('search_path', 'analytics', false)",
            ] {
                assert!(!query(sql).status.success(), "must reject: {sql}");
                assert_success(
                    &query("SELECT count(*) FROM public.users"),
                    "source pool recovers",
                );
            }
            let tls = std::path::PathBuf::from(std::env::var_os("YASP_TEST_TLS_DIR").unwrap());
            let recovery = identity
                .connection(address)
                .command()
                .env("PGSSLMODE", "verify-full")
                .env("PGSSLROOTCERT", tls.join("ca.crt"))
                .args([
                    "-v",
                    "ON_ERROR_STOP=0",
                    "-c",
                    "SELECT 1 / 0",
                    "-c",
                    "SELECT 1",
                ])
                .output()
                .unwrap();
            assert_success(
                &recovery,
                "same frontend session recovers after source error",
            );
            assert!(String::from_utf8_lossy(&recovery.stderr).contains("22012"));
            assert_eq!(String::from_utf8_lossy(&recovery.stdout).trim(), "1");
            let transaction = identity
                .connection(address)
                .command()
                .env("PGSSLMODE", "verify-full")
                .env("PGSSLROOTCERT", tls.join("ca.crt"))
                .args([
                    "-v",
                    "ON_ERROR_STOP=1",
                    "-c",
                    "BEGIN",
                    "-c",
                    "SELECT count(*) FROM public.users",
                    "-c",
                    "ROLLBACK",
                    "-c",
                    "SELECT 1",
                ])
                .output()
                .unwrap();
            assert_success(&transaction, "native read-only transaction and recovery");
            let empty = query("SELECT id FROM public.users WHERE 1 = 0");
            assert_success(&empty, "zero-row result");
            assert!(empty.stdout.is_empty());
        });
    }
}

#[test]
fn configured_source_login_grants_are_enforced_after_successful_discovery() {
    for profile in ["postgresql", "redshift"] {
        redshift_host::with_config(
            profile,
            |config| {
                config["hooks"]["select_backend"]["params"]["backend_login"] = "denied".into();
            },
            |_, _, _, query| {
                assert_success(
                    &query("SELECT 1"),
                    "restricted login can execute a constant",
                );
                let denied = query("SELECT * FROM public.users");
                assert!(!denied.status.success());
                assert!(String::from_utf8_lossy(&denied.stderr).contains("42501"));
                assert_success(
                    &query("SELECT 2"),
                    "permission failure does not poison the session",
                );
            },
        );
    }
}

#[test]
fn cache_replays_exact_results_and_transactions_bypass_old_entries() {
    let url = std::env::var("YASP_TEST_REDIS_URL").unwrap();
    for profile in ["postgresql", "redshift"] {
        let namespace = support::RedisNamespace::new(&url, "yasp_rs_cache");
        redshift_host::with_config(
            profile,
            |config| {
                config["cache"] = serde_json::json!({"enabled": true,
                "url_env": "YASP_TEST_REDIS_URL", "namespace": namespace.name,
                "ttl_seconds": 60, "max_rows_per_query": 100, "max_buffered_bytes_total": 196608});
            },
            |fixture, identity, address, query| {
                let sql = "SELECT max(id), sum(amount) FROM analytics.orders";
                let first = query(sql);
                assert_success(&first, "cache fill");
                assert_success(&fixture.query("INSERT INTO analytics.orders (id, user_id, amount, ordered_at) VALUES (999, 1, 123.456789012, '2026-01-03')"), "source mutation after cache fill");
                let hit = query(sql);
                assert_success(&hit, "cache hit");
                assert_eq!(
                    first.stdout, hit.stdout,
                    "raw values preserved on cache hit"
                );
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
                        "BEGIN",
                        "-c",
                        sql,
                        "-c",
                        "ROLLBACK",
                    ])
                    .output()
                    .unwrap();
                assert_success(&transaction, "transaction bypasses cache");
                assert!(String::from_utf8_lossy(&transaction.stdout).contains("999|"));
                assert_eq!(
                    query(sql).stdout,
                    first.stdout,
                    "transaction did not overwrite cache"
                );
            },
        );
    }
}

#[cfg(feature = "client-tests")]
#[path = "support/clients.rs"]
mod clients;

#[cfg(feature = "client-tests")]
#[test]
fn actual_jdbc_and_sqlalchemy_drivers_accept_redshift_backed_frontends() {
    let directory = support::root().join("target/compatibility");
    std::fs::create_dir_all(&directory).unwrap();
    for profile in ["postgresql", "redshift"] {
        redshift_host::with_host(profile, |_, identity, address, _| {
            for sqlalchemy in [false, true] {
                let report =
                    clients::run_client(profile, identity, address, "public", "users", sqlalchemy);
                let name = if sqlalchemy { "sqlalchemy" } else { "jdbc" };
                std::fs::write(
                    directory.join(format!("redshift-{profile}-{name}.json")),
                    serde_json::to_vec_pretty(&report).unwrap(),
                )
                .unwrap();
                for observation in report["observations"]
                    .as_array()
                    .expect("client observations")
                {
                    assert_eq!(observation["passed"], true, "{profile}: {observation}");
                    if sqlalchemy && observation["operation"] == "columns" {
                        assert_eq!(observation["result"].as_array().unwrap().len(), 4);
                    }
                    if sqlalchemy && observation["operation"] == "composite_keys" {
                        let keys = observation["result"].as_array().unwrap();
                        assert_eq!(keys.len(), 1);
                        assert_eq!(keys[0]["constrained_columns"].as_array().unwrap().len(), 2);
                    }
                }
            }
            let report = clients::run_metabase_for(profile, identity, address, "public", "users");
            std::fs::write(
                directory.join(format!("redshift-{profile}-metabase.json")),
                serde_json::to_vec_pretty(&report).unwrap(),
            )
            .unwrap();
            assert_eq!(report["connected"], true, "{profile}: {report}");
            assert_eq!(report["native"]["value"], "1");
            if profile == "redshift" {
                let metadata = &report["metadata"];
                assert!(!metadata["foreign_keys"].as_array().unwrap().is_empty());
                assert_eq!(metadata["filtered_foreign_keys"], metadata["foreign_keys"]);
            }
            let generated = report["generated"]
                .as_array()
                .expect("actual Metabase generated rows");
            assert_eq!(generated.len(), 2);
            assert_eq!(generated[0]["amount"], "123.456789012");
        });
    }
}
