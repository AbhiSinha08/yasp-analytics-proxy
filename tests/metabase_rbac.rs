//! The three local configurations through the configured host and real pgwire.
#![cfg(all(unix, feature = "python"))]
#[path = "support/decision_model.rs"]
mod mock;
mod support;

use serde_json::{Value, json};
use std::{
    fs,
    net::TcpListener,
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};
use support::{ChildGuard, RedisNamespace, TempDirectory, TestDatabase, TestEnvironment};

#[test]
fn metabase_roles_and_email_masks_use_current_accounts_on_raw_cache_hits() {
    let environment = TestEnvironment::load();
    let owner = environment.source(None);
    let limited = environment.source(Some("secondary"));
    let database = TestDatabase::create(&owner, "yasp_metabase_rbac");
    database.populate();
    database.connection.execute(&format!(
        "GRANT USAGE ON SCHEMA public TO {reader}; GRANT SELECT ON products, orders TO {reader}",
        reader = support::quote_ident(&limited.identity.user)
    ));
    let identity = environment.gateway();
    let redis = std::env::var("YASP_TEST_REDIS_URL").unwrap();
    let tls = std::path::PathBuf::from(std::env::var_os("YASP_TEST_TLS_DIR").unwrap());
    let model = mock::MockModel::new();
    for ordinal in 0..6 {
        model.response_by_ordinal.lock().unwrap().insert(
            ordinal,
            json!({"answers":{"email":{"type":"noul", "noul":if (1..=3).contains(&ordinal) {0.99} else {0.01}, "confidence":0.99}}}).to_string(),
        );
    }
    let sql = "SELECT u.id, CASE WHEN u.id = 2 THEN NULL ELSE u.email END AS contact, o.buyer_email AS contact, t.requester_email AS requester, u.phone, p.price FROM public.users u JOIN public.orders o ON o.id = u.id JOIN public.support_tickets t ON t.order_id = o.id JOIN public.products p ON p.id = o.product_id WHERE u.id <= 2 ORDER BY u.id";
    let original = "1|avery.brooks@example.test|avery.brooks@example.test|avery.brooks@example.test|+1-202-555-0101|64.00\n2||jordan.kim@example.test|jordan.kim@example.test|+1-202-555-0102|24.50\n";
    let masked = "1|XXXX|XXXX|XXXX|+1-202-555-0101|64.00\n2||XXXX|XXXX|+1-202-555-0102|24.50\n";
    for (file, callback) in [
        ("local.yml", None),
        ("local-ast.yml", Some("mask_email_from_ast")),
        ("local-model.yml", Some("mask_email_from_model")),
    ] {
        let namespace = RedisNamespace::new(&redis, "yasp_metabase_rbac");
        let temporary = TempDirectory::new("yasp_metabase_rbac");
        let mut config: Value = serde_saphyr::from_str(
            &fs::read_to_string(support::root().join("config").join(file)).unwrap(),
        )
        .unwrap();
        assert_eq!(
            config["hooks"]["query_context"],
            json!({
                "type": "rust", "callback": "builtin.query_context.metabase",
            })
        );
        match callback {
            None => {
                assert!(config.get("data_contexts").is_none());
                assert!(config["hooks"].get("process_result").is_none());
                assert_eq!(
                    config["hooks"]["select_backend"]["params"]["login_by_account"]["limited"],
                    "analyst"
                );
            }
            Some(name) => {
                assert_eq!(config["hooks"]["process_result"]["callback"], name);
                if config["hooks"].get("select_backend").is_some() {
                    assert_eq!(
                        config["hooks"]["select_backend"]["params"]["login_by_account"]["limited"],
                        "restricted"
                    );
                } else {
                    assert_eq!(
                        config["targets"]["primary"]["logins"]
                            .as_object()
                            .unwrap()
                            .len(),
                        1
                    );
                }
                assert_eq!(
                    config["data_contexts"].get("ast").is_some(),
                    name.ends_with("ast")
                );
                assert_eq!(
                    config["application_contexts"]["datasets"]
                        .get("email_columns")
                        .is_some(),
                    name.ends_with("ast")
                );
                assert_eq!(
                    config["data_contexts"].get("decision_model").is_some(),
                    name.ends_with("model")
                );
            }
        }
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        config["gateway"]["listen"] = address.to_string().into();
        config["gateway"]["tls"]["cert_file"] = json!(tls.join("server.crt"));
        config["gateway"]["tls"]["key_file"] = json!(tls.join("server.key"));
        config["targets"]["primary"]["host"] = owner.host.clone().into();
        config["targets"]["primary"]["port"] = owner.port.into();
        config["targets"]["primary"]["database"] =
            database.connection.identity.database.clone().into();
        config["targets"]["primary"]["logins"]["restricted"]["username"] =
            owner.identity.user.clone().into();
        if let Some(login) = config["targets"]["primary"]["logins"].get_mut("analyst") {
            login["username"] = limited.identity.user.clone().into();
        }
        config["cache"]["namespace"] = namespace.name.clone().into();
        config["application_contexts"]["namespace"] = format!("{}:datasets", namespace.name).into();
        for (name, dataset) in config["application_contexts"]["datasets"]
            .as_object_mut()
            .unwrap()
        {
            let path = support::root().join(dataset["loader"]["params"]["file"].as_str().unwrap());
            let path = if name == "email_columns" {
                let mapping = fs::read_to_string(path).unwrap().replace(
                    "\\\"development\\\"",
                    &format!("\\\"{}\\\"", database.connection.identity.database),
                );
                let path = temporary.0.join("email_columns.json");
                fs::write(&path, mapping).unwrap();
                path
            } else {
                path
            };
            dataset["loader"]["params"]["file"] = json!(path);
        }
        for hook in config["hooks"].as_object_mut().unwrap().values_mut() {
            if let Some(path) = hook.get_mut("path") {
                *path = json!(support::root().join("config").join(path.as_str().unwrap()));
            }
        }
        if config["data_contexts"].get("decision_model").is_some() {
            config["data_contexts"]["decision_model"]["endpoint"] = model.endpoint.clone().into();
        }
        // The real parser validates the copied configuration before host startup.
        serde_json::from_value::<yasp::config::FileConfig>(config.clone())
            .unwrap()
            .validate()
            .unwrap();
        let path = temporary.0.join("config.json");
        fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
        let mut process = ChildGuard::new(
            Command::new(env!("CARGO_BIN_EXE_yasp"))
                .args(["--config", path.to_str().unwrap()])
                .env("YASP_REDIS_URL", &redis)
                .env("YASP_PRIMARY_RESTRICTED_PASSWORD", &owner.identity.password)
                .env("YASP_TEST_SECOND_PASSWORD", &limited.identity.password)
                .env("YASP_GATEWAY_USERNAME", &identity.user)
                .env("YASP_GATEWAY_PASSWORD", &identity.password)
                .env("YASP_GATEWAY_DATABASE", &identity.database)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        );
        let query = |user: Option<u32>, sql: &str| {
            let sql = match user {
                Some(user) => {
                    format!(
                        "-- /* partner: \"metabase\", {{\"dashboard_id\":null,\"chart_id\":null,\"optional_user_id\":1,\"optional_account_id\":\"demo\",\"filter_values\":{{}}}} */ Metabase:: userID: {user} queryType: native queryHash: rbac\n{sql}"
                    )
                }
                None => sql.to_owned(),
            };
            identity
                .connection(address)
                .command()
                .env("PGSSLMODE", "verify-full")
                .env("PGSSLROOTCERT", tls.join("ca.crt"))
                .args([
                    "-v",
                    "ON_ERROR_STOP=1",
                    "-v",
                    "VERBOSITY=verbose",
                    "-c",
                    &sql,
                ])
                .output()
                .unwrap()
        };
        let started = Instant::now();
        loop {
            assert!(
                process.child().try_wait().unwrap().is_none(),
                "{file} host exited"
            );
            if query(Some(1), "SELECT 1 AS ready").status.success() {
                break;
            }
            assert!(
                started.elapsed() < Duration::from_secs(10),
                "{file} host did not become ready"
            );
            thread::sleep(Duration::from_millis(20));
        }
        let admin = query(Some(1), sql);
        support::assert_success(&admin, "admin reads original source rows");
        assert_eq!(String::from_utf8(admin.stdout).unwrap(), original);
        let calls = model.count();
        if callback == Some("mask_email_from_model") {
            assert!(
                model
                    .requests
                    .lock()
                    .unwrap()
                    .iter()
                    .all(|request| request["state"].get("sql").is_none())
            );
        }
        let result = query(Some(2), sql);
        if callback.is_some() {
            support::assert_success(&result, "limited user masks cached raw rows");
            assert_eq!(String::from_utf8(result.stdout).unwrap(), masked);
            assert_eq!(model.count(), calls, "cache replay reuses classifications");
            let postgres_remark =
                format!("-- Metabase:: userID: 2 queryType: native queryHash: pg\n{sql}");
            let result = query(None, &postgres_remark);
            support::assert_success(
                &result,
                "unified Metabase provider accepts PostgreSQL remarks",
            );
            assert_eq!(String::from_utf8(result.stdout).unwrap(), masked);
            assert_eq!(
                String::from_utf8(query(Some(1), sql).stdout).unwrap(),
                original
            );
            if callback == Some("mask_email_from_model") {
                // Observed Laya probability for email embedded alongside a name.
                model.response_by_ordinal.lock().unwrap().insert(
                    0,
                    json!({"answers":{"email":{"type":"noul","noul":0.5841,"confidence":0.5841}}})
                        .to_string(),
                );
            }
            let combined =
                "SELECT email || ' — ' || full_name AS customer_identity FROM users WHERE id = 1";
            let result = query(Some(2), combined);
            support::assert_success(&result, "an email embedded in text masks the entire field");
            assert_eq!(String::from_utf8(result.stdout).unwrap(), "XXXX\n");
            let result = query(Some(1), combined);
            support::assert_success(&result, "admin still reads the combined identity");
            assert_eq!(
                String::from_utf8(result.stdout).unwrap(),
                "avery.brooks@example.test — Avery Brooks\n"
            );
        } else {
            assert!(!result.status.success());
            assert!(String::from_utf8_lossy(&result.stderr).contains("42501"));
            support::assert_success(
                &query(Some(2), "SELECT name FROM products"),
                "limited source grants allow products",
            );
            for table in ["users", "support_tickets"] {
                assert!(
                    !query(Some(2), &format!("SELECT * FROM {table}"))
                        .status
                        .success()
                );
            }
        }
        for user in [None, Some(999)] {
            let result = query(user, sql);
            assert!(!result.status.success());
            let expected = if config["hooks"].get("select_backend").is_some() {
                "42501"
            } else {
                "38000"
            };
            assert!(String::from_utf8_lossy(&result.stderr).contains(expected));
        }
        if callback == Some("mask_email_from_model") {
            model
                .response_by_ordinal
                .lock()
                .unwrap()
                .insert(0, "{}".into());
            let result = query(
                Some(2),
                "SELECT email AS unavailable FROM users WHERE id = 1",
            );
            assert!(!result.status.success());
            assert!(String::from_utf8_lossy(&result.stderr).contains("38000"));
            assert!(!String::from_utf8_lossy(&result.stdout).contains("example.test"));
            support::assert_success(
                &query(
                    Some(1),
                    "SELECT email AS unavailable FROM users WHERE id = 1",
                ),
                "admin does not require model availability",
            );
            model.response_by_ordinal.lock().unwrap().insert(
                0,
                json!({"answers":{"email":{"type":"noul","noul":0.1,"confidence":0.1}}})
                    .to_string(),
            );
            let result = query(Some(2), "SELECT email AS uncertain FROM users WHERE id = 1");
            assert!(!result.status.success());
            assert!(String::from_utf8_lossy(&result.stderr).contains("38000"));
            assert!(!String::from_utf8_lossy(&result.stdout).contains("example.test"));
            support::assert_success(&query(Some(2), sql), "subsequent valid cache hit recovers");
        }
        assert!(
            Command::new("kill")
                .args(["-TERM", &process.id().to_string()])
                .status()
                .unwrap()
                .success()
        );
        let started = Instant::now();
        loop {
            if let Some(status) = process.child().try_wait().unwrap() {
                assert!(status.success());
                break;
            }
            assert!(started.elapsed() < Duration::from_secs(5));
            thread::sleep(Duration::from_millis(20));
        }
    }
}
