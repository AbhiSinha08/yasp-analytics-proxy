//! Compute-class routing, shared source capacity, overflow, and idle reclamation.
#![cfg(unix)]

mod support;

use std::{
    sync::Arc,
    thread,
    time::{Duration, Instant},
};
use support::{
    GatewayServer, PostgresConnection, RedisNamespace, TestDatabase, TestEnvironment,
    assert_success, quote_ident,
};
use yasp::{
    backend::{PostgresBackends, PostgresBinding},
    cache::RedisCache,
    config::{BackendSettings, CacheSettings, GatewaySettings},
    hooks::{BackendDecision, BackendSelection, bind_cache_eligibility, bind_select_backend},
};

fn text(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

fn wait_running(source: &PostgresConnection, sql: &str) {
    let started = Instant::now();
    loop {
        let probe = source.query(&format!(
            "SELECT count(*) FROM pg_stat_activity WHERE query = '{}' AND state = 'active'",
            sql.replace('\'', "''")
        ));
        if text(&probe) == "1" {
            return;
        }
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "query reached its source: {sql}"
        );
        thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn compute_types_overflow_share_capacity_and_reclaim_idle_connections() {
    let environment = TestEnvironment::load();
    let source = environment.source(None);
    let reader = environment.source(Some("secondary"));
    let identity = environment.gateway();
    let databases = [
        TestDatabase::create(&source, "yasp_compute"),
        TestDatabase::create(&source, "yasp_compute"),
        TestDatabase::create(&source, "yasp_compute"),
    ];
    for database in &databases {
        database.populate();
        let name = quote_ident(&database.connection.identity.database);
        let user = quote_ident(&reader.identity.user);
        database.connection.execute(&format!(
            "GRANT CONNECT ON DATABASE {name} TO {user}; GRANT USAGE ON SCHEMA public TO {user}; GRANT SELECT ON ALL TABLES IN SCHEMA public TO {user}"
        ));
        database.connection.execute("CREATE TABLE public.compute_private (secret text); INSERT INTO public.compute_private VALUES ('private')");
    }
    let route = |login: &str| BackendSelection {
        target: "analytics".into(),
        backend_login: login.into(),
    };
    let mut bindings = Vec::new();
    for login in ["owner", "reader"] {
        for (index, compute) in ["small_a", "small_b", "large_a"].into_iter().enumerate() {
            let connection = if login == "owner" {
                databases[index].connection.clone()
            } else {
                reader.database(&databases[index].connection.identity.database)
            };
            bindings.push(PostgresBinding {
                selection: route(login),
                compute_type: Some(if index < 2 { "small" } else { "large" }.into()),
                compute: compute.into(),
                default_compute_type: index < 2,
                max_connections: 1,
                max_in_flight: 1,
                config: connection.backend_config(),
            });
        }
    }
    let backends = Arc::new(
        PostgresBackends::with_bindings(
            bindings,
            BackendSettings {
                max_connections_per_pool: 2,
                max_connections_total: 2,
            },
        )
        .unwrap(),
    );
    let redis_url =
        std::env::var("YASP_TEST_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".into());
    let namespace = RedisNamespace::new(&redis_url, "yasp_compute_cache");
    let cache = Arc::new(
        RedisCache::new(
            CacheSettings {
                enabled: true,
                namespace: namespace.name.clone(),
                ttl_seconds: 60,
                ..CacheSettings::default()
            },
            &redis_url,
            Some(
                bind_cache_eligibility((), |request, _| Ok(!request.query.sql().contains("held_")))
                    .into(),
            ),
        )
        .unwrap(),
    );
    let gateway = identity
        .gateway_config()
        .with_schema_discovery(route("owner"))
        .with_schema_discovery_compute("small_a".into())
        .with_cache(cache);
    let server = GatewayServer::routed(
        gateway,
        backends,
        bind_select_backend((), |request, _| {
            Ok(BackendDecision {
                target: None,
                backend_login: Some(
                    if request.query.sql().contains("limited") {
                        "reader"
                    } else {
                        "owner"
                    }
                    .into(),
                ),
                compute_type: if request.query.sql().contains("large_compute") {
                    Some("large".into())
                } else if request.query.sql().contains("invalid_compute") {
                    Some("unknown".into())
                } else {
                    None
                },
            })
        }),
    );
    let client = identity.connection(server.address);
    let ordinary = "SELECT 'ordinary', current_database(), current_user";
    let initial = client.query(ordinary);
    assert_success(&initial, "default small source");
    assert!(text(&initial).contains(&databases[0].connection.identity.database));
    let cache_sql = "SELECT clock_timestamp() /* cache while occupied */";
    let cached = client.query(cache_sql);
    assert_success(&cached, "seed source cache");

    thread::scope(|scope| {
        let sql = "SELECT pg_sleep(2) /* held_small */";
        let held = scope.spawn(|| client.query(sql));
        wait_running(&source, sql);
        let before = Instant::now();
        let hit = client.query(cache_sql);
        assert_success(&hit, "cache replay while preferred compute is occupied");
        assert_eq!(text(&hit), text(&cached));
        assert!(
            before.elapsed() < Duration::from_secs(1),
            "cache replay waited for compute capacity"
        );

        let overflow =
            client.query("SELECT 'ordinary', current_database(), current_user /* limited */");
        assert_success(&overflow, "another login overflows the same full compute");
        assert!(text(&overflow).contains(&databases[1].connection.identity.database));
        assert!(text(&overflow).contains(&reader.identity.user));

        let pinned = client.queries(&[
            "BEGIN",
            "SELECT pg_backend_pid(), current_database() /* limited */",
            "SELECT pg_backend_pid(), current_database() /* limited */",
            "COMMIT",
        ]);
        assert_success(&pinned, "transaction retains its actual overflow instance");
        let output = text(&pinned);
        let rows: Vec<_> = output.lines().filter(|line| line.contains('|')).collect();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], rows[1]);
        assert!(rows[0].contains(&databases[1].connection.identity.database));

        let conflict = client.queries(&[
            "BEGIN",
            "SELECT pg_backend_pid(), current_database() /* limited */",
            "SELECT 'escaped', current_database() || ':' || current_user /* limited large_compute */",
            "SELECT 'still_failed' /* limited */",
            "ROLLBACK",
            "SELECT 'after_conflict' /* limited */",
        ]);
        let errors = String::from_utf8_lossy(&conflict.stderr);
        assert!(
            errors.contains("25000"),
            "compute change must fail: {errors}"
        );
        assert!(
            errors.contains("25P02"),
            "transaction must stay failed: {errors}"
        );
        let output = text(&conflict);
        assert!(!output.contains("escaped"));
        assert!(!output.contains("still_failed"));
        assert!(!output.contains(&databases[2].connection.identity.database));
        assert!(output.contains("after_conflict"));

        let large =
            client.query("SELECT 'ordinary', current_database(), current_user /* large_compute */");
        assert_success(&large, "large class reclaims idle source capacity");
        assert!(text(&large).contains(&databases[2].connection.identity.database));
        let names = databases
            .iter()
            .map(|db| format!("'{}'", db.connection.identity.database))
            .collect::<Vec<_>>()
            .join(",");
        let open = source.query(&format!(
            "SELECT count(*) FROM pg_stat_activity WHERE datname IN ({names})"
        ));
        assert_success(&open, "count physical source connections");
        assert!(
            text(&open).parse::<usize>().unwrap() <= 2,
            "shared physical cap was exceeded"
        );
        assert_success(&held.join().unwrap(), "held small query");
    });

    thread::scope(|scope| {
        let first_sql = "SELECT pg_sleep(3) /* held_first_instance */";
        let first = scope.spawn(|| client.query(first_sql));
        wait_running(&source, first_sql);
        let second_sql = "SELECT pg_sleep(0.5) /* held_second_instance */";
        let second = scope.spawn(|| client.query(second_sql));
        wait_running(&source, second_sql);
        let started = Instant::now();
        let resumed = client.query("SELECT 'after_wait', current_database(), current_user");
        assert_success(
            &resumed,
            "waiting query uses whichever eligible instance frees first",
        );
        assert!(text(&resumed).contains(&databases[1].connection.identity.database));
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "query waited only for the first configured instance"
        );
        assert_success(&second.join().unwrap(), "second held instance");
        assert_success(&first.join().unwrap(), "first held instance");
    });

    let denied = client.queries(&[
        "SELECT secret FROM public.compute_private /* limited */",
        "SELECT 'ordinary', current_database(), current_user /* limited */",
    ]);
    assert!(
        String::from_utf8_lossy(&denied.stderr).contains("42501"),
        "source permission denial was hidden"
    );
    assert!(
        text(&denied).contains(&reader.identity.user),
        "client did not recover with its selected source role"
    );
    let invalid = client.queries(&[
        "SELECT 1 /* invalid_compute */",
        "SELECT 'ordinary', current_database(), current_user",
    ]);
    assert!(String::from_utf8_lossy(&invalid.stderr).contains("42501"));
    assert!(text(&invalid).contains(&databases[0].connection.identity.database));
    server.stop();
}

#[test]
fn saturated_shared_budget_waits_then_reports_capacity_and_recovers() {
    let environment = TestEnvironment::load();
    let source = environment.source(None);
    let identity = environment.gateway();
    let database = TestDatabase::create(&source, "yasp_budget");
    let reader = environment
        .source(Some("secondary"))
        .database(&database.connection.identity.database);
    database.connection.execute(&format!(
        "GRANT CONNECT ON DATABASE {} TO {}",
        quote_ident(&reader.identity.database),
        quote_ident(&reader.identity.user)
    ));
    let bindings = [
        ("owner", database.connection.clone()),
        ("reader", reader.clone()),
    ]
    .into_iter()
    .map(|(login, connection)| PostgresBinding {
        selection: BackendSelection {
            target: "only".into(),
            backend_login: login.into(),
        },
        compute_type: None,
        compute: "default".into(),
        default_compute_type: true,
        max_connections: 1,
        max_in_flight: 2,
        config: connection.backend_config(),
    })
    .collect();
    let backends = Arc::new(
        PostgresBackends::with_bindings(
            bindings,
            BackendSettings {
                max_connections_per_pool: 1,
                max_connections_total: 1,
            },
        )
        .unwrap(),
    );
    let server = GatewayServer::routed(
        identity
            .gateway_config()
            .with_settings(GatewaySettings {
                queue_timeout_ms: 10_000,
                ..GatewaySettings::default()
            })
            .unwrap()
            .with_schema_discovery(BackendSelection {
                target: "only".into(),
                backend_login: "owner".into(),
            }),
        backends,
        bind_select_backend((), |request, _| {
            Ok(BackendDecision {
                backend_login: Some(
                    if request.query.sql().contains("limited") {
                        "reader"
                    } else {
                        "owner"
                    }
                    .into(),
                ),
                ..BackendDecision::default()
            })
        }),
    );
    let client = identity.connection(server.address);
    thread::scope(|scope| {
        let sql = "SELECT pg_sleep(12) /* exhaust_shared_budget */";
        let held = scope.spawn(|| client.query(sql));
        wait_running(&source, sql);
        let before = Instant::now();
        let waiting = client.queries(&[
            "SELECT current_user /* limited */",
            "SELECT current_user /* limited */",
        ]);
        assert!(
            String::from_utf8_lossy(&waiting.stderr).contains("53300"),
            "saturation must report a bounded capacity error: {}",
            String::from_utf8_lossy(&waiting.stderr)
        );
        assert!(
            before.elapsed() >= Duration::from_secs(9),
            "capacity did not wait asynchronously under its deadline"
        );
        assert!(
            text(&waiting).contains(&reader.identity.user),
            "same frontend session did not recover after capacity became available"
        );
        assert_success(
            &held.join().unwrap(),
            "active query survives pressure reclamation",
        );
    });
    server.stop();
}

#[test]
fn configured_host_resolves_partial_decisions_and_credential_overrides() {
    use std::{
        fs,
        net::TcpListener,
        process::{Command, Stdio},
    };
    use support::{ChildGuard, TempDirectory};
    let environment = TestEnvironment::load();
    let source = environment.source(None);
    let reader = environment.source(Some("secondary"));
    let identity = environment.gateway();
    let database = TestDatabase::create(&source, "yasp_compute_host");
    database.connection.execute(&format!(
        "GRANT CONNECT ON DATABASE {} TO {}",
        quote_ident(&database.connection.identity.database),
        quote_ident(&reader.identity.user)
    ));
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let temporary = TempDirectory::new("yasp_compute_host");
    let path = temporary.0.join("config.json");
    let config = serde_json::json!({
        "version": 1,
        "gateway": {"listen": address.to_string()},
        "backend": {"max_connections_per_pool": 2, "max_connections_total": 1},
        "targets": {"analytics": {
            "engine": "postgresql", "database": database.connection.identity.database,
            "tls": {"mode": "disable"},
            "logins": {
                "owner": {"username": source.identity.user, "password_env": "YASP_COMPUTE_TEST_OWNER_PASSWORD"},
                "reader": {
                    "username": source.identity.user, "password_env": "YASP_COMPUTE_TEST_OWNER_PASSWORD",
                    "allowed_computes": ["large_a"],
                    "overrides": {"large_a": {"username": reader.identity.user, "password_env": "YASP_COMPUTE_TEST_READER_PASSWORD"}}
                }
            },
            "compute": {"default_type": "small", "types": {
                "small": {"max_in_flight": 1, "max_connections": 1, "instances": [{"id": "small_a", "host": source.host, "port": source.port}]},
                "large": {"max_in_flight": 1, "max_connections": 1, "instances": [{"id": "large_a", "host": source.host, "port": source.port}]}
            }}
        }},
        "schema_discovery": {"backend_login": "owner", "compute": "small_a", "catalog": {"schemas": ["public"]}},
        "hooks": {"select_backend": {
            "type": "rust", "callback": "builtin.select_backend.passthrough",
            "params": {"backend_login": "reader", "compute_type": "large"}
        }}
    });
    fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
    let mut process = ChildGuard::new(
        Command::new(env!("CARGO_BIN_EXE_yasp"))
            .args(["--config", path.to_str().unwrap()])
            .env(
                "YASP_COMPUTE_TEST_OWNER_PASSWORD",
                &source.identity.password,
            )
            .env(
                "YASP_COMPUTE_TEST_READER_PASSWORD",
                &reader.identity.password,
            )
            .env("YASP_GATEWAY_USERNAME", &identity.user)
            .env("YASP_GATEWAY_PASSWORD", &identity.password)
            .env("YASP_GATEWAY_DATABASE", &identity.database)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let started = Instant::now();
    loop {
        assert!(
            process.child().try_wait().unwrap().is_none(),
            "configured host exited before serving"
        );
        let result = identity.connection(address).query("SELECT current_user");
        if result.status.success() {
            assert_eq!(
                text(&result),
                reader.identity.user,
                "host did not use the large compute's credential override"
            );
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "configured host did not become ready"
        );
        thread::sleep(Duration::from_millis(20));
    }
}
