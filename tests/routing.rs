//! Exercise routed library serving with isolated PostgreSQL databases.

#[cfg(unix)]
#[test]
fn routed_gateway_contract() {
    use std::{
        collections::BTreeMap,
        path::Path,
        process::{Command, Output},
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
            mpsc,
        },
        thread,
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    };
    use tokio::{net::TcpListener, runtime::Builder, sync::oneshot};
    use yasp::{
        backend::PostgresBackends,
        config::{BackendConfig, BackendSettings, FileConfig, GatewayConfig},
        gateway::serve_routed,
        hooks::{BackendSelection, SelectionError, bind_select_backend},
    };

    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut values = std::collections::HashMap::new();
    for item in dotenvy::from_path_iter(root.join(".env"))
        .into_iter()
        .flatten()
    {
        let (key, value) = item.expect("test dotenv must be valid");
        values.entry(key).or_insert(value);
    }
    let value = |key: &str| {
        std::env::var(key)
            .ok()
            .or_else(|| values.get(key).cloned())
            .unwrap_or_else(|| panic!("required test environment value is missing: {key}"))
    };
    let gateway_user = value("YASP_GATEWAY_USERNAME");
    let gateway_password = value("YASP_GATEWAY_PASSWORD");
    let gateway_database = value("YASP_GATEWAY_DATABASE");
    let test_config_path = std::env::var_os("YASP_TEST_CONFIG")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| root.join("tests/config.yml"));
    let test_config =
        FileConfig::read(&test_config_path).expect("test backend configuration must be valid");
    let default = test_config
        .default_backend()
        .expect("test default must be configured");
    let source_target = &test_config.targets[&default.target];
    let source_login = &source_target.logins[&default.backend_login];
    let source_user = source_login.username.clone();
    let source_password = value(&source_login.password_env);
    let source_host = source_target.host.clone();
    let source_port = source_target.port;
    let second_login = source_target
        .logins
        .get("secondary")
        .expect("test YAML must configure the secondary source login");
    let second_user = second_login.username.clone();
    let second_password = value(&second_login.password_env);
    assert_ne!(
        second_user, source_user,
        "secondary must name a different pre-provisioned PostgreSQL login"
    );

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after Unix epoch")
        .as_nanos();
    let database_names = [
        format!("yasp_route_{}_{}", std::process::id(), timestamp),
        format!("yasp_route_{}_{}_b", std::process::id(), timestamp),
    ];
    let cleanup = DatabaseCleanup {
        names: database_names.clone(),
        user: source_user.clone(),
        password: source_password.clone(),
        host: source_host.clone(),
        port: source_port,
    };
    let maintenance = |sql: &str, database: &str| {
        let output = psql(
            sql,
            &source_user,
            database,
            &source_password,
            &source_host,
            source_port,
        );
        assert!(
            output.status.success(),
            "PostgreSQL fixture operation failed"
        );
        output
    };
    for database in &database_names {
        maintenance(
            &format!("CREATE DATABASE \"{database}\" TEMPLATE template0"),
            "postgres",
        );
        let fixture = psql_file(
            root.join("tests/fixtures/read_forwarding.sql"),
            &source_user,
            database,
            &source_password,
            &source_host,
            source_port,
        );
        assert!(
            fixture.status.success(),
            "cannot populate a disposable test database"
        );
    }
    let second_id = quote_ident(&second_user);
    maintenance(
        &format!(
            "REVOKE CONNECT ON DATABASE \"{}\" FROM PUBLIC; \
             REVOKE CONNECT ON DATABASE \"{}\" FROM {second_id}",
            database_names[0], database_names[1]
        ),
        "postgres",
    );
    let grants = psql(
        &format!(
            "GRANT CONNECT ON DATABASE \"{}\" TO {second_id}; \
             GRANT USAGE ON SCHEMA public TO {second_id}; \
             GRANT SELECT ON ALL TABLES IN SCHEMA public TO {second_id}",
            database_names[1]
        ),
        &source_user,
        &database_names[1],
        &source_password,
        &source_host,
        source_port,
    );
    assert!(
        grants.status.success(),
        "cannot grant fixture read access to the second source login"
    );
    let role = psql(
        "SELECT rolsuper FROM pg_roles WHERE rolname = current_user",
        &second_user,
        &database_names[1],
        &second_password,
        &source_host,
        source_port,
    );
    assert!(
        role.status.success() && String::from_utf8_lossy(&role.stdout).trim() == "f",
        "the second source login must be a non-superuser to verify database ACLs"
    );
    let private_table = psql(
        "CREATE TABLE public.yasp_private_probe (secret text); INSERT INTO public.yasp_private_probe VALUES ('owner-only')",
        &source_user,
        &database_names[1],
        &source_password,
        &source_host,
        source_port,
    );
    assert!(
        private_table.status.success(),
        "cannot create the private source ACL probe table"
    );

    let route_one = BackendSelection {
        target: "target_one".into(),
        backend_login: "reader".into(),
    };
    let route_two = BackendSelection {
        target: "target_two".into(),
        backend_login: "reader".into(),
    };
    let route_owner = BackendSelection {
        target: "target_two".into(),
        backend_login: "owner".into(),
    };
    let unreachable = BackendSelection {
        target: "unreachable".into(),
        backend_login: "reader".into(),
    };
    let backend_config = |username: String, password: String, database: String, port| {
        BackendConfig::new(source_host.clone(), port, username, password, database)
            .expect("test backend configuration is valid")
    };
    let failure_user = source_user.clone();
    let routes = BTreeMap::from([
        (
            route_one.clone(),
            backend_config(
                source_user.clone(),
                source_password.clone(),
                database_names[0].clone(),
                source_port,
            ),
        ),
        (
            route_two.clone(),
            backend_config(
                second_user.clone(),
                second_password.clone(),
                database_names[1].clone(),
                source_port,
            ),
        ),
        (
            route_owner.clone(),
            backend_config(
                source_user.clone(),
                source_password.clone(),
                database_names[1].clone(),
                source_port,
            ),
        ),
        (
            unreachable.clone(),
            backend_config(failure_user, "unused".into(), "postgres".into(), 1),
        ),
    ]);
    let backends = Arc::new(
        PostgresBackends::new(
            routes,
            BackendSettings {
                max_connections_per_pool: 1,
                max_connections_total: 4,
            },
        )
        .expect("route pools must fit the configured bounds"),
    );
    let callback_count = Arc::new(AtomicUsize::new(0));
    let callback_identity_ok = Arc::new(AtomicBool::new(true));
    let selector =
        bind_select_backend(
            (
                callback_count.clone(),
                callback_identity_ok.clone(),
                gateway_user.clone(),
                route_one.clone(),
                route_two.clone(),
                route_owner.clone(),
                unreachable.clone(),
            ),
            |request,
             (
                count,
                identity_ok,
                expected_user,
                route_one,
                route_two,
                route_owner,
                unreachable,
            )| {
                count.fetch_add(1, Ordering::SeqCst);
                if request.authenticated_user != expected_user {
                    identity_ok.store(false, Ordering::SeqCst);
                }
                let sql = request.query.sql().to_ascii_lowercase();
                if sql.contains("route_denied") {
                    Err(SelectionError::Denied)
                } else if sql.contains("route_failed") {
                    Err(SelectionError::Failed)
                } else if sql.contains("route_unknown") {
                    Ok(BackendSelection {
                        target: "not_configured".into(),
                        backend_login: "reader".into(),
                    })
                } else if sql.contains("route_backend_failure") {
                    Ok(unreachable.clone())
                } else if sql.contains("route_two") {
                    Ok(route_two.clone())
                } else if sql.contains("route_owner") {
                    Ok(route_owner.clone())
                } else {
                    Ok(route_one.clone())
                }
            },
        );

    let gateway_password_for_client = gateway_password.clone();
    let gateway = GatewayConfig::new(
        gateway_user.clone(),
        gateway_password,
        gateway_database.clone(),
    )
    .expect("gateway identity is valid");
    let (startup_tx, startup_rx) = mpsc::channel();
    let server = thread::spawn(move || {
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime starts");
        runtime.block_on(async move {
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("ephemeral loopback listener binds");
            let address = listener.local_addr().expect("listener address exists");
            let (shutdown_tx, shutdown_rx) = oneshot::channel();
            startup_tx
                .send((address, shutdown_tx))
                .expect("test receives the bound listener");
            let result = serve_routed(listener, gateway, backends, selector, async move {
                let _ = shutdown_rx.await;
            })
            .await;
            result.expect("routed gateway shuts down cleanly");
        });
    });
    let (address, shutdown) = startup_rx.recv().expect("routed server starts");
    let host = address.ip().to_string();
    let port = address.port();
    let client = |sql: &[&str], user: &str, password: &str| {
        let mut command = Command::new("psql");
        command
            .args(["-X", "-w", "-At", "-v", "VERBOSITY=verbose"])
            .env("PGPASSFILE", "/dev/null")
            .env("PGSSLMODE", "disable")
            .env("PGCONNECT_TIMEOUT", "3")
            .env("PGHOST", &host)
            .env("PGPORT", port.to_string())
            .env("PGUSER", user)
            .env("PGPASSWORD", password)
            .env("PGDATABASE", &gateway_database);
        for query in sql {
            command.args(["-c", query]);
        }
        command.output().expect("psql and libpq are installed")
    };
    let check = |queries: &[&str], output: &Output| {
        assert!(output.status.success(), "routed query sequence failed");
        for query in queries {
            assert!(
                String::from_utf8_lossy(&output.stdout).contains(query),
                "routed result is missing an expected marker"
            );
        }
    };

    let route_one_marker = format!("{}:{source_user}", database_names[0]);
    let result = client(
        &["SELECT current_database() || ':' || current_user"],
        &gateway_user,
        &gateway_password_for_client,
    );
    check(&[&route_one_marker], &result);
    let route_two_marker = format!("{}:{second_user}", database_names[1]);
    let result = client(
        &[
            "SELECT 'route_two' AS route_two, current_database() || ':' || current_user",
            "SELECT current_database() || ':' || current_user",
        ],
        &gateway_user,
        &gateway_password_for_client,
    );
    check(&[&route_two_marker, &route_one_marker], &result);

    let granted = client(
        &["SELECT count(*) FROM public.users /* route_two */"],
        &gateway_user,
        &gateway_password_for_client,
    );
    assert!(
        granted.status.success(),
        "secondary login cannot read granted fixture data"
    );
    assert_eq!(String::from_utf8_lossy(&granted.stdout).trim(), "10");

    let route_errors = [
        ("SELECT 'route_denied'", "42501", "backend selection denied"),
        ("SELECT 'route_failed'", "XX000", "backend selection failed"),
        (
            "SELECT 'route_unknown'",
            "42501",
            "backend selection is not configured",
        ),
        (
            "SELECT 'route_backend_failure'",
            "08006",
            "PostgreSQL connection unavailable",
        ),
    ];
    for (query, code, message) in route_errors {
        let result = client(
            &[query, "SELECT current_database() || ':' || current_user"],
            &gateway_user,
            &gateway_password_for_client,
        );
        let stderr = String::from_utf8_lossy(&result.stderr);
        assert!(
            stderr.contains(code),
            "routed failure did not return its SQLSTATE"
        );
        assert!(
            stderr.contains(message),
            "routed failure message was not preserved for {query}: {stderr}"
        );
        assert!(
            String::from_utf8_lossy(&result.stdout).contains(&route_one_marker),
            "the same authenticated session did not recover after a routing error"
        );
    }

    let pids = client(
        &[
            "SELECT pg_backend_pid()",
            "SELECT 'route_two' AS route_two, pg_backend_pid()",
            "SELECT 'route_owner' AS route_owner, pg_backend_pid()",
            "SELECT pg_backend_pid()",
        ],
        &gateway_user,
        &gateway_password_for_client,
    );
    assert!(pids.status.success(), "routed pool reuse query failed");
    let pids: Vec<_> = String::from_utf8_lossy(&pids.stdout)
        .lines()
        .map(|line| line.rsplit('|').next().unwrap_or(line).to_owned())
        .collect();
    assert_eq!(pids.len(), 4, "each routed query returned one backend PID");
    assert_eq!(
        pids[0], pids[3],
        "the first route reuses its own pool connection"
    );
    assert_ne!(
        pids[0], pids[1],
        "the two routes have isolated pool connections"
    );
    assert_ne!(
        pids[1], pids[2],
        "different login profiles on the same target have separate pools"
    );

    thread::scope(|scope| {
        let held = scope.spawn(|| {
            client(
                &["SELECT pg_sleep(12)"],
                &gateway_user,
                &gateway_password_for_client,
            )
        });
        let started = Instant::now();
        loop {
            let active = maintenance(
                &format!(
                    "SELECT count(*) FROM pg_stat_activity WHERE datname = '{}' AND query = 'SELECT pg_sleep(12)' AND state = 'active'",
                    database_names[0]
                ),
                "postgres",
            );
            if String::from_utf8_lossy(&active.stdout).trim() == "1" {
                break;
            }
            assert!(
                started.elapsed() < Duration::from_secs(3),
                "route query did not occupy its pool"
            );
            thread::sleep(Duration::from_millis(30));
        }
        let started = Instant::now();
        let other = client(
            &["SELECT 'route_two', current_database() || ':' || current_user"],
            &gateway_user,
            &gateway_password_for_client,
        );
        check(&[&route_two_marker], &other);
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "one saturated route blocked a different pool"
        );
        let started = Instant::now();
        let exhausted = client(
            &[
                "SELECT 1",
                "SELECT current_database() || ':' || current_user",
            ],
            &gateway_user,
            &gateway_password_for_client,
        );
        assert!(
            String::from_utf8_lossy(&exhausted.stderr).contains("53300"),
            "route capacity was not enforced"
        );
        assert!(started.elapsed() >= Duration::from_secs(9));
        assert!(
            String::from_utf8_lossy(&exhausted.stdout).contains(&route_one_marker),
            "session did not recover after pool exhaustion"
        );
        assert!(held.join().unwrap().status.success());
    });

    let owner = client(
        &["SELECT secret FROM public.yasp_private_probe /* route_owner */"],
        &gateway_user,
        &gateway_password_for_client,
    );
    check(&["owner-only"], &owner);
    let forbidden = psql(
        "SELECT 1",
        &second_user,
        &database_names[0],
        &second_password,
        &source_host,
        source_port,
    );
    assert!(
        !forbidden.status.success(),
        "second source login unexpectedly connects to the first source database"
    );
    let allowed = psql(
        "SELECT current_user",
        &second_user,
        &database_names[1],
        &second_password,
        &source_host,
        source_port,
    );
    assert!(
        allowed.status.success(),
        "second source login lacks its target database access"
    );
    let before = callback_count.load(Ordering::SeqCst);
    let private = client(
        &[
            "SELECT secret FROM public.yasp_private_probe /* route_two */",
            "SELECT current_database() || ':' || current_user",
        ],
        &gateway_user,
        &gateway_password_for_client,
    );
    let stderr = String::from_utf8_lossy(&private.stderr);
    assert!(
        stderr.contains("42501"),
        "the second source login unexpectedly read an owner-only table"
    );
    assert!(
        String::from_utf8_lossy(&private.stdout).contains(&route_one_marker),
        "frontend session did not recover after a source permission error"
    );
    assert_eq!(
        callback_count.load(Ordering::SeqCst),
        before + 2,
        "selector runs once for each parsed read query"
    );

    let before = callback_count.load(Ordering::SeqCst);
    let invalid = client(
        &[
            "  ",
            "DELETE FROM public.users",
            "SELECT current_database() || ':' || current_user",
        ],
        &gateway_user,
        &gateway_password_for_client,
    );
    assert!(
        String::from_utf8_lossy(&invalid.stderr).contains("0A000"),
        "writes are rejected before routing"
    );
    assert!(
        String::from_utf8_lossy(&invalid.stdout).contains(&route_one_marker),
        "session recovers after input validation"
    );
    assert_eq!(
        callback_count.load(Ordering::SeqCst),
        before + 1,
        "empty and rejected queries never reach the selector"
    );
    assert!(
        callback_identity_ok.load(Ordering::SeqCst),
        "selector receives the authenticated frontend identity"
    );

    let _ = shutdown.send(());
    server.join().expect("routed gateway task joins");
    drop(cleanup);
}

#[cfg(unix)]
fn psql(
    query: &str,
    user: &str,
    database: &str,
    password: &str,
    host: &str,
    port: u16,
) -> std::process::Output {
    std::process::Command::new("psql")
        .args(["-X", "-w", "-At", "-v", "ON_ERROR_STOP=1", "-c", query])
        .env("PGPASSFILE", "/dev/null")
        .env("PGSSLMODE", "disable")
        .env("PGCONNECT_TIMEOUT", "3")
        .env("PGHOST", host)
        .env("PGPORT", port.to_string())
        .env("PGUSER", user)
        .env("PGPASSWORD", password)
        .env("PGDATABASE", database)
        .output()
        .expect("psql and libpq are installed")
}

#[cfg(unix)]
fn psql_file(
    file: std::path::PathBuf,
    user: &str,
    database: &str,
    password: &str,
    host: &str,
    port: u16,
) -> std::process::Output {
    std::process::Command::new("psql")
        .args(["-X", "-w", "-v", "ON_ERROR_STOP=1", "-f"])
        .arg(file)
        .env("PGPASSFILE", "/dev/null")
        .env("PGSSLMODE", "disable")
        .env("PGCONNECT_TIMEOUT", "3")
        .env("PGHOST", host)
        .env("PGPORT", port.to_string())
        .env("PGUSER", user)
        .env("PGPASSWORD", password)
        .env("PGDATABASE", database)
        .output()
        .expect("psql and libpq are installed")
}

#[cfg(unix)]
struct DatabaseCleanup {
    names: [String; 2],
    user: String,
    password: String,
    host: String,
    port: u16,
}

#[cfg(unix)]
impl Drop for DatabaseCleanup {
    fn drop(&mut self) {
        for database in &self.names {
            let _ = psql(
                &format!("DROP DATABASE IF EXISTS \"{database}\" WITH (FORCE)"),
                &self.user,
                "postgres",
                &self.password,
                &self.host,
                self.port,
            );
        }
    }
}

#[cfg(unix)]
fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}
