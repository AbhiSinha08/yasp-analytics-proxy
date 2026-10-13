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
        config::{BackendConfig, BackendSettings, FileConfig, GatewayConfig, GatewaySettings},
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
    let make_routes = || {
        BTreeMap::from([
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
                backend_config(source_user.clone(), "unused".into(), "postgres".into(), 1),
            ),
        ])
    };
    let backends = Arc::new(
        PostgresBackends::new(
            make_routes(),
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
    .expect("gateway identity is valid")
    .with_settings(GatewaySettings {
        max_sessions: 2,
        ..GatewaySettings::default()
    })
    .expect("test gateway limits are valid");
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

    let route_two_pid_before = client(
        &["SELECT pg_backend_pid() /* route_two */"],
        &gateway_user,
        &gateway_password_for_client,
    );
    assert!(route_two_pid_before.status.success());
    let route_two_pid_before = String::from_utf8_lossy(&route_two_pid_before.stdout)
        .trim()
        .parse::<i32>()
        .expect("route query returns the PostgreSQL backend PID");
    routed_cancellation_contract(
        address,
        &gateway_user,
        &gateway_password_for_client,
        &gateway_database,
    );
    let route_two_pid_after = client(
        &["SELECT pg_backend_pid() /* route_two */"],
        &gateway_user,
        &gateway_password_for_client,
    );
    assert!(route_two_pid_after.status.success());
    let route_two_pid_after = String::from_utf8_lossy(&route_two_pid_after.stdout)
        .trim()
        .parse::<i32>()
        .expect("route query returns the PostgreSQL backend PID");
    assert_ne!(
        route_two_pid_before, route_two_pid_after,
        "a backend whose cancellation may still be in flight is discarded"
    );

    assert!(
        callback_identity_ok.load(Ordering::SeqCst),
        "selector receives the authenticated frontend identity"
    );

    let _ = shutdown.send(());
    server.join().expect("routed gateway task joins");
    #[cfg(feature = "python")]
    python_routing_contract(
        Arc::new(
            PostgresBackends::new(
                make_routes(),
                BackendSettings {
                    max_connections_per_pool: 1,
                    max_connections_total: 4,
                },
            )
            .unwrap(),
        ),
        route_one,
        route_two,
        &gateway_user,
        &gateway_password_for_client,
        &gateway_database,
        &route_one_marker,
        &route_two_marker,
    );
    drop(cleanup);
}

#[cfg(all(unix, feature = "python"))]
#[allow(clippy::too_many_arguments)]
fn python_routing_contract(
    backends: std::sync::Arc<yasp::backend::PostgresBackends>,
    route_one: yasp::hooks::BackendSelection,
    route_two: yasp::hooks::BackendSelection,
    username: &str,
    password: &str,
    database: &str,
    marker_one: &str,
    marker_two: &str,
) {
    use futures::SinkExt;
    use pgwire::{
        api::client::{
            ClientInfo, Config, auth::DefaultStartupHandler, query::DefaultSimpleQueryHandler,
        },
        error::PgWireClientError,
        messages::{PgWireFrontendMessage, cancel::CancelRequest},
        tokio::client::{PgWireClient, PgWireMessageClientCodec},
    };
    use std::{
        fs,
        sync::Arc,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };
    use tokio::{
        net::{TcpListener, TcpStream},
        sync::oneshot,
        time::{sleep, timeout},
    };
    use tokio_util::codec::Framed;
    use yasp::{
        config::{GatewayConfig, GatewaySettings, PythonSettings},
        hooks::python::PythonRuntime,
    };

    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let directory = std::env::temp_dir().join(format!(
        "yasp_python_route_{}_{}",
        std::process::id(),
        stamp
    ));
    fs::create_dir(&directory).unwrap();
    let path = directory.join("application.py");
    let entered = directory.join("entered");
    let release = directory.join("release");
    let finished = directory.join("finished");
    fs::write(
        &path,
        r#"
import time
from pathlib import Path

def application_route(request, params):
    assert set(request) == {'authenticated_user', 'client_id', 'query_id', 'sql'}
    assert request['authenticated_user'] == params['username']
    assert len(request['query_id']) == 64
    assert all(character in '0123456789abcdef' for character in request['query_id'])
    sql = request['sql']
    if 'denied' in sql:
        return None
    if 'failed' in sql:
        raise RuntimeError('private Python failure')
    if 'unknown' in sql:
        return {'target': 'absent', 'backend_login': 'reader'}
    if 'malformed' in sql:
        return {'target': 'absent'}
    if 'sequence' in sql:
        return [params['one']['target'], params['one']['backend_login']]
    if 'slow' in sql:
        Path(params['entered']).touch()
        deadline = time.monotonic() + 5
        while not Path(params['release']).exists():
            if time.monotonic() > deadline:
                raise RuntimeError('test synchronization failed')
            time.sleep(0.005)
        Path(params['finished']).touch()
    return params['two'] if 'route_two' in sql else params['one']
"#,
    )
    .unwrap();
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
        let runtime = PythonRuntime::new(PythonSettings {
            queue_capacity: 1, execution_timeout_ms: 500,
            ..PythonSettings::default()
        }).await.unwrap();
        let params = serde_json::json!({"one": route_one, "two": route_two, "username": username,
                                         "entered": entered, "release": release, "finished": finished});
        let selector = runtime.register(&path, "application_route", params.as_object().unwrap().clone()).await.unwrap();
        let gateway = GatewayConfig::new(username.into(), password.into(), database.into()).unwrap()
            .with_settings(GatewaySettings { shutdown_timeout_ms: 50, ..GatewaySettings::default() }).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown, stopped) = oneshot::channel();
        let server = tokio::spawn(yasp::gateway::serve_routed(listener, gateway, backends, selector, async {
            let _ = stopped.await;
        }));
        let mut config = Config::new();
        config.host(address.ip().to_string()).port(address.port()).user(username).password(password).dbname(database);
        let mut client = PgWireClient::connect(Arc::new(config), DefaultStartupHandler::new(), None).await.unwrap();

        async fn marker(client: &mut PgWireClient, sql: &str) -> String {
            let mut response = client.simple_query(DefaultSimpleQueryHandler::new(), sql).await.unwrap()
                .remove(0).into_data_rows_reader();
            response.next_row().unwrap().next_value::<String>().unwrap().unwrap()
        }
        fn code(error: PgWireClientError) -> String {
            match error {
                PgWireClientError::RemoteError(info) => {
                    assert!(!info.message.contains("private Python"));
                    info.code
                }
                other => panic!("unexpected frontend error: {other:?}"),
            }
        }
        for (tag, expected) in [("route_one", marker_one), ("route_two", marker_two)] {
            assert_eq!(marker(&mut client, &format!("SELECT current_database() || ':' || current_user /* {tag} */")).await, expected);
        }
        for (tag, expected) in [("denied", "42501"), ("failed", "XX000"), ("unknown", "42501"), ("malformed", "XX000"), ("sequence", "XX000")] {
            let error = client.simple_query(DefaultSimpleQueryHandler::new(), &format!("SELECT 1 /* {tag} */")).await.unwrap_err();
            assert_eq!(code(error), expected);
            assert_eq!(marker(&mut client, "SELECT current_database() || ':' || current_user").await, marker_one);
        }
        for cancel in [true, false] {
            let pid = client.process_id();
            let secret = client.secret_key().clone();
            let mut query = Box::pin(client.simple_query(DefaultSimpleQueryHandler::new(), "SELECT 1 /* slow */"));
            timeout(Duration::from_secs(2), async {
                while !entered.exists() {
                    tokio::select! {
                        result = &mut query => panic!("selector completed before synchronization: {result:?}"),
                        _ = sleep(Duration::from_millis(5)) => {}
                    }
                }
            }).await.unwrap();
            if cancel {
                let socket = TcpStream::connect(address).await.unwrap();
                Framed::new(socket, PgWireMessageClientCodec::default())
                    .send(PgWireFrontendMessage::CancelRequest(CancelRequest::new(pid, secret))).await.unwrap();
            }
            assert_eq!(code(query.await.unwrap_err()), if cancel { "57014" } else { "XX000" });
            // Neither cancellation nor timeout releases the executing worker.
            assert_eq!(code(client.simple_query(DefaultSimpleQueryHandler::new(), "SELECT 1").await.unwrap_err()), "XX000");
            fs::write(&release, "").unwrap();
            timeout(Duration::from_secs(2), async {
                while !finished.exists() { sleep(Duration::from_millis(5)).await; }
            }).await.unwrap();
            timeout(Duration::from_secs(2), async {
                loop {
                    match client.simple_query(DefaultSimpleQueryHandler::new(), "SELECT current_database() || ':' || current_user").await {
                        Ok(mut response) => {
                            let mut rows = response.remove(0).into_data_rows_reader();
                            assert_eq!(rows.next_row().unwrap().next_value::<String>().unwrap().unwrap(), marker_one);
                            break;
                        }
                        Err(error) => {
                            assert_eq!(code(error), "XX000");
                            sleep(Duration::from_millis(5)).await;
                        }
                    }
                }
            }).await.unwrap();
            for path in [&entered, &release, &finished] { fs::remove_file(path).unwrap(); }
        }
        drop(client);
        shutdown.send(()).unwrap();
        server.await.unwrap().unwrap();
    });
    fs::remove_dir_all(directory).unwrap();
}

#[cfg(unix)]
fn routed_cancellation_contract(
    address: std::net::SocketAddr,
    username: &str,
    password: &str,
    database: &str,
) {
    use std::sync::Arc;
    use tokio::runtime::Builder;

    Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("cancellation test runtime starts")
        .block_on(async {
            use pgwire::{
                api::client::{
                    ClientInfo, Config, auth::DefaultStartupHandler,
                    query::DefaultSimpleQueryHandler,
                },
                error::PgWireClientError,
                messages::{
                    PgWireFrontendMessage, cancel::CancelRequest, startup::SecretKey,
                },
                tokio::client::{PgWireClient, PgWireMessageClientCodec},
            };
            use futures::SinkExt;
            use tokio::{net::TcpStream, time::sleep};
            use tokio_util::codec::Framed;

            async fn connect(
                address: std::net::SocketAddr,
                username: &str,
                password: &str,
                database: &str,
            ) -> PgWireClient {
                let mut config = Config::new();
                config
                    .host(address.ip().to_string())
                    .port(address.port())
                    .user(username)
                    .password(password)
                    .dbname(database);
                PgWireClient::connect(
                    Arc::new(config),
                    DefaultStartupHandler::new(),
                    None,
                )
                .await
                .expect("frontend authenticates with the gateway")
            }

            async fn cancel(
                address: std::net::SocketAddr,
                pid: i32,
                secret_key: SecretKey,
            ) {
                let socket = TcpStream::connect(address)
                    .await
                    .expect("cancel connection reaches the gateway");
                let mut socket = Framed::new(socket, PgWireMessageClientCodec::default());
                socket
                    .send(PgWireFrontendMessage::CancelRequest(CancelRequest::new(
                        pid, secret_key,
                    )))
                    .await
                    .expect("cancel request is sent");
            }

            let mut client = connect(address, username, password, database).await;
            let client_pid = client.process_id();
            let client_key = client.secret_key().clone();

            // A valid key with no active query is harmless.
            cancel(address, client_pid, client_key.clone()).await;
            sleep(std::time::Duration::from_millis(50)).await;
            client
                .simple_query(DefaultSimpleQueryHandler::new(), "SELECT 1 /* route_two */")
                .await
                .expect("idle cancellation does not affect the next query");

            let mut unknown_key_query = Box::pin(client.simple_query(
                DefaultSimpleQueryHandler::new(),
                "SELECT pg_sleep(0.5) /* route_two */",
            ));
            tokio::select! {
                result = &mut unknown_key_query => panic!("query completed before the unknown cancel: {result:?}"),
                _ = sleep(std::time::Duration::from_millis(100)) => {}
            }
            cancel(address, i32::MIN, SecretKey::I32(0)).await;
            unknown_key_query
                .await
                .expect("unknown cancellation key does not interrupt an active query");

            let mut other = connect(address, username, password, database).await;
            let mut cancelled_query = Box::pin(client.simple_query(
                DefaultSimpleQueryHandler::new(),
                "SELECT pg_sleep(8) /* route_two */",
            ));
            let mut other_query = Box::pin(other.simple_query(
                DefaultSimpleQueryHandler::new(),
                "SELECT pg_sleep(2) /* route_one */",
            ));

            tokio::select! {
                result = &mut cancelled_query => panic!("long query completed before cancellation: {result:?}"),
                result = &mut other_query => panic!("other route completed before cancellation: {result:?}"),
                _ = sleep(std::time::Duration::from_millis(200)) => {}
            }
            cancel(address, client_pid, client_key.clone()).await;

            let error = cancelled_query
                .await
                .expect_err("active routed query is cancelled");
            match error {
                PgWireClientError::RemoteError(info) => assert_eq!(info.code, "57014"),
                other => panic!("cancellation returned an unexpected error: {other:?}"),
            }
            other_query
                .await
                .expect("cancelling one routed query leaves the other route running");

            client
                .simple_query(DefaultSimpleQueryHandler::new(), "SELECT 1 /* route_two */")
                .await
                .expect("frontend session is usable after query cancellation");

            // A late request from the first session cannot cancel another client's work.
            let mut other_query = Box::pin(other.simple_query(
                DefaultSimpleQueryHandler::new(),
                "SELECT pg_sleep(1) /* route_one */",
            ));
            tokio::select! {
                result = &mut other_query => panic!("other route completed before the stale cancel: {result:?}"),
                _ = sleep(std::time::Duration::from_millis(100)) => {}
            }
            cancel(address, client_pid, client_key).await;
            other_query
                .await
                .expect("late cancellation cannot interrupt another client");
        });
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
