use super::*;

pub(super) struct CacheScenario {
    pub(super) identity: Identity,
    pub(super) route: BackendSelection,
    pub(super) primary: PostgresConnection,
    pub(super) secondary: PostgresConnection,
    pub(super) redis_url: String,
    pub(super) namespace: RedisNamespace,
    _database: TestDatabase,
}

impl CacheScenario {
    pub(super) fn new() -> Self {
        let environment = TestEnvironment::load();
        let identity = environment.gateway();
        let route = environment.config.default_backend().unwrap();
        let database = TestDatabase::create(&environment.source(None), "yasp_cache");
        database.populate();
        let name = &database.connection.identity.database;
        let primary = database.connection.clone();
        let secondary = environment.source(Some("secondary")).database(name);
        assert_ne!(
            primary.identity.user, secondary.identity.user,
            "cache scope needs distinct source logins"
        );
        primary.execute(&format!("GRANT CONNECT ON DATABASE {} TO {}; GRANT USAGE ON SCHEMA public TO {}; GRANT SELECT ON ALL TABLES IN SCHEMA public TO {}", quote_ident(name), quote_ident(&secondary.identity.user), quote_ident(&secondary.identity.user), quote_ident(&secondary.identity.user)));
        let redis_url = std::env::var("YASP_TEST_REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1:6379/".into());
        let namespace = RedisNamespace::new(&redis_url, "yasp_cache");
        Self {
            identity,
            route,
            primary,
            secondary,
            redis_url,
            namespace,
            _database: database,
        }
    }

    pub(super) fn backends(&self) -> Arc<PostgresBackends> {
        Arc::new(
            PostgresBackends::new(
                BTreeMap::from([
                    (self.route.clone(), self.primary.backend_config()),
                    (
                        BackendSelection {
                            target: self.route.target.clone(),
                            backend_login: "secondary".into(),
                        },
                        self.secondary.backend_config(),
                    ),
                ]),
                BackendSettings::default(),
            )
            .unwrap(),
        )
    }

    pub(super) fn selector(&self) -> impl Into<yasp::hooks::RoutedSelector> + Send + 'static {
        bind_select_backend(self.route.clone(), |_, selected| Ok(selected.clone()))
    }
}

#[cfg(unix)]
pub(super) fn settings(
    namespace: &str,
    ttl: u32,
    max_rows: usize,
    budget: usize,
) -> yasp::config::CacheSettings {
    yasp::config::CacheSettings {
        enabled: true,
        url_env: "YASP_TEST_REDIS_URL".into(),
        namespace: namespace.into(),
        ttl_seconds: ttl,
        max_rows_per_query: max_rows,
        max_buffered_bytes_total: budget,
        eligibility: None,
    }
}

pub(super) fn start_server(
    gateway: GatewayConfig,
    backends: Arc<PostgresBackends>,
    selector: impl Into<yasp::hooks::RoutedSelector> + Send + 'static,
) -> GatewayServer {
    GatewayServer::routed(gateway, backends, selector)
}

#[cfg(unix)]
pub(super) fn psql(
    query: &str,
    address: &std::net::SocketAddr,
    user: &str,
    password: &str,
    database: &str,
) -> std::process::Output {
    psql_command(
        &address.ip().to_string(),
        address.port(),
        user,
        password,
        database,
    )
    .args(["-c", query])
    .output()
    .expect("psql and libpq are installed")
}

#[cfg(unix)]
pub(super) fn psql_direct(
    query: &str,
    host: &str,
    port: u16,
    user: &str,
    password: &str,
    database: &str,
) -> std::process::Output {
    psql_command(host, port, user, password, database)
        .args(["-c", query])
        .output()
        .expect("psql and libpq are installed")
}

#[cfg(unix)]
pub(super) fn psql_child(
    query: &str,
    address: &std::net::SocketAddr,
    user: &str,
    password: &str,
    database: &str,
) -> support::ChildGuard {
    support::ChildGuard::new(
        psql_command(
            &address.ip().to_string(),
            address.port(),
            user,
            password,
            database,
        )
        .args(["-c", query])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("psql and libpq are installed"),
    )
}

pub(super) fn psql_command(
    host: &str,
    port: u16,
    user: &str,
    password: &str,
    database: &str,
) -> Command {
    let mut command = PostgresConnection {
        host: host.into(),
        port,
        identity: Identity {
            user: user.into(),
            password: password.into(),
            database: database.into(),
        },
    }
    .command();
    command.args(["-v", "ON_ERROR_STOP=1"]);
    command
}

#[cfg(unix)]
pub(super) fn redis_set(url: &str, key: &str, value: &str) {
    let output = std::process::Command::new("redis-cli")
        .args(["-u", url, "SET", key, value])
        .output()
        .expect("redis-cli is installed");
    assert!(
        output.status.success(),
        "could not seed an invalid Redis entry"
    );
    assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "OK");
}

#[cfg(unix)]
pub(super) fn cache_key(namespace: &str, sql: &str) -> String {
    format!(
        "{namespace}:{}",
        yasp::query::ParsedQuery::parse(sql).unwrap().query_id()
    )
}

#[cfg(unix)]
pub(super) fn redis_exists(url: &str, key: &str) -> bool {
    let output = std::process::Command::new("redis-cli")
        .args(["-u", url, "EXISTS", key])
        .output()
        .expect("redis-cli is installed");
    assert!(output.status.success(), "Redis EXISTS succeeds");
    String::from_utf8_lossy(&output.stdout).trim() == "1"
}

#[cfg(unix)]
pub(super) fn redis_get(url: &str, key: &str) -> Vec<u8> {
    let output = std::process::Command::new("redis-cli")
        .args(["-u", url, "--raw", "GET", key])
        .output()
        .expect("redis-cli is installed");
    assert!(
        output.status.success(),
        "could not read the test cache entry"
    );
    let mut bytes = output.stdout;
    assert_eq!(bytes.pop(), Some(b'\n'));
    bytes
}

#[cfg(unix)]
pub(super) fn redis_set_bytes(url: &str, key: &str, value: &[u8]) {
    use std::io::Write;
    let mut child = std::process::Command::new("redis-cli")
        .args(["-u", url, "-x", "SET", key])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .expect("redis-cli is installed");
    child
        .stdin
        .take()
        .expect("Redis input is piped")
        .write_all(value)
        .expect("malformed cache packet reaches Redis");
    let output = child.wait_with_output().expect("Redis SET finishes");
    assert!(
        output.status.success(),
        "could not write the test cache entry"
    );
    assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "OK");
}

#[cfg(unix)]
pub(super) fn stdout(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}
