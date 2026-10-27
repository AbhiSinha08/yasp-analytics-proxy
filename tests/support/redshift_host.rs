//! Isolated databases on the configured persistent or runner-owned Redshift fixture.
#![allow(dead_code)]
use super::support::{self, ChildGuard, Identity, PostgresConnection, TempDirectory};
use std::{
    fs,
    net::{SocketAddr, TcpListener},
    path::PathBuf,
    process::{Command, Output},
    thread,
    time::{Duration, Instant},
};

pub(super) struct Fixture {
    pub source: PostgresConnection,
    pub ca: PathBuf,
    container: String,
}

impl Fixture {
    pub fn new() -> Self {
        let container = std::env::var("YASP_REDSHIFT_CONTAINER")
            .expect("run Redshift tests through scripts/test.py");
        let source = PostgresConnection {
            host: std::env::var("YASP_REDSHIFT_HOST").unwrap(),
            port: std::env::var("YASP_REDSHIFT_PORT")
                .unwrap()
                .parse()
                .unwrap(),
            identity: Identity {
                user: std::env::var("YASP_REDSHIFT_USER").unwrap(),
                password: std::env::var("YASP_REDSHIFT_PASSWORD").unwrap(),
                database: support::unique_name("yasp_rs"),
            },
        };
        let fixture = Self {
            source,
            ca: std::env::var_os("YASP_REDSHIFT_CA_FILE").unwrap().into(),
            container,
        };
        let output = fixture.manage("create-database");
        support::assert_success(&output, "create isolated Redshift database");
        fixture
    }

    fn manage(&self, operation: &str) -> Output {
        Command::new("docker")
            .args([
                "exec",
                &self.container,
                "python3",
                "/yasp/qualify.py",
                operation,
                &self.source.identity.database,
            ])
            .output()
            .unwrap()
    }

    pub fn command(&self) -> Command {
        let mut command = self.source.command();
        command
            .env("PGSSLMODE", "verify-full")
            .env("PGSSLROOTCERT", &self.ca);
        command
    }

    pub fn query(&self, sql: &str) -> Output {
        self.command()
            .args(["-v", "ON_ERROR_STOP=1", "-c", sql])
            .output()
            .unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let result = self.manage("drop-database");
        if !result.status.success() {
            if thread::panicking() {
                eprintln!(
                    "isolated Redshift database cleanup failed: {}",
                    String::from_utf8_lossy(&result.stderr)
                );
            } else {
                support::assert_success(&result, "drop isolated Redshift database");
            }
        }
    }
}

pub(super) fn with_host(
    profile: &str,
    check: impl FnOnce(&Fixture, &Identity, SocketAddr, &dyn Fn(&str) -> Output),
) {
    with_config(profile, |_| {}, check);
}

pub(super) fn with_config(
    profile: &str,
    configure: impl FnOnce(&mut serde_json::Value),
    check: impl FnOnce(&Fixture, &Identity, SocketAddr, &dyn Fn(&str) -> Output),
) {
    let fixture = Fixture::new();
    let identity = support::TestEnvironment::load().gateway();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let temporary = TempDirectory::new("yasp_redshift_host");
    let tls = PathBuf::from(std::env::var_os("YASP_TEST_TLS_DIR").unwrap());
    let mut config = serde_json::json!({
        "version": 1,
        "gateway": {"listen": address.to_string(), "protocol": profile,
            "tls": {"mode": "enable", "cert_file": tls.join("server.crt"), "key_file": tls.join("server.key")}},
        "targets": {"redshift": {"engine": "redshift", "host": fixture.source.host,
            "port": fixture.source.port, "database": fixture.source.identity.database,
            "tls": {"mode": "enable", "ca_file": fixture.ca},
            "logins": {
                "discovery": {"username": fixture.source.identity.user, "password_env": "YASP_REDSHIFT_PASSWORD"},
                "reader": {"username": "yasp_reader", "password_env": "YASP_RS_READER_PASSWORD"},
                "denied": {"username": "yasp_denied", "password_env": "YASP_RS_DENIED_PASSWORD"}
            }}},
        "schema_discovery": {"target": "redshift", "backend_login": "discovery",
            "catalog": {"schemas": ["public", "analytics"], "refresh_interval_seconds": 1}},
        "hooks": {
            "query_context": {"type": "rust", "callback": "builtin.query_context.metabase"},
            "select_backend": {"type": "rust", "callback": "builtin.select_backend.passthrough",
                "params": {"target": "redshift", "backend_login": "reader"}}
        }
    });
    configure(&mut config);
    let path = temporary.0.join("config.json");
    fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
    let log = temporary.0.join("host.log");
    let log_file = fs::File::create(&log).unwrap();
    let mut process = ChildGuard::new(
        Command::new(env!("CARGO_BIN_EXE_yasp"))
            .args(["--config", path.to_str().unwrap()])
            .env("LOG_LEVEL", "DEBUG")
            .env("YASP_GATEWAY_USERNAME", &identity.user)
            .env("YASP_GATEWAY_PASSWORD", &identity.password)
            .env("YASP_GATEWAY_DATABASE", &identity.database)
            .env("YASP_REDSHIFT_PASSWORD", &fixture.source.identity.password)
            .env(
                "YASP_RS_READER_PASSWORD",
                std::env::var("YASP_REDSHIFT_READER_PASSWORD")
                    .unwrap_or_else(|_| "yasp_reader_password".into()),
            )
            .env(
                "YASP_RS_DENIED_PASSWORD",
                std::env::var("YASP_REDSHIFT_DENIED_PASSWORD")
                    .unwrap_or_else(|_| "yasp_denied_password".into()),
            )
            .stdout(log_file.try_clone().unwrap())
            .stderr(log_file)
            .spawn()
            .unwrap(),
    );
    let query = |sql: &str| {
        identity
            .connection(address)
            .command()
            .env("PGSSLMODE", "verify-full")
            .env("PGSSLROOTCERT", tls.join("ca.crt"))
            .args(["-v", "ON_ERROR_STOP=1", "-c", sql])
            .output()
            .unwrap()
    };
    let started = Instant::now();
    loop {
        assert!(
            process.child().try_wait().unwrap().is_none(),
            "Redshift YAML host exited: {}",
            fs::read_to_string(&log).unwrap()
        );
        if query("SELECT 1").status.success() {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(15),
            "Redshift YAML host not ready: {}",
            fs::read_to_string(&log).unwrap()
        );
        thread::sleep(Duration::from_millis(20));
    }
    check(&fixture, &identity, address, &query);
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
            assert!(
                status.success(),
                "host shutdown: {}",
                fs::read_to_string(&log).unwrap()
            );
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "host shutdown timed out"
        );
        thread::sleep(Duration::from_millis(20));
    }
}
