//! Configured catalog host shared by BI schema compatibility scenarios.
use super::support::{ChildGuard, TempDirectory, TestDatabase, TestEnvironment};
use std::{
    fs,
    net::TcpListener,
    path::PathBuf,
    process::{Command, Output, Stdio},
    thread,
    time::{Duration, Instant},
};

pub(super) fn with_catalog_host(
    query_context: Option<serde_json::Value>,
    check: impl FnOnce(&TestDatabase, &dyn Fn(&str) -> Output),
) {
    let environment = TestEnvironment::load();
    let source = environment.source(None);
    let reader = environment.source(Some("secondary"));
    let identity = environment.gateway();
    let database = TestDatabase::create(&source, "yasp_bi_catalog");
    database.populate();
    database.connection.execute("COMMENT ON COLUMN public.users.id IS 'user identifier'; CREATE SCHEMA excluded; CREATE TABLE excluded.hidden (id integer)");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let temporary = TempDirectory::new("yasp_bi_catalog_host");
    let directory =
        PathBuf::from(std::env::var_os("YASP_TEST_TLS_DIR").expect("use scripts/test.py"));
    let mut config = serde_json::json!({
        "version": 1,
        "gateway": {"listen": address.to_string(), "protocol": "redshift", "tls": {"mode": "enable", "cert_file": directory.join("server.crt"), "key_file": directory.join("server.key")}},
        "targets": {"primary": {"engine": "postgresql", "host": source.host, "port": source.port, "database": database.connection.identity.database, "logins": {
            "discovery": {"username": source.identity.user, "password_env": "YASP_BI_OWNER_PASSWORD"},
            "ordinary": {"username": reader.identity.user, "password_env": "YASP_BI_READER_PASSWORD"}
        }}},
        "schema_discovery": {"target": "primary", "backend_login": "discovery", "catalog": {"schemas": ["public"], "refresh_interval_seconds": 1}},
        "hooks": {
            "select_backend": {"type": "rust", "callback": "builtin.select_backend.passthrough", "params": {"target": "primary", "backend_login": "ordinary"}}
        }
    });
    if let Some(options) = query_context {
        config["hooks"]["query_context"] = options;
    }
    let path = temporary.0.join("config.json");
    fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
    let mut process = ChildGuard::new(
        Command::new(env!("CARGO_BIN_EXE_yasp"))
            .args(["--config", path.to_str().unwrap()])
            .env("YASP_BI_OWNER_PASSWORD", &source.identity.password)
            .env("YASP_BI_READER_PASSWORD", &reader.identity.password)
            .env("YASP_GATEWAY_USERNAME", &identity.user)
            .env("YASP_GATEWAY_PASSWORD", &identity.password)
            .env("YASP_GATEWAY_DATABASE", &identity.database)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let query = |sql: &str| {
        identity
            .connection(address)
            .command()
            .env("PGSSLMODE", "verify-full")
            .env("PGSSLROOTCERT", directory.join("ca.crt"))
            .args(["-v", "ON_ERROR_STOP=1", "-c", sql])
            .output()
            .unwrap()
    };
    let started = Instant::now();
    loop {
        assert!(
            process.child().try_wait().unwrap().is_none(),
            "configured catalog host exited"
        );
        if query("SELECT 1").status.success() {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "catalog host did not become ready"
        );
        thread::sleep(Duration::from_millis(20));
    }
    check(&database, &query);
    #[cfg(unix)]
    {
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
            assert!(
                started.elapsed() < Duration::from_secs(5),
                "catalog host shutdown did not complete"
            );
            thread::sleep(Duration::from_millis(20));
        }
    }
}
