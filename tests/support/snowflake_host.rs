//! The normal YAML executable with no PostgreSQL target or discovery dependency.
#![allow(dead_code)]
use super::support::{ChildGuard, Identity, TempDirectory, TestEnvironment};
use std::{
    fs,
    net::{SocketAddr, TcpListener},
    path::PathBuf,
    process::{Command, Output},
    thread,
    time::{Duration, Instant},
};

pub(super) fn with_host(
    profile: &str,
    check: impl FnOnce(&Identity, SocketAddr, &dyn Fn(&str) -> Output),
) {
    with_config(profile, |_| {}, check);
}

pub(super) fn with_config(
    profile: &str,
    configure: impl FnOnce(&mut serde_json::Value),
    check: impl FnOnce(&Identity, SocketAddr, &dyn Fn(&str) -> Output),
) {
    let identity = TestEnvironment::load().gateway();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let temporary = TempDirectory::new("yasp_snowflake_host");
    let tls = PathBuf::from(std::env::var_os("YASP_TEST_TLS_DIR").unwrap());
    let mut config = serde_json::json!({
        "version":1,
        "gateway":{"listen":address.to_string(),"protocol":profile,"tls":{"mode":"enable","cert_file":tls.join("server.crt"),"key_file":tls.join("server.key")}},
        "targets":{"snowflake":{"engine":"snowflake","database":"YASP_DEMO",
            "snowflake":{"account":"local","schema":"PUBLIC","warehouse":"LOCAL","emulator":std::env::var("YASP_SNOWFLAKE_ENDPOINT").unwrap()},
            "logins":{"reader":{"username":"fake","private_key_file":std::env::var("YASP_SNOWFLAKE_PRIVATE_KEY_FILE").unwrap(),"role":"PUBLIC"}}}},
        "schema_discovery":{"target":"snowflake","backend_login":"reader","catalog":{"schemas":["PUBLIC","ANALYTICS"],"refresh_interval_seconds":1}},
        "hooks":{"query_context":{"type":"rust","callback":"builtin.query_context.metabase"},
        "select_backend":{"type":"rust","callback":"builtin.select_backend.passthrough","params":{"target":"snowflake","backend_login":"reader"}}}
    });
    configure(&mut config);
    let path = temporary.0.join("config.json");
    fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
    let log = temporary.0.join(format!("snowflake-host-{profile}.log"));
    let log_file = fs::File::create(&log).unwrap();
    let mut process = ChildGuard::new(
        Command::new(env!("CARGO_BIN_EXE_yasp"))
            .args(["--config", path.to_str().unwrap()])
            .env("LOG_LEVEL", "DEBUG")
            .env("YASP_GATEWAY_USERNAME", &identity.user)
            .env("YASP_GATEWAY_PASSWORD", &identity.password)
            .env("YASP_GATEWAY_DATABASE", &identity.database)
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
            "Snowflake host exited: {}",
            fs::read_to_string(&log).unwrap()
        );
        if query("SELECT 1").status.success() {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(15),
            "Snowflake host startup: {}",
            fs::read_to_string(&log).unwrap()
        );
        thread::sleep(Duration::from_millis(20));
    }
    check(&identity, address, &query);
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
        assert!(started.elapsed() < Duration::from_secs(10));
        thread::sleep(Duration::from_millis(20));
    }
}
