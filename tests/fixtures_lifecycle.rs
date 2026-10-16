//! Verify integration fixtures reclaim owned resources after a failed scenario.

#![cfg(unix)]
mod support;

use std::{net::TcpStream, panic::AssertUnwindSafe, process::Command, sync::Arc};
use support::{
    ChildGuard, GatewayServer, RedisNamespace, TempDirectory, TestDatabase, TestEnvironment,
    assert_success,
};

#[test]
fn fixture_cleanup_runs_during_unwind() {
    let environment = TestEnvironment::load();
    let source = environment.source(None);
    let identity = environment.gateway();
    let redis_url =
        std::env::var("YASP_TEST_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".into());
    let mut database_name = String::new();
    let mut address = None;
    let mut child_pid = 0;
    let mut temporary_path = None;
    let mut key = String::new();
    let failure = std::panic::catch_unwind(AssertUnwindSafe(|| {
        let database = TestDatabase::create(&source, "yasp_cleanup");
        database_name = database.connection.identity.database.clone();
        let temporary = TempDirectory::new("yasp_cleanup");
        temporary_path = Some(temporary.0.clone());
        let namespace = RedisNamespace::new(&redis_url, "yasp_cleanup");
        key = format!("{}:fixture", namespace.name);
        let seeded = Command::new("redis-cli")
            .args(["-u", &redis_url, "SET", &key, "owned"])
            .output()
            .expect("redis-cli is installed");
        assert_success(&seeded, "seed cleanup fixture");
        assert_eq!(String::from_utf8_lossy(&seeded.stdout).trim(), "OK");
        let backend = Arc::new(
            yasp::backend::PostgresBackend::new(database.connection.backend_config()).unwrap(),
        );
        let server = GatewayServer::fixed(identity.gateway_config(), backend);
        address = Some(server.address);
        let query = identity
            .connection(server.address)
            .execute("SELECT current_database()");
        assert_eq!(String::from_utf8_lossy(&query.stdout).trim(), database_name);
        let child = ChildGuard::new(
            database
                .connection
                .command()
                .args(["-c", "SELECT pg_sleep(30)"])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()
                .expect("psql is installed"),
        );
        child_pid = child.id();
        panic!("exercise fixture teardown after an assertion failure");
    }));
    assert!(failure.is_err());
    let databases = source.execute(&format!(
        "SELECT count(*) FROM pg_database WHERE datname = '{database_name}'"
    ));
    assert_eq!(String::from_utf8_lossy(&databases.stdout).trim(), "0");
    assert!(
        TcpStream::connect(address.unwrap()).is_err(),
        "gateway listener is released"
    );
    assert!(
        !std::path::Path::new(&format!("/proc/{child_pid}")).exists(),
        "test client is killed and reaped"
    );
    assert!(!temporary_path.unwrap().exists());
    let remaining = Command::new("redis-cli")
        .args(["-u", &redis_url, "EXISTS", &key])
        .output()
        .expect("redis-cli is installed");
    assert_success(&remaining, "check cleanup fixture key");
    assert_eq!(String::from_utf8_lossy(&remaining.stdout).trim(), "0");
}

#[test]
fn stalled_gateway_cleanup_times_out_and_releases_its_listener() {
    use std::{
        sync::mpsc,
        time::{Duration, Instant},
    };

    let (release, held) = mpsc::channel();
    let (entered, started) = mpsc::channel();
    let (completed, finished) = mpsc::channel();
    let server = GatewayServer::start(move |listener, _shutdown| async move {
        entered.send(()).unwrap();
        held.recv_timeout(Duration::from_secs(2)).unwrap();
        drop(listener);
        completed.send(()).unwrap();
    });
    let address = server.address;
    started.recv_timeout(Duration::from_secs(1)).unwrap();
    let before_stop = Instant::now();
    let timeout = std::panic::catch_unwind(AssertUnwindSafe(|| {
        server.stop_with_timeout(Duration::from_millis(25));
    }));
    let elapsed = before_stop.elapsed();
    let _ = release.send(());
    finished
        .recv_timeout(Duration::from_secs(1))
        .expect("released server finishes");
    assert!(timeout.is_err(), "stalled cleanup fails the scenario");
    assert!(
        elapsed < Duration::from_secs(1),
        "cleanup observes its deadline"
    );
    assert!(
        TcpStream::connect(address).is_err(),
        "released listener closes"
    );
}
