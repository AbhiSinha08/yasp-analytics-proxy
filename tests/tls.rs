//! Transport contracts against the disposable TLS services from scripts/test.py.
mod support;

use futures::SinkExt;
use pgwire::{
    api::client::{
        ClientInfo, Config, auth::DefaultStartupHandler, query::DefaultSimpleQueryHandler,
    },
    messages::{PgWireFrontendMessage, cancel::CancelRequest},
    tokio::{
        TlsConnector,
        client::{PgWireClient, PgWireMessageClientCodec},
    },
};
use rustls::pki_types::{CertificateDer, ServerName, pem::PemObject};
use std::{collections::BTreeMap, path::PathBuf, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::{sleep, timeout},
};
use tokio_util::codec::Framed;
use yasp::{
    backend::PostgresBackend,
    config::{BackendConfig, ClientTlsSettings, GatewaySettings, GatewayTls},
};

fn file(name: &str) -> PathBuf {
    PathBuf::from(
        std::env::var("YASP_TEST_TLS_DIR").expect("run TLS tests with python3 scripts/test.py"),
    )
    .join(name)
}

fn outbound() -> ClientTlsSettings {
    ClientTlsSettings {
        ca_file: Some(file("ca.crt")),
        cert_file: Some(file("client.crt")),
        key_file: Some(file("client.key")),
    }
}

fn settings(name: &str) -> GatewaySettings {
    GatewaySettings {
        tls: GatewayTls {
            mode: "enable".into(),
            cert_file: Some(file(&format!("{name}.crt"))),
            key_file: Some(file(&format!("{name}.key"))),
        },
        max_sessions: 1,
        startup_timeout_ms: 700,
        max_message_bytes: 1024,
        max_sql_bytes: 512,
        ..GatewaySettings::default()
    }
}

fn backend(environment: &support::TestEnvironment, tls: ClientTlsSettings) -> BackendConfig {
    let source = environment.source(None);
    BackendConfig::new_with_tls(
        source.host,
        source.port,
        source.identity.user,
        source.identity.password,
        source.identity.database,
        tls,
    )
    .unwrap()
}

fn connector(ca: &str) -> TlsConnector {
    let mut roots = rustls::RootCertStore::empty();
    let pem = std::fs::read(file(ca)).unwrap();
    for cert in CertificateDer::pem_slice_iter(&pem) {
        roots.add(cert.unwrap()).unwrap();
    }
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    TlsConnector::from(Arc::new(config))
}

async fn encrypted(
    address: std::net::SocketAddr,
) -> pgwire::tokio::tokio_rustls::client::TlsStream<TcpStream> {
    let mut socket = TcpStream::connect(address).await.unwrap();
    socket
        .write_all(&[0, 0, 0, 8, 4, 210, 22, 47])
        .await
        .unwrap();
    assert_eq!(socket.read_u8().await.unwrap(), b'S');
    connector("ca.crt")
        .connect(ServerName::try_from("localhost").unwrap(), socket)
        .await
        .unwrap()
}

async fn connect_client(
    address: std::net::SocketAddr,
    identity: &support::Identity,
) -> PgWireClient {
    let mut config: Config = "sslmode=require".parse().unwrap();
    config
        .host("localhost")
        .port(address.port())
        .user(&identity.user)
        .password(&identity.password)
        .dbname(&identity.database);
    timeout(Duration::from_secs(5), async {
        loop {
            match PgWireClient::connect(
                Arc::new(config.clone()),
                DefaultStartupHandler::new(),
                Some(connector("ca.crt")),
            )
            .await
            {
                Ok(client) => break client,
                Err(_) => sleep(Duration::from_millis(20)).await,
            }
        }
    })
    .await
    .expect("verified TLS client connects")
}

async fn query(client: &mut PgWireClient, sql: &str) {
    client
        .simple_query(DefaultSimpleQueryHandler::new(), sql)
        .await
        .unwrap();
}

fn psql(
    address: std::net::SocketAddr,
    identity: &support::Identity,
    sql: &str,
    tls: bool,
) -> std::process::Output {
    std::process::Command::new("psql")
        .args(["-X", "-w", "-At", "-v", "ON_ERROR_STOP=1", "-c", sql])
        .env("PGHOST", "localhost")
        .env("PGPORT", address.port().to_string())
        .env("PGUSER", &identity.user)
        .env("PGPASSWORD", &identity.password)
        .env("PGDATABASE", &identity.database)
        .env("PGSSLMODE", if tls { "verify-full" } else { "disable" })
        .env("PGSSLROOTCERT", file("ca.crt"))
        .env("PGCHANNELBINDING", if tls { "require" } else { "disable" })
        .env("PGCONNECT_TIMEOUT", "2")
        .output()
        .unwrap()
}

fn redis_command(arguments: &[&str]) -> String {
    let output = std::process::Command::new("redis-cli")
        .args(["-u", &std::env::var("YASP_TEST_REDIS_URL").unwrap()])
        .args(arguments)
        .output()
        .unwrap();
    support::assert_success(&output, "isolated Redis administration");
    let output = String::from_utf8(output.stdout).unwrap();
    assert!(!output.starts_with("ERR"), "Redis command failed");
    output
}

struct RedisUser(String);
impl Drop for RedisUser {
    fn drop(&mut self) {
        redis_command(&["ACL", "DELUSER", &self.0]);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn configured_host_tls_paths_compute_cache_and_datasets() {
    let env = support::TestEnvironment::load();
    let identity = env.gateway();
    let database = support::TestDatabase::create(&env.source(None), "tls_host");
    database.connection.execute(
        "CREATE TABLE public.tls_value (value int); INSERT INTO public.tls_value VALUES (41)",
    );
    let directory = support::TempDirectory::new("tls_host");
    for name in [
        "ca.crt",
        "server.crt",
        "server.key",
        "client.crt",
        "client.key",
    ] {
        std::fs::copy(file(name), directory.0.join(name)).unwrap();
    }
    // Each transport can be configured independently, including an entirely plaintext host.
    for (frontend_tls, source_tls, cache_tls, dataset_tls) in [
        (true, true, true, true),
        (false, false, false, false),
        (true, false, true, false),
        (false, true, false, true),
        (true, true, false, false),
        (false, false, true, true),
    ] {
        database
            .connection
            .execute("UPDATE public.tls_value SET value = 41");
        let address = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap();
        let mut config = serde_json::to_value(&env.config).unwrap();
        let route = env.config.default_backend().unwrap();
        let target = &env.config.targets[&route.target];
        let relative_tls = serde_json::json!({"ca_file": "ca.crt", "cert_file": "client.crt", "key_file": "client.key"});
        // Exercise target TLS propagation into compute bindings and metadata discovery.
        config["targets"][&route.target] = serde_json::json!({
            "engine": "postgresql", "database": database.connection.identity.database,
            "tls": if source_tls {
                let mut tls = relative_tls.clone();
                tls["mode"] = "enable".into();
                tls
            } else { serde_json::json!({"mode": "disable"}) },
            "logins": {"reader": {"username": database.connection.identity.user, "password_env": "TLS_TEST_SOURCE_PASSWORD"}},
            "compute": {"types": {"small": {"max_connections": 2, "max_in_flight": 2,
                "instances": [{"id": "one", "host": "localhost", "port": target.port}]}}}
        });
        let namespace = support::RedisNamespace::new(
            &std::env::var("YASP_TEST_REDIS_URL").unwrap(),
            "tls_host",
        );
        let namespace = &namespace.name;
        config["gateway"] = if frontend_tls {
            serde_json::json!({"listen": address.to_string(), "tls": {"mode": "enable", "cert_file": "server.crt", "key_file": "server.key"}})
        } else {
            serde_json::json!({"listen": format!("0.0.0.0:{}", address.port()), "tls": {"mode": "disable"}})
        };
        config["hooks"] = serde_json::json!({"select_backend": {"type": "rust", "callback": "builtin.select_backend.passthrough", "params": {}}});
        config["schema_discovery"] =
            serde_json::json!({"target": route.target, "backend_login": "reader"});
        config["cache"] = serde_json::json!({"enabled": true, "namespace": format!("{namespace}:cache"), "url_env": if cache_tls { "YASP_TEST_REDIS_TLS_URL" } else { "YASP_TEST_REDIS_URL" }, "tls": if cache_tls { relative_tls.clone() } else { serde_json::json!({}) }});
        config["application_contexts"] = serde_json::json!({"namespace": format!("{namespace}:datasets"), "url_env": if dataset_tls { "YASP_TEST_REDIS_TLS_URL" } else { "YASP_TEST_REDIS_URL" }, "tls": if dataset_tls { relative_tls.clone() } else { serde_json::json!({}) },
        "datasets": {"email_locations": {"loader": {"type": "rust", "callback": "example.datasets.email_locations"}}}});
        let path = directory.0.join("config.json");
        std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
        let mut child = support::ChildGuard::new(
            std::process::Command::new(env!("CARGO_BIN_EXE_yasp"))
                .args(["--config", path.to_str().unwrap()])
                .current_dir(support::root())
                .env(
                    "TLS_TEST_SOURCE_PASSWORD",
                    &database.connection.identity.password,
                )
                .env("YASP_GATEWAY_USERNAME", &identity.user)
                .env("YASP_GATEWAY_PASSWORD", &identity.password)
                .env("YASP_GATEWAY_DATABASE", &identity.database)
                .stdout(std::process::Stdio::null())
                .spawn()
                .unwrap(),
        );
        timeout(Duration::from_secs(8), async {
            loop {
                assert!(
                    child.child().try_wait().unwrap().is_none(),
                    "TLS host stays running"
                );
                if psql(address, &identity, "SELECT 1", frontend_tls)
                    .status
                    .success()
                {
                    break;
                }
                sleep(Duration::from_millis(30)).await;
            }
        })
        .await
        .expect("host loads relative certificate paths before serving");
        let first = psql(
            address,
            &identity,
            "SELECT value FROM public.tls_value",
            frontend_tls,
        );
        support::assert_success(&first, "TLS source query");
        assert_eq!(String::from_utf8_lossy(&first.stdout).trim(), "41");
        database
            .connection
            .execute("UPDATE public.tls_value SET value = 99");
        let cached = psql(
            address,
            &identity,
            "SELECT value FROM public.tls_value",
            frontend_tls,
        );
        support::assert_success(&cached, "TLS cache replay");
        assert_eq!(
            first.stdout, cached.stdout,
            "cache replay retains the captured source result"
        );
        let metadata = psql(
            address,
            &identity,
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'",
            frontend_tls,
        );
        support::assert_success(&metadata, "TLS discovery");
        assert!(String::from_utf8_lossy(&metadata.stdout).contains("tls_value"));
        drop(child);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn encrypted_disconnect_drops_active_work_and_releases_the_session() {
    struct Dropped(Arc<std::sync::atomic::AtomicBool>);
    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }
    let entered = Arc::new(tokio::sync::Notify::new());
    let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let hook = yasp::hooks::bind_context((entered.clone(), dropped.clone()), |request, state| {
        let pending = request.query.sql().contains("disconnect_test");
        Box::pin(async move {
            if pending {
                let _guard = Dropped(state.1.clone());
                state.0.notify_one();
                std::future::pending::<()>().await;
            }
            Ok(serde_json::Map::new())
        })
    });
    let env = support::TestEnvironment::load();
    let identity = env.gateway();
    let backend = Arc::new(PostgresBackend::new(backend(&env, outbound())).unwrap());
    let mut limits = settings("server");
    limits.context_timeout_ms = 30_000;
    let server = support::GatewayServer::fixed(
        identity
            .gateway_config()
            .with_settings(limits)
            .unwrap()
            .with_context_hook(hook),
        backend,
    );
    let mut client = connect_client(server.address, &identity).await;
    let work = tokio::spawn(async move {
        query(&mut client, "SELECT 1 /* disconnect_test */").await;
    });
    timeout(Duration::from_secs(3), entered.notified())
        .await
        .unwrap();
    work.abort();
    let _ = work.await;
    timeout(Duration::from_secs(2), async {
        while !dropped.load(std::sync::atomic::Ordering::SeqCst) {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("TLS EOF drops active context before its deadline");
    let mut recovered = connect_client(server.address, &identity).await;
    query(&mut recovered, "SELECT 1").await;
    drop(recovered);
    server.stop();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn encrypted_gateway_postgres_cancellation_and_bounds() {
    let env = support::TestEnvironment::load();
    let identity = env.gateway();
    let backend = Arc::new(PostgresBackend::new(backend(&env, outbound())).unwrap());
    let server = support::GatewayServer::fixed(
        identity
            .gateway_config()
            .with_settings(settings("server"))
            .unwrap(),
        backend,
    );
    let mut client = connect_client(server.address, &identity).await;
    query(&mut client, "SELECT current_user").await;
    query(
        &mut client,
        "SELECT table_name FROM information_schema.tables LIMIT 1",
    )
    .await;

    let pid = client.process_id();
    let key = client.secret_key().clone();
    let address = server.address;
    let completed = std::sync::atomic::AtomicBool::new(false);
    let execute = async {
        let result = client
            .simple_query(DefaultSimpleQueryHandler::new(), "SELECT pg_sleep(15)")
            .await;
        completed.store(true, std::sync::atomic::Ordering::SeqCst);
        result
    };
    let cancel = async {
        sleep(Duration::from_millis(150)).await;
        // Legacy cancellation is refused even when the normal session limit is full.
        let raw = TcpStream::connect(address).await.unwrap();
        let mut raw = Framed::new(raw, PgWireMessageClientCodec::default());
        raw.send(PgWireFrontendMessage::CancelRequest(CancelRequest::new(
            pid,
            key.clone(),
        )))
        .await
        .unwrap();
        sleep(Duration::from_millis(150)).await;
        assert!(
            !completed.load(std::sync::atomic::Ordering::SeqCst),
            "plaintext cancellation was refused"
        );
        let secure = encrypted(address).await;
        let mut secure = Framed::new(secure, PgWireMessageClientCodec::default());
        secure
            .send(PgWireFrontendMessage::CancelRequest(CancelRequest::new(
                pid, key,
            )))
            .await
            .unwrap();
    };
    let (result, ()) = timeout(Duration::from_secs(5), async {
        tokio::join!(execute, cancel)
    })
    .await
    .expect("encrypted cancellation works while saturated");
    let error = result.unwrap_err();
    assert!(
        matches!(error, pgwire::error::PgWireClientError::RemoteError(ref info) if info.code == "57014")
    );
    query(&mut client, "SELECT 42").await;
    drop(client);
    sleep(Duration::from_millis(100)).await;

    // The same frame limit applies after decryption, before a large payload is read.
    let mut socket = encrypted(address).await;
    socket.write_all(&[0, 1, 0, 0, 0, 3, 0, 0]).await.unwrap();
    let mut byte = [0];
    let closed = timeout(Duration::from_secs(2), socket.read(&mut byte))
        .await
        .unwrap();
    assert!(closed.is_err() || closed.unwrap() == 0);
    drop(socket);
    sleep(Duration::from_millis(100)).await;

    let mut stalled = TcpStream::connect(address).await.unwrap();
    stalled
        .write_all(&[0, 0, 0, 8, 4, 210, 22, 47])
        .await
        .unwrap();
    assert_eq!(stalled.read_u8().await.unwrap(), b'S');
    assert_eq!(
        timeout(Duration::from_secs(2), stalled.read(&mut byte))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    let mut recovered = connect_client(address, &identity).await;
    query(&mut recovered, "SELECT 1").await;
    drop(recovered);
    server.stop();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn certificate_validation_and_channel_binding() {
    let env = support::TestEnvironment::load();
    let identity = env.gateway();
    for name in ["server", "wrong", "expired"] {
        let backend = Arc::new(PostgresBackend::new(backend(&env, outbound())).unwrap());
        let server = support::GatewayServer::fixed(
            identity
                .gateway_config()
                .with_settings(settings(name))
                .unwrap(),
            backend,
        );
        // Wait for discovery to finish before testing the client verification result.
        sleep(Duration::from_millis(150)).await;
        let output = std::process::Command::new("psql")
            .args(["-X", "-w", "-At", "-c", "SELECT 42"])
            .env("PGHOST", "localhost")
            .env("PGPORT", server.address.port().to_string())
            .env("PGUSER", &identity.user)
            .env("PGPASSWORD", &identity.password)
            .env("PGDATABASE", &identity.database)
            .env("PGSSLMODE", "verify-full")
            .env("PGSSLROOTCERT", file("ca.crt"))
            .env("PGCHANNELBINDING", "require")
            .env("PGCONNECT_TIMEOUT", "3")
            .output()
            .unwrap();
        if name == "server" {
            support::assert_success(&output, "verified SCRAM channel binding");
        } else {
            assert!(!output.status.success(), "reject {name} certificate");
        }
        if name == "server" {
            let mut socket = TcpStream::connect(server.address).await.unwrap();
            socket.write_all(&[0, 0, 0, 8, 0, 3, 0, 0]).await.unwrap();
            assert_eq!(
                socket.read_u8().await.unwrap_err().kind(),
                std::io::ErrorKind::UnexpectedEof
            );
        }
        server.stop();
    }
    let mut missing = settings("server");
    missing.tls.key_file = None;
    assert!(identity.gateway_config().with_settings(missing).is_err());
    let mut mismatch = settings("server");
    mismatch.tls.key_file = Some(file("client.key"));
    assert!(identity.gateway_config().with_settings(mismatch).is_err());
    let mut mismatch_client = outbound();
    mismatch_client.key_file = Some(file("server.key"));
    let source = env.source(None);
    assert!(
        BackendConfig::new_with_tls(
            source.host,
            source.port,
            source.identity.user,
            source.identity.password,
            source.identity.database,
            mismatch_client
        )
        .is_err()
    );
    for ca_file in [Some(file("other-ca.crt")), None] {
        let mut untrusted = outbound();
        untrusted.ca_file = ca_file;
        // System roots do not trust the disposable CA; an unrelated bundle also fails.
        let bad_backend = Arc::new(PostgresBackend::new(backend(&env, untrusted)).unwrap());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        assert!(
            yasp::gateway::serve(
                listener,
                identity.gateway_config(),
                bad_backend,
                std::future::pending()
            )
            .await
            .is_err()
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startup_rejections_reach_tls_clients_before_disconnect() {
    let env = support::TestEnvironment::load();
    let identity = env.gateway();
    for profile in ["postgresql", "redshift"] {
        let backend = Arc::new(PostgresBackend::new(backend(&env, outbound())).unwrap());
        let config = GatewaySettings {
            protocol: profile.into(),
            max_sessions: 8,
            startup_timeout_ms: 5_000,
            ..settings("server")
        };
        let server = support::GatewayServer::fixed(
            identity.gateway_config().with_settings(config).unwrap(),
            backend,
        );
        for (field, message) in [
            ("database", "unknown gateway database"),
            ("user", "authentication failed"),
            ("password", "authentication failed"),
            ("application_name", "unsupported startup setting"),
        ] {
            let mut client = identity.connection(server.address);
            match field {
                "database" => client.identity.database = "unknown_database".into(),
                "user" => client.identity.user = "unknown_user".into(),
                "password" => client.identity.password = "wrong_password".into(),
                _ => {}
            }
            let mut command = client.command();
            command
                .env("PGSSLMODE", "verify-full")
                .env("PGSSLROOTCERT", file("ca.crt"))
                .env("PGCHANNELBINDING", "prefer");
            if field == "application_name" {
                command.env("PGAPPNAME", "x".repeat(64));
            }
            let output = command.args(["-c", "SELECT 1"]).output().unwrap();
            assert!(!output.status.success());
            let error = String::from_utf8_lossy(&output.stderr);
            assert!(error.contains(message), "{profile} {field}: {error}");
        }
        server.stop();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn source_tls_refusal_and_invalid_certificates_never_send_credentials() {
    use pgwire::tokio::TlsAcceptor;
    use rustls::pki_types::PrivateKeyDer;
    let env = support::TestEnvironment::load();
    for name in ["refuse", "wrong", "expired"] {
        let peer = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = peer.local_addr().unwrap().port();
        let peer = tokio::spawn(async move {
            let (mut socket, _) = peer.accept().await.unwrap();
            let mut header = [0; 8];
            socket.read_exact(&mut header).await.unwrap();
            assert_eq!(header, [0, 0, 0, 8, 4, 210, 22, 47]);
            if name == "refuse" {
                socket.write_all(b"N").await.unwrap();
                assert_eq!(
                    socket.read_u8().await.unwrap_err().kind(),
                    std::io::ErrorKind::UnexpectedEof,
                    "no plaintext startup after TLS refusal"
                );
            } else {
                socket.write_all(b"S").await.unwrap();
                let pem = std::fs::read(file(&format!("{name}.crt"))).unwrap();
                let certs = CertificateDer::pem_slice_iter(&pem)
                    .map(|cert| cert.unwrap().into_owned())
                    .collect();
                let key = PrivateKeyDer::from_pem_file(file(&format!("{name}.key"))).unwrap();
                let config = rustls::ServerConfig::builder_with_provider(Arc::new(
                    rustls::crypto::ring::default_provider(),
                ))
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_no_client_auth()
                .with_single_cert(certs, key)
                .unwrap();
                assert!(
                    TlsAcceptor::from(Arc::new(config))
                        .accept(socket)
                        .await
                        .is_err(),
                    "unverified peer receives no PostgreSQL startup or credentials"
                );
            }
        });
        let config = BackendConfig::new_with_tls(
            "localhost".into(),
            port,
            "user".into(),
            "secret".into(),
            "db".into(),
            outbound(),
        )
        .unwrap();
        let backend = Arc::new(PostgresBackend::new(config).unwrap());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        assert!(
            timeout(
                Duration::from_secs(3),
                yasp::gateway::serve(
                    listener,
                    env.gateway().gateway_config(),
                    backend,
                    std::future::pending()
                )
            )
            .await
            .unwrap()
            .is_err()
        );
        timeout(Duration::from_secs(2), peer)
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_tls_cache_and_dataset_refresh() {
    use yasp::{
        cache::RedisCache,
        config::{ApplicationContextsSettings, CacheSettings, DatasetSettings, SelectorSettings},
        datasets::{Datasets, bind_dataset_loader},
    };
    let url = std::env::var("YASP_TEST_REDIS_TLS_URL").unwrap();
    let plain_url = std::env::var("YASP_TEST_REDIS_URL").unwrap();
    let namespace = support::RedisNamespace::new(&plain_url, "tls_datasets");
    let user = RedisUser(support::unique_name("tls_user"));
    redis_command(&[
        "ACL",
        "SETUSER",
        &user.0,
        "on",
        ">test-password",
        "~*",
        "+@all",
    ]);
    let address = match redis::Client::open(url.as_str())
        .unwrap()
        .get_connection_info()
        .addr()
    {
        redis::ConnectionAddr::TcpTls { host, port, .. } => format!("{host}:{port}"),
        _ => panic!("TLS Redis URL required"),
    };
    let dataset_url = format!("rediss://{}:test-password@{address}/", user.0);
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let loader = bind_dataset_loader(calls.clone(), |_, calls| {
        Box::pin(async move {
            let revision = calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(serde_json::Map::from_iter([(
                "key".into(),
                serde_json::json!(revision),
            )]))
        })
    });
    let dataset_settings = ApplicationContextsSettings {
        tls: outbound(),
        namespace: namespace.name.clone(),
        datasets: BTreeMap::from([(
            "test".into(),
            DatasetSettings {
                refresh_interval_seconds: 1,
                loader: SelectorSettings::Rust("test".into()),
            },
        )]),
        ..Default::default()
    };
    let datasets = Datasets::start(
        dataset_settings.clone(),
        &dataset_url,
        BTreeMap::from([("test".into(), loader.clone().into())]),
    )
    .await
    .unwrap();
    assert!(
        datasets
            .reader()
            .get("test", "key")
            .await
            .unwrap()
            .is_some()
    );
    // Kill only this test's authenticated connections; other suites keep their services.
    let killed = redis_command(&["CLIENT", "KILL", "USER", &user.0]);
    assert!(killed.trim().parse::<usize>().unwrap() >= 2);
    timeout(Duration::from_secs(3), async {
        loop {
            if datasets.reader().get("test", "key").await.is_ok() {
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("dataset reconnects with verified TLS and its client identity");
    timeout(Duration::from_secs(5), async {
        while calls.load(std::sync::atomic::Ordering::SeqCst) < 2 {
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("dataset refreshes through TLS");
    datasets.shutdown().await;

    let mut bad = dataset_settings;
    bad.tls.ca_file = Some(file("other-ca.crt"));
    assert!(
        Datasets::start(bad, &url, BTreeMap::from([("test".into(), loader.into())]))
            .await
            .is_err()
    );
    let cache_namespace = support::RedisNamespace::new(&plain_url, "tls_cache");
    let cache_settings = CacheSettings {
        enabled: true,
        tls: outbound(),
        namespace: cache_namespace.name.clone(),
        ..Default::default()
    };
    // Plaintext accepts remote endpoints but rejects certificate fields that would be ignored.
    assert!(RedisCache::new(cache_settings.clone(), "redis://remote.invalid/", None).is_err());
    let plain_settings = CacheSettings {
        tls: Default::default(),
        ..cache_settings.clone()
    };
    assert!(RedisCache::new(plain_settings, "redis://redis.internal:6379/", None).is_ok());
    assert!(RedisCache::new(cache_settings.clone(), &format!("{url}#insecure"), None).is_err());
    let cache = Arc::new(RedisCache::new(cache_settings, &url, None).unwrap());
    let env = support::TestEnvironment::load();
    let identity = env.gateway();
    let backend = Arc::new(PostgresBackend::new(backend(&env, outbound())).unwrap());
    let server = support::GatewayServer::fixed(
        identity
            .gateway_config()
            .with_settings(settings("server"))
            .unwrap()
            .with_cache(cache.clone()),
        backend,
    );
    let mut client = connect_client(server.address, &identity).await;
    query(&mut client, "SELECT 987654321").await;
    query(&mut client, "SELECT 987654321").await;
    // Inspect cache storage through the disposable local plaintext administrative connection.
    let output = std::process::Command::new("redis-cli")
        .args([
            "-u",
            &std::env::var("YASP_TEST_REDIS_URL").unwrap(),
            "--scan",
            "--pattern",
            &format!("{}:*", cache_namespace.name),
        ])
        .output()
        .unwrap();
    assert!(!output.stdout.is_empty(), "result was stored over TLS");
    drop(client);
    server.stop();
}
