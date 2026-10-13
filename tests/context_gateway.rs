//! Exercise context cancellation and fallback through a fixed-backend gateway.

#[cfg(unix)]
#[tokio::test]
async fn fixed_gateway_context_lifecycle() {
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
        collections::HashMap,
        path::Path,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };
    use tokio::{net::TcpStream, sync::Notify, time::timeout};
    use tokio_util::codec::Framed;
    use yasp::{
        backend::PostgresBackend,
        config::{BackendConfig, FileConfig, GatewayConfig, GatewaySettings},
        gateway::serve,
        hooks::{ContextError, bind_context},
    };

    struct PendingContext {
        entered: Arc<Notify>,
        completed: Arc<AtomicBool>,
        dropped: Arc<AtomicBool>,
    }

    struct CompletionGuard {
        completed: Arc<AtomicBool>,
        dropped: Arc<AtomicBool>,
    }

    impl Drop for CompletionGuard {
        fn drop(&mut self) {
            if !self.completed.load(Ordering::SeqCst) {
                self.dropped.store(true, Ordering::SeqCst);
            }
        }
    }

    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut dotenv = HashMap::new();
    if let Ok(items) = dotenvy::from_path_iter(root.join(".env")) {
        for item in items {
            let (key, value) = item.expect("test dotenv is valid");
            dotenv.entry(key).or_insert(value);
        }
    }
    let value = |key: &str| {
        std::env::var(key)
            .ok()
            .or_else(|| dotenv.get(key).cloned())
            .unwrap_or_else(|| panic!("required test environment value is missing: {key}"))
    };
    let file = FileConfig::read(&root.join("tests/config.yml"))
        .expect("test backend configuration is valid");
    let route = file.default_backend().expect("test default route exists");
    let target = &file.targets[&route.target];
    let login = &target.logins[&route.backend_login];
    let gateway_user = value("YASP_GATEWAY_USERNAME");
    let gateway_password = value("YASP_GATEWAY_PASSWORD");
    let gateway_database = value("YASP_GATEWAY_DATABASE");
    let backend = Arc::new(
        PostgresBackend::new(
            BackendConfig::new(
                target.host.clone(),
                target.port,
                login.username.clone(),
                value(&login.password_env),
                target.database.clone(),
            )
            .expect("configured PostgreSQL backend is valid"),
        )
        .expect("configured PostgreSQL backend initializes"),
    );

    let pending = Arc::new(PendingContext {
        entered: Arc::new(Notify::new()),
        completed: Arc::new(AtomicBool::new(false)),
        dropped: Arc::new(AtomicBool::new(false)),
    });
    let disconnected = Arc::new(PendingContext {
        entered: Arc::new(Notify::new()),
        completed: Arc::new(AtomicBool::new(false)),
        dropped: Arc::new(AtomicBool::new(false)),
    });
    let shutting_down = Arc::new(PendingContext {
        entered: Arc::new(Notify::new()),
        completed: Arc::new(AtomicBool::new(false)),
        dropped: Arc::new(AtomicBool::new(false)),
    });
    let requests = Arc::new(Mutex::new(Vec::new()));
    let hook = bind_context(
        (
            pending.clone(),
            disconnected.clone(),
            shutting_down.clone(),
            requests.clone(),
        ),
        |request, state| {
            let sql = request.query.sql().to_owned();
            let (pending, completion) = if sql.contains("context_cancel") {
                (true, state.0.clone())
            } else if sql.contains("context_disconnect") {
                (true, state.1.clone())
            } else if sql.contains("context_shutdown") {
                (true, state.2.clone())
            } else {
                (false, state.0.clone())
            };
            state.3.lock().unwrap().push(sql.clone());
            Box::pin(async move {
                if pending {
                    let _guard = CompletionGuard {
                        completed: completion.completed.clone(),
                        dropped: completion.dropped.clone(),
                    };
                    completion.entered.notify_one();
                    std::future::pending::<()>().await;
                    completion.completed.store(true, Ordering::SeqCst);
                }
                if sql.contains("context_failed") {
                    Err(ContextError::Failed)
                } else {
                    Ok(serde_json::Map::new())
                }
            })
        },
    );
    let gateway = GatewayConfig::new(
        gateway_user.clone(),
        gateway_password.clone(),
        gateway_database.clone(),
    )
    .expect("frontend identity is valid")
    .with_settings(GatewaySettings {
        context_timeout_ms: 30_000,
        shutdown_timeout_ms: 100,
        ..GatewaySettings::default()
    })
    .expect("test gateway limits are valid")
    .with_context_hook(hook);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("ephemeral gateway listener binds");
    let address = listener.local_addr().expect("listener address exists");
    let (shutdown, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(serve(listener, gateway, backend, async move {
        let _ = stopped.await;
    }));

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
        PgWireClient::connect(Arc::new(config), DefaultStartupHandler::new(), None)
            .await
            .expect("frontend authenticates with the gateway")
    }

    fn code(error: PgWireClientError) -> String {
        match error {
            PgWireClientError::RemoteError(info) => info.code,
            other => panic!("unexpected frontend error: {other:?}"),
        }
    }

    async fn query_value(client: &mut PgWireClient, sql: &str) -> String {
        let mut results = client
            .simple_query(DefaultSimpleQueryHandler::new(), sql)
            .await
            .expect("query succeeds")
            .remove(0)
            .into_data_rows_reader();
        results
            .next_row()
            .expect("query has a row")
            .next_value::<String>()
            .expect("query has a column")
            .expect("column is not NULL")
    }

    async fn wait_entered(pending: &PendingContext) {
        timeout(Duration::from_secs(3), pending.entered.notified())
            .await
            .expect("context callback starts");
    }

    async fn send_cancel(
        address: std::net::SocketAddr,
        pid: i32,
        key: pgwire::messages::startup::SecretKey,
    ) {
        let socket = TcpStream::connect(address)
            .await
            .expect("cancel connection reaches gateway");
        Framed::new(socket, PgWireMessageClientCodec::default())
            .send(PgWireFrontendMessage::CancelRequest(CancelRequest::new(
                pid, key,
            )))
            .await
            .expect("CancelRequest is sent");
    }

    let mut client = connect(address, &gateway_user, &gateway_password, &gateway_database).await;
    assert_eq!(
        query_value(&mut client, "SELECT 11 /* context_first */").await,
        "11"
    );
    assert_eq!(
        query_value(&mut client, "SELECT 12 /* context_failed */").await,
        "12",
        "context failure falls back to the empty context and query execution continues"
    );
    assert_eq!(
        requests.lock().unwrap().as_slice(),
        [
            "SELECT 11 /* context_first */",
            "SELECT 12 /* context_failed */"
        ]
    );

    let pid = client.process_id();
    let secret = client.secret_key().clone();
    let mut query = Box::pin(client.simple_query(
        DefaultSimpleQueryHandler::new(),
        "SELECT 13 /* context_cancel */",
    ));
    tokio::select! {
        result = &mut query => panic!("context finished before cancellation: {result:?}"),
        _ = wait_entered(&pending) => {}
    }
    send_cancel(address, pid, secret).await;
    assert_eq!(
        code(query.await.expect_err("cancelled context query fails")),
        "57014"
    );
    assert!(!pending.completed.load(Ordering::SeqCst));
    assert!(pending.dropped.load(Ordering::SeqCst));
    assert_eq!(query_value(&mut client, "SELECT 1").await, "1");

    let disconnected_query = {
        let username = gateway_user.clone();
        let password = gateway_password.clone();
        let database = gateway_database.clone();
        tokio::spawn(async move {
            let mut disconnected_client = connect(address, &username, &password, &database).await;
            disconnected_client
                .simple_query(
                    DefaultSimpleQueryHandler::new(),
                    "SELECT 14 /* context_disconnect */",
                )
                .await
        })
    };
    wait_entered(&disconnected).await;
    disconnected_query.abort();
    let _ = disconnected_query.await;
    timeout(Duration::from_secs(3), async {
        while !disconnected.dropped.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("disconnection drops the pending Rust context");
    assert!(!disconnected.completed.load(Ordering::SeqCst));

    let shutdown_query = {
        let username = gateway_user.clone();
        let password = gateway_password.clone();
        let database = gateway_database.clone();
        tokio::spawn(async move {
            let mut shutdown_client = connect(address, &username, &password, &database).await;
            shutdown_client
                .simple_query(
                    DefaultSimpleQueryHandler::new(),
                    "SELECT 15 /* context_shutdown */",
                )
                .await
        })
    };
    wait_entered(&shutting_down).await;
    shutdown.send(()).expect("gateway shutdown signal is sent");
    timeout(Duration::from_secs(3), server)
        .await
        .expect("gateway shutdown completes")
        .expect("gateway task joins")
        .expect("gateway shuts down cleanly");
    assert!(!shutting_down.completed.load(Ordering::SeqCst));
    assert!(shutting_down.dropped.load(Ordering::SeqCst));
    shutdown_query.abort();
    let _ = shutdown_query.await;
}
