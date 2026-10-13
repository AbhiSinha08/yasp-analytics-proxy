//! PostgreSQL frontend sessions, authentication, and responses.

use crate::{
    backend::{PostgresBackend, PostgresBackends},
    cache::{CacheBuffer, RedisCache},
    config::{GatewayConfig, GatewaySettings},
    hooks::{ContextHook, ContextRequest, RoutedSelector, SelectionError, SelectionRequest},
    logger,
    query::{ParsedQuery, QueryError},
    transport::FrameGuard,
};
use async_trait::async_trait;
use futures::{Sink, SinkExt, StreamExt};
use pgwire::{
    api::{
        ClientInfo, ConnectionHandle, ConnectionManager, DefaultClient, NoopHandler,
        PgWireConnectionState,
        auth::{
            AuthSource, DefaultServerParameterProvider, LoginInfo, Password,
            sasl::{
                SASLAuthStartupHandler,
                scram::{SCRAM_ITERATIONS, ScramAuth, gen_salted_password, random_nonce},
            },
        },
    },
    error::{ErrorInfo, PgWireError, PgWireResult},
    messages::{
        DecodeContext, Message, PgWireBackendMessage, PgWireFrontendMessage,
        SslNegotiationMetaMessage,
        response::{
            EmptyQueryResponse, GssEncResponse, ReadyForQuery, SslResponse, TransactionStatus,
        },
    },
    tokio::server::{PgWireMessageServerCodec, process_error, process_message},
};
use sha2::{Digest, Sha256};
use std::{
    fmt,
    future::Future,
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::watch,
    task::JoinSet,
    time::{Instant, timeout, timeout_at},
};
use tokio_util::codec::Framed;

const MAX_CANCEL_CONNECTIONS: usize = 8;
const CANCEL_CONNECTION_TIMEOUT: Duration = Duration::from_secs(1);

/// Serve on a host-owned loopback listener (port 0 is useful in tests).
/// Stop accepting on shutdown, then close sessions after the configured drain timeout.
pub async fn serve(
    listener: TcpListener,
    config: GatewayConfig,
    backend: Arc<PostgresBackend>,
    shutdown: impl Future<Output = ()>,
) -> io::Result<()> {
    serve_sessions(listener, config, Execution::Fixed(backend), shutdown).await
}

/// Serve with an application-registered selector and configured route pools.
/// Decisions are checked on every query before a pool connection is acquired.
pub async fn serve_routed(
    listener: TcpListener,
    config: GatewayConfig,
    backends: Arc<PostgresBackends>,
    selector: impl Into<RoutedSelector>,
    shutdown: impl Future<Output = ()>,
) -> io::Result<()> {
    serve_sessions(
        listener,
        config,
        Execution::Routed {
            backends,
            selector: selector.into(),
        },
        shutdown,
    )
    .await
}

enum Execution {
    Fixed(Arc<PostgresBackend>),
    Routed {
        backends: Arc<PostgresBackends>,
        selector: RoutedSelector,
    },
}

impl Execution {
    async fn select(&self, request: &SelectionRequest<'_>) -> PgWireResult<&PostgresBackend> {
        match self {
            Self::Fixed(backend) => Ok(backend),
            Self::Routed { backends, selector } => {
                let route = selector
                    .select(request)
                    .await
                    .map_err(|error| match error {
                        SelectionError::Denied => {
                            protocol_error("ERROR", "42501", "backend selection denied")
                        }
                        SelectionError::Failed => {
                            protocol_error("ERROR", "XX000", "backend selection failed")
                        }
                    })?;
                let backend = backends.get(&route).ok_or_else(|| {
                    protocol_error("ERROR", "42501", "backend selection is not configured")
                })?;
                tracing::debug!(
                    query_id = request.query_id,
                    client_id = request.client_id,
                    target = %route.target,
                    backend_login = %route.backend_login,
                    "backend selected"
                );
                Ok(backend)
            }
        }
    }

    fn close(&self) {
        match self {
            Self::Fixed(backend) => backend.close(),
            Self::Routed { backends, selector } => {
                selector.shutdown();
                backends.close();
            }
        }
    }

    fn stop_hooks(&self) {
        if let Self::Routed { selector, .. } = self {
            selector.shutdown();
        }
    }
}

async fn serve_sessions(
    listener: TcpListener,
    config: GatewayConfig,
    execution: Execution,
    shutdown: impl Future<Output = ()>,
) -> io::Result<()> {
    if !listener.local_addr()?.ip().is_loopback() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "gateway requires a loopback listener",
        ));
    }
    let settings = Arc::new(config.settings);
    let execution = Arc::new(execution);
    let cancel_manager = Arc::new(ConnectionManager::new());
    let salt = random_nonce().into_bytes();
    let password = Password::new(
        Some(salt.clone()),
        gen_salted_password(&config.password, &salt, SCRAM_ITERATIONS),
    );
    drop(config.password);
    let cache = config.cache;
    let context_hook = config.context_hook;
    let (stopping, stopped) = watch::channel(false);
    let source = Arc::new(Credentials {
        username: config.username,
        database: config.database,
        password,
        cache: cache.clone(),
        context_hook: context_hook.clone(),
        stopped,
    });
    let mut sessions = JoinSet::new();
    tokio::pin!(shutdown);
    let outcome = loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => break Ok(()),
            result = sessions.join_next(), if !sessions.is_empty() => {
                if result.is_some_and(|r| r.is_err()) {
                    tracing::error!("gateway session task failed");
                }
            }
            accepted = listener.accept() => {
                let (socket, peer) = match accepted {
                    Ok(accepted) => accepted,
                    Err(error) => break Err(error),
                };
                let session_limit = settings.max_sessions;
                let cancel_only = sessions.len() >= session_limit;
                // CancelRequest uses a new connection, even when all client slots are occupied.
                if sessions.len() >= session_limit.saturating_add(MAX_CANCEL_CONNECTIONS) {
                    tracing::warn!(%peer, "gateway session limit reached");
                    continue;
                }
                let client_id = logger::next_client_id();
                let source = source.clone();
                let execution = execution.clone();
                let settings = settings.clone();
                let cancel_manager = cancel_manager.clone();
                sessions.spawn(async move {
                    tracing::info!(client_id, %peer, "client connected");
                    if let Err(error) = connection(
                        socket,
                        source,
                        execution,
                        settings,
                        cancel_manager,
                        cancel_only,
                        client_id,
                    )
                    .await
                    {
                        // Protocol errors can contain client data. Log only their category.
                        tracing::error!(client_id, %peer, kind = ?error.kind(), "client session failed");
                    }
                    tracing::info!(client_id, %peer, "client disconnected");
                });
            }
        }
    };
    drop(listener);
    tracing::info!("gateway shutting down");
    stopping.send_replace(true);
    if let Some(hook) = &context_hook {
        hook.shutdown();
    }
    execution.stop_hooks();
    if timeout(
        Duration::from_millis(u64::from(settings.shutdown_timeout_ms)),
        async { while sessions.join_next().await.is_some() {} },
    )
    .await
    .is_err()
    {
        sessions.abort_all();
        while sessions.join_next().await.is_some() {}
    }
    execution.close();
    if let Some(cache) = cache {
        cache.close().await;
    }
    outcome
}

struct Credentials {
    username: String,
    database: String,
    password: Password,
    cache: Option<Arc<RedisCache>>,
    context_hook: Option<Arc<ContextHook>>,
    stopped: watch::Receiver<bool>,
}

impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Credentials").finish_non_exhaustive()
    }
}

#[async_trait]
impl AuthSource for Credentials {
    async fn get_password(&self, login: &LoginInfo) -> PgWireResult<Password> {
        if login.user() != Some(self.username.as_str()) {
            return Err(protocol_error("FATAL", "28P01", "authentication failed"));
        }
        Ok(self.password.clone())
    }
}

fn protocol_error(severity: &str, code: &str, message: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        severity.into(),
        code.into(),
        message.into(),
    )))
}

async fn connection(
    socket: TcpStream,
    source: Arc<Credentials>,
    execution: Arc<Execution>,
    settings: Arc<GatewaySettings>,
    cancel_manager: Arc<ConnectionManager>,
    cancel_only: bool,
    client_id: u64,
) -> io::Result<()> {
    let write_timeout = settings.write_timeout();
    socket.set_nodelay(true)?;
    // A second descriptor observes disconnects without consuming protocol bytes.
    let raw_socket = socket.into_std()?;
    let observer = TcpStream::from_std(raw_socket.try_clone()?)?;
    let socket = TcpStream::from_std(raw_socket)?;
    let client = DefaultClient::<String>::new(socket.peer_addr()?, false);
    let mut socket = Framed::new(
        FrameGuard::frontend(socket, settings.max_message_bytes),
        PgWireMessageServerCodec::new(client),
    );
    let mut parameters = DefaultServerParameterProvider::default();
    parameters.is_superuser = false;
    parameters.default_transaction_read_only = true;
    parameters.date_style = "ISO, MDY".into();
    parameters.time_zone = "UTC".into();
    // SASL state belongs to one session; only immutable salted credentials are shared.
    let auth = Arc::new(
        SASLAuthStartupHandler::new(Arc::new(parameters))
            .with_scram(ScramAuth::new(source.clone()))
            .with_connection_manager(cancel_manager.clone()),
    );
    let noop = Arc::new(NoopHandler);
    let startup_timeout = if cancel_only {
        CANCEL_CONNECTION_TIMEOUT
    } else {
        Duration::from_millis(u64::from(settings.startup_timeout_ms))
    };
    let deadline = Instant::now() + startup_timeout;
    let read_timeout = Duration::from_millis(u64::from(settings.read_timeout_ms));
    let mut authenticated = false;
    loop {
        let message = if authenticated {
            timeout(read_timeout, socket.next())
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "frontend read timed out"))?
        } else {
            timeout_at(deadline, socket.next())
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "startup timed out"))?
        };
        let Some(message) = message else {
            return Ok(());
        };
        let message = message
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid frontend frame"))?;
        if matches!(&message, PgWireFrontendMessage::Terminate(_)) {
            return Ok(());
        }
        if let PgWireFrontendMessage::CancelRequest(cancel) = &message {
            cancel_manager.cancel(cancel.pid, &cancel.secret_key).await;
            return Ok(());
        }
        if cancel_only
            && !matches!(
                &message,
                PgWireFrontendMessage::SslNegotiation(SslNegotiationMetaMessage::None)
            )
        {
            return Ok(());
        }
        if authenticated
            && !matches!(socket.state(), PgWireConnectionState::AwaitingSync)
            && let PgWireFrontendMessage::Query(query) = &message
        {
            socket.set_state(PgWireConnectionState::QueryInProgress);
            let cancel = socket
                .session_extensions()
                .get::<Arc<ConnectionHandle>>()
                .expect("authenticated session has a registered cancellation handle")
                .start_query()
                .await;
            let result = tokio::select! {
                result = execute_query(&mut socket, &execution, &query.query, &settings, client_id, &source, cancel) => result,
                _ = disconnected(&observer) => return Ok(()),
            };
            if let Err(error) = result {
                let mut info: ErrorInfo = error.into();
                if info.is_fatal() {
                    return Err(io::Error::other("frontend response failed"));
                }
                info.severity = "ERROR".into();
                timeout(
                    write_timeout,
                    socket.send(PgWireBackendMessage::ErrorResponse(info.into())),
                )
                .await
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::TimedOut, "error response timed out")
                })??;
            }
            socket.set_state(PgWireConnectionState::ReadyForQuery);
            socket.set_transaction_status(TransactionStatus::Idle);
            timeout(
                write_timeout,
                socket.send(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                    TransactionStatus::Idle,
                ))),
            )
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "ready response timed out"))??;
            continue;
        }
        let extended = message.is_extended_query();
        let operation = async {
            match message {
                PgWireFrontendMessage::SslNegotiation(negotiation) => {
                    match negotiation {
                        SslNegotiationMetaMessage::PostgresSsl(_) => {
                            socket
                                .send(PgWireBackendMessage::SslResponse(SslResponse::Refuse))
                                .await?
                        }
                        SslNegotiationMetaMessage::PostgresGss(_) => {
                            socket
                                .send(PgWireBackendMessage::GssEncResponse(GssEncResponse::Refuse))
                                .await?
                        }
                        SslNegotiationMetaMessage::None => {
                            socket.set_state(PgWireConnectionState::AwaitingStartup)
                        }
                    }
                    Ok(())
                }
                message => {
                    if let PgWireFrontendMessage::Startup(startup) = &message {
                        if startup.parameters.get("user") != Some(&source.username) {
                            return Err(protocol_error("FATAL", "28P01", "authentication failed"));
                        }
                        if startup.parameters.get("database") != Some(&source.database) {
                            return Err(protocol_error(
                                "FATAL",
                                "3D000",
                                "unknown gateway database",
                            ));
                        }
                        if startup.parameters.contains_key("replication")
                            || startup.parameters.contains_key("options")
                        {
                            return Err(protocol_error(
                                "FATAL",
                                "0A000",
                                "startup options and replication are unsupported",
                            ));
                        }
                    }
                    if !matches!(socket.state(), PgWireConnectionState::AwaitingSync) {
                        let supported = if authenticated {
                            matches!(
                                message,
                                PgWireFrontendMessage::Query(_)
                                    | PgWireFrontendMessage::Sync(_)
                                    | PgWireFrontendMessage::Flush(_)
                            )
                        } else {
                            matches!(
                                message,
                                PgWireFrontendMessage::Startup(_)
                                    | PgWireFrontendMessage::PasswordMessageFamily(_)
                            )
                        };
                        if !supported {
                            return Err(protocol_error(
                                if authenticated { "ERROR" } else { "FATAL" },
                                "0A000",
                                "frontend operation is unsupported",
                            ));
                        }
                    }
                    process_message(
                        message,
                        &mut socket,
                        auth.clone(),
                        noop.clone(),
                        noop.clone(),
                        noop.clone(),
                        noop.clone(),
                    )
                    .await
                }
            }
        };
        let response_deadline = if authenticated {
            Instant::now() + write_timeout
        } else {
            deadline.min(Instant::now() + write_timeout)
        };
        let result = timeout_at(response_deadline, operation)
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "response timed out"))?;
        if let Err(error) = result {
            tracing::error!(client_id, "client protocol operation failed");
            let mut info: ErrorInfo = error.into();
            if !authenticated {
                info.severity = "FATAL".into();
            }
            let fatal = info.is_fatal();
            if fatal {
                // Startup failures end the connection without ReadyForQuery.
                timeout_at(
                    response_deadline,
                    socket.send(PgWireBackendMessage::ErrorResponse(info.into())),
                )
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "error response timed out"))?
                .map_err(|_| io::Error::other("error response failed"))?;
                return Ok(());
            }
            timeout_at(
                response_deadline,
                process_error(
                    &mut socket,
                    PgWireError::UserError(Box::new(info)),
                    extended,
                ),
            )
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "error response timed out"))??;
        }
        authenticated |= matches!(socket.state(), PgWireConnectionState::ReadyForQuery);
    }
}

async fn disconnected(socket: &TcpStream) {
    let mut byte = [0];
    match socket.peek(&mut byte).await {
        Ok(0) | Err(_) => {}
        Ok(_) if byte[0] == b'X' => {}
        // Pipelined requests remain in the protocol reader. Query deadlines
        // bound their wait; this observer never consumes another request.
        Ok(_) => std::future::pending::<()>().await,
    }
}

#[allow(clippy::too_many_arguments)]
async fn execute_query(
    socket: &mut Framed<FrameGuard, PgWireMessageServerCodec<String>>,
    execution: &Execution,
    sql: &str,
    settings: &GatewaySettings,
    client_id: u64,
    source: &Credentials,
    mut cancel: futures::channel::oneshot::Receiver<()>,
) -> PgWireResult<()> {
    let authenticated_user = source.username.as_str();
    let frontend_database = source.database.as_str();
    let cache = source.cache.as_deref();
    let write_timeout = settings.write_timeout();
    let query = match ParsedQuery::parse_with_limit(sql, settings.max_sql_bytes) {
        Ok(query) => query,
        Err(QueryError::Empty) => {
            timeout(
                write_timeout,
                socket.send(PgWireBackendMessage::EmptyQueryResponse(
                    EmptyQueryResponse::new(),
                )),
            )
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "empty response timed out"))??;
            return Ok(());
        }
        Err(error) => {
            tracing::error!(client_id, "query parsing failed");
            return Err(protocol_error(
                "ERROR",
                match error {
                    QueryError::Syntax => "42601",
                    QueryError::TooLarge | QueryError::TooComplex => "54000",
                    _ => "0A000",
                },
                &error.to_string(),
            ));
        }
    };
    let query_id = query.query_id();
    tracing::debug!(query_id, client_id, sql = ?query.sql(), "query received");
    let result = async {
        query
            .validate_read_only()
            .map_err(|error| protocol_error("ERROR", "0A000", &error.to_string()))?;
        let context = if let Some(hook) = &source.context_hook {
            let mut stopped = source.stopped.clone();
            let context_request = ContextRequest {
                authenticated_user,
                frontend_database,
                client_id,
                query_id,
                query: &query,
            };
            tokio::select! {
                biased;
                _ = &mut cancel => return Err(protocol_error("ERROR", "57014", "query cancelled")),
                _ = stopped.wait_for(|stopped| *stopped) => return Err(protocol_error("ERROR", "57014", "query cancelled")),
                context = hook.derive(context_request, Duration::from_millis(u64::from(settings.context_timeout_ms)), settings.max_context_bytes) => context,
            }
        } else {
            serde_json::Map::new()
        };
        let request = SelectionRequest {
            authenticated_user,
            client_id,
            query_id,
            query: &query,
            context: &context,
        };
        let backend = tokio::select! {
            biased;
            _ = &mut cancel => return Err(protocol_error("ERROR", "57014", "query cancelled")),
            result = execution.select(&request) => result?,
        };
        let mut buffer = None;
        if let Some(cache) = cache {
            let eligible = tokio::select! {
                biased;
                _ = &mut cancel => return Err(protocol_error("ERROR", "57014", "query cancelled")),
                eligible = cache.eligible(&request) => eligible,
            };
            if eligible {
                let scope = cache_scope(backend, authenticated_user, frontend_database, settings);
                buffer = cache.reserve(&scope);
                if let Some(entry) = &mut buffer {
                    let hit = tokio::select! {
                        biased;
                        _ = &mut cancel => return Err(protocol_error("ERROR", "57014", "query cancelled")),
                        hit = cache.get(query_id, &scope, entry) => hit,
                    };
                    if hit && valid_cached_response(&entry.bytes, cache.max_rows_per_query()) {
                        tracing::debug!(query_id, client_id, database = ?backend.target(), database_user = %backend.database_user(), sql = ?query.sql(), "query served from cache");
                        while !entry.bytes.is_empty() {
                            // Finish each frame before observing cancellation, so a
                            // cancel cannot leave a partially written protocol frame.
                            if cancel.try_recv().ok().flatten().is_some() {
                                return Err(protocol_error("ERROR", "57014", "query cancelled"));
                            }
                            let message = PgWireBackendMessage::decode(&mut entry.bytes, &DecodeContext::default())?
                                .expect("validated complete cache frame");
                            timeout(write_timeout, socket.send(message)).await
                                .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "frontend write timed out"))??;
                        }
                        return Ok(());
                    }
                    if hit {
                        // A corrupt payload must not be extended with a fresh response.
                        drop(buffer.take());
                        buffer = cache.reserve(&scope);
                    }
                }
                tracing::debug!(query_id, client_id, "cache miss or buffer limit reached");
            }
        }
        tracing::debug!(
            query_id,
            client_id,
            database = ?backend.target(),
            database_user = %backend.database_user(),
            sql = ?query.sql(),
            "query sent to backend"
        );
        let mut output = CaptureSink { inner: socket, buffer, max_entry_bytes: cache.map_or(0, RedisCache::max_entry_bytes), max_rows_per_query: cache.map_or(0, RedisCache::max_rows_per_query), rows: 0 };
        let cacheable_completion = backend
            .execute_with_cancel(
                &query,
                &mut output,
                settings.query_timeout(),
                write_timeout,
                cancel,
            )
            .await?;
        if cacheable_completion && let (Some(cache), Some(buffer)) = (cache, &output.buffer) {
            cache.put(query_id, buffer).await;
        }
        Ok(())
    }.await;
    if result.is_err() {
        tracing::error!(query_id, client_id, "query failed");
    }
    result
}

fn cache_scope(
    backend: &PostgresBackend,
    user: &str,
    database: &str,
    settings: &GatewaySettings,
) -> [u8; 32] {
    let mut hash = Sha256::new();
    for value in [
        "postgresql:text:v1",
        user,
        database,
        backend.target(),
        backend.database_user(),
        crate::backend::SESSION_DEFAULTS,
    ] {
        hash.update((value.len() as u64).to_be_bytes());
        hash.update(value.as_bytes());
    }
    hash.update(settings.query_timeout_ms.to_be_bytes());
    hash.update(settings.write_timeout_ms.to_be_bytes());
    hash.finalize().into()
}

struct CaptureSink<'a, S> {
    inner: &'a mut S,
    buffer: Option<CacheBuffer>,
    max_entry_bytes: usize,
    max_rows_per_query: usize,
    rows: usize,
}

impl<S: Sink<PgWireBackendMessage> + Unpin> Sink<PgWireBackendMessage> for CaptureSink<'_, S> {
    type Error = S::Error;
    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut *self.get_mut().inner).poll_ready(cx)
    }
    fn start_send(self: Pin<&mut Self>, message: PgWireBackendMessage) -> Result<(), Self::Error> {
        let this = self.get_mut();
        if this.buffer.is_some() && matches!(&message, PgWireBackendMessage::DataRow(_)) {
            this.rows += 1;
            if this.rows > this.max_rows_per_query {
                this.buffer = None;
            }
        }
        if let Some(buffer) = &mut this.buffer {
            let size = match &message {
                PgWireBackendMessage::RowDescription(m) => Some(m.message_length() + 1),
                PgWireBackendMessage::DataRow(m) => Some(m.message_length() + 1),
                PgWireBackendMessage::NoticeResponse(m) => Some(m.message_length() + 1),
                PgWireBackendMessage::CommandComplete(m) => Some(m.message_length() + 1),
                _ => None,
            };
            if size
                .is_none_or(|size| size > this.max_entry_bytes.saturating_sub(buffer.bytes.len()))
                || message.encode(&mut buffer.bytes).is_err()
            {
                this.buffer = None;
            }
        }
        Pin::new(&mut *this.inner).start_send(message)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut *self.get_mut().inner).poll_flush(cx)
    }
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut *self.get_mut().inner).poll_close(cx)
    }
}

// Validate raw frames before invoking pgwire's decoders, which assume their
// field bodies are complete. A malformed cache entry is a miss, never a panic
// or a partial response followed by a database fallback.
fn valid_cached_response(mut bytes: &[u8], max_rows: usize) -> bool {
    let mut columns = None;
    let mut rows = 0usize;
    while bytes.len() >= 5 {
        let kind = bytes[0];
        let length = i32::from_be_bytes(bytes[1..5].try_into().unwrap());
        let native_limit = match kind {
            b'T' => pgwire::messages::data::RowDescription::max_message_length(),
            b'D' => pgwire::messages::data::DataRow::max_message_length(),
            b'N' => pgwire::messages::response::NoticeResponse::max_message_length(),
            b'C' => pgwire::messages::response::CommandComplete::max_message_length(),
            _ => return false,
        };
        if length as usize > native_limit {
            return false;
        }
        if length < 4 || length as usize > 8 * 1024 * 1024 || length as usize >= bytes.len() {
            return false;
        }
        let (frame, rest) = bytes.split_at(length as usize + 1);
        let mut body = &frame[5..];
        match kind {
            b'T' if columns.is_none() && body.len() >= 2 => {
                let count = i16::from_be_bytes(body[..2].try_into().unwrap());
                if count < 0 {
                    return false;
                }
                body = &body[2..];
                for _ in 0..count {
                    if cstring(&mut body).is_none() || body.len() < 18 {
                        return false;
                    }
                    if body[16..18] != [0, 0] {
                        return false;
                    }
                    body = &body[18..];
                }
                if !body.is_empty() {
                    return false;
                }
                columns = Some(count);
            }
            b'D' if body.len() >= 2 => {
                let count = i16::from_be_bytes(body[..2].try_into().unwrap());
                if columns != Some(count)
                    || crate::backend::validate_text_fields(count, &body[2..]).is_err()
                {
                    return false;
                }
                rows += 1;
                if rows > max_rows {
                    return false;
                }
            }
            b'N' => {
                while body.first().is_some_and(|&byte| byte != 0) {
                    body = &body[1..];
                    if cstring(&mut body).is_none() {
                        return false;
                    }
                }
                if body != [0] {
                    return false;
                }
            }
            b'C' if columns.is_some() => {
                let tag = cstring(&mut body);
                return rest.is_empty()
                    && body.is_empty()
                    && tag.is_some_and(|tag| {
                        tag == "SHOW"
                            || tag
                                .strip_prefix("SELECT ")
                                .and_then(|count| count.parse::<usize>().ok())
                                == Some(rows)
                    });
            }
            _ => return false,
        }
        bytes = rest;
    }
    false
}

fn cstring<'a>(bytes: &mut &'a [u8]) -> Option<&'a str> {
    let end = bytes.iter().position(|&byte| byte == 0)?;
    let text = std::str::from_utf8(&bytes[..end]).ok()?;
    *bytes = &bytes[end + 1..];
    Some(text)
}
