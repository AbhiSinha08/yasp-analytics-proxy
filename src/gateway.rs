//! PostgreSQL frontend sessions, authentication, and responses.

use crate::config::GatewayConfig;
use async_trait::async_trait;
use futures::{Sink, SinkExt, StreamExt, stream};
use pgwire::{
    api::{
        ClientInfo, ClientPortalStore, DefaultClient, NoopHandler, PgWireConnectionState, Type,
        auth::{
            AuthSource, DefaultServerParameterProvider, LoginInfo, Password,
            sasl::{
                SASLAuthStartupHandler,
                scram::{SCRAM_ITERATIONS, ScramAuth, gen_salted_password, random_nonce},
            },
        },
        query::SimpleQueryHandler,
        results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response},
        store::PortalStore,
    },
    error::{ErrorInfo, PgWireError, PgWireResult},
    messages::{
        PgWireBackendMessage, PgWireFrontendMessage, SslNegotiationMetaMessage,
        response::{GssEncResponse, SslResponse},
    },
    tokio::server::{PgWireMessageServerCodec, process_error, process_message},
};
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
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpStream},
    task::JoinSet,
    time::{Instant, timeout, timeout_at},
};
use tokio_util::codec::Framed;

const MAX_SESSIONS: usize = 32;
const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;
const STARTUP_TIMEOUT: Duration = Duration::from_secs(60);
const WRITE_TIMEOUT: Duration = Duration::from_secs(30);
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

/// Serve on a host-owned loopback listener (port 0 is useful in tests).
/// Stop accepting on shutdown, then close remaining sessions after five seconds.
pub async fn serve(
    listener: TcpListener,
    config: GatewayConfig,
    shutdown: impl Future<Output = ()>,
) -> io::Result<()> {
    if !listener.local_addr()?.ip().is_loopback() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "gateway requires a loopback listener",
        ));
    }
    let salt = random_nonce().into_bytes();
    let password = Password::new(
        Some(salt.clone()),
        gen_salted_password(&config.password, &salt, SCRAM_ITERATIONS),
    );
    drop(config.password);
    let source = Arc::new(Credentials {
        username: config.username,
        database: config.database,
        password,
    });
    let mut sessions = JoinSet::new();
    tokio::pin!(shutdown);
    let outcome = loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => break Ok(()),
            result = sessions.join_next(), if !sessions.is_empty() => {
                if result.is_some_and(|r| r.is_err()) {
                    tracing::warn!("gateway session task failed");
                }
            }
            accepted = listener.accept() => {
                let (socket, peer) = match accepted {
                    Ok(accepted) => accepted,
                    Err(error) => break Err(error),
                };
                if sessions.len() >= MAX_SESSIONS {
                    tracing::warn!(%peer, "gateway session limit reached");
                    continue;
                }
                let source = source.clone();
                sessions.spawn(async move {
                    tracing::debug!(%peer, "gateway session opened");
                    if let Err(error) = connection(socket, source).await {
                        // Protocol errors can contain client data. Log only their category.
                        tracing::warn!(%peer, kind = ?error.kind(), "gateway connection failed");
                    }
                    tracing::debug!(%peer, "gateway session closed");
                });
            }
        }
    };
    drop(listener);
    tracing::info!("gateway shutting down");
    if timeout(SHUTDOWN_GRACE, async {
        while sessions.join_next().await.is_some() {}
    })
    .await
    .is_err()
    {
        sessions.abort_all();
        while sessions.join_next().await.is_some() {}
    }
    outcome
}

struct Credentials {
    username: String,
    database: String,
    password: Password,
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

struct HealthQuery;

#[async_trait]
impl SimpleQueryHandler for HealthQuery {
    async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
        C::Error: fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let sql = query.trim();
        let sql = sql.strip_suffix(';').unwrap_or(sql).trim();
        let mut words = sql.split_ascii_whitespace();
        if !words
            .next()
            .is_some_and(|s| s.eq_ignore_ascii_case("SELECT"))
            || words.next() != Some("1")
            || words.next().is_some()
        {
            return Err(protocol_error(
                "ERROR",
                "0A000",
                "only simple-query SELECT 1 is supported",
            ));
        }
        let schema = Arc::new(vec![
            FieldInfo::new("?column?".into(), None, None, Type::INT4, FieldFormat::Text)
                .with_type_size(4),
        ]);
        let mut encoder = DataRowEncoder::new(schema.clone());
        encoder.encode_field(&1_i32)?;
        Ok(vec![Response::Query(QueryResponse::new(
            schema,
            stream::iter([Ok(encoder.take_row())]),
        ))])
    }
}

async fn connection(socket: TcpStream, source: Arc<Credentials>) -> io::Result<()> {
    socket.set_nodelay(true)?;
    let client = DefaultClient::<String>::new(socket.peer_addr()?, false);
    let mut socket = Framed::new(
        FrameGuard::new(socket),
        PgWireMessageServerCodec::new(client),
    );
    let mut parameters = DefaultServerParameterProvider::default();
    parameters.is_superuser = false;
    parameters.default_transaction_read_only = true;
    // SASL state belongs to one session; only immutable salted credentials are shared.
    let auth = Arc::new(
        SASLAuthStartupHandler::new(Arc::new(parameters))
            .with_scram(ScramAuth::new(source.clone())),
    );
    let query = Arc::new(HealthQuery);
    let noop = Arc::new(NoopHandler);
    let deadline = Instant::now() + STARTUP_TIMEOUT;
    let mut authenticated = false;
    loop {
        let message = if authenticated {
            socket.next().await
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
        if matches!(
            message,
            PgWireFrontendMessage::Terminate(_) | PgWireFrontendMessage::CancelRequest(_)
        ) {
            return Ok(());
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
                        query.clone(),
                        noop.clone(),
                        noop.clone(),
                        noop.clone(),
                    )
                    .await
                }
            }
        };
        let response_deadline = if authenticated {
            Instant::now() + WRITE_TIMEOUT
        } else {
            deadline.min(Instant::now() + WRITE_TIMEOUT)
        };
        let result = timeout_at(response_deadline, operation)
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "response timed out"))?;
        if let Err(error) = result {
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

/// Check the length before delivering a frame header to pgwire. Never read ahead
/// into another frame: pipelined requests each pass through this same bound.
struct FrameGuard {
    socket: TcpStream,
    startup: bool,
    header: [u8; 8],
    read: usize,
    sent: usize,
    header_len: usize,
    remaining: usize,
}

impl FrameGuard {
    fn new(socket: TcpStream) -> Self {
        Self {
            socket,
            startup: true,
            header: [0; 8],
            read: 0,
            sent: 0,
            header_len: 8,
            remaining: 0,
        }
    }
}

impl AsyncRead for FrameGuard {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        while this.read < this.header_len {
            // Validate startup length after four bytes, before reading the code.
            let end = if this.startup && this.read < 4 {
                4
            } else {
                this.header_len
            };
            let mut header = ReadBuf::new(&mut this.header[this.read..end]);
            match Pin::new(&mut this.socket).poll_read(cx, &mut header) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(())) => {}
            }
            let n = header.filled().len();
            if n == 0 {
                return Poll::Ready(if this.read == 0 {
                    Ok(())
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "truncated frame header",
                    ))
                });
            }
            this.read += n;
            if (this.startup && this.read >= 4) || (!this.startup && this.read == 5) {
                let offset = usize::from(!this.startup);
                let length = u32::from_be_bytes(this.header[offset..offset + 4].try_into().unwrap())
                    as usize;
                let minimum = if this.startup { 8 } else { 4 };
                if length < minimum || length > MAX_FRAME_BYTES - offset {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "invalid frame length",
                    )));
                }
                this.remaining = length + offset - this.header_len;
            }
        }
        if this.sent < this.header_len {
            let n = (this.header_len - this.sent).min(buf.remaining());
            buf.put_slice(&this.header[this.sent..this.sent + n]);
            this.sent += n;
        } else if this.remaining > 0 {
            let n = this.remaining.min(buf.remaining());
            let mut payload = ReadBuf::new(buf.initialize_unfilled_to(n));
            match Pin::new(&mut this.socket).poll_read(cx, &mut payload) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(())) => {}
            }
            let n = payload.filled().len();
            if n == 0 {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "truncated frame",
                )));
            }
            buf.advance(n);
            this.remaining -= n;
        }
        if this.sent == this.header_len && this.remaining == 0 {
            if this.startup {
                let code = u32::from_be_bytes(this.header[4..8].try_into().unwrap());
                this.startup = matches!(code, 80877103 | 80877104);
            }
            this.header_len = if this.startup { 8 } else { 5 };
            this.read = 0;
            this.sent = 0;
        }
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for FrameGuard {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().socket).poll_write(cx, buf)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().socket).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().socket).poll_shutdown(cx)
    }
}
