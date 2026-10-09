//! PostgreSQL frontend sessions, authentication, and responses.

use crate::{
    backend::PostgresBackend,
    config::GatewayConfig,
    query::{ParsedQuery, QueryError},
    transport::FrameGuard,
};
use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use pgwire::{
    api::{
        ClientInfo, DefaultClient, NoopHandler, PgWireConnectionState,
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
        PgWireBackendMessage, PgWireFrontendMessage, SslNegotiationMetaMessage,
        response::{
            EmptyQueryResponse, GssEncResponse, ReadyForQuery, SslResponse, TransactionStatus,
        },
    },
    tokio::server::{PgWireMessageServerCodec, process_error, process_message},
};
use std::{fmt, future::Future, io, sync::Arc, time::Duration};
use tokio::{
    net::{TcpListener, TcpStream},
    task::JoinSet,
    time::{Instant, timeout, timeout_at},
};
use tokio_util::codec::Framed;

const MAX_SESSIONS: usize = 32;
const STARTUP_TIMEOUT: Duration = Duration::from_secs(120);
const WRITE_TIMEOUT: Duration = Duration::from_secs(60);
const SHUTDOWN_GRACE: Duration = Duration::from_secs(10);

/// Serve on a host-owned loopback listener (port 0 is useful in tests).
/// Stop accepting on shutdown, then close remaining sessions after ten seconds.
pub async fn serve(
    listener: TcpListener,
    config: GatewayConfig,
    backend: Arc<PostgresBackend>,
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
                let backend = backend.clone();
                sessions.spawn(async move {
                    tracing::debug!(%peer, "gateway session opened");
                    if let Err(error) = connection(socket, source, backend).await {
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
    backend.close();
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

async fn connection(
    socket: TcpStream,
    source: Arc<Credentials>,
    backend: Arc<PostgresBackend>,
) -> io::Result<()> {
    socket.set_nodelay(true)?;
    // A second descriptor observes disconnects without consuming protocol bytes.
    let raw_socket = socket.into_std()?;
    let observer = TcpStream::from_std(raw_socket.try_clone()?)?;
    let socket = TcpStream::from_std(raw_socket)?;
    let client = DefaultClient::<String>::new(socket.peer_addr()?, false);
    let mut socket = Framed::new(
        FrameGuard::frontend(socket),
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
            .with_scram(ScramAuth::new(source.clone())),
    );
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
        if authenticated
            && !matches!(socket.state(), PgWireConnectionState::AwaitingSync)
            && let PgWireFrontendMessage::Query(query) = &message
        {
            socket.set_state(PgWireConnectionState::QueryInProgress);
            let result = tokio::select! {
                result = execute_query(&mut socket, &backend, &query.query) => result,
                _ = disconnected(&observer) => return Ok(()),
            };
            if let Err(error) = result {
                let mut info: ErrorInfo = error.into();
                if info.is_fatal() {
                    return Err(io::Error::other("frontend response failed"));
                }
                info.severity = "ERROR".into();
                timeout(
                    WRITE_TIMEOUT,
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
                WRITE_TIMEOUT,
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

async fn execute_query(
    socket: &mut Framed<FrameGuard, PgWireMessageServerCodec<String>>,
    backend: &PostgresBackend,
    sql: &str,
) -> PgWireResult<()> {
    let query = match ParsedQuery::parse(sql) {
        Ok(query) => query,
        Err(QueryError::Empty) => {
            timeout(
                WRITE_TIMEOUT,
                socket.send(PgWireBackendMessage::EmptyQueryResponse(
                    EmptyQueryResponse::new(),
                )),
            )
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "empty response timed out"))??;
            return Ok(());
        }
        Err(error) => {
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
    backend.execute(&query, socket).await
}
