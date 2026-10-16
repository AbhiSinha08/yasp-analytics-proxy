//! PostgreSQL startup, authentication, message ordering, and session recovery.

use super::{
    FrontendSocket, protocol_error, query::QuerySession, routing::Execution, server::Credentials,
};
use crate::{
    config::GatewaySettings, discovery::SessionSettings, query::ParsedQuery, transport::FrameGuard,
};
use futures::{SinkExt, StreamExt};
use pgwire::{
    api::{
        ClientInfo, ConnectionHandle, ConnectionManager, DefaultClient, NoopHandler,
        PgWireConnectionState,
        auth::{
            DefaultServerParameterProvider,
            sasl::{SASLAuthStartupHandler, scram::ScramAuth},
        },
    },
    error::{ErrorInfo, PgWireError, PgWireResult},
    messages::{
        PgWireBackendMessage, PgWireFrontendMessage, SslNegotiationMetaMessage,
        response::{GssEncResponse, ReadyForQuery, SslResponse, TransactionStatus},
        startup::Startup,
    },
    tokio::server::{PgWireMessageServerCodec, process_error, process_message},
};
use std::{io, sync::Arc, time::Duration};
use tokio::{
    net::TcpStream,
    time::{Instant, timeout, timeout_at},
};
use tokio_util::codec::Framed;

const CANCEL_CONNECTION_TIMEOUT: Duration = Duration::from_secs(1);

pub(super) async fn connection(
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
    parameters.server_version = source.server_version.clone();
    parameters.search_path = SessionSettings::default().search_path;
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
    let mut session = SessionSettings::default();
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
            let mut query_session = QuerySession {
                execution: &execution,
                settings: &settings,
                client_id,
                source: &source,
                session: &mut session,
            };
            let result = tokio::select! {
                result = query_session.execute(&mut socket, &query.query, cancel) => result,
                _ = disconnected(&observer) => return Ok(()),
            };
            finish_query(&mut socket, result, write_timeout).await?;
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
                        apply_startup(startup, &source, &mut session)?;
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

/// Validate frontend identity before accepting any supported setup parameters.
fn apply_startup(
    startup: &Startup,
    source: &Credentials,
    session: &mut SessionSettings,
) -> PgWireResult<()> {
    if startup.parameters.get("user") != Some(&source.username) {
        return Err(protocol_error("FATAL", "28P01", "authentication failed"));
    }
    if startup.parameters.get("database") != Some(&source.database) {
        return Err(protocol_error("FATAL", "3D000", "unknown gateway database"));
    }
    if startup.parameters.contains_key("replication") || startup.parameters.contains_key("options")
    {
        return Err(protocol_error(
            "FATAL",
            "0A000",
            "startup options and replication are unsupported",
        ));
    }
    for setting in ["application_name", "search_path", "client_encoding"] {
        if let Some(value) = startup.parameters.get(setting) {
            if setting == "search_path" {
                session.set_search_path(value).map_err(|_| {
                    protocol_error("FATAL", "0A000", "unsupported startup search path")
                })?;
                continue;
            }
            let sql = format!("SET {setting} = '{}'", value.replace('\'', "''"));
            let parsed = ParsedQuery::parse(&sql)
                .map_err(|_| protocol_error("FATAL", "0A000", "unsupported startup setting"))?;
            session
                .handle(parsed.statement())
                .map_err(|_| protocol_error("FATAL", "0A000", "unsupported startup setting"))?;
        }
    }
    Ok(())
}

/// Send a simple-query error when needed, then the final idle ReadyForQuery frame.
async fn finish_query(
    socket: &mut FrontendSocket,
    result: PgWireResult<()>,
    write_timeout: Duration,
) -> io::Result<()> {
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
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "error response timed out"))??;
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
    Ok(())
}
