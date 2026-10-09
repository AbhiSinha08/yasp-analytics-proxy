//! Concrete PostgreSQL execution: bounded pooled connections and native text
//! responses. Query syntax lives in `query`; organization policy lives outside it.

use crate::{config::BackendConfig, query::ParsedQuery, transport::FrameGuard};
use deadpool::{
    Runtime,
    managed::{Manager, Metrics, Object, Pool, RecycleError, RecycleResult, Timeouts},
};
use futures::{Sink, SinkExt, Stream, StreamExt};
use pgwire::{
    api::client::{
        ClientInfo, Config, ReadyState, ServerInformation,
        auth::{DefaultStartupHandler, StartupHandler},
    },
    error::{ErrorInfo, PgWireClientError, PgWireError, PgWireResult},
    messages::{
        PgWireBackendMessage, PgWireFrontendMessage, ProtocolVersion, data::DataRow,
        response::TransactionStatus, simplequery::Query, startup::SecretKey,
    },
    tokio::client::PgWireMessageClientCodec,
};
use std::{
    collections::BTreeMap,
    fmt, io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    net::TcpStream,
    time::{Instant, timeout},
};
use tokio_util::codec::Framed;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const ACQUIRE_TIMEOUT: Duration = Duration::from_secs(10);
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(10);
const SESSION_DEFAULTS: &str = "SET client_encoding = 'UTF8'; SET standard_conforming_strings = on; SET DateStyle = 'ISO, MDY'; SET TimeZone = 'UTC'; SET IntervalStyle = 'postgres'; SET bytea_output = 'hex'; SET default_transaction_read_only = on; SET client_connection_check_interval = '1s'";

/// One target/login pool. No organization routing or callback policy is embedded.
pub struct PostgresBackend {
    pool: Pool<PgManager>,
}

impl PostgresBackend {
    pub fn new(config: BackendConfig) -> io::Result<Self> {
        let mut wire = Config::new();
        wire.user(config.username)
            .password(config.password)
            .dbname(config.database);
        wire.protocol_version(ProtocolVersion::PROTOCOL3_0);
        let manager = PgManager {
            host: config.host,
            port: config.port,
            config: Arc::new(wire),
        };
        let pool = Pool::builder(manager)
            .max_size(8)
            .runtime(Runtime::Tokio1)
            .timeouts(Timeouts {
                wait: Some(ACQUIRE_TIMEOUT),
                create: Some(CONNECT_TIMEOUT),
                recycle: Some(CLEANUP_TIMEOUT),
            })
            .build()
            .map_err(|_| io::Error::other("cannot construct PostgreSQL pool"))?;
        Ok(Self { pool })
    }

    /// Execute the already parsed statement. Native PostgreSQL messages stay
    /// inside the PostgreSQL adapter, preserving opaque values and type metadata.
    pub async fn execute<S>(
        &self,
        query: &ParsedQuery,
        output: &mut S,
        query_timeout: Duration,
        write_timeout: Duration,
    ) -> PgWireResult<()>
    where
        S: Sink<PgWireBackendMessage> + Unpin + Send,
        S::Error: fmt::Debug,
        PgWireError: From<S::Error>,
    {
        query
            .validate_read_only()
            .map_err(|error| error_response("0A000", &error.to_string()))?;
        let object = timeout(ACQUIRE_TIMEOUT, self.pool.get())
            .await
            .map_err(|_| error_response("53300", "PostgreSQL pool acquisition timed out"))?
            .map_err(|error| match error {
                deadpool::managed::PoolError::Timeout(_) => {
                    error_response("53300", "PostgreSQL pool acquisition timed out")
                }
                _ => error_response("08006", "PostgreSQL connection unavailable"),
            })?;
        let mut lease = Lease(Some(object));
        let connection = lease.0.as_mut().unwrap();
        connection.clean = false;
        connection.query_complete = false;
        // The gateway owns execution policy; PostgreSQL also enforces that budget.
        // Local settings end at rollback and cannot leak to the next pool borrower.
        let transaction = format!(
            "BEGIN READ ONLY; SET LOCAL statement_timeout = '{}ms'; SET LOCAL idle_in_transaction_session_timeout = '{}ms'",
            query_timeout.as_millis(),
            query_timeout.max(write_timeout).as_millis(),
        );
        timeout(CONNECT_TIMEOUT, connection.control(&transaction))
            .await
            .map_err(|_| error_response("08006", "PostgreSQL transaction setup timed out"))??;
        if connection.status != TransactionStatus::Transaction {
            return Err(error_response(
                "08006",
                "unexpected PostgreSQL transaction state",
            ));
        }
        let completion = connection
            .stream_query(query.sql(), output, query_timeout, write_timeout)
            .await;
        // A cancelled future may have unread protocol messages. Its lease is
        // discarded; it must never enter the pool's idle queue.
        if !connection.query_complete {
            return completion.map(|_| ());
        }
        if connection.status != TransactionStatus::Transaction
            && connection.status != TransactionStatus::Error
        {
            return Err(error_response(
                "08006",
                "unexpected PostgreSQL transaction state",
            ));
        }
        let cleanup = timeout(CLEANUP_TIMEOUT, async {
            connection.control("ROLLBACK").await?;
            connection.control("DISCARD ALL").await?;
            connection.control(SESSION_DEFAULTS).await?;
            if connection.status != TransactionStatus::Idle {
                return Err(error_response(
                    "08006",
                    "PostgreSQL cleanup could not be verified",
                ));
            }
            connection.control("SELECT 1").await?;
            connection.clean = true;
            Ok(())
        })
        .await
        .map_err(|_| error_response("08006", "PostgreSQL cleanup timed out"))
        .and_then(|result| result);
        if let Err(error) = cleanup {
            return Err(completion.err().unwrap_or(error));
        }
        // Return only a verified idle connection. Completion is emitted after
        // cleanup so a failed cleanup cannot be advertised as a successful read.
        let message = match completion {
            Ok(message) => message,
            Err(error) => {
                lease.release();
                return Err(error);
            }
        };
        timeout(write_timeout, output.send(message))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "frontend write timed out"))??;
        lease.release();
        Ok(())
    }

    pub(crate) fn close(&self) {
        self.pool.close();
    }
}

fn error_response(code: &str, message: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".into(),
        code.into(),
        message.into(),
    )))
}

struct PgManager {
    host: String,
    port: u16,
    config: Arc<Config>,
}

impl Manager for PgManager {
    type Type = PgConnection;
    type Error = PgWireError;

    async fn create(&self) -> PgWireResult<PgConnection> {
        let socket = TcpStream::connect((self.host.as_str(), self.port)).await?;
        if !socket.peer_addr()?.ip().is_loopback() {
            return Err(error_response(
                "08006",
                "PostgreSQL requires a loopback address",
            ));
        }
        socket.set_nodelay(true)?;
        let mut connection = PgConnection {
            socket: Framed::new(
                FrameGuard::backend(socket),
                PgWireMessageClientCodec::default(),
            ),
            config: self.config.clone(),
            info: ServerInformation::default(),
            protocol: self.config.get_protocol_version(),
            status: TransactionStatus::Idle,
            clean: false,
            query_complete: false,
        };
        let mut auth = DefaultStartupHandler::new();
        auth.startup(&mut connection).await.map_err(client_error)?;
        loop {
            let message = connection
                .next()
                .await
                .ok_or_else(|| error_response("08006", "PostgreSQL closed during startup"))??;
            if let ReadyState::Ready(info) = auth
                .on_message(&mut connection, message)
                .await
                .map_err(client_error)?
            {
                connection.info = info;
                break;
            }
        }
        connection.control(SESSION_DEFAULTS).await?;
        connection.clean = true;
        Ok(connection)
    }

    async fn recycle(
        &self,
        connection: &mut PgConnection,
        _: &Metrics,
    ) -> RecycleResult<PgWireError> {
        if !connection.clean || connection.status != TransactionStatus::Idle {
            return Err(RecycleError::message("unclean PostgreSQL connection"));
        }
        connection
            .control("SELECT 1")
            .await
            .map_err(RecycleError::Backend)
    }
}

fn client_error(error: PgWireClientError) -> PgWireError {
    match error {
        PgWireClientError::RemoteError(mut info) => {
            info.severity = "ERROR".into();
            PgWireError::UserError(info)
        }
        _ => error_response("08006", "PostgreSQL startup failed"),
    }
}

// Drop is synchronous: interrupted tasks remove and close their physical
// connection rather than scheduling an unbounded background cleanup worker.
struct Lease(Option<Object<PgManager>>);
impl Lease {
    fn release(mut self) {
        drop(self.0.take());
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        if let Some(object) = self.0.take() {
            drop(Object::take(object));
        }
    }
}

struct PgConnection {
    socket: Framed<FrameGuard, PgWireMessageClientCodec>,
    config: Arc<Config>,
    info: ServerInformation,
    protocol: ProtocolVersion,
    status: TransactionStatus,
    clean: bool,
    query_complete: bool,
}

impl PgConnection {
    async fn control(&mut self, sql: &str) -> PgWireResult<()> {
        self.socket
            .send(PgWireFrontendMessage::Query(Query::new(sql.into())))
            .await?;
        let mut error = None;
        loop {
            let message = self
                .socket
                .next()
                .await
                .ok_or_else(|| error_response("08006", "PostgreSQL connection closed"))??;
            match message {
                PgWireBackendMessage::ReadyForQuery(ready) => {
                    self.status = ready.status;
                    return error.map_or(Ok(()), Err);
                }
                PgWireBackendMessage::ErrorResponse(response) => {
                    let mut info = ErrorInfo::from(response);
                    info.severity = "ERROR".into();
                    error = Some(PgWireError::UserError(Box::new(info)));
                }
                PgWireBackendMessage::ParameterStatus(parameter) => {
                    self.info.parameters.insert(parameter.name, parameter.value);
                }
                PgWireBackendMessage::RowDescription(_)
                | PgWireBackendMessage::DataRow(_)
                | PgWireBackendMessage::CommandComplete(_)
                | PgWireBackendMessage::NoticeResponse(_) => {}
                _ => {
                    return Err(error_response(
                        "08006",
                        "unexpected PostgreSQL control response",
                    ));
                }
            }
        }
    }

    async fn stream_query<S>(
        &mut self,
        sql: &str,
        output: &mut S,
        query_timeout: Duration,
        write_timeout: Duration,
    ) -> PgWireResult<PgWireBackendMessage>
    where
        S: Sink<PgWireBackendMessage> + Unpin + Send,
        S::Error: fmt::Debug,
        PgWireError: From<S::Error>,
    {
        let started = Instant::now();
        timeout(
            query_timeout,
            self.socket
                .send(PgWireFrontendMessage::Query(Query::new(sql.into()))),
        )
        .await
        .map_err(|_| error_response("57014", "PostgreSQL query timed out"))??;
        let mut remaining = query_timeout.saturating_sub(started.elapsed());
        let mut completion = None;
        let mut error = None;
        loop {
            // Frontend backpressure has its own write deadline and does not
            // consume the backend wait budget. PostgreSQL also limits execution.
            let started = Instant::now();
            let received = timeout(remaining, self.socket.next())
                .await
                .map_err(|_| error_response("57014", "PostgreSQL query timed out"))?;
            remaining = remaining.saturating_sub(started.elapsed());
            let message = match received {
                Some(Ok(message)) => message,
                _ => {
                    return Err(error.unwrap_or_else(|| {
                        error_response(
                            "08006",
                            "PostgreSQL connection failed or frame exceeded 8 MiB",
                        )
                    }));
                }
            };
            match message {
                PgWireBackendMessage::ReadyForQuery(ready) => {
                    self.status = ready.status;
                    self.query_complete = true;
                    if let Some(error) = error {
                        return Err(error);
                    }
                    return completion
                        .ok_or_else(|| error_response("08006", "missing PostgreSQL completion"));
                }
                message @ PgWireBackendMessage::CommandComplete(_) => {
                    completion = Some(message);
                }
                PgWireBackendMessage::ErrorResponse(response) => {
                    let mut info = ErrorInfo::from(response);
                    info.severity = "ERROR".into();
                    error = Some(PgWireError::UserError(Box::new(info)));
                }
                PgWireBackendMessage::ParameterStatus(parameter) => {
                    if matches!(
                        parameter.name.as_str(),
                        "client_encoding"
                            | "DateStyle"
                            | "TimeZone"
                            | "IntervalStyle"
                            | "standard_conforming_strings"
                    ) && self.info.parameters.get(&parameter.name) != Some(&parameter.value)
                    {
                        return Err(error_response(
                            "0A000",
                            "changing PostgreSQL text settings is unsupported",
                        ));
                    }
                    self.info.parameters.insert(parameter.name, parameter.value);
                }
                message @ (PgWireBackendMessage::RowDescription(_)
                | PgWireBackendMessage::DataRow(_)
                | PgWireBackendMessage::NoticeResponse(_)) => {
                    if let PgWireBackendMessage::DataRow(row) = &message {
                        validate_text_row(row)?;
                    }
                    timeout(write_timeout, output.send(message))
                        .await
                        .map_err(|_| {
                            io::Error::new(io::ErrorKind::TimedOut, "frontend write timed out")
                        })??;
                }
                _ => {
                    return Err(error_response(
                        "08006",
                        "unexpected PostgreSQL query response",
                    ));
                }
            }
        }
    }
}

fn validate_text_row(row: &DataRow) -> PgWireResult<()> {
    let mut data = row.data.as_ref();
    for _ in 0..row.field_count {
        let length = data
            .get(..4)
            .ok_or_else(|| error_response("08006", "invalid PostgreSQL row"))?;
        let length = i32::from_be_bytes(length.try_into().unwrap());
        data = &data[4..];
        if length == -1 {
            continue;
        }
        if length < 0 || length as usize > data.len() {
            return Err(error_response("08006", "invalid PostgreSQL row"));
        }
        let (value, rest) = data.split_at(length as usize);
        std::str::from_utf8(value)
            .map_err(|_| error_response("0A000", "PostgreSQL text must remain UTF8"))?;
        data = rest;
    }
    if row.field_count < 0 || !data.is_empty() {
        return Err(error_response("08006", "invalid PostgreSQL row"));
    }
    Ok(())
}

impl ClientInfo for PgConnection {
    fn config(&self) -> &Config {
        &self.config
    }
    fn server_parameters(&self) -> &BTreeMap<String, String> {
        &self.info.parameters
    }
    fn set_server_parameter(&mut self, name: String, value: String) {
        self.info.parameters.insert(name, value);
    }
    fn process_id(&self) -> i32 {
        self.info.process_id
    }
    fn secret_key(&self) -> &SecretKey {
        &self.info.secret_key
    }
    fn protocol_version(&self) -> ProtocolVersion {
        self.protocol
    }
    fn set_protocol_version(&mut self, protocol: ProtocolVersion) {
        self.protocol = protocol;
    }
    fn transaction_status(&self) -> TransactionStatus {
        self.status
    }
    fn set_transaction_status(&mut self, status: TransactionStatus) {
        self.status = status;
    }
}

impl Stream for PgConnection {
    type Item = PgWireResult<PgWireBackendMessage>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.socket).poll_next(cx)
    }
}
impl Sink<PgWireFrontendMessage> for PgConnection {
    type Error = PgWireError;
    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<PgWireResult<()>> {
        Pin::new(&mut self.socket).poll_ready(cx)
    }
    fn start_send(mut self: Pin<&mut Self>, message: PgWireFrontendMessage) -> PgWireResult<()> {
        Pin::new(&mut self.socket).start_send(message)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<PgWireResult<()>> {
        Pin::new(&mut self.socket).poll_flush(cx)
    }
    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<PgWireResult<()>> {
        Pin::new(&mut self.socket).poll_close(cx)
    }
}
