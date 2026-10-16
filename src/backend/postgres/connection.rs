//! PostgreSQL message streaming, cancellation, and lossless text-row validation.

use super::{CONNECT_TIMEOUT, error_response};
use crate::transport::FrameGuard;
use futures::{Sink, SinkExt, Stream, StreamExt};
use pgwire::{
    api::client::{ClientInfo, Config, ServerInformation},
    error::{ErrorInfo, PgWireError, PgWireResult},
    messages::{
        PgWireBackendMessage, PgWireFrontendMessage, ProtocolVersion, cancel::CancelRequest,
        data::DataRow, response::TransactionStatus, simplequery::Query, startup::SecretKey,
    },
    tokio::client::PgWireMessageClientCodec,
};
use std::{
    collections::BTreeMap,
    fmt, io,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    net::TcpStream,
    time::{Instant, timeout, timeout_at},
};
use tokio_util::codec::Framed;

pub(super) struct PgConnection {
    pub(super) socket: Framed<FrameGuard, PgWireMessageClientCodec>,
    pub(super) config: Arc<Config>,
    pub(super) info: ServerInformation,
    pub(super) protocol: ProtocolVersion,
    pub(super) address: SocketAddr,
    pub(super) status: TransactionStatus,
    pub(super) clean: bool,
    pub(super) query_complete: bool,
    pub(super) cancel_requested: bool,
}

impl PgConnection {
    pub(super) async fn control(&mut self, sql: &str) -> PgWireResult<()> {
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

    pub(super) async fn stream_query<S>(
        &mut self,
        sql: &str,
        output: &mut S,
        query_timeout: Duration,
        write_timeout: Duration,
        cancel: Option<futures::channel::oneshot::Receiver<()>>,
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
        let cancel_signal = async move {
            match cancel {
                Some(receiver) => {
                    if receiver.await.is_err() {
                        std::future::pending::<()>().await;
                    }
                }
                None => std::future::pending::<()>().await,
            }
        };
        tokio::pin!(cancel_signal);
        let mut cancel_sent = false;
        loop {
            // Frontend backpressure has its own write deadline and does not
            // consume the backend wait budget. PostgreSQL also limits execution.
            let started = Instant::now();
            let received = tokio::select! {
                biased;
                received = timeout(remaining, self.socket.next()) => received,
                _ = &mut cancel_signal, if !cancel_sent => {
                    cancel_sent = true;
                    self.cancel_requested = true;
                    send_cancel_request(
                        self.address,
                        self.info.process_id,
                        self.info.secret_key.clone(),
                        remaining.min(CONNECT_TIMEOUT),
                    ).await?;
                    remaining = remaining.saturating_sub(started.elapsed());
                    continue;
                }
            }
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

async fn send_cancel_request(
    address: SocketAddr,
    process_id: i32,
    secret_key: SecretKey,
    budget: Duration,
) -> PgWireResult<()> {
    let deadline = Instant::now() + budget;
    let socket = timeout_at(deadline, TcpStream::connect(address))
        .await
        .map_err(|_| error_response("08006", "PostgreSQL cancellation connection timed out"))??;
    let mut socket = Framed::new(socket, PgWireMessageClientCodec::default());
    timeout_at(
        deadline,
        socket.send(PgWireFrontendMessage::CancelRequest(CancelRequest::new(
            process_id, secret_key,
        ))),
    )
    .await
    .map_err(|_| error_response("08006", "PostgreSQL cancellation send timed out"))??;
    Ok(())
}

fn validate_text_row(row: &DataRow) -> PgWireResult<()> {
    validate_text_fields(row.field_count, row.data.as_ref())
}

pub(super) fn validate_text_fields(field_count: i16, mut data: &[u8]) -> PgWireResult<()> {
    for _ in 0..field_count {
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
    if field_count < 0 || !data.is_empty() {
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
