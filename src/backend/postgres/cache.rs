//! PostgreSQL text response capture and validation for the shared result cache.
//! Redis stores opaque bytes; only this adapter interprets protocol frames.

use super::PostgresBackend;
use crate::{
    cache::{CacheBuffer, RedisCache},
    config::GatewaySettings,
    discovery::SessionSettings,
};
use futures::{Sink, SinkExt, channel::oneshot};
use pgwire::{
    error::{PgWireError, PgWireResult},
    messages::{DecodeContext, Message, PgWireBackendMessage},
};
use sha2::{Digest, Sha256};
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tokio::time::timeout;

pub(crate) fn scope(
    backend: &PostgresBackend,
    user: &str,
    database: &str,
    settings: &GatewaySettings,
    session: &SessionSettings,
) -> [u8; 32] {
    let mut hash = Sha256::new();
    for value in [
        "postgresql:text:v1",
        user,
        database,
        backend.target(),
        backend.database_user(),
        backend.compute_type().unwrap_or(""),
        backend.compute(),
        super::SESSION_DEFAULTS,
        &session.search_path,
        &session.application_name,
    ] {
        hash.update((value.len() as u64).to_be_bytes());
        hash.update(value.as_bytes());
    }
    hash.update(settings.query_timeout_ms.to_be_bytes());
    hash.update(settings.write_timeout_ms.to_be_bytes());
    hash.finalize().into()
}

pub(crate) struct CaptureSink<'a, S> {
    inner: &'a mut S,
    buffer: Option<CacheBuffer>,
    max_entry_bytes: usize,
    max_rows_per_query: usize,
    rows: usize,
}

impl<'a, S> CaptureSink<'a, S> {
    pub(crate) fn new(
        inner: &'a mut S,
        buffer: Option<CacheBuffer>,
        cache: Option<&RedisCache>,
    ) -> Self {
        Self {
            inner,
            buffer,
            max_entry_bytes: cache.map_or(0, RedisCache::max_entry_bytes),
            max_rows_per_query: cache.map_or(0, RedisCache::max_rows_per_query),
            rows: 0,
        }
    }

    pub(crate) fn captured(&self) -> Option<&CacheBuffer> {
        self.buffer.as_ref()
    }
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

/// Replay only a completely validated response. Cancellation is observed between
/// frames so it cannot leave a partially written PostgreSQL message on the wire.
pub(crate) async fn replay<S>(
    buffer: &mut CacheBuffer,
    output: &mut S,
    write_timeout: Duration,
    cancel: &mut oneshot::Receiver<()>,
) -> PgWireResult<()>
where
    S: Sink<PgWireBackendMessage> + Unpin,
    PgWireError: From<S::Error>,
{
    while !buffer.bytes.is_empty() {
        if cancel.try_recv().ok().flatten().is_some() {
            return Err(super::error_response("57014", "query cancelled"));
        }
        let message = PgWireBackendMessage::decode(&mut buffer.bytes, &DecodeContext::default())?
            .expect("validated complete cache frame");
        timeout(write_timeout, output.send(message))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "frontend write timed out"))??;
    }
    Ok(())
}

// Validate raw frames before invoking pgwire's decoders, which assume their
// field bodies are complete. A malformed cache entry is a miss, never a panic
// or a partial response followed by a database fallback.
pub(crate) fn valid_response(mut bytes: &[u8], max_rows: usize) -> bool {
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
                if columns != Some(count) || super::validate_text_fields(count, &body[2..]).is_err()
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
