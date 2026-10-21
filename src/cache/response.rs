//! Bounded frontend response capture and validation, independent of the source driver.
use crate::{
    backend::result::{ExecutionError, ExecutionResult, ResultEvent},
    cache::{CacheBuffer, RedisCache},
};
use futures::{Sink, SinkExt, channel::oneshot};
use pgwire::messages::{DecodeContext, Message, PgWireBackendMessage};
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tokio::time::timeout;

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

    pub(crate) fn take_buffer(&mut self) -> Option<CacheBuffer> {
        self.buffer.take()
    }
}

impl<S: Sink<ResultEvent> + Unpin> Sink<ResultEvent> for CaptureSink<'_, S> {
    type Error = S::Error;
    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut *self.get_mut().inner).poll_ready(cx)
    }
    fn start_send(self: Pin<&mut Self>, event: ResultEvent) -> Result<(), Self::Error> {
        let this = self.get_mut();
        if this.buffer.is_none() {
            return Pin::new(&mut *this.inner).start_send(event);
        }
        let response = crate::wire::encode(event);
        let message = response.message();
        if matches!(message, PgWireBackendMessage::DataRow(_)) {
            this.rows += 1;
            if this.rows > this.max_rows_per_query {
                this.buffer = None;
            }
        }
        if let Some(buffer) = &mut this.buffer {
            let size = match message {
                PgWireBackendMessage::RowDescription(m) => Some(m.message_length() + 1),
                PgWireBackendMessage::DataRow(m) => Some(m.message_length() + 1),
                PgWireBackendMessage::NoticeResponse(m) => Some(m.message_length() + 1),
                PgWireBackendMessage::CommandComplete(m) => Some(m.message_length() + 1),
                _ => None,
            };
            if size.is_none_or(|size| !buffer.prepare(size, this.max_entry_bytes))
                || message.encode(&mut buffer.bytes).is_err()
            {
                this.buffer = None;
            }
        }
        Pin::new(&mut *this.inner).start_send(ResultEvent::Wire(response))
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut *self.get_mut().inner).poll_flush(cx)
    }
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut *self.get_mut().inner).poll_close(cx)
    }
}

/// Inspect only the metadata frame, without cloning a whole cached result.
/// Callers have already validated the complete response framing.
pub(super) fn columns(mut bytes: &[u8]) -> Vec<crate::decision_model::ColumnIdentity> {
    while bytes.len() >= 5 {
        let size = u32::from_be_bytes(bytes[1..5].try_into().unwrap()) as usize + 1;
        if size > bytes.len() || size < 5 {
            break;
        }
        if bytes[0] == b'T' {
            let mut frame = bytes::BytesMut::from(&bytes[..size]);
            let message = PgWireBackendMessage::decode(&mut frame, &DecodeContext::default());
            drop(frame);
            return message
                .ok()
                .flatten()
                .and_then(|message| {
                    crate::wire::sample_columns(&crate::wire::Response::from_message(message))
                })
                .unwrap_or_default();
        }
        bytes = &bytes[size..];
    }
    Vec::new()
}

/// Replay only a completely validated response. Cancellation is observed between
/// frames so it cannot leave a partially written PostgreSQL message on the wire.
pub(crate) async fn replay<S>(
    buffer: &mut CacheBuffer,
    output: &mut S,
    write_timeout: Duration,
    cancel: &mut oneshot::Receiver<()>,
) -> ExecutionResult<()>
where
    S: Sink<ResultEvent, Error = ExecutionError> + Unpin,
{
    while !buffer.bytes.is_empty() {
        if cancel.try_recv().ok().flatten().is_some() {
            return Err(ExecutionError::cancelled());
        }
        let message = PgWireBackendMessage::decode(&mut buffer.bytes, &DecodeContext::default())?
            .expect("validated complete cache frame");
        timeout(write_timeout, output.send(message.into()))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "frontend write timed out"))??;
    }
    Ok(())
}

// Validate raw frames before invoking pgwire's decoders, which assume their
// field bodies are complete. A malformed cache entry is a miss, never a panic
// or a partial response followed by a database fallback.
pub(crate) fn valid_response(mut bytes: &[u8], max_rows: usize, max_frame: usize) -> bool {
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
        if length < 4 || length as usize >= max_frame || length as usize >= bytes.len() {
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
                    || crate::wire::validate_text_fields(count, &body[2..]).is_err()
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
