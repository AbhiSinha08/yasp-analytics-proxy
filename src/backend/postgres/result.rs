//! Lossless PostgreSQL representation at the shared execution boundary.

use crate::backend::result::{ExecutionError, ResultEvent};
use futures::Sink;
use pgwire::{error::PgWireError, messages::PgWireBackendMessage};
use std::{
    fmt,
    pin::Pin,
    task::{Context, Poll},
};

pub(crate) struct NativeError(PgWireError);
impl fmt::Debug for NativeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("NativeError { .. }")
    }
}
impl NativeError {
    pub(crate) fn into_wire(self) -> PgWireError {
        self.0
    }
}
impl From<PgWireError> for ExecutionError {
    fn from(error: PgWireError) -> Self {
        Self::Postgres(NativeError(error))
    }
}

/// Only this adapter wraps PostgreSQL messages. The shared consumer sees opaque
/// values; no row buffering, serialization, or task/channel is added.
pub(crate) struct Output<'a, S>(pub(crate) &'a mut S);
impl<S: Sink<ResultEvent, Error = ExecutionError> + Unpin> Sink<PgWireBackendMessage>
    for Output<'_, S>
{
    type Error = ExecutionError;
    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut *self.get_mut().0).poll_ready(cx)
    }
    fn start_send(self: Pin<&mut Self>, message: PgWireBackendMessage) -> Result<(), Self::Error> {
        Pin::new(&mut *self.get_mut().0).start_send(message.into())
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut *self.get_mut().0).poll_flush(cx)
    }
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut *self.get_mut().0).poll_close(cx)
    }
}
