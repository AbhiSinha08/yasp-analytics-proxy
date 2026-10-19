//! Encode shared execution events and errors for the two pgwire profiles.

use super::{FrontendSocket, protocol_error};
use crate::backend::result::{ExecutionError, ResultEvent};
use futures::{Sink, SinkExt, future};
use pgwire::error::PgWireError;

impl From<ExecutionError> for PgWireError {
    fn from(error: ExecutionError) -> Self {
        match error {
            ExecutionError::Rejected { code, message } => protocol_error("ERROR", code, &message),
            ExecutionError::Io(error) => error.into(),
            ExecutionError::Postgres(error) => error.into_wire(),
            #[cfg(feature = "snowflake")]
            ExecutionError::Source { code, message } => protocol_error("ERROR", &code, &message),
        }
    }
}

pub(super) fn output(
    socket: &mut FrontendSocket,
) -> impl Sink<ResultEvent, Error = ExecutionError> + Unpin + '_ {
    socket
        .with(|event| future::ready(Ok::<_, PgWireError>(crate::wire::encode(event).into_wire())))
        .sink_map_err(ExecutionError::from)
}

impl From<crate::execution::TransactionStatus> for pgwire::messages::response::TransactionStatus {
    fn from(status: crate::execution::TransactionStatus) -> Self {
        match status {
            crate::execution::TransactionStatus::Idle => Self::Idle,
            crate::execution::TransactionStatus::Transaction => Self::Transaction,
            crate::execution::TransactionStatus::Error => Self::Error,
        }
    }
}
