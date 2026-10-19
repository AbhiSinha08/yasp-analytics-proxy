//! Encode shared execution events and errors for the two pgwire profiles.

use super::{FrontendSocket, protocol_error};
use crate::backend::result::{ExecutionError, ResultEvent};
use futures::{Sink, SinkExt, future};
use pgwire::{
    error::PgWireError,
    messages::{PgWireBackendMessage, startup::ParameterStatus},
};

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
        .with(|event| {
            future::ready(Ok::<_, PgWireError>(match event {
                ResultEvent::Postgres(message) => message.into_wire(),
                ResultEvent::Parameter { name, value } => {
                    PgWireBackendMessage::ParameterStatus(ParameterStatus::new(name, value))
                }
                ResultEvent::Columns(columns) => columns_message(columns),
                ResultEvent::Row(values) => {
                    use bytes::BufMut;
                    let count = values.len() as i16;
                    let mut data = bytes::BytesMut::new();
                    for value in values {
                        match value {
                            Some(value) => {
                                data.put_i32(value.len() as i32);
                                data.extend_from_slice(&value);
                            }
                            None => data.put_i32(-1),
                        }
                    }
                    PgWireBackendMessage::DataRow(pgwire::messages::data::DataRow::new(data, count))
                }
                ResultEvent::Complete(rows) => PgWireBackendMessage::CommandComplete(
                    pgwire::messages::response::CommandComplete::new(format!("SELECT {rows}")),
                ),
            }))
        })
        .sink_map_err(ExecutionError::from)
}

fn columns_message(columns: Vec<crate::backend::result::ResultColumn>) -> PgWireBackendMessage {
    use crate::backend::result::ColumnType;
    use pgwire::messages::data::{FieldDescription, RowDescription};
    let fields = columns
        .into_iter()
        .map(|column| {
            let (oid, size, modifier) = match column.kind {
                ColumnType::Bool => (16, 1, -1),
                ColumnType::Int16 => (21, 2, -1),
                ColumnType::Int32 => (23, 4, -1),
                ColumnType::Int64 => (20, 8, -1),
                ColumnType::Float64 => (701, 8, -1),
                ColumnType::Decimal { precision, scale } => (
                    1700,
                    -1,
                    ((i32::from(precision) << 16) | i32::from(scale)) + 4,
                ),
                ColumnType::Text => (25, -1, -1),
                ColumnType::Binary => (17, -1, -1),
                ColumnType::Date => (1082, 4, -1),
                ColumnType::Time => (1083, 8, 6),
                ColumnType::Timestamp { utc } => (if utc { 1184 } else { 1114 }, 8, 6),
            };
            FieldDescription::new(column.name, 0, 0, oid, size, modifier, 0)
        })
        .collect();
    PgWireBackendMessage::RowDescription(RowDescription::new(fields))
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
