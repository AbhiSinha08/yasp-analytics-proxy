//! Typed execution failures and source result events. Pre-encoded frontend
//! responses remain opaque so native metadata and text values survive unchanged.

use super::postgres::NativeError;
use std::io;

#[derive(Debug, thiserror::Error)]
pub(crate) enum ExecutionError {
    #[error("{message}")]
    Rejected { code: &'static str, message: String },
    #[error("execution transport failed")]
    Io(#[from] io::Error),
    #[error("source execution failed")]
    Postgres(NativeError),
    #[cfg(feature = "snowflake")]
    #[error("source execution failed")]
    Source { code: String, message: String },
}

impl ExecutionError {
    pub(crate) fn rejected(code: &'static str, message: &str) -> Self {
        Self::Rejected {
            code,
            message: message.into(),
        }
    }

    pub(crate) fn cancelled() -> Self {
        Self::rejected("57014", "query cancelled")
    }
}

pub(crate) type ExecutionResult<T> = Result<T, ExecutionError>;

pub(crate) enum ResultEvent {
    Wire(crate::wire::Response),
    Parameter { name: String, value: String },
    Columns(Vec<ResultColumn>),
    Row(Vec<Option<bytes::Bytes>>),
    Complete(u64),
}

pub(crate) struct ResultColumn {
    pub name: String,
    pub kind: ColumnType,
}

#[allow(dead_code)] // Some types are only emitted by the optional Snowflake adapter.
pub(crate) enum ColumnType {
    Bool,
    Int16,
    Int32,
    Oid,
    Int64,
    Int16Array,
    TextArray,
    Json,
    Float32,
    Float64,
    Decimal { precision: u8, scale: i8 },
    Text,
    Binary,
    Date,
    Time,
    Timestamp { utc: bool },
}
