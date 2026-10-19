//! Cheap, synchronous input hints supplied by the host before parsing.

use crate::query::{InputOptions, SqlDialect};

/// SQL has already passed the configured byte bound. No credentials or routing
/// decisions are available here; the ordinary policy stages still run afterward.
pub struct InputRequest<'a> {
    pub sql: &'a str,
    pub frontend_dialect: SqlDialect,
}

pub type InputOptionsResolver = dyn Fn(InputRequest<'_>) -> InputOptions + Send + Sync;
