//! Bounded application values and input dialect, after frontend compatibility.
use super::payload;
#[cfg(feature = "python")]
use super::python;
use crate::{datasets::DatasetReader, query::SqlDialect};
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::{sync::Arc, time::Duration};
use thiserror::Error;

/// Byte-bounded original SQL. Parsing and source selection have not run yet.
#[derive(Clone, Copy)]
pub struct QueryContextRequest<'a> {
    pub authenticated_user: &'a str,
    pub frontend_database: &'a str,
    pub client_id: u64,
    pub sql: &'a str,
    pub gateway_dialect: SqlDialect,
    /// Present only when all configured data bindings have one dialect.
    pub default_target_dialect: Option<SqlDialect>,
    pub datasets: Option<&'a DatasetReader>,
}

/// Only dialect affects engine behavior; application values remain opaque.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct QueryContextOutput {
    pub dialect: Option<SqlDialect>,
    pub values: Map<String, Value>,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum QueryContextError {
    #[error("query context callback failed")]
    Failed,
    #[error("query context callback timed out")]
    TimedOut,
    #[error("query context callback returned unsupported data")]
    InvalidOutput,
    #[error("query context exceeds size or nesting limits")]
    PayloadTooLarge,
    #[error("native input dialect is ambiguous; configure native_dialect or return a dialect")]
    AmbiguousDialect,
    #[cfg(feature = "python")]
    #[error("Python query context callback failed: {0}")]
    Python(#[from] python::PythonHookError),
}

impl From<Map<String, Value>> for QueryContextOutput {
    fn from(values: Map<String, Value>) -> Self {
        Self {
            dialect: None,
            values,
        }
    }
}

pub type QueryContextCallback = Arc<
    dyn for<'a> Fn(
            QueryContextRequest<'a>,
        ) -> BoxFuture<'a, Result<QueryContextOutput, QueryContextError>>
        + Send
        + Sync,
>;
pub enum QueryContextHook {
    Rust(QueryContextCallback),
    #[cfg(feature = "python")]
    Python(python::PythonCallback),
}
impl From<QueryContextCallback> for QueryContextHook {
    fn from(callback: QueryContextCallback) -> Self {
        Self::Rust(callback)
    }
}
#[cfg(feature = "python")]
impl From<python::PythonCallback> for QueryContextHook {
    fn from(callback: python::PythonCallback) -> Self {
        Self::Python(callback)
    }
}
impl QueryContextHook {
    pub async fn derive(
        &self,
        request: QueryContextRequest<'_>,
        deadline: Duration,
        max_bytes: usize,
    ) -> Result<QueryContextOutput, QueryContextError> {
        let result = async {
            let output = match self {
                Self::Rust(callback) => {
                    let (datasets, _guard) = request
                        .datasets
                        .map(DatasetReader::invocation)
                        .map_or((None, None), |(reader, guard)| (Some(reader), Some(guard)));
                    callback(QueryContextRequest {
                        datasets: datasets.as_ref(),
                        ..request
                    })
                    .await?
                }
                #[cfg(feature = "python")]
                Self::Python(callback) => {
                    let value = callback
                        .call_with_datasets(
                            serde_json::json!({
                                "authenticated_user": request.authenticated_user,
                                "frontend_database": request.frontend_database,
                                "client_id": request.client_id,
                                "sql": request.sql,
                                "gateway_dialect": request.gateway_dialect,
                                "default_target_dialect": request.default_target_dialect,
                            }),
                            request.datasets.cloned(),
                        )
                        .await?;
                    if !value.is_object() {
                        return Err(QueryContextError::InvalidOutput);
                    }
                    payload::payload_size(&value, max_bytes)
                        .map_err(|_| QueryContextError::PayloadTooLarge)?;
                    serde_json::from_value(value).map_err(|_| QueryContextError::InvalidOutput)?
                }
            };
            let value =
                serde_json::to_value(&output).map_err(|_| QueryContextError::InvalidOutput)?;
            payload::payload_size(&value, max_bytes)
                .map_err(|_| QueryContextError::PayloadTooLarge)?;
            Ok(output)
        };
        let result = tokio::time::timeout(deadline, result)
            .await
            .unwrap_or(Err(QueryContextError::TimedOut));
        if let Err(error) = &result {
            tracing::error!(client_id = request.client_id, category = %error, "query context unavailable");
        }
        result
    }
    pub(crate) fn shutdown(&self) {
        #[cfg(feature = "python")]
        if let Self::Python(callback) = self {
            callback.runtime().shutdown();
        }
    }
}
/// Callbacks must use cooperative async I/O; deadlines cannot interrupt blocking Rust.
pub fn bind_query_context<State, F>(state: State, function: F) -> QueryContextCallback
where
    State: Send + Sync + 'static,
    F: for<'a> Fn(
            QueryContextRequest<'a>,
            Arc<State>,
        ) -> BoxFuture<'a, Result<QueryContextOutput, QueryContextError>>
        + Send
        + Sync
        + 'static,
{
    let state = Arc::new(state);
    Arc::new(move |request| function(request, state.clone()))
}
