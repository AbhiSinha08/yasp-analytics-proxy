//! Application-owned SQL replacement before fingerprints, lineage, and routing.

#[cfg(feature = "python")]
use super::python;
use crate::{
    datasets::DatasetReader,
    query::{SqlDialect, Statement},
};
use futures::future::BoxFuture;
use serde_json::{Map, Value};
use std::{sync::Arc, time::Duration};
use thiserror::Error;

/// Incoming SQL and its immutable AST, after context extraction. Lineage and
/// query IDs are derived from the effective query only after this hook returns.
#[derive(Clone, Copy)]
pub struct QueryModificationRequest<'a> {
    pub authenticated_user: &'a str,
    pub frontend_database: &'a str,
    pub client_id: u64,
    pub sql: &'a str,
    pub statement: &'a Statement,
    pub dialect: SqlDialect,
    pub gateway_dialect: SqlDialect,
    pub default_target_dialect: Option<SqlDialect>,
    pub query_context: &'a Map<String, Value>,
    pub datasets: Option<&'a DatasetReader>,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum QueryModificationError {
    #[error("query denied by modification callback")]
    Denied,
    #[error("query modification callback failed")]
    Failed,
    #[error("query modification callback timed out")]
    TimedOut,
    #[error("query modification callback returned unsupported data")]
    InvalidOutput,
    #[error("modified SQL exceeds the configured byte limit")]
    PayloadTooLarge,
    #[cfg(feature = "python")]
    #[error("Python query modification callback failed: {0}")]
    Python(#[from] python::PythonHookError),
}

/// `None` preserves the original SQL; `Some` supplies SQL for engine validation.
pub type QueryModificationCallback = Arc<
    dyn for<'a> Fn(
            QueryModificationRequest<'a>,
        ) -> BoxFuture<'a, Result<Option<String>, QueryModificationError>>
        + Send
        + Sync,
>;

pub enum QueryModificationHook {
    Rust(QueryModificationCallback),
    #[cfg(feature = "python")]
    Python(python::PythonCallback),
}

impl From<QueryModificationCallback> for QueryModificationHook {
    fn from(callback: QueryModificationCallback) -> Self {
        Self::Rust(callback)
    }
}

#[cfg(feature = "python")]
impl From<python::PythonCallback> for QueryModificationHook {
    fn from(callback: python::PythonCallback) -> Self {
        Self::Python(callback)
    }
}

impl QueryModificationHook {
    /// Invoke the application. The gateway reparses and validates replacement SQL.
    pub async fn modify(
        &self,
        request: QueryModificationRequest<'_>,
        deadline: Duration,
        max_sql_bytes: usize,
    ) -> Result<Option<String>, QueryModificationError> {
        let result = async {
            let output = match self {
                Self::Rust(callback) => {
                    let (datasets, _guard) = request
                        .datasets
                        .map(DatasetReader::invocation)
                        .map_or((None, None), |(reader, guard)| (Some(reader), Some(guard)));
                    callback(QueryModificationRequest {
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
                                "query_ast": request.statement,
                                "dialect": request.dialect,
                                "gateway_dialect": request.gateway_dialect,
                                "default_target_dialect": request.default_target_dialect,
                                "query_context": request.query_context,
                            }),
                            request.datasets.cloned(),
                        )
                        .await?;
                    match value {
                        Value::Null => None,
                        Value::String(sql) => Some(sql),
                        _ => return Err(QueryModificationError::InvalidOutput),
                    }
                }
            };
            if let Some(sql) = &output {
                if sql.len() > max_sql_bytes {
                    return Err(QueryModificationError::PayloadTooLarge);
                }
                if sql.contains('\0') {
                    return Err(QueryModificationError::InvalidOutput);
                }
            }
            Ok(output)
        };
        let result = tokio::time::timeout(deadline, result)
            .await
            .unwrap_or(Err(QueryModificationError::TimedOut));
        if let Err(error) = &result {
            tracing::error!(client_id = request.client_id, category = %error, "query modification unavailable");
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

/// Bind application state. Use cooperative async I/O; deadlines cannot interrupt
/// blocking Rust code. All returned SQL remains subject to engine validation.
pub fn bind_modify_query<State, F>(state: State, function: F) -> QueryModificationCallback
where
    State: Send + Sync + 'static,
    F: for<'a> Fn(
            QueryModificationRequest<'a>,
            Arc<State>,
        ) -> BoxFuture<'a, Result<Option<String>, QueryModificationError>>
        + Send
        + Sync
        + 'static,
{
    let state = Arc::new(state);
    Arc::new(move |request| function(request, state.clone()))
}
