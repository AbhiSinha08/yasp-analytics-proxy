//! Derive bounded application metadata without replacing authenticated identity.

use super::payload;
#[cfg(feature = "python")]
use super::python;
use crate::datasets::DatasetReader;
use crate::query::ParsedQuery;
use futures::future::BoxFuture;
#[cfg(feature = "python")]
use serde_json::json;
use serde_json::{Map, Value};
use std::{sync::Arc, time::Duration};
use thiserror::Error;

/// Available query metadata before the application derives context.
#[derive(Clone, Copy)]
pub struct ContextRequest<'a> {
    pub data_contexts: &'a crate::lineage::DataContexts,
    pub authenticated_user: &'a str,
    pub frontend_database: &'a str,
    pub client_id: u64,
    pub query_id: &'a str,
    pub query: &'a ParsedQuery,
    pub datasets: Option<&'a DatasetReader>,
}

/// Context diagnostics contain no application data or lookup error text.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ContextError {
    #[error("context callback failed")]
    Failed,
    #[error("context callback timed out")]
    TimedOut,
    #[error("context callback returned unsupported data")]
    InvalidOutput,
    #[error("context exceeds size or nesting limits")]
    PayloadTooLarge,
    #[cfg(feature = "python")]
    #[error("Python context callback failed: {0}")]
    Python(#[from] python::PythonHookError),
}

pub type ContextCallback = Arc<
    dyn for<'a> Fn(ContextRequest<'a>) -> BoxFuture<'a, Result<Map<String, Value>, ContextError>>
        + Send
        + Sync,
>;

/// One optional context stage; identity mappings and authorization belong to the host.
pub enum ContextHook {
    Rust(ContextCallback),
    #[cfg(feature = "python")]
    Python(python::PythonCallback),
}

impl From<ContextCallback> for ContextHook {
    fn from(callback: ContextCallback) -> Self {
        Self::Rust(callback)
    }
}

#[cfg(feature = "python")]
impl From<python::PythonCallback> for ContextHook {
    fn from(callback: python::PythonCallback) -> Self {
        Self::Python(callback)
    }
}

impl ContextHook {
    /// Derive one bounded context snapshot; failures use the application's default policy.
    pub async fn derive(
        &self,
        request: ContextRequest<'_>,
        deadline: Duration,
        max_bytes: usize,
    ) -> Map<String, Value> {
        let result = async {
            let value = match self {
                Self::Rust(callback) => {
                    let (datasets, _guard) = request
                        .datasets
                        .map(DatasetReader::invocation)
                        .map_or((None, None), |(reader, guard)| (Some(reader), Some(guard)));
                    let request = ContextRequest {
                        datasets: datasets.as_ref(),
                        ..request
                    };
                    Value::Object(callback(request).await?)
                }
                #[cfg(feature = "python")]
                Self::Python(callback) => {
                    callback
                        .call_with_datasets(
                            json!({
                                "authenticated_user": request.authenticated_user,
                                "frontend_database": request.frontend_database,
                                "client_id": request.client_id,
                                "query_id": request.query_id,
                                "sql": request.query.sql(),
                                "query_ast": request.query.serialized_ast(),
                                "data_contexts": request.data_contexts,
                            }),
                            request.datasets.cloned(),
                        )
                        .await?
                }
            };
            if value.is_null() {
                return Ok(Map::new());
            }
            if !value.is_object() {
                return Err(ContextError::InvalidOutput);
            }
            payload::payload_size(&value, max_bytes).map_err(|_| ContextError::PayloadTooLarge)?;
            let Value::Object(context) = value else {
                unreachable!()
            };
            Ok(context)
        };
        match tokio::time::timeout(deadline, result)
            .await
            .unwrap_or(Err(ContextError::TimedOut))
        {
            Ok(context) => context,
            Err(error) => {
                tracing::error!(
                    query_id = request.query_id,
                    client_id = request.client_id,
                    category = %error,
                    "context unavailable; using default context"
                );
                Map::new()
            }
        }
    }

    pub(crate) fn shutdown(&self) {
        #[cfg(feature = "python")]
        if let Self::Python(callback) = self {
            callback.runtime().shutdown();
        }
    }
}

/// Bind host state to asynchronous context derivation. Callbacks must use
/// cooperative async I/O; a deadline cannot interrupt blocking Rust code.
pub fn bind_context<State, F>(state: State, function: F) -> ContextCallback
where
    State: Send + Sync + 'static,
    F: for<'a> Fn(
            ContextRequest<'a>,
            Arc<State>,
        ) -> BoxFuture<'a, Result<Map<String, Value>, ContextError>>
        + Send
        + Sync
        + 'static,
{
    let state = Arc::new(state);
    Arc::new(move |request| function(request, state.clone()))
}
