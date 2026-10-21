//! Application-owned route choice over immutable query metadata.
//! The gateway resolves each decision against configured targets and bindings.

#[cfg(feature = "python")]
use super::python;
use crate::datasets::DatasetReader;
use crate::query::ParsedQuery;
#[cfg(feature = "python")]
use serde_json::json;
use serde_json::{Map, Value};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;

/// Query and authenticated identity supplied to a backend selector.
pub struct SelectionRequest<'a> {
    pub data_contexts: &'a crate::lineage::DataContexts,
    pub authenticated_user: &'a str,
    pub client_id: u64,
    pub query_id: &'a str,
    pub query: &'a ParsedQuery,
    pub context: &'a Map<String, Value>,
    pub datasets: Option<&'a DatasetReader>,
    pub async_hook_timeout_ms: u32,
}

/// A partial policy decision resolved against configured connections by the engine.
#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct BackendDecision {
    pub target: Option<String>,
    pub backend_login: Option<String>,
    pub compute_type: Option<String>,
}

impl From<BackendSelection> for BackendDecision {
    fn from(selection: BackendSelection) -> Self {
        Self {
            target: Some(selection.target),
            backend_login: Some(selection.backend_login),
            compute_type: None,
        }
    }
}

/// A target and login pair selected from the host's configured connections.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct BackendSelection {
    pub target: String,
    pub backend_login: String,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum SelectionError {
    #[error("query denied by backend selector")]
    Denied,
    #[error("backend selector failed")]
    Failed,
}

pub type BackendSelector = Arc<
    dyn for<'a> Fn(&SelectionRequest<'a>) -> Result<BackendDecision, SelectionError> + Send + Sync,
>;

/// Registered selection behavior; the router owns validation and acquisition.
pub enum RoutedSelector {
    Rust(BackendSelector),
    RustAsync(AsyncBackendSelector),
    #[cfg(feature = "python")]
    Python(python::PythonCallback),
}

pub type AsyncBackendSelector = Arc<
    dyn for<'a> Fn(
            &'a SelectionRequest<'a>,
        )
            -> futures::future::BoxFuture<'a, Result<BackendDecision, SelectionError>>
        + Send
        + Sync,
>;

impl From<BackendSelector> for RoutedSelector {
    fn from(selector: BackendSelector) -> Self {
        Self::Rust(selector)
    }
}

impl From<AsyncBackendSelector> for RoutedSelector {
    fn from(selector: AsyncBackendSelector) -> Self {
        Self::RustAsync(selector)
    }
}

#[cfg(feature = "python")]
impl From<python::PythonCallback> for RoutedSelector {
    fn from(callback: python::PythonCallback) -> Self {
        Self::Python(callback)
    }
}

impl RoutedSelector {
    pub async fn select(
        &self,
        request: &SelectionRequest<'_>,
    ) -> Result<BackendDecision, SelectionError> {
        let python_hook = {
            #[cfg(feature = "python")]
            {
                matches!(self, Self::Python(_))
            }
            #[cfg(not(feature = "python"))]
            {
                false
            }
        };
        let (datasets, _guard) = if python_hook {
            (request.datasets.cloned(), None)
        } else {
            request
                .datasets
                .map(DatasetReader::invocation)
                .map_or((None, None), |(reader, guard)| (Some(reader), Some(guard)))
        };
        let scoped = SelectionRequest {
            data_contexts: request.data_contexts,
            authenticated_user: request.authenticated_user,
            client_id: request.client_id,
            query_id: request.query_id,
            query: request.query,
            context: request.context,
            datasets: datasets.as_ref(),
            async_hook_timeout_ms: request.async_hook_timeout_ms,
        };
        let request = &scoped;
        match self {
            Self::Rust(selector) => selector(request),
            Self::RustAsync(selector) => tokio::time::timeout(
                Duration::from_millis(u64::from(request.async_hook_timeout_ms)),
                selector(request),
            )
            .await
            .map_err(|_| SelectionError::Failed)?,
            #[cfg(feature = "python")]
            Self::Python(callback) => {
                let value = callback
                    .call_with_datasets(request_payload(request), request.datasets.cloned())
                    .await
                    .map_err(|error| {
                        tracing::error!(
                            query_id = request.query_id,
                            client_id = request.client_id,
                            category = %error,
                            "Python selector failed"
                        );
                        SelectionError::Failed
                    })?;
                if value.is_null() {
                    return Err(SelectionError::Denied);
                }
                if !value.is_object() {
                    return Err(SelectionError::Failed);
                }
                let selected: BackendDecision =
                    serde_json::from_value(value).map_err(|_| SelectionError::Failed)?;
                if [
                    &selected.target,
                    &selected.backend_login,
                    &selected.compute_type,
                ]
                .iter()
                .filter_map(|name| name.as_ref())
                .any(|name| name.trim().is_empty() || name.contains('\0'))
                {
                    return Err(SelectionError::Failed);
                }
                Ok(selected)
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

pub fn bind_select_backend_async<State, F, Decision>(
    state: State,
    function: F,
) -> AsyncBackendSelector
where
    State: Send + Sync + 'static,
    Decision: Into<BackendDecision> + 'static,
    F: for<'a> Fn(
            &'a SelectionRequest<'a>,
            Arc<State>,
        ) -> futures::future::BoxFuture<'a, Result<Decision, SelectionError>>
        + Send
        + Sync
        + 'static,
{
    let state = Arc::new(state);
    Arc::new(move |request| {
        let future = function(request, state.clone());
        Box::pin(async move { future.await.map(Into::into) })
    })
}

#[cfg(feature = "python")]
pub(super) fn request_payload(request: &SelectionRequest<'_>) -> Value {
    json!({
        "authenticated_user": request.authenticated_user,
        "client_id": request.client_id,
        "query_id": request.query_id,
        "sql": request.query.sql(),
        "query_ast": request.query.serialized_ast(),
        "data_contexts": request.data_contexts,
        "context": request.context,
    })
}

/// Bind host-owned state to a callback while retaining the query's borrow.
/// Callbacks execute in the query task and must be trusted, fast, and nonblocking.
/// Rust code has no enforceable execution timeout; initialize policy data at startup.
///
/// ```
/// use yasp::{hooks::{bind_select_backend, BackendSelection, SelectionRequest}, query::ParsedQuery};
/// let selector = bind_select_backend(
///     BackendSelection { target: "local".into(), backend_login: "reader".into() },
///     |_request, selection| Ok(selection.clone()),
/// );
/// let query = ParsedQuery::parse("SELECT 1").unwrap();
/// let selected = selector(&SelectionRequest {
///     data_contexts: &yasp::lineage::DataContexts::EMPTY,
///     authenticated_user: "analytics", client_id: 1, query_id: query.query_id(), query: &query,
///     context: &serde_json::Map::new(),
///     datasets: None, async_hook_timeout_ms: 2000,
/// }).unwrap();
/// assert_eq!(selected.backend_login.as_deref(), Some("reader"));
/// ```
///
/// Captured state must be safe to share between query tasks:
/// ```compile_fail
/// use yasp::hooks::{bind_select_backend, BackendSelection};
/// use std::rc::Rc;
/// let state = Rc::new(BackendSelection { target: "local".into(), backend_login: "reader".into() });
/// let _ = bind_select_backend(state, |_, selection| Ok(selection.as_ref().clone()));
/// ```
///
/// A callback must return a concrete pair or a decision:
/// ```compile_fail
/// use yasp::hooks::{bind_select_backend, SelectionRequest};
/// let _ = bind_select_backend((), |_request: &SelectionRequest<'_>, _state| Ok(()));
/// ```
///
/// The request argument must use the selection contract:
/// ```compile_fail
/// use yasp::hooks::{bind_select_backend, BackendSelection};
/// let _ = bind_select_backend((), |_sql: &str, _state| {
///     Ok(BackendSelection { target: "local".into(), backend_login: "reader".into() })
/// });
/// ```
pub fn bind_select_backend<State, F, Decision>(state: State, function: F) -> BackendSelector
where
    State: Send + Sync + 'static,
    Decision: Into<BackendDecision>,
    F: for<'a> Fn(&SelectionRequest<'a>, &State) -> Result<Decision, SelectionError>
        + Send
        + Sync
        + 'static,
{
    Arc::new(move |request| function(request, &state).map(Into::into))
}
