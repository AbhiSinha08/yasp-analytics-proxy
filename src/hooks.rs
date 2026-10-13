//! Host-owned context, backend selection, and cache eligibility callbacks.

use crate::query::ParsedQuery;
use futures::future::BoxFuture;
#[cfg(feature = "python")]
use serde_json::json;
use serde_json::{Map, Value};
use std::{sync::Arc, time::Duration};
use thiserror::Error;

mod payload;

#[path = "../hooks/builtin/mod.rs"]
pub mod builtin;

#[cfg(feature = "python")]
pub mod python;

/// Query and authenticated identity supplied to a backend selector.
pub struct SelectionRequest<'a> {
    pub authenticated_user: &'a str,
    pub client_id: u64,
    pub query_id: &'a str,
    pub query: &'a ParsedQuery,
    pub context: &'a Map<String, Value>,
}

/// Available query metadata before the application derives context.
#[derive(Clone, Copy)]
pub struct ContextRequest<'a> {
    pub authenticated_user: &'a str,
    pub frontend_database: &'a str,
    pub client_id: u64,
    pub query_id: &'a str,
    pub query: &'a ParsedQuery,
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
                Self::Rust(callback) => Value::Object(callback(request).await?),
                #[cfg(feature = "python")]
                Self::Python(callback) => {
                    callback
                        .call(json!({
                            "authenticated_user": request.authenticated_user,
                            "frontend_database": request.frontend_database,
                            "client_id": request.client_id,
                            "query_id": request.query_id,
                            "sql": request.query.sql(),
                        }))
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
                tracing::error!(query_id = request.query_id, client_id = request.client_id, category = %error, "context unavailable; using default context");
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

#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("cache eligibility callback failed")]
pub struct CacheEligibilityError;

pub type BackendSelector = Arc<
    dyn for<'a> Fn(&SelectionRequest<'a>) -> Result<BackendSelection, SelectionError> + Send + Sync,
>;

pub type CacheEligibilityCallback =
    Arc<dyn for<'a> Fn(&SelectionRequest<'a>) -> Result<bool, CacheEligibilityError> + Send + Sync>;

/// Registered selection behavior; the router owns validation and acquisition.
pub enum RoutedSelector {
    Rust(BackendSelector),
    #[cfg(feature = "python")]
    Python(python::PythonCallback),
}

/// Registered cache eligibility behavior supplied by the host application.
pub enum CacheEligibility {
    Rust(CacheEligibilityCallback),
    #[cfg(feature = "python")]
    Python(python::PythonCallback),
}

impl From<CacheEligibilityCallback> for CacheEligibility {
    fn from(callback: CacheEligibilityCallback) -> Self {
        Self::Rust(callback)
    }
}

#[cfg(feature = "python")]
impl From<python::PythonCallback> for CacheEligibility {
    fn from(callback: python::PythonCallback) -> Self {
        Self::Python(callback)
    }
}

impl CacheEligibility {
    pub async fn eligible(
        &self,
        request: &SelectionRequest<'_>,
    ) -> Result<bool, CacheEligibilityError> {
        match self {
            Self::Rust(callback) => callback(request),
            #[cfg(feature = "python")]
            Self::Python(callback) => {
                let value = callback
                    .call(request_payload(request))
                    .await
                    .map_err(|error| {
                        tracing::error!(query_id = request.query_id, client_id = request.client_id, category = %error, "Python cache eligibility callback failed");
                        CacheEligibilityError
                    })?;
                value.as_bool().ok_or(CacheEligibilityError)
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

impl From<BackendSelector> for RoutedSelector {
    fn from(selector: BackendSelector) -> Self {
        Self::Rust(selector)
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
    ) -> Result<BackendSelection, SelectionError> {
        match self {
            Self::Rust(selector) => selector(request),
            #[cfg(feature = "python")]
            Self::Python(callback) => {
                let value = callback.call(request_payload(request)).await.map_err(|error| {
                    tracing::error!(query_id = request.query_id, client_id = request.client_id, category = %error, "Python selector failed");
                    SelectionError::Failed
                })?;
                if value.is_null() {
                    return Err(SelectionError::Denied);
                }
                if !value.is_object() {
                    return Err(SelectionError::Failed);
                }
                let selected: BackendSelection =
                    serde_json::from_value(value).map_err(|_| SelectionError::Failed)?;
                if [&selected.target, &selected.backend_login]
                    .iter()
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

#[cfg(feature = "python")]
fn request_payload(request: &SelectionRequest<'_>) -> Value {
    json!({
        "authenticated_user": request.authenticated_user,
        "client_id": request.client_id,
        "query_id": request.query_id,
        "sql": request.query.sql(),
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
///     authenticated_user: "analytics", client_id: 1, query_id: query.query_id(), query: &query,
///     context: &serde_json::Map::new(),
/// }).unwrap();
/// assert_eq!(selected.backend_login, "reader");
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
/// A callback must return a backend selection:
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
pub fn bind_select_backend<State, F>(state: State, function: F) -> BackendSelector
where
    State: Send + Sync + 'static,
    F: for<'a> Fn(&SelectionRequest<'a>, &State) -> Result<BackendSelection, SelectionError>
        + Send
        + Sync
        + 'static,
{
    Arc::new(move |request| function(request, &state))
}

/// Bind host-owned state to a cache eligibility callback.
pub fn bind_cache_eligibility<State, F>(state: State, function: F) -> CacheEligibilityCallback
where
    State: Send + Sync + 'static,
    F: for<'a> Fn(&SelectionRequest<'a>, &State) -> Result<bool, CacheEligibilityError>
        + Send
        + Sync
        + 'static,
{
    Arc::new(move |request| function(request, &state))
}
