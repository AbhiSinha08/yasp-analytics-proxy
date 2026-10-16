//! Application-owned route choice over immutable query metadata.
//! The gateway validates each returned configured target/login pair.

#[cfg(feature = "python")]
use super::python;
use crate::query::ParsedQuery;
#[cfg(feature = "python")]
use serde_json::json;
use serde_json::{Map, Value};
use std::sync::Arc;
use thiserror::Error;

/// Query and authenticated identity supplied to a backend selector.
pub struct SelectionRequest<'a> {
    pub authenticated_user: &'a str,
    pub client_id: u64,
    pub query_id: &'a str,
    pub query: &'a ParsedQuery,
    pub context: &'a Map<String, Value>,
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
    dyn for<'a> Fn(&SelectionRequest<'a>) -> Result<BackendSelection, SelectionError> + Send + Sync,
>;

/// Registered selection behavior; the router owns validation and acquisition.
pub enum RoutedSelector {
    Rust(BackendSelector),
    #[cfg(feature = "python")]
    Python(python::PythonCallback),
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
                let value = callback
                    .call(request_payload(request))
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
pub(super) fn request_payload(request: &SelectionRequest<'_>) -> Value {
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
