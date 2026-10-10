//! Typed callbacks that let a host application select configured backends.

use crate::query::ParsedQuery;
use std::sync::Arc;
use thiserror::Error;

#[path = "../hooks/builtin/mod.rs"]
pub mod builtin;

/// Query and authenticated identity supplied to a backend selector.
pub struct SelectionRequest<'a> {
    pub authenticated_user: &'a str,
    pub client_id: u64,
    pub query_id: u64,
    pub query: &'a ParsedQuery,
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
///     authenticated_user: "analytics", client_id: 1, query_id: 2, query: &query,
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
