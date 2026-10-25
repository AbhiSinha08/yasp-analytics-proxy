//! Compile with the consuming application and bind its own department policy.
use futures::future::BoxFuture;
use std::{collections::BTreeMap, sync::Arc};
use yasp::hooks::{
    AsyncBackendSelector, BackendDecision, SelectionError, SelectionRequest,
    bind_select_backend_async,
};

pub struct RoutingPolicy {
    pub target: String,
    pub department_logins: BTreeMap<String, String>,
}

pub fn bind(policy: RoutingPolicy) -> AsyncBackendSelector {
    bind_select_backend_async(policy, select_backend)
}

fn select_backend<'a>(
    request: &'a SelectionRequest<'a>,
    policy: Arc<RoutingPolicy>,
) -> BoxFuture<'a, Result<BackendDecision, SelectionError>> {
    Box::pin(async move {
        let user_id = request
            .query_context
            .get("user_id")
            .and_then(serde_json::Value::as_str)
            .ok_or(SelectionError::Denied)?;
        let user = request
            .datasets
            .ok_or(SelectionError::Failed)?
            .get("users", user_id)
            .await
            .map_err(|_| SelectionError::Failed)?
            .ok_or(SelectionError::Denied)?;
        let department = user
            .get("department")
            .and_then(serde_json::Value::as_str)
            .ok_or(SelectionError::Denied)?;
        let login = policy
            .department_logins
            .get(department)
            .ok_or(SelectionError::Denied)?;
        Ok(BackendDecision {
            target: Some(policy.target.clone()),
            backend_login: Some(login.clone()),
            compute_type: None,
        })
    })
}
