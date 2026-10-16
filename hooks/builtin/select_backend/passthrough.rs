use crate::hooks::{BackendDecision, SelectionError, SelectionRequest};

/// Return the configured partial decision for engine resolution.
pub fn passthrough(
    _request: &SelectionRequest<'_>,
    default: &BackendDecision,
) -> Result<BackendDecision, SelectionError> {
    Ok(default.clone())
}
