use crate::hooks::{BackendSelection, SelectionError, SelectionRequest};

/// Select the configured default pair. Hosts can replace this callback with
/// their own routing policy.
pub fn passthrough(
    _request: &SelectionRequest<'_>,
    default: &BackendSelection,
) -> Result<BackendSelection, SelectionError> {
    Ok(default.clone())
}
