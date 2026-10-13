use crate::hooks::{CacheEligibilityError, SelectionRequest};
/// Keep matching queries fresh using case-insensitive literal SQL fragments.
pub fn exclude_sql_containing(
    request: &SelectionRequest<'_>,
    excluded_sql_fragments: &[String],
) -> Result<bool, CacheEligibilityError> {
    let sql = request.query.sql().to_lowercase();
    Ok(!excluded_sql_fragments
        .iter()
        .any(|fragment| sql.contains(&fragment.to_lowercase())))
}
