//! BI remark interpretation belongs to the host; metadata is already handled.
use crate::{
    hooks::{QueryContextError, QueryContextOutput, QueryContextRequest},
    query::SqlDialect,
};
pub fn metabase(
    request: QueryContextRequest<'_>,
    native_dialect: Option<SqlDialect>,
) -> Result<QueryContextOutput, QueryContextError> {
    let values = super::extractors::metabase_rs(request.sql)
        .ok()
        .flatten()
        .or_else(|| super::extractors::metabase_pg(request.sql).ok().flatten())
        .unwrap_or_default();
    let dialect = if values.get("query_type").and_then(serde_json::Value::as_str) == Some("native")
    {
        native_dialect
            .or(request.default_target_dialect)
            .ok_or(QueryContextError::AmbiguousDialect)?
    } else {
        request.gateway_dialect
    };
    Ok(QueryContextOutput {
        dialect: Some(dialect),
        values,
    })
}
pub fn redash(
    request: QueryContextRequest<'_>,
    dialect: Option<SqlDialect>,
) -> Result<QueryContextOutput, QueryContextError> {
    let values = super::extractors::redash(request.sql)
        .map_err(|_| QueryContextError::Failed)?
        .unwrap_or_default();
    let dialect = if values.is_empty() {
        request.gateway_dialect
    } else {
        dialect.unwrap_or(request.gateway_dialect)
    };
    Ok(QueryContextOutput {
        dialect: Some(dialect),
        values,
    })
}
