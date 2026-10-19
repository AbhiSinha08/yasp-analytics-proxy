//! Metabase remark interpretation is reusable host policy, not engine routing.

use crate::{
    hooks::InputRequest,
    query::{InputOptions, SqlDialect},
};

pub fn metabase(
    native_dialect: SqlDialect,
) -> impl Fn(InputRequest<'_>) -> InputOptions + Send + Sync {
    metabase_with_default(native_dialect, None)
}

/// Missing/unknown remarks may use an application-configured native data default.
/// Explicit MBQL retains the frontend grammar; remarks never grant discovery.
pub fn metabase_with_default(
    native_dialect: SqlDialect,
    unmarked_data_dialect: Option<SqlDialect>,
) -> impl Fn(InputRequest<'_>) -> InputOptions + Send + Sync {
    move |request| {
        let metadata = super::extractors::metabase_pg(request.sql)
            .ok()
            .flatten()
            .or_else(|| super::extractors::metabase_rs(request.sql).ok().flatten());
        match metadata
            .as_ref()
            .and_then(|fields| fields.get("query_type"))
            .and_then(serde_json::Value::as_str)
        {
            Some("native") => InputOptions::native(native_dialect),
            Some("MBQL") => InputOptions::frontend(request.frontend_dialect),
            _ => unmarked_data_dialect.map_or_else(
                || InputOptions::frontend(request.frontend_dialect),
                |native| InputOptions::native_after_compatibility(request.frontend_dialect, native),
            ),
        }
    }
}
