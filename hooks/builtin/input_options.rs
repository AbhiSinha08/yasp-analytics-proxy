//! Metabase remark interpretation is reusable host policy, not engine routing.

use crate::{
    hooks::InputRequest,
    query::{InputOptions, SqlDialect},
};

pub fn metabase(
    native_dialect: SqlDialect,
) -> impl Fn(InputRequest<'_>) -> InputOptions + Send + Sync {
    move |request| {
        let metadata = super::extractors::metabase_pg(request.sql)
            .ok()
            .flatten()
            .or_else(|| super::extractors::metabase_rs(request.sql).ok().flatten());
        if metadata
            .as_ref()
            .and_then(|fields| fields.get("query_type"))
            .and_then(serde_json::Value::as_str)
            == Some("native")
        {
            InputOptions::native(native_dialect)
        } else {
            InputOptions::frontend(request.frontend_dialect)
        }
    }
}
