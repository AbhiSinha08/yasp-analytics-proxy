//! Redash 26.9 Redshift autocomplete query, using discovery-role schema facts.
use super::*;

pub(super) async fn serve<S>(
    request: &Request,
    snapshot: &Snapshot,
    output: &mut S,
    settings: &GatewaySettings,
) -> ExecutionResult<u64>
where
    S: Sink<ResultEvent, Error = ExecutionError> + Send + Unpin,
{
    if snapshot.source.dialect != crate::query::SqlDialect::Postgresql {
        return Err(ExecutionError::rejected(
            "0A000",
            "Redash Redshift scans require PostgreSQL catalog privilege facts",
        ));
    }
    let mut rows = Vec::new();
    for relation in snapshot.relations.values() {
        let schema = &relation.id.schema;
        if matches!(
            schema.as_str(),
            "pg_internal" | "pg_catalog" | "information_schema"
        ) || like(schema, "pg_temp_%")
            || relation.selectable != Some(true)
        {
            continue;
        }
        // The PostgreSQL provider already restricts schemas to USAGE privilege.
        // A column-only grant does not satisfy this driver's table SELECT check.
        for column in &relation.columns {
            rows.push((relation, column));
        }
    }
    rows.sort_by_key(|(relation, column)| (&relation.id.name, column.ordinal, &relation.id.schema));
    let count = rows.len() as u64;
    for (relation, column) in rows {
        request
            .send(
                output,
                vec![
                    some(&relation.id.name),
                    some(&relation.id.schema),
                    some(&column.id.name),
                    some(&column.native_type.name),
                ],
                settings,
            )
            .await?;
    }
    Ok(count)
}
