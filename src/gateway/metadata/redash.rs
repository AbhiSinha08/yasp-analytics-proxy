//! Redash 26.9 PostgreSQL/Redshift autocomplete over discovery-role schema facts.
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
    let mut rows = Vec::new();
    for relation in snapshot.relations.values() {
        let schema = &relation.id.schema;
        if relation.selectable.is_none() {
            return Err(ExecutionError::rejected(
                "0A000",
                "catalog provider does not report SELECT privilege facts",
            ));
        }
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
                    some(
                        if snapshot.source.dialect == crate::query::SqlDialect::Snowflake {
                            FrontendType::column(column)?.sql_name()
                        } else {
                            column.native_type.name.clone()
                        },
                    ),
                ],
                settings,
            )
            .await?;
    }
    Ok(count)
}

pub(super) async fn serve_postgres<S>(
    request: &Request,
    snapshot: &Snapshot,
    output: &mut S,
    settings: &GatewaySettings,
) -> ExecutionResult<u64>
where
    S: Sink<ResultEvent, Error = ExecutionError> + Send + Unpin,
{
    let mut count = 0;
    for relation in snapshot.relations.values() {
        if relation.id.schema == "pg_catalog" || relation.id.schema == "information_schema" {
            continue;
        }
        if relation.selectable.is_none() {
            return Err(ExecutionError::rejected(
                "0A000",
                "catalog provider does not report SELECT privilege facts",
            ));
        }
        if relation.selectable != Some(true) {
            continue;
        }
        for column in &relation.columns {
            let ty = if matches!(relation.kind, RelationKind::MaterializedView) {
                None
            } else {
                some(FrontendType::column(column)?.sql_name())
            };
            request
                .send(
                    output,
                    vec![
                        some(&relation.id.schema),
                        some(&relation.id.name),
                        some(&column.id.name),
                        ty,
                    ],
                    settings,
                )
                .await?;
            count += 1;
        }
    }
    Ok(count)
}
