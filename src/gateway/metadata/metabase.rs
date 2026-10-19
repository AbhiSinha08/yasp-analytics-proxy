//! Metabase's Redshift driver uses custom SQL rather than JDBC getTables/getColumns.
use super::*;

pub(super) fn templates() -> [(Kind, &'static str); 6] {
    [
        (
            Kind::MetabaseTables,
            include_str!("queries/metabase_redshift_tables.sql"),
        ),
        (
            Kind::MetabaseTables,
            include_str!("queries/metabase_redshift_tables_filtered.sql"),
        ),
        (
            Kind::MetabaseColumns,
            include_str!("queries/metabase_redshift_columns.sql"),
        ),
        (
            Kind::MetabaseColumns,
            include_str!("queries/metabase_redshift_columns_schema.sql"),
        ),
        (
            Kind::MetabaseColumns,
            include_str!("queries/metabase_redshift_columns_table.sql"),
        ),
        (
            Kind::MetabaseColumns,
            include_str!("queries/metabase_redshift_columns_filtered.sql"),
        ),
    ]
}

pub(super) fn columns(kind: Kind) -> Vec<(&'static str, u8)> {
    match kind {
        Kind::MetabaseTables => vec![
            ("name", 0),
            ("schema", 0),
            ("type", 0),
            ("description", 0),
            ("selectable", 3),
        ],
        Kind::MetabaseColumns => vec![
            ("name", 0),
            ("database-type", 0),
            ("database-position", 2),
            ("table-schema", 0),
            ("table-name", 0),
            ("pk?", 3),
            ("field-comment", 0),
        ],
        _ => unreachable!("Metabase metadata kind"),
    }
}

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
            "Metabase Redshift scans require PostgreSQL catalog privilege and key facts",
        ));
    }
    let mut count = 0;
    for relation in snapshot.relations.values() {
        let schema = &relation.id.schema;
        // These literal predicates reproduce the captured driver's fixed regex.
        if schema.starts_with("information_schema")
            || schema.contains("catalog_history")
            || schema.contains("pg_")
            || (matches!(request.kind, Kind::MetabaseTables) && schema.contains("metabase_cache_"))
            || (!request.schemas.is_empty() && !request.schemas.contains(schema))
            || (!request.tables.is_empty() && !request.tables.contains(&relation.id.name))
        {
            continue;
        }
        if matches!(request.kind, Kind::MetabaseTables) {
            let kind = match relation.kind {
                RelationKind::Table => "table",
                RelationKind::View => "view",
                RelationKind::MaterializedView => "materialized view",
                RelationKind::ForeignTable => "foreign table",
            };
            // The PostgreSQL provider filters SELECT visibility for the discovery identity.
            request
                .send(
                    output,
                    vec![
                        some(&relation.id.name),
                        some(schema),
                        some(kind),
                        relation.comment.clone(),
                        some("t"),
                    ],
                    settings,
                )
                .await?;
            count += 1;
        } else {
            for column in &relation.columns {
                let primary_key = column.primary_key.ok_or_else(|| {
                    ExecutionError::rejected(
                        "0A000",
                        "catalog provider does not report declared primary-key membership",
                    )
                })?;
                request
                    .send(
                        output,
                        vec![
                            some(&column.id.name),
                            some(&column.native_type.name),
                            some(column.ordinal - 1),
                            some(schema),
                            some(&relation.id.name),
                            some(if primary_key { "t" } else { "f" }),
                            column.comment.clone().filter(|s| !s.is_empty()),
                        ],
                        settings,
                    )
                    .await?;
                count += 1;
            }
        }
    }
    Ok(count)
}
