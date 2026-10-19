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
    let mut count = 0;
    // Redshift's Metabase driver lowercases its field filters, including names
    // learned from table discovery. Keep the catalog and returned names intact.
    let folded_filters = matches!(request.kind, Kind::MetabaseColumns)
        && snapshot.source.dialect == crate::query::SqlDialect::Snowflake;
    let matches = |filters: &[String], name: &str| {
        filters.is_empty()
            || filters.iter().any(|filter| filter == name)
            || (folded_filters && filters.contains(&name.to_lowercase()))
    };
    let mut folded_names = std::collections::BTreeMap::new();
    for relation in snapshot.relations.values() {
        let schema = &relation.id.schema;
        // These literal predicates reproduce the captured driver's fixed regex.
        if schema.starts_with("information_schema")
            || schema.contains("catalog_history")
            || schema.contains("pg_")
            || (matches!(request.kind, Kind::MetabaseTables) && schema.contains("metabase_cache_"))
            || !matches(&request.schemas, schema)
            || !matches(&request.tables, &relation.id.name)
        {
            continue;
        }
        if folded_filters {
            for (scope, name, filtered) in [
                (None, schema, !request.schemas.is_empty()),
                (Some(schema), &relation.id.name, !request.tables.is_empty()),
            ] {
                if filtered
                    && let Some(previous) = folded_names.insert((scope, name.to_lowercase()), name)
                    && previous != name
                {
                    return Err(ExecutionError::rejected(
                        "0A000",
                        "Metabase Redshift field filters are ambiguous for case-distinct source names",
                    ));
                }
            }
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
                        relation
                            .selectable
                            .map(|v| if v { "t".into() } else { "f".into() }),
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
                            some(
                                if snapshot.source.dialect == crate::query::SqlDialect::Snowflake {
                                    FrontendType::column(column)?.sql_name()
                                } else {
                                    column.native_type.name.clone()
                                },
                            ),
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

pub(super) fn foreign_templates(
    profile: FrontendProfile,
) -> Vec<(super::foreign::Kind, &'static str)> {
    // The Redshift driver inherits PostgreSQL's bulk foreign-key query.
    let mut templates = vec![(
        super::foreign::Kind::Metabase(3),
        include_str!("queries/metabase_foreign_keys.sql"),
    )];
    if profile == FrontendProfile::Postgres {
        templates.extend(
            [
                include_str!("queries/metabase_postgresql_privileges.sql"),
                include_str!("queries/metabase_postgresql_tables.sql"),
                include_str!("queries/metabase_postgresql_columns.sql"),
                include_str!("queries/metabase_postgresql_foreign_keys.sql"),
                include_str!("queries/metabase_postgresql_indexes.sql"),
            ]
            .into_iter()
            .enumerate()
            .map(|(i, sql)| (super::foreign::Kind::Metabase(i as u8), sql)),
        );
    }
    templates
}
pub(super) fn postgres_columns(kind: u8) -> Vec<(&'static str, u8)> {
    match kind {
        0 => vec![
            ("role", 0),
            ("schema", 0),
            ("table", 0),
            ("update", 3),
            ("select", 3),
            ("insert", 3),
            ("delete", 3),
        ],
        1 => vec![
            ("schema", 0),
            ("name", 0),
            ("type", 0),
            ("description", 0),
            ("estimated_row_count", 4),
        ],
        2 => vec![
            ("name", 0),
            ("database-type", 0),
            ("database-position", 2),
            ("table-schema", 0),
            ("table-name", 0),
            ("pk?", 3),
            ("field-comment", 0),
            ("database-required", 3),
            ("database-default", 0),
            ("database-is-auto-increment", 3),
            ("database-is-generated", 3),
            ("database-is-nullable", 3),
        ],
        3 => vec![
            ("fk-table-schema", 0),
            ("fk-table-name", 0),
            ("fk-column-name", 0),
            ("pk-table-schema", 0),
            ("pk-table-name", 0),
            ("pk-column-name", 0),
        ],
        4 => vec![("table-schema", 0), ("table-name", 0), ("field-name", 0)],
        _ => unreachable!("captured Metabase metadata kind"),
    }
}
pub(super) async fn serve_postgres<S>(
    kind: u8,
    schemas: &[String],
    request: &Request,
    snapshot: &Snapshot,
    output: &mut S,
    settings: &GatewaySettings,
) -> ExecutionResult<u64>
where
    S: Sink<ResultEvent, Error = ExecutionError> + Send + Unpin,
{
    let boolean = |value: bool| some(if value { "t" } else { "f" });
    let mut count = 0;
    for r in snapshot.relations.values() {
        if (!schemas.is_empty() && !schemas.contains(&r.id.schema))
            || r.id.schema.starts_with("pg_")
            || r.id.schema == "information_schema"
        {
            continue;
        }
        let mut rows = Vec::new();
        match kind {
            0 => rows.push(vec![
                None,
                some(&r.id.schema),
                some(&r.id.name),
                None,
                r.selectable.and_then(boolean),
                None,
                None,
            ]),
            1 => rows.push(vec![
                some(&r.id.schema),
                some(&r.id.name),
                some(
                    if matches!(r.kind, RelationKind::View | RelationKind::MaterializedView) {
                        "VIEW"
                    } else {
                        "TABLE"
                    },
                ),
                r.comment.clone(),
                None,
            ]),
            2 => {
                for c in &r.columns {
                    let ty = FrontendType::column(c)?;
                    rows.push(vec![
                        some(&c.id.name),
                        some(ty.name),
                        some(c.ordinal - 1),
                        some(&r.id.schema),
                        some(&r.id.name),
                        c.primary_key.and_then(boolean),
                        c.comment.clone(),
                        boolean(!c.nullable && c.default.is_none() && !c.identity),
                        c.default.clone(),
                        boolean(c.identity),
                        c.generated.and_then(boolean),
                        boolean(c.nullable),
                    ]);
                }
            }
            3 => {
                for key in r.constraints.as_ref().ok_or_else(|| {
                    ExecutionError::rejected("0A000", "catalog key facts are unavailable")
                })? {
                    if let Some(reference) = &key.references {
                        for (local, remote) in key.columns.iter().zip(&reference.columns) {
                            rows.push(vec![
                                some(&r.id.schema),
                                some(&r.id.name),
                                some(local),
                                some(&reference.relation.schema),
                                some(&reference.relation.name),
                                some(remote),
                            ]);
                        }
                    }
                }
            }
            4 => {} // Standard Snowflake tables have no physical PostgreSQL indexes.
            _ => unreachable!("captured Metabase metadata kind"),
        }
        for row in rows {
            request.send(output, row, settings).await?;
            count += 1;
        }
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{Relation, RelationId, SourceIdentity};

    #[tokio::test]
    async fn lowercase_field_filters_reject_ambiguous_source_identifiers() {
        let sql = include_str!("queries/metabase_redshift_columns_filtered.sql")
            .replace("__YASP_SCHEMAS__", "public")
            .replace("__YASP_TABLES__", "orders");
        let request = Request::recognize(&sql, FrontendProfile::Redshift).unwrap();
        for names in [
            [("PUBLIC", "ORDERS"), ("Public", "ORDERS")],
            [("PUBLIC", "ORDERS"), ("PUBLIC", "Orders")],
        ] {
            let relations = names
                .into_iter()
                .map(|(schema, name)| {
                    let id = RelationId {
                        database: "YASP_DEMO".into(),
                        schema: schema.into(),
                        name: name.into(),
                    };
                    (
                        id.clone(),
                        Relation {
                            id,
                            kind: RelationKind::Table,
                            comment: None,
                            native_id: None,
                            selectable: Some(true),
                            view_definition: None,
                            constraints: Some(Vec::new()),
                            columns: Vec::new(),
                        },
                    )
                })
                .collect();
            let snapshot = Snapshot {
                generation: 1,
                loaded_at: std::time::SystemTime::now(),
                source: SourceIdentity {
                    dialect: crate::query::SqlDialect::Snowflake,
                    endpoint: "fixture".into(),
                    principal: "reader".into(),
                    role: None,
                    warehouse: None,
                    default_schema: None,
                },
                schemas: Default::default(),
                relations,
            };
            let mut output = futures::sink::drain().sink_map_err(
                |never: std::convert::Infallible| -> ExecutionError { match never {} },
            );
            let result = serve(
                &request,
                &snapshot,
                &mut output,
                &GatewaySettings::default(),
            )
            .await;
            assert!(matches!(
                result,
                Err(ExecutionError::Rejected { code: "0A000", .. })
            ));
        }
    }
}
