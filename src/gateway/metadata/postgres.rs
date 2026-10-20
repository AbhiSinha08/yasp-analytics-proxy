//! PostgreSQL reflection uses source facts; Snowflake compatibility stays separate.
use super::{snapshot::Kind, *};
use crate::catalog::{PostgresMetadata, PostgresRelation, Relation};
type Row = Vec<Option<String>>;
fn boolean(value: bool) -> Option<String> {
    some(if value { "t" } else { "f" })
}

pub(super) fn global(
    kind: Kind,
    pg: &PostgresMetadata,
    profile: FrontendProfile,
) -> Option<Vec<Row>> {
    use super::sqlalchemy::Kind as P;
    Some(match kind {
        Kind::Version(_) if profile == FrontendProfile::Postgres => vec![vec![some(&pg.version)]],
        Kind::Setting(name, _) => vec![vec![some(match (name, profile) {
            ("server_version", FrontendProfile::Redshift) => "8.0.2",
            ("server_version_num", FrontendProfile::Redshift) => "80002",
            ("server_version", _) => &pg.server_version,
            _ => &pg.server_version_num,
        })]],
        Kind::VersionSetting => vec![vec![some("server_version"), some(&pg.server_version)]],
        Kind::CurrentSchema => vec![vec![pg.current_schema.clone()]],
        Kind::MaxIndexKeys => vec![vec![some(&pg.max_index_keys)]],
        Kind::Hstore => pg
            .types
            .iter()
            .filter(|t| t.name == "hstore")
            .map(|t| vec![some(t.oid), some(t.array_oid)])
            .collect(),
        Kind::PgSqlAlchemy(P::Domains) => pg
            .types
            .iter()
            .filter(|t| t.kind == "d")
            .map(|t| {
                vec![
                    some(&t.name),
                    some(&t.base_type),
                    boolean(t.nullable),
                    t.default.clone(),
                    boolean(t.visible),
                    some(&t.schema),
                ]
            })
            .collect(),
        Kind::PgSqlAlchemy(P::Enums) => pg
            .types
            .iter()
            .filter(|t| t.kind == "e")
            .map(|t| {
                vec![
                    some(&t.name),
                    boolean(t.visible),
                    some(&t.schema),
                    t.label.clone(),
                ]
            })
            .collect(),
        _ => return None,
    })
}

pub(super) fn rows(
    kind: Kind,
    r: &Relation,
    pg: &PostgresRelation,
    profile: FrontendProfile,
) -> ExecutionResult<Vec<Row>> {
    use super::sqlalchemy::Kind as P;
    use crate::discovery::redshift::Kind as R;
    let mut rows = match kind {
        Kind::SqlAlchemy(R::Relations) => vec![vec![
            some(if pg.kind == "p" { "r" } else { &pg.kind }),
            some(pg.schema_oid),
            some(&r.id.schema),
            some(pg.oid),
            some(&r.id.name),
            None,
            some(pg.owner_id),
            some(&pg.owner_name),
            pg.view_definition.clone(),
            pg.privileges.clone(),
        ]],
        Kind::SqlAlchemy(R::Columns) => r
            .columns
            .iter()
            .map(|c| {
                let native = &pg.columns[&c.ordinal];
                vec![
                    some(&r.id.schema),
                    some(&r.id.name),
                    some(&c.id.name),
                    None,
                    some(&native.formatted_type),
                    None,
                    None,
                    boolean(native.not_null),
                    c.comment.clone(),
                    c.default.clone(),
                    some(c.ordinal),
                    some(&native.formatted_type),
                    c.default.clone(),
                    some(pg.schema_oid),
                    some(pg.oid),
                ]
            })
            .collect(),
        Kind::SqlAlchemy(R::Constraints) => pg
            .constraints
            .iter()
            .flat_map(|k| {
                k.columns.iter().zip(&k.ordinals).map(|(name, pos)| {
                    vec![
                        some(&r.id.schema),
                        some(&r.id.name),
                        some(&k.kind),
                        some(&k.name),
                        some(array(&k.ordinals)),
                        some(pos),
                        some(name),
                        some(&k.definition),
                        some(pg.schema_oid),
                        some(pg.oid),
                    ]
                })
            })
            .collect(),
        Kind::PgSqlAlchemy(P::Tables) => {
            if matches!(pg.kind.as_str(), "r" | "p") {
                vec![vec![some(&r.id.name)]]
            } else {
                Vec::new()
            }
        }
        Kind::PgSqlAlchemy(P::Views) => {
            if matches!(pg.kind.as_str(), "v" | "m") {
                vec![vec![some(&r.id.name)]]
            } else {
                Vec::new()
            }
        }
        Kind::PgSqlAlchemy(P::Oid) => vec![vec![some(pg.oid)]],
        Kind::PgSqlAlchemy(P::Columns) => r
            .columns
            .iter()
            .map(|c| {
                let native = &pg.columns[&c.ordinal];
                vec![
                    some(&c.id.name),
                    some(&native.formatted_type),
                    c.default.clone(),
                    boolean(native.not_null),
                    some(pg.oid),
                    c.comment.clone(),
                    some(&native.generated),
                    native.identity_options.as_ref().map(ToString::to_string),
                ]
            })
            .collect(),
        Kind::PgSqlAlchemy(P::ForeignKeys) => pg
            .constraints
            .iter()
            .filter(|k| k.kind == "f")
            .map(|k| vec![some(&k.name), some(&k.definition), k.foreign_schema.clone()])
            .collect(),
        Kind::PgSqlAlchemy(P::Indexes) => pg
            .indexes
            .iter()
            .filter(|i| !i.primary)
            .map(|i| {
                vec![
                    some(&i.name),
                    boolean(i.unique),
                    i.expressions.clone(),
                    i.column_name.clone(),
                    i.column_ordinal.map(|v| v.to_string()),
                    i.constraint_relation.map(|v| v.to_string()),
                    some(&i.keys),
                    some(&i.options),
                    i.relation_options.clone(),
                    some(&i.method),
                    i.predicate.clone(),
                    some(i.key_count),
                ]
            })
            .collect(),
        Kind::Indexes if profile == FrontendProfile::Postgres => pg
            .indexes
            .iter()
            .map(|i| {
                vec![
                    some(&r.id.database),
                    some(&r.id.schema),
                    some(&r.id.name),
                    boolean(!i.unique),
                    None,
                    some(&i.name),
                    some(if i.clustered {
                        1
                    } else if i.method == "hash" {
                        2
                    } else {
                        3
                    }),
                    some(i.ordinal),
                    some(&i.definition),
                    i.direction.clone(),
                    some(&i.cardinality),
                    some(&i.pages),
                    i.predicate.clone(),
                    i.comment.clone(),
                ]
            })
            .collect(),
        Kind::ForeignKeys => {
            pg.constraints
                .iter()
                .filter(|k| k.kind == "f")
                .flat_map(|k| {
                    k.columns.iter().zip(&k.foreign_columns).enumerate().map(
                        |(i, (local, remote))| {
                            vec![
                                some(&r.id.database),
                                k.foreign_schema.clone(),
                                k.foreign_table.clone(),
                                some(remote),
                                some(&r.id.database),
                                some(&r.id.schema),
                                some(&r.id.name),
                                some(local),
                                some(i + 1),
                                some(action(&k.update_rule)),
                                some(action(&k.delete_rule)),
                                some(&k.name),
                                k.foreign_constraint.clone(),
                                some(if k.deferred {
                                    5
                                } else if k.deferrable {
                                    6
                                } else {
                                    7
                                }),
                            ]
                        },
                    )
                })
                .collect()
        }
        Kind::Metabase(0) => vec![vec![
            None,
            some(&r.id.schema),
            some(&r.id.name),
            boolean(pg.privileges_update),
            boolean(pg.privileges_select),
            boolean(pg.privileges_insert),
            boolean(pg.privileges_delete),
        ]],
        Kind::Metabase(1) => vec![vec![
            some(&r.id.schema),
            some(&r.id.name),
            some(match pg.kind.as_str() {
                "p" => "PARTITIONED TABLE",
                "v" => "VIEW",
                "m" => "MATERIALIZED VIEW",
                "f" => "FOREIGN TABLE",
                _ => "TABLE",
            }),
            r.comment.clone(),
            pg.estimated_rows.map(|v| v.to_string()),
        ]],
        Kind::Metabase(2) => r
            .columns
            .iter()
            .map(|c| {
                vec![
                    some(&c.id.name),
                    some(&c.native_type.name),
                    some(c.ordinal - 1),
                    some(&r.id.schema),
                    some(&r.id.name),
                    c.primary_key.and_then(boolean),
                    c.comment.clone(),
                    boolean(!c.nullable && c.default.is_none() && !c.identity),
                    c.default.clone(),
                    boolean(
                        c.identity
                            || c.default
                                .as_deref()
                                .is_some_and(|s| s.starts_with("nextval(")),
                    ),
                    c.generated.and_then(boolean),
                    boolean(c.nullable),
                ]
            })
            .collect(),
        Kind::Metabase(4) => pg
            .indexes
            .iter()
            .filter(|i| i.ordinal == 1 && i.predicate.is_none())
            .map(|i| vec![some(&r.id.schema), some(&r.id.name), some(&i.definition)])
            .collect(),
        Kind::RedashColumns => {
            if r.selectable == Some(true) {
                r.columns
                    .iter()
                    .map(|c| {
                        vec![
                            some(&r.id.schema),
                            some(&r.id.name),
                            some(&c.id.name),
                            pg.columns[&c.ordinal].information_schema_type.clone(),
                        ]
                    })
                    .collect()
            } else {
                Vec::new()
            }
        }
        Kind::PgSqlAlchemy(p) => super::sqlalchemy::rows(p, r)?,
        Kind::Indexes => Vec::new(),
        _ => {
            return Err(ExecutionError::rejected(
                "0A000",
                "unsupported PostgreSQL snapshot response",
            ));
        }
    };
    match kind {
        Kind::ForeignKeys => rows.sort_by_key(|r| {
            (
                r[1].clone(),
                r[2].clone(),
                r[11].clone(),
                r[8].as_deref().and_then(|s| s.parse::<u32>().ok()),
            )
        }),
        Kind::Indexes => rows.sort_by_key(|r| {
            (
                r[3].clone(),
                r[6].clone(),
                r[5].clone(),
                r[7].as_deref().and_then(|s| s.parse::<u32>().ok()),
            )
        }),
        Kind::PgSqlAlchemy(P::ForeignKeys | P::Indexes) => rows.sort_by_key(|r| r[0].clone()),
        _ => {}
    }
    Ok(rows)
}
fn array(values: &[u32]) -> String {
    format!(
        "{{{}}}",
        values
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(",")
    )
}
fn action(value: &str) -> u8 {
    match value {
        "c" => 0,
        "r" => 1,
        "n" => 2,
        "d" => 4,
        _ => 3,
    }
}

pub(super) async fn serve<S>(
    query: &super::snapshot::Request,
    request: &Request,
    snapshot: &Snapshot,
    profile: FrontendProfile,
    output: &mut S,
    settings: &GatewaySettings,
) -> ExecutionResult<Option<u64>>
where
    S: Sink<ResultEvent, Error = ExecutionError> + Send + Unpin,
{
    let Some(pg) = &snapshot.postgres else {
        return Ok(None);
    };
    if let Some(rows) = global(query.kind, pg, profile) {
        let count = rows.len() as u64;
        for row in rows {
            request.send(output, row, settings).await?;
        }
        return Ok(Some(count));
    }
    if matches!(
        query.kind,
        Kind::PrimaryKeys
            | Kind::Metabase(3)
            | Kind::ConnectionCheck
            | Kind::Schemas
            | Kind::Version(_)
    ) {
        return Ok(None);
    }
    let mut count = 0;
    for r in snapshot.relations.values() {
        let native = r
            .native_id
            .as_deref()
            .and_then(|s| s.parse::<u32>().ok())
            .and_then(|oid| pg.relations.get(&oid))
            .ok_or_else(|| {
                ExecutionError::rejected("55000", "PostgreSQL relation facts are unavailable")
            })?;
        if query.oid.is_some_and(|oid| native.oid != oid)
            || query.schema.as_ref().is_some_and(|s| &r.id.schema != s)
            || query.table.as_ref().is_some_and(|s| &r.id.name != s)
            || (!query.schemas.is_empty() && !query.schemas.contains(&r.id.schema))
        {
            continue;
        }
        for row in rows(query.kind, r, native, profile)? {
            request.send(output, row, settings).await?;
            count += 1;
        }
    }
    Ok(Some(count))
}
