//! Captured PostgreSQL SQLAlchemy requests and shared reflection facts.
use super::*;
use crate::catalog::{Constraint, ConstraintKind, Relation};
#[derive(Clone, Copy)]
pub(super) enum Kind {
    Tables,
    Views,
    Oid,
    Columns,
    PrimaryKeyColumns,
    ForeignKeys,
    Indexes,
    Uniques,
    Comment,
    Domains,
    PrimaryKeyName,
    Enums,
}

pub(super) fn templates() -> Vec<(super::snapshot::Kind, &'static str)> {
    [
        (
            Kind::Enums,
            include_str!("queries/sqlalchemy_postgresql_enums.sql"),
        ),
        (
            Kind::Domains,
            include_str!("queries/sqlalchemy_postgresql_domains.sql"),
        ),
        (
            Kind::PrimaryKeyName,
            include_str!("queries/sqlalchemy_postgresql_pk_name.sql"),
        ),
        (
            Kind::Tables,
            include_str!("queries/sqlalchemy_postgresql_tables.sql"),
        ),
        (
            Kind::Views,
            include_str!("queries/sqlalchemy_postgresql_views.sql"),
        ),
        (
            Kind::Oid,
            include_str!("queries/sqlalchemy_postgresql_oid.sql"),
        ),
        (
            Kind::Columns,
            include_str!("queries/sqlalchemy_postgresql_columns.sql"),
        ),
        (
            Kind::PrimaryKeyColumns,
            include_str!("queries/sqlalchemy_postgresql_pk_columns.sql"),
        ),
        (
            Kind::ForeignKeys,
            include_str!("queries/sqlalchemy_postgresql_foreign_keys.sql"),
        ),
        (
            Kind::Indexes,
            include_str!("queries/sqlalchemy_postgresql_indexes.sql"),
        ),
        (
            Kind::Uniques,
            include_str!("queries/sqlalchemy_postgresql_uniques.sql"),
        ),
        (
            Kind::Comment,
            include_str!("queries/sqlalchemy_postgresql_comment.sql"),
        ),
    ]
    .into_iter()
    .map(|(kind, sql)| (super::snapshot::Kind::PgSqlAlchemy(kind), sql))
    .collect()
}
pub(super) fn columns(kind: Kind) -> Vec<(&'static str, u8)> {
    match kind {
        Kind::Enums => vec![("name", 0), ("visible", 3), ("schema", 0), ("label", 0)],
        Kind::Domains => vec![
            ("name", 0),
            ("attype", 0),
            ("nullable", 3),
            ("default", 0),
            ("visible", 3),
            ("schema", 0),
        ],
        Kind::PrimaryKeyName => vec![("conname", 0)],
        Kind::Tables | Kind::Views => vec![("relname", 0)],
        Kind::Oid => vec![("oid", 8)],
        Kind::Columns => vec![
            ("attname", 0),
            ("format_type", 0),
            ("default", 0),
            ("attnotnull", 3),
            ("table_oid", 8),
            ("comment", 0),
            ("generated", 0),
            ("identity_options", 6),
        ],
        Kind::PrimaryKeyColumns => vec![("attname", 0)],
        Kind::ForeignKeys => vec![("conname", 0), ("condef", 0), ("conschema", 0)],
        Kind::Indexes => vec![
            ("relname", 0),
            ("indisunique", 3),
            ("indexprs", 0),
            ("attname", 0),
            ("attnum", 2),
            ("conrelid", 8),
            ("indkey", 0),
            ("indoption", 0),
            ("reloptions", 7),
            ("amname", 0),
            ("pg_get_expr", 0),
            ("indnkeyatts", 2),
        ],
        Kind::Uniques => vec![("name", 0), ("key", 5), ("col_num", 2), ("col_name", 0)],
        Kind::Comment => vec![("table_comment", 0)],
    }
}
pub(super) fn rows(kind: Kind, r: &Relation) -> ExecutionResult<Vec<Vec<Option<String>>>> {
    let keys = r
        .constraints
        .as_ref()
        .ok_or_else(|| ExecutionError::rejected("0A000", "catalog key facts are unavailable"))?;
    Ok(match kind {
        Kind::Tables if matches!(r.kind, RelationKind::Table | RelationKind::ForeignTable) => {
            vec![vec![some(&r.id.name)]]
        }
        Kind::Views if matches!(r.kind, RelationKind::View | RelationKind::MaterializedView) => {
            vec![vec![some(&r.id.name)]]
        }
        Kind::Oid => vec![vec![some(r.id.frontend_oid())]],
        Kind::Columns => r
            .columns
            .iter()
            .map(|c| {
                if c.identity {
                    return Err(ExecutionError::rejected(
                        "0A000",
                        "SQLAlchemy identity options are outside this metadata surface",
                    ));
                }
                let ty = FrontendType::column(c)?;
                Ok(vec![
                    some(&c.id.name),
                    some(ty.sql_name()),
                    c.default.clone(),
                    some(if c.nullable { "f" } else { "t" }),
                    some(r.id.frontend_oid()),
                    c.comment.clone(),
                    c.generated
                        .map(|v| if v { "s".into() } else { String::new() }),
                    None,
                ])
            })
            .collect::<ExecutionResult<Vec<_>>>()?,
        Kind::PrimaryKeyName => keys
            .iter()
            .filter(|k| k.kind == ConstraintKind::PrimaryKey)
            .map(|k| vec![some(&k.name)])
            .collect(),
        Kind::PrimaryKeyColumns => keys
            .iter()
            .filter(|k| k.kind == ConstraintKind::PrimaryKey)
            .flat_map(|k| k.columns.iter().map(|c| vec![some(c)]))
            .collect(),
        Kind::ForeignKeys => keys
            .iter()
            .filter_map(|key| {
                key.references.as_ref().map(|reference| {
                    vec![
                        some(&key.name),
                        some(definition(key)),
                        some(&reference.relation.schema),
                    ]
                })
            })
            .collect(),
        Kind::Uniques => {
            let mut rows = Vec::new();
            for key in keys.iter().filter(|k| k.kind == ConstraintKind::Unique) {
                let ordinals = key
                    .columns
                    .iter()
                    .map(|name| {
                        r.columns
                            .iter()
                            .find(|c| c.id.name == *name)
                            .expect("validated key column")
                            .ordinal
                    })
                    .collect::<Vec<_>>();
                let array = format!(
                    "{{{}}}",
                    ordinals
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(",")
                );
                for (name, ordinal) in key.columns.iter().zip(ordinals) {
                    rows.push(vec![
                        some(&key.name),
                        some(&array),
                        some(ordinal),
                        some(name),
                    ]);
                }
            }
            rows
        }
        Kind::Comment => vec![vec![r.comment.clone()]],
        _ => Vec::new(),
    })
}
pub(super) fn definition(key: &Constraint) -> String {
    let names = key
        .columns
        .iter()
        .map(|s| quote(s))
        .collect::<Vec<_>>()
        .join(", ");
    match &key.references {
        Some(reference) => {
            let mut sql = format!(
                "FOREIGN KEY ({names}) REFERENCES {}.{}({})",
                quote(&reference.relation.schema),
                quote(&reference.relation.name),
                reference
                    .columns
                    .iter()
                    .map(|s| quote(s))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            for (operation, rule) in [
                ("UPDATE", &reference.update_rule),
                ("DELETE", &reference.delete_rule),
            ] {
                if rule != "NO ACTION" {
                    sql.push_str(&format!(" ON {operation} {rule}"));
                }
            }
            sql
        }
        None => format!(
            "{} ({names})",
            if key.kind == ConstraintKind::PrimaryKey {
                "PRIMARY KEY"
            } else {
                "UNIQUE"
            }
        ),
    }
}
fn quote(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}
