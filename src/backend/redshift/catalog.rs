//! Native Redshift definitions, read through the ordinary admitted pgwire lease.

use crate::{
    backend::postgres::{PostgresBackend, PostgresTransaction, decode_row},
    catalog::{
        CatalogError, CatalogSettings, Constraint, ConstraintKind, ForeignKey, RelationId, Rows,
    },
    query::{ParsedQuery, SqlDialect},
};
use futures::Sink;
use pgwire::{
    error::{PgWireError, PgWireResult},
    messages::PgWireBackendMessage,
};
use std::{
    collections::BTreeMap,
    pin::Pin,
    task::{Context, Poll},
};

type Row = Vec<Option<String>>;
type Facts = BTreeMap<RelationId, (Option<String>, Vec<Constraint>)>;

impl PostgresBackend {
    pub(crate) async fn redshift_catalog_rows(
        &self,
        settings: &CatalogSettings,
    ) -> Result<Rows, CatalogError> {
        let sql = |template: &str| scoped_sql(template, settings);
        let mut transaction = self
            .route()
            .acquire(settings.refresh_timeout)
            .await
            .map_err(|_| CatalogError::SourceUnavailable)?
            .begin(settings.refresh_timeout, settings.refresh_timeout)
            .await
            .map_err(|_| CatalogError::SourceUnavailable)?;
        let mut rows = Rows::new(settings);
        let mut facts = Facts::new();
        let mut remaining_rows = settings.max_rows;
        let mut remaining_bytes = settings.max_bytes;
        let operation = async {
            // Redshift's HAS_*_PRIVILEGE functions are leader-only and cannot
            // appear in a statement that also references SVV system views.
            let privileges = read(&mut transaction, &sql(include_str!("catalog_privileges.sql")),
                5, settings, &mut remaining_rows, &mut remaining_bytes).await?;
            let privileges = Privileges::collect(privileges)?;
            let columns = read(&mut transaction, &sql(include_str!("catalog_columns.sql")),
                22, settings, &mut remaining_rows, &mut remaining_bytes).await?;
            for mut row in columns {
                if !privileges.apply(&mut row)? { continue; }
                if let Some(name) = &row[2] {
                    facts.entry(RelationId {
                        database: required(&row, 0)?.into(),
                        schema: required(&row, 1)?.into(),
                        name: name.clone(),
                    }).or_default();
                }
                rows.push(row)?;
            }
            let views = read(&mut transaction, &sql(
                "SELECT table_catalog, table_schema, table_name, view_definition FROM information_schema.views WHERE table_catalog = current_database() AND table_schema IN (__SCHEMAS__) ORDER BY table_schema, table_name LIMIT __LIMIT__"),
                4, settings, &mut remaining_rows, &mut remaining_bytes).await?;
            for row in views {
                let id = relation(&row)?;
                if let Some((definition, _)) = facts.get_mut(&id) {
                    *definition = row[3].clone();
                }
            }
            // AWS JDBC reads 32 ordered conkey/confkey slots and PG_DEPEND.
            // Parentheses keep variable subscripts unambiguous with Redshift's
            // bracket-quoted identifiers in the SQL parser.
            let keys = read(&mut transaction, &sql(include_str!("catalog_keys.sql")),
                14, settings, &mut remaining_rows, &mut remaining_bytes).await?;
            append_keys(keys, &mut facts)?;
            for (id, (view, keys)) in facts {
                rows.facts(id, view, keys)?;
            }
            Ok::<_, CatalogError>(rows)
        }.await;
        let cleanup = transaction.finish(false).await;
        let rows = operation?;
        cleanup.map_err(|_| CatalogError::SourceUnavailable)?;
        Ok(rows)
    }
}

fn scoped_sql(template: &str, settings: &CatalogSettings) -> String {
    let schemas = settings
        .schemas
        .iter()
        // Redshift interprets backslash escapes even in ordinary string literals.
        .map(|s| format!("'{}'", s.replace('\\', "\\\\").replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(",");
    // Replace the numeric bound first: a source identifier may itself contain
    // the literal placeholder text, which must remain part of that identifier.
    template
        .replace("__LIMIT__", &(settings.max_rows + 1).to_string())
        .replace("__SCHEMAS__", &schemas)
}

#[derive(Default)]
struct Privileges {
    schemas: BTreeMap<(String, String), bool>,
    relations: BTreeMap<RelationId, bool>,
}
impl Privileges {
    fn collect(rows: Vec<Row>) -> Result<Self, CatalogError> {
        let mut privileges = Self::default();
        for row in rows {
            let scope = (required(&row, 0)?.to_owned(), required(&row, 1)?.to_owned());
            let usage = flag(required(&row, 3)?)?;
            if privileges
                .schemas
                .insert(scope.clone(), usage)
                .is_some_and(|previous| previous != usage)
            {
                return Err(CatalogError::InvalidMetadata);
            }
            if let Some(name) = &row[2] {
                let id = RelationId {
                    database: scope.0,
                    schema: scope.1,
                    name: name.clone(),
                };
                if privileges
                    .relations
                    .insert(id, flag(required(&row, 4)?)?)
                    .is_some()
                {
                    return Err(CatalogError::InvalidMetadata);
                }
            }
        }
        Ok(privileges)
    }

    fn apply(&self, row: &mut Row) -> Result<bool, CatalogError> {
        let scope = (required(row, 0)?.to_owned(), required(row, 1)?.to_owned());
        if self.schemas.get(&scope) != Some(&true) {
            return Ok(false);
        }
        if row[2].is_some() {
            row[21] = None;
            let id = relation(row)?;
            match self.relations.get(&id) {
                Some(false) => return Ok(false),
                Some(true) => row[21] = Some("YES".into()),
                // External objects have no local PG_CLASS entry; do not infer
                // a whole-table grant from their role-visible SVV metadata.
                None => {}
            }
        }
        Ok(true)
    }
}

fn flag(value: &str) -> Result<bool, CatalogError> {
    match value {
        "YES" => Ok(true),
        "NO" => Ok(false),
        _ => Err(CatalogError::InvalidMetadata),
    }
}

async fn read(
    transaction: &mut PostgresTransaction<'_>,
    sql: &str,
    fields: i16,
    settings: &CatalogSettings,
    remaining_rows: &mut usize,
    remaining_bytes: &mut usize,
) -> Result<Vec<Row>, CatalogError> {
    let query = ParsedQuery::parse_with_dialect(sql, 1024 * 1024, SqlDialect::Redshift)
        .map_err(|_| CatalogError::InvalidSettings)?;
    let mut output = Collector {
        rows: Vec::new(),
        fields,
        remaining_rows: *remaining_rows,
        remaining_bytes: *remaining_bytes,
        error: None,
    };
    let result = transaction
        .execute(
            &query,
            &mut output,
            settings.refresh_timeout,
            settings.refresh_timeout,
            None,
            None,
        )
        .await;
    if let Some(error) = output.error {
        return Err(error);
    }
    result.map_err(|_| CatalogError::SourceUnavailable)?;
    *remaining_rows = output.remaining_rows;
    *remaining_bytes = output.remaining_bytes;
    Ok(output.rows)
}

fn required(row: &Row, index: usize) -> Result<&str, CatalogError> {
    row[index].as_deref().ok_or(CatalogError::InvalidMetadata)
}

fn relation(row: &Row) -> Result<RelationId, CatalogError> {
    Ok(RelationId {
        database: required(row, 0)?.into(),
        schema: required(row, 1)?.into(),
        name: required(row, 2)?.into(),
    })
}

fn append_keys(rows: Vec<Row>, facts: &mut Facts) -> Result<(), CatalogError> {
    for row in rows {
        let id = relation(&row)?;
        let Some((_, keys)) = facts.get_mut(&id) else {
            continue;
        };
        let name = required(&row, 3)?;
        let kind = match required(&row, 4)? {
            "PRIMARY KEY" => ConstraintKind::PrimaryKey,
            "UNIQUE" => ConstraintKind::Unique,
            "FOREIGN KEY" => ConstraintKind::ForeignKey,
            _ => return Err(CatalogError::InvalidMetadata),
        };
        let position: usize = required(&row, 6)?
            .parse()
            .map_err(|_| CatalogError::InvalidMetadata)?;
        if position == 1 {
            if keys.iter().any(|key| key.name == name) {
                return Err(CatalogError::InvalidMetadata);
            }
            keys.push(Constraint {
                name: name.into(),
                kind: kind.clone(),
                columns: Vec::new(),
                references: if kind == ConstraintKind::ForeignKey {
                    Some(ForeignKey {
                        relation: RelationId {
                            database: required(&row, 7)?.into(),
                            schema: required(&row, 8)?.into(),
                            name: required(&row, 9)?.into(),
                        },
                        columns: Vec::new(),
                        constraint_name: row[11].clone(),
                        update_rule: required(&row, 12)?.into(),
                        delete_rule: required(&row, 13)?.into(),
                    })
                } else {
                    None
                },
            });
        }
        let key = keys.last_mut().ok_or(CatalogError::InvalidMetadata)?;
        if key.name != name || key.kind != kind || key.columns.len() + 1 != position {
            return Err(CatalogError::InvalidMetadata);
        }
        key.columns.push(required(&row, 5)?.into());
        if let Some(reference) = &mut key.references {
            if reference.relation
                != (RelationId {
                    database: required(&row, 7)?.into(),
                    schema: required(&row, 8)?.into(),
                    name: required(&row, 9)?.into(),
                })
                || reference.constraint_name != row[11]
                || reference.update_rule != required(&row, 12)?
                || reference.delete_rule != required(&row, 13)?
            {
                return Err(CatalogError::InvalidMetadata);
            }
            reference.columns.push(required(&row, 10)?.into());
        }
    }
    Ok(())
}

struct Collector {
    rows: Vec<Row>,
    fields: i16,
    remaining_rows: usize,
    remaining_bytes: usize,
    error: Option<CatalogError>,
}
impl Collector {
    fn row(&mut self, row: pgwire::messages::data::DataRow) -> Result<(), CatalogError> {
        let row = decode_row(row, self.fields)?;
        self.remaining_rows = self
            .remaining_rows
            .checked_sub(1)
            .ok_or(CatalogError::TooLarge)?;
        let bytes = row
            .iter()
            .flatten()
            .try_fold(0usize, |n, s| n.checked_add(s.len()))
            .ok_or(CatalogError::TooLarge)?;
        self.remaining_bytes = self
            .remaining_bytes
            .checked_sub(bytes)
            .ok_or(CatalogError::TooLarge)?;
        self.rows.push(row);
        Ok(())
    }
}
impl Sink<PgWireBackendMessage> for Collector {
    type Error = PgWireError;
    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<PgWireResult<()>> {
        Poll::Ready(Ok(()))
    }
    fn start_send(mut self: Pin<&mut Self>, event: PgWireBackendMessage) -> PgWireResult<()> {
        if let PgWireBackendMessage::DataRow(row) = event
            && let Err(error) = self.row(row)
        {
            self.error = Some(error);
            return Err(std::io::Error::other("catalog collection failed").into());
        }
        Ok(())
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<PgWireResult<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<PgWireResult<()>> {
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::SourceIdentity;
    use bytes::{BufMut, BytesMut};

    fn column(table: &str, name: &str, ordinal: usize, kind: &str) -> Row {
        let mut row = vec![None; 22];
        for (index, value) in [
            (0, "warehouse"),
            (1, "analytics"),
            (2, table),
            (3, "TABLE"),
            (5, name),
            (7, kind),
            (8, "YES"),
            (21, "YES"),
        ] {
            row[index] = Some(value.into());
        }
        row[6] = Some(ordinal.to_string());
        row
    }

    fn foreign(column: &str, position: usize, remote: &str) -> Row {
        [
            Some("warehouse"),
            Some("analytics"),
            Some("events"),
            Some("events_account_fk"),
            Some("FOREIGN KEY"),
            Some(column),
            Some(if position == 1 { "1" } else { "2" }),
            Some("warehouse"),
            Some("analytics"),
            Some("accounts"),
            Some(remote),
            Some("accounts_pk"),
            Some("NO ACTION"),
            Some("NO ACTION"),
        ]
        .into_iter()
        .map(|s| s.map(str::to_owned))
        .collect()
    }

    #[test]
    fn snapshot_preserves_super_source_identity_and_ordered_composite_relationships() {
        let settings = CatalogSettings::new(vec!["analytics".into()]);
        let mut rows = Rows::new(&settings);
        let id = RelationId {
            database: "warehouse".into(),
            schema: "analytics".into(),
            name: "events".into(),
        };
        rows.push(column("events", "tenant_id", 1, "integer"))
            .unwrap();
        rows.push(column("events", "account_id", 2, "bigint"))
            .unwrap();
        rows.push(column("events", "payload", 3, "super")).unwrap();
        let mut facts = BTreeMap::from([(id.clone(), (None, Vec::new()))]);
        append_keys(
            vec![
                foreign("tenant_id", 1, "tenant_id"),
                foreign("account_id", 2, "id"),
            ],
            &mut facts,
        )
        .unwrap();
        for (id, (view, keys)) in facts {
            rows.facts(id, view, keys).unwrap();
        }
        let snapshot = rows
            .snapshot(
                SourceIdentity {
                    dialect: SqlDialect::Redshift,
                    endpoint: "warehouse:5439/warehouse".into(),
                    principal: "reader".into(),
                    role: None,
                    warehouse: None,
                    default_schema: None,
                },
                1,
            )
            .unwrap();
        assert_eq!(snapshot.source.dialect, SqlDialect::Redshift);
        assert!(snapshot.postgres.is_none());
        let relation = &snapshot.relations[&id];
        assert_eq!(relation.columns[2].native_type.name, "super");
        assert!(relation.columns[2].native_type.postgres_oid.is_none());
        assert_eq!(relation.selectable, Some(true));
        let key = &relation.constraints.as_ref().unwrap()[0];
        assert_eq!(key.columns, ["tenant_id", "account_id"]);
        assert_eq!(
            key.references.as_ref().unwrap().columns,
            ["tenant_id", "id"]
        );
        assert_eq!(
            key.references.as_ref().unwrap().constraint_name.as_deref(),
            Some("accounts_pk")
        );
        assert!(
            relation
                .columns
                .iter()
                .all(|c| c.primary_key == Some(false))
        );
    }

    #[test]
    fn incomplete_or_inconsistent_composite_keys_are_rejected() {
        let id = RelationId {
            database: "warehouse".into(),
            schema: "analytics".into(),
            name: "events".into(),
        };
        for broken in [
            vec![foreign("account_id", 2, "id")],
            vec![
                foreign("tenant_id", 1, "tenant_id"),
                foreign("account_id", 1, "id"),
            ],
            {
                let mut second = foreign("account_id", 2, "id");
                second[9] = Some("another_table".into());
                vec![foreign("tenant_id", 1, "tenant_id"), second]
            },
        ] {
            let mut facts = BTreeMap::from([(id.clone(), (None, Vec::new()))]);
            assert_eq!(
                append_keys(broken, &mut facts),
                Err(CatalogError::InvalidMetadata)
            );
        }
    }

    #[test]
    fn metadata_queries_are_read_only_redshift_and_preserve_quoted_schema_scope() {
        let settings = CatalogSettings::new(vec![
            "Mixed\\.__LIMIT__.Schema's".into(),
            "x\\'; SELECT secret FROM public.users --".into(),
        ]);
        let literal = "'Mixed\\\\.__LIMIT__.Schema''s'";
        for template in [
            include_str!("catalog_columns.sql"),
            include_str!("catalog_keys.sql"),
            include_str!("catalog_privileges.sql"),
        ] {
            let sql = scoped_sql(template, &settings);
            assert!(sql.contains(literal));
            ParsedQuery::parse_with_dialect(&sql, 1024 * 1024, SqlDialect::Redshift)
                .expect("native metadata SQL parses")
                .validate_read_only()
                .unwrap();
        }
    }

    #[test]
    fn source_privilege_scope_filters_tables_without_inventing_external_grants() {
        let privileges = Privileges::collect(vec![
            [
                Some("warehouse"),
                Some("analytics"),
                Some("events"),
                Some("YES"),
                Some("YES"),
            ]
            .into_iter()
            .map(|s| s.map(str::to_owned))
            .collect(),
            [
                Some("warehouse"),
                Some("analytics"),
                Some("secret"),
                Some("YES"),
                Some("NO"),
            ]
            .into_iter()
            .map(|s| s.map(str::to_owned))
            .collect(),
        ])
        .unwrap();
        let mut allowed = column("events", "payload", 1, "super");
        assert!(privileges.apply(&mut allowed).unwrap());
        assert_eq!(allowed[21].as_deref(), Some("YES"));
        let mut denied = column("secret", "value", 1, "varchar");
        assert!(!privileges.apply(&mut denied).unwrap());
        let mut external = column("external_events", "payload", 1, "super");
        assert!(privileges.apply(&mut external).unwrap());
        assert!(external[21].is_none());
        external[1] = Some("excluded".into());
        assert!(!privileges.apply(&mut external).unwrap());
    }

    #[test]
    fn collector_rejects_oversized_and_malformed_native_metadata_before_publication() {
        let row = |values: &[Option<&str>]| {
            let mut data = BytesMut::new();
            for value in values {
                if let Some(value) = value {
                    data.put_i32(value.len() as i32);
                    data.extend_from_slice(value.as_bytes());
                } else {
                    data.put_i32(-1);
                }
            }
            pgwire::messages::data::DataRow::new(data, values.len() as i16)
        };
        let mut output = Collector {
            rows: Vec::new(),
            fields: 2,
            remaining_rows: 1,
            remaining_bytes: 4,
            error: None,
        };
        assert_eq!(output.row(row(&[Some("a"), Some("b")])), Ok(()));
        assert_eq!(
            output.row(row(&[Some("c"), Some("d")])),
            Err(CatalogError::TooLarge)
        );
        assert_eq!(output.rows.len(), 1);
        output.remaining_rows = 1;
        assert_eq!(
            output.row(row(&[Some("long"), Some("text")])),
            Err(CatalogError::TooLarge)
        );
        assert_eq!(output.rows.len(), 1);
        assert_eq!(
            output.row(row(&[Some("missing")])),
            Err(CatalogError::InvalidMetadata)
        );
        let mut bytes = BytesMut::new();
        bytes.put_i32(100);
        assert_eq!(
            output.row(pgwire::messages::data::DataRow::new(bytes, 2)),
            Err(CatalogError::InvalidMetadata)
        );
    }
}
