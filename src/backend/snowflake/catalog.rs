//! Snowflake metadata reads share the adapter's SQL API admission and resource bounds.
use super::SnowflakeBackend;
use crate::{
    backend::result::{ExecutionError, ResultEvent},
    catalog::{
        CatalogError, CatalogSettings, Constraint, ConstraintKind, ForeignKey, RelationId, Rows,
        SourceIdentity,
    },
    config::GatewaySettings,
    discovery::SessionSettings,
};
use futures::{Sink, channel::oneshot};
use std::{
    collections::BTreeMap,
    pin::Pin,
    task::{Context, Poll},
};

type Row = Vec<Option<String>>;

impl SnowflakeBackend {
    pub(crate) fn catalog_identity(&self) -> SourceIdentity {
        self.inner.catalog_identity.clone()
    }

    pub(crate) async fn catalog_rows(
        &self,
        settings: &CatalogSettings,
    ) -> Result<Rows, CatalogError> {
        let literals = settings
            .schemas
            .iter()
            .map(|s| literal(s))
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            r#"SELECT s.catalog_name, s.schema_name, t.table_name, t.table_type, t.comment,
            c.column_name, c.ordinal_position, c.data_type, c.is_nullable, c.column_default, c.comment,
            c.character_maximum_length, c.numeric_precision, c.numeric_scale, NULL,
            NULL, NULL, c.is_identity, NULL, NULL, NULL, NULL, v.view_definition
            FROM information_schema.schemata s
            LEFT JOIN information_schema.tables t ON t.table_catalog = s.catalog_name AND t.table_schema = s.schema_name
            LEFT JOIN information_schema.columns c ON c.table_catalog = t.table_catalog AND c.table_schema = t.table_schema AND c.table_name = t.table_name
            LEFT JOIN information_schema.views v ON v.table_catalog = t.table_catalog AND v.table_schema = t.table_schema AND v.table_name = t.table_name
            WHERE s.catalog_name = CURRENT_DATABASE() AND s.schema_name IN ({literals})
            ORDER BY s.schema_name, t.table_name, c.ordinal_position LIMIT {}"#,
            settings.max_rows + 1
        );
        let execution = GatewaySettings {
            queue_timeout_ms: u32::try_from(settings.refresh_timeout.as_millis())
                .map_err(|_| CatalogError::InvalidSettings)?,
            query_timeout_ms: u32::try_from(settings.refresh_timeout.as_millis())
                .map_err(|_| CatalogError::InvalidSettings)?,
            ..GatewaySettings::default()
        };
        let mut session = self
            .open(&execution, false)
            .await
            .map_err(|_| CatalogError::SourceUnavailable)?;
        let result = async {
            let mut remaining_rows = settings.max_rows;
            let mut remaining_bytes = settings.max_bytes;
            let mut read = async |sql: &str| -> Result<Vec<Row>, CatalogError> {
                let mut output = Collector {
                    rows: Vec::new(),
                    max_rows: remaining_rows,
                    remaining_bytes,
                    error: None,
                };
                let (_cancel, receiver) = oneshot::channel();
                let result = session
                    .execute_sql(
                        sql,
                        &mut output,
                        &execution,
                        &SessionSettings::default(),
                        receiver,
                    )
                    .await;
                if let Some(error) = output.error {
                    return Err(error);
                }
                result.map_err(|_| CatalogError::SourceUnavailable)?;
                remaining_rows -= output.rows.len();
                remaining_bytes = output.remaining_bytes;
                Ok(output.rows)
            };
            let definitions = read(&sql).await?;
            let mut facts = BTreeMap::<RelationId, (Option<String>, Vec<Constraint>)>::new();
            let mut rows = Rows::new(settings);
            for mut row in definitions {
                if row.len() != 23 {
                    return Err(CatalogError::InvalidMetadata);
                }
                let view = row.pop().flatten();
                if let Some(name) = &row[2] {
                    facts
                        .entry(RelationId {
                            database: text(&row, 0)?,
                            schema: text(&row, 1)?,
                            name: name.clone(),
                        })
                        .or_insert((view, Vec::new()));
                }
                // The local emulator has no access controls. Real Snowflake visibility
                // alone does not establish whole-table SELECT privilege.
                if self.inner.emulator && row[2].is_some() {
                    row[21] = Some("YES".into());
                }
                rows.push(row)?;
            }
            for schema in &settings.schemas {
                for (kind, command) in [
                    (ConstraintKind::PrimaryKey, "PRIMARY"),
                    (ConstraintKind::Unique, "UNIQUE"),
                    (ConstraintKind::ForeignKey, "IMPORTED"),
                ] {
                    let keys = read(&format!(
                        "SHOW {command} KEYS IN SCHEMA {}.{}",
                        identifier(&self.inner.database),
                        identifier(schema)
                    ))
                    .await?;
                    let mut grouped = BTreeMap::<
                        (RelationId, String),
                        Vec<(u32, String, Option<ForeignKey>)>,
                    >::new();
                    for row in keys {
                        let foreign = kind == ConstraintKind::ForeignKey;
                        let (db, sc, table, column, sequence, name) = if foreign {
                            (5, 6, 7, 8, 9, 12)
                        } else {
                            (1, 2, 3, 4, 5, 6)
                        };
                        let id = RelationId {
                            database: text(&row, db)?,
                            schema: text(&row, sc)?,
                            name: text(&row, table)?,
                        };
                        if id.database != self.inner.database
                            || id.schema != *schema
                            || !facts.contains_key(&id)
                        {
                            return Err(CatalogError::InvalidMetadata);
                        }
                        let reference = if foreign {
                            Some(ForeignKey {
                                relation: RelationId {
                                    database: text(&row, 1)?,
                                    schema: text(&row, 2)?,
                                    name: text(&row, 3)?,
                                },
                                columns: vec![text(&row, 4)?],
                                constraint_name: row.get(13).cloned().flatten(),
                                update_rule: text(&row, 10)?,
                                delete_rule: text(&row, 11)?,
                            })
                        } else {
                            None
                        };
                        let sequence = text(&row, sequence)?
                            .parse()
                            .map_err(|_| CatalogError::InvalidMetadata)?;
                        grouped.entry((id, text(&row, name)?)).or_default().push((
                            sequence,
                            text(&row, column)?,
                            reference,
                        ));
                    }
                    for ((id, name), mut columns) in grouped {
                        columns.sort_by_key(|(ordinal, _, _)| *ordinal);
                        if columns
                            .iter()
                            .enumerate()
                            .any(|(i, (ordinal, _, _))| *ordinal as usize != i + 1)
                        {
                            return Err(CatalogError::InvalidMetadata);
                        }
                        let mut references = columns[0].2.clone();
                        if let Some(reference) = &mut references {
                            reference.columns.clear();
                            for (_, _, other) in &columns {
                                let other = other.as_ref().ok_or(CatalogError::InvalidMetadata)?;
                                if other.relation != reference.relation
                                    || other.constraint_name != reference.constraint_name
                                    || other.update_rule != reference.update_rule
                                    || other.delete_rule != reference.delete_rule
                                {
                                    return Err(CatalogError::InvalidMetadata);
                                }
                                reference.columns.extend(other.columns.clone());
                            }
                        }
                        facts
                            .get_mut(&id)
                            .ok_or(CatalogError::InvalidMetadata)?
                            .1
                            .push(Constraint {
                                name,
                                kind: kind.clone(),
                                columns: columns.into_iter().map(|(_, name, _)| name).collect(),
                                references,
                            });
                    }
                }
            }
            for (id, (view, constraints)) in facts {
                rows.facts(id, view, constraints)?;
            }
            Ok(rows)
        }
        .await;
        let finish = session
            .finish(false)
            .await
            .map_err(|_| CatalogError::SourceUnavailable);
        result.and_then(|rows| finish.map(|_| rows))
    }
}

fn identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}
fn literal(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
}
fn text(row: &Row, index: usize) -> Result<String, CatalogError> {
    row.get(index)
        .and_then(Clone::clone)
        .filter(|s| !s.is_empty())
        .ok_or(CatalogError::InvalidMetadata)
}

struct Collector {
    rows: Vec<Row>,
    max_rows: usize,
    remaining_bytes: usize,
    error: Option<CatalogError>,
}
impl Sink<ResultEvent> for Collector {
    type Error = ExecutionError;
    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn start_send(mut self: Pin<&mut Self>, event: ResultEvent) -> Result<(), Self::Error> {
        if let ResultEvent::Row(row) = event {
            let bytes = row.iter().flatten().map(|v| v.len()).sum::<usize>();
            let result = if self.rows.len() >= self.max_rows || bytes > self.remaining_bytes {
                Err(CatalogError::TooLarge)
            } else {
                row.into_iter()
                    .map(|v| {
                        v.map(|v| {
                            String::from_utf8(v.to_vec()).map_err(|_| CatalogError::InvalidMetadata)
                        })
                        .transpose()
                    })
                    .collect()
            };
            match result {
                Ok(row) => {
                    self.remaining_bytes -= bytes;
                    self.rows.push(row);
                }
                Err(error) => {
                    self.error = Some(error);
                    return Err(ExecutionError::rejected(
                        "54000",
                        "catalog collection failed",
                    ));
                }
            }
        }
        Ok(())
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}
