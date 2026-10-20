//! Bounded intermediate metadata accumulated before publishing a generation.

use super::{CatalogError, CatalogSettings, SourceIdentity, model::*};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::SystemTime,
};

pub(crate) struct Rows {
    rows: Vec<Vec<Option<String>>>,
    bytes: usize,
    max_rows: usize,
    max_bytes: usize,
    postgres: Option<super::PostgresMetadata>,
    fact_rows: usize,
    postgres_bytes: usize,
    facts: BTreeMap<RelationId, (Option<String>, Vec<Constraint>)>,
}
impl Rows {
    pub(crate) fn new(settings: &CatalogSettings) -> Self {
        Self {
            postgres: None,
            fact_rows: 0,
            postgres_bytes: 0,
            rows: Vec::new(),
            bytes: 0,
            max_rows: settings.max_rows,
            max_bytes: settings.max_bytes,
            facts: BTreeMap::new(),
        }
    }
    #[cfg(feature = "snowflake")]
    pub(crate) fn facts(
        &mut self,
        relation: RelationId,
        view: Option<String>,
        constraints: Vec<Constraint>,
    ) -> Result<(), CatalogError> {
        let bytes = fact_bytes(&relation, &view, &constraints);
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .ok_or(CatalogError::TooLarge)?;
        if self.bytes > self.max_bytes
            || self.rows.len()
                + self.facts.values().map(|(_, c)| c.len()).sum::<usize>()
                + constraints.len()
                > self.max_rows
        {
            return Err(CatalogError::TooLarge);
        }
        self.facts.insert(relation, (view, constraints));
        Ok(())
    }
    pub(crate) fn push(&mut self, row: Vec<Option<String>>) -> Result<(), CatalogError> {
        if row.len() != 22 {
            return Err(CatalogError::InvalidMetadata);
        }
        let bytes = row
            .iter()
            .flatten()
            .try_fold(self.bytes, |n, s| n.checked_add(s.len()))
            .ok_or(CatalogError::TooLarge)?;
        if self.rows.len() + self.fact_rows >= self.max_rows || bytes > self.max_bytes {
            return Err(CatalogError::TooLarge);
        }
        self.bytes = bytes;
        self.rows.push(row);
        Ok(())
    }
    pub(crate) fn postgres_row(
        &mut self,
        kind: &str,
        oid: u32,
        json: &str,
    ) -> Result<(), CatalogError> {
        self.bytes = self
            .bytes
            .checked_add(json.len())
            .ok_or(CatalogError::TooLarge)?;
        self.postgres_bytes += json.len();
        self.fact_rows += 1;
        if self.bytes > self.max_bytes || self.rows.len() + self.fact_rows > self.max_rows {
            return Err(CatalogError::TooLarge);
        }
        self.postgres
            .get_or_insert_with(Default::default)
            .record(kind, oid, json)
    }

    pub(crate) fn snapshot(
        mut self,
        source: SourceIdentity,
        generation: u64,
    ) -> Result<Snapshot, CatalogError> {
        if let Some(pg) = &mut self.postgres {
            pg.shared_facts(&mut self.facts)?;
        }
        let mut schemas = BTreeSet::new();
        let mut relations: BTreeMap<RelationId, Relation> = BTreeMap::new();
        for mut row in self.rows {
            let mut text = |i: usize| row[i].take().ok_or(CatalogError::InvalidMetadata);
            let database = text(0)?;
            let schema = text(1)?;
            if database.is_empty() || schema.is_empty() {
                return Err(CatalogError::InvalidMetadata);
            }
            schemas.insert((database.clone(), schema.clone()));
            let Some(name) = row[2].take() else {
                continue;
            };
            let id = RelationId {
                database,
                schema,
                name,
            };
            let kind = match row[3].as_deref() {
                Some("TABLE" | "BASE TABLE") => RelationKind::Table,
                Some("VIEW") => RelationKind::View,
                Some("MATERIALIZED VIEW") => RelationKind::MaterializedView,
                Some("FOREIGN TABLE" | "EXTERNAL TABLE") => RelationKind::ForeignTable,
                _ => return Err(CatalogError::InvalidMetadata),
            };
            let comment = row[4].take();
            let native_id = row[19].take();
            let selectable = yes_no(&row[21])?;
            let relation = relations.entry(id.clone()).or_insert_with(|| Relation {
                id: id.clone(),
                kind: kind.clone(),
                comment: comment.clone(),
                native_id: native_id.clone(),
                selectable,
                view_definition: None,
                constraints: None,
                columns: Vec::new(),
            });
            if relation.kind != kind
                || relation.comment != comment
                || relation.native_id != native_id
                || relation.selectable != selectable
            {
                return Err(CatalogError::InvalidMetadata);
            }
            let Some(name) = row[5].take() else {
                continue;
            };
            let ordinal = number(&row[6])?.ok_or(CatalogError::InvalidMetadata)?;
            if ordinal == 0 {
                return Err(CatalogError::InvalidMetadata);
            }
            relation.columns.push(Column {
                id: ColumnId { relation: id, name },
                ordinal,
                native_type: NativeType {
                    name: row[7].take().ok_or(CatalogError::InvalidMetadata)?,
                    character_length: number(&row[11])?,
                    precision: number(&row[12])?,
                    scale: number(&row[13])?,
                    datetime_precision: number(&row[14])?,
                    postgres_oid: number(&row[15])?,
                    postgres_modifier: number(&row[16])?,
                },
                nullable: yes_no(&row[8])?.ok_or(CatalogError::InvalidMetadata)?,
                default: row[9].take(),
                comment: row[10].take(),
                identity: yes_no(&row[17])?.unwrap_or(false),
                generated: yes_no(&row[18])?,
                primary_key: yes_no(&row[20])?,
            });
        }
        for relation in relations.values_mut() {
            if let Some((view, constraints)) = self.facts.get(&relation.id) {
                if constraints.iter().any(|key| {
                    key.name.is_empty()
                        || key.columns.is_empty()
                        || key
                            .columns
                            .iter()
                            .any(|name| !relation.columns.iter().any(|c| c.id.name == *name))
                        || key
                            .references
                            .as_ref()
                            .is_some_and(|r| r.columns.len() != key.columns.len())
                }) {
                    return Err(CatalogError::InvalidMetadata);
                }
                relation.view_definition = view.clone();
                relation.constraints = Some(constraints.clone());
                for column in &mut relation.columns {
                    column.primary_key.get_or_insert_with(|| {
                        constraints.iter().any(|c| {
                            c.kind == ConstraintKind::PrimaryKey
                                && c.columns.contains(&column.id.name)
                        })
                    });
                }
            }
            relation.columns.sort_by_key(|c| c.ordinal);
            if relation
                .columns
                .windows(2)
                .any(|pair| pair[0].ordinal == pair[1].ordinal)
                || relation
                    .columns
                    .iter()
                    .map(|c| &c.id.name)
                    .collect::<BTreeSet<_>>()
                    .len()
                    != relation.columns.len()
            {
                return Err(CatalogError::InvalidMetadata);
            }
        }
        if let Some(pg) = &self.postgres {
            pg.validate_relations(&relations)?;
        }
        if source.dialect == crate::query::SqlDialect::Snowflake {
            let ids = relations
                .keys()
                .map(|r| r.frontend_oid())
                .chain(schemas.iter().map(|(database, schema)| {
                    RelationId {
                        database: database.clone(),
                        schema: schema.clone(),
                        name: String::new(),
                    }
                    .frontend_schema_oid()
                }));
            let mut unique = BTreeSet::new();
            if ids.into_iter().any(|id| !unique.insert(id)) {
                return Err(CatalogError::InvalidMetadata);
            }
        }
        let id_bytes = |id: &RelationId| id.database.len() + id.schema.len() + id.name.len();
        let optional = |s: &Option<String>| s.as_ref().map_or(0, String::len);
        let bytes = schemas
            .iter()
            .map(|(d, s)| d.len() + s.len())
            .sum::<usize>()
            + relations
                .values()
                .map(|r| {
                    2 * id_bytes(&r.id)
                        + optional(&r.comment)
                        + optional(&r.native_id)
                        + r.columns
                            .iter()
                            .map(|c| {
                                id_bytes(&c.id.relation)
                                    + c.id.name.len()
                                    + c.native_type.name.len()
                                    + optional(&c.default)
                                    + optional(&c.comment)
                            })
                            .sum::<usize>()
                })
                .sum::<usize>();
        let bytes = bytes
            + self.postgres_bytes
            + self
                .facts
                .iter()
                .map(|(id, (view, constraints))| fact_bytes(id, view, constraints))
                .sum::<usize>();
        if bytes > self.max_bytes {
            return Err(CatalogError::TooLarge);
        }
        Ok(Snapshot {
            postgres: self.postgres,
            generation,
            loaded_at: SystemTime::now(),
            source,
            schemas,
            relations,
        })
    }
}

fn fact_bytes(relation: &RelationId, view: &Option<String>, constraints: &[Constraint]) -> usize {
    let mut bytes = relation.database.len()
        + relation.schema.len()
        + relation.name.len()
        + view.as_ref().map_or(0, String::len);
    for key in constraints {
        bytes += key.name.len() + key.columns.iter().map(String::len).sum::<usize>();
        if let Some(reference) = &key.references {
            bytes += reference.relation.database.len()
                + reference.relation.schema.len()
                + reference.relation.name.len()
                + reference.columns.iter().map(String::len).sum::<usize>()
                + reference.constraint_name.as_ref().map_or(0, String::len)
                + reference.update_rule.len()
                + reference.delete_rule.len();
        }
    }
    bytes
}

fn number<T: std::str::FromStr>(value: &Option<String>) -> Result<Option<T>, CatalogError> {
    value
        .as_deref()
        .map(|v| v.parse().map_err(|_| CatalogError::InvalidMetadata))
        .transpose()
}
fn yes_no(value: &Option<String>) -> Result<Option<bool>, CatalogError> {
    match value.as_deref() {
        Some("YES") => Ok(Some(true)),
        Some("NO") => Ok(Some(false)),
        None => Ok(None),
        _ => Err(CatalogError::InvalidMetadata),
    }
}
