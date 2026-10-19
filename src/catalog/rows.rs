//! Bounded intermediate rows shared by the two metadata providers. Their single
//! statement returns schemas, relations and columns in one source observation.

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
}
impl Rows {
    pub(crate) fn new(settings: &CatalogSettings) -> Self {
        Self {
            rows: Vec::new(),
            bytes: 0,
            max_rows: settings.max_rows,
            max_bytes: settings.max_bytes,
        }
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
        if self.rows.len() >= self.max_rows || bytes > self.max_bytes {
            return Err(CatalogError::TooLarge);
        }
        self.bytes = bytes;
        self.rows.push(row);
        Ok(())
    }
    pub(crate) fn snapshot(
        self,
        source: SourceIdentity,
        generation: u64,
    ) -> Result<Snapshot, CatalogError> {
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
        if bytes > self.max_bytes {
            return Err(CatalogError::TooLarge);
        }
        Ok(Snapshot {
            generation,
            loaded_at: SystemTime::now(),
            source,
            schemas,
            relations,
        })
    }
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
