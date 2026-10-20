//! PostgreSQL-only facts retained by a catalog generation.
use super::{CatalogError, Constraint, ConstraintKind, ForeignKey, Relation, RelationId};
use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Clone, Debug, Default)]
pub struct PostgresMetadata {
    pub server_version: String,
    pub version: String,
    pub server_version_num: String,
    pub current_schema: Option<String>,
    pub max_index_keys: String,
    pub database: String,
    pub relations: BTreeMap<u32, PostgresRelation>,
    pub types: Vec<PostgresType>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct PostgresRelation {
    pub oid: u32,
    pub schema_oid: u32,
    pub database: String,
    pub schema: String,
    pub name: String,
    pub kind: String,
    pub owner_id: u32,
    pub owner_name: String,
    pub privileges: Option<String>,
    pub privileges_update: bool,
    pub privileges_select: bool,
    pub privileges_insert: bool,
    pub privileges_delete: bool,
    pub estimated_rows: Option<i64>,
    pub view_definition: Option<String>,
    #[serde(default)]
    pub columns: BTreeMap<u32, PostgresColumn>,
    #[serde(default)]
    pub constraints: Vec<PostgresConstraint>,
    #[serde(default)]
    pub indexes: Vec<PostgresIndex>,
}
impl PostgresRelation {
    pub fn id(&self) -> RelationId {
        RelationId {
            database: self.database.clone(),
            schema: self.schema.clone(),
            name: self.name.clone(),
        }
    }
}
#[derive(Clone, Debug, Deserialize)]
pub struct PostgresColumn {
    pub name: String,
    pub ordinal: u32,
    pub information_schema_type: Option<String>,
    pub formatted_type: String,
    pub not_null: bool,
    pub generated: String,
    pub length: i16,
    pub type_modifier: i32,
    pub base_type: u32,
    pub type_kind: String,
    pub identity: String,
    pub identity_options: Option<serde_json::Value>,
}
#[derive(Clone, Debug, Deserialize)]
pub struct PostgresConstraint {
    pub name: String,
    pub kind: String,
    pub columns: Vec<String>,
    pub ordinals: Vec<u32>,
    pub definition: String,
    pub foreign_schema: Option<String>,
    pub foreign_table: Option<String>,
    pub foreign_columns: Vec<String>,
    pub foreign_constraint: Option<String>,
    pub update_rule: String,
    pub delete_rule: String,
    pub deferrable: bool,
    pub deferred: bool,
}
#[derive(Clone, Debug, Deserialize)]
pub struct PostgresIndex {
    pub name: String,
    pub unique: bool,
    pub primary: bool,
    pub clustered: bool,
    pub method: String,
    pub expressions: Option<String>,
    pub keys: String,
    pub options: String,
    pub relation_options: Option<String>,
    pub predicate: Option<String>,
    pub key_count: u32,
    pub constraint_relation: Option<u32>,
    pub ordinal: u32,
    pub column_ordinal: Option<u32>,
    pub column_name: Option<String>,
    pub definition: String,
    pub direction: Option<String>,
    pub cardinality: String,
    pub pages: String,
    pub comment: Option<String>,
}
#[derive(Clone, Debug, Deserialize)]
pub struct PostgresType {
    pub name: String,
    pub schema: String,
    pub oid: u32,
    pub array_oid: u32,
    pub kind: String,
    pub base_type: String,
    pub nullable: bool,
    pub default: Option<String>,
    pub visible: bool,
    pub label: Option<String>,
    pub label_order: Option<u32>,
}

impl PostgresMetadata {
    pub(super) fn record(
        &mut self,
        kind: &str,
        oid: u32,
        json: &str,
    ) -> Result<(), super::CatalogError> {
        use super::CatalogError;
        let pg = self;
        let invalid = |_| CatalogError::InvalidMetadata;
        match kind {
            "profile" => {
                #[derive(serde::Deserialize)]
                struct Profile {
                    server_version: String,
                    version: String,
                    server_version_num: String,
                    current_schema: Option<String>,
                    max_index_keys: String,
                    database: String,
                }
                let value: Profile = serde_json::from_str(json).map_err(invalid)?;
                pg.server_version = value.server_version;
                pg.version = value.version;
                pg.server_version_num = value.server_version_num;
                pg.current_schema = value.current_schema;
                pg.max_index_keys = value.max_index_keys;
                pg.database = value.database;
            }
            "relation" => {
                let relation: super::PostgresRelation =
                    serde_json::from_str(json).map_err(invalid)?;
                if relation.oid != oid || pg.relations.insert(oid, relation).is_some() {
                    return Err(CatalogError::InvalidMetadata);
                }
            }
            "type" => pg.types.push(serde_json::from_str(json).map_err(invalid)?),
            kind => {
                let r = pg
                    .relations
                    .get_mut(&oid)
                    .ok_or(CatalogError::InvalidMetadata)?;
                match kind {
                    "column" => {
                        let c: super::PostgresColumn =
                            serde_json::from_str(json).map_err(invalid)?;
                        if r.columns.insert(c.ordinal, c).is_some() {
                            return Err(CatalogError::InvalidMetadata);
                        }
                    }
                    "constraint" => r
                        .constraints
                        .push(serde_json::from_str(json).map_err(invalid)?),
                    "index" => r.indexes.push(serde_json::from_str(json).map_err(invalid)?),
                    _ => return Err(CatalogError::InvalidMetadata),
                }
            }
        }
        Ok(())
    }
}

impl PostgresMetadata {
    pub(super) fn shared_facts(
        &mut self,
        facts: &mut BTreeMap<RelationId, (Option<String>, Vec<Constraint>)>,
    ) -> Result<(), CatalogError> {
        self.types.sort_by(|a, b| {
            (&a.schema, &a.name, a.label_order).cmp(&(&b.schema, &b.name, b.label_order))
        });
        if self.server_version.is_empty() || self.max_index_keys.is_empty() {
            return Err(CatalogError::InvalidMetadata);
        }
        for r in self.relations.values() {
            let mut keys = Vec::new();
            for k in &r.constraints {
                let kind = match k.kind.as_str() {
                    "p" => ConstraintKind::PrimaryKey,
                    "u" => ConstraintKind::Unique,
                    "f" => ConstraintKind::ForeignKey,
                    _ => continue,
                };
                if k.columns.len() != k.ordinals.len()
                    || k.columns
                        .iter()
                        .zip(&k.ordinals)
                        .any(|(name, pos)| r.columns.get(pos).is_none_or(|c| &c.name != name))
                {
                    return Err(CatalogError::InvalidMetadata);
                }
                let references = if kind == ConstraintKind::ForeignKey {
                    Some(ForeignKey {
                        relation: RelationId {
                            database: r.database.clone(),
                            schema: k
                                .foreign_schema
                                .clone()
                                .ok_or(CatalogError::InvalidMetadata)?,
                            name: k
                                .foreign_table
                                .clone()
                                .ok_or(CatalogError::InvalidMetadata)?,
                        },
                        columns: k.foreign_columns.clone(),
                        constraint_name: k.foreign_constraint.clone(),
                        update_rule: rule(&k.update_rule)?.into(),
                        delete_rule: rule(&k.delete_rule)?.into(),
                    })
                } else {
                    None
                };
                keys.push(Constraint {
                    name: k.name.clone(),
                    kind,
                    columns: k.columns.clone(),
                    references,
                });
            }
            facts.insert(r.id(), (r.view_definition.clone(), keys));
        }
        Ok(())
    }
    pub(super) fn validate_relations(
        &self,
        relations: &BTreeMap<RelationId, Relation>,
    ) -> Result<(), CatalogError> {
        if self.relations.len() != relations.len()
            || self.relations.values().any(|native| {
                relations.get(&native.id()).is_none_or(|r| {
                    r.native_id.as_deref().and_then(|s| s.parse::<u32>().ok()) != Some(native.oid)
                        || r.columns.len() != native.columns.len()
                        || r.columns.iter().any(|c| {
                            native
                                .columns
                                .get(&c.ordinal)
                                .is_none_or(|n| n.name != c.id.name)
                        })
                })
            })
        {
            return Err(CatalogError::InvalidMetadata);
        }
        Ok(())
    }
}

fn rule(value: &str) -> Result<&'static str, CatalogError> {
    match value {
        "a" => Ok("NO ACTION"),
        "r" => Ok("RESTRICT"),
        "c" => Ok("CASCADE"),
        "n" => Ok("SET NULL"),
        "d" => Ok("SET DEFAULT"),
        _ => Err(CatalogError::InvalidMetadata),
    }
}
