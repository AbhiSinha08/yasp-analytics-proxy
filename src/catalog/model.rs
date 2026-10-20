use super::SourceIdentity;
use std::{
    collections::{BTreeMap, BTreeSet},
    time::SystemTime,
};

/// Logical coordinates preserve source spelling across refreshes. They are not
/// durable object IDs: dropping and recreating a name can change its meaning.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct RelationId {
    pub database: String,
    pub schema: String,
    pub name: String,
}
impl RelationId {
    pub(crate) fn frontend_oid(&self) -> u32 {
        frontend_oid(&self.database, &self.schema, &self.name)
    }
    pub(crate) fn frontend_schema_oid(&self) -> u32 {
        frontend_oid(&self.database, &self.schema, "")
    }
}
fn frontend_oid(database: &str, schema: &str, name: &str) -> u32 {
    use sha2::{Digest, Sha256};
    let mut hash = Sha256::new();
    for part in [database, schema, name] {
        hash.update((part.len() as u64).to_be_bytes());
        hash.update(part.as_bytes());
    }
    let digest = hash.finalize();
    (u32::from_be_bytes(digest[..4].try_into().expect("four hash bytes")) & 0x3fff_ffff)
        | 0x4000_0000
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ColumnId {
    pub relation: RelationId,
    pub name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RelationKind {
    Table,
    View,
    MaterializedView,
    ForeignTable,
}

#[derive(Clone, Debug)]
pub struct NativeType {
    pub name: String,
    pub character_length: Option<u32>,
    pub precision: Option<u32>,
    pub scale: Option<i32>,
    pub datetime_precision: Option<u32>,
    pub postgres_oid: Option<u32>,
    pub postgres_modifier: Option<i32>,
}

#[derive(Clone, Debug)]
pub struct Column {
    pub id: ColumnId,
    pub ordinal: u32,
    pub native_type: NativeType,
    pub nullable: bool,
    pub default: Option<String>,
    pub comment: Option<String>,
    pub identity: bool,
    /// None means the provider does not report this property.
    pub generated: Option<bool>,
    /// Declared primary-key membership; None when the provider does not report it.
    pub primary_key: Option<bool>,
}

#[derive(Clone, Debug)]
pub struct Relation {
    pub id: RelationId,
    pub kind: RelationKind,
    pub comment: Option<String>,
    pub native_id: Option<String>,
    /// Whole-relation SELECT privilege for the discovery identity; None if unknown.
    pub selectable: Option<bool>,
    pub view_definition: Option<String>,
    /// Visible complete declared keys, without source enforcement guarantees.
    /// None means the provider does not report these facts.
    pub constraints: Option<Vec<Constraint>>,
    /// Ordered by source ordinal, including gaps left by dropped columns.
    pub columns: Vec<Column>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConstraintKind {
    PrimaryKey,
    Unique,
    ForeignKey,
}

#[derive(Clone, Debug)]
pub struct ForeignKey {
    pub relation: RelationId,
    pub columns: Vec<String>,
    pub constraint_name: Option<String>,
    pub update_rule: String,
    pub delete_rule: String,
}

#[derive(Clone, Debug)]
pub struct Constraint {
    pub name: String,
    pub kind: ConstraintKind,
    pub columns: Vec<String>,
    pub references: Option<ForeignKey>,
}

#[derive(Debug)]
pub struct Snapshot {
    /// Source-specific facts captured in the same generation as shared metadata.
    pub postgres: Option<super::PostgresMetadata>,
    pub generation: u64,
    pub loaded_at: SystemTime,
    pub source: SourceIdentity,
    pub schemas: BTreeSet<(String, String)>,
    pub relations: BTreeMap<RelationId, Relation>,
}
