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
    /// Ordered by source ordinal, including gaps left by dropped columns.
    pub columns: Vec<Column>,
}

#[derive(Debug)]
pub struct Snapshot {
    pub generation: u64,
    pub loaded_at: SystemTime,
    pub source: SourceIdentity,
    pub schemas: BTreeSet<(String, String)>,
    pub relations: BTreeMap<RelationId, Relation>,
}
