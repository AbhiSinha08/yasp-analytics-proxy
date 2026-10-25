//! Static output constituents. This describes visible SQL inputs, not permissions,
//! sensitive-data classification, or reads hidden inside database functions.

mod analyzer;
mod dialect;

use crate::{
    catalog::{ColumnId, Snapshot},
    query::ParsedQuery,
};
use serde::{Serialize, Serializer, ser::SerializeMap};

/// Optional AST lineage configuration. Parsing itself is always enabled.
#[derive(Clone, Debug, serde::Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct AstSettings {
    pub enabled: bool,
    /// Redis metadata prefix on the result cache's shared connection and TTL.
    pub namespace: String,
}

impl Default for AstSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            namespace: "yasp:ast_cache".into(),
        }
    }
}

/// A literal key, explicit index, PostgreSQL text-path token, or unknown path step.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(untagged)]
pub enum PathSegment {
    Key(String),
    Index(i64),
    Element { element: String },
    Unknown { unknown: bool },
    Any { any: bool },
}

/// Known physical coordinates or a visible unresolved input.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum LineageSource {
    Column {
        column: ColumnId,
        path: Vec<PathSegment>,
    },
    Unknown {
        reference: String,
    },
}

impl Serialize for LineageSource {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        match self {
            Self::Column { column, path } => {
                // Quoting every component keeps dots and quotes in names unambiguous.
                let quote = |s: &str| format!("\"{}\"", s.replace('"', "\"\""));
                let relation = &column.relation;
                let name = [
                    &relation.database,
                    &relation.schema,
                    &relation.name,
                    &column.name,
                ]
                .map(|s| quote(s))
                .join(".");
                map.serialize_entry("column", &name)?;
                if !path.is_empty() {
                    map.serialize_entry("path", path)?;
                }
            }
            Self::Unknown { reference } => map.serialize_entry("unknown", reference)?,
        }
        map.end()
    }
}

/// Output identity uses position; repeated aliases remain distinct.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ColumnLineage {
    pub ordinal: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub sources: Vec<LineageSource>,
}

/// Static output sources and uncertainty; no dictionary or policy decisions.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct AstDataContext {
    pub columns: Vec<ColumnLineage>,
    /// A star or unsupported relation prevents knowing the complete output shape.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub unknown_outputs: bool,
}

/// Engine-owned query data, separate from application context and identity.
#[derive(Clone, Debug, Default, Serialize)]
pub struct DataContexts {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ast: Option<AstDataContext>,
}

impl DataContexts {
    pub const EMPTY: Self = Self { ast: None };
}

impl AstDataContext {
    /// Analyze locally against the currently available catalog. Missing facts
    /// produce unknown constituents and do not reject SQL execution.
    pub fn analyze(query: &ParsedQuery, schema: Option<&Snapshot>, max_bytes: usize) -> Self {
        let result = analyzer::analyze(query, schema, max_bytes);
        if crate::hooks::payload::serialized_size(&result, max_bytes).is_ok() {
            result
        } else {
            Self {
                columns: Vec::new(),
                unknown_outputs: true,
            }
        }
    }

    /// Associate with actual result metadata without inventing output positions.
    pub fn for_columns(&self, names: &[&str]) -> Self {
        if !self.unknown_outputs
            && self.columns.len() == names.len()
            && self
                .columns
                .iter()
                .zip(names)
                .all(|(c, name)| c.name.as_deref().is_none_or(|n| n == *name))
        {
            let mut result = self.clone();
            for (column, name) in result.columns.iter_mut().zip(names) {
                column.name = Some((*name).into());
            }
            result
        } else {
            // Analysis stops before an unexpandable star, so only this prefix
            // has known positions. A complete-but-mismatched shape is unsafe.
            let prefix = if self.unknown_outputs
                && self.columns.len() <= names.len()
                && self
                    .columns
                    .iter()
                    .zip(names)
                    .all(|(c, name)| c.name.as_deref().is_none_or(|n| n == *name))
            {
                self.columns.as_slice()
            } else {
                &[]
            };
            Self {
                columns: names
                    .iter()
                    .enumerate()
                    .map(|(ordinal, name)| ColumnLineage {
                        ordinal,
                        name: Some((*name).into()),
                        sources: prefix.get(ordinal).map_or_else(
                            || {
                                vec![LineageSource::Unknown {
                                    reference: "output shape".into(),
                                }]
                            },
                            |column| column.sources.clone(),
                        ),
                    })
                    .collect(),
                unknown_outputs: false,
            }
        }
    }
}
