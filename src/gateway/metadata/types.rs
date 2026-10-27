//! Describe source columns in the type vocabulary expected by PostgreSQL clients.
//!
//! OIDs are PostgreSQL type identifiers; JDBC codes describe types to Java clients.
//! Keep native PostgreSQL identifiers when available, otherwise map the supported
//! source subset. Unknown types fail explicitly instead of advertising guessed
//! metadata. This module describes columns; it does not convert result values.

use super::FrontendProfile;
use crate::{
    backend::result::{ExecutionError, ExecutionResult},
    catalog::Column,
};

pub(super) struct FrontendType {
    pub oid: u32,
    pub jdbc: i16,
    pub name: String,
    pub length: i16,
    pub modifier: i32,
    pub size: Option<u32>,
    pub scale: Option<i32>,
}

impl FrontendType {
    pub(super) fn sql_name(&self) -> String {
        match self.oid {
            1700 => self
                .size
                .zip(self.scale)
                .map(|(p, s)| format!("numeric({p},{s})"))
                .unwrap_or_else(|| "numeric".into()),
            1043 => self
                .size
                .map(|n| format!("character varying({n})"))
                .unwrap_or_else(|| "character varying".into()),
            _ => self.name.clone(),
        }
    }

    pub(super) fn column(c: &Column) -> ExecutionResult<Self> {
        Self::for_profile(c, FrontendProfile::Postgres)
    }

    pub(super) fn for_profile(c: &Column, profile: FrontendProfile) -> ExecutionResult<Self> {
        let t = &c.native_type;
        let (oid, jdbc, name, length, size) = match t.name.to_ascii_uppercase().as_str() {
            "BOOL" | "BOOLEAN" => (16, -7, "bool", 1, Some(1)),
            "INT2" | "SMALLINT" => (21, 5, "int2", 2, Some(5)),
            "INT4" | "INTEGER" => (23, 4, "int4", 4, Some(10)),
            "INT8" | "BIGINT" => (20, -5, "int8", 8, Some(19)),
            "FLOAT4" | "REAL" => (700, 7, "float4", 4, Some(8)),
            "FLOAT8" | "FLOAT" | "DOUBLE" | "DOUBLE PRECISION" => (701, 8, "float8", 8, Some(17)),
            "NUMBER" | "NUMERIC" | "DECIMAL" => (1700, 2, "numeric", -1, t.precision),
            "TEXT" if t.postgres_oid.is_none() && t.character_length.is_some() => {
                (1043, 12, "varchar", -1, t.character_length)
            }
            "TEXT" => (25, 12, "text", -1, t.character_length),
            "VARCHAR" | "CHARACTER VARYING" => (1043, 12, "varchar", -1, t.character_length),
            "BPCHAR" | "CHAR" | "CHARACTER" => (1042, 1, "bpchar", -1, t.character_length),
            "BYTEA" | "BINARY" => (17, -2, "bytea", -1, t.character_length),
            "VARBYTE" if profile == FrontendProfile::Redshift => {
                (6551, -2, "varbyte", -1, t.character_length)
            }
            "VARBYTE" => (17, -2, "bytea", -1, t.character_length),
            "SUPER" if t.postgres_oid.is_none() => {
                if profile == FrontendProfile::Redshift {
                    (4000, 1111, "super", -1, None)
                } else {
                    (114, 1111, "json", -1, None)
                }
            }
            "DATE" => (1082, 91, "date", 4, Some(13)),
            "TIME" | "TIME WITHOUT TIME ZONE" => (1083, 92, "time", 8, Some(15)),
            "TIMETZ" | "TIME WITH TIME ZONE" => (1266, 2013, "timetz", 12, Some(21)),
            "TIMESTAMP" | "TIMESTAMP_NTZ" | "TIMESTAMP WITHOUT TIME ZONE" => {
                (1114, 93, "timestamp", 8, Some(29))
            }
            "TIMESTAMPTZ" | "TIMESTAMP_LTZ" | "TIMESTAMP_TZ" | "TIMESTAMP WITH TIME ZONE" => {
                (1184, 2014, "timestamptz", 8, Some(35))
            }
            _ if t.postgres_oid.is_some() => (
                t.postgres_oid.unwrap(),
                1111,
                t.name.as_str(),
                -1,
                t.character_length,
            ),
            _ => {
                return Err(ExecutionError::rejected(
                    "0A000",
                    "column type is outside the frontend metadata compatibility surface",
                ));
            }
        };
        let scale = if oid == 1700 {
            t.scale
        } else if matches!(oid, 1083 | 1266 | 1114 | 1184) {
            t.datetime_precision.and_then(|v| i32::try_from(v).ok())
        } else {
            Some(0)
        };
        let modifier = if let Some(modifier) = t.postgres_modifier {
            modifier
        } else if oid == 1700 {
            match (t.precision, t.scale) {
                (Some(p), Some(s)) if p <= 38 && (0..=38).contains(&s) => {
                    ((p as i32) << 16 | s) + 4
                }
                (None, _) | (_, None) => -1,
                _ => {
                    return Err(ExecutionError::rejected(
                        "0A000",
                        "numeric column precision is unavailable or unsupported",
                    ));
                }
            }
        } else if matches!(oid, 1042 | 1043) {
            t.character_length
                .and_then(|v| i32::try_from(v).ok())
                .and_then(|v| v.checked_add(4))
                .unwrap_or(-1)
        } else {
            -1
        };
        Ok(Self {
            oid: t.postgres_oid.unwrap_or(oid),
            jdbc,
            name: name.into(),
            length,
            modifier,
            size,
            scale,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnId, NativeType, RelationId};

    #[test]
    fn redshift_native_types_follow_the_frontend_without_mutating_source_facts() {
        let mut column = Column {
            id: ColumnId {
                relation: RelationId {
                    database: "warehouse".into(),
                    schema: "public".into(),
                    name: "events".into(),
                },
                name: "payload".into(),
            },
            ordinal: 1,
            native_type: NativeType {
                name: "super".into(),
                character_length: None,
                precision: None,
                scale: None,
                datetime_precision: None,
                postgres_oid: None,
                postgres_modifier: None,
            },
            nullable: true,
            default: None,
            comment: None,
            identity: false,
            generated: None,
            primary_key: None,
        };
        for (native, pg, redshift) in [
            ("super", (114, "json", 1111), (4000, "super", 1111)),
            ("varbyte", (17, "bytea", -2), (6551, "varbyte", -2)),
            (
                "timestamp without time zone",
                (1114, "timestamp", 93),
                (1114, "timestamp", 93),
            ),
            (
                "timestamp with time zone",
                (1184, "timestamptz", 2014),
                (1184, "timestamptz", 2014),
            ),
            (
                "time with time zone",
                (1266, "timetz", 2013),
                (1266, "timetz", 2013),
            ),
        ] {
            column.native_type.name = native.into();
            for (profile, expected) in [
                (FrontendProfile::Postgres, pg),
                (FrontendProfile::Redshift, redshift),
            ] {
                let reported = FrontendType::for_profile(&column, profile).unwrap();
                assert_eq!(
                    (reported.oid, reported.name.as_str(), reported.jdbc),
                    expected
                );
            }
            assert_eq!(column.native_type.name, native);
            assert!(column.native_type.postgres_oid.is_none());
        }
    }
}
