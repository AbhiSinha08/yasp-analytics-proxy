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
            "DATE" => (1082, 91, "date", 4, Some(13)),
            "TIME" => (1083, 92, "time", 8, Some(15)),
            "TIMESTAMP" | "TIMESTAMP_NTZ" => (1114, 93, "timestamp", 8, Some(29)),
            "TIMESTAMPTZ" | "TIMESTAMP_LTZ" | "TIMESTAMP_TZ" => {
                (1184, 2014, "timestamptz", 8, Some(35))
            }
            _ => {
                return Err(ExecutionError::rejected(
                    "0A000",
                    "column type is outside the frontend metadata compatibility surface",
                ));
            }
        };
        let scale = if oid == 1700 {
            t.scale
        } else if matches!(oid, 1083 | 1114 | 1184) {
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
