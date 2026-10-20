//! SQL API jsonv2 strings to lossless frontend text values. Output formats are
//! fixed on every submission; sub-microsecond temporal values fail explicitly.
use super::rest::{Field, protocol};
use crate::backend::result::{ColumnType, ExecutionError, ExecutionResult, ResultColumn};
use bytes::Bytes;
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
fn unsupported() -> ExecutionError {
    ExecutionError::rejected(
        "0A000",
        "Snowflake result type or precision cannot be represented by this frontend",
    )
}
fn oversized() -> ExecutionError {
    ExecutionError::rejected("54000", "Snowflake result exceeds frontend frame bound")
}
fn kind(field: &Field) -> ExecutionResult<ColumnType> {
    Ok(match field.kind.to_ascii_lowercase().as_str() {
        "fixed" | "number" => {
            if !(1..=38).contains(&field.precision)
                || !(0..=field.precision as i8).contains(&field.scale)
            {
                return Err(unsupported());
            }
            ColumnType::Decimal {
                precision: field.precision,
                scale: field.scale,
            }
        }
        "real" | "float" => ColumnType::Float64,
        "boolean" => ColumnType::Bool,
        "text" | "varchar" | "variant" | "object" | "array" => ColumnType::Text,
        "binary" => ColumnType::Binary,
        "date" => ColumnType::Date,
        "time" => ColumnType::Time,
        "timestamp_ntz" => ColumnType::Timestamp { utc: false },
        "timestamp_ltz" | "timestamp_tz" => ColumnType::Timestamp { utc: true },
        _ => return Err(unsupported()),
    })
}
pub(super) fn columns(fields: &[Field], bound: usize) -> ExecutionResult<Vec<ResultColumn>> {
    if fields.len() > i16::MAX as usize
        || fields
            .iter()
            .try_fold(6usize, |n, f| {
                n.checked_add(f.name.len() + 19).filter(|n| *n <= bound)
            })
            .is_none()
    {
        return Err(oversized());
    }
    fields
        .iter()
        .map(|f| {
            if f.name.contains('\0') {
                return Err(protocol());
            }
            Ok(ResultColumn {
                name: f.name.clone(),
                kind: kind(f)?,
            })
        })
        .collect()
}
pub(super) fn row(
    fields: &[Field],
    values: Vec<Option<String>>,
    bound: usize,
) -> ExecutionResult<Vec<Option<Bytes>>> {
    if fields.len() != values.len() {
        return Err(protocol());
    }
    let mut remaining = bound
        .checked_sub(6 + 4 * fields.len())
        .ok_or_else(oversized)?;
    fields
        .iter()
        .zip(values)
        .map(|(field, value)| {
            let Some(value) = value else {
                return Ok(None);
            };
            if value.contains('\0') {
                return Err(ExecutionError::rejected(
                    "22021",
                    "text results cannot contain NUL",
                ));
            }
            let text = match kind(field)? {
                ColumnType::Bool => match value.as_str() {
                    "true" => "t".into(),
                    "false" => "f".into(),
                    _ => return Err(protocol()),
                },
                ColumnType::Decimal { precision, scale } => {
                    let digits = value.strip_prefix('-').unwrap_or(&value);
                    let (integer, fraction) = digits.split_once('.').unwrap_or((digits, ""));
                    if integer.is_empty()
                        || !integer
                            .bytes()
                            .chain(fraction.bytes())
                            .all(|b| b.is_ascii_digit())
                        || fraction.len() > scale as usize
                        || integer.trim_start_matches('0').len()
                            > usize::from(precision) - scale as usize
                    {
                        return Err(ExecutionError::rejected(
                            "22003",
                            "Snowflake decimal exceeds its declared precision",
                        ));
                    }
                    value
                }
                ColumnType::Binary => {
                    if value.len() % 2 != 0 || !value.bytes().all(|b| b.is_ascii_hexdigit()) {
                        return Err(protocol());
                    }
                    format!("\\x{}", value.to_ascii_lowercase())
                }
                ColumnType::Date => {
                    let date =
                        NaiveDate::parse_from_str(&value, "%Y-%m-%d").map_err(|_| protocol())?;
                    if !(1..=9999).contains(&date.year()) {
                        return Err(unsupported());
                    }
                    value
                }
                ColumnType::Time => {
                    let time =
                        NaiveTime::parse_from_str(&value, "%H:%M:%S%.f").map_err(|_| protocol())?;
                    if time.nanosecond() % 1000 != 0 {
                        return Err(unsupported());
                    }
                    time.format("%H:%M:%S%.6f").to_string()
                }
                ColumnType::Timestamp { utc } => {
                    let timestamp = if utc {
                        DateTime::parse_from_str(&value, "%Y-%m-%d %H:%M:%S%.f %:z")
                            .map_err(|_| protocol())?
                            .naive_utc()
                    } else {
                        NaiveDateTime::parse_from_str(&value, "%Y-%m-%d %H:%M:%S%.f")
                            .map_err(|_| protocol())?
                    };
                    if timestamp.nanosecond() % 1000 != 0 || !(1..=9999).contains(&timestamp.year())
                    {
                        return Err(unsupported());
                    }
                    let text = timestamp.format("%Y-%m-%d %H:%M:%S%.6f").to_string();
                    if utc { format!("{text}+00") } else { text }
                }
                ColumnType::Float64 => {
                    value.parse::<f64>().map_err(|_| protocol())?;
                    value
                }
                _ => value,
            };
            remaining = remaining.checked_sub(text.len()).ok_or_else(oversized)?;
            Ok(Some(Bytes::from(text)))
        })
        .collect()
}
