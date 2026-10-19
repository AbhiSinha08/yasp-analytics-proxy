//! Lossless text values for the common pgwire frontend surface. Arrow stays here.

use super::connection;
use crate::backend::result::{ColumnType, ExecutionError, ExecutionResult, ResultColumn};
use arrow_array::{types::*, *};
use arrow_schema::{DataType, Schema, TimeUnit};
use bytes::Bytes;
use chrono::{DateTime, Datelike, NaiveDate, Utc};
use std::fmt::Write;

impl From<connection::Error> for ExecutionError {
    fn from(error: connection::Error) -> Self {
        let fallback = match error.status {
            2 => "0A000",
            3 => "42P01",
            4 => "42P07",
            5 => "22023",
            6 => "55000",
            7 => "22000",
            8 => "23000",
            11 | 12 => "57014",
            10 => "08006",
            13 => "28000",
            14 => "42501",
            _ => "XX000",
        };
        Self::Source {
            code: error.sqlstate.clone().unwrap_or_else(|| fallback.into()),
            message: error.to_string(),
        }
    }
}

fn unsupported() -> ExecutionError {
    ExecutionError::rejected(
        "0A000",
        "Snowflake result type or precision cannot be represented by this frontend",
    )
}
fn oversized() -> ExecutionError {
    ExecutionError::rejected(
        "54000",
        "Snowflake result exceeds the configured frame or batch bound",
    )
}

pub(super) fn columns(schema: &Schema, bound: usize) -> ExecutionResult<Vec<ResultColumn>> {
    let size = schema.fields().iter().try_fold(6usize, |size, field| {
        size.checked_add(field.name().len() + 19)
            .filter(|n| *n <= bound)
    });
    if size.is_none() || schema.fields().len() > i16::MAX as usize {
        return Err(oversized());
    }
    schema
        .fields()
        .iter()
        .map(|field| {
            if field.name().contains('\0') {
                return Err(unsupported());
            }
            let kind = match field.data_type() {
                DataType::Null | DataType::Utf8 | DataType::LargeUtf8 => ColumnType::Text,
                DataType::Boolean => ColumnType::Bool,
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                    ColumnType::Int64
                }
                DataType::Float32 | DataType::Float64 => ColumnType::Float64,
                DataType::Decimal128(precision, scale)
                    if (1..=38).contains(precision) && (0..=38).contains(scale) =>
                {
                    ColumnType::Decimal {
                        precision: *precision,
                        scale: *scale,
                    }
                }
                DataType::Binary | DataType::LargeBinary => ColumnType::Binary,
                DataType::Date32 => ColumnType::Date,
                DataType::Time64(TimeUnit::Microsecond | TimeUnit::Nanosecond) => ColumnType::Time,
                DataType::Timestamp(_, timezone)
                    if timezone
                        .as_deref()
                        .is_none_or(|s| matches!(s, "UTC" | "+00:00")) =>
                {
                    ColumnType::Timestamp {
                        utc: timezone.is_some(),
                    }
                }
                _ => return Err(unsupported()),
            };
            Ok(ResultColumn {
                name: field.name().clone(),
                kind,
            })
        })
        .collect()
}

pub(super) fn row(
    batch: &RecordBatch,
    index: usize,
    bound: usize,
) -> ExecutionResult<Vec<Option<Bytes>>> {
    let mut remaining = bound
        .checked_sub(6 + 4 * batch.num_columns())
        .ok_or_else(oversized)?;
    batch
        .columns()
        .iter()
        .map(|column| {
            if column.is_null(index) {
                return Ok(None);
            }
            let text = value(column.as_ref(), index, remaining)?;
            remaining = remaining.checked_sub(text.len()).ok_or_else(oversized)?;
            Ok(Some(Bytes::from(text)))
        })
        .collect()
}

fn value(array: &dyn Array, index: usize, bound: usize) -> ExecutionResult<String> {
    macro_rules! scalar {
        ($kind:ty) => {
            array
                .as_any()
                .downcast_ref::<$kind>()
                .ok_or_else(unsupported)?
                .value(index)
        };
    }
    macro_rules! primitive {
        ($kind:ty) => {
            scalar!(PrimitiveArray<$kind>).to_string()
        };
    }
    let result = match array.data_type() {
        DataType::Boolean => if scalar!(BooleanArray) { "t" } else { "f" }.into(),
        DataType::Int8 => primitive!(Int8Type),
        DataType::Int16 => primitive!(Int16Type),
        DataType::Int32 => primitive!(Int32Type),
        DataType::Int64 => primitive!(Int64Type),
        DataType::Float32 => float(f64::from(scalar!(Float32Array))),
        DataType::Float64 => float(scalar!(Float64Array)),
        DataType::Decimal128(precision, scale) => {
            let value = scalar!(Decimal128Array);
            let scale = u32::try_from(*scale).map_err(|_| unsupported())?;
            let digits = value.unsigned_abs().to_string();
            if digits.len() > usize::from(*precision) {
                return Err(ExecutionError::rejected(
                    "22003",
                    "Snowflake decimal exceeds its declared precision",
                ));
            }
            let text = if scale == 0 {
                digits
            } else {
                let padded = format!("{digits:0>width$}", width = scale as usize + 1);
                let split = padded.len() - scale as usize;
                format!("{}.{}", &padded[..split], &padded[split..])
            };
            if value < 0 { format!("-{text}") } else { text }
        }
        DataType::Utf8 | DataType::LargeUtf8 => {
            let text = if matches!(array.data_type(), DataType::Utf8) {
                scalar!(StringArray)
            } else {
                scalar!(LargeStringArray)
            };
            if text.len() > bound {
                return Err(oversized());
            }
            if text.contains('\0') {
                return Err(ExecutionError::rejected(
                    "22021",
                    "text results cannot contain NUL",
                ));
            }
            text.into()
        }
        DataType::Binary | DataType::LargeBinary => {
            let data = if matches!(array.data_type(), DataType::Binary) {
                scalar!(BinaryArray)
            } else {
                scalar!(LargeBinaryArray)
            };
            if data.len().saturating_mul(2).saturating_add(2) > bound {
                return Err(oversized());
            }
            let mut text = String::with_capacity(data.len() * 2 + 2);
            text.push_str("\\x");
            for byte in data {
                write!(text, "{byte:02x}").expect("writing a String");
            }
            text
        }
        DataType::Date32 => {
            let days = scalar!(Date32Array);
            let date = NaiveDate::from_ymd_opt(1970, 1, 1)
                .expect("epoch date")
                .checked_add_signed(chrono::Duration::days(i64::from(days)))
                .ok_or_else(unsupported)?;
            if !(1..=9999).contains(&date.year()) {
                return Err(unsupported());
            }
            date.to_string()
        }
        DataType::Timestamp(unit, zone) => {
            let (value, nanos) = match unit {
                TimeUnit::Second => (scalar!(TimestampSecondArray), 1_000_000_000),
                TimeUnit::Millisecond => (scalar!(TimestampMillisecondArray), 1_000_000),
                TimeUnit::Microsecond => (scalar!(TimestampMicrosecondArray), 1_000),
                TimeUnit::Nanosecond => (scalar!(TimestampNanosecondArray), 1),
            };
            let per_second = 1_000_000_000 / nanos;
            let fraction = value.rem_euclid(per_second) * nanos;
            if fraction % 1_000 != 0 {
                return Err(unsupported());
            }
            let timestamp =
                DateTime::<Utc>::from_timestamp(value.div_euclid(per_second), fraction as u32)
                    .ok_or_else(unsupported)?;
            if !(1..=9999).contains(&timestamp.year()) {
                return Err(unsupported());
            }
            let text = timestamp.format("%Y-%m-%d %H:%M:%S%.6f").to_string();
            if zone.is_some() {
                format!("{text}+00")
            } else {
                text
            }
        }
        DataType::Time64(unit) => {
            let (value, divisor) = match unit {
                TimeUnit::Microsecond => (scalar!(Time64MicrosecondArray), 1),
                TimeUnit::Nanosecond => (scalar!(Time64NanosecondArray), 1_000),
                _ => return Err(unsupported()),
            };
            if value < 0 || value % divisor != 0 || value / divisor >= 86_400_000_000 {
                return Err(unsupported());
            }
            let micros = value / divisor;
            format!(
                "{:02}:{:02}:{:02}.{:06}",
                micros / 3_600_000_000,
                micros / 60_000_000 % 60,
                micros / 1_000_000 % 60,
                micros % 1_000_000
            )
        }
        _ => return Err(unsupported()),
    };
    if result.len() > bound {
        return Err(oversized());
    }
    Ok(result)
}

fn float(value: f64) -> String {
    if value == f64::INFINITY {
        "Infinity".into()
    } else if value == f64::NEG_INFINITY {
        "-Infinity".into()
    } else {
        value.to_string()
    }
}
