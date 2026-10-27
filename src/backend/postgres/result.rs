//! Lossless PostgreSQL representation at the shared execution boundary.

use crate::{
    backend::result::{ExecutionError, ResultEvent},
    config::PgWireEngine,
};
use futures::Sink;
use pgwire::{error::PgWireError, messages::PgWireBackendMessage};
use std::{
    fmt,
    pin::Pin,
    task::{Context, Poll},
};

pub(crate) struct NativeError(PgWireError);
impl fmt::Debug for NativeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("NativeError { .. }")
    }
}
impl NativeError {
    pub(crate) fn into_wire(self) -> PgWireError {
        self.0
    }
}
impl From<PgWireError> for ExecutionError {
    fn from(error: PgWireError) -> Self {
        Self::Postgres(NativeError(error))
    }
}

/// Preserve native text rows, translating Redshift-specific types for a
/// PostgreSQL frontend. Only VARBYTE conversion rebuilds a row.
pub(crate) struct Output<'a, S> {
    inner: &'a mut S,
    postgres_profile: bool,
    engine: PgWireEngine,
    binary_columns: Vec<bool>,
}

impl<'a, S> Output<'a, S> {
    pub(crate) fn new(inner: &'a mut S, engine: PgWireEngine, profile: &str) -> Self {
        Self {
            inner,
            postgres_profile: profile == "postgresql",
            engine,
            binary_columns: Vec::new(),
        }
    }

    fn response(
        &mut self,
        mut message: PgWireBackendMessage,
    ) -> Result<ResultEvent, ExecutionError> {
        if self.engine != PgWireEngine::Redshift {
            return Ok(message.into());
        }
        match &mut message {
            PgWireBackendMessage::RowDescription(description) => {
                if description
                    .fields
                    .iter()
                    .any(|field| field.format_code != 0)
                {
                    return Err(ExecutionError::rejected(
                        "0A000",
                        "binary Redshift results are unsupported",
                    ));
                }
                // Frontend relation identifiers belong to the normalized catalog.
                // Native source identifiers cannot resolve against that namespace.
                for field in &mut description.fields {
                    field.table_id = 0;
                    field.column_id = 0;
                }
                if !self.postgres_profile {
                    return Ok(message.into());
                }
                self.binary_columns = description
                    .fields
                    .iter_mut()
                    .map(|field| {
                        let binary = field.type_id == 6551;
                        match field.type_id {
                            4000 => field.type_id = 114,
                            6551 => field.type_id = 17,
                            _ => {}
                        }
                        if binary || field.type_id == 114 {
                            field.type_size = -1;
                            field.type_modifier = -1;
                        }
                        binary
                    })
                    .collect();
            }
            PgWireBackendMessage::DataRow(row)
                if self.postgres_profile && self.binary_columns.contains(&true) =>
            {
                use bytes::{Buf, BufMut};
                let mut source = row.data.as_ref();
                let mut data = bytes::BytesMut::new();
                if usize::try_from(row.field_count).ok() != Some(self.binary_columns.len()) {
                    return Err(ExecutionError::rejected(
                        "22000",
                        "invalid Redshift result row",
                    ));
                }
                for &binary in &self.binary_columns {
                    if source.remaining() < 4 {
                        return Err(ExecutionError::rejected(
                            "22000",
                            "invalid Redshift result row",
                        ));
                    }
                    let length = source.get_i32();
                    if length == -1 {
                        data.put_i32(-1);
                        continue;
                    }
                    let length = usize::try_from(length)
                        .ok()
                        .filter(|&n| n <= source.remaining())
                        .ok_or_else(|| {
                            ExecutionError::rejected("22000", "invalid Redshift result row")
                        })?;
                    let value = &source[..length];
                    if binary {
                        if length % 2 != 0 || !value.iter().all(u8::is_ascii_hexdigit) {
                            return Err(ExecutionError::rejected(
                                "22000",
                                "invalid Redshift VARBYTE result",
                            ));
                        }
                        data.put_i32((length + 2) as i32);
                        data.extend_from_slice(b"\\x");
                    } else {
                        data.put_i32(length as i32);
                    }
                    data.extend_from_slice(value);
                    source.advance(length);
                }
                if !source.is_empty() {
                    return Err(ExecutionError::rejected(
                        "22000",
                        "invalid Redshift result row",
                    ));
                }
                row.data = data;
            }
            _ => {}
        }
        Ok(message.into())
    }
}

impl<S: Sink<ResultEvent, Error = ExecutionError> + Unpin> Sink<PgWireBackendMessage>
    for Output<'_, S>
{
    type Error = ExecutionError;
    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut *self.get_mut().inner).poll_ready(cx)
    }
    fn start_send(self: Pin<&mut Self>, message: PgWireBackendMessage) -> Result<(), Self::Error> {
        let this = self.get_mut();
        let event = this.response(message)?;
        Pin::new(&mut *this.inner).start_send(event)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut *self.get_mut().inner).poll_flush(cx)
    }
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut *self.get_mut().inner).poll_close(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};
    use pgwire::messages::data::{DataRow, FieldDescription, RowDescription};

    fn columns() -> PgWireBackendMessage {
        PgWireBackendMessage::RowDescription(RowDescription::new(
            [("document", 4000), ("payload", 6551), ("exact", 1700)]
                .into_iter()
                .map(|(name, oid)| FieldDescription::new(name.into(), 17176, 1, oid, -1, -1, 0))
                .collect(),
        ))
    }

    fn row(values: &[Option<&[u8]>]) -> PgWireBackendMessage {
        let mut data = BytesMut::new();
        for value in values {
            if let Some(value) = value {
                data.put_i32(value.len() as i32);
                data.extend_from_slice(value);
            } else {
                data.put_i32(-1);
            }
        }
        PgWireBackendMessage::DataRow(DataRow::new(data, values.len() as i16))
    }

    fn wire(event: ResultEvent) -> PgWireBackendMessage {
        crate::wire::encode(event).into_wire()
    }

    #[test]
    fn redshift_native_types_follow_the_frontend_without_changing_postgres() {
        for (engine, profile, expected) in [
            (PgWireEngine::Redshift, "postgresql", vec![114, 17, 1700]),
            (PgWireEngine::Redshift, "redshift", vec![4000, 6551, 1700]),
            (
                PgWireEngine::Postgresql,
                "postgresql",
                vec![4000, 6551, 1700],
            ),
            (PgWireEngine::Postgresql, "redshift", vec![4000, 6551, 1700]),
        ] {
            let mut sink = ();
            let mut adapter = Output::new(&mut sink, engine, profile);
            let PgWireBackendMessage::RowDescription(description) =
                wire(adapter.response(columns()).unwrap())
            else {
                panic!("expected columns")
            };
            let expected_origin = if engine == PgWireEngine::Redshift {
                (0, 0)
            } else {
                (17176, 1)
            };
            assert!(
                description
                    .fields
                    .iter()
                    .all(|field| { (field.table_id, field.column_id) == expected_origin })
            );
            assert_eq!(
                description
                    .fields
                    .iter()
                    .map(|f| f.type_id)
                    .collect::<Vec<_>>(),
                expected
            );
        }
    }

    #[test]
    fn super_numbers_nulls_and_varbyte_survive_conversion_without_json_reserialization() {
        let document = br#"{"n":12345678901234567890123456789012345678,"a":[null,"",true]}"#;
        let exact = b"12345678901234567890.123456789";
        for profile in ["postgresql", "redshift"] {
            let mut sink = ();
            let mut adapter = Output::new(&mut sink, PgWireEngine::Redshift, profile);
            adapter.response(columns()).unwrap();
            for bytes in [Some(b"00Ff".as_slice()), Some(b"".as_slice()), None] {
                let response = adapter
                    .response(row(&[Some(document), bytes, Some(exact)]))
                    .unwrap();
                let response = crate::wire::encode(response);
                let values = crate::wire::sample_row(&response).unwrap();
                assert_eq!(
                    values[0].as_deref(),
                    Some(std::str::from_utf8(document).unwrap())
                );
                assert_eq!(
                    values[2].as_deref(),
                    Some(std::str::from_utf8(exact).unwrap())
                );
                let expected = bytes.map(|b| {
                    format!(
                        "{}{}",
                        if profile == "postgresql" { "\\x" } else { "" },
                        std::str::from_utf8(b).unwrap()
                    )
                });
                assert_eq!(values[1], expected);
            }
            let response = crate::wire::encode(adapter.response(row(&[None, None, None])).unwrap());
            assert_eq!(
                crate::wire::sample_row(&response).unwrap(),
                vec![None, None, None]
            );
        }
    }

    #[test]
    fn malformed_native_binary_is_rejected_and_later_queries_can_recover() {
        let mut sink = ();
        let mut adapter = Output::new(&mut sink, PgWireEngine::Redshift, "postgresql");
        adapter.response(columns()).unwrap();
        for invalid in [b"f".as_slice(), b"zz", b"\\x00"] {
            assert!(
                adapter
                    .response(row(&[Some(b"{}"), Some(invalid), None]))
                    .is_err()
            );
        }
        assert!(adapter.response(row(&[Some(b"{}")])).is_err());
        assert!(
            adapter
                .response(row(&[Some(b"{}"), Some(b"00"), None]))
                .is_ok()
        );
        let PgWireBackendMessage::RowDescription(mut description) = columns() else {
            unreachable!()
        };
        description.fields[0].format_code = 1;
        assert!(
            adapter
                .response(PgWireBackendMessage::RowDescription(description))
                .is_err()
        );
        adapter.response(columns()).unwrap();
        assert!(adapter.response(row(&[Some(b"null"), None, None])).is_ok());
    }
}
