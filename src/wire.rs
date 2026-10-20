//! Lossless response encoding shared by the PostgreSQL and Redshift frontends.
use crate::backend::result::ResultEvent;
use pgwire::{
    error::{ErrorInfo, PgWireError, PgWireResult},
    messages::{PgWireBackendMessage, startup::ParameterStatus},
};

pub(crate) struct Response(PgWireBackendMessage);
impl Response {
    pub(crate) fn into_wire(self) -> PgWireBackendMessage {
        self.0
    }
    pub(crate) fn message(&self) -> &PgWireBackendMessage {
        &self.0
    }
}
impl From<PgWireBackendMessage> for ResultEvent {
    fn from(value: PgWireBackendMessage) -> Self {
        Self::Wire(Response(value))
    }
}
pub(crate) fn encode(event: ResultEvent) -> Response {
    Response(match event {
        ResultEvent::Wire(message) => return message,
        ResultEvent::Parameter { name, value } => {
            PgWireBackendMessage::ParameterStatus(ParameterStatus::new(name, value))
        }
        ResultEvent::Columns(columns) => columns_message(columns),
        ResultEvent::Row(values) => {
            use bytes::BufMut;
            let count = values.len() as i16;
            let mut data = bytes::BytesMut::new();
            for value in values {
                match value {
                    Some(value) => {
                        data.put_i32(value.len() as i32);
                        data.extend_from_slice(&value);
                    }
                    None => data.put_i32(-1),
                }
            }
            PgWireBackendMessage::DataRow(pgwire::messages::data::DataRow::new(data, count))
        }
        ResultEvent::Complete(rows) => PgWireBackendMessage::CommandComplete(
            pgwire::messages::response::CommandComplete::new(format!("SELECT {rows}")),
        ),
    })
}

fn columns_message(columns: Vec<crate::backend::result::ResultColumn>) -> PgWireBackendMessage {
    use crate::backend::result::ColumnType;
    use pgwire::messages::data::{FieldDescription, RowDescription};
    let fields = columns
        .into_iter()
        .map(|column| {
            let (oid, size, modifier) = match column.kind {
                ColumnType::Bool => (16, 1, -1),
                ColumnType::Int16 => (21, 2, -1),
                ColumnType::Int32 => (23, 4, -1),
                ColumnType::Oid => (26, 4, -1),
                ColumnType::Int64 => (20, 8, -1),
                ColumnType::Int16Array => (1005, -1, -1),
                ColumnType::TextArray => (1009, -1, -1),
                ColumnType::Json => (114, -1, -1),
                ColumnType::Float32 => (700, 4, -1),
                ColumnType::Float64 => (701, 8, -1),
                ColumnType::Decimal { precision, scale } => (
                    1700,
                    -1,
                    ((i32::from(precision) << 16) | i32::from(scale)) + 4,
                ),
                ColumnType::Text => (25, -1, -1),
                ColumnType::Binary => (17, -1, -1),
                ColumnType::Date => (1082, 4, -1),
                ColumnType::Time => (1083, 8, 6),
                ColumnType::Timestamp { utc } => (if utc { 1184 } else { 1114 }, 8, 6),
            };
            FieldDescription::new(column.name, 0, 0, oid, size, modifier, 0)
        })
        .collect();
    PgWireBackendMessage::RowDescription(RowDescription::new(fields))
}

pub(crate) fn validate_text_fields(field_count: i16, mut data: &[u8]) -> PgWireResult<()> {
    for _ in 0..field_count {
        let length = data
            .get(..4)
            .ok_or_else(|| error_response("08006", "invalid PostgreSQL row"))?;
        let length = i32::from_be_bytes(length.try_into().unwrap());
        data = &data[4..];
        if length == -1 {
            continue;
        }
        if length < 0 || length as usize > data.len() {
            return Err(error_response("08006", "invalid PostgreSQL row"));
        }
        let (value, rest) = data.split_at(length as usize);
        std::str::from_utf8(value)
            .map_err(|_| error_response("0A000", "PostgreSQL text must remain UTF8"))?;
        data = rest;
    }
    if field_count < 0 || !data.is_empty() {
        return Err(error_response("08006", "invalid PostgreSQL row"));
    }
    Ok(())
}

fn error_response(code: &str, message: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".into(),
        code.into(),
        message.into(),
    )))
}
