//! Lossless response encoding shared by the PostgreSQL and Redshift frontends.
use crate::backend::result::ResultEvent;
use pgwire::{
    error::{ErrorInfo, PgWireError, PgWireResult},
    messages::{PgWireBackendMessage, startup::ParameterStatus},
};

pub(crate) struct Response(PgWireBackendMessage);

/// The frontend adapter retains native row framing behind an opaque batch value.
pub(crate) struct ResultRow(pgwire::messages::data::DataRow);
impl ResultRow {
    pub(crate) fn column_count(&self) -> usize {
        self.0.field_count.max(0) as usize
    }
    pub(crate) fn capacity(&self) -> usize {
        self.0.data.capacity()
    }
    pub(crate) fn len(&self) -> usize {
        self.0.data.len() + 7
    }
    pub(crate) fn value(&self, range: std::ops::Range<usize>) -> &[u8] {
        &self.0.data[range]
    }
    pub(crate) fn fields(
        &self,
    ) -> Result<Vec<Option<std::ops::Range<usize>>>, crate::hooks::ResultHookError> {
        use crate::hooks::ResultHookError;
        let mut position = 0;
        let mut fields = Vec::with_capacity(self.column_count());
        for _ in 0..self.column_count() {
            let length = self
                .0
                .data
                .get(position..position + 4)
                .ok_or(ResultHookError::InvalidOutput)?;
            let length = i32::from_be_bytes(length.try_into().unwrap());
            position += 4;
            if length == -1 {
                fields.push(None);
                continue;
            }
            let length = usize::try_from(length).map_err(|_| ResultHookError::InvalidOutput)?;
            let end = position
                .checked_add(length)
                .filter(|n| *n <= self.0.data.len())
                .ok_or(ResultHookError::InvalidOutput)?;
            fields.push(Some(position..end));
            position = end;
        }
        if position != self.0.data.len() || self.0.field_count < 0 {
            return Err(ResultHookError::InvalidOutput);
        }
        Ok(fields)
    }
    pub(crate) fn into_response(self) -> Response {
        Response(PgWireBackendMessage::DataRow(self.0))
    }
}

pub(crate) enum ProcessingEvent {
    Columns(Vec<crate::hooks::ResultColumn>, Response),
    Row(ResultRow),
    Complete,
    Other(Response),
}
pub(crate) fn processing_event(response: Response) -> ProcessingEvent {
    match response.0 {
        PgWireBackendMessage::RowDescription(description) => {
            let columns = description
                .fields
                .iter()
                .enumerate()
                .map(|(ordinal, field)| crate::hooks::ResultColumn {
                    ordinal,
                    name: field.name.clone(),
                    data_type: format!("pgwire:{}:{}", field.type_id, field.type_modifier),
                    text: field.format_code == 0 && matches!(field.type_id, 25 | 1042 | 1043),
                    max_chars: matches!(field.type_id, 1042 | 1043)
                        .then(|| {
                            field
                                .type_modifier
                                .checked_sub(4)
                                .and_then(|n| usize::try_from(n).ok())
                        })
                        .flatten(),
                })
                .collect();
            ProcessingEvent::Columns(
                columns,
                Response(PgWireBackendMessage::RowDescription(description)),
            )
        }
        PgWireBackendMessage::DataRow(row) => ProcessingEvent::Row(ResultRow(row)),
        PgWireBackendMessage::CommandComplete(_) => ProcessingEvent::Complete,
        message => ProcessingEvent::Other(Response(message)),
    }
}
pub(crate) fn result_row<'a>(
    values: impl Iterator<Item = Option<&'a [u8]>> + Clone,
    count: usize,
) -> Response {
    use bytes::BufMut;
    let bytes = values.clone().map(|v| 4 + v.map_or(0, <[u8]>::len)).sum();
    let mut data = bytes::BytesMut::with_capacity(bytes);
    for value in values {
        if let Some(value) = value {
            data.put_i32(value.len() as i32);
            data.extend_from_slice(value);
        } else {
            data.put_i32(-1);
        }
    }
    Response(PgWireBackendMessage::DataRow(
        pgwire::messages::data::DataRow::new(data, count as i16),
    ))
}
impl Response {
    pub(crate) fn buffer_bytes(&self) -> usize {
        use pgwire::messages::Message;
        128 + match self.message() {
            PgWireBackendMessage::DataRow(row) => row.data.capacity(),
            PgWireBackendMessage::RowDescription(columns) => {
                columns.fields.capacity()
                    * std::mem::size_of::<pgwire::messages::data::FieldDescription>()
                    + columns
                        .fields
                        .iter()
                        .map(|field| field.name.capacity())
                        .sum::<usize>()
            }
            PgWireBackendMessage::NoticeResponse(notice) => notice.message_length() * 8,
            _ => 256,
        }
    }
    pub(crate) fn from_message(message: PgWireBackendMessage) -> Self {
        Self(message)
    }
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

/// Sampling inspects the frontend's already lossless text representation. Native
/// backend codecs and PostgreSQL OIDs stay outside the model client.
pub(crate) fn sample_columns(
    response: &Response,
) -> Option<Vec<crate::decision_model::ColumnIdentity>> {
    let PgWireBackendMessage::RowDescription(description) = response.message() else {
        return None;
    };
    Some(
        description
            .fields
            .iter()
            .enumerate()
            .map(|(ordinal, field)| crate::decision_model::ColumnIdentity {
                ordinal,
                name: field.name.clone(),
                data_type: format!("pgwire:{}:{}", field.type_id, field.type_modifier),
            })
            .collect(),
    )
}

pub(crate) fn sample_row(response: &Response) -> Option<Vec<Option<String>>> {
    use bytes::Buf;
    let PgWireBackendMessage::DataRow(row) = response.message() else {
        return None;
    };
    let mut data = row.data.clone();
    let mut values = Vec::with_capacity(row.field_count.max(0) as usize);
    for _ in 0..row.field_count {
        if data.remaining() < 4 {
            return None;
        }
        let length = data.get_i32();
        if length == -1 {
            values.push(None);
            continue;
        }
        let length = usize::try_from(length).ok()?;
        if length > data.remaining() {
            return None;
        }
        let bytes = data.split_to(length);
        values.push(Some(std::str::from_utf8(&bytes).ok()?.to_owned()));
    }
    if data.has_remaining() {
        return None;
    }
    Some(values)
}
