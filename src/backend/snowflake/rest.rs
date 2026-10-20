//! Bounded HTTP responses and SQL API result envelopes. Redirects are disabled;
//! server-provided URLs are never followed with credentials.
use super::Inner;
use crate::backend::result::{ExecutionError, ExecutionResult};
use futures::StreamExt;
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Deserialize)]
pub(super) struct Field {
    pub name: String,
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub precision: u8,
    #[serde(default)]
    pub scale: i8,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Metadata {
    pub num_rows: u64,
    pub format: String,
    pub row_type: Vec<Field>,
    #[serde(default)]
    pub partition_info: Vec<Partition>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Partition {
    pub row_count: u64,
    pub uncompressed_size: usize,
}
#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Response {
    pub result_set_meta_data: Option<Metadata>,
    #[serde(default)]
    pub data: Vec<Vec<Option<String>>>,
    #[serde(default)]
    pub statement_handle: String,
    #[serde(default)]
    pub code: String,
    #[serde(default)]
    pub sql_state: String,
    #[serde(default)]
    pub message: String,
}
impl Response {
    pub(super) fn pending(&self) -> bool {
        self.code == "333334" || self.code == "333333"
    }
    pub(super) fn check(&self) -> ExecutionResult<()> {
        if self.pending()
            || self.code.is_empty()
            || self.code == "090001"
            || self.code == "000000"
            || self.code == "0"
        {
            return Ok(());
        }
        let code = if self.code == "000604" {
            "57014".into()
        } else if self.sql_state.len() == 5
            && self
                .sql_state
                .bytes()
                .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
            && self.sql_state != "00000"
        {
            self.sql_state.clone()
        } else {
            "XX000".into()
        };
        Err(ExecutionError::Source {
            code,
            message: self.message.clone(),
        })
    }
}
pub(super) fn protocol() -> ExecutionError {
    ExecutionError::rejected("08006", "invalid Snowflake SQL API response")
}
pub(super) fn oversized() -> ExecutionError {
    ExecutionError::rejected("54000", "Snowflake response exceeds max_response_bytes")
}
impl Inner {
    pub(super) async fn request(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<Value>,
    ) -> ExecutionResult<Response> {
        let token = self.auth.token().map_err(|_| {
            ExecutionError::rejected("28000", "cannot sign Snowflake authentication token")
        })?;
        let mut request = self
            .client
            .request(method, format!("{}{path}", self.endpoint))
            .bearer_auth(token)
            .header("Accept", "application/json")
            .header("Content-Type", "application/json")
            .header("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT");
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request
            .send()
            .await
            .map_err(|_| ExecutionError::rejected("08006", "Snowflake HTTP request failed"))?;
        let status = response.status();
        if response
            .content_length()
            .is_some_and(|n| n > self.max_response_bytes as u64)
        {
            return Err(oversized());
        }
        let mut stream = response.bytes_stream();
        let mut bytes = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|_| {
                ExecutionError::rejected("08006", "Snowflake response download failed")
            })?;
            if chunk.len() > self.max_response_bytes.saturating_sub(bytes.len()) {
                return Err(oversized());
            }
            bytes.extend_from_slice(&chunk);
        }
        let result: Response = serde_json::from_slice(&bytes).map_err(|_| protocol())?;
        if !(status.is_success() || status.as_u16() == 202) {
            // SQL errors (422) and execution timeouts (408) are terminal.
            // Return its envelope so the operation can release admission normally.
            if matches!(status.as_u16(), 408 | 422) && result.check().is_err() {
                return Ok(result);
            }
            result.check()?;
            return Err(ExecutionError::rejected(
                "08006",
                "Snowflake HTTP request was rejected",
            ));
        }
        Ok(result)
    }
}
/// Handles are UUID-shaped opaque identifiers, never URLs or arbitrary paths.
pub(super) fn handle(value: &str) -> ExecutionResult<&str> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'-')
    {
        return Err(protocol());
    }
    Ok(value)
}
