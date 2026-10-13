//! Named BI helpers awaiting verified client metadata formats.

use serde_json::{Map, Value};

#[derive(Debug, thiserror::Error)]
pub enum ExtractionError {
    #[error("BI metadata extraction is not implemented")]
    NotImplemented,
}

pub fn metabase(_sql: &str) -> Result<Option<Map<String, Value>>, ExtractionError> {
    Err(ExtractionError::NotImplemented)
}

pub fn superset(_sql: &str) -> Result<Option<Map<String, Value>>, ExtractionError> {
    Err(ExtractionError::NotImplemented)
}

pub fn redash(_sql: &str) -> Result<Option<Map<String, Value>>, ExtractionError> {
    Err(ExtractionError::NotImplemented)
}
