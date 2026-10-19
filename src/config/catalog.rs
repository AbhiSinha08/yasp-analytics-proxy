use super::ConfigError;
use crate::catalog::CatalogSettings;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// A bounded snapshot of the configured discovery identity's schema definitions.
#[derive(Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct SchemaCatalogSettings {
    pub schemas: Vec<String>,
    pub refresh_interval_seconds: u64,
    pub refresh_timeout_ms: u64,
    pub max_age_seconds: u64,
    pub max_rows: usize,
    pub max_bytes: usize,
}

impl Default for SchemaCatalogSettings {
    fn default() -> Self {
        let defaults = CatalogSettings::new(Vec::new());
        Self {
            schemas: defaults.schemas,
            refresh_interval_seconds: defaults.refresh_interval.as_secs(),
            refresh_timeout_ms: defaults.refresh_timeout.as_millis() as u64,
            max_age_seconds: defaults.max_age.as_secs(),
            max_rows: defaults.max_rows,
            max_bytes: defaults.max_bytes,
        }
    }
}

impl SchemaCatalogSettings {
    pub fn runtime(&self) -> Result<CatalogSettings, ConfigError> {
        let settings = CatalogSettings {
            schemas: self.schemas.clone(),
            refresh_interval: Duration::from_secs(self.refresh_interval_seconds),
            refresh_timeout: Duration::from_millis(self.refresh_timeout_ms),
            max_age: Duration::from_secs(self.max_age_seconds),
            max_rows: self.max_rows,
            max_bytes: self.max_bytes,
        };
        settings
            .validate()
            .map_err(|_| ConfigError::InvalidCatalog)?;
        Ok(settings)
    }
}
