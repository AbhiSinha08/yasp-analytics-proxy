//! Configurable limits, hook references, and development defaults.

use super::{ConfigError, validate_value};
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, time::Duration};

/// Optional sources of context about query output data.
#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct DataContextsSettings {
    pub ast: crate::lineage::AstSettings,
    pub decision_model: Option<crate::decision_model::DecisionModelSettings>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct HooksSettings {
    pub query_context: Option<SelectorSettings>,
    pub process_result: Option<SelectorSettings>,
    pub select_backend: SelectorSettings,
    pub python: PythonSettings,
}

impl Default for HooksSettings {
    fn default() -> Self {
        Self {
            query_context: None,
            process_result: None,
            select_backend: SelectorSettings::Rust("builtin.select_backend.passthrough".into()),
            python: PythonSettings::default(),
        }
    }
}

/// Compiled callback names retain their string configuration syntax.
#[derive(Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub enum SelectorSettings {
    Rust(String),
    RustReference(RustHookSettings),
    Python(PythonHookSettings),
}

impl SelectorSettings {
    /// The host resolves this name to its own compiled callback at startup.
    pub fn rust_callback(&self) -> Option<&str> {
        match self {
            Self::Rust(name) => Some(name),
            Self::RustReference(hook) => Some(&hook.callback),
            Self::Python(_) => None,
        }
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RustHookSettings {
    #[serde(rename = "type")]
    pub kind: RustHookKind,
    pub callback: String,
    #[serde(default)]
    pub params: serde_json::Map<String, serde_json::Value>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RustHookKind {
    Rust,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PythonHookSettings {
    #[serde(rename = "type")]
    pub kind: PythonHookKind,
    pub path: std::path::PathBuf,
    pub callback: String,
    #[serde(default)]
    pub params: serde_json::Map<String, serde_json::Value>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PythonHookKind {
    Python,
}

/// One trusted interpreter with a query worker and optional dataset loader worker.
#[derive(Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct PythonSettings {
    pub search_paths: Vec<std::path::PathBuf>,
    pub queue_capacity: usize,
    pub queue_max_bytes: usize,
    pub max_payload_bytes: usize,
    pub execution_timeout_ms: u32,
}

impl Default for PythonSettings {
    fn default() -> Self {
        Self {
            search_paths: Vec::new(),
            queue_capacity: 32,
            queue_max_bytes: 4 * 1024 * 1024,
            max_payload_bytes: 2 * 1024 * 1024,
            execution_timeout_ms: 2000,
        }
    }
}

impl PythonSettings {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.queue_capacity == 0
            || self.queue_capacity > tokio::sync::Semaphore::MAX_PERMITS
            || self.max_payload_bytes == 0
            || self.max_payload_bytes > self.queue_max_bytes
            || self.queue_max_bytes > u32::MAX as usize
            || self.queue_max_bytes > tokio::sync::Semaphore::MAX_PERMITS
            || self.execution_timeout_ms == 0
        {
            return Err(ConfigError::InvalidPythonLimit);
        }
        for path in &self.search_paths {
            validate_value("hooks.python.search_paths", &path.to_string_lossy())?;
        }
        Ok(())
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct BackendSettings {
    pub max_connections_per_pool: usize,
    pub max_connections_total: usize,
    pub acquire_timeout_ms: u64,
}

impl Default for BackendSettings {
    fn default() -> Self {
        Self {
            max_connections_per_pool: 8,
            max_connections_total: 16,
            acquire_timeout_ms: 60_000,
        }
    }
}

/// Opt-in result reuse; the host resolves the Redis URL without exposing secrets.
#[derive(Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct CacheSettings {
    pub enabled: bool,
    pub url_env: String,
    pub tls: super::ClientTlsSettings,
    pub namespace: String,
    pub ttl_seconds: u32,
    pub max_rows_per_query: usize,
    pub max_buffered_bytes_total: usize,
    pub eligibility: Option<SelectorSettings>,
}

impl Default for CacheSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            url_env: "YASP_REDIS_URL".into(),
            tls: Default::default(),
            namespace: "yasp:dev:v1".into(),
            ttl_seconds: 3600,
            max_rows_per_query: 10_000,
            max_buffered_bytes_total: 32 * 1024 * 1024,
            eligibility: None,
        }
    }
}

/// Shared application-owned lookup snapshots loaded before serving queries.
#[derive(Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ApplicationContextsSettings {
    pub url_env: String,
    pub tls: super::ClientTlsSettings,
    pub namespace: String,
    pub refresh_timeout_ms: u32,
    pub max_dataset_bytes: usize,
    pub max_lookup_bytes: usize,
    pub datasets: std::collections::BTreeMap<String, DatasetSettings>,
}

impl Default for ApplicationContextsSettings {
    fn default() -> Self {
        Self {
            url_env: "YASP_CONTEXT_REDIS_URL".into(),
            tls: Default::default(),
            namespace: "yasp:datasets".into(),
            refresh_timeout_ms: 30_000,
            max_dataset_bytes: 16_777_216,
            max_lookup_bytes: 65_536,
            datasets: std::collections::BTreeMap::new(),
        }
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct DatasetSettings {
    pub refresh_interval_seconds: u64,
    pub loader: SelectorSettings,
}

impl Default for DatasetSettings {
    fn default() -> Self {
        Self {
            refresh_interval_seconds: 0,
            loader: SelectorSettings::Rust("".into()),
        }
    }
}

impl ApplicationContextsSettings {
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.tls.validate()?;
        validate_value("application_contexts.namespace", &self.namespace)?;
        validate_value("application_contexts.url_env", &self.url_env)?;
        if self.url_env.contains('=')
            || !self
                .namespace
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b":._-".contains(&byte))
            || self.refresh_timeout_ms == 0
            || self.max_dataset_bytes == 0
            || self.max_lookup_bytes == 0
            || self.max_lookup_bytes > self.max_dataset_bytes
            || self.max_dataset_bytes > u32::MAX as usize / 4
            || self.max_dataset_bytes > tokio::sync::Semaphore::MAX_PERMITS / 4
            || self.max_lookup_bytes > u32::MAX as usize / (32 * 3)
        {
            return Err(ConfigError::InvalidApplicationContexts);
        }
        for (name, dataset) in &self.datasets {
            validate_value("application context dataset name", name)?;
            if !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"_-".contains(&byte))
                || dataset.refresh_interval_seconds > i64::MAX as u64 / 1000
            {
                return Err(ConfigError::InvalidApplicationContexts);
            }
            match &dataset.loader {
                SelectorSettings::Rust(name) => validate_value("dataset loader", name)?,
                SelectorSettings::RustReference(hook) => {
                    validate_value("dataset loader callback", &hook.callback)?;
                }
                SelectorSettings::Python(hook) => {
                    validate_value("dataset loader path", &hook.path.to_string_lossy())?;
                    validate_value("dataset loader callback", &hook.callback)?;
                }
            }
        }
        Ok(())
    }
}

impl CacheSettings {
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.tls.validate()?;
        validate_value("cache.namespace", &self.namespace)?;
        validate_value("cache.url_env", &self.url_env)?;
        if self.url_env.contains('=')
            || self.ttl_seconds == 0
            || self.max_rows_per_query == 0
            || self.max_buffered_bytes_total == 0
            || self.max_buffered_bytes_total > u32::MAX as usize
            || self.max_buffered_bytes_total > tokio::sync::Semaphore::MAX_PERMITS
        {
            return Err(ConfigError::InvalidCache);
        }
        if let Some(hook) = &self.eligibility {
            match hook {
                SelectorSettings::Rust(name) => validate_value("cache.eligibility", name)?,
                SelectorSettings::RustReference(hook) => {
                    validate_value("cache.eligibility.callback", &hook.callback)?;
                }
                SelectorSettings::Python(hook) => {
                    validate_value("cache eligibility path", &hook.path.to_string_lossy())?;
                    validate_value("cache eligibility callback", &hook.callback)?;
                }
            }
        }
        Ok(())
    }
}

/// Implemented gateway settings. Omitted fields retain the development defaults.
#[derive(Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct GatewaySettings {
    pub protocol: String,
    pub listen: SocketAddr,
    pub max_sessions: usize,
    pub max_message_bytes: usize,
    pub max_sql_bytes: usize,
    pub max_context_bytes: usize,
    pub query_context_timeout_ms: u32,
    pub async_hook_timeout_ms: u32,
    pub query_timeout_ms: u32,
    pub read_timeout_ms: u32,
    pub write_timeout_ms: u32,
    pub startup_timeout_ms: u32,
    pub shutdown_timeout_ms: u32,
    pub tls: GatewayTls,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct GatewayTls {
    pub mode: String,
    pub cert_file: Option<std::path::PathBuf>,
    pub key_file: Option<std::path::PathBuf>,
}

impl Default for GatewayTls {
    fn default() -> Self {
        Self {
            mode: "disable".into(),
            cert_file: None,
            key_file: None,
        }
    }
}

impl Default for GatewaySettings {
    fn default() -> Self {
        Self {
            protocol: "postgresql".into(),
            listen: SocketAddr::from(([127, 0, 0, 1], 6432)),
            max_sessions: 32,
            max_message_bytes: 8 * 1024 * 1024,
            max_sql_bytes: 1024 * 1024,
            max_context_bytes: 65536,
            query_context_timeout_ms: 2000,
            async_hook_timeout_ms: 2000,
            query_timeout_ms: 60_000,
            read_timeout_ms: 600_000,
            write_timeout_ms: 60_000,
            startup_timeout_ms: 120_000,
            shutdown_timeout_ms: 10_000,
            tls: GatewayTls::default(),
        }
    }
}

impl GatewaySettings {
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.tls.validate()?;
        let profile = crate::gateway::FrontendProfile::from_protocol(&self.protocol)?;
        if profile == crate::gateway::FrontendProfile::Redshift && self.tls.mode != "enable" {
            return Err(ConfigError::RedshiftRequiresTls);
        }
        if self.listen.port() == 0 {
            return Err(ConfigError::InvalidListener);
        }
        if self.max_sessions == 0
            || self.max_sql_bytes == 0
            || self.max_context_bytes == 0
            || [
                self.query_context_timeout_ms,
                self.async_hook_timeout_ms,
                self.query_timeout_ms,
                self.read_timeout_ms,
                self.write_timeout_ms,
                self.startup_timeout_ms,
                self.shutdown_timeout_ms,
            ]
            .contains(&0)
        {
            return Err(ConfigError::InvalidGatewayLimit);
        }
        // PostgreSQL statement and idle-transaction timers use signed millisecond values.
        if self.query_timeout_ms > i32::MAX as u32 || self.write_timeout_ms > i32::MAX as u32 {
            return Err(ConfigError::InvalidExecutionTimeout);
        }
        if !(8..=i32::MAX as usize).contains(&self.max_message_bytes) {
            return Err(ConfigError::InvalidFrameLimit);
        }
        Ok(())
    }

    pub(crate) fn write_timeout(&self) -> Duration {
        Duration::from_millis(u64::from(self.write_timeout_ms))
    }

    pub(crate) fn query_timeout(&self) -> Duration {
        Duration::from_millis(u64::from(self.query_timeout_ms))
    }
}
