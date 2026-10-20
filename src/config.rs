//! Validated gateway and PostgreSQL settings supplied by the host application.

mod catalog;
mod file;
mod runtime;
mod settings;

pub use crate::tls::{ClientTlsSettings, TargetTls};
pub use catalog::SchemaCatalogSettings;
pub use file::{
    ComputeInstanceSettings, ComputeSettings, ComputeTypeSettings, CredentialOverride, FileConfig,
    LoginSettings, Logins, SchemaDiscoverySettings, SnowflakeSettings, TargetSettings,
};
pub use runtime::{BackendConfig, GatewayConfig};
pub use settings::{
    ApplicationContextsSettings, BackendSettings, CacheSettings, DatasetSettings, GatewaySettings,
    GatewayTls, HooksSettings, PythonHookKind, PythonHookSettings, PythonSettings, RustHookKind,
    RustHookSettings, SelectorSettings,
};

use std::io;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("invalid TLS configuration or certificate material")]
    InvalidTls,
    #[error("schema_discovery.target is required when multiple targets are configured")]
    MissingDiscoveryTarget,
    #[error("schema_discovery must identify a configured target and login")]
    InvalidDiscoveryBackend,
    #[error("schema_discovery.compute is required when several eligible instances exist")]
    MissingDiscoveryCompute,
    #[error("compute topology, limits, and login eligibility must be valid")]
    InvalidComputeTopology,
    #[error("cache requires positive row, byte-budget, and TTL limits")]
    InvalidCache,
    #[error("application context dataset limits or names are invalid")]
    InvalidApplicationContexts,
    #[error(
        "schema_discovery.catalog is required with nonempty schemas and valid refresh, age, row and byte bounds"
    )]
    InvalidCatalog,
    #[error(
        "Python limits must be positive, the payload must fit the byte budget, and queue bounds must fit their counters"
    )]
    InvalidPythonLimit,
    #[error("cannot read configuration file")]
    ReadFile(#[source] io::Error),
    #[error("configuration exceeds the 64 KiB limit")]
    TooLarge,
    #[error("invalid YAML configuration: check fields, types, and duplicate keys")]
    InvalidYaml,
    #[error("configuration version must be 1")]
    UnsupportedVersion,
    #[error("at least one target with one login is required")]
    MissingRoutes,
    #[error("connection() requires exactly one configured target with one login")]
    UnsupportedTopology,
    #[error("backend pool limits must be positive and acquire_timeout_ms must be 1..=2147483647")]
    InvalidPoolLimit,
    #[error(
        "passthrough params must contain optional target, backend_login, and compute_type strings"
    )]
    InvalidSelectorParams,
    #[error("passthrough params must identify a configured target and backend login")]
    InvalidDefaultBackend,
    #[error("passthrough fields must resolve to one configured target, login, and compute type")]
    MissingDefaultBackend,
    #[error("target engine must be postgresql or snowflake, with engine-specific settings")]
    UnsupportedTarget,
    #[error("Snowflake targets require the snowflake Cargo feature")]
    SnowflakeDisabled,
    #[error("invalid Snowflake target or login settings")]
    InvalidSnowflake,
    #[error("gateway protocol must be postgresql or redshift")]
    UnsupportedGateway,
    #[error("the Redshift password authentication profile requires gateway TLS")]
    RedshiftRequiresTls,
    #[error("gateway.listen requires a nonzero port")]
    InvalidListener,
    #[error("gateway session, SQL, context, hook, and timeout limits must be positive")]
    InvalidGatewayLimit,
    #[error("gateway query/write timeouts must not exceed 2147483647 milliseconds")]
    InvalidExecutionTimeout,
    #[error("gateway.max_message_bytes must be between 8 and 2147483647")]
    InvalidFrameLimit,
    #[error("{0} must be nonempty and contain no NUL characters")]
    InvalidValue(&'static str),
    #[error("target.port must be between 1 and 65535")]
    InvalidPort,
    #[error("target.host requires localhost or a loopback IP address")]
    NonLocalBackend,
}

fn validate_value(name: &'static str, value: &str) -> Result<(), ConfigError> {
    if value.trim().is_empty() || value.contains('\0') {
        return Err(ConfigError::InvalidValue(name));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
