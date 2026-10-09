//! Validated gateway and PostgreSQL settings supplied by the host application.

use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::File,
    io::{self, Read},
    net::SocketAddr,
    path::Path,
    time::Duration,
};
use thiserror::Error;

/// Runtime connection details and secret key names. The host resolves passwords.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FileConfig {
    pub version: u16,
    #[serde(default)]
    pub gateway: GatewaySettings,
    pub targets: BTreeMap<String, TargetSettings>,
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
}

impl Default for GatewayTls {
    fn default() -> Self {
        Self {
            mode: "local_development".into(),
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
            query_timeout_ms: 60_000,
            read_timeout_ms: 60_000,
            write_timeout_ms: 60_000,
            startup_timeout_ms: 120_000,
            shutdown_timeout_ms: 10_000,
            tls: GatewayTls::default(),
        }
    }
}

impl GatewaySettings {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.protocol != "postgresql" || self.tls.mode != "local_development" {
            return Err(ConfigError::UnsupportedGateway);
        }
        if !self.listen.ip().is_loopback() || self.listen.port() == 0 {
            return Err(ConfigError::InvalidListener);
        }
        if self.max_sessions == 0
            || self.max_sql_bytes == 0
            || [
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

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LoginSettings {
    pub username: String,
    pub password_env: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TargetSettings {
    pub engine: String,
    pub host: String,
    pub port: u16,
    pub database: String,
    pub tls_mode: String,
    pub logins: BTreeMap<String, LoginSettings>,
}

impl FileConfig {
    pub fn read(path: &Path) -> Result<Self, ConfigError> {
        let mut input = String::new();
        File::open(path)
            .and_then(|file| file.take(65537).read_to_string(&mut input))
            .map_err(ConfigError::ReadFile)?;
        if input.len() > 65536 {
            return Err(ConfigError::TooLarge);
        }
        // YAML diagnostics can contain configuration values. Keep them private.
        let config: Self = serde_saphyr::from_str(&input).map_err(|_| ConfigError::InvalidYaml)?;
        config.gateway.validate()?;
        config.connection()?;
        Ok(config)
    }

    /// Fixed forwarding uses one configured target/login. Per-query selection is
    /// deferred; additional connections must not be silently ignored.
    pub fn connection(&self) -> Result<(&TargetSettings, &LoginSettings), ConfigError> {
        if self.version != 1 {
            return Err(ConfigError::UnsupportedVersion);
        }
        if self.targets.len() != 1 {
            return Err(ConfigError::UnsupportedTopology);
        }
        let target = self.targets.values().next().unwrap();
        if target.logins.len() != 1 {
            return Err(ConfigError::UnsupportedTopology);
        }
        if target.engine != "postgresql" || target.tls_mode != "disable" {
            return Err(ConfigError::UnsupportedTarget);
        }
        Ok((target, target.logins.values().next().unwrap()))
    }
}

/// Credentials supplied by a host application. Secrets are not printable.
pub struct GatewayConfig {
    pub(crate) username: String,
    pub(crate) password: String,
    pub(crate) database: String,
    pub(crate) settings: GatewaySettings,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("cannot read configuration file")]
    ReadFile(#[source] io::Error),
    #[error("configuration exceeds the 64 KiB limit")]
    TooLarge,
    #[error("invalid YAML configuration: check fields, types, and duplicate keys")]
    InvalidYaml,
    #[error("configuration version must be 1")]
    UnsupportedVersion,
    #[error("exactly one target with one login is currently supported")]
    UnsupportedTopology,
    #[error("target engine must be postgresql and tls_mode must be disable")]
    UnsupportedTarget,
    #[error("gateway protocol must be postgresql and tls.mode must be local_development")]
    UnsupportedGateway,
    #[error("gateway.listen must be a loopback IP address with a nonzero port")]
    InvalidListener,
    #[error("gateway session, SQL, and timeout limits must be positive")]
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

impl GatewayConfig {
    pub fn new(username: String, password: String, database: String) -> Result<Self, ConfigError> {
        for (name, value) in [
            ("gateway.username", &username),
            ("gateway password", &password),
            ("gateway.database", &database),
        ] {
            if value.trim().is_empty() || value.contains('\0') {
                return Err(ConfigError::InvalidValue(name));
            }
        }
        Ok(Self {
            username,
            password,
            database,
            settings: GatewaySettings::default(),
        })
    }

    pub fn with_settings(mut self, settings: GatewaySettings) -> Result<Self, ConfigError> {
        settings.validate()?;
        self.settings = settings;
        Ok(self)
    }

    pub fn settings(&self) -> &GatewaySettings {
        &self.settings
    }
}

/// One local PostgreSQL login. The host owns secret loading; library consumers
/// supply these settings directly. This development adapter has no TLS yet.
pub struct BackendConfig {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) username: String,
    pub(crate) password: String,
    pub(crate) database: String,
}

impl BackendConfig {
    pub fn new(
        host: String,
        port: u16,
        username: String,
        password: String,
        database: String,
    ) -> Result<Self, ConfigError> {
        for (name, value) in [
            ("target.host", &host),
            ("target login.username", &username),
            ("target login password", &password),
            ("target.database", &database),
        ] {
            if value.trim().is_empty() || value.contains('\0') {
                return Err(ConfigError::InvalidValue(name));
            }
        }
        if port == 0 {
            return Err(ConfigError::InvalidPort);
        }
        if host != "localhost"
            && !host
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
        {
            return Err(ConfigError::NonLocalBackend);
        }
        Ok(Self {
            host,
            port,
            username,
            password,
            database,
        })
    }
}
