//! Validated gateway and PostgreSQL settings supplied by the host application.

use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::File,
    io::{self, Read},
    path::Path,
};
use thiserror::Error;

/// Runtime connection details and secret key names. The host resolves passwords.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FileConfig {
    pub version: u16,
    pub targets: BTreeMap<String, TargetSettings>,
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
        })
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
