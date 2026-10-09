//! Validated gateway credentials supplied by the host application.

use thiserror::Error;

/// Credentials supplied by a host application. Secrets are not printable.
pub struct GatewayConfig {
    pub(crate) username: String,
    pub(crate) password: String,
    pub(crate) database: String,
}

#[derive(Debug, Error)]
#[error("{0} must be nonempty and contain no NUL characters")]
pub struct ConfigError(&'static str);

impl GatewayConfig {
    pub fn new(username: String, password: String, database: String) -> Result<Self, ConfigError> {
        for (name, value) in [
            ("YASP_GATEWAY_USERNAME", &username),
            ("YASP_GATEWAY_PASSWORD", &password),
            ("YASP_GATEWAY_DATABASE", &database),
        ] {
            if value.trim().is_empty() || value.contains('\0') {
                return Err(ConfigError(name));
            }
        }
        Ok(Self {
            username,
            password,
            database,
        })
    }
}
