//! Runtime connection settings supplied by the host after secret resolution.

use super::{ConfigError, GatewaySettings};

/// Credentials supplied by a host application. Secrets are not printable.
pub struct GatewayConfig {
    pub(crate) username: String,
    pub(crate) password: String,
    pub(crate) database: String,
    pub(crate) settings: GatewaySettings,
    pub(crate) cache: Option<std::sync::Arc<crate::cache::RedisCache>>,
    pub(crate) context_hook: Option<std::sync::Arc<crate::hooks::ContextHook>>,
    pub(crate) schema_discovery: Option<crate::hooks::BackendSelection>,
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
            cache: None,
            context_hook: None,
            schema_discovery: None,
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

    /// Use a configured pair for discovery without invoking application hooks.
    pub fn with_schema_discovery(mut self, route: crate::hooks::BackendSelection) -> Self {
        self.schema_discovery = Some(route);
        self
    }

    /// Attach one process-wide cache, shared by all frontend sessions.
    pub fn with_cache(mut self, cache: std::sync::Arc<crate::cache::RedisCache>) -> Self {
        self.cache = Some(cache);
        self
    }

    /// Derive application-owned context once per ordinary query before downstream hooks.
    pub fn with_context_hook(mut self, hook: impl Into<crate::hooks::ContextHook>) -> Self {
        self.context_hook = Some(std::sync::Arc::new(hook.into()));
        self
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
