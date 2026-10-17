//! Runtime connection settings supplied by the host after secret resolution.

use super::{ConfigError, GatewaySettings};

/// Credentials supplied by a host application. Secrets are not printable.
pub struct GatewayConfig {
    pub(crate) username: String,
    pub(crate) password: String,
    pub(crate) database: String,
    pub(crate) settings: GatewaySettings,
    pub(crate) tls: Option<std::sync::Arc<crate::tls::ServerTls>>,
    pub(crate) cache: Option<std::sync::Arc<crate::cache::RedisCache>>,
    pub(crate) context_hook: Option<std::sync::Arc<crate::hooks::ContextHook>>,
    pub(crate) schema_discovery: Option<crate::hooks::BackendSelection>,
    pub(crate) schema_discovery_compute: Option<String>,
    pub(crate) datasets: Option<crate::datasets::DatasetReader>,
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
            tls: None,
            cache: None,
            context_hook: None,
            schema_discovery: None,
            schema_discovery_compute: None,
            datasets: None,
        })
    }

    pub fn with_settings(mut self, settings: GatewaySettings) -> Result<Self, ConfigError> {
        settings.validate()?;
        self.tls = crate::tls::ServerTls::load(&settings.tls)?;
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

    pub fn with_schema_discovery_compute(mut self, compute: String) -> Self {
        self.schema_discovery_compute = Some(compute);
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

    pub fn with_datasets(mut self, reader: crate::datasets::DatasetReader) -> Self {
        self.datasets = Some(reader);
        self
    }
}

/// One PostgreSQL login. The host owns secret loading and certificate paths.
pub struct BackendConfig {
    pub(crate) tls: Option<std::sync::Arc<crate::tls::ClientTls>>,
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) username: String,
    pub(crate) password: String,
    pub(crate) database: String,
}

impl BackendConfig {
    /// Connect without TLS to the configured endpoint, including private-network hosts.
    pub fn new(
        host: String,
        port: u16,
        username: String,
        password: String,
        database: String,
    ) -> Result<Self, ConfigError> {
        Self::build(host, port, username, password, database, None)
    }

    /// Connect with certificate and hostname verification, optionally using a client identity.
    pub fn new_with_tls(
        host: String,
        port: u16,
        username: String,
        password: String,
        database: String,
        tls: super::ClientTlsSettings,
    ) -> Result<Self, ConfigError> {
        let tls = std::sync::Arc::new(crate::tls::ClientTls::load(&tls)?);
        Self::build(host, port, username, password, database, Some(tls))
    }

    fn build(
        host: String,
        port: u16,
        username: String,
        password: String,
        database: String,
        tls: Option<std::sync::Arc<crate::tls::ClientTls>>,
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
        if tls.is_some() {
            rustls::pki_types::ServerName::try_from(host.as_str())
                .map_err(|_| ConfigError::InvalidTls)?;
        }
        Ok(Self {
            tls,
            host,
            port,
            username,
            password,
            database,
        })
    }
}
