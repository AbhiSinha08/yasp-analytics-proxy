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
    #[serde(default)]
    pub hooks: HooksSettings,
    #[serde(default)]
    pub backend: BackendSettings,
    pub targets: BTreeMap<String, TargetSettings>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct HooksSettings {
    pub select_backend: SelectorSettings,
    pub python: PythonSettings,
}

impl Default for HooksSettings {
    fn default() -> Self {
        Self {
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

/// One trusted interpreter and one dedicated worker per host process.
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
}

impl Default for BackendSettings {
    fn default() -> Self {
        Self {
            max_connections_per_pool: 8,
            max_connections_total: 16,
        }
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
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.version != 1 {
            return Err(ConfigError::UnsupportedVersion);
        }
        self.gateway.validate()?;
        self.hooks.python.validate()?;
        match &self.hooks.select_backend {
            SelectorSettings::Rust(name) => validate_value("hooks.select_backend", name)?,
            SelectorSettings::RustReference(hook) => {
                validate_value("hooks.select_backend.callback", &hook.callback)?;
            }
            SelectorSettings::Python(hook) => {
                validate_value("Python hook path", &hook.path.to_string_lossy())?;
                validate_value("Python callback", &hook.callback)?;
            }
        }
        if self.backend.max_connections_per_pool == 0
            || self.backend.max_connections_total == 0
            || self.backend.max_connections_per_pool > self.backend.max_connections_total
        {
            return Err(ConfigError::InvalidPoolLimit);
        }
        let mut pool_capacity = 0usize;
        for (target_name, target) in &self.targets {
            validate_value("target name", target_name)?;
            for (name, value) in [
                ("target.engine", &target.engine),
                ("target.host", &target.host),
                ("target.database", &target.database),
                ("target.tls_mode", &target.tls_mode),
            ] {
                validate_value(name, value)?;
            }
            if target.engine != "postgresql" || target.tls_mode != "disable" {
                return Err(ConfigError::UnsupportedTarget);
            }
            if target.port == 0 {
                return Err(ConfigError::InvalidPort);
            }
            if target.host != "localhost"
                && !target
                    .host
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback())
            {
                return Err(ConfigError::NonLocalBackend);
            }
            if target.logins.is_empty() {
                return Err(ConfigError::MissingRoutes);
            }
            for (login_name, login) in &target.logins {
                validate_value("backend login name", login_name)?;
                validate_value("backend login username", &login.username)?;
                validate_value("backend login password_env", &login.password_env)?;
                if login.password_env.contains('=') {
                    return Err(ConfigError::InvalidValue("backend login password_env"));
                }
                pool_capacity = pool_capacity
                    .checked_add(self.backend.max_connections_per_pool)
                    .ok_or(ConfigError::InvalidPoolLimit)?;
            }
        }
        if self.targets.is_empty() {
            return Err(ConfigError::MissingRoutes);
        }
        if pool_capacity > self.backend.max_connections_total {
            return Err(ConfigError::InvalidPoolLimit);
        }
        if self.hooks.select_backend.rust_callback() == Some("builtin.select_backend.passthrough") {
            self.default_backend()?;
        }
        Ok(())
    }

    /// Resolve builtin passthrough params, or infer the sole configured pair.
    /// Application selectors interpret their own params when the host binds them.
    pub fn default_backend(&self) -> Result<crate::hooks::BackendSelection, ConfigError> {
        if let SelectorSettings::RustReference(hook) = &self.hooks.select_backend
            && hook.callback == "builtin.select_backend.passthrough"
            && !hook.params.is_empty()
        {
            let selection = serde_json::from_value(serde_json::Value::Object(hook.params.clone()))
                .map_err(|_| ConfigError::InvalidSelectorParams)?;
            self.validate_selection(&selection)?;
            return Ok(selection);
        }
        let mut pairs = self.targets.iter().flat_map(|(target, settings)| {
            settings
                .logins
                .keys()
                .map(move |login| crate::hooks::BackendSelection {
                    target: target.clone(),
                    backend_login: login.clone(),
                })
        });
        let first = pairs.next();
        if pairs.next().is_some() {
            return Err(ConfigError::MissingDefaultBackend);
        }
        first.ok_or(ConfigError::MissingDefaultBackend)
    }

    fn validate_selection(
        &self,
        selection: &crate::hooks::BackendSelection,
    ) -> Result<(), ConfigError> {
        validate_value("hooks.select_backend.params.target", &selection.target)?;
        validate_value(
            "hooks.select_backend.params.backend_login",
            &selection.backend_login,
        )?;
        if !self
            .targets
            .get(&selection.target)
            .is_some_and(|target| target.logins.contains_key(&selection.backend_login))
        {
            return Err(ConfigError::InvalidDefaultBackend);
        }
        Ok(())
    }

    /// Return the configured default pair for hosts that still need one connection.
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

#[cfg(test)]
mod tests {
    use super::*;

    fn make_config(hooks: &str, targets: &str, backend: &str) -> FileConfig {
        serde_saphyr::from_str(&format!(
            "version: 1\nhooks:\n  select_backend: {hooks}\nbackend:\n{backend}\ntargets:\n{targets}"
        ))
        .unwrap()
    }

    const ONE_TARGET: &str = "  primary:\n    engine: postgresql\n    host: localhost\n    port: 5432\n    database: analytics\n    tls_mode: disable\n    logins:\n      reader:\n        username: reader\n        password_env: READER_PASSWORD";

    #[test]
    fn example_is_loadable_without_secrets_or_database_access() {
        let config =
            FileConfig::read(&Path::new(env!("CARGO_MANIFEST_DIR")).join("config/example.yml"))
                .unwrap();
        let selected = config.default_backend().unwrap();
        assert_eq!(selected.target, "primary");
        assert_eq!(selected.backend_login, "restricted");
        assert_eq!(
            config
                .targets
                .values()
                .map(|target| target.logins.len())
                .sum::<usize>(),
            4
        );
    }

    #[test]
    fn application_params_are_preserved_for_both_languages() {
        let params = serde_json::json!({"rbac": {"group": [true, null, 4]}, "label": "custom"});
        for reference in [
            "{type: rust, callback: app.selector, params: {rbac: {group: [true, null, 4]}, label: custom}}",
            "{type: python, path: ../app.py, callback: custom, params: {rbac: {group: [true, null, 4]}, label: custom}}",
        ] {
            let config = make_config(reference, ONE_TARGET, "  {}");
            config.validate().unwrap();
            let preserved = match config.hooks.select_backend {
                SelectorSettings::RustReference(hook) => hook.params,
                SelectorSettings::Python(hook) => hook.params,
                _ => panic!("object reference expected"),
            };
            assert_eq!(serde_json::Value::Object(preserved), params);
        }
        for reference in [
            "{type: rust, callback: builtin.select_backend.passthrough}",
            "{type: rust, callback: builtin.select_backend.passthrough, params: {}}",
        ] {
            let config = make_config(reference, ONE_TARGET, "  {}");
            config.validate().unwrap();
            assert_eq!(config.default_backend().unwrap().backend_login, "reader");
        }
        let config: FileConfig =
            serde_saphyr::from_str(&format!("version: 1\ntargets:\n{ONE_TARGET}")).unwrap();
        config.validate().unwrap();
        assert_eq!(config.default_backend().unwrap().target, "primary");
    }

    #[test]
    fn rejects_malformed_references_and_unsupported_sections() {
        for reference in [
            "{type: rust, callback: builtin.select_backend.passthrough, path: app.py}",
            "{type: rust, callback: app.selector, params: [1]}",
            "{type: python, path: app.py, callback: custom, params: [1]}",
            "{type: python, callback: custom}",
        ] {
            assert!(
                serde_saphyr::from_str::<HooksSettings>(&format!("select_backend: {reference}"))
                    .is_err()
            );
        }
        for section in [
            "routing: {default_backend: {target: primary, backend_login: reader}}",
            "principals: {}",
            "cache: {enabled: false}",
            "policy: {result_masks: []}",
            "hooks: {result_processing: {enabled: false}}",
            "hooks: {python: {worker_count: 1}}",
        ] {
            assert!(
                serde_saphyr::from_str::<FileConfig>(&format!(
                    "version: 1\ntargets:\n{ONE_TARGET}\n{section}"
                ))
                .is_err()
            );
        }
        for params in [
            "{target: primary}",
            "{target: 1, backend_login: reader}",
            "{target: primary, backend_login: reader, extra: true}",
        ] {
            let config = make_config(
                &format!(
                    "{{type: rust, callback: builtin.select_backend.passthrough, params: {params}}}"
                ),
                ONE_TARGET,
                "  {}",
            );
            assert!(matches!(
                config.validate(),
                Err(ConfigError::InvalidSelectorParams)
            ));
        }
        for params in [
            "{target: '', backend_login: reader}",
            "{target: primary, backend_login: ' '}",
        ] {
            let config = make_config(
                &format!(
                    "{{type: rust, callback: builtin.select_backend.passthrough, params: {params}}}"
                ),
                ONE_TARGET,
                "  {}",
            );
            assert!(matches!(
                config.validate(),
                Err(ConfigError::InvalidValue(_))
            ));
        }
    }

    #[test]
    fn python_reference_and_runtime_bounds() {
        let reference =
            "{type: python, path: ../app.py, callback: custom, params: {nested: [true, null, 4]}}";
        let mut config = make_config(
            reference,
            ONE_TARGET,
            "  max_connections_per_pool: 8\n  max_connections_total: 8",
        );
        assert!(config.validate().is_ok());
        let SelectorSettings::Python(hook) = &config.hooks.select_backend else {
            panic!("Python reference expected")
        };
        assert_eq!(hook.path, Path::new("../app.py"));
        assert_eq!(hook.callback, "custom");
        config.hooks.python.queue_max_bytes = config.hooks.python.max_payload_bytes - 1;
        assert!(matches!(
            config.validate(),
            Err(ConfigError::InvalidPythonLimit)
        ));
        for reference in [
            "{type: python, path: app.py, callback: custom, unexpected: true}",
            "{type: unknown, path: app.py, callback: custom}",
        ] {
            assert!(
                serde_saphyr::from_str::<HooksSettings>(&format!("select_backend: {reference}"))
                    .is_err()
            );
        }
    }

    #[test]
    fn defaults_infer_one_pair_and_reject_pool_overcommit() {
        let config = make_config(
            "builtin.select_backend.passthrough",
            ONE_TARGET,
            "  max_connections_per_pool: 8\n  max_connections_total: 8",
        );
        assert_eq!(config.default_backend().unwrap().target, "primary");
        assert!(config.validate().is_ok());

        let config = make_config(
            "builtin.select_backend.passthrough",
            ONE_TARGET,
            "  max_connections_per_pool: 8\n  max_connections_total: 7",
        );
        assert!(matches!(
            config.validate(),
            Err(ConfigError::InvalidPoolLimit)
        ));
    }

    #[test]
    fn passthrough_requires_unambiguous_or_explicit_default() {
        let two_targets = format!(
            "{ONE_TARGET}\n  reporting:\n    engine: postgresql\n    host: localhost\n    port: 5432\n    database: reports\n    tls_mode: disable\n    logins:\n      reader:\n        username: reader\n        password_env: REPORTS_PASSWORD"
        );
        let config = make_config(
            "builtin.select_backend.passthrough",
            &two_targets,
            "  max_connections_per_pool: 8\n  max_connections_total: 16",
        );
        assert!(matches!(
            config.validate(),
            Err(ConfigError::MissingDefaultBackend)
        ));

        let config = make_config(
            "{type: rust, callback: builtin.select_backend.passthrough, params: {target: reporting, backend_login: reader}}",
            &two_targets,
            "  max_connections_per_pool: 8\n  max_connections_total: 16",
        );
        assert!(config.validate().is_ok());
        assert_eq!(config.default_backend().unwrap().target, "reporting");

        let config = make_config(
            "custom.selector",
            &two_targets,
            "  max_connections_per_pool: 8\n  max_connections_total: 16",
        );
        assert!(config.validate().is_ok());
    }

    #[test]
    fn rejects_invalid_default_and_non_postgresql_target() {
        let config = make_config(
            "{type: rust, callback: builtin.select_backend.passthrough, params: {target: missing, backend_login: reader}}",
            ONE_TARGET,
            "  max_connections_per_pool: 8\n  max_connections_total: 8",
        );
        assert!(matches!(
            config.validate(),
            Err(ConfigError::InvalidDefaultBackend)
        ));

        let config = make_config(
            "custom.selector",
            &ONE_TARGET.replace("engine: postgresql", "engine: mysql"),
            "  max_connections_per_pool: 8\n  max_connections_total: 8",
        );
        assert!(matches!(
            config.validate(),
            Err(ConfigError::UnsupportedTarget)
        ));
    }
}

fn validate_value(name: &'static str, value: &str) -> Result<(), ConfigError> {
    if value.trim().is_empty() || value.contains('\0') {
        return Err(ConfigError::InvalidValue(name));
    }
    Ok(())
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
    #[error("at least one PostgreSQL target with one login is required")]
    MissingRoutes,
    #[error("connection() requires exactly one configured target with one login")]
    UnsupportedTopology,
    #[error(
        "backend pool limits must be positive and max_connections_total must cover every configured target/login pool; adjust either setting"
    )]
    InvalidPoolLimit,
    #[error("passthrough params must contain exactly target and backend_login strings")]
    InvalidSelectorParams,
    #[error("passthrough params must identify a configured target and backend login")]
    InvalidDefaultBackend,
    #[error("provide target and backend_login in passthrough params when multiple pairs exist")]
    MissingDefaultBackend,
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
