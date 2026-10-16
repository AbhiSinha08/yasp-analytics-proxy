//! YAML configuration loading, target topology, and route validation.

use super::{
    BackendSettings, CacheSettings, ConfigError, GatewaySettings, HooksSettings, SelectorSettings,
    validate_value,
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fs::File, io::Read, path::Path};

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
    #[serde(default)]
    pub cache: CacheSettings,
    #[serde(default)]
    pub schema_discovery: SchemaDiscoverySettings,
    pub targets: BTreeMap<String, TargetSettings>,
}

/// A source for metadata, independent of application query routing.
#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct SchemaDiscoverySettings {
    pub target: Option<String>,
    pub backend_login: Option<String>,
}

/// Named logins retain their declaration order for the discovery default.
pub struct Logins {
    entries: BTreeMap<String, LoginSettings>,
    first: Option<String>,
}

impl std::ops::Deref for Logins {
    type Target = BTreeMap<String, LoginSettings>;
    fn deref(&self) -> &Self::Target {
        &self.entries
    }
}

impl<'a> IntoIterator for &'a Logins {
    type Item = (&'a String, &'a LoginSettings);
    type IntoIter = std::collections::btree_map::Iter<'a, String, LoginSettings>;
    fn into_iter(self) -> Self::IntoIter {
        self.entries.iter()
    }
}

impl Serialize for Logins {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // Serialize the default first so a configuration round trip preserves it.
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(self.entries.len()))?;
        if let Some(first) = &self.first {
            map.serialize_entry(first, &self.entries[first])?;
        }
        for (name, login) in &self.entries {
            if Some(name) != self.first.as_ref() {
                map.serialize_entry(name, login)?;
            }
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for Logins {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct LoginMap;
        impl<'de> serde::de::Visitor<'de> for LoginMap {
            type Value = Logins;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a map of named logins")
            }
            fn visit_map<M: serde::de::MapAccess<'de>>(
                self,
                mut map: M,
            ) -> Result<Logins, M::Error> {
                let mut entries = BTreeMap::new();
                let mut first = None;
                while let Some((name, login)) = map.next_entry::<String, LoginSettings>()? {
                    if first.is_none() {
                        first = Some(name.clone());
                    }
                    if entries.insert(name, login).is_some() {
                        return Err(serde::de::Error::custom("duplicate login"));
                    }
                }
                Ok(Logins { entries, first })
            }
        }
        deserializer.deserialize_map(LoginMap)
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
    pub logins: Logins,
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
        self.cache.validate()?;
        if let Some(hook) = &self.hooks.context {
            match hook {
                SelectorSettings::Rust(name) => validate_value("hooks.context", name)?,
                SelectorSettings::RustReference(hook) => {
                    validate_value("hooks.context.callback", &hook.callback)?
                }
                SelectorSettings::Python(hook) => {
                    validate_value("context hook path", &hook.path.to_string_lossy())?;
                    validate_value("context callback", &hook.callback)?;
                }
            }
        }
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
        self.discovery_backend()?;
        Ok(())
    }

    pub fn discovery_backend(&self) -> Result<crate::hooks::BackendSelection, ConfigError> {
        let target = match &self.schema_discovery.target {
            Some(target) => target,
            None if self.targets.len() == 1 => self.targets.keys().next().unwrap(),
            None => return Err(ConfigError::MissingDiscoveryTarget),
        };
        let settings = self
            .targets
            .get(target)
            .ok_or(ConfigError::InvalidDiscoveryBackend)?;
        let login = self
            .schema_discovery
            .backend_login
            .as_ref()
            .or(settings.logins.first.as_ref())
            .ok_or(ConfigError::InvalidDiscoveryBackend)?;
        if !settings.logins.contains_key(login) {
            return Err(ConfigError::InvalidDiscoveryBackend);
        }
        Ok(crate::hooks::BackendSelection {
            target: target.clone(),
            backend_login: login.clone(),
        })
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
