//! YAML configuration loading, target topology, and route validation.

use super::{
    ApplicationContextsSettings, BackendSettings, CacheSettings, ConfigError, GatewaySettings,
    HooksSettings, SelectorSettings, validate_value,
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
    pub application_contexts: ApplicationContextsSettings,
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
    pub compute: Option<String>,
    pub catalog: Option<super::SchemaCatalogSettings>,
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
    #[serde(default)]
    pub role: Option<String>,
    #[serde(default)]
    pub allowed_computes: Option<Vec<String>>,
    #[serde(default)]
    pub overrides: BTreeMap<String, CredentialOverride>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CredentialOverride {
    pub username: String,
    pub password_env: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TargetSettings {
    pub engine: String,
    #[serde(default)]
    pub host: String,
    #[serde(default)]
    pub port: u16,
    pub database: String,
    #[serde(default)]
    pub tls: super::TargetTls,
    pub logins: Logins,
    pub compute: Option<ComputeSettings>,
    pub snowflake: Option<SnowflakeSettings>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SnowflakeSettings {
    pub driver_library: std::path::PathBuf,
    pub driver_sha256_env: String,
    pub account: String,
    pub schema: String,
    pub warehouse: String,
    pub emulator: Option<std::net::SocketAddr>,
    #[serde(default = "snowflake_batch_bytes")]
    pub max_batch_bytes: usize,
}
fn snowflake_batch_bytes() -> usize {
    8 * 1024 * 1024
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ComputeSettings {
    pub default_type: Option<String>,
    pub types: BTreeMap<String, ComputeTypeSettings>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ComputeTypeSettings {
    pub max_in_flight: usize,
    pub max_connections: usize,
    pub instances: Vec<ComputeInstanceSettings>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ComputeInstanceSettings {
    pub id: String,
    pub host: String,
    pub port: u16,
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

    /// Resolve host-provided TLS file references against the configuration directory.
    pub fn resolve_tls_paths(&mut self, base: &Path) {
        for path in [
            &mut self.gateway.tls.cert_file,
            &mut self.gateway.tls.key_file,
        ]
        .into_iter()
        .flatten()
        {
            if path.is_relative() {
                *path = base.join(&*path);
            }
        }
        for target in self.targets.values_mut() {
            target.tls.client.resolve(base);
            if let Some(snowflake) = &mut target.snowflake
                && snowflake.driver_library.is_relative()
            {
                snowflake.driver_library = base.join(&snowflake.driver_library);
            }
        }
        self.cache.tls.resolve(base);
        self.application_contexts.tls.resolve(base);
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.version != 1 {
            return Err(ConfigError::UnsupportedVersion);
        }
        self.gateway.validate()?;
        self.hooks.python.validate()?;
        self.cache.validate()?;
        self.application_contexts.validate()?;
        if let Some(catalog) = &self.schema_discovery.catalog {
            catalog.runtime()?;
        }
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
            || self.backend.acquire_timeout_ms == 0
            || self.backend.acquire_timeout_ms > i32::MAX as u64
        {
            return Err(ConfigError::InvalidPoolLimit);
        }
        for (target_name, target) in &self.targets {
            validate_value("target name", target_name)?;
            for (name, value) in [
                ("target.engine", &target.engine),
                ("target.database", &target.database),
            ] {
                validate_value(name, value)?;
            }
            match target.engine.as_str() {
                "postgresql" if target.snowflake.is_none() => {}
                "snowflake" => {
                    if !cfg!(feature = "snowflake") {
                        return Err(ConfigError::SnowflakeDisabled);
                    }
                    let sf = target
                        .snowflake
                        .as_ref()
                        .ok_or(ConfigError::InvalidSnowflake)?;
                    if !target.host.is_empty()
                        || target.port != 0
                        || target.compute.is_some()
                        || target.tls.mode != "disable"
                        || sf.max_batch_bytes == 0
                        || sf.max_batch_bytes > 64 * 1024 * 1024
                        || sf
                            .emulator
                            .is_some_and(|a| !a.ip().is_loopback() || a.port() == 0)
                    {
                        return Err(ConfigError::InvalidSnowflake);
                    }
                    for value in [
                        &sf.account,
                        &sf.schema,
                        &sf.warehouse,
                        &sf.driver_sha256_env,
                    ] {
                        validate_value("Snowflake setting", value)?;
                    }
                    validate_value(
                        "Snowflake driver library",
                        &sf.driver_library.to_string_lossy(),
                    )?;
                }
                _ => return Err(ConfigError::UnsupportedTarget),
            }
            target.tls.validate()?;
            match (&target.compute, target.host.is_empty(), target.port) {
                (None, true, 0) if target.engine == "snowflake" => {}
                (None, false, port) if port != 0 => validate_endpoint(&target.host, port)?,
                (Some(_), true, 0) => {}
                _ => return Err(ConfigError::InvalidComputeTopology),
            }
            if target.logins.is_empty() {
                return Err(ConfigError::MissingRoutes);
            }
            let mut instance_ids = BTreeMap::new();
            if let Some(compute) = &target.compute {
                if compute.types.is_empty() {
                    return Err(ConfigError::InvalidComputeTopology);
                }
                for (type_name, settings) in &compute.types {
                    validate_value("compute type name", type_name)?;
                    if settings.max_in_flight == 0
                        || settings.max_connections == 0
                        || settings.instances.is_empty()
                    {
                        return Err(ConfigError::InvalidComputeTopology);
                    }
                    for instance in &settings.instances {
                        validate_value("compute instance id", &instance.id)?;
                        validate_endpoint(&instance.host, instance.port)?;
                        if instance_ids
                            .insert(instance.id.as_str(), type_name.as_str())
                            .is_some()
                        {
                            return Err(ConfigError::InvalidComputeTopology);
                        }
                    }
                }
                if let Some(default_type) = &compute.default_type {
                    validate_value("compute.default_type", default_type)?;
                    if !compute.types.contains_key(default_type) {
                        return Err(ConfigError::InvalidComputeTopology);
                    }
                }
            }
            for (login_name, login) in &target.logins {
                if target.engine == "snowflake" {
                    validate_value(
                        "Snowflake role",
                        login.role.as_deref().ok_or(ConfigError::InvalidSnowflake)?,
                    )?;
                } else if login.role.is_some() {
                    return Err(ConfigError::InvalidSnowflake);
                }
                validate_value("backend login name", login_name)?;
                validate_value("backend login username", &login.username)?;
                validate_value("backend login password_env", &login.password_env)?;
                if login.password_env.contains(['=', '\0']) {
                    return Err(ConfigError::InvalidValue("backend login password_env"));
                }
                if let Some(allowed) = &login.allowed_computes
                    && (allowed.is_empty()
                        || allowed
                            .iter()
                            .any(|id| !instance_ids.contains_key(id.as_str())))
                {
                    return Err(ConfigError::InvalidComputeTopology);
                }
                for (id, credentials) in &login.overrides {
                    if !instance_ids.contains_key(id.as_str())
                        || login
                            .allowed_computes
                            .as_ref()
                            .is_some_and(|allowed| !allowed.contains(id))
                    {
                        return Err(ConfigError::InvalidComputeTopology);
                    }
                    validate_value("compute override username", &credentials.username)?;
                    validate_value("compute override password_env", &credentials.password_env)?;
                    if credentials.password_env.contains(['=', '\0']) {
                        return Err(ConfigError::InvalidValue("compute override password_env"));
                    }
                }
            }
            if let Some(compute) = &target.compute
                && compute.types.values().any(|settings| {
                    !settings.instances.iter().any(|instance| {
                        target.logins.values().any(|login| {
                            login
                                .allowed_computes
                                .as_ref()
                                .is_none_or(|ids| ids.contains(&instance.id))
                        })
                    })
                })
            {
                return Err(ConfigError::InvalidComputeTopology);
            }
        }
        if self.targets.is_empty() {
            return Err(ConfigError::MissingRoutes);
        }
        if self.hooks.select_backend.rust_callback() == Some("builtin.select_backend.passthrough") {
            self.default_decision()?;
        }
        let discovery = self.discovery_backend()?;
        if self.targets[&discovery.target].engine == "snowflake"
            && self.schema_discovery.catalog.is_none()
        {
            return Err(ConfigError::InvalidCatalog);
        }
        self.discovery_compute()?;
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

    pub fn discovery_compute(&self) -> Result<Option<String>, ConfigError> {
        let selection = self.discovery_backend()?;
        let target = &self.targets[&selection.target];
        let Some(compute) = &target.compute else {
            return self
                .schema_discovery
                .compute
                .as_ref()
                .map_or(Ok(None), |_| Err(ConfigError::InvalidDiscoveryBackend));
        };
        let login = &target.logins[&selection.backend_login];
        let eligible: Vec<_> = compute
            .types
            .values()
            .flat_map(|kind| kind.instances.iter())
            .filter(|instance| {
                login
                    .allowed_computes
                    .as_ref()
                    .is_none_or(|ids| ids.contains(&instance.id))
            })
            .collect();
        if let Some(id) = &self.schema_discovery.compute {
            return eligible
                .iter()
                .any(|instance| instance.id == *id)
                .then(|| Some(id.clone()))
                .ok_or(ConfigError::InvalidDiscoveryBackend);
        }
        match eligible.as_slice() {
            [instance] => Ok(Some(instance.id.clone())),
            [] => Err(ConfigError::InvalidDiscoveryBackend),
            _ => Err(ConfigError::MissingDiscoveryCompute),
        }
    }

    pub fn login_allows(&self, target: &str, login: &str, instance: &str) -> bool {
        self.targets
            .get(target)
            .and_then(|target| target.logins.get(login))
            .is_some_and(|login| {
                login
                    .allowed_computes
                    .as_ref()
                    .is_none_or(|allowed| allowed.iter().any(|id| id == instance))
            })
    }

    /// Resolve the built-in selector to a concrete target and login pair.
    pub fn default_backend(&self) -> Result<crate::hooks::BackendSelection, ConfigError> {
        let decision = self.default_decision()?;
        let target = decision
            .target
            .or_else(|| {
                (self.targets.len() == 1).then(|| self.targets.keys().next().unwrap().clone())
            })
            .ok_or(ConfigError::MissingDefaultBackend)?;
        let settings = self
            .targets
            .get(&target)
            .ok_or(ConfigError::InvalidDefaultBackend)?;
        let backend_login = decision
            .backend_login
            .or_else(|| {
                (settings.logins.len() == 1).then(|| settings.logins.keys().next().unwrap().clone())
            })
            .ok_or(ConfigError::MissingDefaultBackend)?;
        Ok(crate::hooks::BackendSelection {
            target,
            backend_login,
        })
    }

    /// Return configured passthrough fields as a partial decision. The router
    /// resolves omitted fields against this target's eligible bindings.
    pub fn default_decision(&self) -> Result<crate::hooks::BackendDecision, ConfigError> {
        let decision = match &self.hooks.select_backend {
            SelectorSettings::RustReference(hook)
                if hook.callback == "builtin.select_backend.passthrough" =>
            {
                serde_json::from_value(serde_json::Value::Object(hook.params.clone()))
                    .map_err(|_| ConfigError::InvalidSelectorParams)?
            }
            _ => {
                let mut pairs = self.targets.iter().flat_map(|(target, settings)| {
                    settings
                        .logins
                        .keys()
                        .map(move |login| crate::hooks::BackendSelection {
                            target: target.clone(),
                            backend_login: login.clone(),
                        })
                });
                let first = pairs.next().ok_or(ConfigError::MissingDefaultBackend)?;
                if pairs.next().is_some() {
                    return Err(ConfigError::MissingDefaultBackend);
                }
                crate::hooks::BackendDecision::from(first)
            }
        };
        for value in [
            &decision.target,
            &decision.backend_login,
            &decision.compute_type,
        ]
        .into_iter()
        .flatten()
        {
            validate_value("hooks.select_backend.params", value)?;
        }
        let target_name = match decision.target.as_deref() {
            Some(target) => target,
            None if self.targets.len() == 1 => self.targets.keys().next().unwrap(),
            None => return Err(ConfigError::MissingDefaultBackend),
        };
        let target = self
            .targets
            .get(target_name)
            .ok_or(ConfigError::InvalidDefaultBackend)?;
        let login_name = match decision.backend_login.as_deref() {
            Some(login) => login,
            None if target.logins.len() == 1 => target.logins.keys().next().unwrap(),
            None => return Err(ConfigError::MissingDefaultBackend),
        };
        let login = target
            .logins
            .get(login_name)
            .ok_or(ConfigError::InvalidDefaultBackend)?;
        if let Some(compute) = &target.compute {
            let type_name = match decision.compute_type.as_deref() {
                Some(type_name) => type_name,
                None => match compute.default_type.as_deref() {
                    Some(default_type) => default_type,
                    None if compute.types.len() == 1 => compute.types.keys().next().unwrap(),
                    None => return Err(ConfigError::MissingDefaultBackend),
                },
            };
            let type_settings = compute
                .types
                .get(type_name)
                .ok_or(ConfigError::InvalidDefaultBackend)?;
            let has_eligible_instance = type_settings.instances.iter().any(|instance| {
                login
                    .allowed_computes
                    .as_ref()
                    .is_none_or(|ids| ids.contains(&instance.id))
            });
            if !has_eligible_instance {
                return Err(ConfigError::InvalidDefaultBackend);
            }
        } else if decision.compute_type.is_some() {
            return Err(ConfigError::InvalidDefaultBackend);
        }
        Ok(decision)
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
        if target.engine != "postgresql" {
            return Err(ConfigError::UnsupportedTarget);
        }
        target.tls.validate()?;
        Ok((target, target.logins.values().next().unwrap()))
    }
}

fn validate_endpoint(host: &str, port: u16) -> Result<(), ConfigError> {
    validate_value("target.host", host)?;
    if port == 0 {
        return Err(ConfigError::InvalidPort);
    }
    Ok(())
}
