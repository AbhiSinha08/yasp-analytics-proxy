//! Host configuration and environment-backed secret resolution.

use std::{
    collections::{BTreeMap, HashMap},
    env, io,
    path::Path,
};
use yasp::{
    config::{BackendConfig, FileConfig, GatewayConfig},
    hooks::BackendSelection,
};

pub(super) struct HostConfig {
    pub(super) gateway: GatewayConfig,
    pub(super) backends: BTreeMap<BackendSelection, BackendConfig>,
    pub(super) file: FileConfig,
    pub(super) log_level: String,
    pub(super) cache_url: Option<String>,
}

pub(super) fn load_config(path: &Path) -> Result<HostConfig, Box<dyn std::error::Error>> {
    let file = FileConfig::read(path)?;
    super::hooks::validate_names(&file)?;
    // dotenv errors can contain source text, so expose a fixed diagnostic.
    let mut values = HashMap::new();
    match dotenvy::from_path_iter(".env") {
        Ok(entries) => {
            for entry in entries {
                let (key, value) = entry.map_err(|_| io::Error::other("malformed .env"))?;
                values.entry(key).or_insert(value);
            }
        }
        Err(dotenvy::Error::Io(error)) if error.kind() == io::ErrorKind::NotFound => {}
        Err(_) => return Err(io::Error::other("cannot read .env").into()),
    }
    let log_level = match env::var("LOG_LEVEL") {
        Ok(level) => level,
        Err(env::VarError::NotPresent) => values
            .get("LOG_LEVEL")
            .cloned()
            .unwrap_or_else(|| "INFO".to_owned()),
        Err(env::VarError::NotUnicode(_)) => {
            return Err(io::Error::other("invalid LOG_LEVEL").into());
        }
    };
    let required = |name: &str, label: &str| -> Result<String, io::Error> {
        if name.is_empty() || name.contains(['\0', '=']) {
            return Err(io::Error::other("invalid password_env key"));
        }
        match env::var(name) {
            Ok(value) => Ok(value),
            Err(env::VarError::NotPresent) => values
                .get(name)
                .cloned()
                .ok_or_else(|| io::Error::other(format!("missing {label}"))),
            Err(_) => Err(io::Error::other(format!("invalid {label}"))),
        }
    };
    let gateway = GatewayConfig::new(
        required("YASP_GATEWAY_USERNAME", "YASP_GATEWAY_USERNAME")?,
        required("YASP_GATEWAY_PASSWORD", "YASP_GATEWAY_PASSWORD")?,
        required("YASP_GATEWAY_DATABASE", "YASP_GATEWAY_DATABASE")?,
    )?
    .with_settings(file.gateway.clone())?
    .with_schema_discovery(file.discovery_backend()?);
    let cache_url = file
        .cache
        .enabled
        .then(|| required(&file.cache.url_env, "cache URL environment variable"))
        .transpose()?;
    let mut backends = BTreeMap::new();
    for (target_name, target) in &file.targets {
        for (login_name, login) in &target.logins {
            let route = BackendSelection {
                target: target_name.clone(),
                backend_login: login_name.clone(),
            };
            let config = BackendConfig::new(
                target.host.clone(),
                target.port,
                login.username.clone(),
                required(
                    &login.password_env,
                    "target login password environment variable",
                )?,
                target.database.clone(),
            )?;
            backends.insert(route, config);
        }
    }
    Ok(HostConfig {
        gateway,
        backends,
        file,
        log_level,
        cache_url,
    })
}
