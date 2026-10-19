//! Host configuration and environment-backed secret resolution.

use std::{collections::HashMap, env, io, path::Path};
use yasp::{
    backend::PostgresBinding,
    config::{BackendConfig, FileConfig, GatewayConfig},
    hooks::BackendSelection,
};

pub(super) struct HostConfig {
    pub(super) gateway: GatewayConfig,
    pub(super) backends: Vec<PostgresBinding>,
    #[cfg(feature = "snowflake")]
    pub(super) snowflake: Vec<(BackendSelection, yasp::backend::snowflake::SnowflakeConfig)>,
    pub(super) file: FileConfig,
    pub(super) log_level: String,
    pub(super) cache_url: Option<String>,
    pub(super) application_contexts_url: Option<String>,
}

pub(super) fn load_config(path: &Path) -> Result<HostConfig, Box<dyn std::error::Error>> {
    let mut file = FileConfig::read(path)?;
    file.resolve_tls_paths(path.parent().unwrap_or_else(|| Path::new(".")));
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
    let gateway = if let Some(hook) = &file.hooks.input_options {
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Params {
            native_dialect: yasp::query::SqlDialect,
            #[serde(default)]
            unmarked_data_dialect: Option<yasp::query::SqlDialect>,
        }
        let params: Params =
            serde_json::from_value(serde_json::Value::Object(hook.params.clone()))?;
        gateway.with_input_options(yasp::hooks::builtin::input_options::metabase_with_default(
            params.native_dialect,
            params.unmarked_data_dialect,
        ))
    } else {
        gateway
    };
    let gateway = match file.discovery_compute()? {
        Some(compute) => gateway.with_schema_discovery_compute(compute),
        None => gateway,
    };
    let cache_url = file
        .cache
        .enabled
        .then(|| required(&file.cache.url_env, "cache URL environment variable"))
        .transpose()?;
    let application_contexts_url = (!file.application_contexts.datasets.is_empty())
        .then(|| {
            required(
                &file.application_contexts.url_env,
                "application contexts URL environment variable",
            )
        })
        .transpose()?;
    let mut backends = Vec::new();
    #[cfg(feature = "snowflake")]
    let mut snowflake = Vec::new();
    for (target_name, target) in &file.targets {
        #[cfg(feature = "snowflake")]
        if let Some(sf) = &target.snowflake {
            for (name, login) in &target.logins {
                snowflake.push((
                    BackendSelection {
                        target: target_name.clone(),
                        backend_login: name.clone(),
                    },
                    yasp::backend::snowflake::SnowflakeConfig {
                        driver_library: sf.driver_library.clone(),
                        driver_sha256: required(&sf.driver_sha256_env, "Snowflake driver digest")?,
                        account: sf.account.clone(),
                        username: login.username.clone(),
                        password: required(&login.password_env, "Snowflake login password")?,
                        database: target.database.clone(),
                        schema: sf.schema.clone(),
                        warehouse: sf.warehouse.clone(),
                        role: login.role.clone().expect("validated Snowflake role"),
                        emulator: sf.emulator,
                        max_connections: file
                            .backend
                            .max_connections_per_pool
                            .min(file.backend.max_connections_total),
                        acquire_timeout: std::time::Duration::from_millis(
                            file.backend.acquire_timeout_ms,
                        ),
                        max_batch_bytes: sf.max_batch_bytes,
                    },
                ));
            }
            continue;
        }
        let make_backend = |host, port, username, password| {
            if target.tls.mode == "enable" {
                BackendConfig::new_with_tls(
                    host,
                    port,
                    username,
                    password,
                    target.database.clone(),
                    target.tls.client.clone(),
                )
            } else {
                BackendConfig::new(host, port, username, password, target.database.clone())
            }
        };
        for (login_name, login) in &target.logins {
            let route = BackendSelection {
                target: target_name.clone(),
                backend_login: login_name.clone(),
            };
            if let Some(compute) = &target.compute {
                for (compute_type, settings) in &compute.types {
                    for instance in &settings.instances {
                        if !file.login_allows(target_name, login_name, &instance.id) {
                            continue;
                        }
                        let (username, password_env) = login
                            .overrides
                            .get(&instance.id)
                            .map(|override_| {
                                (override_.username.as_str(), override_.password_env.as_str())
                            })
                            .unwrap_or((&login.username, &login.password_env));
                        let config = make_backend(
                            instance.host.clone(),
                            instance.port,
                            username.to_owned(),
                            required(password_env, "target login password environment variable")?,
                        )?;
                        let default_compute_type = compute.default_type.as_deref()
                            == Some(compute_type.as_str())
                            || (compute.default_type.is_none() && compute.types.len() == 1);
                        backends.push(PostgresBinding {
                            selection: route.clone(),
                            compute_type: Some(compute_type.clone()),
                            compute: instance.id.clone(),
                            default_compute_type,
                            max_connections: settings.max_connections,
                            max_in_flight: settings.max_in_flight,
                            config,
                        });
                    }
                }
            } else {
                let config = make_backend(
                    target.host.clone(),
                    target.port,
                    login.username.clone(),
                    required(
                        &login.password_env,
                        "target login password environment variable",
                    )?,
                )?;
                backends.push(PostgresBinding {
                    selection: route,
                    compute_type: None,
                    compute: "default".to_owned(),
                    default_compute_type: true,
                    max_connections: file.backend.max_connections_total,
                    max_in_flight: file.backend.max_connections_total,
                    config,
                });
            }
        }
    }
    Ok(HostConfig {
        gateway,
        backends,
        #[cfg(feature = "snowflake")]
        snowflake,
        file,
        log_level,
        cache_url,
        application_contexts_url,
    })
}
