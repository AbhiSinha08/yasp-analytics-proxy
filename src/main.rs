//! Local gateway host: environment resources, logging, and process lifecycle.

use std::{
    collections::{BTreeMap, HashMap},
    env, io,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::{net::TcpListener, runtime::Runtime};
use yasp::{
    backend::PostgresBackends,
    config::{BackendConfig, BackendSettings, FileConfig, GatewayConfig},
    gateway,
    hooks::{BackendSelection, BackendSelector, bind_select_backend, builtin},
    logger,
};

struct HostConfig {
    gateway: GatewayConfig,
    backends: BTreeMap<BackendSelection, BackendConfig>,
    pool_settings: BackendSettings,
    selector: BackendSelector,
    log_level: String,
}

fn load_config(path: &Path) -> Result<HostConfig, Box<dyn std::error::Error>> {
    let file = FileConfig::read(path)?;
    // The host maps names to compiled functions once, before accepting clients.
    if file.hooks.select_backend != "builtin.select_backend.passthrough" {
        return Err(io::Error::other("unregistered hooks.select_backend name").into());
    }
    let selector = bind_select_backend(
        file.default_backend()?,
        builtin::select_backend::passthrough,
    );
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
    .with_settings(file.gateway.clone())?;
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
        pool_settings: file.backend,
        selector,
        log_level,
    })
}

fn main() {
    if let Err(error) = run() {
        eprintln!("YASP startup failed: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = env::args_os().skip(1).collect();
    let path = match args.as_slice() {
        [] => PathBuf::from("config/local.yml"),
        [flag, path] if flag == "--config" => PathBuf::from(path),
        [flag] if flag == "--help" || flag == "-h" => {
            println!("Usage: yasp [--config PATH]\nDefault: config/local.yml");
            return Ok(());
        }
        _ => return Err(io::Error::other("usage: yasp [--config PATH]").into()),
    };
    let host = load_config(&path)?;
    logger::init_with_level(&host.log_level)?;
    Runtime::new()?.block_on(async {
        let backends = Arc::new(PostgresBackends::new(host.backends, host.pool_settings)?);
        let listener = TcpListener::bind(host.gateway.settings().listen).await?;
        tracing::info!(address = %listener.local_addr()?, "YASP gateway listening");
        gateway::serve_routed(listener, host.gateway, backends, host.selector, async {
            #[cfg(unix)]
            {
                let mut terminate =
                    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                        .expect("cannot register SIGTERM handler");
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {},
                    _ = terminate.recv() => {},
                }
            }
            #[cfg(not(unix))]
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
    })?;
    Ok(())
}
