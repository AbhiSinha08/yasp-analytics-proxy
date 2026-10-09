//! Local gateway host: environment resources, logging, and process lifecycle.

use std::{
    collections::HashMap,
    env, io,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::{net::TcpListener, runtime::Runtime};
use yasp::{
    backend::PostgresBackend,
    config::{BackendConfig, FileConfig, GatewayConfig},
    gateway,
};

fn load_config(path: &Path) -> Result<(GatewayConfig, BackendConfig), Box<dyn std::error::Error>> {
    let file = FileConfig::read(path)?;
    let (target, login) = file.connection()?;
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
    let backend = BackendConfig::new(
        target.host.clone(),
        target.port,
        login.username.clone(),
        required(
            &login.password_env,
            "target login password environment variable",
        )?,
        target.database.clone(),
    )?;
    Ok((gateway, backend))
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
    let (config, backend_config) = load_config(&path)?;
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    Runtime::new()?.block_on(async {
        let backend = Arc::new(PostgresBackend::new(backend_config)?);
        let listener = TcpListener::bind(config.settings().listen).await?;
        tracing::info!(address = %listener.local_addr()?, "YASP gateway listening");
        gateway::serve(listener, config, backend, async {
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
