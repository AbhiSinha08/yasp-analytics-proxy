//! Local gateway host: environment resources, logging, and process lifecycle.

use std::{collections::HashMap, env, io};
use tokio::{net::TcpListener, runtime::Runtime};
use yasp::{config::GatewayConfig, gateway};

fn load_config() -> Result<GatewayConfig, Box<dyn std::error::Error>> {
    // dotenv errors can contain source text, so expose a fixed diagnostic.
    let entries =
        dotenvy::from_path_iter(".env").map_err(|_| io::Error::other("cannot read .env"))?;
    let mut values = HashMap::new();
    for entry in entries {
        let (key, value) = entry.map_err(|_| io::Error::other("malformed .env"))?;
        values.entry(key).or_insert(value);
    }
    let mut required = |name: &str| -> Result<String, io::Error> {
        match env::var(name) {
            Ok(value) => Ok(value),
            Err(env::VarError::NotPresent) => values
                .remove(name)
                .ok_or_else(|| io::Error::other(format!("missing {name}"))),
            Err(_) => Err(io::Error::other(format!("invalid {name}"))),
        }
    };
    Ok(GatewayConfig::new(
        required("YASP_GATEWAY_USERNAME")?,
        required("YASP_GATEWAY_PASSWORD")?,
        required("YASP_GATEWAY_DATABASE")?,
    )?)
}

fn main() {
    if let Err(error) = run() {
        eprintln!("YASP startup failed: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let config = load_config()?;
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    Runtime::new()?.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:6432").await?;
        tracing::info!(address = %listener.local_addr()?, "YASP gateway listening");
        gateway::serve(listener, config, async {
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
