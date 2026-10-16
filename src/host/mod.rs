//! Assemble the gateway and own the host process lifecycle.

mod config;
mod hooks;

use std::{env, io, path::PathBuf, sync::Arc};
use tokio::{net::TcpListener, runtime::Runtime};
use yasp::{backend::PostgresBackends, cache::RedisCache, gateway, logger};

use config::load_config;
use hooks::load_hooks;

pub(super) fn run() -> Result<(), Box<dyn std::error::Error>> {
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
        let (selector, eligibility, context) = load_hooks(&host.file, &path).await?;
        let gateway = if let Some(url) = &host.cache_url {
            host.gateway.with_cache(Arc::new(RedisCache::new(
                host.file.cache.clone(),
                url,
                eligibility,
            )?))
        } else {
            host.gateway
        };
        let gateway = if let Some(context) = context {
            gateway.with_context_hook(context)
        } else {
            gateway
        };
        let backends = Arc::new(PostgresBackends::with_bindings(
            host.backends,
            host.file.backend,
        )?);
        let listener = TcpListener::bind(gateway.settings().listen).await?;
        tracing::info!(address = %listener.local_addr()?, "YASP gateway listening");
        gateway::serve_routed(listener, gateway, backends, selector, async {
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
        .await?;
        Ok::<(), Box<dyn std::error::Error>>(())
    })?;
    Ok(())
}
