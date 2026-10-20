//! Assemble the gateway and own the host process lifecycle.

mod config;
mod hooks;

#[path = "../../hooks/examples/datasets.rs"]
mod datasets_example;

use std::{env, io, path::PathBuf, sync::Arc};
use tokio::{net::TcpListener, runtime::Runtime};
use yasp::{backend::PostgresBackends, cache::RedisCache, datasets::Datasets, gateway, logger};

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
        let (selector, eligibility, context, loaders) = load_hooks(&host.file, &path).await?;
        let datasets = match &host.application_contexts_url {
            Some(url) => {
                Some(Datasets::start(host.file.application_contexts.clone(), url, loaders).await?)
            }
            None => None,
        };
        let result = async {
            let gateway = if let Some(url) = &host.cache_url {
                host.gateway.with_cache(Arc::new(RedisCache::new(
                    host.file.cache.clone(),
                    url,
                    eligibility,
                )?))
            } else {
                host.gateway
            };
            let gateway = if let Some(model) = host.decision_model {
                gateway.with_decision_model(model)
            } else {
                gateway
            };
            let gateway = if let Some(context) = context {
                gateway.with_context_hook(context)
            } else {
                gateway
            };
            let gateway = match &datasets {
                Some(datasets) => gateway.with_datasets(datasets.reader()),
                None => gateway,
            };
            let discovery = host.file.discovery_backend()?;
            let discovery_compute = host.file.discovery_compute()?;
            let budget =
                yasp::backend::ConnectionBudget::new(host.file.backend.max_connections_total)?;
            #[cfg(not(feature = "snowflake"))]
            let backends = Arc::new(PostgresBackends::with_bindings_and_budget(
                host.backends,
                host.file.backend,
                budget,
            )?);
            #[cfg(feature = "snowflake")]
            let backends = {
                let mut backends = if host.backends.is_empty() {
                    yasp::backend::Backends::with_budget(budget)
                } else {
                    yasp::backend::Backends::new(Arc::new(
                        PostgresBackends::with_bindings_and_budget(
                            host.backends,
                            host.file.backend,
                            budget,
                        )?,
                    ))
                };
                for (selection, settings) in host.snowflake {
                    backends = backends.with_snowflake(
                        selection,
                        Arc::new(yasp::backend::snowflake::SnowflakeBackend::new(settings)?),
                    )?;
                }
                Arc::new(backends)
            };
            let settings = host
                .file
                .schema_discovery
                .catalog
                .as_ref()
                .ok_or(yasp::config::ConfigError::InvalidCatalog)?
                .runtime()?;
            #[cfg(feature = "snowflake")]
            let catalog = backends
                .catalog(&discovery, discovery_compute.as_deref(), settings)
                .await?;
            #[cfg(not(feature = "snowflake"))]
            let catalog = {
                let source = backends
                    .get_discovery(&discovery, discovery_compute.as_deref())
                    .ok_or_else(|| io::Error::other("catalog discovery binding is unavailable"))?;
                yasp::catalog::Catalog::postgres(Arc::new(source.clone()), settings).await?
            };
            let gateway = gateway.with_catalog(catalog.clone());
            let serving = async {
                let listener = TcpListener::bind(gateway.settings().listen).await?;
                tracing::info!(address = %listener.local_addr()?, "YASP gateway listening");
                let shutdown = async {
                    #[cfg(unix)]
                    {
                        let mut terminate = tokio::signal::unix::signal(
                            tokio::signal::unix::SignalKind::terminate(),
                        )
                        .expect("cannot register SIGTERM handler");
                        tokio::select! {
                            _ = tokio::signal::ctrl_c() => {},
                            _ = terminate.recv() => {},
                        }
                    }
                    #[cfg(not(unix))]
                    let _ = tokio::signal::ctrl_c().await;
                };
                #[cfg(feature = "snowflake")]
                {
                    gateway::serve_backends(listener, gateway, backends, selector, shutdown).await
                }
                #[cfg(not(feature = "snowflake"))]
                {
                    gateway::serve_routed(listener, gateway, backends, selector, shutdown).await
                }
            }
            .await;
            catalog.stop_refreshing().await;
            serving?;
            Ok::<(), Box<dyn std::error::Error>>(())
        }
        .await;
        if let Some(datasets) = datasets {
            datasets.shutdown().await;
        }
        result?;
        Ok::<(), Box<dyn std::error::Error>>(())
    })?;
    Ok(())
}
