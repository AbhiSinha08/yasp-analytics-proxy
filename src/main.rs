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
    cache::RedisCache,
    config::{BackendConfig, FileConfig, GatewayConfig, SelectorSettings},
    gateway,
    hooks::{
        BackendSelection, CacheEligibility, ContextHook, RoutedSelector, bind_cache_eligibility,
        bind_select_backend, builtin,
    },
    logger,
};

struct HostConfig {
    gateway: GatewayConfig,
    backends: BTreeMap<BackendSelection, BackendConfig>,
    file: FileConfig,
    log_level: String,
    cache_url: Option<String>,
}

fn load_config(path: &Path) -> Result<HostConfig, Box<dyn std::error::Error>> {
    let file = FileConfig::read(path)?;
    // The host maps names to compiled functions once, before accepting clients.
    if file
        .hooks
        .context
        .as_ref()
        .and_then(SelectorSettings::rust_callback)
        .is_some()
    {
        return Err(io::Error::other("unregistered hooks.context name").into());
    }
    #[cfg(not(feature = "python"))]
    if matches!(file.hooks.context, Some(SelectorSettings::Python(_))) {
        return Err(
            io::Error::other("Python context hook requires the python Cargo feature").into(),
        );
    }
    match &file.hooks.select_backend {
        selector
            if selector
                .rust_callback()
                .is_some_and(|name| name != "builtin.select_backend.passthrough") =>
        {
            return Err(io::Error::other("unregistered hooks.select_backend name").into());
        }
        #[cfg(not(feature = "python"))]
        SelectorSettings::Python(_) => {
            return Err(
                io::Error::other("Python selector requires the python Cargo feature").into(),
            );
        }
        _ => {}
    }
    if let Some(hook) = &file.cache.eligibility
        && hook
            .rust_callback()
            .is_some_and(|name| name != "builtin.cache_eligibility.exclude_sql_containing")
    {
        return Err(io::Error::other("unregistered cache.eligibility name").into());
    }
    #[cfg(not(feature = "python"))]
    if matches!(file.cache.eligibility, Some(SelectorSettings::Python(_))) {
        return Err(
            io::Error::other("Python cache eligibility requires the python Cargo feature").into(),
        );
    }
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

async fn load_hooks(
    file: &FileConfig,
    _path: &Path,
) -> Result<
    (
        RoutedSelector,
        Option<CacheEligibility>,
        Option<ContextHook>,
    ),
    Box<dyn std::error::Error>,
> {
    #[cfg(feature = "python")]
    let config_path = _path.canonicalize()?;
    #[cfg(feature = "python")]
    let base = config_path.parent().expect("config file has a parent");
    #[cfg(feature = "python")]
    let runtime = if matches!(file.hooks.select_backend, SelectorSettings::Python(_))
        || matches!(file.cache.eligibility, Some(SelectorSettings::Python(_)))
        || matches!(file.hooks.context, Some(SelectorSettings::Python(_)))
    {
        let mut settings = file.hooks.python.clone();
        for path in &mut settings.search_paths {
            *path = base.join(&*path);
        }
        Some(yasp::hooks::python::PythonRuntime::new(settings).await?)
    } else {
        None
    };
    let context = match &file.hooks.context {
        None => None,
        Some(SelectorSettings::Python(_hook)) => {
            #[cfg(feature = "python")]
            {
                Some(
                    runtime
                        .as_ref()
                        .expect("Python context hook has a runtime")
                        .register(
                            &base.join(&_hook.path),
                            &_hook.callback,
                            _hook.params.clone(),
                        )
                        .await?
                        .into(),
                )
            }
            #[cfg(not(feature = "python"))]
            {
                return Err(io::Error::other(
                    "Python context hook requires the python Cargo feature",
                )
                .into());
            }
        }
        Some(_) => return Err(io::Error::other("unregistered hooks.context name").into()),
    };
    let selector: Result<RoutedSelector, Box<dyn std::error::Error>> = match &file
        .hooks
        .select_backend
    {
        SelectorSettings::Rust(_) | SelectorSettings::RustReference(_) => Ok(bind_select_backend(
            file.default_backend()?,
            builtin::select_backend::passthrough,
        )
        .into()),
        SelectorSettings::Python(_hook) => {
            #[cfg(feature = "python")]
            {
                Ok(runtime
                    .as_ref()
                    .expect("Python selector has a runtime")
                    .register(
                        &base.join(&_hook.path),
                        &_hook.callback,
                        _hook.params.clone(),
                    )
                    .await?
                    .into())
            }
            #[cfg(not(feature = "python"))]
            Err(io::Error::other("Python selector requires the python Cargo feature").into())
        }
    };
    let selector = selector?;
    let eligibility = match &file.cache.eligibility {
        None => None,
        Some(SelectorSettings::Rust(_) | SelectorSettings::RustReference(_)) => {
            #[derive(serde::Deserialize, Default)]
            #[serde(default, deny_unknown_fields)]
            struct Params {
                excluded_sql_fragments: Vec<String>,
            }
            let params = match &file.cache.eligibility {
                Some(SelectorSettings::RustReference(hook)) => {
                    serde_json::from_value::<Params>(serde_json::Value::Object(hook.params.clone()))
                        .map_err(|_| io::Error::other("invalid cache eligibility params"))?
                }
                _ => Params::default(),
            };
            if params
                .excluded_sql_fragments
                .iter()
                .any(|fragment| fragment.trim().is_empty())
            {
                return Err(io::Error::other(
                    "excluded_sql_fragments must contain nonblank SQL fragments",
                )
                .into());
            }
            Some(
                bind_cache_eligibility(params.excluded_sql_fragments, |request, fragments| {
                    builtin::cache_eligibility::exclude_sql_containing(request, fragments)
                })
                .into(),
            )
        }
        Some(SelectorSettings::Python(_hook)) => {
            #[cfg(feature = "python")]
            {
                Some(
                    runtime
                        .as_ref()
                        .expect("Python eligibility has a runtime")
                        .register(
                            &base.join(&_hook.path),
                            &_hook.callback,
                            _hook.params.clone(),
                        )
                        .await?
                        .into(),
                )
            }
            #[cfg(not(feature = "python"))]
            {
                return Err(io::Error::other(
                    "Python cache eligibility requires the python Cargo feature",
                )
                .into());
            }
        }
    };
    Ok((selector, eligibility, context))
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
        let backends = Arc::new(PostgresBackends::new(host.backends, host.file.backend)?);
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
