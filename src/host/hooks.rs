//! Startup registration of the host's compiled and external Python callbacks.

use std::{io, path::Path};
use yasp::{
    config::{FileConfig, SelectorSettings},
    hooks::{
        CacheEligibility, ContextHook, RoutedSelector, bind_cache_eligibility, bind_select_backend,
        builtin,
    },
};

pub(super) fn validate_names(file: &FileConfig) -> Result<(), Box<dyn std::error::Error>> {
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
    Ok(())
}

pub(super) async fn load_hooks(
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
                    register_python(
                        runtime.as_ref().expect("Python context hook has a runtime"),
                        base,
                        _hook,
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
                Ok(register_python(
                    runtime.as_ref().expect("Python selector has a runtime"),
                    base,
                    _hook,
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
                    register_python(
                        runtime.as_ref().expect("Python eligibility has a runtime"),
                        base,
                        _hook,
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

#[cfg(feature = "python")]
async fn register_python(
    runtime: &yasp::hooks::python::PythonRuntime,
    base: &Path,
    hook: &yasp::config::PythonHookSettings,
) -> Result<yasp::hooks::python::PythonCallback, yasp::hooks::python::PythonHookError> {
    runtime
        .register(&base.join(&hook.path), &hook.callback, hook.params.clone())
        .await
}
