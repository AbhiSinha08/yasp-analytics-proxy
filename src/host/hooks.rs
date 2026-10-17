//! Startup registration of the host's compiled and external Python callbacks.

use std::{io, path::Path};
use yasp::{
    config::{FileConfig, SelectorSettings},
    datasets::DatasetLoader,
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
    for dataset in file.application_contexts.datasets.values() {
        match &dataset.loader {
            SelectorSettings::Rust(name) if name != "example.datasets.email_locations" => {
                return Err(io::Error::other("unregistered application context loader").into());
            }
            SelectorSettings::RustReference(hook)
                if hook.callback != "example.datasets.email_locations" =>
            {
                return Err(io::Error::other("unregistered application context loader").into());
            }
            SelectorSettings::RustReference(hook) if !hook.params.is_empty() => {
                return Err(io::Error::other("example dataset loader accepts no params").into());
            }
            _ => {}
        }
    }
    #[cfg(not(feature = "python"))]
    if file
        .application_contexts
        .datasets
        .values()
        .any(|dataset| matches!(dataset.loader, SelectorSettings::Python(_)))
    {
        return Err(
            io::Error::other("Python dataset loader requires the python Cargo feature").into(),
        );
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
        std::collections::BTreeMap<String, DatasetLoader>,
    ),
    Box<dyn std::error::Error>,
> {
    #[cfg(feature = "python")]
    let config_path = _path.canonicalize()?;
    #[cfg(feature = "python")]
    let base = config_path.parent().expect("config file has a parent");
    #[cfg(feature = "python")]
    let has_python_dataset_loader = file
        .application_contexts
        .datasets
        .values()
        .any(|dataset| matches!(dataset.loader, SelectorSettings::Python(_)));
    #[cfg(feature = "python")]
    let runtime = if matches!(file.hooks.select_backend, SelectorSettings::Python(_))
        || matches!(file.cache.eligibility, Some(SelectorSettings::Python(_)))
        || matches!(file.hooks.context, Some(SelectorSettings::Python(_)))
        || file
            .application_contexts
            .datasets
            .values()
            .any(|dataset| matches!(dataset.loader, SelectorSettings::Python(_)))
    {
        let mut settings = file.hooks.python.clone();
        for path in &mut settings.search_paths {
            *path = base.join(&*path);
        }
        Some(if has_python_dataset_loader {
            yasp::hooks::python::PythonRuntime::new_with_dataset_limit(
                settings,
                file.application_contexts.max_dataset_bytes,
            )
            .await?
        } else {
            yasp::hooks::python::PythonRuntime::new(settings).await?
        })
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
            file.default_decision()?,
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
    let mut loaders = std::collections::BTreeMap::new();
    for (name, dataset) in &file.application_contexts.datasets {
        let loader = match &dataset.loader {
            SelectorSettings::Rust(_) | SelectorSettings::RustReference(_) => {
                super::datasets_example::bind_email_locations()
            }
            SelectorSettings::Python(_hook) => {
                #[cfg(feature = "python")]
                {
                    runtime
                        .as_ref()
                        .expect("Python dataset loader has a runtime")
                        .register_loader(
                            &base.join(&_hook.path),
                            &_hook.callback,
                            _hook.params.clone(),
                            file.application_contexts.max_dataset_bytes,
                            std::time::Duration::from_millis(u64::from(
                                file.application_contexts.refresh_timeout_ms,
                            )),
                        )
                        .await?
                        .into()
                }
                #[cfg(not(feature = "python"))]
                {
                    return Err(io::Error::other(
                        "Python dataset loader requires the python Cargo feature",
                    )
                    .into());
                }
            }
        };
        loaders.insert(name.clone(), loader);
    }
    Ok((selector, eligibility, context, loaders))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dataset_loaders_must_be_registered_by_the_host() {
        let config: FileConfig = serde_saphyr::from_str(
            "version: 1\ntargets:\n  primary:\n    engine: postgresql\n    host: localhost\n    port: 5432\n    database: analytics\n    tls_mode: disable\n    logins:\n      reader: {username: reader, password_env: READER_PASSWORD}\napplication_contexts:\n  datasets:\n    locations:\n      loader: {type: rust, callback: unknown.loader}\n",
        )
        .unwrap();
        assert!(validate_names(&config).is_err());
    }

    #[cfg(not(feature = "python"))]
    #[test]
    fn python_dataset_loader_requires_the_python_feature() {
        let config: FileConfig = serde_saphyr::from_str(
            "version: 1\ntargets:\n  primary:\n    engine: postgresql\n    host: localhost\n    port: 5432\n    database: analytics\n    tls_mode: disable\n    logins:\n      reader: {username: reader, password_env: READER_PASSWORD}\napplication_contexts:\n  datasets:\n    locations:\n      loader: {type: python, path: loader.py, callback: load}\n",
        )
        .unwrap();
        assert!(validate_names(&config).is_err());
    }
}
