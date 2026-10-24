//! Startup registration of the host's compiled and external Python callbacks.

use std::{
    io,
    path::{Path, PathBuf},
};
use yasp::{
    config::{FileConfig, RustHookSettings, SelectorSettings},
    datasets::{DatasetLoader, bind_dataset_loader},
    hooks::{
        CacheEligibility, ContextError, ContextHook, ResultHook, RoutedSelector,
        bind_cache_eligibility, bind_context, bind_select_backend, builtin,
    },
};

const EXAMPLE_DATASET_LOADER: &str = "example.datasets.email_locations";
const JSON_FILE_DATASET_LOADER: &str = "builtin.dataset.load_json";

type ContextExtractor = fn(
    &str,
) -> Result<
    Option<serde_json::Map<String, serde_json::Value>>,
    builtin::extractors::ExtractionError,
>;

fn context_extractor(name: &str) -> Option<ContextExtractor> {
    match name {
        "builtin.extractors.metabase_rs" => Some(builtin::extractors::metabase_rs),
        "builtin.extractors.metabase_pg" => Some(builtin::extractors::metabase_pg),
        "builtin.extractors.redash" => Some(builtin::extractors::redash),
        _ => None,
    }
}

fn dataset_file(hook: &RustHookSettings) -> Result<&str, io::Error> {
    if hook.params.len() != 1 {
        return Err(io::Error::other(
            "builtin.dataset.load_json accepts only the file param",
        ));
    }
    hook.params
        .get("file")
        .and_then(serde_json::Value::as_str)
        .filter(|file| !file.trim().is_empty())
        .ok_or_else(|| io::Error::other("builtin.dataset.load_json requires a file param"))
}

pub(super) fn validate_names(file: &FileConfig) -> Result<(), Box<dyn std::error::Error>> {
    if file
        .hooks
        .input_options
        .as_ref()
        .is_some_and(|hook| hook.callback != "builtin.input_options.metabase")
    {
        return Err(io::Error::other("unregistered hooks.input_options name").into());
    }
    // The host maps names to compiled functions once, before accepting clients.
    if file
        .hooks
        .context
        .as_ref()
        .and_then(SelectorSettings::rust_callback)
        .is_some_and(|name| context_extractor(name).is_none())
    {
        return Err(io::Error::other("unregistered hooks.context name").into());
    }
    if let Some(SelectorSettings::RustReference(hook)) = &file.hooks.context
        && !hook.params.is_empty()
    {
        return Err(io::Error::other("builtin context extractors accept no params").into());
    }
    #[cfg(not(feature = "python"))]
    if matches!(file.hooks.context, Some(SelectorSettings::Python(_))) {
        return Err(
            io::Error::other("Python context hook requires the python Cargo feature").into(),
        );
    }
    if file
        .hooks
        .process_result
        .as_ref()
        .and_then(SelectorSettings::rust_callback)
        .is_some_and(|name| name != "builtin.process_result.uppercase")
    {
        return Err(io::Error::other("unregistered hooks.process_result name").into());
    }
    #[cfg(not(feature = "python"))]
    if matches!(file.hooks.process_result, Some(SelectorSettings::Python(_))) {
        return Err(
            io::Error::other("Python result hook requires the python Cargo feature").into(),
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
            SelectorSettings::Rust(name) if name != EXAMPLE_DATASET_LOADER => {
                return Err(io::Error::other("unregistered application context loader").into());
            }
            SelectorSettings::RustReference(hook) if hook.callback == EXAMPLE_DATASET_LOADER => {
                if !hook.params.is_empty() {
                    return Err(io::Error::other("example dataset loader accepts no params").into());
                }
            }
            SelectorSettings::RustReference(hook) if hook.callback == JSON_FILE_DATASET_LOADER => {
                dataset_file(hook)?;
            }
            SelectorSettings::RustReference(_) => {
                return Err(io::Error::other("unregistered application context loader").into());
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
        Option<ResultHook>,
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
        || matches!(file.hooks.process_result, Some(SelectorSettings::Python(_)))
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
        Some(hook @ (SelectorSettings::Rust(_) | SelectorSettings::RustReference(_))) => {
            let extractor = hook
                .rust_callback()
                .and_then(context_extractor)
                .ok_or_else(|| io::Error::other("unregistered hooks.context name"))?;
            Some(
                bind_context(extractor, |request, extractor| {
                    Box::pin(async move {
                        extractor(request.query.sql())
                            .map(Option::unwrap_or_default)
                            .map_err(|_| ContextError::Failed)
                    })
                })
                .into(),
            )
        }
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
    let result_hook = match &file.hooks.process_result {
        None => None,
        Some(SelectorSettings::RustReference(hook))
            if hook.callback == "builtin.process_result.uppercase" =>
        {
            #[derive(serde::Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Params {
                column: String,
            }
            let params: Params = serde_json::from_value(serde_json::Value::Object(
                hook.params.clone(),
            ))
            .map_err(|_| io::Error::other("uppercase requires only a column string param"))?;
            if params.column.is_empty() || params.column.contains('\0') {
                return Err(io::Error::other("uppercase requires a nonempty column name").into());
            }
            Some(
                yasp::hooks::bind_process_result(params.column, builtin::process_result::uppercase)
                    .into(),
            )
        }
        Some(SelectorSettings::Rust(_) | SelectorSettings::RustReference(_)) => {
            return Err(io::Error::other(
                "unregistered or missing params for hooks.process_result",
            )
            .into());
        }
        Some(SelectorSettings::Python(_hook)) => {
            #[cfg(feature = "python")]
            {
                Some(
                    register_python(
                        runtime.as_ref().expect("Python result hook has a runtime"),
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
                    "Python result hook requires the python Cargo feature",
                )
                .into());
            }
        }
    };
    let mut loaders = std::collections::BTreeMap::new();
    for (name, dataset) in &file.application_contexts.datasets {
        let loader = match &dataset.loader {
            SelectorSettings::Rust(name) if name == EXAMPLE_DATASET_LOADER => {
                super::datasets_example::bind_email_locations()
            }
            SelectorSettings::RustReference(hook) if hook.callback == EXAMPLE_DATASET_LOADER => {
                super::datasets_example::bind_email_locations()
            }
            SelectorSettings::RustReference(hook) if hook.callback == JSON_FILE_DATASET_LOADER => {
                let path = PathBuf::from(dataset_file(hook)?);
                bind_dataset_loader(
                    (path, file.application_contexts.max_dataset_bytes),
                    builtin::dataset::load_json,
                )
                .into()
            }
            SelectorSettings::Rust(_) | SelectorSettings::RustReference(_) => {
                return Err(io::Error::other("unregistered application context loader").into());
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
    Ok((selector, eligibility, context, result_hook, loaders))
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

    #[tokio::test]
    async fn builtin_context_extractors_run_without_python_or_datasets() {
        let base = serde_json::json!({
            "version": 1,
            "targets": {"primary": {
                "engine": "postgresql", "host": "localhost", "port": 5432,
                "database": "analytics", "tls": {"mode": "disable"},
                "logins": {"reader": {"username": "reader", "password_env": "READER_PASSWORD"}}
            }}
        });
        for (callback, sql) in [
            (
                "builtin.extractors.metabase_rs",
                "-- /* partner: \"metabase\", {\"dashboard_id\":null,\"chart_id\":null,\"optional_user_id\":1,\"optional_account_id\":null,\"filter_values\":{}} */ Metabase:: userID: 2 queryType: native queryHash: rs\nSELECT 1",
            ),
            (
                "builtin.extractors.metabase_pg",
                "-- Metabase:: userID: 2 queryType: native queryHash: pg\nSELECT 1",
            ),
            (
                "builtin.extractors.redash",
                "/* user_id: 2, query_id: adhoc, Job ID: job, Query Hash: hash, Scheduled: False */ SELECT 1",
            ),
        ] {
            let mut value = base.clone();
            value["hooks"] = serde_json::json!({"context": {"type": "rust", "callback": callback}});
            let config: FileConfig = serde_json::from_value(value.clone()).unwrap();
            validate_names(&config).unwrap();
            let (_, _, context, _, _) = load_hooks(
                &config,
                &Path::new(env!("CARGO_MANIFEST_DIR")).join("config/example.yml"),
            )
            .await
            .unwrap();
            let context = context.unwrap();
            for sql in [sql, "SELECT 1"] {
                let query = yasp::query::ParsedQuery::parse(sql).unwrap();
                let extracted = context
                    .derive(
                        yasp::hooks::ContextRequest {
                            data_contexts: &yasp::lineage::DataContexts::EMPTY,
                            authenticated_user: "bi",
                            frontend_database: "analytics",
                            client_id: 1,
                            query_id: query.query_id(),
                            query: &query,
                            datasets: None,
                        },
                        std::time::Duration::from_secs(1),
                        4096,
                    )
                    .await;
                if sql == "SELECT 1" {
                    assert!(extracted.is_empty());
                } else {
                    assert_eq!(extracted["user_id"], 2);
                    assert!(!extracted.contains_key("account"));
                }
            }
            value["hooks"]["context"]["params"] = serde_json::json!({"unused": true});
            assert!(validate_names(&serde_json::from_value(value.clone()).unwrap()).is_err());
            value["hooks"]["context"] =
                serde_json::json!({"type": "rust", "callback": "unknown.extractor"});
            assert!(validate_names(&serde_json::from_value(value).unwrap()).is_err());
        }
    }

    #[test]
    fn dataset_loaders_must_be_registered_by_the_host() {
        let config: FileConfig = serde_saphyr::from_str(
            "version: 1\ntargets:\n  primary:\n    engine: postgresql\n    host: localhost\n    port: 5432\n    database: analytics\n    tls:\n      mode: disable\n    logins:\n      reader: {username: reader, password_env: READER_PASSWORD}\napplication_contexts:\n  datasets:\n    locations:\n      loader: {type: rust, callback: unknown.loader}\n",
        )
        .unwrap();
        assert!(validate_names(&config).is_err());
    }

    #[cfg(not(feature = "python"))]
    #[test]
    fn python_dataset_loader_requires_the_python_feature() {
        let config: FileConfig = serde_saphyr::from_str(
            "version: 1\ntargets:\n  primary:\n    engine: postgresql\n    host: localhost\n    port: 5432\n    database: analytics\n    tls:\n      mode: disable\n    logins:\n      reader: {username: reader, password_env: READER_PASSWORD}\napplication_contexts:\n  datasets:\n    locations:\n      loader: {type: python, path: loader.py, callback: load}\n",
        )
        .unwrap();
        assert!(validate_names(&config).is_err());
    }
}
