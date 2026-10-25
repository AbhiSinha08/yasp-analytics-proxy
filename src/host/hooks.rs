//! Startup registration of the host's compiled and external Python callbacks.

use std::{
    io,
    path::{Path, PathBuf},
};
use yasp::{
    config::{FileConfig, RustHookSettings, SelectorSettings},
    datasets::{DatasetLoader, bind_dataset_loader},
    hooks::{
        CacheEligibility, QueryContextHook, QueryModificationHook, ResultHook, RoutedSelector,
        bind_cache_eligibility, bind_modify_query, bind_query_context, bind_select_backend,
        builtin,
    },
};

const EXAMPLE_DATASET_LOADER: &str = "example.datasets.email_locations";
const JSON_FILE_DATASET_LOADER: &str = "builtin.dataset.load_json";
const EXAMPLE_QUERY_MODIFIER: &str = "example.modify_query.rename_table";

#[derive(Default, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
struct MetabaseParams {
    native_dialect: Option<yasp::query::SqlDialect>,
}
#[derive(Default, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RedashParams {
    dialect: Option<yasp::query::SqlDialect>,
}

fn builtin_query_context(
    hook: &SelectorSettings,
) -> Result<QueryContextHook, Box<dyn std::error::Error>> {
    let name = hook
        .rust_callback()
        .ok_or_else(|| io::Error::other("invalid query context hook"))?;
    let params = match hook {
        SelectorSettings::RustReference(hook) => serde_json::Value::Object(hook.params.clone()),
        _ => serde_json::json!({}),
    };
    match name {
        "builtin.query_context.metabase" => {
            let params: MetabaseParams = serde_json::from_value(params)?;
            Ok(
                bind_query_context(params.native_dialect, |request, dialect| {
                    Box::pin(async move { builtin::query_context::metabase(request, *dialect) })
                })
                .into(),
            )
        }
        "builtin.query_context.redash" => {
            let params: RedashParams = serde_json::from_value(params)?;
            Ok(bind_query_context(params.dialect, |request, dialect| {
                Box::pin(async move { builtin::query_context::redash(request, *dialect) })
            })
            .into())
        }
        _ => Err(io::Error::other("unregistered hooks.query_context name").into()),
    }
}

fn example_query_modification(
    hook: &SelectorSettings,
) -> Result<QueryModificationHook, Box<dyn std::error::Error>> {
    use sqlparser::{
        ast::ObjectName, dialect::PostgreSqlDialect, parser::Parser, tokenizer::Token,
    };
    if hook.rust_callback() != Some(EXAMPLE_QUERY_MODIFIER) {
        return Err(io::Error::other("unregistered hooks.modify_query name").into());
    }
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Params {
        from: String,
        to: String,
    }
    let SelectorSettings::RustReference(hook) = hook else {
        return Err(io::Error::other("table rename requires from and to params").into());
    };
    let params: Params = serde_json::from_value(serde_json::Value::Object(hook.params.clone()))?;
    fn table_name(sql: &str) -> Result<ObjectName, Box<dyn std::error::Error>> {
        let mut parser = Parser::new(&PostgreSqlDialect {}).try_with_sql(sql)?;
        let name = parser.parse_object_name(false)?;
        if name.0.len() < 2
            || name.0.iter().any(|part| part.as_ident().is_none())
            || parser.peek_token().token != Token::EOF
        {
            return Err(io::Error::other("table rename requires qualified table names").into());
        }
        Ok(name)
    }
    Ok(bind_modify_query(
        super::query_modifier_example::TableRename {
            from: table_name(&params.from)?,
            to: table_name(&params.to)?,
        },
        super::query_modifier_example::rename_table,
    )
    .into())
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
    if let Some(hook) = &file.hooks.modify_query
        && hook.rust_callback().is_some()
    {
        example_query_modification(hook)?;
    }
    #[cfg(not(feature = "python"))]
    if matches!(file.hooks.modify_query, Some(SelectorSettings::Python(_))) {
        return Err(
            io::Error::other("Python modification hook requires the python Cargo feature").into(),
        );
    }
    if let Some(hook) = &file.hooks.query_context
        && hook.rust_callback().is_some()
    {
        builtin_query_context(hook)?;
    }
    #[cfg(not(feature = "python"))]
    if matches!(file.hooks.query_context, Some(SelectorSettings::Python(_))) {
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
        Option<QueryContextHook>,
        Option<QueryModificationHook>,
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
        || matches!(file.hooks.query_context, Some(SelectorSettings::Python(_)))
        || matches!(file.hooks.modify_query, Some(SelectorSettings::Python(_)))
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
    let context = match &file.hooks.query_context {
        None => None,
        Some(hook @ (SelectorSettings::Rust(_) | SelectorSettings::RustReference(_))) => {
            Some(builtin_query_context(hook)?)
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
    let modification = match &file.hooks.modify_query {
        None => None,
        Some(hook @ (SelectorSettings::Rust(_) | SelectorSettings::RustReference(_))) => {
            Some(example_query_modification(hook)?)
        }
        Some(SelectorSettings::Python(_hook)) => {
            #[cfg(feature = "python")]
            {
                Some(
                    register_python(
                        runtime
                            .as_ref()
                            .expect("Python modification hook has a runtime"),
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
                    "Python modification hook requires the python Cargo feature",
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
    Ok((
        selector,
        eligibility,
        context,
        modification,
        result_hook,
        loaders,
    ))
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
    async fn compiled_table_rename_loads_from_configuration_without_python() {
        let mut config: FileConfig =
            serde_saphyr::from_str(include_str!("../../config/example.yml")).unwrap();
        config.hooks.modify_query = Some(
            serde_json::from_value(serde_json::json!({
                "type": "rust", "callback": "example.modify_query.rename_table",
                "params": {"from": "public.orders", "to": "reporting.orders_v2"}
            }))
            .unwrap(),
        );
        validate_names(&config).unwrap();
        let (_, _, _, modifier, _, _) = load_hooks(
            &config,
            &Path::new(env!("CARGO_MANIFEST_DIR")).join("config/example.yml"),
        )
        .await
        .unwrap();
        let parsed =
            yasp::query::ParsedQuery::parse("SELECT o.id FROM public.orders AS o").unwrap();
        let context = serde_json::Map::new();
        let output = modifier
            .unwrap()
            .modify(
                yasp::hooks::QueryModificationRequest {
                    authenticated_user: "bi",
                    frontend_database: "analytics",
                    client_id: 1,
                    sql: parsed.sql(),
                    statement: parsed.statement(),
                    dialect: parsed.dialect(),
                    gateway_dialect: parsed.dialect(),
                    default_target_dialect: None,
                    query_context: &context,
                    datasets: None,
                },
                std::time::Duration::from_secs(1),
                4096,
            )
            .await
            .unwrap();
        assert_eq!(
            output.as_deref(),
            Some("SELECT o.id FROM reporting.orders_v2 AS o")
        );
        for params in [
            serde_json::json!({}),
            serde_json::json!({"from":"orders","to":"reporting.orders_v2"}),
            serde_json::json!({"from":"public.orders AS o","to":"reporting.orders_v2"}),
            serde_json::json!({"from":"public.orders","to":"reporting.orders_v2; SELECT 1"}),
            serde_json::json!({"from":"public.orders","to":"reporting.orders_v2","extra":true}),
        ] {
            config.hooks.modify_query = Some(
                serde_json::from_value(serde_json::json!({
                    "type":"rust", "callback":"example.modify_query.rename_table", "params":params,
                }))
                .unwrap(),
            );
            assert!(validate_names(&config).is_err());
        }
    }

    #[test]
    fn modification_requires_host_registration_and_python_feature() {
        let base: FileConfig =
            serde_saphyr::from_str(include_str!("../../config/example.yml")).unwrap();
        let mut value = serde_json::to_value(base).unwrap();
        value["hooks"]["modify_query"] =
            serde_json::json!({"type":"rust","callback":"app.rewrite"});
        let config: FileConfig = serde_json::from_value(value.clone()).unwrap();
        config.validate().unwrap();
        assert!(
            validate_names(&config)
                .unwrap_err()
                .to_string()
                .contains("unregistered hooks.modify_query")
        );
        #[cfg(not(feature = "python"))]
        {
            value["hooks"]["modify_query"] =
                serde_json::json!({"type":"python","path":"rewrite.py","callback":"modify"});
            let config: FileConfig = serde_json::from_value(value).unwrap();
            assert!(
                validate_names(&config)
                    .unwrap_err()
                    .to_string()
                    .contains("requires the python Cargo feature")
            );
        }
    }

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
                "builtin.query_context.metabase",
                "-- /* partner: \"metabase\", {\"dashboard_id\":null,\"chart_id\":null,\"optional_user_id\":1,\"optional_account_id\":null,\"filter_values\":{}} */ Metabase:: userID: 2 queryType: native queryHash: rs\nSELECT 1",
            ),
            (
                "builtin.query_context.metabase",
                "-- Metabase:: userID: 2 queryType: native queryHash: pg\nSELECT 1",
            ),
            (
                "builtin.query_context.redash",
                "/* user_id: 2, query_id: adhoc, Job ID: job, Query Hash: hash, Scheduled: False */ SELECT 1",
            ),
        ] {
            let mut value = base.clone();
            value["hooks"] =
                serde_json::json!({"query_context": {"type": "rust", "callback": callback}});
            let config: FileConfig = serde_json::from_value(value.clone()).unwrap();
            validate_names(&config).unwrap();
            let (_, _, context, _, _, _) = load_hooks(
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
                        yasp::hooks::QueryContextRequest {
                            authenticated_user: "bi",
                            frontend_database: "analytics",
                            client_id: 1,

                            datasets: None,

                            sql: (query).sql(),
                            gateway_dialect: yasp::query::SqlDialect::Postgresql,
                            default_target_dialect: Some(yasp::query::SqlDialect::Postgresql),
                        },
                        std::time::Duration::from_secs(1),
                        4096,
                    )
                    .await
                    .unwrap()
                    .values;
                if sql == "SELECT 1" {
                    assert!(extracted.is_empty());
                } else {
                    assert_eq!(extracted["user_id"], 2);
                    assert!(!extracted.contains_key("account"));
                }
            }
            value["hooks"]["query_context"]["params"] = serde_json::json!({"unused": true});
            assert!(validate_names(&serde_json::from_value(value.clone()).unwrap()).is_err());
            value["hooks"]["query_context"] =
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
