//! Cross-tool remark extraction through the example context and role hooks.
#![cfg(feature = "python")]
mod support;
use std::{fs, time::Duration};

#[tokio::test]
async fn bi_user_remarks_reach_the_existing_context_and_role_mapping() {
    use serde_json::{Map, json};
    use yasp::{
        config::{ApplicationContextsSettings, DatasetSettings, PythonSettings, SelectorSettings},
        datasets::{DatasetLoader, Datasets, bind_dataset_loader},
        hooks::{
            ContextHook, ContextRequest, RoutedSelector, SelectionRequest, python::PythonRuntime,
        },
        query::ParsedQuery,
    };

    let redis = std::env::var("YASP_TEST_REDIS_URL").unwrap();
    let namespace = support::RedisNamespace::new(&redis, "yasp_bi_context");
    let examples = support::root().join("hooks/examples");
    let names = ["users", "email_locations"];
    let loaders = names
        .into_iter()
        .map(|name| {
            let data: Map<String, serde_json::Value> = serde_json::from_str(
                &fs::read_to_string(examples.join(format!("{name}.json"))).unwrap(),
            )
            .unwrap();
            let loader: DatasetLoader =
                bind_dataset_loader(data, |_, data| Box::pin(async move { Ok((*data).clone()) }))
                    .into();
            (name.into(), loader)
        })
        .collect();
    let datasets = Datasets::start(
        ApplicationContextsSettings {
            namespace: namespace.name.clone(),
            datasets: names
                .into_iter()
                .map(|name| {
                    (
                        name.into(),
                        DatasetSettings {
                            loader: SelectorSettings::Rust("test.examples".into()),
                            ..DatasetSettings::default()
                        },
                    )
                })
                .collect(),
            ..ApplicationContextsSettings::default()
        },
        &redis,
        loaders,
    )
    .await
    .unwrap();
    let reader = datasets.reader();
    let runtime = PythonRuntime::new(PythonSettings::default()).await.unwrap();
    let context = ContextHook::Python(
        runtime
            .register(
                &examples.join("context.py"),
                "derive_context",
                Map::from_iter([
                    ("workload".into(), json!("reporting")),
                    ("report_kind".into(), json!("dashboard")),
                ]),
            )
            .await
            .unwrap(),
    );
    let selector = RoutedSelector::Python(
        runtime
            .register(
                &examples.join("role_selector.py"),
                "select_backend",
                serde_json::from_value(json!({
                    "target_by_workload": {"reporting": "primary"},
                    "login_by_department": {"finance": {"primary": "restricted"}},
                    "type_by_report": {"dashboard": null}
                }))
                .unwrap(),
            )
            .await
            .unwrap(),
    );
    let cases = [
        (
            "-- /* partner: \"metabase\", {\"dashboard_id\":null,\"chart_id\":null,\"optional_user_id\":1,\"optional_account_id\":\"account-1\",\"filter_values\":{}} */ Metabase:: userID: 1 queryType: native queryHash: abc\nSELECT 1",
            Some("1"),
        ),
        (
            "-- Metabase:: userID: 1 queryType: MBQL queryHash: abc\nSELECT 1",
            Some("1"),
        ),
        (
            "/* user_id: 1, query_id: 19, Job ID: job-1, Query Hash: hash-1, Scheduled: False */ SELECT 1",
            Some("1"),
        ),
        (
            "/* user_id: 1, query_id: adhoc, Job ID: 8a4cee94-0a4e-44e7-b640-8bd69dbf0f70, Query Hash: 7613f9e534ec59f2d3aaea6d9b60242d, Scheduled: False */ SELECT 1",
            Some("1"),
        ),
        (
            "/* yasp-test user=alice workload=reporting report=dashboard */ SELECT 1",
            Some("alice"),
        ),
        (
            "-- Metabase:: userID: 999 queryType: native queryHash: abc\nSELECT 1",
            None,
        ),
        ("SELECT 1", None),
    ];
    for (sql, user_id) in cases {
        let query = ParsedQuery::parse(sql).unwrap();
        let query_id = query.query_id();
        let derived = context
            .derive(
                ContextRequest {
                    data_contexts: &yasp::lineage::DataContexts::EMPTY,
                    authenticated_user: "bi-service",
                    frontend_database: "analytics",
                    client_id: 1,
                    query_id,
                    query: &query,
                    datasets: Some(&reader),
                },
                Duration::from_secs(1),
                8192,
            )
            .await;
        if let Some(user_id) = user_id {
            assert_eq!(derived["user_id"], user_id);
            assert_eq!(derived["department"], "finance");
            assert!(
                derived["email_columns"]
                    .as_array()
                    .unwrap()
                    .contains(&json!("crm.contacts.email"))
            );
        } else {
            assert!(derived.is_empty());
        }
        let selected = selector
            .select(&SelectionRequest {
                data_contexts: &yasp::lineage::DataContexts::EMPTY,
                authenticated_user: "bi-service",
                client_id: 1,
                query_id,
                query: &query,
                context: &derived,
                datasets: Some(&reader),
                async_hook_timeout_ms: 1000,
            })
            .await
            .unwrap();
        assert_eq!(selected.target.as_deref(), Some("primary"));
        assert_eq!(
            selected.backend_login.as_deref(),
            Some(if user_id.is_some() {
                "restricted"
            } else {
                "analyst"
            })
        );
    }
    datasets.shutdown().await;
    runtime.shutdown();
}
