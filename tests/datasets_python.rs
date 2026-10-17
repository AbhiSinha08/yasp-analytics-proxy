//! Python dataset callbacks share a bounded invocation reader and an isolated Redis store.
#![cfg(feature = "python")]

mod support;

use serde_json::{Map, json};
use std::{
    collections::BTreeMap,
    fs,
    time::{Duration, Instant},
};
use support::{RedisNamespace, TempDirectory};
use tokio::time::{sleep, timeout};
use yasp::{
    config::{ApplicationContextsSettings, DatasetSettings, PythonSettings, SelectorSettings},
    datasets::{DatasetLoader, Datasets},
    hooks::{
        CacheEligibility, ContextError, ContextHook, ContextRequest, RoutedSelector,
        SelectionRequest, bind_context,
        python::{PythonHookError, PythonRuntime},
    },
    query::ParsedQuery,
};

async fn wait_file(path: &std::path::Path) {
    timeout(Duration::from_secs(3), async {
        while !path.exists() {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("Python callback reaches the synchronization point");
}

fn redis_url() -> String {
    std::env::var("YASP_TEST_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".into())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn python_dataset_loader_and_query_helpers_are_bounded_and_scoped() {
    let url = redis_url();
    let namespace = RedisNamespace::new(&url, "yasp_python_datasets");
    let scripts = TempDirectory::new("yasp_python_datasets");
    let cancelled = scripts.0.join("cancelled.txt");
    let loader_started = scripts.0.join("loader_started.txt");
    let python = scripts.0.join("callbacks.py");
    fs::write(
        &python,
        r#"
import json
import time
from pathlib import Path

retained = None

def load_json(request, params):
    return json.loads(Path(params["file"]).read_text(encoding="utf-8"))

def load(request, params):
    if request.get("slow"):
        Path(request["started"]).touch()
        time.sleep(0.35)
    data = {
        "alice": {"email": "alice@example.test", "roles": ["reader", "analyst"]},
        "enabled": True,
        "target": "warehouse",
        "backend_login": "reader",
        "loader": {"name": request["name"], "reason": request["reason"], "tag": params["tag"]},
    }
    if request["name"] == "users":
        data["part1"] = "x" * 700
        data["part2"] = "y" * 700
    return data

def hook(request, params):
    global retained
    mode = request.get("operation", params["mode"])
    datasets = request.get("datasets")
    if mode == "context":
        return {
            "email": datasets.get("users", "alice")["email"],
            "fields": datasets.get_many("users", ["enabled", "missing"]),
            "all": datasets.read("compact"),
        }
    if mode == "selector":
        return {
            "target": datasets.get("users", "target"),
            "backend_login": datasets.get("users", "backend_login"),
        }
    if mode == "eligibility":
        return datasets.get("users", "enabled")
    if mode == "retain":
        retained = datasets
        return {"retained": True}
    if mode == "probe":
        try:
            retained.get("users", "alice")
        except retained.DatasetError as error:
            return {"closed": str(error)}
        return {"closed": "no"}
    if mode == "cancel":
        retained = datasets
        time.sleep(0.25)
        try:
            retained.get("users", "alice")
        except retained.DatasetError as error:
            Path(request["marker"]).write_text(str(error))
            return {"closed": str(error)}
        Path(request["marker"]).write_text("open")
        return {"closed": "no"}
    if mode == "many":
        for _ in range(33):
            datasets.get("users", "enabled")
        return {"calls": 33}
    if mode == "too_many_keys":
        try:
            datasets.get_many("users", ["enabled"] * 1000000)
        except ValueError as error:
            return {"error": str(error)}
    if mode == "too_large":
        try:
            datasets.read("users")
        except datasets.DatasetError as error:
            return {"error": str(error)}
    if mode == "unknown":
        try:
            datasets.get("private_dataset_name", "x")
        except datasets.DatasetError as error:
            return {"error": str(error)}
    if mode == "malformed":
        try:
            datasets.get("malformed", "bad")
        except datasets.DatasetError as error:
            return {"error": str(error)}
    if mode == "plain":
        return request
    return {"ok": True}

def ordinary(request, params):
    return {"request": request, "params": params}

def queued(request, params):
    if request.get("pause"):
        time.sleep(0.1)
    return {"ok": True}
"#,
    )
    .unwrap();

    let runtime = PythonRuntime::new_with_dataset_limit(
        PythonSettings {
            search_paths: vec![scripts.0.clone()],
            queue_capacity: 4,
            queue_max_bytes: 4 * 1024 * 1024,
            max_payload_bytes: 4 * 1024 * 1024,
            execution_timeout_ms: 1000,
        },
        16 * 1024 * 1024,
    )
    .await
    .expect("runtime supports default-size datasets independently of query byte capacity");
    let examples = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("hooks/examples");
    for file in ["users.json", "email_locations.json"] {
        let file_path = examples.join(file);
        let example = runtime
            .register_loader(
                &python,
                "load_json",
                Map::from_iter([("file".into(), json!(file_path.to_string_lossy()))]),
                256,
                Duration::from_secs(2),
            )
            .await
            .unwrap();
        let value = example
            .call(json!({"name": "example", "reason": "startup"}))
            .await
            .unwrap();
        assert!(
            value.is_object(),
            "the supplied loader returns a plain JSON object"
        );
        if file == "users.json" {
            assert_eq!(value["alice"]["department"], "finance");
        } else {
            assert_eq!(value["columns"][0], "crm.contacts.email");
        }
    }
    let loader = runtime
        .register_loader(
            &python,
            "load",
            Map::from_iter([("tag".into(), json!("python-loader"))]),
            16 * 1024 * 1024,
            Duration::from_secs(30),
        )
        .await
        .unwrap();
    let datasets_config = ApplicationContextsSettings {
        namespace: namespace.name.clone(),
        max_lookup_bytes: 1024,
        datasets: ["users", "compact", "malformed"]
            .into_iter()
            .map(|name| {
                (
                    name.into(),
                    DatasetSettings {
                        refresh_interval_seconds: 0,
                        loader: SelectorSettings::Rust("test.loader".into()),
                    },
                )
            })
            .collect(),
        ..ApplicationContextsSettings::default()
    };
    let datasets = Datasets::start(
        datasets_config,
        &url,
        BTreeMap::from([
            ("users".into(), DatasetLoader::Python(loader.clone())),
            ("compact".into(), DatasetLoader::Python(loader.clone())),
            ("malformed".into(), DatasetLoader::Python(loader.clone())),
        ]),
    )
    .await
    .expect("Python loader completes startup refresh");
    let reader = datasets.reader();
    assert_eq!(
        reader.get("users", "loader").await.unwrap(),
        Some(json!({"name": "users", "reason": "startup", "tag": "python-loader"}))
    );

    let callback = |mode: &str| {
        let mode = mode.to_owned();
        let runtime = runtime.clone();
        let python = python.clone();
        async move {
            runtime
                .register(
                    &python,
                    "hook",
                    Map::from_iter([("mode".into(), json!(mode))]),
                )
                .await
                .unwrap()
        }
    };
    let context_python = callback("context").await;
    let selector_python = callback("selector").await;
    let eligibility_python = callback("eligibility").await;
    let stateful_python = callback("stateful").await;
    let many_python = callback("many").await;
    let too_many_keys_python = callback("too_many_keys").await;
    let too_large_python = callback("too_large").await;
    let unknown_python = callback("unknown").await;
    let malformed_python = callback("malformed").await;
    let plain_python = runtime
        .register(&python, "ordinary", Map::new())
        .await
        .unwrap();
    let queued_python = runtime
        .register(&python, "queued", Map::new())
        .await
        .unwrap();
    let concurrent = Datasets::start(
        ApplicationContextsSettings {
            namespace: format!("{}__queued", namespace.name),
            datasets: BTreeMap::from([(
                "users".into(),
                DatasetSettings {
                    loader: SelectorSettings::Rust("test.loader".into()),
                    ..DatasetSettings::default()
                },
            )]),
            ..ApplicationContextsSettings::default()
        },
        &url,
        BTreeMap::from([("users".into(), DatasetLoader::Python(loader.clone()))]),
    )
    .await
    .unwrap();
    // Unused read capacity must not consume the entire 4 MiB admission budget.
    let (first, second) = tokio::join!(
        queued_python.call_with_datasets(json!({"pause": true}), Some(concurrent.reader())),
        queued_python.call_with_datasets(json!({}), Some(concurrent.reader())),
    );
    assert_eq!(first.unwrap(), json!({"ok": true}));
    assert_eq!(second.unwrap(), json!({"ok": true}));

    let query = ParsedQuery::parse("SELECT 1").unwrap();
    let query_id = query.query_id();
    let context_request = ContextRequest {
        authenticated_user: "bi-service",
        frontend_database: "analytics",
        client_id: 7,
        query_id,
        query: &query,
        datasets: Some(&reader),
    };
    let context = ContextHook::Python(context_python)
        .derive(context_request, Duration::from_secs(1), 8192)
        .await;
    assert_eq!(context["email"], "alice@example.test");
    assert_eq!(context["fields"], json!({"enabled": true}));
    assert_eq!(context["all"]["loader"]["reason"], "startup");

    // The Rust context wrapper receives the same reader API.
    let rust_context = bind_context((), |request, _| {
        Box::pin(async move {
            request
                .datasets
                .unwrap()
                .get_many("users", &["enabled", "target"])
                .await
                .map_err(|_| ContextError::Failed)
        })
    });
    let rust_context = ContextHook::Rust(rust_context)
        .derive(context_request, Duration::from_secs(1), 8192)
        .await;
    assert_eq!(
        rust_context,
        Map::from_iter([
            ("enabled".into(), json!(true)),
            ("target".into(), json!("warehouse"))
        ])
    );

    let selection_request = SelectionRequest {
        authenticated_user: "bi-service",
        client_id: 7,
        query_id,
        query: &query,
        context: &rust_context,
        datasets: Some(&reader),
        async_hook_timeout_ms: 1000,
    };
    assert_eq!(
        RoutedSelector::Python(selector_python)
            .select(&selection_request)
            .await
            .unwrap(),
        yasp::hooks::BackendDecision {
            target: Some("warehouse".into()),
            backend_login: Some("reader".into()),
            compute_type: None,
        }
    );
    assert!(
        CacheEligibility::Python(eligibility_python)
            .eligible(&selection_request)
            .await
            .unwrap()
    );

    let plain = plain_python
        .call(json!({"value": [1, true, null]}))
        .await
        .unwrap();
    assert_eq!(
        plain,
        json!({"request": {"value": [1, true, null]}, "params": {}})
    );
    assert_eq!(
        stateful_python
            .call_with_datasets(json!({"operation": "retain"}), Some(reader.clone()))
            .await
            .unwrap(),
        json!({"retained": true})
    );
    assert_eq!(
        stateful_python
            .call_with_datasets(json!({"operation": "probe"}), Some(reader.clone()))
            .await
            .unwrap()["closed"],
        "dataset reader is closed"
    );
    assert_eq!(
        many_python
            .call_with_datasets(json!({}), Some(reader.clone()))
            .await,
        Err(PythonHookError::Execution)
    );
    assert_eq!(
        too_many_keys_python
            .call_with_datasets(json!({}), Some(reader.clone()))
            .await
            .unwrap()["error"],
        "dataset lookup exceeds configured limits"
    );
    assert_eq!(
        too_large_python
            .call_with_datasets(json!({}), Some(reader.clone()))
            .await
            .unwrap()["error"],
        "dataset lookup exceeds configured limits"
    );
    assert_eq!(
        unknown_python
            .call_with_datasets(json!({}), Some(reader.clone()))
            .await
            .unwrap()["error"],
        "unknown dataset"
    );

    let client = redis::Client::open(url.as_str()).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    redis::cmd("HSET")
        .arg(format!("{}::malformed::data", namespace.name))
        .arg("bad")
        .arg("PRIVATE_MALFORMED_VALUE")
        .query_async::<()>(&mut connection)
        .await
        .unwrap();
    assert_eq!(
        malformed_python
            .call_with_datasets(json!({}), Some(reader.clone()))
            .await
            .unwrap()["error"],
        "dataset data is malformed"
    );

    // Loader work is admitted on its own worker while query callbacks remain responsive.
    let loader_job = tokio::spawn({
        let loader = loader.clone();
        let started = loader_started.clone();
        async move {
            loader
                .call(json!({"name": "users", "reason": "slow", "slow": true, "started": started}))
                .await
        }
    });
    wait_file(&loader_started).await;
    let started = Instant::now();
    assert_eq!(
        plain_python.call(json!({"query": "fast"})).await.unwrap()["request"]["query"],
        "fast"
    );
    assert!(started.elapsed() < Duration::from_millis(250));
    loader_job.await.unwrap().unwrap();

    // Cancellation closes the retained helper while the Python worker keeps its byte permit.
    let marker = cancelled.clone();
    assert!(
        timeout(
            Duration::from_millis(25),
            stateful_python.call_with_datasets(
                json!({"operation": "cancel", "marker": marker}),
                Some(reader.clone()),
            ),
        )
        .await
        .is_err()
    );
    wait_file(&cancelled).await;
    assert_eq!(
        fs::read_to_string(&cancelled).unwrap(),
        "dataset reader is closed"
    );

    datasets.shutdown().await;
    concurrent.shutdown().await;
    runtime.shutdown();
}
