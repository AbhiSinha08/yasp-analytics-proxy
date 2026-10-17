//! Shared dataset integration scenarios against the disposable Redis service.
mod support;

use serde_json::{Map, Value, json};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use support::RedisNamespace;
use tokio::sync::Notify;
use yasp::{
    config::{ApplicationContextsSettings, DatasetSettings, SelectorSettings},
    datasets::{
        DatasetError, DatasetLoadError, DatasetLoadRequest, DatasetLoader, DatasetReader, Datasets,
        bind_dataset_loader,
    },
    hooks::{ContextHook, ContextRequest, bind_context},
    query::ParsedQuery,
};

fn redis_url() -> String {
    std::env::var("YASP_TEST_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".into())
}

fn settings(namespace: &str, datasets: &[(&str, u64)]) -> ApplicationContextsSettings {
    let mut settings = ApplicationContextsSettings {
        namespace: namespace.into(),
        max_lookup_bytes: 65_536,
        ..ApplicationContextsSettings::default()
    };
    settings.datasets = datasets
        .iter()
        .map(|(name, interval)| {
            (
                (*name).into(),
                DatasetSettings {
                    refresh_interval_seconds: *interval,
                    loader: SelectorSettings::Rust("tests.datasets.loader".into()),
                },
            )
        })
        .collect();
    settings
}

fn loader<F>(state: F) -> DatasetLoader
where
    F: for<'a> Fn(
            DatasetLoadRequest<'a>,
            Arc<()>,
        )
            -> futures::future::BoxFuture<'a, Result<Map<String, Value>, DatasetLoadError>>
        + Send
        + Sync
        + 'static,
{
    bind_dataset_loader((), state).into()
}

fn loaders(entries: Vec<(&str, DatasetLoader)>) -> BTreeMap<String, DatasetLoader> {
    entries
        .into_iter()
        .map(|(name, loader)| (name.into(), loader))
        .collect()
}

async fn wait_for(description: &str, mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !condition() {
        assert!(
            Instant::now() < deadline,
            "{description} did not become true before deadline"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_value(reader: &DatasetReader, key: &str, expected: Value) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if reader.get("users", key).await == Ok(Some(expected.clone())) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("dataset value updates before deadline");
}

async fn assert_bad_refresh_keeps_generation(
    settings: &ApplicationContextsSettings,
    url: &str,
    namespace: &str,
    invalid_data: Map<String, Value>,
) {
    let bad_data = loader(move |_, _| {
        let data = invalid_data.clone();
        Box::pin(async move { Ok(data) })
    });
    let error = Datasets::start(settings.clone(), url, loaders(vec![("users", bad_data)]))
        .await
        .err()
        .expect("invalid data prevents startup publication");
    assert_eq!(error, DatasetError::Load(DatasetLoadError::Limit));

    let client = redis::Client::open(url).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    let stored: String = redis::cmd("HGET")
        .arg(format!("{namespace}::users::data"))
        .arg("generation")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(stored, "3", "invalid refresh retains the published dataset");
}

#[tokio::test]
async fn reads_json_values_missing_keys_limits_and_new_redis_values() {
    let url = redis_url();
    let namespace = RedisNamespace::new(&url, "yasp_datasets");
    let values = loader(|request, _| {
        Box::pin(async move {
            Ok(match request.name {
                "values" => serde_json::from_value(json!({
                    "null_value": null,
                    "array": [1, true, "x"],
                    "scalar": 17,
                    "object": {"nested": [null, false]}
                }))
                .unwrap(),
                "empty" => Map::new(),
                "many" => (0..65)
                    .map(|index| (format!("f{index:02}"), json!(index)))
                    .collect(),
                "bytes" => Map::from_iter([
                    ("a".into(), json!("a".repeat(40_000))),
                    ("b".into(), json!("b".repeat(40_000))),
                ]),
                _ => unreachable!(),
            })
        })
    });
    let datasets = Datasets::start(
        settings(
            &namespace.name,
            &[("values", 0), ("empty", 0), ("many", 0), ("bytes", 0)],
        ),
        &url,
        loaders(vec![
            ("values", values),
            ("empty", loader(|_, _| Box::pin(async { Ok(Map::new()) }))),
            (
                "many",
                loader(|_, _| {
                    Box::pin(async {
                        Ok((0..65)
                            .map(|index| (format!("f{index:02}"), json!(index)))
                            .collect())
                    })
                }),
            ),
            (
                "bytes",
                loader(|_, _| {
                    Box::pin(async {
                        Ok(Map::from_iter([
                            ("a".into(), json!("a".repeat(40_000))),
                            ("b".into(), json!("b".repeat(40_000))),
                        ]))
                    })
                }),
            ),
        ]),
    )
    .await
    .unwrap();
    let reader = datasets.reader();

    assert_eq!(
        reader.get("values", "null_value").await.unwrap(),
        Some(Value::Null)
    );
    assert_eq!(
        reader.get("values", "array").await.unwrap(),
        Some(json!([1, true, "x"]))
    );
    assert_eq!(
        reader.get("values", "scalar").await.unwrap(),
        Some(json!(17))
    );
    assert_eq!(reader.get("values", "missing").await.unwrap(), None);
    let selected = reader
        .get_many("values", &["scalar", "missing", "scalar", "null_value"])
        .await
        .unwrap();
    assert_eq!(
        selected,
        Map::from_iter([
            ("null_value".into(), Value::Null),
            ("scalar".into(), json!(17))
        ])
    );
    assert!(reader.get_many("values", &vec!["x"; 65]).await.is_err());
    assert_eq!(reader.read("empty").await.unwrap(), Map::new());
    assert_eq!(reader.read("many").await, Err(DatasetError::Limit));
    assert_eq!(
        reader.get_many("bytes", &["a", "b"]).await,
        Err(DatasetError::Limit)
    );
    assert_eq!(reader.read("bytes").await, Err(DatasetError::Limit));
    assert_eq!(
        reader.get("not_configured", "x").await,
        Err(DatasetError::UnknownDataset)
    );

    // A Rust callback can retain a clone while running, but the hook scope closes it
    // as soon as the callback finishes.
    let retained = Arc::new(Mutex::new(None));
    let hook: ContextHook = bind_context(retained.clone(), |request, state| {
        Box::pin(async move {
            let scoped = request.datasets.expect("reader is attached").clone();
            assert_eq!(
                scoped.get("values", "scalar").await.unwrap(),
                Some(json!(17))
            );
            *state.lock().unwrap() = Some(scoped);
            Ok(Map::new())
        })
    })
    .into();
    let query = ParsedQuery::parse("SELECT 1").unwrap();
    let query_id = query.query_id();
    let context_request = ContextRequest {
        authenticated_user: "frontend",
        frontend_database: "analytics",
        client_id: 1,
        query_id,
        query: &query,
        datasets: Some(&reader),
    };
    assert_eq!(
        hook.derive(context_request, Duration::from_secs(1), 65_536)
            .await,
        Map::new()
    );
    let retained = retained.lock().unwrap().take().unwrap();
    assert_eq!(
        retained.get("values", "scalar").await,
        Err(DatasetError::Closed)
    );

    // A reader has no process-local value cache; an external registry publish is visible next call.
    let client = redis::Client::open(url.as_str()).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    redis::cmd("HSET")
        .arg(format!("{}::values::state", namespace.name))
        .arg("format_version")
        .arg("1")
        .query_async::<()>(&mut connection)
        .await
        .unwrap();
    redis::cmd("HSET")
        .arg(format!("{}::values::data", namespace.name))
        .arg("")
        .arg("1")
        .arg("scalar")
        .arg("99")
        .query_async::<()>(&mut connection)
        .await
        .unwrap();
    assert_eq!(
        reader.get("values", "scalar").await.unwrap(),
        Some(json!(99))
    );
    datasets.shutdown().await;
}

#[tokio::test]
async fn startup_reloads_existing_data_and_loader_errors_are_redacted() {
    let url = redis_url();
    let namespace = RedisNamespace::new(&url, "yasp_datasets_startup");
    let settings = settings(&namespace.name, &[("users", 0)]);
    let count = Arc::new(AtomicUsize::new(0));
    let count_for_loader = count.clone();
    let first = loader(move |_, _| {
        let generation = count_for_loader.fetch_add(1, Ordering::SeqCst) + 1;
        Box::pin(async move {
            let mut data = Map::from_iter([("generation".into(), json!(generation))]);
            data.insert("stale".into(), json!(true));
            Ok(data)
        })
    });
    let datasets = Datasets::start(settings.clone(), &url, loaders(vec![("users", first)]))
        .await
        .unwrap();
    assert_eq!(
        datasets.reader().get("users", "generation").await.unwrap(),
        Some(json!(1))
    );
    datasets.shutdown().await;

    let count_for_second = count.clone();
    let second = loader(move |_, _| {
        let generation = count_for_second.fetch_add(1, Ordering::SeqCst) + 1;
        Box::pin(async move { Ok(Map::from_iter([("generation".into(), json!(generation))])) })
    });
    let datasets = Datasets::start(settings.clone(), &url, loaders(vec![("users", second)]))
        .await
        .unwrap();
    assert_eq!(
        datasets.reader().get("users", "generation").await.unwrap(),
        Some(json!(2))
    );
    assert_eq!(
        datasets.reader().get("users", "stale").await.unwrap(),
        None,
        "publication removes fields omitted by the new complete dataset"
    );
    datasets.shutdown().await;

    let empty = loader(|_, _| Box::pin(async { Ok(Map::new()) }));
    let datasets = Datasets::start(settings.clone(), &url, loaders(vec![("users", empty)]))
        .await
        .unwrap();
    assert_eq!(datasets.reader().read("users").await.unwrap(), Map::new());
    datasets.shutdown().await;

    let count_for_fourth = count.clone();
    let fourth = loader(move |_, _| {
        let generation = count_for_fourth.fetch_add(1, Ordering::SeqCst) + 1;
        Box::pin(async move { Ok(Map::from_iter([("generation".into(), json!(generation))])) })
    });
    let datasets = Datasets::start(settings.clone(), &url, loaders(vec![("users", fourth)]))
        .await
        .unwrap();
    assert_eq!(
        datasets.reader().get("users", "generation").await.unwrap(),
        Some(json!(3))
    );
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    assert_eq!(
        count.load(Ordering::SeqCst),
        3,
        "zero interval does not schedule refreshes"
    );
    datasets.shutdown().await;

    let invalid = Datasets::start(settings.clone(), &url, BTreeMap::new())
        .await
        .err()
        .expect("configured names require matching loaders");
    assert_eq!(invalid, DatasetError::InvalidSettings);

    let failed = loader(|_, _| Box::pin(async { Err(DatasetLoadError::Failed) }));
    let error = Datasets::start(settings.clone(), &url, loaders(vec![("users", failed)]))
        .await
        .err()
        .expect("configured loader failure prevents startup");
    assert_eq!(error, DatasetError::Load(DatasetLoadError::Failed));
    assert!(!error.to_string().contains(&namespace.name));
    let client = redis::Client::open(url.as_str()).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    let stored: String = redis::cmd("HGET")
        .arg(format!("{}::users::data", namespace.name))
        .arg("generation")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(
        stored, "3",
        "failed startup leaves the last published value intact"
    );

    assert_bad_refresh_keeps_generation(
        &settings,
        &url,
        &namespace.name,
        Map::from_iter([("".into(), json!(1))]),
    )
    .await;
    let mut too_deep = json!(null);
    for _ in 0..65 {
        too_deep = json!([too_deep]);
    }
    assert_bad_refresh_keeps_generation(
        &settings,
        &url,
        &namespace.name,
        Map::from_iter([("deep".into(), too_deep)]),
    )
    .await;
    assert_bad_refresh_keeps_generation(
        &settings,
        &url,
        &namespace.name,
        Map::from_iter([("large".into(), json!("x".repeat(65_537)))]),
    )
    .await;
}

#[tokio::test]
async fn concurrent_startup_uses_one_loader_and_scheduled_refresh_fences_late_writer() {
    struct Gate {
        loads: AtomicUsize,
        started: Notify,
        release: Notify,
    }
    let url = redis_url();
    let namespace = RedisNamespace::new(&url, "yasp_datasets_refresh");
    let gate = Arc::new(Gate {
        loads: AtomicUsize::new(0),
        started: Notify::new(),
        release: Notify::new(),
    });
    let make_loader = |gate: Arc<Gate>| {
        loader(move |request, _state| {
            let gate = gate.clone();
            Box::pin(async move {
                let generation = gate.loads.fetch_add(1, Ordering::SeqCst) + 1;
                if request.reason == "startup" && generation == 1 {
                    gate.started.notify_one();
                    gate.release.notified().await;
                }
                Ok(Map::from_iter([("generation".into(), json!(generation))]))
            })
        })
    };
    let first_settings = settings(&namespace.name, &[("users", 1)]);
    let first_url = url.clone();
    let first_namespace = namespace.name.clone();
    let first_loader = make_loader(gate.clone());
    let first = tokio::spawn(async move {
        Datasets::start(
            first_settings,
            &first_url,
            loaders(vec![("users", first_loader)]),
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(5), gate.started.notified())
        .await
        .expect("first startup loader begins");
    let second_settings = settings(&namespace.name, &[("users", 1)]);
    let second_url = url.clone();
    let second_loader = make_loader(gate.clone());
    let second = tokio::spawn(async move {
        Datasets::start(
            second_settings,
            &second_url,
            loaders(vec![("users", second_loader)]),
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(gate.loads.load(Ordering::SeqCst), 1);
    gate.release.notify_one();
    let first = first.await.unwrap().unwrap();
    let second = second.await.unwrap().unwrap();
    assert_eq!(gate.loads.load(Ordering::SeqCst), 1);
    assert_eq!(
        first.reader().get("users", "generation").await.unwrap(),
        Some(json!(1))
    );
    assert_eq!(
        second.reader().get("users", "generation").await.unwrap(),
        Some(json!(1))
    );
    first.shutdown().await;
    second.shutdown().await;

    // A new manager starts with the current value; its scheduled loader is held while
    // a newer lease token is installed, so its eventual publish must be rejected.
    let scheduled = Arc::new(Gate {
        loads: AtomicUsize::new(0),
        started: Notify::new(),
        release: Notify::new(),
    });
    let scheduled_loader = {
        let scheduled = scheduled.clone();
        loader(move |request, _state| {
            let scheduled = scheduled.clone();
            Box::pin(async move {
                if request.reason == "scheduled" {
                    match scheduled.loads.fetch_add(1, Ordering::SeqCst) + 1 {
                        1 => {
                            scheduled.started.notify_one();
                            scheduled.release.notified().await;
                            Ok(Map::from_iter([("generation".into(), json!(2))]))
                        }
                        2 => Err(DatasetLoadError::Failed),
                        _ => Ok(Map::from_iter([("generation".into(), json!(3))])),
                    }
                } else {
                    Ok(Map::from_iter([("generation".into(), json!(1))]))
                }
            })
        })
    };
    let settings = settings(&first_namespace, &[("users", 1)]);
    let datasets = Datasets::start(settings, &url, loaders(vec![("users", scheduled_loader)]))
        .await
        .unwrap();
    // The startup load publishes generation 2, so set a known old value before the refresh.
    let reader = datasets.reader();
    let client = redis::Client::open(url.as_str()).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    redis::cmd("HSET")
        .arg(format!("{first_namespace}::users::data"))
        .arg("generation")
        .arg("1")
        .query_async::<()>(&mut connection)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), scheduled.started.notified())
        .await
        .expect("scheduled loader begins");
    redis::cmd("SET")
        .arg(format!("{first_namespace}::users::lease"))
        .arg("newer-owner")
        .query_async::<()>(&mut connection)
        .await
        .unwrap();
    scheduled.release.notify_one();
    wait_for("first scheduled loader", || {
        scheduled.loads.load(Ordering::SeqCst) == 1
    })
    .await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        reader.get("users", "generation").await.unwrap(),
        Some(json!(1))
    );

    // Let the next scheduled attempt fail and then recover on its following interval.
    redis::cmd("DEL")
        .arg(format!("{first_namespace}::users::lease"))
        .query_async::<()>(&mut connection)
        .await
        .unwrap();
    redis::cmd("HSET")
        .arg(format!("{first_namespace}::users::state"))
        .arg("next_refresh_at_ms")
        .arg("0")
        .query_async::<()>(&mut connection)
        .await
        .unwrap();
    wait_for("failed scheduled loader", || {
        scheduled.loads.load(Ordering::SeqCst) >= 2
    })
    .await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        reader.get("users", "generation").await.unwrap(),
        Some(json!(1))
    );
    wait_for("retry scheduled loader", || {
        scheduled.loads.load(Ordering::SeqCst) >= 3
    })
    .await;
    wait_for_value(&reader, "generation", json!(3)).await;
    datasets.shutdown().await;
}

#[tokio::test]
async fn redis_outage_returns_a_bounded_redacted_error() {
    let namespace = RedisNamespace::new(&redis_url(), "yasp_datasets_unavailable");
    let callback =
        loader(|_, _| Box::pin(async { Ok(Map::from_iter([("safe".into(), json!(true))])) }));
    let error = Datasets::start(
        settings(&namespace.name, &[("users", 0)]),
        "redis://127.0.0.1:1/",
        loaders(vec![("users", callback)]),
    )
    .await
    .err()
    .expect("unavailable Redis prevents startup");
    assert!(matches!(
        error,
        DatasetError::Unavailable | DatasetError::TimedOut
    ));
    assert!(!error.to_string().contains("127.0.0.1"));
}

#[tokio::test]
async fn malformed_revision_does_not_replace_published_data() {
    let url = redis_url();
    let namespace = RedisNamespace::new(&url, "yasp_datasets_revision");
    let config = settings(&namespace.name, &[("users", 0)]);
    let datasets = Datasets::start(
        config.clone(),
        &url,
        loaders(vec![(
            "users",
            loader(|_, _| {
                Box::pin(async { Ok(Map::from_iter([("value".into(), json!("previous"))])) })
            }),
        )]),
    )
    .await
    .unwrap();
    datasets.shutdown().await;
    let mut connection = redis::Client::open(url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    for revision in ["invalid", "0001", "999999999999999999999"] {
        redis::cmd("HSET")
            .arg(format!("{}::users::state", namespace.name))
            .arg("revision")
            .arg(revision)
            .query_async::<()>(&mut connection)
            .await
            .unwrap();
        let result = Datasets::start(
            config.clone(),
            &url,
            loaders(vec![(
                "users",
                loader(|_, _| {
                    Box::pin(async { Ok(Map::from_iter([("value".into(), json!("replacement"))])) })
                }),
            )]),
        )
        .await;
        assert!(matches!(result, Err(DatasetError::InvalidData)));
        let current: String = redis::cmd("HGET")
            .arg(format!("{}::users::data", namespace.name))
            .arg("value")
            .query_async(&mut connection)
            .await
            .unwrap();
        assert_eq!(current, "\"previous\"");
    }
}
