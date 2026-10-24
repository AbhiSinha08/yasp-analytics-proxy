#![cfg(unix)]
#[path = "support/decision_model.rs"]
mod mock;
#[cfg(feature = "snowflake")]
#[path = "support/snowflake_host.rs"]
mod snowflake_host;
mod support;

use serde_json::{Map, json};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use support::{GatewayServer, TestDatabase, TestEnvironment, assert_success};
use yasp::{
    backend::{PostgresBackend, PostgresBackends},
    config::{CacheSettings, GatewaySettings, ResultProcessingSettings},
    hooks::{BackendSelection, bind_context, bind_process_result, bind_select_backend, builtin},
};

#[test]
fn rust_batches_filter_preserve_values_and_completion_in_both_profiles() {
    let env = TestEnvironment::load();
    let db = TestDatabase::create(&env.source(None), "yasp_result_values");
    let tls = std::path::PathBuf::from(std::env::var_os("YASP_TEST_TLS_DIR").unwrap());
    for profile in ["postgresql", "redshift"] {
        let sizes = Arc::new(Mutex::new(Vec::new()));
        let hook = bind_process_result(sizes.clone(), |request, batch, sizes| {
            Box::pin(async move {
                assert!(!request.authenticated_user.is_empty());
                assert_eq!(request.query_id, request.query.query_id());
                sizes.lock().unwrap().push(batch.len());
                let label = batch.column_index("label")?;
                assert!(batch.columns()[label].is_text());
                let keep = (0..batch.len())
                    .map(|row| {
                        batch.text(row, 0).unwrap().unwrap().parse::<u32>().unwrap() % 2 == 0
                    })
                    .collect::<Vec<_>>();
                builtin::process_result::uppercase(request, batch, Arc::new("label".to_owned()))
                    .await?;
                batch.retain_rows(&keep)?;
                Ok(())
            })
        });
        let gateway = env
            .gateway()
            .gateway_config()
            .with_settings(GatewaySettings {
                protocol: profile.into(),
                tls: yasp::config::GatewayTls {
                    mode: "enable".into(),
                    cert_file: Some(tls.join("server.crt")),
                    key_file: Some(tls.join("server.key")),
                },
                ..Default::default()
            })
            .unwrap()
            .with_result_hook(
                hook,
                ResultProcessingSettings {
                    max_rows_per_batch: 64,
                    target_batch_bytes: 4096,
                    ..Default::default()
                },
            )
            .unwrap();
        let server = GatewayServer::fixed(
            gateway,
            Arc::new(PostgresBackend::new(db.connection.backend_config()).unwrap()),
        );
        let connection = env.gateway().connection(server.address);
        let sql = "SELECT i, CASE WHEN i % 4 = 0 THEN NULL ELSE 'straße café ﬃ'::text END AS label, 12345678901234567890.123456789::numeric AS n, decode('00ff','hex') AS b, ARRAY[1,2] AS a, '2025-01-02 03:04:05.123456'::timestamp AS t FROM generate_series(1,513) i";
        let output = connection
            .command()
            .env("PGSSLMODE", "verify-full")
            .env("PGSSLROOTCERT", tls.join("ca.crt"))
            .args(["-c", sql, "-c", "\\echo :ROW_COUNT"])
            .output()
            .unwrap();
        assert_success(&output, "filtered complete source result");
        let text = String::from_utf8(output.stdout).unwrap();
        let lines = text.lines().collect::<Vec<_>>();
        assert_eq!(lines.len(), 257);
        assert_eq!(lines.last(), Some(&"256"));
        assert_eq!(
            lines[0],
            "2|STRASSE CAFÉ FFI|12345678901234567890.123456789|\\x00ff|{1,2}|2025-01-02 03:04:05.123456"
        );
        assert!(lines[1].starts_with("4||"));
        let observed = sizes.lock().unwrap();
        assert!(observed.len() > 1 && observed.iter().all(|n| *n <= 64));
        assert_eq!(observed.iter().sum::<usize>(), 513);
        drop(observed);
        let output = connection
            .command()
            .env("PGSSLMODE", "verify-full")
            .env("PGSSLROOTCERT", tls.join("ca.crt"))
            .args([
                "-c",
                "BEGIN",
                "-c",
                "SELECT 1 AS i, 'hidden'::text AS label",
                "-c",
                "\\echo :ROW_COUNT",
                "-c",
                "ROLLBACK",
            ])
            .output()
            .unwrap();
        assert_success(&output, "all rows filtered inside transaction");
        assert!(String::from_utf8_lossy(&output.stdout).contains("\n0\n"));
        let before = sizes.lock().unwrap().len();
        let empty = connection
            .command()
            .env("PGSSLMODE", "verify-full")
            .env("PGSSLROOTCERT", tls.join("ca.crt"))
            .args(["-c", "SELECT 1 AS i, 'empty'::text AS label WHERE false"])
            .output()
            .unwrap();
        assert_success(&empty, "empty result invokes hook once");
        assert_eq!(&sizes.lock().unwrap()[before..], &[0]);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cache_reprocesses_current_context_and_exposes_raw_classification_and_datasets() {
    use yasp::{
        config::{ApplicationContextsSettings, DatasetSettings, SelectorSettings},
        datasets::{DatasetReader, Datasets, bind_dataset_loader},
    };
    let env = TestEnvironment::load();
    let db = TestDatabase::create(&env.source(None), "yasp_result_cache");
    let redis = std::env::var("YASP_TEST_REDIS_URL").unwrap();
    let namespace = support::RedisNamespace::new(&redis, "yasp_result_cache");
    let datasets = Datasets::start(
        ApplicationContextsSettings {
            namespace: format!("{}:datasets", namespace.name),
            datasets: BTreeMap::from([(
                "users".into(),
                DatasetSettings {
                    refresh_interval_seconds: 0,
                    loader: SelectorSettings::Rust("test.users".into()),
                },
            )]),
            ..Default::default()
        },
        &redis,
        BTreeMap::from([(
            "users".into(),
            bind_dataset_loader((), |_, _| {
                Box::pin(async { Ok(Map::from_iter([("viewer".into(), json!({"allowed":true}))])) })
            })
            .into(),
        )]),
    )
    .await
    .unwrap();
    let turn = Arc::new(AtomicUsize::new(0));
    let context = bind_context(turn, |_, turn| {
        Box::pin(async move {
            Ok(Map::from_iter([
                (
                    "turn".into(),
                    json!(turn.fetch_add(1, Ordering::SeqCst) + 1),
                ),
                ("authenticated_user".into(), json!("untrusted")),
            ]))
        })
    });
    let retained = Arc::new(Mutex::new(None::<DatasetReader>));
    let hook = bind_process_result(retained.clone(), |request, batch, retained| {
        Box::pin(async move {
            assert_ne!(request.authenticated_user, "untrusted");
            assert_eq!(request.target, "source");
            assert_eq!(request.backend_login, "reader");
            assert_eq!(request.data_contexts.ast.unwrap().columns.len(), 2);
            let decisions = request.data_contexts.decision_model.unwrap();
            assert!(matches!(
                decisions.outcome(0),
                yasp::decision_model::ClassificationOutcome::Available { .. }
            ));
            assert_eq!(
                decisions.outcome(1),
                &yasp::decision_model::ClassificationOutcome::Unavailable
            );
            let reader = request.datasets.unwrap();
            assert_eq!(
                reader.get("users", "viewer").await.unwrap(),
                Some(json!({"allowed":true}))
            );
            *retained.lock().unwrap() = Some(reader.clone());
            let turn = request.context["turn"].as_u64().unwrap();
            let keep = (0..batch.len())
                .map(|row| {
                    batch.text(row, 0).unwrap().unwrap().parse::<u64>().unwrap() % 2 == turn % 2
                })
                .collect::<Vec<_>>();
            for row in 0..batch.len() {
                batch.set_text(row, 1, &format!("viewer-{turn}"))?;
            }
            batch.retain_rows(&keep)
        })
    });
    let model = mock::MockModel::new();
    model
        .response_by_ordinal
        .lock()
        .unwrap()
        .insert(1, "{}".into());
    let route = BackendSelection {
        target: "source".into(),
        backend_login: "reader".into(),
    };
    let backends = Arc::new(
        PostgresBackends::new(
            BTreeMap::from([(route.clone(), db.connection.backend_config())]),
            env.config.backend.clone(),
        )
        .unwrap(),
    );
    let gateway = env
        .gateway()
        .gateway_config()
        .with_context_hook(context)
        .with_datasets(datasets.reader())
        .with_ast_context(yasp::lineage::AstSettings {
            enabled: true,
            ..Default::default()
        })
        .with_decision_model(model.model(false))
        .with_cache(Arc::new(
            yasp::cache::RedisCache::new(
                CacheSettings {
                    enabled: true,
                    namespace: namespace.name.clone(),
                    ..Default::default()
                },
                &redis,
                None,
            )
            .unwrap(),
        ))
        .with_result_hook(hook, ResultProcessingSettings::default())
        .unwrap();
    let server = GatewayServer::routed(
        gateway,
        backends,
        bind_select_backend(route, |_, route| Ok(route.clone())),
    );
    let connection = env.gateway().connection(server.address);
    let sql = "SELECT i, 'raw-label'::text AS label FROM generate_series(1,100) i";
    for turn in 1..=2 {
        let output = connection.queries(&[sql, "\\echo :ROW_COUNT"]);
        assert_success(&output, "current viewer processing on miss and hit");
        let text = String::from_utf8(output.stdout).unwrap();
        assert_eq!(text.lines().count(), 51);
        assert_eq!(text.lines().last(), Some("50"));
        assert!(
            text.lines()
                .next()
                .unwrap()
                .ends_with(&format!("viewer-{turn}"))
        );
        assert_eq!(model.count(), 2, "a cache hit reuses raw decisions");
        let reader = retained.lock().unwrap().clone().unwrap();
        assert!(reader.get("users", "viewer").await.is_err());
    }
    let mut redis = redis::Client::open(redis)
        .unwrap()
        .get_connection()
        .unwrap();
    let keys: Vec<String> = redis::cmd("KEYS")
        .arg(format!("{}:*", namespace.name))
        .query(&mut redis)
        .unwrap();
    let key = keys
        .iter()
        .find(|key| key.contains("source_reader"))
        .unwrap();
    let raw: Vec<u8> = redis::cmd("GET").arg(key).query(&mut redis).unwrap();
    assert!(raw.windows(b"raw-label".len()).any(|v| v == b"raw-label"));
    assert!(raw.windows(b"SELECT 100".len()).any(|v| v == b"SELECT 100"));
    assert!(!raw.windows(b"viewer-".len()).any(|v| v == b"viewer-"));
    server.stop();
    datasets.shutdown().await;
}

#[test]
fn invalid_edits_byte_limits_and_hook_timeouts_fail_without_raw_fallback() {
    let env = TestEnvironment::load();
    let db = TestDatabase::create(&env.source(None), "yasp_result_errors");
    let backend = Arc::new(PostgresBackend::new(db.connection.backend_config()).unwrap());
    let hook = bind_process_result("label".to_owned(), builtin::process_result::uppercase);
    let server = GatewayServer::fixed(
        env.gateway()
            .gateway_config()
            .with_result_hook(
                hook,
                ResultProcessingSettings {
                    target_batch_bytes: 8192,
                    max_buffered_bytes_total: 65536,
                    ..Default::default()
                },
            )
            .unwrap(),
        backend.clone(),
    );
    let connection = env.gateway().connection(server.address);
    for sql in [
        "SELECT 'a'::text AS missing",
        "SELECT 'a'::text AS label, 'b'::text AS label",
        "SELECT 1 AS label",
        "SELECT 'ß'::varchar(1) AS label",
        "SELECT repeat('x', 262144) AS label",
    ] {
        let result = connection.query(sql);
        assert!(!result.status.success(), "{sql}");
        assert!(
            result.stdout.is_empty(),
            "failed batch never falls back to raw data"
        );
        assert_eq!(
            connection.execute("SELECT 'good'::text AS label").stdout,
            b"GOOD\n"
        );
    }
    server.stop();
    let hook = bind_process_result((), |request, batch, _| {
        Box::pin(async move {
            if request.query.sql().contains("sleep_hook") {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            if request.query.sql().contains("bad_mask") {
                return batch.retain_rows(&[]);
            }
            if request.query.sql().contains("null_number") {
                batch.set_null(0, 0)?;
            }
            Ok(())
        })
    });
    let gateway = env
        .gateway()
        .gateway_config()
        .with_settings(GatewaySettings {
            async_hook_timeout_ms: 20,
            write_timeout_ms: 10,
            ..Default::default()
        })
        .unwrap()
        .with_result_hook(hook, ResultProcessingSettings::default())
        .unwrap();
    let server = GatewayServer::fixed(
        gateway,
        Arc::new(PostgresBackend::new(db.connection.backend_config()).unwrap()),
    );
    let connection = env.gateway().connection(server.address);
    let result = connection.query("SELECT 'sleep_hook'::text AS label");
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("57014"));
    assert!(
        connection
            .query("SELECT 'bad_mask'::text AS label")
            .stdout
            .is_empty()
    );
    assert_eq!(
        connection.execute("SELECT 123 AS null_number").stdout,
        b"\n"
    );
    assert_eq!(
        connection
            .execute("SELECT 'recovered'::text AS label")
            .stdout,
        b"recovered\n"
    );
}

#[cfg(feature = "python")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn python_batches_are_lazy_scoped_and_recover_after_timeout_and_saturation() {
    use yasp::{config::PythonSettings, hooks::python::PythonRuntime};
    let env = TestEnvironment::load();
    let db = TestDatabase::create(&env.source(None), "yasp_result_python");
    let directory = support::TempDirectory::new("yasp_result_python");
    let path = directory.0.join("result.py");
    std::fs::write(
        &path,
        r#"
import time
previous = []
def process(request, batch, params):
    for old in previous:
        try:
            len(old)
        except RuntimeError:
            pass
        else:
            raise AssertionError('retained batch is still live')
    previous[:] = [batch]
    assert request['context']['prefix'] == 'custom'
    try:
        request['context']['prefix'] = 'mutated'
    except TypeError:
        pass
    else:
        raise AssertionError('mutable metadata')
    if 'slow_hook' in request['sql'] or 'reserve_hook' in request['sql']:
        time.sleep(0.2)
    if 'bad_return' in request['sql']:
        return []
    if 'throw_hook' in request['sql']:
        raise ValueError('PRIVATE_RESULT_VALUE')
    column = batch.column_index(params['column'])
    if 'oversized_output' in request['sql']:
        batch.set_text(0, column, 'a' * 2097152)
        return
    values = batch.text_column(column)
    if ' AS n' in request['sql']:
        assert all(isinstance(v, bytes) for v in batch.bytes_column(batch.column_index('n')))
    assert all(v is None or isinstance(v, str) for v in values)
    class Values(list):
        def __getitem__(self, index):
            raise AssertionError('Python sequence callbacks must not run while the native batch is borrowed')
    batch.replace_text_column(column, Values([None if v is None else v.upper() for v in values]))
    if ' AS i' in request['sql']:
        numbers = batch.text_column(batch.column_index('i'))
        batch.retain_rows([int(i) % 2 == 0 for i in numbers])
"#,
    )
    .unwrap();
    let runtime = PythonRuntime::new(PythonSettings {
        queue_capacity: 1,
        execution_timeout_ms: 50,
        ..Default::default()
    })
    .await
    .unwrap();
    let callback = runtime
        .register(
            &path,
            "process",
            json!({"column":"label"}).as_object().unwrap().clone(),
        )
        .await
        .unwrap();
    let context = bind_context((), |_, _| {
        Box::pin(async { Ok(Map::from_iter([("prefix".into(), json!("custom"))])) })
    });
    let gateway = env
        .gateway()
        .gateway_config()
        .with_context_hook(context)
        .with_result_hook(
            callback,
            ResultProcessingSettings {
                max_rows_per_batch: 32,
                ..Default::default()
            },
        )
        .unwrap();
    let server = GatewayServer::fixed(
        gateway,
        Arc::new(PostgresBackend::new(db.connection.backend_config()).unwrap()),
    );
    let connection = env.gateway().connection(server.address);
    let output = connection.queries(&["SELECT i AS i, CASE WHEN i % 4 = 0 THEN NULL ELSE 'straße ﬃ'::text END AS label, 12345678901234567890.123456789::numeric AS n FROM generate_series(1,129) i", "\\echo :ROW_COUNT"]);
    assert_success(&output, "Python lazy projected batch");
    let text = String::from_utf8(output.stdout).unwrap();
    assert_eq!(text.lines().count(), 65);
    assert_eq!(text.lines().last(), Some("64"));
    assert!(text.starts_with("2|STRASSE FFI|12345678901234567890.123456789\n4||"));
    for marker in ["bad_return", "throw_hook", "oversized_output"] {
        let result = connection.query(&format!("SELECT '{marker}'::text AS label"));
        assert!(!result.status.success());
        assert!(result.stdout.is_empty());
        assert!(!String::from_utf8_lossy(&result.stderr).contains("PRIVATE_RESULT_VALUE"));
    }
    std::thread::scope(|scope| {
        let jobs = (0..4)
            .map(|_| scope.spawn(|| connection.query("SELECT 'slow_hook'::text AS label")))
            .collect::<Vec<_>>();
        for job in jobs {
            assert!(!job.join().unwrap().status.success());
        }
    });
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(
        connection
            .execute("SELECT 'recovered'::text AS label")
            .stdout,
        b"RECOVERED\n"
    );
    let large = connection.execute("SELECT repeat('a',300000) AS label");
    assert_eq!(large.stdout.len(), 300001);
    assert!(large.stdout[..300000].iter().all(|b| *b == b'A'));
    let callback = runtime
        .register(
            &path,
            "process",
            json!({"column":"label"}).as_object().unwrap().clone(),
        )
        .await
        .unwrap();
    let gateway = env
        .gateway()
        .gateway_config()
        .with_context_hook(bind_context((), |_, _| {
            Box::pin(async { Ok(Map::from_iter([("prefix".into(), json!("custom"))])) })
        }))
        .with_result_hook(
            callback,
            ResultProcessingSettings {
                target_batch_bytes: 4096,
                max_buffered_bytes_total: 131072,
                ..Default::default()
            },
        )
        .unwrap();
    let bounded = GatewayServer::fixed(
        gateway,
        Arc::new(PostgresBackend::new(db.connection.backend_config()).unwrap()),
    );
    let bounded_connection = env.gateway().connection(bounded.address);
    let errors = std::thread::scope(|scope| {
        (0..3)
            .map(|_| {
                scope.spawn(|| {
                    bounded_connection.query("/* reserve_hook */ SELECT repeat('x',48000) AS label")
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|job| job.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert!(
        errors
            .iter()
            .all(|output| !output.status.success() && output.stdout.is_empty())
    );
    assert!(
        errors
            .iter()
            .any(|output| String::from_utf8_lossy(&output.stderr).contains("54000")),
        "executing/queued abandoned batches retain the shared byte budget"
    );
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(
        bounded_connection
            .execute("SELECT 'released'::text AS label")
            .stdout,
        b"RELEASED\n"
    );
    bounded.stop();
    server.stop();
}

#[cfg(feature = "snowflake")]
#[test]
fn snowflake_host_processes_results_and_replays_raw_cache_in_both_profiles() {
    let redis = std::env::var("YASP_TEST_REDIS_URL").unwrap();
    for profile in ["postgresql", "redshift"] {
        let namespace = support::RedisNamespace::new(&redis, "yasp_result_snowflake");
        snowflake_host::with_config(
            profile,
            |config| {
                config["hooks"]["process_result"] = json!({"type":"rust", "callback":"builtin.process_result.uppercase", "params":{"column":"label"}});
                config["cache"] = json!({"enabled":true, "url_env":"YASP_TEST_REDIS_URL", "namespace":namespace.name});
            },
            |_, _, query| {
                let sql = "SELECT 'hello' AS \"label\", 1234567890.12345 AS n";
                let first = query(sql);
                assert_success(&first, "Snowflake transformed miss");
                assert!(String::from_utf8_lossy(&first.stdout).starts_with("HELLO|"));
                assert_eq!(first.stdout, query(sql).stdout);
                assert!(!query("SELECT 123 AS \"label\"").status.success());
                assert_eq!(first.stdout, query(sql).stdout);
            },
        );
    }
}

#[test]
fn cancellation_and_disconnect_during_a_batch_release_single_source_capacity() {
    use std::{
        process::{Command, Stdio},
        sync::mpsc,
    };
    let env = TestEnvironment::load();
    let db = TestDatabase::create(&env.source(None), "yasp_result_cancel");
    let route = BackendSelection {
        target: "one".into(),
        backend_login: "reader".into(),
    };
    let (notify, started) = mpsc::channel();
    let hook = bind_process_result(notify, |request, _, notify| {
        Box::pin(async move {
            if request.query.sql().contains("blocked_hook") {
                let _ = notify.send(());
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
            Ok(())
        })
    });
    let backends = Arc::new(
        PostgresBackends::new(
            BTreeMap::from([(route.clone(), db.connection.backend_config())]),
            yasp::config::BackendSettings {
                max_connections_per_pool: 1,
                max_connections_total: 1,
                acquire_timeout_ms: 500,
            },
        )
        .unwrap(),
    );
    let gateway = env
        .gateway()
        .gateway_config()
        .with_result_hook(
            hook,
            ResultProcessingSettings {
                max_rows_per_batch: 1,
                ..Default::default()
            },
        )
        .unwrap();
    let server = GatewayServer::routed(
        gateway,
        backends,
        bind_select_backend(route, |_, route| Ok(route.clone())),
    );
    let connection = env.gateway().connection(server.address);
    for disconnect in [false, true] {
        let mut child = support::ChildGuard::new(
            connection
                .command()
                .args([
                    "-c",
                    "SELECT i, 'blocked_hook'::text AS label FROM generate_series(1,1000000) i",
                ])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        );
        started.recv_timeout(Duration::from_secs(3)).unwrap();
        if disconnect {
            child.child().kill().unwrap();
        } else {
            assert!(
                Command::new("kill")
                    .args(["-INT", &child.child().id().to_string()])
                    .status()
                    .unwrap()
                    .success()
            );
        }
        assert!(!child.child().wait().unwrap().success());
        assert_eq!(
            connection
                .execute("SELECT 'recovered'::text AS label")
                .stdout,
            b"recovered\n"
        );
    }
}

#[cfg(all(feature = "snowflake", feature = "python"))]
#[test]
fn snowflake_python_filters_cached_raw_rows_in_both_profiles() {
    let redis_url = std::env::var("YASP_TEST_REDIS_URL").unwrap();
    let temporary = support::TempDirectory::new("yasp_result_snowflake_python");
    let path = temporary.0.join("process.py");
    std::fs::write(&path, "def process(request, batch, params):\n    assert request['data_contexts']['decision_model'] is None\n    column = batch.column_index(params['column'])\n    batch.replace_text_column(column, [v.upper() for v in batch.text_column(column)])\n    batch.retain_rows([int(v) % 2 == 0 for v in batch.text_column(batch.column_index('i'))])\n").unwrap();
    for profile in ["postgresql", "redshift"] {
        let namespace = support::RedisNamespace::new(&redis_url, "yasp_result_sf_python");
        snowflake_host::with_config(
            profile,
            |config| {
                config["hooks"]["process_result"] = json!({"type":"python", "path":path, "callback":"process", "params":{"column":"label"}});
                config["cache"] = json!({"enabled":true,"url_env":"YASP_TEST_REDIS_URL","namespace":namespace.name});
            },
            |_, _, query| {
                let sql = "SELECT 1 AS \"i\", 'first' AS \"label\" UNION ALL SELECT 2, 'second'";
                let first = query(sql);
                assert_success(&first, "Snowflake Python result filtering");
                assert_eq!(first.stdout, b"2|SECOND\n");
                assert_eq!(first.stdout, query(sql).stdout);
                let mut redis = redis::Client::open(redis_url.as_str())
                    .unwrap()
                    .get_connection()
                    .unwrap();
                let keys: Vec<String> = redis::cmd("KEYS")
                    .arg(format!("{}:*", namespace.name))
                    .query(&mut redis)
                    .unwrap();
                assert_eq!(keys.len(), 1);
                let raw: Vec<u8> = redis::cmd("GET").arg(&keys[0]).query(&mut redis).unwrap();
                assert!(raw.windows(5).any(|v| v == b"first"));
                assert!(raw.windows(6).any(|v| v == b"second"));
                assert!(raw.windows(8).any(|v| v == b"SELECT 2"));
            },
        );
    }
}
