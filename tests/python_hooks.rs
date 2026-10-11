//! Public callback contract, independent of SQL routing or PostgreSQL.
#![cfg(feature = "python")]

use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::time::{sleep, timeout};
use yasp::{
    config::PythonSettings,
    hooks::python::{PythonHookError as Error, PythonRuntime},
};

struct Scripts(PathBuf);

impl Scripts {
    fn new() -> Self {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("yasp_python_{}_{}", std::process::id(), stamp));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn write(&self, name: &str, source: &str) -> PathBuf {
        let path = self.0.join(name);
        fs::write(&path, source).unwrap();
        path
    }
}

impl Drop for Scripts {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

async fn wait_file(path: &Path) {
    timeout(Duration::from_secs(3), async {
        while !path.exists() {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("Python reaches synchronization point");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn external_callback_contract() {
    let scripts = Scripts::new();
    scripts.write(
        "application_helper.py",
        "value = 'imported application resource'\n",
    );
    let script = scripts.write(
        "custom.py",
        r#"
import application_helper
import decimal
import _ssl
import sys
import time
from pathlib import Path

calls = 0
not_callable = 42

async def asynchronous(request, options):
    return request

def custom_behavior(request, options):
    global calls
    calls += 1
    operation = request.get('operation')
    if operation == 'block':
        Path(request['entered']).touch()
        deadline = time.monotonic() + 10
        while not Path(request['release']).exists():
            if time.monotonic() > deadline:
                raise RuntimeError('test synchronization failed')
            time.sleep(0.005)
        Path(request['finished']).touch()
        return calls
    if operation == 'queued':
        Path(request['executed']).touch()
        return calls
    if operation == 'raise':
        raise RuntimeError('private callback data must not escape')
    if operation == 'oversized':
        return 'x' * 8192
    if operation == 'deep':
        value = None
        for _ in range(65):
            value = [value]
        return value
    if operation == 'cycle':
        value = []
        value.append(value)
        return value
    if operation == 'unsupported':
        return object()
    if operation == 'nan':
        return float('nan')
    if operation == 'large_integer':
        return 2**64
    if operation == 'coroutine':
        return asynchronous(request, options)
    if operation == 'echo':
        return request['value']
    if operation == 'inspect':
        return {'path': sys.path, 'isolated': sys.flags.isolated,
                'site': 'site' in sys.modules}
    previous = options['label']
    options['label'] = 'mutated'
    return {'calls': calls, 'label': previous, 'helper': application_helper.value,
            'request': request, 'decimal': str(decimal.Decimal('1.0001'))}
"#,
    );
    let runtime = PythonRuntime::new(PythonSettings {
        search_paths: vec![scripts.0.clone()],
        queue_capacity: 1,
        queue_max_bytes: 4096,
        max_payload_bytes: 4096,
        execution_timeout_ms: 500,
    })
    .await
    .unwrap();
    assert!(matches!(
        PythonRuntime::new(PythonSettings::default()).await,
        Err(Error::AlreadyInitialized)
    ));
    let options = json!({"label": "host data"}).as_object().unwrap().clone();
    let callback = runtime
        .register(&script, "custom_behavior", options.clone())
        .await
        .unwrap();
    let request = json!({"nested": [true, null, 1.5], "sql": "unused by this arbitrary callback"});
    let result = callback.call(request.clone()).await.unwrap();
    assert_eq!(
        result,
        json!({"calls": 1, "label": "host data", "helper": "imported application resource",
                            "request": request, "decimal": "1.0001"})
    );
    assert_eq!(
        callback.call(json!({})).await.unwrap()["label"],
        "host data"
    );
    let second = runtime
        .register(&script, "custom_behavior", options.clone())
        .await
        .unwrap();
    assert_eq!(
        second.call(json!({})).await.unwrap()["calls"],
        1,
        "registrations own distinct modules"
    );
    let environment = callback
        .call(json!({"operation": "inspect"}))
        .await
        .unwrap();
    assert_eq!(environment["isolated"], 1);
    assert_eq!(environment["site"], false);
    for path in environment["path"].as_array().unwrap() {
        let path = path.as_str().unwrap();
        assert!(!path.is_empty() && !path.contains("site-packages"));
        assert_ne!(Path::new(path), std::env::current_dir().unwrap());
    }
    let edge_values = json!([
        i64::MIN,
        u64::MAX,
        "unicode: \u{1f642}",
        "quoted \"\n",
        false,
        null
    ]);
    assert_eq!(
        callback
            .call(json!({"operation": "echo", "value": edge_values}))
            .await
            .unwrap(),
        edge_values
    );
    for name in ["missing", "not_callable", "asynchronous"] {
        assert!(matches!(
            runtime.register(&script, name, options.clone()).await,
            Err(Error::Registration)
        ));
    }
    let missing_import = scripts.write("missing_import.py", "import yasp_nonexistent_dependency\n");
    assert!(matches!(
        runtime
            .register(&missing_import, "anything", options.clone())
            .await,
        Err(Error::Registration)
    ));
    assert!(matches!(
        runtime
            .register(&scripts.0.join("missing.py"), "anything", options.clone())
            .await,
        Err(Error::ReadScript)
    ));
    let oversized = scripts.write("too_large.py", &"#".repeat(4097));
    assert!(matches!(
        runtime
            .register(&oversized, "anything", options.clone())
            .await,
        Err(Error::PayloadTooLarge)
    ));
    assert_eq!(
        callback.call(json!({"operation": "raise"})).await,
        Err(Error::Execution)
    );
    for operation in ["unsupported", "nan", "large_integer", "coroutine"] {
        assert_eq!(
            callback.call(json!({"operation": operation})).await,
            Err(Error::InvalidOutput)
        );
    }
    for operation in ["oversized", "deep", "cycle"] {
        assert_eq!(
            callback.call(json!({"operation": operation})).await,
            Err(Error::PayloadTooLarge)
        );
    }
    assert_eq!(
        callback.call(json!({"value": "x".repeat(4096)})).await,
        Err(Error::PayloadTooLarge)
    );
    let mut deep = Value::Null;
    for _ in 0..65 {
        deep = json!([deep]);
    }
    assert_eq!(callback.call(deep).await, Err(Error::PayloadTooLarge));

    // Deadline and count bound: queued expiry never invokes application code.
    let entered = scripts.0.join("entered");
    let release = scripts.0.join("release");
    let finished = scripts.0.join("finished");
    let executed = scripts.0.join("executed");
    let active = tokio::spawn({
        let callback = callback.clone();
        let request = json!({"operation": "block", "entered": entered, "release": release, "finished": finished});
        async move { callback.call(request).await }
    });
    wait_file(&entered).await;
    let mut queued = Box::pin(callback.call(json!({"operation": "queued", "executed": executed})));
    tokio::select! {
        result = &mut queued => panic!("queued call completed while worker is blocked: {result:?}"),
        _ = sleep(Duration::from_millis(10)) => {}
    }
    assert_eq!(callback.call(json!({})).await, Err(Error::Saturated));
    assert_eq!(active.await.unwrap(), Err(Error::TimedOut));
    assert_eq!(queued.await, Err(Error::TimedOut));
    assert_eq!(
        callback.call(json!({})).await,
        Err(Error::Saturated),
        "expired work retains bounded queue storage"
    );
    fs::write(&release, "").unwrap();
    wait_file(&finished).await;
    timeout(Duration::from_secs(2), async {
        loop {
            match callback.call(json!({})).await {
                Ok(_) => break,
                Err(Error::Saturated) => sleep(Duration::from_millis(5)).await,
                other => panic!("unexpected recovery result: {other:?}"),
            }
        }
    })
    .await
    .unwrap();
    assert!(!executed.exists(), "expired queued job never ran");

    // Byte bound and dropped caller: the running call still owns its reservation.
    for path in [&entered, &release, &finished] {
        fs::remove_file(path).unwrap();
    }
    let active = tokio::spawn({
        let callback = callback.clone();
        let request = json!({"operation": "block", "entered": entered, "release": release,
                             "finished": finished, "padding": "x".repeat(2800)});
        async move { callback.call(request).await }
    });
    wait_file(&entered).await;
    active.abort();
    assert!(active.await.unwrap_err().is_cancelled());
    assert_eq!(
        callback.call(json!({"padding": "x".repeat(2000)})).await,
        Err(Error::Saturated)
    );
    let mut queued = Box::pin(callback.call(json!({})));
    tokio::select! {
        result = &mut queued => panic!("queued call completed before shutdown: {result:?}"),
        _ = sleep(Duration::from_millis(10)) => {}
    }
    runtime.shutdown();
    assert_eq!(queued.await, Err(Error::Closed));
    assert_eq!(callback.call(json!({})).await, Err(Error::Closed));
    fs::write(&release, "").unwrap();
    wait_file(&finished).await;
}
