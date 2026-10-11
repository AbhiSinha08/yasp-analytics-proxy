//! External trusted Python callbacks, isolated imports, and bounded invocation.
//!
//! One runtime may be created per process. Clone it to register more callbacks.
//! Python owns module state; each call receives fresh JSON-compatible objects.

use crate::config::PythonSettings;
use pyo3::{
    IntoPyObjectExt, ffi,
    prelude::*,
    types::{PyBool, PyDict, PyFloat, PyInt, PyList, PyModule, PyString},
};
use serde_json::{Map, Number, Value};
use std::{
    ffi::CString,
    fs::File,
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use thiserror::Error;
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch},
    time::{Instant, timeout_at},
};

const MAX_DEPTH: usize = 64;
static INITIALIZED: AtomicBool = AtomicBool::new(false);

/// Diagnostics intentionally exclude Python exception messages and source text.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum PythonHookError {
    #[error("invalid Python runtime settings")]
    InvalidSettings,
    #[error("Python runtime already initialized; share the existing runtime")]
    AlreadyInitialized,
    #[error("Python interpreter initialization failed")]
    Initialization,
    #[error("cannot read Python script")]
    ReadScript,
    #[error("Python callback registration failed")]
    Registration,
    #[error("Python callback execution failed")]
    Execution,
    #[error("Python callback returned unsupported data")]
    InvalidOutput,
    #[error("Python payload exceeds size or nesting limits")]
    PayloadTooLarge,
    #[error("Python worker queue is saturated")]
    Saturated,
    #[error("Python operation timed out")]
    TimedOut,
    #[error("Python runtime is closed")]
    Closed,
}

/// A process-wide interpreter with bounded asynchronous admission.
///
/// ```no_run
/// use serde_json::json;
/// use yasp::{config::PythonSettings, hooks::python::PythonRuntime};
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let runtime = PythonRuntime::new(PythonSettings::default()).await?;
/// let callback = runtime.register(
///     std::path::Path::new("/opt/application/custom.py"), "custom_callback",
///     json!({"application_option": true}).as_object().unwrap().clone(),
/// ).await?;
/// let value = callback.call(json!({"application_request": [1, 2]})).await?;
/// runtime.shutdown();
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct PythonRuntime(Arc<Inner>);

struct Inner {
    sender: Mutex<Option<mpsc::Sender<Job>>>,
    stopped: watch::Sender<bool>,
    bytes: Arc<Semaphore>,
    settings: PythonSettings,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.stopped.send_replace(true);
    }
}

/// A registered function and its application-owned params.
#[derive(Clone)]
pub struct PythonCallback {
    runtime: PythonRuntime,
    id: usize,
    params_bytes: usize,
}

enum Operation {
    Register {
        path: PathBuf,
        callback: String,
        params: Value,
    },
    Call {
        id: usize,
        request: Value,
    },
}

enum Reply {
    Registered(usize),
    Value(Value),
}

struct Job {
    operation: Operation,
    deadline: Instant,
    reply: oneshot::Sender<Result<Reply, PythonHookError>>,
    // Kept with the job, including after its caller stops waiting.
    _bytes: OwnedSemaphorePermit,
}

struct Registered {
    function: Py<PyAny>,
    params: Value,
}

impl PythonRuntime {
    /// Initialize isolated CPython on a dedicated thread before serving requests.
    /// Search paths must be absolute; base/user site-packages are not imported.
    pub async fn new(mut settings: PythonSettings) -> Result<Self, PythonHookError> {
        settings
            .validate()
            .map_err(|_| PythonHookError::InvalidSettings)?;
        for path in &mut settings.search_paths {
            if !path.is_absolute() || !path.is_dir() {
                return Err(PythonHookError::InvalidSettings);
            }
            *path = path
                .canonicalize()
                .map_err(|_| PythonHookError::InvalidSettings)?;
        }
        let (sender, receiver) = mpsc::channel(settings.queue_capacity);
        let (stopped, stop_receiver) = watch::channel(false);
        let (ready, initialized) = oneshot::channel();
        let runtime = Self(Arc::new(Inner {
            sender: Mutex::new(Some(sender)),
            stopped,
            bytes: Arc::new(Semaphore::new(settings.queue_max_bytes)),
            settings: settings.clone(),
        }));
        let deadline = Instant::now() + runtime.timeout();
        std::thread::Builder::new()
            .name("yasp-python".into())
            .spawn(move || {
                let result = initialize(&settings.search_paths);
                let success = result.is_ok();
                let _ = ready.send(result);
                if success {
                    worker(receiver, stop_receiver, settings.max_payload_bytes);
                }
            })
            .map_err(|_| PythonHookError::Initialization)?;
        timeout_at(deadline, initialized)
            .await
            .map_err(|_| PythonHookError::TimedOut)?
            .map_err(|_| PythonHookError::Initialization)??;
        Ok(runtime)
    }

    /// Read an external file and retain its function without embedding its source.
    pub async fn register(
        &self,
        path: &Path,
        callback: &str,
        params: Map<String, Value>,
    ) -> Result<PythonCallback, PythonHookError> {
        if !path.is_absolute() || callback.trim().is_empty() || callback.contains('\0') {
            return Err(PythonHookError::Registration);
        }
        let params = Value::Object(params);
        let params_bytes = payload_size(&params, self.0.settings.max_payload_bytes)?;
        let path_bytes = path.to_str().ok_or(PythonHookError::Registration)?.len();
        if path_bytes
            .saturating_add(callback.len())
            .saturating_add(params_bytes)
            > self.0.settings.max_payload_bytes
        {
            return Err(PythonHookError::PayloadTooLarge);
        }
        let operation = Operation::Register {
            path: path.to_owned(),
            callback: callback.to_owned(),
            params,
        };
        // Reserve room for bounded script input before reading it on the worker.
        match self
            .submit(operation, self.0.settings.max_payload_bytes)
            .await?
        {
            Reply::Registered(id) => Ok(PythonCallback {
                runtime: self.clone(),
                id,
                params_bytes,
            }),
            Reply::Value(_) => unreachable!("registration returns a handle"),
        }
    }

    /// Stop admission and wake waiters; never join an executing Python callback.
    pub fn shutdown(&self) {
        self.0.stopped.send_replace(true);
        self.0.sender.lock().unwrap().take();
    }

    fn timeout(&self) -> Duration {
        Duration::from_millis(u64::from(self.0.settings.execution_timeout_ms))
    }

    async fn submit(&self, operation: Operation, bytes: usize) -> Result<Reply, PythonHookError> {
        let deadline = Instant::now() + self.timeout();
        let mut stopped = self.0.stopped.subscribe();
        if *stopped.borrow() {
            return Err(PythonHookError::Closed);
        }
        let permit = self
            .0
            .bytes
            .clone()
            .try_acquire_many_owned(bytes as u32)
            .map_err(|_| PythonHookError::Saturated)?;
        let (reply, result) = oneshot::channel();
        let job = Job {
            operation,
            deadline,
            reply,
            _bytes: permit,
        };
        self.0
            .sender
            .lock()
            .unwrap()
            .as_ref()
            .ok_or(PythonHookError::Closed)?
            .try_send(job)
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => PythonHookError::Saturated,
                mpsc::error::TrySendError::Closed(_) => PythonHookError::Closed,
            })?;
        tokio::select! {
            biased;
            _ = stopped.changed() => Err(PythonHookError::Closed),
            result = timeout_at(deadline, result) => result
                .map_err(|_| PythonHookError::TimedOut)?
                .map_err(|_| PythonHookError::Closed)?,
        }
    }
}

impl PythonCallback {
    pub fn runtime(&self) -> &PythonRuntime {
        &self.runtime
    }

    /// Invoke function(request, params) with fresh plain Python values.
    /// Dropping this future abandons the response, not an executing function.
    pub async fn call(&self, request: Value) -> Result<Value, PythonHookError> {
        let limit = self.runtime.0.settings.max_payload_bytes;
        let bytes = payload_size(&request, limit)?
            .checked_add(self.params_bytes)
            .filter(|bytes| *bytes <= limit)
            .ok_or(PythonHookError::PayloadTooLarge)?;
        match self
            .runtime
            .submit(
                Operation::Call {
                    id: self.id,
                    request,
                },
                bytes,
            )
            .await?
        {
            Reply::Value(value) => Ok(value),
            Reply::Registered(_) => unreachable!("invocation returns a value"),
        }
    }
}

fn initialize(paths: &[PathBuf]) -> Result<(), PythonHookError> {
    if INITIALIZED.swap(true, Ordering::SeqCst) {
        return Err(PythonHookError::AlreadyInitialized);
    }
    // SAFETY: this runtime claims initialization once, before attaching workers.
    // Consuming hosts must not initialize/finalize CPython concurrently with YASP.
    unsafe {
        if ffi::Py_IsInitialized() != 0 {
            return Err(PythonHookError::AlreadyInitialized);
        }
        let mut config = std::mem::MaybeUninit::<ffi::PyConfig>::uninit();
        ffi::PyConfig_InitIsolatedConfig(config.as_mut_ptr());
        let mut config = config.assume_init();
        config.site_import = 0;
        config.write_bytecode = 0;
        let status = ffi::Py_InitializeFromConfig(&config);
        ffi::PyConfig_Clear(&mut config);
        if ffi::PyStatus_Exception(status) != 0 {
            return Err(PythonHookError::Initialization);
        }
        ffi::PyEval_SaveThread();
    }
    Python::attach(|py| -> PyResult<()> {
        let sys = py.import("sys")?;
        let search = sys.getattr("path")?.cast_into::<PyList>()?;
        for path in paths {
            search.append(
                path.to_str()
                    .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("invalid path"))?,
            )?;
        }
        // Load helpers before accepting registrations or invoking application code.
        py.import("inspect")?;
        Ok(())
    })
    .map_err(|_| PythonHookError::Initialization)
}

fn worker(mut receiver: mpsc::Receiver<Job>, stopped: watch::Receiver<bool>, limit: usize) {
    let mut callbacks = Vec::<Registered>::new();
    let mut next_module = 0;
    while let Some(job) = receiver.blocking_recv() {
        if *stopped.borrow() {
            break;
        }
        if job.reply.is_closed() || Instant::now() >= job.deadline {
            continue;
        }
        let result = Python::attach(|py| {
            if job.reply.is_closed() || Instant::now() >= job.deadline {
                return Err(PythonHookError::TimedOut);
            }
            match job.operation {
                Operation::Register {
                    path,
                    callback,
                    params,
                } => {
                    let module_id = next_module;
                    next_module += 1;
                    let function = load(py, &path, &callback, module_id, limit, &params)?;
                    let id = callbacks.len();
                    callbacks.push(Registered { function, params });
                    Ok(Reply::Registered(id))
                }
                Operation::Call { id, request } => {
                    let registered = &callbacks[id];
                    let request =
                        to_python(py, &request).map_err(|_| PythonHookError::Execution)?;
                    let params = to_python(py, &registered.params)
                        .map_err(|_| PythonHookError::Execution)?;
                    let result = registered
                        .function
                        .call1(py, (request, params))
                        .map_err(|_| PythonHookError::Execution)?;
                    let result = result.bind(py);
                    let inspect = py
                        .import("inspect")
                        .map_err(|_| PythonHookError::Execution)?;
                    if inspect
                        .call_method1("iscoroutine", (result,))
                        .and_then(|value| value.extract::<bool>())
                        .map_err(|_| PythonHookError::Execution)?
                    {
                        let _ = result.call_method0("close");
                        return Err(PythonHookError::InvalidOutput);
                    }
                    from_python(result, 0, &mut Counter { bytes: 0, limit }).map(Reply::Value)
                }
            }
        });
        let result = if Instant::now() >= job.deadline {
            Err(PythonHookError::TimedOut)
        } else {
            result
        };
        let _ = job.reply.send(result);
    }
}

fn load(
    py: Python<'_>,
    path: &Path,
    callback: &str,
    id: usize,
    limit: usize,
    params: &Value,
) -> Result<Py<PyAny>, PythonHookError> {
    let mut source = String::new();
    File::open(path)
        .and_then(|file| file.take(limit as u64 + 1).read_to_string(&mut source))
        .map_err(|_| PythonHookError::ReadScript)?;
    if source
        .len()
        .saturating_add(payload_size(params, limit)?)
        .saturating_add(path.as_os_str().len())
        .saturating_add(callback.len())
        > limit
    {
        return Err(PythonHookError::PayloadTooLarge);
    }
    let source = CString::new(source).map_err(|_| PythonHookError::Registration)?;
    let filename = CString::new(path.to_str().ok_or(PythonHookError::Registration)?)
        .map_err(|_| PythonHookError::Registration)?;
    let name = CString::new(format!("_yasp_hook_{id}")).unwrap();
    let module = PyModule::from_code(py, &source, &filename, &name)
        .map_err(|_| PythonHookError::Registration)?;
    let function = module
        .getattr(callback)
        .map_err(|_| PythonHookError::Registration)?;
    let inspect = py
        .import("inspect")
        .map_err(|_| PythonHookError::Registration)?;
    if !function.is_callable()
        || ["iscoroutinefunction", "isasyncgenfunction"]
            .iter()
            .any(|method| {
                inspect
                    .call_method1(*method, (&function,))
                    .and_then(|value| value.extract::<bool>())
                    .unwrap_or(true)
            })
    {
        return Err(PythonHookError::Registration);
    }
    Ok(function.unbind())
}

fn to_python<'py>(py: Python<'py>, value: &Value) -> PyResult<Py<PyAny>> {
    match value {
        Value::Null => Ok(py.None()),
        Value::Bool(value) => value.into_py_any(py),
        Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                value.into_py_any(py)
            } else if let Some(value) = value.as_u64() {
                value.into_py_any(py)
            } else {
                value.as_f64().unwrap().into_py_any(py)
            }
        }
        Value::String(value) => value.into_py_any(py),
        Value::Array(values) => {
            let list = PyList::empty(py);
            for value in values {
                list.append(to_python(py, value)?)?;
            }
            Ok(list.into_any().unbind())
        }
        Value::Object(values) => {
            let dict = PyDict::new(py);
            for (key, value) in values {
                dict.set_item(key, to_python(py, value)?)?;
            }
            Ok(dict.into_any().unbind())
        }
    }
}

fn from_python(
    value: &Bound<'_, PyAny>,
    depth: usize,
    count: &mut Counter,
) -> Result<Value, PythonHookError> {
    if depth > MAX_DEPTH {
        return Err(PythonHookError::PayloadTooLarge);
    }
    let invalid = |_| PythonHookError::InvalidOutput;
    let scalar = if value.is_none() {
        Value::Null
    } else if value.is_exact_instance_of::<PyBool>() {
        Value::Bool(value.extract().map_err(invalid)?)
    } else if value.is_exact_instance_of::<PyInt>() {
        if let Ok(value) = value.extract::<i64>() {
            Value::from(value)
        } else {
            Value::from(value.extract::<u64>().map_err(invalid)?)
        }
    } else if value.is_exact_instance_of::<PyFloat>() {
        Value::Number(
            Number::from_f64(value.extract().map_err(invalid)?)
                .ok_or(PythonHookError::InvalidOutput)?,
        )
    } else if value.is_exact_instance_of::<PyString>() {
        let text = value
            .cast::<PyString>()
            .map_err(|_| PythonHookError::InvalidOutput)?;
        if text.len().map_err(invalid)? > count.remaining() {
            return Err(PythonHookError::PayloadTooLarge);
        }
        let text = text.to_str().map_err(invalid)?;
        count.json(text)?;
        return Ok(Value::String(text.to_owned()));
    } else if value.is_exact_instance_of::<PyList>() {
        let list = value
            .cast::<PyList>()
            .map_err(|_| PythonHookError::InvalidOutput)?;
        count.charge(2)?;
        if list.len() > count.remaining() {
            return Err(PythonHookError::PayloadTooLarge);
        }
        let mut values = Vec::new();
        for (index, value) in list.iter().enumerate() {
            if index != 0 {
                count.charge(1)?;
            }
            values.push(from_python(&value, depth + 1, count)?);
        }
        return Ok(Value::Array(values));
    } else if value.is_exact_instance_of::<PyDict>() {
        let dict = value
            .cast::<PyDict>()
            .map_err(|_| PythonHookError::InvalidOutput)?;
        count.charge(2)?;
        if dict.len() > count.remaining() {
            return Err(PythonHookError::PayloadTooLarge);
        }
        let mut values = Map::new();
        for (index, (key, value)) in dict.iter().enumerate() {
            if index != 0 {
                count.charge(1)?;
            }
            if !key.is_exact_instance_of::<PyString>() {
                return Err(PythonHookError::InvalidOutput);
            }
            let Value::String(key) = from_python(&key, depth + 1, count)? else {
                unreachable!()
            };
            count.charge(1)?;
            values.insert(key, from_python(&value, depth + 1, count)?);
        }
        return Ok(Value::Object(values));
    } else {
        return Err(PythonHookError::InvalidOutput);
    };
    count.json(&scalar)?;
    Ok(scalar)
}

fn payload_size(value: &Value, limit: usize) -> Result<usize, PythonHookError> {
    fn check_depth(value: &Value, depth: usize) -> Result<(), PythonHookError> {
        if depth > MAX_DEPTH {
            return Err(PythonHookError::PayloadTooLarge);
        }
        match value {
            Value::Array(values) => {
                for value in values {
                    check_depth(value, depth + 1)?;
                }
            }
            Value::Object(values) => {
                for value in values.values() {
                    check_depth(value, depth + 1)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    check_depth(value, 0)?;
    let mut count = Counter { bytes: 0, limit };
    count.json(value)?;
    Ok(count.bytes)
}

struct Counter {
    bytes: usize,
    limit: usize,
}

impl Counter {
    fn remaining(&self) -> usize {
        self.limit - self.bytes
    }
    fn charge(&mut self, bytes: usize) -> Result<(), PythonHookError> {
        if bytes > self.remaining() {
            return Err(PythonHookError::PayloadTooLarge);
        }
        self.bytes += bytes;
        Ok(())
    }
    fn json(&mut self, value: &(impl serde::Serialize + ?Sized)) -> Result<(), PythonHookError> {
        serde_json::to_writer(self, value).map_err(|_| PythonHookError::PayloadTooLarge)
    }
}

impl Write for Counter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.charge(bytes.len())
            .map_err(|_| io::Error::other("payload limit"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
