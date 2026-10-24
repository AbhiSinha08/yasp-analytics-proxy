//! External trusted Python callbacks, isolated imports, and bounded invocation.
//!
//! One runtime may be created per process. Clone it to register more callbacks.
//! Python owns module state; calls receive fresh values and optional dataset reads.

mod conversion;
mod datasets;
mod interpreter;
pub(crate) mod result;

use crate::{
    config::PythonSettings,
    datasets::DatasetReader,
    hooks::payload::{PayloadError, payload_size},
};
use interpreter::{initialize, worker};
use serde_json::{Map, Value};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex, atomic::AtomicUsize},
    time::Duration,
};
use thiserror::Error;
use tokio::runtime::Handle;
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch},
    time::{Instant, timeout_at},
};

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

impl From<PayloadError> for PythonHookError {
    fn from(_: PayloadError) -> Self {
        Self::PayloadTooLarge
    }
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
    loader_sender: Mutex<Option<mpsc::Sender<Job>>>,
    stopped: watch::Sender<bool>,
    bytes: Arc<Semaphore>,
    loader_bytes: Option<Arc<Semaphore>>,
    loader_limit: Option<usize>,
    loader_input_limit: usize,
    loader_queue_bytes: usize,
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
    loader: bool,
    payload_limit: usize,
    input_limit: usize,
    timeout: Duration,
}

enum Operation {
    ProcessResult {
        id: usize,
        request: Arc<result::PreparedRequest>,
        batch: crate::hooks::ResultBatch,
        datasets: Option<DatasetReader>,
        runtime: Handle,
    },
    Register {
        path: PathBuf,
        callback: String,
        params: Value,
        limit: usize,
        script_limit: usize,
    },
    Call {
        id: usize,
        request: Value,
        datasets: Option<DatasetReader>,
        runtime: Handle,
    },
}

enum Reply {
    Batch(crate::hooks::ResultBatch),
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

impl PythonRuntime {
    /// Initialize isolated CPython on a dedicated thread before serving requests.
    /// Search paths must be absolute; base/user site-packages are not imported.
    pub async fn new(settings: PythonSettings) -> Result<Self, PythonHookError> {
        Self::initialize(settings, None).await
    }

    /// Initialize Python with independent bounded capacity for dataset loader jobs.
    pub async fn new_with_dataset_limit(
        settings: PythonSettings,
        max_dataset_bytes: usize,
    ) -> Result<Self, PythonHookError> {
        Self::initialize(settings, Some(max_dataset_bytes)).await
    }

    async fn initialize(
        mut settings: PythonSettings,
        max_dataset_bytes: Option<usize>,
    ) -> Result<Self, PythonHookError> {
        if max_dataset_bytes == Some(0) {
            return Err(PythonHookError::InvalidSettings);
        }
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
        let (loader_sender, loader_receiver) = if max_dataset_bytes.is_some() {
            let (sender, receiver) = mpsc::channel(1);
            (Some(sender), Some(receiver))
        } else {
            (None, None)
        };
        let (stopped, stop_receiver) = watch::channel(false);
        let loader_limit = max_dataset_bytes;
        let loader_input_limit =
            max_dataset_bytes.map_or(0, |limit| limit.max(settings.max_payload_bytes));
        let loader_queue_bytes = if max_dataset_bytes.is_some() {
            loader_input_limit
                .checked_mul(4)
                .filter(|bytes| *bytes <= u32::MAX as usize && *bytes <= Semaphore::MAX_PERMITS)
                .ok_or(PythonHookError::InvalidSettings)?
        } else {
            0
        };
        let (ready, initialized) = oneshot::channel();
        let runtime = Self(Arc::new(Inner {
            sender: Mutex::new(Some(sender)),
            loader_sender: Mutex::new(loader_sender),
            stopped,
            bytes: Arc::new(Semaphore::new(settings.queue_max_bytes)),
            loader_bytes: max_dataset_bytes.map(|_| Arc::new(Semaphore::new(loader_queue_bytes))),
            loader_limit,
            loader_input_limit,
            loader_queue_bytes,
            settings: settings.clone(),
        }));
        let deadline = Instant::now() + runtime.timeout();
        let module_ids = Arc::new(AtomicUsize::new(0));
        std::thread::Builder::new()
            .name("yasp-python".into())
            .spawn(move || {
                let mut result = initialize(&settings.search_paths);
                if result.is_ok()
                    && let Some(loader_receiver) = loader_receiver
                {
                    let loader_stop = stop_receiver.clone();
                    let loader_ids = module_ids.clone();
                    if std::thread::Builder::new()
                        .name("yasp-python-loader".into())
                        .spawn(move || {
                            interpreter::loader_worker(loader_receiver, loader_stop, loader_ids);
                        })
                        .is_err()
                    {
                        result = Err(PythonHookError::Initialization);
                    }
                }
                let success = result.is_ok();
                let _ = ready.send(result);
                if success {
                    worker(
                        receiver,
                        stop_receiver,
                        settings.max_payload_bytes,
                        module_ids,
                    );
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
            limit: self.0.settings.max_payload_bytes,
            script_limit: self.0.settings.max_payload_bytes,
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
                loader: false,
                payload_limit: self.0.settings.max_payload_bytes,
                input_limit: self.0.settings.max_payload_bytes,
                timeout: self.timeout(),
            }),
            _ => unreachable!("registration returns a handle"),
        }
    }

    /// Stop admission and wake waiters; never join an executing Python callback.
    pub fn shutdown(&self) {
        self.0.stopped.send_replace(true);
        self.0.sender.lock().unwrap().take();
        self.0.loader_sender.lock().unwrap().take();
    }

    /// Register a bounded loader callback on the separate dataset worker.
    pub async fn register_loader(
        &self,
        path: &Path,
        callback: &str,
        params: Map<String, Value>,
        max_dataset_bytes: usize,
        timeout: Duration,
    ) -> Result<PythonCallback, PythonHookError> {
        if !path.is_absolute()
            || callback.trim().is_empty()
            || callback.contains('\0')
            || max_dataset_bytes == 0
            || timeout.is_zero()
        {
            return Err(PythonHookError::Registration);
        }
        let Some(loader_limit) = self.0.loader_limit else {
            return Err(PythonHookError::InvalidSettings);
        };
        if max_dataset_bytes > loader_limit {
            return Err(PythonHookError::InvalidSettings);
        }
        let params = Value::Object(params);
        let params_bytes = payload_size(&params, self.0.loader_input_limit)?;
        if path
            .as_os_str()
            .len()
            .saturating_add(callback.len())
            .saturating_add(params_bytes)
            > self.0.loader_input_limit
        {
            return Err(PythonHookError::PayloadTooLarge);
        }
        let deadline = timeout;
        let operation = Operation::Register {
            path: path.to_owned(),
            callback: callback.to_owned(),
            params,
            limit: max_dataset_bytes,
            script_limit: self.0.loader_input_limit,
        };
        let reservation = self.0.loader_input_limit;
        let reply = self
            .submit_on(operation, reservation, true, deadline)
            .await?;
        match reply {
            Reply::Registered(id) => Ok(PythonCallback {
                runtime: self.clone(),
                id,
                params_bytes,
                loader: true,
                payload_limit: max_dataset_bytes,
                input_limit: self.0.loader_input_limit,
                timeout,
            }),
            _ => unreachable!("loader registration returns a handle"),
        }
    }

    fn timeout(&self) -> Duration {
        Duration::from_millis(u64::from(self.0.settings.execution_timeout_ms))
    }

    async fn submit(&self, operation: Operation, bytes: usize) -> Result<Reply, PythonHookError> {
        self.submit_on(operation, bytes, false, self.timeout())
            .await
    }

    async fn submit_on(
        &self,
        operation: Operation,
        bytes: usize,
        loader: bool,
        timeout: Duration,
    ) -> Result<Reply, PythonHookError> {
        let byte_limit = if loader {
            self.0.loader_queue_bytes
        } else {
            self.0.settings.queue_max_bytes
        };
        if bytes > byte_limit || bytes > u32::MAX as usize {
            return Err(PythonHookError::Saturated);
        }
        let deadline = Instant::now() + timeout;
        let mut stopped = self.0.stopped.subscribe();
        if *stopped.borrow() {
            return Err(PythonHookError::Closed);
        }
        let permit = (if loader {
            self.0
                .loader_bytes
                .as_ref()
                .ok_or(PythonHookError::Closed)?
        } else {
            &self.0.bytes
        })
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
        (if loader {
            &self.0.loader_sender
        } else {
            &self.0.sender
        })
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
        self.call_inner(request, None).await
    }

    /// Invoke function(request, params) with a bounded, invocation-scoped dataset helper.
    pub async fn call_with_datasets(
        &self,
        request: Value,
        datasets: Option<DatasetReader>,
    ) -> Result<Value, PythonHookError> {
        self.call_inner(request, datasets).await
    }

    async fn call_inner(
        &self,
        request: Value,
        datasets: Option<DatasetReader>,
    ) -> Result<Value, PythonHookError> {
        let limit = self.payload_limit;
        let input_bytes = payload_size(&request, self.input_limit)?
            .checked_add(self.params_bytes)
            .filter(|bytes| *bytes <= self.input_limit)
            .ok_or(PythonHookError::PayloadTooLarge)?;
        let bytes = input_bytes;
        let (datasets, _guard) = datasets
            .map(|reader| reader.invocation_with_budget(self.runtime.0.bytes.clone()))
            .map_or((None, None), |(reader, guard)| (Some(reader), Some(guard)));
        let runtime = Handle::try_current().map_err(|_| PythonHookError::Execution)?;
        let reservation = if self.loader {
            bytes
                .checked_add(limit)
                .ok_or(PythonHookError::PayloadTooLarge)?
        } else {
            bytes
        };
        let result = self
            .runtime
            .submit_on(
                Operation::Call {
                    id: self.id,
                    request,
                    datasets,
                    runtime,
                },
                reservation,
                self.loader,
                self.timeout,
            )
            .await?;
        match result {
            Reply::Value(value) => Ok(value),
            _ => unreachable!("invocation returns a value"),
        }
    }
}
