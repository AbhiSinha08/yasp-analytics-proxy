//! External trusted Python callbacks, isolated imports, and bounded invocation.
//!
//! One runtime may be created per process. Clone it to register more callbacks.
//! Python owns module state; each call receives fresh JSON-compatible objects.

mod conversion;
mod interpreter;

use crate::{
    config::PythonSettings,
    hooks::payload::{PayloadError, payload_size},
};
use interpreter::{initialize, worker};
use serde_json::{Map, Value};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};
use thiserror::Error;
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
