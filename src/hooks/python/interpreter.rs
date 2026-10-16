//! Isolated interpreter setup and the dedicated registration/invocation worker.
//! Application imports and Python objects stay on this thread.

use super::{
    Job, Operation, PythonHookError, Reply,
    conversion::{from_python, to_python},
};
use crate::hooks::payload::{Counter, payload_size};
use pyo3::{
    ffi,
    prelude::*,
    types::{PyCFunction, PyList, PyModule},
};
use serde_json::{Map, Value};
use std::{
    ffi::CString,
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, Ordering},
};
use tokio::{
    sync::{mpsc, watch},
    time::Instant,
};

static INITIALIZED: AtomicBool = AtomicBool::new(false);

struct Registered {
    function: Py<PyAny>,
    params: Value,
}

pub(super) fn initialize(paths: &[PathBuf]) -> Result<(), PythonHookError> {
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
        let extractors = PyModule::new(py, "yasp_extractors")?;
        use crate::hooks::builtin::extractors as builtin;
        type Extractor = fn(&str) -> Result<Option<Map<String, Value>>, builtin::ExtractionError>;
        for (name, function) in [
            ("metabase", builtin::metabase as Extractor),
            ("superset", builtin::superset as Extractor),
            ("redash", builtin::redash as Extractor),
        ] {
            let callback = PyCFunction::new_closure(
                py,
                None,
                None,
                move |args, kwargs| -> PyResult<Py<PyAny>> {
                    if kwargs.is_some_and(|kwargs| !kwargs.is_empty()) {
                        return Err(pyo3::exceptions::PyTypeError::new_err(
                            "extractor expects one SQL string",
                        ));
                    }
                    let (sql,): (String,) = args.extract()?;
                    match function(&sql) {
                        Ok(metadata) => to_python(
                            args.py(),
                            &metadata.map(Value::Object).unwrap_or(Value::Null),
                        ),
                        Err(builtin::ExtractionError::NotImplemented) => {
                            Err(pyo3::exceptions::PyNotImplementedError::new_err(
                                "BI metadata extraction is not implemented",
                            ))
                        }
                    }
                },
            )?;
            extractors.add(name, callback)?;
        }
        sys.getattr("modules")?
            .set_item("yasp_extractors", extractors)?;
        Ok(())
    })
    .map_err(|_| PythonHookError::Initialization)
}

pub(super) fn worker(
    mut receiver: mpsc::Receiver<Job>,
    stopped: watch::Receiver<bool>,
    limit: usize,
) {
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
