//! Native batches cross the worker queue without JSON cell conversion.
use super::{Operation, PythonCallback, PythonHookError, Reply, conversion::to_python};
use crate::{
    datasets::DatasetReader,
    hooks::{
        ResultBatch, ResultColumn, ResultHookError, ResultRequest, payload::payload_size,
        result::Reservation,
    },
};
use pyo3::{
    prelude::*,
    types::{PyBool, PyBytes, PyDict, PyList, PyString, PyTuple},
};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use tokio::{runtime::Handle, sync::Semaphore};

pub(crate) struct PreparedRequest {
    value: Value,
    python: Mutex<Option<Py<PyDict>>>,
    bytes: usize,
    _reservation: Reservation,
}

fn frozen(py: Python<'_>, value: &Value) -> PyResult<Py<PyAny>> {
    match value {
        Value::Object(values) => {
            let dict = PyDict::new(py);
            for (key, value) in values {
                dict.set_item(key, frozen(py, value)?)?;
            }
            Ok(py
                .import("types")?
                .getattr("MappingProxyType")?
                .call1((dict,))?
                .unbind())
        }
        Value::Array(values) => {
            let values = values
                .iter()
                .map(|value| frozen(py, value))
                .collect::<PyResult<Vec<_>>>()?;
            Ok(PyTuple::new(py, values)?.into_any().unbind())
        }
        _ => to_python(py, value),
    }
}

// Conservative allowance for owned JSON, Python containers, strings and copies.
fn metadata_bytes(value: &Value) -> usize {
    256 + match value {
        Value::Object(values) => values
            .iter()
            .map(|(key, value)| key.capacity() * 4 + 256 + metadata_bytes(value))
            .sum(),
        Value::Array(values) => values.iter().map(metadata_bytes).sum(),
        Value::String(value) => value.capacity() * 8,
        _ => 0,
    }
}

impl PythonCallback {
    pub(crate) fn result_payload_limit(&self) -> usize {
        self.payload_limit
    }

    pub(crate) fn prepare_result(
        &self,
        request: ResultRequest<'_>,
        columns: &[ResultColumn],
        budget: Arc<Semaphore>,
    ) -> Result<Arc<PreparedRequest>, PythonHookError> {
        let value = json!({
            "authenticated_user": request.authenticated_user,
            "frontend_database": request.frontend_database,
            "client_id": request.client_id, "query_id": request.query_id,
            "sql": request.query.sql(), "query_ast": request.query.serialized_ast(),
            "target": request.target, "backend_login": request.backend_login,
            "context": request.context,
            "data_contexts": { "ast": request.data_contexts.ast,
                "decision_model": request.data_contexts.decision_model },
            "columns": columns.iter().map(|c| json!({ "ordinal": c.ordinal, "name": c.name,
                "data_type": c.data_type, "is_text": c.is_text() })).collect::<Vec<_>>(),
        });
        let bytes = payload_size(&value, self.input_limit)?;
        let mut reservation = Reservation::new(budget);
        reservation
            .charge(metadata_bytes(&value))
            .map_err(|_| PythonHookError::PayloadTooLarge)?;
        Ok(Arc::new(PreparedRequest {
            value,
            python: Mutex::new(None),
            bytes,
            _reservation: reservation,
        }))
    }

    pub(crate) async fn call_result(
        &self,
        request: Arc<PreparedRequest>,
        mut batch: ResultBatch,
        datasets: Option<DatasetReader>,
    ) -> Result<ResultBatch, PythonHookError> {
        if self.loader {
            return Err(PythonHookError::InvalidOutput);
        }
        batch
            .charge_external(self.params_bytes.saturating_mul(8))
            .map_err(|_| PythonHookError::PayloadTooLarge)?;
        let bytes = request
            .bytes
            .checked_add(batch.reserved_bytes())
            .and_then(|n| n.checked_add(self.params_bytes))
            .filter(|n| *n <= self.input_limit)
            .ok_or(PythonHookError::PayloadTooLarge)?;
        let (datasets, _guard) = datasets
            .map(|reader| reader.invocation_with_budget(self.runtime.0.bytes.clone()))
            .map_or((None, None), |(reader, guard)| (Some(reader), Some(guard)));
        match self
            .runtime
            .submit_on(
                Operation::ProcessResult {
                    id: self.id,
                    request,
                    batch,
                    datasets,
                    runtime: Handle::try_current().map_err(|_| PythonHookError::Execution)?,
                },
                bytes,
                false,
                self.timeout,
            )
            .await?
        {
            Reply::Batch(batch) => Ok(batch),
            _ => unreachable!("result invocation returns a batch"),
        }
    }
}

fn operation_error(_: ResultHookError) -> PyErr {
    pyo3::exceptions::PyValueError::new_err("invalid or oversized result batch operation")
}
fn closed() -> PyErr {
    pyo3::exceptions::PyRuntimeError::new_err("result batch invocation has ended")
}

#[pyclass(name = "ResultBatch", module = "yasp", frozen)]
struct PythonBatch {
    batch: Mutex<Option<ResultBatch>>,
}

impl PythonBatch {
    fn access(&self) -> PyResult<std::sync::MutexGuard<'_, Option<ResultBatch>>> {
        self.batch
            .try_lock()
            .map_err(|_| pyo3::exceptions::PyRuntimeError::new_err("result batch is in use"))
    }
}

#[pymethods]
impl PythonBatch {
    fn __len__(&self) -> PyResult<usize> {
        Ok(self.access()?.as_ref().ok_or_else(closed)?.len())
    }
    fn column_index(&self, name: &str) -> PyResult<usize> {
        self.access()?
            .as_ref()
            .ok_or_else(closed)?
            .column_index(name)
            .map_err(operation_error)
    }
    /// Materialize only this column, retaining exact text and SQL NULLs.
    fn text_column(&self, py: Python<'_>, column: usize) -> PyResult<Py<PyList>> {
        let mut guard = self.access()?;
        let batch = guard.as_mut().ok_or_else(closed)?;
        if column >= batch.columns().len() {
            return Err(operation_error(ResultHookError::Column));
        }
        let bytes = (0..batch.len())
            .try_fold(0, |total, r| {
                batch
                    .value(r, column)
                    .map(|v| total + 80 + v.map_or(0, |v| v.len() * 4))
            })
            .map_err(operation_error)?;
        batch.charge_external(bytes).map_err(operation_error)?;
        let list = PyList::empty(py);
        for row in 0..batch.len() {
            match batch.text(row, column).map_err(operation_error)? {
                Some(value) => list.append(PyString::new(py, value))?,
                None => list.append(py.None())?,
            }
        }
        Ok(list.unbind())
    }
    fn bytes_column(&self, py: Python<'_>, column: usize) -> PyResult<Py<PyList>> {
        let mut guard = self.access()?;
        let batch = guard.as_mut().ok_or_else(closed)?;
        if column >= batch.columns().len() {
            return Err(operation_error(ResultHookError::Column));
        }
        let bytes = (0..batch.len())
            .try_fold(0, |total, r| {
                batch
                    .value(r, column)
                    .map(|v| total + 80 + v.map_or(0, <[u8]>::len))
            })
            .map_err(operation_error)?;
        batch.charge_external(bytes).map_err(operation_error)?;
        let list = PyList::empty(py);
        for row in 0..batch.len() {
            match batch.value(row, column).map_err(operation_error)? {
                Some(value) => list.append(PyBytes::new(py, value))?,
                None => list.append(py.None())?,
            }
        }
        Ok(list.unbind())
    }
    fn set_text(&self, row: usize, column: usize, value: &str) -> PyResult<()> {
        self.access()?
            .as_mut()
            .ok_or_else(closed)?
            .set_text(row, column, value)
            .map_err(operation_error)
    }
    fn set_null(&self, row: usize, column: usize) -> PyResult<()> {
        self.access()?
            .as_mut()
            .ok_or_else(closed)?
            .set_null(row, column)
            .map_err(operation_error)
    }
    fn replace_text_column(&self, column: usize, values: &Bound<'_, PyList>) -> PyResult<()> {
        let mut guard = self.access()?;
        let batch = guard.as_mut().ok_or_else(closed)?;
        if values.len() != batch.len() {
            return Err(operation_error(ResultHookError::InvalidOutput));
        }
        for row in 0..batch.len() {
            let value = values.get_item(row)?;
            if value.is_none() {
                batch.set_null(row, column).map_err(operation_error)?;
            } else {
                batch
                    .set_text(row, column, value.cast::<PyString>()?.to_str()?)
                    .map_err(operation_error)?;
            }
        }
        Ok(())
    }
    fn retain_rows(&self, keep: &Bound<'_, PyList>) -> PyResult<()> {
        let mut guard = self.access()?;
        let batch = guard.as_mut().ok_or_else(closed)?;
        if keep.len() != batch.len() {
            return Err(operation_error(ResultHookError::InvalidOutput));
        }
        batch
            .charge_external(batch.len())
            .map_err(operation_error)?;
        let keep = (0..batch.len())
            .map(|i| keep.get_item(i)?.cast::<PyBool>()?.extract::<bool>())
            .collect::<PyResult<Vec<_>>>()?;
        batch.retain_rows(&keep).map_err(operation_error)
    }
}

pub(super) fn invoke(
    py: Python<'_>,
    function: &Py<PyAny>,
    params: &Value,
    request: &PreparedRequest,
    batch: ResultBatch,
    datasets: Option<DatasetReader>,
    runtime: Handle,
) -> Result<ResultBatch, PythonHookError> {
    let result = (|| -> PyResult<_> {
        let mut cached = request.python.lock().unwrap();
        if cached.is_none() {
            let dict = PyDict::new(py);
            for (key, value) in request
                .value
                .as_object()
                .expect("result metadata is an object")
            {
                dict.set_item(key, frozen(py, value)?)?;
            }
            *cached = Some(dict.unbind());
        }
        let dict = cached.as_ref().unwrap().bind(py).copy()?;
        drop(cached);
        if let Some(datasets) = datasets {
            let helper = super::datasets::to_python(py, datasets, runtime).map_err(|_| {
                pyo3::exceptions::PyRuntimeError::new_err("dataset helper unavailable")
            })?;
            dict.set_item("datasets", helper)?;
        }
        let request = py
            .import("types")?
            .getattr("MappingProxyType")?
            .call1((dict,))?;
        Ok((request, to_python(py, params)?))
    })()
    .map_err(|_| PythonHookError::Execution)?;
    let native = Py::new(
        py,
        PythonBatch {
            batch: Mutex::new(Some(batch)),
        },
    )
    .map_err(|_| PythonHookError::Execution)?;
    let returned = function.call1(py, (result.0, native.clone_ref(py), result.1));
    // Invalidate even retained views on every exit; executing jobs retain storage
    // and byte permits until they actually return, including after a timeout.
    let batch = native
        .borrow(py)
        .batch
        .lock()
        .unwrap()
        .take()
        .expect("invocation owns its batch");
    let returned = returned.map_err(|_| PythonHookError::Execution)?;
    if !returned.bind(py).is_none() {
        if let Ok(true) = py
            .import("inspect")
            .and_then(|m| m.call_method1("iscoroutine", (returned.bind(py),)))
            .and_then(|v| v.extract::<bool>())
        {
            let _ = returned.bind(py).call_method0("close");
        }
        return Err(PythonHookError::InvalidOutput);
    }
    Ok(batch)
}
