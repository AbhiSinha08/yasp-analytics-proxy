//! Build the invocation-scoped Python dataset helper without exposing Rust types.

use crate::datasets::{DatasetError, DatasetReader};
use pyo3::{
    exceptions::{PyRuntimeError, PyTypeError, PyValueError},
    prelude::*,
    types::{PyCFunction, PyDict, PyList, PyString, PyTuple, PyType},
};
use serde_json::{Map, Value};
use tokio::runtime::Handle;

use super::{PythonHookError, conversion::to_python as json_to_python};

fn dataset_error(py: Python<'_>, class: &Py<PyType>, error: DatasetError) -> PyErr {
    PyErr::from_type(class.bind(py).clone(), error.to_string())
}

fn extract_keys(args: &Bound<'_, PyTuple>) -> PyResult<(String, Vec<String>)> {
    if args.len() != 2 {
        return Err(pyo3::exceptions::PyTypeError::new_err(
            "get_many expects dataset and keys",
        ));
    }
    let name = args.get_item(0)?.extract::<String>()?;
    let keys = args.get_item(1)?;
    if !keys.is_exact_instance_of::<PyList>() && !keys.is_exact_instance_of::<PyTuple>() {
        return Err(PyTypeError::new_err(
            "get_many keys must be a list or tuple",
        ));
    }
    let length = keys.len()?;
    if length > 64 {
        return Err(PyValueError::new_err(
            "dataset lookup exceeds configured limits",
        ));
    }
    let mut result = Vec::with_capacity(length);
    let mut push_key = |key: Bound<'_, PyAny>| -> PyResult<()> {
        if !key.is_exact_instance_of::<PyString>() {
            return Err(PyTypeError::new_err("dataset keys must be strings"));
        }
        let key = key.cast_into::<PyString>()?;
        let text = key.to_str()?;
        if text.len() > 256 {
            return Err(PyValueError::new_err(
                "dataset lookup exceeds configured limits",
            ));
        }
        result.push(text.to_owned());
        Ok(())
    };
    if keys.is_exact_instance_of::<PyList>() {
        for key in keys.cast::<PyList>()?.iter() {
            push_key(key)?;
        }
    } else {
        for key in keys.cast::<PyTuple>()?.iter() {
            push_key(key)?;
        }
    }
    Ok((name, result))
}

pub(super) fn to_python(
    py: Python<'_>,
    reader: DatasetReader,
    runtime: Handle,
) -> Result<Py<PyAny>, PythonHookError> {
    let methods = PyDict::new(py);
    let exception_bases = PyTuple::new(py, [py.get_type::<PyRuntimeError>()])
        .map_err(|_| PythonHookError::Execution)?;
    let exception = py
        .get_type::<PyType>()
        .call1(("DatasetError", exception_bases, PyDict::new(py)))
        .and_then(|ty| ty.cast_into::<PyType>().map_err(Into::into))
        .map_err(|_| PythonHookError::Execution)?
        .unbind();
    methods
        .set_item("DatasetError", exception.bind(py))
        .map_err(|_| PythonHookError::Execution)?;
    let get_reader = reader.clone();
    let get_runtime = runtime.clone();
    let get_exception = exception.clone_ref(py);
    let get = PyCFunction::new_closure(py, None, None, move |args, kwargs| {
        if kwargs.is_some_and(|kwargs| !kwargs.is_empty()) {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "get expects dataset and key",
            ));
        }
        let (name, key): (&str, &str) = args.extract()?;
        let value = args
            .py()
            .detach(|| get_runtime.block_on(get_reader.get(name, key)))
            .map_err(|error| dataset_error(args.py(), &get_exception, error))?;
        match value {
            Some(value) => json_to_python(args.py(), &value),
            None => Ok(args.py().None()),
        }
    })
    .map_err(|_| PythonHookError::Execution)?;
    methods
        .set_item("get", get)
        .map_err(|_| PythonHookError::Execution)?;

    let many_reader = reader.clone();
    let many_runtime = runtime.clone();
    let many_exception = exception.clone_ref(py);
    let get_many = PyCFunction::new_closure(py, None, None, move |args, kwargs| {
        if kwargs.is_some_and(|kwargs| !kwargs.is_empty()) {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "get_many expects dataset and keys",
            ));
        }
        let (name, keys) = extract_keys(args)?;
        let keys = keys.iter().map(String::as_str).collect::<Vec<_>>();
        let value: Map<String, Value> = args
            .py()
            .detach(|| many_runtime.block_on(many_reader.get_many(&name, &keys)))
            .map_err(|error| dataset_error(args.py(), &many_exception, error))?;
        json_to_python(args.py(), &Value::Object(value))
    })
    .map_err(|_| PythonHookError::Execution)?;
    methods
        .set_item("get_many", get_many)
        .map_err(|_| PythonHookError::Execution)?;

    let read_runtime = runtime;
    let read_exception = exception;
    let read = PyCFunction::new_closure(py, None, None, move |args, kwargs| {
        if kwargs.is_some_and(|kwargs| !kwargs.is_empty()) {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "read expects dataset name",
            ));
        }
        let (name,): (&str,) = args.extract()?;
        let value = args
            .py()
            .detach(|| read_runtime.block_on(reader.read(name)))
            .map_err(|error| dataset_error(args.py(), &read_exception, error))?;
        json_to_python(args.py(), &Value::Object(value))
    })
    .map_err(|_| PythonHookError::Execution)?;
    methods
        .set_item("read", read)
        .map_err(|_| PythonHookError::Execution)?;

    let helper = py
        .import("types")
        .map_err(|_| PythonHookError::Execution)?
        .getattr("SimpleNamespace")
        .map_err(|_| PythonHookError::Execution)?
        .call((), Some(&methods))
        .map_err(|_| PythonHookError::Execution)?;
    Ok(helper.unbind())
}
