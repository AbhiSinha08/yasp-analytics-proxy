//! Plain Python/JSON conversion with exact scalar types and bounded output.

use super::PythonHookError;
use crate::hooks::payload::{Counter, MAX_DEPTH};
use pyo3::{
    IntoPyObjectExt,
    prelude::*,
    types::{PyBool, PyDict, PyFloat, PyInt, PyList, PyString},
};
use serde_json::{Map, Number, Value};

pub(super) fn to_python<'py>(py: Python<'py>, value: &Value) -> PyResult<Py<PyAny>> {
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

pub(super) fn from_python(
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
