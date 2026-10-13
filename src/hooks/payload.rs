//! Encoded JSON bounds shared by application context and Python callbacks.

use serde_json::Value;
use std::io::{self, Write};

pub(super) const MAX_DEPTH: usize = 64;

pub(super) struct PayloadError;

pub(super) fn payload_size(value: &Value, limit: usize) -> Result<usize, PayloadError> {
    fn check_depth(value: &Value, depth: usize) -> Result<(), PayloadError> {
        if depth > MAX_DEPTH {
            return Err(PayloadError);
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

pub(super) struct Counter {
    pub(super) bytes: usize,
    pub(super) limit: usize,
}

impl Counter {
    pub(super) fn remaining(&self) -> usize {
        self.limit - self.bytes
    }

    pub(super) fn charge(&mut self, bytes: usize) -> Result<(), PayloadError> {
        if bytes > self.remaining() {
            return Err(PayloadError);
        }
        self.bytes += bytes;
        Ok(())
    }

    pub(super) fn json(
        &mut self,
        value: &(impl serde::Serialize + ?Sized),
    ) -> Result<(), PayloadError> {
        serde_json::to_writer(self, value).map_err(|_| PayloadError)
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
