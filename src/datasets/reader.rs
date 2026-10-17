//! Read-only registry handles. Invocation scopes bound retained hook access.

use super::{DatasetError, store};
use crate::hooks::payload;
use redis::Value as RedisValue;
use serde_json::{Map, Value};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};

const MAX_CALLS: usize = 32;
const MAX_FIELDS: usize = 64;

/// Cloneable access to configured datasets. Values are always fetched from Redis.
#[derive(Clone)]
pub struct DatasetReader {
    pub(super) store: Arc<store::Store>,
    invocation: Option<Arc<Invocation>>,
}

struct Invocation {
    closed: watch::Sender<bool>,
    calls: AtomicUsize,
    bytes: AtomicUsize,
    budget: Option<Arc<Semaphore>>,
    reservations: Mutex<Vec<OwnedSemaphorePermit>>,
}

/// Closes a hook's lookup scope on completion, cancellation, or timeout.
pub(crate) struct DatasetReadGuard(Arc<Invocation>);

impl Drop for DatasetReadGuard {
    fn drop(&mut self) {
        self.0.closed.send_replace(true);
    }
}

impl DatasetReader {
    pub(super) fn new(store: Arc<store::Store>) -> Self {
        Self {
            store,
            invocation: None,
        }
    }

    pub(crate) fn invocation(&self) -> (Self, DatasetReadGuard) {
        self.scoped(None)
    }

    #[cfg(feature = "python")]
    pub(crate) fn invocation_with_budget(
        &self,
        budget: Arc<Semaphore>,
    ) -> (Self, DatasetReadGuard) {
        self.scoped(Some(budget))
    }

    fn scoped(&self, budget: Option<Arc<Semaphore>>) -> (Self, DatasetReadGuard) {
        let (closed, _) = watch::channel(false);
        let invocation = Arc::new(Invocation {
            closed,
            calls: AtomicUsize::new(0),
            bytes: AtomicUsize::new(0),
            budget,
            reservations: Mutex::new(Vec::new()),
        });
        (
            Self {
                store: self.store.clone(),
                invocation: Some(invocation.clone()),
            },
            DatasetReadGuard(invocation),
        )
    }

    pub(crate) fn lookup_budget_bytes(&self) -> usize {
        self.store.settings.max_lookup_bytes * MAX_CALLS
    }

    #[cfg(feature = "python")]
    pub(crate) fn close_invocation(&self) {
        if let Some(invocation) = &self.invocation {
            invocation.closed.send_replace(true);
            // Worker completion releases fetched-data reservations even if Python
            // retained the expired helper. Caller cancellation only closes it.
            invocation.reservations.lock().unwrap().clear();
        }
    }

    /// Fetch one top-level field. A stored JSON null is Some(Value::Null).
    pub async fn get(&self, name: &str, key: &str) -> Result<Option<Value>, DatasetError> {
        Ok(self.get_many(name, &[key]).await?.remove(key))
    }

    /// Fetch fields atomically from one dataset; missing fields are omitted.
    pub async fn get_many(
        &self,
        name: &str,
        keys: &[&str],
    ) -> Result<Map<String, Value>, DatasetError> {
        if keys.len() > MAX_FIELDS || keys.iter().any(|key| key.is_empty() || key.len() > 256) {
            return Err(DatasetError::Limit);
        }
        let mut keys = keys.to_vec();
        keys.sort_unstable();
        keys.dedup();
        self.lookup(name, Some(&keys)).await
    }

    /// Read an entire small dataset within the same field and byte limits.
    pub async fn read(&self, name: &str) -> Result<Map<String, Value>, DatasetError> {
        self.lookup(name, None).await
    }

    async fn lookup(
        &self,
        name: &str,
        keys: Option<&[&str]>,
    ) -> Result<Map<String, Value>, DatasetError> {
        if !self.store.settings.datasets.contains_key(name) {
            return Err(DatasetError::UnknownDataset);
        }
        let mut closed = if let Some(invocation) = &self.invocation {
            if *invocation.closed.borrow() {
                return Err(DatasetError::Closed);
            }
            if invocation.calls.fetch_add(1, Ordering::Relaxed) >= MAX_CALLS {
                return Err(DatasetError::Limit);
            }
            Some(invocation.closed.subscribe())
        } else {
            None
        };
        let operation = self.read_from_redis(name, keys);
        if let Some(closed) = &mut closed {
            tokio::select! {
                biased;
                _ = closed.wait_for(|closed| *closed) => Err(DatasetError::Closed),
                result = operation => result,
            }
        } else {
            operation.await
        }
    }

    async fn read_from_redis(
        &self,
        name: &str,
        keys: Option<&[&str]>,
    ) -> Result<Map<String, Value>, DatasetError> {
        let limit = self.store.settings.max_lookup_bytes;
        let _permit = self
            .store
            .budget
            .clone()
            .try_acquire_many_owned((limit * 3) as u32)
            .map_err(|_| {
                if self.store.budget.is_closed() {
                    DatasetError::Closed
                } else {
                    DatasetError::Limit
                }
            })?;
        let mut cmd = redis::cmd("EVAL");
        cmd.arg(store::READ)
            .arg(2)
            .arg(self.store.key(name, "state"))
            .arg(self.store.key(name, "data"))
            .arg(if keys.is_some() { "fields" } else { "all" })
            .arg(limit);
        if let Some(keys) = keys {
            for key in keys {
                cmd.arg(key);
            }
        }
        let response = self.store.read_command(cmd).await?;
        let RedisValue::Array(values) = response else {
            return Err(DatasetError::InvalidData);
        };
        match values.as_slice() {
            [RedisValue::Int(-1)] => return Err(DatasetError::Unavailable),
            [RedisValue::Int(-2)] => return Err(DatasetError::Limit),
            _ => {}
        }
        if values.len() % 2 != 0 || values.len() > MAX_FIELDS * 2 {
            return Err(DatasetError::InvalidData);
        }
        let mut data = Map::new();
        let mut bytes = 2usize;
        for pair in values.chunks_exact(2) {
            let [RedisValue::BulkString(key), RedisValue::BulkString(raw)] = pair else {
                return Err(DatasetError::InvalidData);
            };
            bytes = bytes
                .saturating_add(key.len())
                .saturating_add(raw.len())
                .saturating_add(4);
            if bytes > limit || key.is_empty() || key.len() > 256 {
                return Err(DatasetError::Limit);
            }
            let key = std::str::from_utf8(key).map_err(|_| DatasetError::InvalidData)?;
            let value: Value =
                serde_json::from_slice(raw).map_err(|_| DatasetError::InvalidData)?;
            payload::payload_size(&value, limit).map_err(|_| DatasetError::InvalidData)?;
            if data.insert(key.to_owned(), value).is_some() {
                return Err(DatasetError::InvalidData);
            }
        }
        if let Some(invocation) = &self.invocation {
            let previous = invocation.bytes.fetch_add(bytes, Ordering::Relaxed);
            if previous.saturating_add(bytes) > self.lookup_budget_bytes() {
                return Err(DatasetError::Limit);
            }
            let mut reservations = invocation.reservations.lock().unwrap();
            if *invocation.closed.borrow() {
                return Err(DatasetError::Closed);
            }
            if let Some(budget) = &invocation.budget {
                let permit = budget
                    .clone()
                    .try_acquire_many_owned(bytes as u32)
                    .map_err(|_| DatasetError::Limit)?;
                reservations.push(permit);
            }
        }
        Ok(data)
    }
}
