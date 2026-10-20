//! Shared, bounded result caching for every backend and pgwire frontend.

mod classification;
mod execution;
mod response;
pub(crate) use execution::execute;

use crate::{
    config::CacheSettings,
    hooks::{CacheEligibility, SelectionRequest},
};
use bytes::{Buf, Bytes, BytesMut};
use redis::{AsyncConnectionConfig, Client, Cmd, Value, aio::MultiplexedConnection};
use std::{io, sync::Arc, time::Duration};
use tokio::{
    sync::{Mutex, OwnedSemaphorePermit, Semaphore},
    time::timeout,
};

const MAGIC: &[u8] = b"YASP\x01";
const REDIS_OPERATION_TIMEOUT: Duration = Duration::from_millis(100);
pub(crate) const HEADER_BYTES: usize = MAGIC.len() + 32;

/// One shared cache per host. URLs and Redis errors are never included in logs.
pub struct RedisCache {
    settings: CacheSettings,
    client: Client,
    connection: Mutex<Option<MultiplexedConnection>>,
    budget: Arc<Semaphore>,
    eligibility: Option<CacheEligibility>,
}

pub(crate) struct CacheBuffer {
    pub(crate) bytes: BytesMut,
    pub(crate) association: Option<[u8; 32]>,
    _permit: OwnedSemaphorePermit,
}

impl RedisCache {
    /// Validate transport configuration and certificate material without connecting to Redis.
    /// An unavailable cache is retried lazily and never prevents database execution.
    pub fn new(
        settings: CacheSettings,
        url: &str,
        eligibility: Option<CacheEligibility>,
    ) -> io::Result<Self> {
        settings
            .validate()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        let client = crate::tls::redis_client(url, &settings.tls)?;
        Ok(Self {
            budget: Arc::new(Semaphore::new(settings.max_buffered_bytes_total)),
            settings,
            client,
            connection: Mutex::new(None),
            eligibility,
        })
    }

    pub(crate) async fn eligible(&self, request: &SelectionRequest<'_>) -> bool {
        if !self.settings.enabled {
            return false;
        }
        match &self.eligibility {
            None => true,
            Some(callback) => match callback.eligible(request).await {
                Ok(eligible) => eligible,
                Err(_) => {
                    tracing::debug!(
                        query_id = request.query_id,
                        client_id = request.client_id,
                        "cache eligibility failed; bypassing cache"
                    );
                    false
                }
            },
        }
    }

    pub(crate) fn reserve(&self, scope: &[u8; 32]) -> Option<CacheBuffer> {
        // Reserve for capture, Redis serialization, and a bounded response copy.
        // Permits remain owned until all associated buffers and commands are dropped.
        let max_entry_bytes = self.max_entry_bytes();
        if max_entry_bytes <= HEADER_BYTES {
            return None;
        }
        let permit = self
            .budget
            .clone()
            .try_acquire_many_owned((max_entry_bytes * 3) as u32)
            .ok()?;
        let mut bytes = BytesMut::with_capacity(max_entry_bytes);
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(scope);
        Some(CacheBuffer {
            bytes,
            association: None,
            _permit: permit,
        })
    }

    pub(crate) fn max_entry_bytes(&self) -> usize {
        self.settings.max_buffered_bytes_total / 3
    }

    pub(crate) fn max_rows_per_query(&self) -> usize {
        self.settings.max_rows_per_query
    }

    pub(crate) async fn get(
        &self,
        target: &str,
        backend_login: &str,
        query_id: &str,
        scope: &[u8; 32],
        buffer: &mut CacheBuffer,
    ) -> bool {
        let mut cmd = redis::cmd("GETRANGE");
        cmd.arg(cache_key(
            &self.settings.namespace,
            target,
            backend_login,
            query_id,
        ))
        .arg(0)
        .arg(self.max_entry_bytes());
        if let Some(Value::BulkString(value)) = self.command(cmd).await
            && value.len() > HEADER_BYTES
            && value.len() <= self.max_entry_bytes()
            && (value.starts_with(MAGIC) || value.starts_with(classification::CLASSIFIED_MAGIC))
            && &value[MAGIC.len()..HEADER_BYTES] == scope
        {
            let (header, association) = if value.starts_with(classification::CLASSIFIED_MAGIC) {
                if value.len() <= classification::CLASSIFIED_HEADER {
                    return false;
                }
                (
                    classification::CLASSIFIED_HEADER,
                    Some(
                        value[HEADER_BYTES..classification::CLASSIFIED_HEADER]
                            .try_into()
                            .unwrap(),
                    ),
                )
            } else {
                (HEADER_BYTES, None)
            };
            buffer.bytes = Bytes::from(value)
                .try_into_mut()
                .expect("unshared Redis response");
            buffer.association = association;
            buffer.bytes.advance(header);
            return true;
        }
        false
    }

    pub(crate) async fn put(
        &self,
        target: &str,
        backend_login: &str,
        query_id: &str,
        buffer: &CacheBuffer,
    ) {
        if buffer.bytes.len() <= HEADER_BYTES || buffer.bytes.len() > self.max_entry_bytes() {
            return;
        }
        let mut cmd = redis::cmd("SET");
        cmd.arg(cache_key(
            &self.settings.namespace,
            target,
            backend_login,
            query_id,
        ))
        .arg(buffer.bytes.as_ref())
        .arg("EX")
        .arg(self.settings.ttl_seconds);
        if self.command(cmd).await.is_none() {
            tracing::debug!(
                query_id,
                "cache write failed; query result remains successful"
            );
        }
    }

    async fn command(&self, cmd: Cmd) -> Option<Value> {
        let deadline = REDIS_OPERATION_TIMEOUT;
        // ponytail: serialize Redis commands for bounded pending payloads; allow
        // concurrent multiplexed commands only when measured throughput needs it.
        let operation = async {
            let mut state = self.connection.lock().await;
            // Taking ownership ensures cancellation/timeout closes the sole handle
            // and its driver instead of retaining queued writes outside our budget.
            let mut connection = match state.take() {
                Some(connection) => connection,
                None => {
                    self.client
                        .get_multiplexed_async_connection_with_config(
                            &AsyncConnectionConfig::new()
                                .set_connection_timeout(Some(deadline))
                                .set_response_timeout(Some(deadline)),
                        )
                        .await?
                }
            };
            let result = cmd.query_async::<Value>(&mut connection).await;
            if result.is_ok() {
                *state = Some(connection);
            }
            result
        };
        match timeout(deadline, operation).await {
            Ok(Ok(value)) => Some(value),
            _ => {
                tracing::debug!("Redis unavailable or timed out; bypassing cache");
                None
            }
        }
    }

    pub(crate) async fn close(&self) {
        if let Some(eligibility) = &self.eligibility {
            eligibility.shutdown();
        }
        self.connection.lock().await.take();
        self.budget.close();
    }
}

pub(crate) fn cache_key(
    namespace: &str,
    target: &str,
    backend_login: &str,
    query_id: &str,
) -> String {
    format!(
        "{namespace}:{}_{}:{query_id}",
        escape_component(target),
        escape_component(backend_login)
    )
}

fn escape_component(name: &str) -> String {
    name.replace('%', "%25")
        .replace(':', "%3A")
        .replace('_', "%5F")
}
