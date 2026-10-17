//! Separate, bounded connections for dataset reads and publication.

use super::DatasetError;
use crate::config::ApplicationContextsSettings;
use redis::{AsyncConnectionConfig, Client, Cmd, Value, aio::MultiplexedConnection};
use std::{sync::Arc, time::Duration};
use tokio::{
    sync::{Mutex, Semaphore},
    time::timeout,
};

mod scripts;
pub(super) use scripts::READ;

const OPERATION_TIMEOUT: Duration = Duration::from_millis(100);
const BUFFER_BUDGET: usize = 32 * 1024 * 1024;

pub(super) struct Store {
    pub settings: ApplicationContextsSettings,
    pub budget: Arc<Semaphore>,
    reads: Connection,
    writes: Connection,
}

struct Connection {
    client: Client,
    state: Mutex<Option<MultiplexedConnection>>,
}

pub(super) enum Claim {
    Acquired { token: String, revision: String },
    Busy,
    Complete,
}

impl Connection {
    async fn command(&self, command: Cmd) -> Result<Value, DatasetError> {
        let operation = async {
            let mut state = self.state.lock().await;
            // Taking the sole handle closes pending work on cancellation/timeout.
            let mut connection = match state.take() {
                Some(connection) => connection,
                None => {
                    self.client
                        .get_multiplexed_async_connection_with_config(
                            &AsyncConnectionConfig::new()
                                .set_connection_timeout(Some(OPERATION_TIMEOUT))
                                .set_response_timeout(Some(OPERATION_TIMEOUT)),
                        )
                        .await?
                }
            };
            let result = command.query_async::<Value>(&mut connection).await;
            if result.is_ok() {
                *state = Some(connection);
            }
            result
        };
        match timeout(OPERATION_TIMEOUT, operation).await {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(_)) => Err(DatasetError::Unavailable),
            Err(_) => Err(DatasetError::TimedOut),
        }
    }
}

impl Store {
    pub fn new(settings: ApplicationContextsSettings, url: &str) -> Result<Self, DatasetError> {
        let client = crate::tls::redis_client(url, &settings.tls)
            .map_err(|_| DatasetError::InvalidSettings)?;
        // Large explicit lookup limits need enough room for Redis, decoded JSON,
        // and response copies; one lookup must fit the aggregate budget.
        let budget = BUFFER_BUDGET.max(settings.max_lookup_bytes.saturating_mul(3));
        Ok(Self {
            settings,
            budget: Arc::new(Semaphore::new(budget)),
            reads: Connection {
                client: client.clone(),
                state: Mutex::new(None),
            },
            writes: Connection {
                client,
                state: Mutex::new(None),
            },
        })
    }

    pub fn key(&self, name: &str, suffix: &str) -> String {
        format!("{}:{name}:{suffix}", self.settings.namespace)
    }

    pub async fn read_command(&self, cmd: Cmd) -> Result<Value, DatasetError> {
        self.reads.command(cmd).await
    }

    pub async fn revision(&self, name: &str) -> Result<String, DatasetError> {
        let mut cmd = redis::cmd("EVAL");
        cmd.arg(scripts::REVISION)
            .arg(1)
            .arg(self.key(name, "state"));
        match self.reads.command(cmd).await? {
            Value::BulkString(value) => {
                String::from_utf8(value).map_err(|_| DatasetError::InvalidData)
            }
            _ => Err(DatasetError::InvalidData),
        }
    }

    pub async fn claim(&self, name: &str, baseline: Option<&str>) -> Result<Claim, DatasetError> {
        let mut cmd = redis::cmd("EVAL");
        cmd.arg(scripts::CLAIM)
            .arg(3)
            .arg(self.key(name, "state"))
            .arg(self.key(name, "lease"))
            .arg(self.key(name, "data"))
            .arg(baseline.unwrap_or(""))
            .arg(self.settings.refresh_timeout_ms)
            .arg(
                self.settings.datasets[name]
                    .refresh_interval_seconds
                    .saturating_mul(1000),
            );
        let Value::Array(values) = self.writes.command(cmd).await? else {
            return Err(DatasetError::InvalidData);
        };
        match values.as_slice() {
            [Value::Int(0)] => Ok(Claim::Complete),
            [Value::Int(1)] => Ok(Claim::Busy),
            [
                Value::Int(2),
                Value::BulkString(token),
                Value::BulkString(revision),
            ] => Ok(Claim::Acquired {
                token: String::from_utf8(token.clone()).map_err(|_| DatasetError::InvalidData)?,
                revision: String::from_utf8(revision.clone())
                    .map_err(|_| DatasetError::InvalidData)?,
            }),
            _ => Err(DatasetError::InvalidData),
        }
    }

    pub async fn publish(
        &self,
        name: &str,
        token: &str,
        revision: &str,
        data: &serde_json::Value,
    ) -> Result<(), DatasetError> {
        let stage = self.key(name, &format!("staging:{token}"));
        let mut begin = redis::cmd("EVAL");
        begin
            .arg(scripts::BEGIN)
            .arg(1)
            .arg(&stage)
            .arg(u64::from(self.settings.refresh_timeout_ms) * 2);
        self.writes.command(begin).await?;
        let map = data.as_object().ok_or(DatasetError::InvalidData)?;
        let mut fields = map.iter().peekable();
        while fields.peek().is_some() {
            let mut cmd = redis::cmd("HSET");
            cmd.arg(&stage);
            let mut bytes = 0usize;
            for (key, value) in fields.by_ref().take(64) {
                let value = serde_json::to_string(value).map_err(|_| DatasetError::InvalidData)?;
                bytes = bytes.saturating_add(key.len()).saturating_add(value.len());
                cmd.arg(key).arg(value);
                if bytes >= self.settings.max_lookup_bytes {
                    break;
                }
            }
            self.writes.command(cmd).await?;
        }
        let mut commit = redis::cmd("EVAL");
        commit
            .arg(scripts::PUBLISH)
            .arg(4)
            .arg(self.key(name, "state"))
            .arg(self.key(name, "lease"))
            .arg(&stage)
            .arg(self.key(name, "data"))
            .arg(token)
            .arg(revision)
            .arg(
                self.settings.datasets[name]
                    .refresh_interval_seconds
                    .saturating_mul(1000),
            );
        match self.writes.command(commit).await? {
            Value::BulkString(revision) => {
                tracing::info!(dataset = name, revision = %String::from_utf8_lossy(&revision), fields = map.len(), "dataset published");
                Ok(())
            }
            Value::Nil => Err(DatasetError::Closed),
            _ => Err(DatasetError::InvalidData),
        }
    }

    pub async fn failed(&self, name: &str, token: &str, revision: &str) {
        let mut cmd = redis::cmd("EVAL");
        cmd.arg(scripts::FAILED)
            .arg(2)
            .arg(self.key(name, "state"))
            .arg(self.key(name, "lease"))
            .arg(token)
            .arg(revision)
            .arg(
                self.settings.datasets[name]
                    .refresh_interval_seconds
                    .saturating_mul(1000),
            );
        let _ = self.writes.command(cmd).await;
    }

    pub async fn close(&self) {
        self.budget.close();
        self.reads.state.lock().await.take();
        self.writes.state.lock().await.take();
    }
}
