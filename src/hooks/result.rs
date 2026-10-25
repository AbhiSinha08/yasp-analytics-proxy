//! Bounded, schema-preserving result customization. Policy belongs to the host.

mod batch;
pub(crate) use batch::Reservation;
pub use batch::{ResultBatch, ResultColumn};

use crate::{
    datasets::DatasetReader, decision_model::Classifications, lineage::AstDataContext,
    query::ParsedQuery,
};
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::sync::Arc;
use tokio::sync::Semaphore;

/// Application metadata describes the raw source result, before edits/filtering.
#[derive(Clone, Copy)]
pub struct ResultDataContexts<'a> {
    pub ast: Option<&'a AstDataContext>,
    pub decision_model: Option<&'a Classifications>,
}

#[derive(Clone, Copy)]
pub struct ResultRequest<'a> {
    pub authenticated_user: &'a str,
    pub frontend_database: &'a str,
    pub client_id: u64,
    pub query_id: &'a str,
    pub query: &'a ParsedQuery,
    pub target: &'a str,
    pub backend_login: &'a str,
    pub query_context: &'a Map<String, Value>,
    pub data_contexts: ResultDataContexts<'a>,
    pub datasets: Option<&'a DatasetReader>,
}

/// Errors contain no result values or application exception text.
#[derive(Debug, Clone, thiserror::Error)]
pub enum ResultHookError {
    #[error("result callback failed")]
    Failed,
    #[error("result callback timed out")]
    TimedOut,
    #[error("invalid result batch operation")]
    InvalidOutput,
    #[error("result column is missing or ambiguous")]
    Column,
    #[error("replacement is incompatible with the result column")]
    Type,
    #[error("result processing byte budget exceeded")]
    Limit,
    #[cfg(feature = "python")]
    #[error("Python result callback failed: {0}")]
    Python(#[from] super::python::PythonHookError),
}

pub type ResultCallback = Arc<
    dyn for<'a> Fn(
            ResultRequest<'a>,
            &'a mut ResultBatch,
        ) -> BoxFuture<'a, Result<(), ResultHookError>>
        + Send
        + Sync,
>;

pub enum ResultHook {
    Rust(ResultCallback),
    #[cfg(feature = "python")]
    Python(super::python::PythonCallback),
}
impl From<ResultCallback> for ResultHook {
    fn from(callback: ResultCallback) -> Self {
        Self::Rust(callback)
    }
}
#[cfg(feature = "python")]
impl From<super::python::PythonCallback> for ResultHook {
    fn from(callback: super::python::PythonCallback) -> Self {
        Self::Python(callback)
    }
}

pub fn bind_process_result<State, F>(state: State, function: F) -> ResultCallback
where
    State: Send + Sync + 'static,
    F: for<'a> Fn(
            ResultRequest<'a>,
            &'a mut ResultBatch,
            Arc<State>,
        ) -> BoxFuture<'a, Result<(), ResultHookError>>
        + Send
        + Sync
        + 'static,
{
    let state = Arc::new(state);
    Arc::new(move |request, batch| function(request, batch, state.clone()))
}

/// Row/byte targets bound ordinary chunks; one larger row may occupy a chunk.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ResultProcessingSettings {
    pub max_rows_per_batch: usize,
    pub target_batch_bytes: usize,
    pub max_buffered_bytes_total: usize,
}
impl Default for ResultProcessingSettings {
    fn default() -> Self {
        Self {
            max_rows_per_batch: 256,
            target_batch_bytes: 256 * 1024,
            max_buffered_bytes_total: 32 * 1024 * 1024,
        }
    }
}
impl ResultProcessingSettings {
    pub fn validate(&self) -> Result<(), crate::config::ConfigError> {
        if self.max_rows_per_batch == 0
            || self.target_batch_bytes == 0
            || self.target_batch_bytes > self.max_buffered_bytes_total
            || self.max_buffered_bytes_total > u32::MAX as usize
            || self.max_buffered_bytes_total > Semaphore::MAX_PERMITS
            || self.max_rows_per_batch > self.max_buffered_bytes_total / 64
        {
            return Err(crate::config::ConfigError::InvalidResultProcessing);
        }
        Ok(())
    }
}

/// Shared across sessions, including batches still executing after abandonment.
pub(crate) struct ResultProcessing {
    pub hook: ResultHook,
    pub settings: ResultProcessingSettings,
    pub budget: Arc<Semaphore>,
}
impl ResultProcessing {
    pub fn new(
        hook: ResultHook,
        settings: ResultProcessingSettings,
    ) -> Result<Self, crate::config::ConfigError> {
        settings.validate()?;
        let budget = Arc::new(Semaphore::new(settings.max_buffered_bytes_total));
        Ok(Self {
            hook,
            settings,
            budget,
        })
    }
    pub fn output_limit(&self) -> Option<usize> {
        match &self.hook {
            ResultHook::Rust(_) => None,
            #[cfg(feature = "python")]
            ResultHook::Python(callback) => Some(callback.result_payload_limit()),
        }
    }
    pub fn shutdown(&self) {
        #[cfg(feature = "python")]
        if let ResultHook::Python(callback) = &self.hook {
            callback.runtime().shutdown();
        }
    }
}
