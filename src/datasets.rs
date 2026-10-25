//! Application-owned JSON datasets, published atomically in Redis.
//!
//! Dataset loading is independent of query context and the result cache. Hooks
//! interpret the data; this component owns refresh coordination and bounded reads.

mod reader;
mod store;

pub use reader::DatasetReader;

use crate::{config::ApplicationContextsSettings, hooks::payload};
use futures::future::BoxFuture;
use serde_json::{Map, Value};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use thiserror::Error;
use tokio::{
    sync::{Mutex, watch},
    task::JoinHandle,
    time::{Instant, timeout_at},
};

/// Loader diagnostics never contain application values or upstream error text.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum DatasetLoadError {
    #[error("dataset loader failed")]
    Failed,
    #[error("dataset loader returned unsupported data")]
    InvalidOutput,
    #[error("dataset exceeds field, byte, or nesting limits")]
    Limit,
    #[error("dataset loader timed out")]
    TimedOut,
}

/// Dataset errors contain categories, never Redis URLs or data.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum DatasetError {
    #[error("invalid dataset configuration or registration")]
    InvalidSettings,
    #[error("unknown dataset")]
    UnknownDataset,
    #[error("dataset unavailable")]
    Unavailable,
    #[error("dataset data is malformed")]
    InvalidData,
    #[error("dataset lookup exceeds configured limits")]
    Limit,
    #[error("dataset operation timed out")]
    TimedOut,
    #[error("dataset reader is closed")]
    Closed,
    #[error("dataset loader failed: {0}")]
    Load(#[from] DatasetLoadError),
}

/// A startup or scheduled load. Providers return a complete JSON object.
#[derive(Clone, Copy)]
pub struct DatasetLoadRequest<'a> {
    pub name: &'a str,
    pub reason: &'a str,
}

pub type DatasetCallback = Arc<
    dyn for<'a> Fn(
            DatasetLoadRequest<'a>,
        ) -> BoxFuture<'a, Result<Map<String, Value>, DatasetLoadError>>
        + Send
        + Sync,
>;

pub enum DatasetLoader {
    Rust(DatasetCallback),
    #[cfg(feature = "python")]
    Python(crate::hooks::python::PythonCallback),
}

impl From<DatasetCallback> for DatasetLoader {
    fn from(callback: DatasetCallback) -> Self {
        Self::Rust(callback)
    }
}

#[cfg(feature = "python")]
impl From<crate::hooks::python::PythonCallback> for DatasetLoader {
    fn from(callback: crate::hooks::python::PythonCallback) -> Self {
        Self::Python(callback)
    }
}

/// Bind compiled application state without embedding policy in the engine.
pub fn bind_dataset_loader<State, F>(state: State, function: F) -> DatasetCallback
where
    State: Send + Sync + 'static,
    F: for<'a> Fn(
            DatasetLoadRequest<'a>,
            Arc<State>,
        ) -> BoxFuture<'a, Result<Map<String, Value>, DatasetLoadError>>
        + Send
        + Sync
        + 'static,
{
    let state = Arc::new(state);
    Arc::new(move |request| function(request, state.clone()))
}

impl DatasetLoader {
    async fn load(
        &self,
        request: DatasetLoadRequest<'_>,
    ) -> Result<Map<String, Value>, DatasetLoadError> {
        match self {
            Self::Rust(callback) => callback(request).await,
            #[cfg(feature = "python")]
            Self::Python(callback) => {
                let value = callback
                    .call(serde_json::json!({"name": request.name, "reason": request.reason}))
                    .await
                    .map_err(|_| DatasetLoadError::Failed)?;
                match value {
                    Value::Object(data) => Ok(data),
                    _ => Err(DatasetLoadError::InvalidOutput),
                }
            }
        }
    }

    fn shutdown(&self) {
        #[cfg(feature = "python")]
        if let Self::Python(callback) = self {
            callback.runtime().shutdown();
        }
    }
}

/// Owns mandatory startup loading and optional scheduled refresh work.
/// Keep this value alive while serving; dropping it cancels refresh admission.
pub struct Datasets {
    reader: DatasetReader,
    stopped: watch::Sender<bool>,
    worker: Mutex<Option<JoinHandle<()>>>,
    loaders: Arc<BTreeMap<String, DatasetLoader>>,
}

impl Datasets {
    /// Load fresh data for every registered name before returning a serving owner.
    /// Scheduled refreshes retain the previous published value on failure. Keep
    /// this owner alive while serving, then call `shutdown` to stop refresh work.
    pub async fn start(
        settings: ApplicationContextsSettings,
        url: &str,
        loaders: BTreeMap<String, DatasetLoader>,
    ) -> Result<Self, DatasetError> {
        settings
            .validate()
            .map_err(|_| DatasetError::InvalidSettings)?;
        if settings.datasets.keys().ne(loaders.keys()) {
            return Err(DatasetError::InvalidSettings);
        }
        let store = Arc::new(store::Store::new(settings, url)?);
        let (stopped, _) = watch::channel(false);
        let datasets = Self {
            reader: DatasetReader::new(store.clone()),
            stopped,
            worker: Mutex::new(None),
            loaders: Arc::new(loaders),
        };
        for (name, loader) in datasets.loaders.iter() {
            refresh(&store, name, loader, true).await?;
        }
        if store
            .settings
            .datasets
            .values()
            .any(|dataset| dataset.refresh_interval_seconds > 0)
        {
            let loaders = datasets.loaders.clone();
            let mut stop = datasets.stopped.subscribe();
            let worker = tokio::spawn(async move {
                // Serial refresh keeps only one dataset load in flight per process.
                // Slow loaders delay other refreshes; readers keep the last good data.
                let mut interval = tokio::time::interval(Duration::from_secs(1));
                loop {
                    tokio::select! {
                        biased;
                        _ = stop.changed() => break,
                        _ = interval.tick() => {}
                    }
                    for (name, loader) in loaders.iter() {
                        if store.settings.datasets[name].refresh_interval_seconds == 0 {
                            continue;
                        }
                        tokio::select! {
                            biased;
                            _ = stop.changed() => return,
                            result = refresh(&store, name, loader, false) => {
                                if let Err(error) = result {
                                    tracing::error!(dataset = name, category = %error, "dataset refresh failed; retaining previous data");
                                }
                            }
                        }
                    }
                }
            });
            *datasets.worker.lock().await = Some(worker);
        }
        Ok(datasets)
    }

    pub fn reader(&self) -> DatasetReader {
        self.reader.clone()
    }

    pub async fn shutdown(&self) {
        self.stopped.send_replace(true);
        if let Some(worker) = self.worker.lock().await.take() {
            let _ = worker.await;
        }
        for loader in self.loaders.values() {
            loader.shutdown();
        }
        self.reader.store.close().await;
    }
}

impl Drop for Datasets {
    fn drop(&mut self) {
        self.stopped.send_replace(true);
    }
}

async fn refresh(
    store: &store::Store,
    name: &str,
    loader: &DatasetLoader,
    startup: bool,
) -> Result<(), DatasetError> {
    let duration = Duration::from_millis(u64::from(store.settings.refresh_timeout_ms));
    let deadline = Instant::now() + duration;
    let baseline = if startup {
        Some(store.revision(name).await?)
    } else {
        None
    };
    let (token, revision) = loop {
        if Instant::now() >= deadline {
            return Err(DatasetError::TimedOut);
        }
        match store.claim(name, baseline.as_deref()).await? {
            store::Claim::Acquired { token, revision } => break (token, revision),
            store::Claim::Complete => return Ok(()),
            store::Claim::Busy if !startup => return Ok(()),
            store::Claim::Busy => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    };
    let result = async {
        let data = Value::Object(
            loader
                .load(DatasetLoadRequest {
                    name,
                    reason: if startup { "startup" } else { "scheduled" },
                })
                .await?,
        );
        validate_data(&data, &store.settings)?;
        store.publish(name, &token, &revision, &data).await
    };
    let result = timeout_at(deadline, result)
        .await
        .unwrap_or(Err(DatasetError::Load(DatasetLoadError::TimedOut)));
    if result.is_err() {
        store.failed(name, &token, &revision).await;
    }
    result
}

fn validate_data(
    data: &Value,
    settings: &ApplicationContextsSettings,
) -> Result<(), DatasetLoadError> {
    let map = data.as_object().ok_or(DatasetLoadError::InvalidOutput)?;
    if map.len() > 100_000 {
        return Err(DatasetLoadError::Limit);
    }
    payload::payload_size(data, settings.max_dataset_bytes).map_err(|_| DatasetLoadError::Limit)?;
    for (key, value) in map {
        // The empty hash field is the engine's marker, present even for {}.
        if key.is_empty() || key.len() > 256 {
            return Err(DatasetLoadError::Limit);
        }
        payload::payload_size(value, settings.max_lookup_bytes)
            .map_err(|_| DatasetLoadError::Limit)?;
    }
    Ok(())
}
