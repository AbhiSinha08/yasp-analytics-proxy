//! Built-in loaders for small, static JSON datasets.

use crate::datasets::{DatasetLoadError, DatasetLoadRequest};
use futures::future::BoxFuture;
use serde_json::{Map, Value};
use std::{fs::File, io::Read, path::PathBuf, sync::Arc};

/// Read a JSON object from the configured file, bounded by the dataset byte limit.
pub fn load_json<'a>(
    _request: DatasetLoadRequest<'a>,
    state: Arc<(PathBuf, usize)>,
) -> BoxFuture<'a, Result<Map<String, Value>, DatasetLoadError>> {
    let path = state.0.clone();
    let max_bytes = state.1;
    Box::pin(async move {
        tokio::task::spawn_blocking(move || {
            let file = File::open(path).map_err(|_| DatasetLoadError::Failed)?;
            let mut bytes = Vec::with_capacity(max_bytes.min(64 * 1024));
            file.take(max_bytes as u64 + 1)
                .read_to_end(&mut bytes)
                .map_err(|_| DatasetLoadError::Failed)?;
            if bytes.len() > max_bytes {
                return Err(DatasetLoadError::Limit);
            }
            match serde_json::from_slice(&bytes).map_err(|_| DatasetLoadError::InvalidOutput)? {
                Value::Object(data) => Ok(data),
                _ => Err(DatasetLoadError::InvalidOutput),
            }
        })
        .await
        .map_err(|_| DatasetLoadError::Failed)?
    })
}
