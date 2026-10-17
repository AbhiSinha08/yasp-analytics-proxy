//! Optional cache bypass decisions. Eligibility cannot widen SQL or engine bounds.

use super::SelectionRequest;
#[cfg(feature = "python")]
use super::{python, selection::request_payload};
use crate::datasets::DatasetReader;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;

#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("cache eligibility callback failed")]
pub struct CacheEligibilityError;

pub type CacheEligibilityCallback =
    Arc<dyn for<'a> Fn(&SelectionRequest<'a>) -> Result<bool, CacheEligibilityError> + Send + Sync>;

/// Registered cache eligibility behavior supplied by the host application.
pub enum CacheEligibility {
    Rust(CacheEligibilityCallback),
    RustAsync(AsyncCacheEligibilityCallback),
    #[cfg(feature = "python")]
    Python(python::PythonCallback),
}

pub type AsyncCacheEligibilityCallback = Arc<
    dyn for<'a> Fn(
            &'a SelectionRequest<'a>,
        ) -> futures::future::BoxFuture<'a, Result<bool, CacheEligibilityError>>
        + Send
        + Sync,
>;

impl From<CacheEligibilityCallback> for CacheEligibility {
    fn from(callback: CacheEligibilityCallback) -> Self {
        Self::Rust(callback)
    }
}

impl From<AsyncCacheEligibilityCallback> for CacheEligibility {
    fn from(callback: AsyncCacheEligibilityCallback) -> Self {
        Self::RustAsync(callback)
    }
}

#[cfg(feature = "python")]
impl From<python::PythonCallback> for CacheEligibility {
    fn from(callback: python::PythonCallback) -> Self {
        Self::Python(callback)
    }
}

impl CacheEligibility {
    pub async fn eligible(
        &self,
        request: &SelectionRequest<'_>,
    ) -> Result<bool, CacheEligibilityError> {
        let python_hook = {
            #[cfg(feature = "python")]
            {
                matches!(self, Self::Python(_))
            }
            #[cfg(not(feature = "python"))]
            {
                false
            }
        };
        let (datasets, _guard) = if python_hook {
            (request.datasets.cloned(), None)
        } else {
            request
                .datasets
                .map(DatasetReader::invocation)
                .map_or((None, None), |(reader, guard)| (Some(reader), Some(guard)))
        };
        let scoped = SelectionRequest {
            authenticated_user: request.authenticated_user,
            client_id: request.client_id,
            query_id: request.query_id,
            query: request.query,
            context: request.context,
            datasets: datasets.as_ref(),
            async_hook_timeout_ms: request.async_hook_timeout_ms,
        };
        let request = &scoped;
        match self {
            Self::Rust(callback) => callback(request),
            Self::RustAsync(callback) => tokio::time::timeout(
                Duration::from_millis(u64::from(request.async_hook_timeout_ms)),
                callback(request),
            )
            .await
            .map_err(|_| CacheEligibilityError)?,
            #[cfg(feature = "python")]
            Self::Python(callback) => {
                let value = callback
                    .call_with_datasets(request_payload(request), request.datasets.cloned())
                    .await
                    .map_err(|error| {
                        tracing::error!(
                            query_id = request.query_id,
                            client_id = request.client_id,
                            category = %error,
                            "Python cache eligibility callback failed"
                        );
                        CacheEligibilityError
                    })?;
                value.as_bool().ok_or(CacheEligibilityError)
            }
        }
    }

    pub(crate) fn shutdown(&self) {
        #[cfg(feature = "python")]
        if let Self::Python(callback) = self {
            callback.runtime().shutdown();
        }
    }
}

pub fn bind_cache_eligibility_async<State, F>(
    state: State,
    function: F,
) -> AsyncCacheEligibilityCallback
where
    State: Send + Sync + 'static,
    F: for<'a> Fn(
            &'a SelectionRequest<'a>,
            Arc<State>,
        ) -> futures::future::BoxFuture<'a, Result<bool, CacheEligibilityError>>
        + Send
        + Sync
        + 'static,
{
    let state = Arc::new(state);
    Arc::new(move |request| function(request, state.clone()))
}

/// Bind host-owned state to a cache eligibility callback.
pub fn bind_cache_eligibility<State, F>(state: State, function: F) -> CacheEligibilityCallback
where
    State: Send + Sync + 'static,
    F: for<'a> Fn(&SelectionRequest<'a>, &State) -> Result<bool, CacheEligibilityError>
        + Send
        + Sync
        + 'static,
{
    Arc::new(move |request| function(request, &state))
}
