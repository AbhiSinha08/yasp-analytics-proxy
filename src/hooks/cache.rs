//! Optional cache bypass decisions. Eligibility cannot widen SQL or engine bounds.

use super::SelectionRequest;
#[cfg(feature = "python")]
use super::{python, selection::request_payload};
use std::sync::Arc;
use thiserror::Error;

#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("cache eligibility callback failed")]
pub struct CacheEligibilityError;

pub type CacheEligibilityCallback =
    Arc<dyn for<'a> Fn(&SelectionRequest<'a>) -> Result<bool, CacheEligibilityError> + Send + Sync>;

/// Registered cache eligibility behavior supplied by the host application.
pub enum CacheEligibility {
    Rust(CacheEligibilityCallback),
    #[cfg(feature = "python")]
    Python(python::PythonCallback),
}

impl From<CacheEligibilityCallback> for CacheEligibility {
    fn from(callback: CacheEligibilityCallback) -> Self {
        Self::Rust(callback)
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
        match self {
            Self::Rust(callback) => callback(request),
            #[cfg(feature = "python")]
            Self::Python(callback) => {
                let value = callback
                    .call(request_payload(request))
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
