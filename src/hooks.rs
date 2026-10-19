//! Host-owned extension contracts. Each stage owns its request, result, and
//! failure policy; callbacks never own backend credentials or engine limits.

mod cache;
mod context;
mod input;
pub(crate) mod payload;
mod selection;

#[path = "../hooks/builtin/mod.rs"]
pub mod builtin;

#[cfg(feature = "python")]
pub mod python;

pub use cache::{
    AsyncCacheEligibilityCallback, CacheEligibility, CacheEligibilityCallback,
    CacheEligibilityError, bind_cache_eligibility, bind_cache_eligibility_async,
};
pub use context::{ContextCallback, ContextError, ContextHook, ContextRequest, bind_context};
pub use input::{InputOptionsResolver, InputRequest};
pub use selection::{
    AsyncBackendSelector, BackendDecision, BackendSelection, BackendSelector, RoutedSelector,
    SelectionError, SelectionRequest, bind_select_backend, bind_select_backend_async,
};
