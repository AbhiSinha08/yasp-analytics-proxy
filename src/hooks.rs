//! Host-owned extension contracts. Each stage owns its request, result, and
//! failure policy; callbacks never own backend credentials or engine limits.

mod cache;
mod context;
mod payload;
mod selection;

#[path = "../hooks/builtin/mod.rs"]
pub mod builtin;

#[cfg(feature = "python")]
pub mod python;

pub use cache::{
    CacheEligibility, CacheEligibilityCallback, CacheEligibilityError, bind_cache_eligibility,
};
pub use context::{ContextCallback, ContextError, ContextHook, ContextRequest, bind_context};
pub use selection::{
    BackendSelection, BackendSelector, RoutedSelector, SelectionError, SelectionRequest,
    bind_select_backend,
};
