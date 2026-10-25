//! Host-owned extension contracts. Each stage owns its request, result, and
//! failure policy; callbacks never own backend credentials or engine limits.

mod cache;
pub(crate) mod payload;
mod query_context;
mod query_modification;
pub(crate) mod result;
mod selection;

#[path = "../hooks/builtin/mod.rs"]
pub mod builtin;

#[cfg(feature = "python")]
pub mod python;

pub use cache::{
    AsyncCacheEligibilityCallback, CacheEligibility, CacheEligibilityCallback,
    CacheEligibilityError, bind_cache_eligibility, bind_cache_eligibility_async,
};
pub use query_context::{
    QueryContextCallback, QueryContextError, QueryContextHook, QueryContextOutput,
    QueryContextRequest, bind_query_context,
};
pub use query_modification::{
    QueryModificationCallback, QueryModificationError, QueryModificationHook,
    QueryModificationRequest, bind_modify_query,
};
pub use result::{
    ResultBatch, ResultCallback, ResultColumn, ResultDataContexts, ResultHook, ResultHookError,
    ResultProcessingSettings, ResultRequest, bind_process_result,
};
pub use selection::{
    AsyncBackendSelector, BackendDecision, BackendSelection, BackendSelector, RoutedSelector,
    SelectionError, SelectionRequest, bind_select_backend, bind_select_backend_async,
};
