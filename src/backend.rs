//! Backend adapters, shared capacity, and client metadata dispatch. Database-specific pools,
//! execution, and response handling stay inside their adapter.

mod budget;
pub use budget::ConnectionBudget;

pub(crate) mod discovery;
pub(crate) mod execution;
pub(crate) mod postgres;
pub(crate) mod redshift;
#[cfg(feature = "snowflake")]
mod registry;
pub(crate) mod result;
#[cfg(feature = "snowflake")]
pub mod snowflake;
#[cfg(feature = "snowflake")]
pub use registry::Backends;

pub use postgres::{
    BackendLease, BackendRoute, PostgresBackend, PostgresBackends, PostgresBinding,
};
