//! Backend adapters and client metadata dispatch. Database-specific pools,
//! execution, and response handling stay inside their adapter.

pub(crate) mod discovery;
pub(crate) mod postgres;

pub use postgres::{
    BackendLease, BackendRoute, PostgresBackend, PostgresBackends, PostgresBinding,
};
