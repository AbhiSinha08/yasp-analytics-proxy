//! Bounded analytics gateway with host-owned extensions and PostgreSQL adapters.
//! See docs/architecture.md for component ownership and docs/PLAN.md for phase scope.

pub mod backend;
pub mod cache;
pub mod config;
pub mod datasets;
pub mod discovery;
pub mod gateway;
pub mod hooks;
pub mod logger;
pub mod policy;
pub mod query;

mod transport;
