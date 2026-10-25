//! A SQL gateway that sits between BI clients and configured source databases.
//!
//! A host application supplies configuration, source credentials, and callbacks,
//! then starts a listener through [`gateway`]. The supplied executable assembles
//! these services in `src/host`; organizations can assemble their own host instead.
//!
//! The frontend authenticates clients and inspects SQL. Shared execution validates
//! routes and coordinates caching/result processing; backend adapters own database
//! connections, native values, cancellation, and cleanup. Application rules belong
//! in [`hooks`], while database roles enforce source access.
//!
//! Read `docs/architecture.md` for diagrams and `docs/development.md` for a source
//! tour. Optional Cargo features enable embedded Python and the Snowflake adapter.

pub mod backend;
pub mod cache;
pub mod catalog;
pub mod config;
pub mod datasets;
pub mod decision_model;
pub mod discovery;
pub mod gateway;
pub mod hooks;
pub mod lineage;
pub mod logger;
pub mod policy;
pub mod query;

mod execution;
mod tls;
mod transport;
mod wire;
