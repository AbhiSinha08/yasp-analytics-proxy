//! PostgreSQL client metadata recognition and local session setup.
//! Backend discovery adapters own execution and source-specific rewriting.

mod catalog;
mod classify;
mod session;

pub(crate) use catalog::{catalog_relation, helper, name};
pub use classify::{DiscoveryKind, classify};
pub(crate) use session::{SessionSettings, SetupResponse};

#[cfg(test)]
mod tests;
