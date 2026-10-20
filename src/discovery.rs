//! Frontend metadata recognition and local session setup.
//! Backend discovery adapters own execution and source-specific rewriting.

mod catalog;
mod classify;
pub(crate) mod redshift;
mod session;
pub(crate) mod template;

pub use classify::{DiscoveryKind, classify};
pub(crate) use session::{SessionSettings, SetupResponse};

#[cfg(test)]
mod tests;
