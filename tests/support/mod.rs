//! Lifecycle helpers for PostgreSQL integration scenarios.
#![allow(dead_code)] // Each integration binary uses a different subset.

use std::{
    path::Path,
    process::Output,
    thread,
    time::{SystemTime, UNIX_EPOCH},
};
mod gateway;
mod postgres;
mod process;
mod redis;
#[allow(unused_imports)] // A test binary only imports the fixtures its scenarios need.
pub use {
    gateway::GatewayServer,
    postgres::{Identity, PostgresConnection, TestDatabase, TestEnvironment, quote_ident},
    process::{ChildGuard, TempDirectory},
    redis::RedisNamespace,
};

pub fn root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
}

pub fn unique_name(prefix: &str) -> String {
    format!(
        "{prefix}_{}_{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

pub fn assert_success(output: &Output, description: &str) {
    assert!(
        output.status.success(),
        "{description} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn cleanup_result(success: bool, description: &str) {
    if thread::panicking() {
        if !success {
            eprintln!("test cleanup failed: {description}");
        }
    } else {
        assert!(success, "test cleanup failed: {description}");
    }
}
