//! Shared stdout logging setup for YASP hosts.

use std::{
    env, io,
    sync::atomic::{AtomicU64, Ordering},
};
use tracing_subscriber::filter::LevelFilter;

static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);
static NEXT_QUERY_ID: AtomicU64 = AtomicU64::new(1);

/// Initialize stdout logging using `LOG_LEVEL` (`INFO` by default).
///
/// Valid levels are `DEBUG`, `INFO`, `WARN`, and `ERROR`. The selected level
/// includes messages at higher severities.
pub fn init() -> io::Result<()> {
    let setting = match env::var("LOG_LEVEL") {
        Ok(setting) => setting,
        Err(env::VarError::NotPresent) => "INFO".to_owned(),
        Err(env::VarError::NotUnicode(_)) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "LOG_LEVEL must be one of DEBUG, INFO, WARN, or ERROR",
            ));
        }
    };
    init_with_level(&setting)
}

/// Initialize stdout logging from a value resolved by the host application.
pub fn init_with_level(setting: &str) -> io::Result<()> {
    let level = match setting {
        "DEBUG" => LevelFilter::DEBUG,
        "INFO" => LevelFilter::INFO,
        "WARN" => LevelFilter::WARN,
        "ERROR" => LevelFilter::ERROR,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "LOG_LEVEL must be one of DEBUG, INFO, WARN, or ERROR",
            ));
        }
    };

    tracing_subscriber::fmt()
        .with_writer(io::stdout)
        .with_max_level(level)
        .try_init()
        .map_err(io::Error::other)
}

pub(crate) fn next_client_id() -> u64 {
    NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed)
}

pub(crate) fn next_query_id() -> u64 {
    NEXT_QUERY_ID.fetch_add(1, Ordering::Relaxed)
}
