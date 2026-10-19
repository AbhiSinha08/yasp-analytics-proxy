//! Session-capable Snowflake execution, enabled explicitly with `snowflake`.
//! Each admitted session owns a blocking native worker and fresh source connection.

mod catalog;
mod connection;
mod result;
#[cfg(test)]
mod tests;
pub(super) mod worker;

use crate::{
    backend::result::{ExecutionError, ExecutionResult},
    config::GatewaySettings,
    discovery::SessionSettings,
    query::{ParsedQuery, SqlDialect},
};
use sha2::{Digest, Sha256};
use std::{
    io::{self, Read},
    path::PathBuf,
    sync::Arc,
};
use tokio::sync::Semaphore;

/// Host-owned Snowflake identity. A binding fixes role and warehouse; SQL remarks
/// cannot change them. The shared library is trusted executable application code.
pub struct SnowflakeConfig {
    pub driver_library: PathBuf,
    pub driver_sha256: String,
    pub account: String,
    pub username: String,
    pub password: String,
    pub database: String,
    pub schema: String,
    pub role: String,
    pub warehouse: String,
    /// Test-only HTTP endpoint. Only numeric loopback addresses are accepted.
    pub emulator: Option<std::net::SocketAddr>,
    pub max_connections: usize,
    pub acquire_timeout: std::time::Duration,
    /// Checked after Arrow allocation; this is not a limit on native process RSS.
    pub max_batch_bytes: usize,
}

pub struct SnowflakeBackend {
    inner: Arc<Inner>,
}
struct Inner {
    catalog_identity: crate::catalog::SourceIdentity,
    database: String,
    database_user: String,
    driver: connection::Driver,
    options: Vec<(String, String)>,
    permits: Arc<Semaphore>,
    acquire_timeout: std::time::Duration,
    max_batch_bytes: usize,
}

impl SnowflakeBackend {
    pub fn new(config: SnowflakeConfig) -> io::Result<Self> {
        let catalog_identity = crate::catalog::SourceIdentity {
            dialect: SqlDialect::Snowflake,
            endpoint: config
                .emulator
                .map(|a| a.to_string())
                .unwrap_or_else(|| config.account.clone()),
            principal: config.username.clone(),
            role: Some(config.role.clone()),
            warehouse: Some(config.warehouse.clone()),
        };
        let database = config.database.clone();
        let database_user = config.username.clone();
        let invalid = || io::Error::new(io::ErrorKind::InvalidInput, "invalid Snowflake binding");
        if config.max_connections == 0
            || config.max_connections > 1024
            || config.acquire_timeout.is_zero()
            || config.max_batch_bytes == 0
            || config.max_batch_bytes > 64 * 1024 * 1024
            || config
                .emulator
                .is_some_and(|a| !a.ip().is_loopback() || a.port() == 0)
            || [
                &config.account,
                &config.username,
                &config.password,
                &config.database,
                &config.schema,
                &config.role,
                &config.warehouse,
            ]
            .iter()
            .any(|s| s.is_empty() || s.len() > 4096 || s.contains('\0'))
        {
            return Err(invalid());
        }
        let driver_library = config.driver_library.canonicalize()?;
        let mut file = std::fs::File::open(&driver_library)?;
        let mut hash = Sha256::new();
        let mut buffer = [0u8; 8192];
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            hash.update(&buffer[..read]);
        }
        let actual: String = hash.finalize().iter().map(|b| format!("{b:02x}")).collect();
        if actual != config.driver_sha256 {
            return Err(invalid());
        }
        let driver = connection::Driver::load(&driver_library)
            .map_err(|_| io::Error::other("cannot load Snowflake driver"))?;
        let mut options = vec![
            ("adbc.snowflake.sql.account".into(), config.account),
            ("username".into(), config.username),
            ("password".into(), config.password),
            ("adbc.snowflake.sql.db".into(), config.database),
            ("adbc.snowflake.sql.schema".into(), config.schema),
            ("adbc.snowflake.sql.role".into(), config.role),
            ("adbc.snowflake.sql.warehouse".into(), config.warehouse),
            (
                "adbc.snowflake.sql.client_option.login_timeout".into(),
                "10s".into(),
            ),
            (
                "adbc.snowflake.sql.client_option.request_timeout".into(),
                "10s".into(),
            ),
            (
                "adbc.snowflake.sql.client_option.disable_telemetry".into(),
                "true".into(),
            ),
            (
                "adbc.snowflake.sql.client_option.use_high_precision".into(),
                "true".into(),
            ),
            (
                "adbc.snowflake.sql.client_option.max_timestamp_precision".into(),
                "nanoseconds_error_on_overflow".into(),
            ),
            ("timezone".into(), "UTC".into()),
        ];
        if let Some(endpoint) = config.emulator {
            options.extend([
                (
                    "adbc.snowflake.sql.uri.host".into(),
                    endpoint.ip().to_string(),
                ),
                (
                    "adbc.snowflake.sql.uri.port".into(),
                    endpoint.port().to_string(),
                ),
                ("adbc.snowflake.sql.uri.protocol".into(), "http".into()),
            ]);
        }
        Ok(Self {
            inner: Arc::new(Inner {
                catalog_identity,
                database,
                database_user,
                driver,
                options,
                permits: Arc::new(Semaphore::new(config.max_connections)),
                acquire_timeout: config.acquire_timeout,
                max_batch_bytes: config.max_batch_bytes,
            }),
        })
    }

    pub(crate) fn close(&self) {
        self.inner.permits.close();
    }

    pub(crate) fn log_query(&self, query: &ParsedQuery, client_id: u64) {
        tracing::debug!(query_id = query.query_id(), client_id,
            database = %self.inner.database, database_user = %self.inner.database_user,
            sql = query.sql(), "Snowflake query execution requested");
    }

    pub(crate) fn validate_input(&self, query: &ParsedQuery) -> ExecutionResult<()> {
        let options = query.input_options();
        if !options.native || options.dialect != SqlDialect::Snowflake {
            return Err(ExecutionError::rejected(
                "0A000",
                "this Snowflake binding requires native Snowflake SQL; generated SQL translation is not enabled",
            ));
        }
        query
            .validate_read_only()
            .map_err(|_| ExecutionError::rejected("0A000", "only read-only queries are supported"))
    }

    pub(crate) async fn open(
        &self,
        settings: &GatewaySettings,
        transaction: bool,
    ) -> ExecutionResult<worker::Session> {
        worker::Session::open(self.inner.clone(), settings, transaction).await
    }
}

fn validate_session(session: &SessionSettings) -> ExecutionResult<()> {
    if session.search_path != SessionSettings::default().search_path {
        return Err(ExecutionError::rejected(
            "0A000",
            "Snowflake uses the binding's configured schema; custom frontend search paths are unsupported",
        ));
    }
    Ok(())
}
