//! Snowflake public SQL API. Each statement is independently autocommitted.
mod auth;
mod catalog;
mod execution;
mod rest;
mod result;
pub(super) mod session;
#[cfg(test)]
mod tests;

use crate::{
    backend::{
        ConnectionBudget,
        budget::CapacityGroup,
        result::{ExecutionError, ExecutionResult},
    },
    config::GatewaySettings,
    discovery::SessionSettings,
    query::{ParsedQuery, SqlDialect},
};
use std::{
    io,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

/// A fixed source identity. Query hints cannot change its role or warehouse.
pub struct SnowflakeConfig {
    pub account: String,
    pub username: String,
    /// PKCS#8 RSA PEM. Register the corresponding public key with Snowflake.
    pub private_key_file: PathBuf,
    pub private_key_passphrase: Option<String>,
    pub database: String,
    pub schema: String,
    pub role: String,
    pub warehouse: String,
    /// Local development only: numeric loopback HTTP endpoint.
    pub emulator: Option<std::net::SocketAddr>,
    pub max_connections: usize,
    /// Bounds each decompressed JSON response, including one result partition.
    pub max_response_bytes: usize,
}

pub struct SnowflakeBackend {
    inner: Arc<Inner>,
}
struct Inner {
    catalog_identity: crate::catalog::SourceIdentity,
    database: String,
    database_user: String,
    emulator: bool,
    endpoint: String,
    client: reqwest::Client,
    auth: auth::KeyPair,
    capacity: Mutex<Option<CapacityGroup>>,
    max_connections: usize,
    max_response_bytes: usize,
}

impl SnowflakeBackend {
    pub fn new(config: SnowflakeConfig) -> io::Result<Self> {
        let invalid = || {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid Snowflake SQL API binding",
            )
        };
        if config.max_connections == 0
            || config.max_connections > 1024
            || !(1024..=128 * 1024 * 1024).contains(&config.max_response_bytes)
            || config
                .emulator
                .is_some_and(|a| !a.ip().is_loopback() || a.port() == 0)
            || [
                &config.account,
                &config.username,
                &config.database,
                &config.schema,
                &config.role,
                &config.warehouse,
            ]
            .iter()
            .any(|s| s.is_empty() || s.len() > 4096 || s.contains('\0'))
            || !config
                .account
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"-._".contains(&c))
        {
            return Err(invalid());
        }
        let endpoint = match config.emulator {
            Some(address) => format!("http://{address}"),
            None => format!("https://{}.snowflakecomputing.com", config.account),
        };
        let auth = auth::KeyPair::load(&config)?;
        let mut builder = reqwest::Client::builder();
        if config.emulator.is_some() {
            builder = builder.no_proxy();
        }
        let client = builder
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(10))
            .pool_max_idle_per_host(config.max_connections)
            .user_agent(concat!("yasp/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|_| invalid())?;
        let catalog_identity = crate::catalog::SourceIdentity {
            dialect: SqlDialect::Snowflake,
            endpoint: format!("{endpoint}/{}", config.database),
            principal: config.username.clone(),
            role: Some(config.role),
            warehouse: Some(config.warehouse),
            default_schema: Some(config.schema),
        };
        Ok(Self {
            inner: Arc::new(Inner {
                catalog_identity,
                database: config.database,
                database_user: config.username,
                emulator: config.emulator.is_some(),
                endpoint,
                client,
                auth,
                capacity: Mutex::new(None),
                max_connections: config.max_connections,
                max_response_bytes: config.max_response_bytes,
            }),
        })
    }
    pub(crate) fn attach_budget(&self, budget: &Arc<ConnectionBudget>) -> io::Result<()> {
        let mut group = self.inner.capacity.lock().unwrap();
        if let Some(group) = &*group {
            if !Arc::ptr_eq(&group.budget(), budget) || group.is_closed() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Snowflake binding already belongs to another budget or is closed",
                ));
            }
        } else {
            *group = Some(budget.group(self.inner.max_connections)?);
        }
        Ok(())
    }
    pub(crate) fn close(&self) {
        self.inner.capacity().close();
    }
    pub(crate) fn log_query(&self, query: &ParsedQuery, client_id: u64) {
        tracing::debug!(query_id = query.query_id(), client_id,
            database = %self.inner.database, database_user = %self.inner.database_user,
            sql = query.sql(), "Snowflake query execution requested");
    }
    pub(crate) fn validate_input(&self, query: &ParsedQuery) -> ExecutionResult<()> {
        query
            .validate_read_only()
            .map_err(|_| ExecutionError::rejected("0A000", "only read-only queries are supported"))
    }
    pub(crate) async fn open(
        &self,
        settings: &GatewaySettings,
        transaction: bool,
    ) -> ExecutionResult<session::Session> {
        session::Session::open(self.inner.clone(), settings, transaction).await
    }
}

pub(super) fn validate_session(session: &SessionSettings) -> ExecutionResult<()> {
    if session.search_path != SessionSettings::default().search_path {
        return Err(ExecutionError::rejected(
            "0A000",
            "Snowflake uses the binding's configured schema; custom frontend search paths are unsupported",
        ));
    }
    Ok(())
}

impl Inner {
    fn capacity(&self) -> CapacityGroup {
        self.capacity
            .lock()
            .unwrap()
            .get_or_insert_with(|| {
                let budget = ConnectionBudget::new(self.max_connections)
                    .expect("validated Snowflake capacity");
                budget
                    .group(self.max_connections)
                    .expect("validated Snowflake capacity")
            })
            .clone()
    }
}
