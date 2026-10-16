//! PostgreSQL routes and read-only execution. Connections own wire I/O, while
//! the pool returns only verified idle sessions for reuse.

pub(crate) mod cache;
mod connection;
mod discovery;
mod pool;

use connection::validate_text_fields;
pub(super) use discovery::discover;
use pool::{Lease, PgManager};

use crate::{
    config::{BackendConfig, BackendSettings},
    hooks::BackendSelection,
    query::ParsedQuery,
};
use deadpool::managed::Pool;
use futures::{Sink, SinkExt};
use pgwire::{
    error::{ErrorInfo, PgWireError, PgWireResult},
    messages::{PgWireBackendMessage, response::TransactionStatus},
};
use std::{collections::BTreeMap, fmt, io, time::Duration};
use tokio::time::timeout;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const ACQUIRE_TIMEOUT: Duration = Duration::from_secs(10);
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(10);
const SESSION_DEFAULTS: &str = "SET client_encoding = 'UTF8'; SET standard_conforming_strings = on; SET DateStyle = 'ISO, MDY'; SET TimeZone = 'UTC'; SET IntervalStyle = 'postgres'; SET bytea_output = 'hex'; SET extra_float_digits = 3; SET default_transaction_read_only = on; SET default_transaction_isolation = 'read committed'; SET client_connection_check_interval = '1s'";

/// One target/login pool. No organization routing or callback policy is embedded.
pub struct PostgresBackend {
    pool: Pool<PgManager>,
    target: String,
    database_user: String,
}

impl PostgresBackend {
    pub(crate) async fn server_version(&self) -> PgWireResult<String> {
        let connection = self
            .pool
            .get()
            .await
            .map_err(|_| error_response("08006", "discovery source is unavailable"))?;
        connection
            .info
            .parameters
            .get("server_version")
            .cloned()
            .ok_or_else(|| {
                error_response(
                    "08006",
                    "discovery source did not supply its PostgreSQL version",
                )
            })
    }

    pub(crate) fn discovery(&self) -> super::discovery::DiscoverySource<'_> {
        super::discovery::DiscoverySource::Postgres(self)
    }
    pub fn new(config: BackendConfig) -> io::Result<Self> {
        Self::with_capacity(config, 8)
    }

    fn with_capacity(config: BackendConfig, capacity: usize) -> io::Result<Self> {
        let target = format!("{}:{}/{}", config.host, config.port, config.database);
        let database_user = config.username.clone();
        let pool = pool::new(config, capacity)?;
        Ok(Self {
            pool,
            target,
            database_user,
        })
    }

    pub(crate) fn target(&self) -> &str {
        &self.target
    }

    pub(crate) fn database_user(&self) -> &str {
        &self.database_user
    }

    /// Execute the already parsed statement. Native PostgreSQL messages stay
    /// inside the PostgreSQL adapter, preserving opaque values and type metadata.
    pub async fn execute<S>(
        &self,
        query: &ParsedQuery,
        output: &mut S,
        query_timeout: Duration,
        write_timeout: Duration,
    ) -> PgWireResult<()>
    where
        S: Sink<PgWireBackendMessage> + Unpin + Send,
        S::Error: fmt::Debug,
        PgWireError: From<S::Error>,
    {
        self.execute_inner(query, output, query_timeout, write_timeout, None, None)
            .await
            .map(|_| ())
    }

    /// True permits caching: cleanup completed and no cancellation was delivered.
    pub(crate) async fn execute_with_cancel<S>(
        &self,
        query: &ParsedQuery,
        output: &mut S,
        query_timeout: Duration,
        write_timeout: Duration,
        cancel: futures::channel::oneshot::Receiver<()>,
    ) -> PgWireResult<bool>
    where
        S: Sink<PgWireBackendMessage> + Unpin + Send,
        S::Error: fmt::Debug,
        PgWireError: From<S::Error>,
    {
        self.execute_inner(
            query,
            output,
            query_timeout,
            write_timeout,
            Some(cancel),
            None,
        )
        .await
    }

    async fn execute_inner<S>(
        &self,
        query: &ParsedQuery,
        output: &mut S,
        query_timeout: Duration,
        write_timeout: Duration,
        cancel: Option<futures::channel::oneshot::Receiver<()>>,
        discovery_setup: Option<&str>,
    ) -> PgWireResult<bool>
    where
        S: Sink<PgWireBackendMessage> + Unpin + Send,
        S::Error: fmt::Debug,
        PgWireError: From<S::Error>,
    {
        query
            .validate_read_only()
            .map_err(|error| error_response("0A000", &error.to_string()))?;
        let object = timeout(ACQUIRE_TIMEOUT, self.pool.get())
            .await
            .map_err(|_| error_response("53300", "PostgreSQL pool acquisition timed out"))?
            .map_err(|error| match error {
                deadpool::managed::PoolError::Timeout(_) => {
                    error_response("53300", "PostgreSQL pool acquisition timed out")
                }
                _ => error_response("08006", "PostgreSQL connection unavailable"),
            })?;
        let mut lease = Lease(Some(object));
        let connection = lease.0.as_mut().unwrap();
        connection.clean = false;
        connection.query_complete = false;
        connection.cancel_requested = false;
        // The gateway owns execution policy; PostgreSQL also enforces that budget.
        // Local settings end at rollback and cannot leak to the next pool borrower.
        let transaction = format!(
            "BEGIN READ ONLY; SET LOCAL statement_timeout = '{}ms'; SET LOCAL idle_in_transaction_session_timeout = '{}ms'",
            query_timeout.as_millis(),
            query_timeout.max(write_timeout).as_millis(),
        );
        timeout(CONNECT_TIMEOUT, connection.control(&transaction))
            .await
            .map_err(|_| error_response("08006", "PostgreSQL transaction setup timed out"))??;
        if connection.status != TransactionStatus::Transaction {
            return Err(error_response(
                "08006",
                "unexpected PostgreSQL transaction state",
            ));
        }
        if let Some(setup) = discovery_setup {
            timeout(CONNECT_TIMEOUT, connection.control(setup))
                .await
                .map_err(|_| error_response("08006", "PostgreSQL metadata setup timed out"))??;
        }
        let completion = connection
            .stream_query(query.sql(), output, query_timeout, write_timeout, cancel)
            .await;
        let cancel_requested = connection.cancel_requested;
        // A cancelled future may have unread protocol messages. Its lease is
        // discarded; it must never enter the pool's idle queue.
        if !connection.query_complete {
            return completion.map(|_| false);
        }
        if connection.status != TransactionStatus::Transaction
            && connection.status != TransactionStatus::Error
        {
            return Err(error_response(
                "08006",
                "unexpected PostgreSQL transaction state",
            ));
        }
        let cleanup = timeout(CLEANUP_TIMEOUT, async {
            connection.control("ROLLBACK").await?;
            connection.control("DISCARD ALL").await?;
            connection.control(SESSION_DEFAULTS).await?;
            if connection.status != TransactionStatus::Idle {
                return Err(error_response(
                    "08006",
                    "PostgreSQL cleanup could not be verified",
                ));
            }
            connection.control("SELECT 1").await?;
            connection.clean = true;
            Ok(())
        })
        .await
        .map_err(|_| error_response("08006", "PostgreSQL cleanup timed out"))
        .and_then(|result| result);
        if let Err(error) = cleanup {
            return Err(completion.err().unwrap_or(error));
        }
        // Return only a verified idle connection. Completion is emitted after
        // cleanup so a failed cleanup cannot be advertised as a successful read.
        let message = match completion {
            Ok(message) => message,
            Err(error) => {
                if !cancel_requested {
                    lease.release();
                }
                return Err(error);
            }
        };
        timeout(write_timeout, output.send(message))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "frontend write timed out"))??;
        if !cancel_requested {
            lease.release();
        }
        Ok(!cancel_requested)
    }

    pub(crate) fn close(&self) {
        self.pool.close();
    }
}

/// Configured PostgreSQL routes, each with its own bounded, lazily opened pool.
/// Construction does not connect to PostgreSQL or expose credentials to hooks.
pub struct PostgresBackends {
    pools: BTreeMap<BackendSelection, PostgresBackend>,
}

impl PostgresBackends {
    pub(crate) fn discovery_route(
        &self,
        route: Option<&BackendSelection>,
    ) -> io::Result<BackendSelection> {
        if let Some(route) = route {
            return self
                .pools
                .contains_key(route)
                .then(|| route.clone())
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "discovery pair is not configured",
                    )
                });
        }
        let first =
            self.pools.keys().next().ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "no discovery source")
            })?;
        if self.pools.keys().any(|route| route.target != first.target) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "supply the discovery pair for multiple targets",
            ));
        }
        Ok(first.clone())
    }
    pub fn new(
        configurations: BTreeMap<BackendSelection, BackendConfig>,
        settings: BackendSettings,
    ) -> io::Result<Self> {
        let capacity = settings.max_connections_per_pool;
        if configurations.is_empty()
            || capacity == 0
            || settings.max_connections_total == 0
            || configurations
                .keys()
                .any(|route| route.target.is_empty() || route.backend_login.is_empty())
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "routes and pool limits must be nonempty and positive",
            ));
        }
        if configurations
            .len()
            .checked_mul(capacity)
            .is_none_or(|total| total > settings.max_connections_total)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "sum of route pool capacities exceeds max_connections_total; reduce max_connections_per_pool or increase max_connections_total",
            ));
        }
        let pools = configurations
            .into_iter()
            .map(|(route, config)| {
                PostgresBackend::with_capacity(config, capacity).map(|pool| (route, pool))
            })
            .collect::<io::Result<_>>()?;
        Ok(Self { pools })
    }

    pub(crate) fn get(&self, route: &BackendSelection) -> Option<&PostgresBackend> {
        self.pools.get(route)
    }

    pub(crate) fn close(&self) {
        for pool in self.pools.values() {
            pool.close();
        }
    }
}

fn error_response(code: &str, message: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".into(),
        code.into(),
        message.into(),
    )))
}
