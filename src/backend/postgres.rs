//! PostgreSQL routes and read-only execution. Connections own wire I/O, while
//! the pool returns only verified idle sessions for reuse.

mod catalog;
mod connection;
mod execution;
mod pool;
mod result;
pub(crate) use result::{NativeError, Output};
mod transaction;

pub(crate) use transaction::PostgresTransaction;

use super::ConnectionBudget;
use pool::{InstanceState, PgManager};

use crate::{
    config::{BackendConfig, BackendSettings, GatewaySettings},
    hooks::{BackendDecision, BackendSelection, SelectionError},
    query::ParsedQuery,
};
use deadpool::managed::Pool;
use futures::{Sink, SinkExt};
use pgwire::{
    error::{ErrorInfo, PgWireError, PgWireResult},
    messages::PgWireBackendMessage,
};
use std::{
    collections::{BTreeMap, HashMap},
    fmt, io,
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::{timeout, timeout_at},
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(10);
const SESSION_DEFAULTS: &str = "SET client_encoding = 'UTF8'; SET standard_conforming_strings = on; SET DateStyle = 'ISO, MDY'; SET TimeZone = 'UTC'; SET IntervalStyle = 'postgres'; SET bytea_output = 'hex'; SET extra_float_digits = 3; SET default_transaction_read_only = on; SET default_transaction_isolation = 'read committed'; SET client_connection_check_interval = '1s'";

/// One target/login pool. Clones share the pool, admission counters and budgets.
/// No organization routing or callback policy is embedded.
#[derive(Clone)]
pub struct PostgresBackend {
    pool: Pool<PgManager>,
    target: String,
    database_user: String,
    selection: BackendSelection,
    compute_type: Option<String>,
    compute: String,
    default_compute_type: bool,
    instance: Arc<InstanceState>,
    pool_slots: Arc<Semaphore>,
    budget: Arc<ConnectionBudget>,
}

impl PostgresBackend {
    pub(crate) fn discovery(&self) -> super::discovery::DiscoverySource<'_> {
        super::discovery::DiscoverySource::Postgres(self)
    }
    pub fn new(config: BackendConfig) -> io::Result<Self> {
        let budget = ConnectionBudget::new(BackendSettings::default().max_connections_total)?;
        Self::with_budget(config, budget)
    }

    /// A fixed source sharing occupied capacity with other backend adapters.
    pub fn with_budget(config: BackendConfig, budget: Arc<ConnectionBudget>) -> io::Result<Self> {
        let settings = BackendSettings::default();
        let total = budget.limit();
        let instance = Arc::new(InstanceState::new(&budget, total, total)?);
        Self::build(
            PostgresBinding {
                selection: BackendSelection {
                    target: "default".into(),
                    backend_login: "default".into(),
                },
                compute_type: None,
                compute: "default".into(),
                default_compute_type: true,
                max_connections: total,
                max_in_flight: total,
                config,
            },
            settings.max_connections_per_pool,
            budget,
            instance,
        )
    }

    fn build(
        binding: PostgresBinding,
        capacity: usize,
        budget: Arc<ConnectionBudget>,
        instance: Arc<InstanceState>,
    ) -> io::Result<Self> {
        let PostgresBinding {
            selection,
            compute_type,
            compute,
            default_compute_type,
            config,
            ..
        } = binding;
        let target = format!("{}:{}/{}", config.host, config.port, config.database);
        let database_user = config.username.clone();
        let pool = pool::new(config, capacity, instance.clone())?;
        Ok(Self {
            pool,
            target,
            database_user,
            selection,
            compute_type,
            compute,
            default_compute_type,
            instance,
            pool_slots: Arc::new(Semaphore::new(capacity)),
            budget,
        })
    }

    pub(crate) fn target(&self) -> &str {
        &self.target
    }

    pub(crate) fn database_user(&self) -> &str {
        &self.database_user
    }

    pub fn compute(&self) -> &str {
        &self.compute
    }

    pub fn compute_type(&self) -> Option<&str> {
        self.compute_type.as_deref()
    }

    pub fn route(&self) -> BackendRoute<'_> {
        BackendRoute {
            selection: self.selection.clone(),
            candidates: vec![self],
        }
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
        query
            .validate_read_only()
            .map_err(|error| error_response("0A000", &error.to_string()))?;
        self.route()
            .acquire(GatewaySettings::default().queue_timeout())
            .await?
            .execute(query, output, query_timeout, write_timeout, None)
            .await
            .map(|_| ())
    }

    async fn acquire_if_usable(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<Option<BackendLease<'_>>, PgWireError> {
        if self.instance.capacity.is_closed() {
            return Err(error_response("08006", "PostgreSQL binding is closed"));
        }
        // Status only guides preference; `get` and the physical budget enforce use.
        let idle_available = self.pool.status().available > 0;
        let Ok(in_flight) = self.instance.in_flight.clone().try_acquire_owned() else {
            return Ok(None);
        };
        let Ok(pool_slot) = self.pool_slots.clone().try_acquire_owned() else {
            return Ok(None);
        };
        if !idle_available && !self.instance.capacity.may_open() {
            return Ok(None);
        }
        let object = match timeout_at(deadline, self.pool.get()).await {
            Ok(Ok(object)) => object,
            Ok(Err(deadpool::managed::PoolError::Backend(PgWireError::UserError(info))))
                if info.code == "53300" =>
            {
                return Ok(None);
            }
            Ok(Err(_)) => {
                return Err(error_response("08006", "PostgreSQL connection unavailable"));
            }
            Err(_) => {
                return Err(error_response(
                    "53300",
                    "PostgreSQL pool acquisition timed out",
                ));
            }
        };
        Ok(Some(BackendLease {
            backend: self,
            object: Some(object),
            _in_flight: AdmissionPermit::new(in_flight, self.budget.clone()),
            _pool_slot: AdmissionPermit::new(pool_slot, self.budget.clone()),
        }))
    }

    pub(crate) fn close(&self) {
        self.pool.close();
        self.instance.capacity.close();
        self.budget.changed.notify_waiters();
    }
}

/// Configured PostgreSQL routes, each with its own bounded, lazily opened pool.
/// Construction does not connect to PostgreSQL or expose credentials to hooks.
pub struct PostgresBackends {
    #[cfg(feature = "snowflake")]
    pub(crate) budget: Arc<ConnectionBudget>,
    pools: BTreeMap<BackendSelection, Vec<PostgresBackend>>,
}

/// One configured source login and one concrete compute instance.
pub struct PostgresBinding {
    pub selection: BackendSelection,
    pub compute_type: Option<String>,
    pub compute: String,
    pub default_compute_type: bool,
    pub max_connections: usize,
    pub max_in_flight: usize,
    pub config: BackendConfig,
}

pub struct BackendRoute<'a> {
    selection: BackendSelection,
    candidates: Vec<&'a PostgresBackend>,
}

impl<'a> BackendRoute<'a> {
    pub fn selection(&self) -> &BackendSelection {
        &self.selection
    }

    /// Cache lookup can use the configured first instance without reserving work.
    pub fn preferred(&self) -> &PostgresBackend {
        self.candidates[0]
    }

    pub(crate) fn contains(&self, backend: &PostgresBackend) -> bool {
        self.candidates
            .iter()
            .any(|candidate| std::ptr::eq(*candidate, backend))
    }

    pub async fn acquire(&self, queue_timeout: Duration) -> PgWireResult<BackendLease<'a>> {
        let deadline = tokio::time::Instant::now() + queue_timeout;
        let budget = &self.candidates[0].budget;
        loop {
            let notified = budget.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let mut last_error = None;
            for backend in &self.candidates {
                match backend.acquire_if_usable(deadline).await {
                    Ok(Some(lease)) => return Ok(lease),
                    Ok(None) => {}
                    Err(error) => last_error = Some(error),
                }
            }
            if let Some(error) = last_error {
                return Err(error);
            }
            timeout_at(deadline, notified)
                .await
                .map_err(|_| error_response("53300", "PostgreSQL connection capacity timed out"))?;
        }
    }
}

pub struct BackendLease<'a> {
    backend: &'a PostgresBackend,
    object: Option<deadpool::managed::Object<PgManager>>,
    _in_flight: AdmissionPermit,
    _pool_slot: AdmissionPermit,
}

struct AdmissionPermit {
    permit: Option<OwnedSemaphorePermit>,
    budget: Arc<ConnectionBudget>,
}

impl AdmissionPermit {
    fn new(permit: OwnedSemaphorePermit, budget: Arc<ConnectionBudget>) -> Self {
        Self {
            permit: Some(permit),
            budget,
        }
    }
}

impl Drop for AdmissionPermit {
    fn drop(&mut self) {
        drop(self.permit.take());
        self.budget.changed.notify_waiters();
    }
}

impl<'a> BackendLease<'a> {
    pub fn backend(&self) -> &PostgresBackend {
        self.backend
    }

    pub async fn execute<S>(
        self,
        query: &ParsedQuery,
        output: &mut S,
        query_timeout: Duration,
        write_timeout: Duration,
        cancel: Option<futures::channel::oneshot::Receiver<()>>,
    ) -> PgWireResult<bool>
    where
        S: Sink<PgWireBackendMessage> + Unpin + Send,
        S::Error: fmt::Debug,
        PgWireError: From<S::Error>,
    {
        self.execute_with_session(query, output, query_timeout, write_timeout, cancel, None)
            .await
    }

    pub(crate) async fn execute_with_session<S>(
        self,
        query: &ParsedQuery,
        output: &mut S,
        query_timeout: Duration,
        write_timeout: Duration,
        cancel: Option<futures::channel::oneshot::Receiver<()>>,
        session: Option<&crate::discovery::SessionSettings>,
    ) -> PgWireResult<bool>
    where
        S: Sink<PgWireBackendMessage> + Unpin + Send,
        S::Error: fmt::Debug,
        PgWireError: From<S::Error>,
    {
        execution::validate_input(query)?;
        query
            .validate_read_only()
            .map_err(|error| error_response("0A000", &error.to_string()))?;
        let mut transaction = self
            .begin(query_timeout, query_timeout.max(write_timeout))
            .await?;
        let completion = transaction
            .execute(query, output, query_timeout, write_timeout, cancel, session)
            .await;
        let reusable = transaction.usable();
        if let Err(error) = transaction.finish(false).await {
            return Err(completion.err().unwrap_or(error));
        }
        let message = completion?;
        timeout(write_timeout, output.send(message))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "frontend write timed out"))??;
        Ok(reusable)
    }
}

impl Drop for BackendLease<'_> {
    fn drop(&mut self) {
        if let Some(object) = self.object.take() {
            let budget = object._physical.budget();
            drop(object);
            budget.changed.notify_waiters();
        }
    }
}

impl PostgresBackends {
    /// Resolve the configured metadata binding, or the sole configured target.
    pub fn discovery_route(
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
        let capacity = settings.max_connections_total;
        let bindings = configurations
            .into_iter()
            .map(|(selection, config)| PostgresBinding {
                selection,
                compute_type: None,
                compute: "default".into(),
                default_compute_type: true,
                max_connections: capacity,
                max_in_flight: capacity,
                config,
            })
            .collect();
        Self::with_bindings(bindings, settings)
    }

    pub fn with_bindings(
        bindings: Vec<PostgresBinding>,
        settings: BackendSettings,
    ) -> io::Result<Self> {
        let budget = ConnectionBudget::new(settings.max_connections_total)?;
        Self::with_bindings_and_budget(bindings, settings, budget)
    }

    /// Construct pools under an existing total. The settings must name that total.
    pub fn with_bindings_and_budget(
        bindings: Vec<PostgresBinding>,
        settings: BackendSettings,
        budget: Arc<ConnectionBudget>,
    ) -> io::Result<Self> {
        if budget.limit() != settings.max_connections_total {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "shared connection budget does not match backend.max_connections_total",
            ));
        }
        if bindings.is_empty()
            || settings.max_connections_per_pool == 0
            || settings.max_connections_per_pool > Semaphore::MAX_PERMITS
            || settings.max_connections_total == 0
            || settings.max_connections_total > Semaphore::MAX_PERMITS
            || bindings.iter().any(|binding| {
                binding.selection.target.is_empty()
                    || binding.selection.backend_login.is_empty()
                    || binding.compute.is_empty()
                    || binding.max_connections == 0
                    || binding.max_connections > Semaphore::MAX_PERMITS
                    || binding.max_in_flight == 0
                    || binding.max_in_flight > Semaphore::MAX_PERMITS
            })
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "routes and pool limits must be nonempty and valid",
            ));
        }

        let mut instances = HashMap::<String, Arc<InstanceState>>::new();
        let mut pools = BTreeMap::<BackendSelection, Vec<PostgresBackend>>::new();
        let mut identities = std::collections::BTreeSet::new();
        for binding in bindings {
            let identity = format!(
                "{}\0{}\0{}",
                binding.selection.target,
                binding.compute_type.as_deref().unwrap_or(""),
                binding.compute
            );
            let pool_identity = (
                binding.selection.clone(),
                binding.compute_type.clone(),
                binding.compute.clone(),
            );
            if !identities.insert(pool_identity) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "duplicate backend compute binding",
                ));
            }
            let instance = if let Some(instance) = instances.get(&identity) {
                if instance.max_connections != binding.max_connections
                    || instance.max_in_flight != binding.max_in_flight
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "inconsistent limits for a shared compute instance",
                    ));
                }
                instance.clone()
            } else {
                let instance = Arc::new(InstanceState::new(
                    &budget,
                    binding.max_connections,
                    binding.max_in_flight,
                )?);
                instances.insert(identity, instance.clone());
                instance
            };
            let route = binding.selection.clone();
            let backend = PostgresBackend::build(
                binding,
                settings.max_connections_per_pool,
                budget.clone(),
                instance,
            )?;
            pools.entry(route).or_default().push(backend);
        }
        Ok(Self {
            pools,
            #[cfg(feature = "snowflake")]
            budget,
        })
    }

    pub fn resolve(&self, decision: &BackendDecision) -> Result<BackendRoute<'_>, SelectionError> {
        let targets = self
            .pools
            .keys()
            .map(|route| route.target.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        let target = match decision.target.as_deref() {
            Some(target) => target,
            None if targets.len() == 1 => *targets.iter().next().unwrap(),
            None => return Err(SelectionError::Failed),
        };
        let target_bindings = self
            .pools
            .iter()
            .filter(|(route, _)| route.target == target)
            .collect::<Vec<_>>();
        if target_bindings.is_empty() {
            return Err(SelectionError::Failed);
        }
        let logins = target_bindings
            .iter()
            .map(|(route, _)| route.backend_login.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        let login = match decision.backend_login.as_deref() {
            Some(login) => login,
            None if logins.len() == 1 => *logins.iter().next().unwrap(),
            None => return Err(SelectionError::Failed),
        };
        let selection = BackendSelection {
            target: target.to_owned(),
            backend_login: login.to_owned(),
        };
        let candidates = self.pools.get(&selection).ok_or(SelectionError::Failed)?;
        let has_compute = target_bindings
            .iter()
            .flat_map(|(_, backends)| backends.iter())
            .any(|backend| backend.compute_type.is_some());
        let desired_type = if let Some(compute_type) = decision.compute_type.as_deref() {
            Some(compute_type)
        } else if !has_compute {
            None
        } else {
            let target_types = target_bindings
                .iter()
                .flat_map(|(_, backends)| backends.iter());
            let defaults = target_types
                .clone()
                .filter(|backend| backend.default_compute_type)
                .filter_map(|backend| backend.compute_type.as_deref())
                .collect::<std::collections::BTreeSet<_>>();
            if defaults.len() == 1 {
                defaults.iter().next().copied()
            } else if defaults.is_empty() {
                let types = target_types
                    .filter_map(|backend| backend.compute_type.as_deref())
                    .collect::<std::collections::BTreeSet<_>>();
                if types.len() == 1 {
                    types.iter().next().copied()
                } else {
                    return Err(SelectionError::Failed);
                }
            } else {
                return Err(SelectionError::Failed);
            }
        };
        let selected = candidates
            .iter()
            .filter(|backend| backend.compute_type.as_deref() == desired_type)
            .collect::<Vec<_>>();
        if selected.is_empty() {
            return Err(SelectionError::Failed);
        }
        Ok(BackendRoute {
            selection,
            candidates: selected,
        })
    }

    pub fn get_discovery(
        &self,
        route: &BackendSelection,
        compute: Option<&str>,
    ) -> Option<&PostgresBackend> {
        let candidates = self.pools.get(route)?;
        match compute {
            Some(compute) => candidates.iter().find(|backend| backend.compute == compute),
            None if candidates.len() == 1 => candidates.first(),
            None => None,
        }
    }

    #[cfg(feature = "snowflake")]
    pub(crate) fn selections(&self) -> impl Iterator<Item = &BackendSelection> {
        self.pools.keys()
    }

    pub(crate) fn close(&self) {
        for pools in self.pools.values() {
            for pool in pools {
                pool.close();
            }
        }
    }
}

pub(super) fn error_response(code: &str, message: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".into(),
        code.into(),
        message.into(),
    )))
}
