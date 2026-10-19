//! Shared source schema facts. Snapshots contain definitions, never application
//! rows or authorization decisions. AST binding and masking are separate stages.

mod model;
mod rows;
pub use model::{
    Column, ColumnId, Constraint, ConstraintKind, ForeignKey, NativeType, Relation, RelationId,
    RelationKind, Snapshot,
};
pub(crate) use rows::Rows;

use crate::{backend::PostgresBackend, query::SqlDialect};
use std::{
    sync::{Arc, RwLock},
    time::{Duration, SystemTime},
};
use tokio::{
    sync::Mutex,
    task::JoinHandle,
    time::{Instant, MissedTickBehavior, timeout},
};

/// One configured discovery identity. No credentials are exposed by the catalog.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceIdentity {
    pub dialect: SqlDialect,
    pub endpoint: String,
    pub principal: String,
    pub role: Option<String>,
    pub warehouse: Option<String>,
    pub default_schema: Option<String>,
}

#[derive(Clone, Debug)]
pub struct CatalogSettings {
    /// Exact source spelling, including case. Wildcards are not interpreted.
    pub schemas: Vec<String>,
    pub refresh_interval: Duration,
    pub refresh_timeout: Duration,
    pub max_age: Duration,
    pub max_rows: usize,
    /// Bounds owned row payloads and snapshot strings, excluding allocator overhead.
    pub max_bytes: usize,
}

impl CatalogSettings {
    pub fn new(schemas: Vec<String>) -> Self {
        Self {
            schemas,
            refresh_interval: Duration::from_secs(60),
            refresh_timeout: Duration::from_secs(10),
            max_age: Duration::from_secs(300),
            max_rows: 100_000,
            max_bytes: 16 * 1024 * 1024,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), CatalogError> {
        if self.schemas.is_empty()
            || self.schemas.len() > 256
            || self
                .schemas
                .iter()
                .any(|s| s.is_empty() || s.len() > 256 || s.contains('\0'))
            || self.refresh_interval.is_zero()
            || self.refresh_timeout.is_zero()
            || self.refresh_interval > Duration::from_secs(365 * 86400)
            || self.refresh_timeout > Duration::from_secs(3600)
            || self.max_age > Duration::from_secs(365 * 86400)
            || self.max_age < self.refresh_interval
            || self.max_rows == 0
            || self.max_rows > 1_000_000
            || self.max_bytes == 0
            || self.max_bytes > 64 * 1024 * 1024
        {
            return Err(CatalogError::InvalidSettings);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum CatalogError {
    #[error("invalid catalog settings")]
    InvalidSettings,
    #[error("catalog refresh is already in progress")]
    RefreshInProgress,
    #[error("catalog source is unavailable")]
    SourceUnavailable,
    #[error("catalog refresh timed out")]
    TimedOut,
    #[error("catalog refresh was interrupted")]
    Interrupted,
    #[error("catalog exceeds the configured bound")]
    TooLarge,
    #[error("catalog source returned inconsistent metadata")]
    InvalidMetadata,
    #[error("catalog snapshot is too old; refresh is required")]
    Expired,
}

#[derive(Clone, Debug)]
pub struct RefreshStatus {
    pub source_dialect: SqlDialect,
    pub generation: u64,
    pub loaded_at: SystemTime,
    pub age: Duration,
    pub refreshing: bool,
    pub last_attempt: SystemTime,
    pub last_error: Option<CatalogError>,
}

enum Source {
    Postgres(Arc<PostgresBackend>),
    #[cfg(feature = "snowflake")]
    Snowflake(Arc<crate::backend::snowflake::SnowflakeBackend>),
}

impl Source {
    fn identity(&self) -> SourceIdentity {
        match self {
            Self::Postgres(source) => SourceIdentity {
                dialect: SqlDialect::Postgresql,
                endpoint: source.target().into(),
                principal: source.database_user().into(),
                role: None,
                warehouse: None,
                default_schema: None,
            },
            #[cfg(feature = "snowflake")]
            Self::Snowflake(source) => source.catalog_identity(),
        }
    }
    async fn read(&self, settings: &CatalogSettings) -> Result<Rows, CatalogError> {
        match self {
            Self::Postgres(source) => source.catalog_rows(settings).await,
            #[cfg(feature = "snowflake")]
            Self::Snowflake(source) => source.catalog_rows(settings).await,
        }
    }
}

struct State {
    snapshot: Arc<Snapshot>,
    loaded: Instant,
    refreshing: bool,
    last_attempt: SystemTime,
    last_error: Option<CatalogError>,
}

/// Hosts can share this handle between listeners and later AST binders. Retaining
/// an Arc<Snapshot> pins a generation even while the catalog refreshes.
pub struct Catalog {
    source: Arc<Source>,
    settings: CatalogSettings,
    state: Arc<RwLock<State>>,
    refresh: Arc<Mutex<()>>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl Catalog {
    /// Initial loading must succeed before a refresh worker starts.
    pub async fn postgres(
        source: Arc<PostgresBackend>,
        settings: CatalogSettings,
    ) -> Result<Arc<Self>, CatalogError> {
        Self::start(Source::Postgres(source), settings).await
    }
    #[cfg(feature = "snowflake")]
    pub async fn snowflake(
        source: Arc<crate::backend::snowflake::SnowflakeBackend>,
        settings: CatalogSettings,
    ) -> Result<Arc<Self>, CatalogError> {
        Self::start(Source::Snowflake(source), settings).await
    }

    async fn start(source: Source, settings: CatalogSettings) -> Result<Arc<Self>, CatalogError> {
        settings.validate()?;
        let snapshot = timeout(settings.refresh_timeout, async {
            source.read(&settings).await?.snapshot(source.identity(), 1)
        })
        .await
        .map_err(|_| CatalogError::TimedOut)??;
        let loaded_at = snapshot.loaded_at;
        let catalog = Arc::new(Self {
            source: Arc::new(source),
            settings,
            state: Arc::new(RwLock::new(State {
                snapshot: Arc::new(snapshot),
                loaded: Instant::now(),
                refreshing: false,
                last_attempt: loaded_at,
                last_error: None,
            })),
            refresh: Arc::new(Mutex::new(())),
            worker: Mutex::new(None),
        });
        // The task owns its source/state, never the Catalog handle whose Drop
        // must be able to abort an in-flight periodic refresh.
        let source = catalog.source.clone();
        let settings = catalog.settings.clone();
        let state = catalog.state.clone();
        let refresh = catalog.refresh.clone();
        let period = catalog.settings.refresh_interval;
        let task = tokio::spawn(async move {
            let mut ticks = tokio::time::interval_at(Instant::now() + period, period);
            ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                ticks.tick().await;
                if let Err(error) = refresh_source(&source, &settings, &state, &refresh).await {
                    tracing::warn!(
                        ?error,
                        "catalog refresh failed; previous generation retained"
                    );
                }
            }
        });
        *catalog.worker.lock().await = Some(task);
        Ok(catalog)
    }

    pub fn snapshot(&self) -> Result<Arc<Snapshot>, CatalogError> {
        let state = self.state.read().expect("catalog state is not poisoned");
        if state.loaded.elapsed() > self.settings.max_age {
            return Err(CatalogError::Expired);
        }
        Ok(state.snapshot.clone())
    }

    pub fn status(&self) -> RefreshStatus {
        let state = self.state.read().expect("catalog state is not poisoned");
        RefreshStatus {
            source_dialect: state.snapshot.source.dialect,
            generation: state.snapshot.generation,
            loaded_at: state.snapshot.loaded_at,
            age: state.loaded.elapsed(),
            refreshing: state.refreshing,
            last_attempt: state.last_attempt,
            last_error: state.last_error,
        }
    }

    /// Serial refreshes publish complete generations. A failure never publishes
    /// an empty or partial replacement and does not extend the previous age.
    pub async fn refresh(&self) -> Result<(), CatalogError> {
        refresh_source(&self.source, &self.settings, &self.state, &self.refresh).await
    }

    /// Stop and join the periodic task before dropping source resources. Manual
    /// refreshes and already pinned snapshots remain usable.
    pub async fn stop_refreshing(&self) {
        let mut worker = self.worker.lock().await;
        if let Some(task) = worker.as_mut() {
            task.abort();
            let _ = task.await;
        }
        worker.take();
    }
}

async fn refresh_source(
    source: &Source,
    settings: &CatalogSettings,
    state: &RwLock<State>,
    refresh: &Mutex<()>,
) -> Result<(), CatalogError> {
    let _guard = refresh
        .try_lock()
        .map_err(|_| CatalogError::RefreshInProgress)?;
    let generation = {
        let mut state = state.write().expect("catalog state is not poisoned");
        let generation = state
            .snapshot
            .generation
            .checked_add(1)
            .ok_or(CatalogError::TooLarge)?;
        state.refreshing = true;
        state.last_attempt = SystemTime::now();
        generation
    };
    let mut attempt = RefreshAttempt {
        state,
        complete: false,
    };
    let result = timeout(settings.refresh_timeout, async {
        source
            .read(settings)
            .await?
            .snapshot(source.identity(), generation)
    })
    .await
    .unwrap_or(Err(CatalogError::TimedOut));
    let mut state = state.write().expect("catalog state is not poisoned");
    state.refreshing = false;
    attempt.complete = true;
    match result {
        Ok(snapshot) => {
            state.snapshot = Arc::new(snapshot);
            state.loaded = Instant::now();
            state.last_error = None;
            Ok(())
        }
        Err(error) => {
            state.last_error = Some(error);
            Err(error)
        }
    }
}

struct RefreshAttempt<'a> {
    state: &'a RwLock<State>,
    complete: bool,
}
impl Drop for RefreshAttempt<'_> {
    fn drop(&mut self) {
        if !self.complete {
            let mut state = self.state.write().expect("catalog state is not poisoned");
            state.refreshing = false;
            state.last_error = Some(CatalogError::Interrupted);
        }
    }
}

impl Drop for Catalog {
    fn drop(&mut self) {
        if let Some(worker) = self.worker.get_mut().take() {
            worker.abort();
        }
    }
}
