//! Resolve application routing decisions to configured source pools.
//! Source connections remain owned by backend adapters.

use super::result::{ExecutionError, ExecutionResult, ResultEvent};
use crate::config::GatewaySettings;
use crate::discovery::SessionSettings;
use crate::query::ParsedQuery;
use crate::{
    backend::{PostgresBackend, PostgresBackends, discovery::DiscoverySource, postgres},
    hooks::{RoutedSelector, SelectionError, SelectionRequest},
};
use futures::{Sink, channel::oneshot};
use std::sync::Arc;
use std::time::Duration;

pub(crate) enum Execution {
    Fixed(Arc<PostgresBackend>),
    Routed {
        backends: Arc<PostgresBackends>,
        selector: RoutedSelector,
        discovery: crate::hooks::BackendSelection,
        discovery_compute: Option<String>,
    },
    #[cfg(feature = "snowflake")]
    Mixed {
        backends: Arc<super::Backends>,
        selector: RoutedSelector,
        discovery: crate::hooks::BackendSelection,
        discovery_compute: Option<String>,
    },
}

impl Execution {
    pub(crate) fn default_target_dialect(&self) -> Option<crate::query::SqlDialect> {
        match self {
            Self::Fixed(_) | Self::Routed { .. } => Some(crate::query::SqlDialect::Postgresql),
            #[cfg(feature = "snowflake")]
            Self::Mixed { backends, .. } => backends.default_target_dialect(),
        }
    }

    pub(crate) fn discovery(&self) -> DiscoverySource<'_> {
        let backend = match self {
            Self::Fixed(backend) => backend,
            Self::Routed {
                backends,
                discovery,
                discovery_compute,
                ..
            } => backends
                .get_discovery(discovery, discovery_compute.as_deref())
                .expect("validated discovery binding"),
            #[cfg(feature = "snowflake")]
            Self::Mixed {
                backends,
                discovery,
                discovery_compute,
                ..
            } => {
                return backends
                    .discovery(discovery, discovery_compute.as_deref())
                    .expect("validated discovery binding");
            }
        };
        backend.discovery()
    }
    pub(crate) async fn select(
        &self,
        request: &SelectionRequest<'_>,
    ) -> ExecutionResult<Route<'_>> {
        match self {
            Self::Fixed(backend) => Ok(Route::Postgres(backend.route())),
            #[cfg(feature = "snowflake")]
            Self::Mixed {
                backends, selector, ..
            } => {
                let decision = selector
                    .select(request)
                    .await
                    .map_err(|error| match error {
                        SelectionError::Denied => {
                            ExecutionError::rejected("42501", "backend selection denied")
                        }
                        SelectionError::Failed => {
                            ExecutionError::rejected("XX000", "backend selection failed")
                        }
                    })?;
                let (selected, route) = backends.resolve(&decision)?;
                tracing::debug!(query_id = request.query_id, client_id = request.client_id,
                    target = %selected.target, backend_login = %selected.backend_login, "backend selected");
                Ok(route)
            }
            Self::Routed {
                backends, selector, ..
            } => {
                let decision = selector
                    .select(request)
                    .await
                    .map_err(|error| match error {
                        SelectionError::Denied => {
                            ExecutionError::rejected("42501", "backend selection denied")
                        }
                        SelectionError::Failed => {
                            ExecutionError::rejected("XX000", "backend selection failed")
                        }
                    })?;
                let route = backends.resolve(&decision).map_err(|_| {
                    ExecutionError::rejected("42501", "backend selection is not configured")
                })?;
                tracing::debug!(
                    query_id = request.query_id,
                    client_id = request.client_id,
                    target = %route.selection().target,
                    backend_login = %route.selection().backend_login,
                    "backend selected"
                );
                Ok(Route::Postgres(route))
            }
        }
    }

    pub(crate) fn close(&self) {
        match self {
            Self::Fixed(backend) => backend.close(),
            #[cfg(feature = "snowflake")]
            Self::Mixed {
                backends, selector, ..
            } => {
                selector.shutdown();
                backends.close();
            }
            Self::Routed {
                backends, selector, ..
            } => {
                selector.shutdown();
                backends.close();
            }
        }
    }

    pub(crate) fn stop_hooks(&self) {
        if let Self::Routed { selector, .. } = self {
            selector.shutdown();
        }
        #[cfg(feature = "snowflake")]
        if let Self::Mixed { selector, .. } = self {
            selector.shutdown();
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct ExecutionRequest<'a, 'query> {
    pub selection: &'a SelectionRequest<'query>,
    pub settings: &'a GatewaySettings,
    pub session: &'a SessionSettings,
}

#[derive(Clone, Copy)]
pub(crate) struct SessionRequest<'a> {
    pub query: &'a ParsedQuery,
    pub settings: &'a GatewaySettings,
    pub session: &'a SessionSettings,
    pub client_id: u64,
}

/// Concrete source dispatch. Native objects cannot escape into shared policy.
pub(crate) enum Route<'a> {
    Postgres(postgres::BackendRoute<'a>),
    #[cfg(feature = "snowflake")]
    Snowflake(
        &'a super::snowflake::SnowflakeBackend,
        &'a crate::hooks::BackendSelection,
    ),
}

pub(crate) enum PinnedSession<'a> {
    Postgres(Box<postgres::PostgresTransaction<'a>>),
    #[cfg(feature = "snowflake")]
    Snowflake {
        backend: &'a super::snowflake::SnowflakeBackend,
        selection: &'a crate::hooks::BackendSelection,
        session: super::snowflake::session::Session,
    },
}

impl<'a> Route<'a> {
    pub(crate) fn validate_input(&self, query: &ParsedQuery) -> ExecutionResult<()> {
        match self {
            Self::Postgres(route) => route.validate_input(query).map_err(Into::into),
            #[cfg(feature = "snowflake")]
            Self::Snowflake(backend, _) => backend.validate_input(query),
        }
    }

    pub(crate) fn accepts(&self, pinned: &PinnedSession<'_>) -> bool {
        match (self, pinned) {
            (Self::Postgres(route), PinnedSession::Postgres(pinned)) => {
                route.contains(pinned.backend())
            }
            #[cfg(feature = "snowflake")]
            (
                Self::Snowflake(backend, selection),
                PinnedSession::Snowflake {
                    backend: pinned,
                    selection: pinned_selection,
                    ..
                },
            ) => selection == pinned_selection && std::ptr::eq(*backend, *pinned),
            #[cfg(feature = "snowflake")]
            _ => false,
        }
    }

    pub(crate) async fn begin(
        &self,
        settings: &GatewaySettings,
        cancel: &mut oneshot::Receiver<()>,
    ) -> ExecutionResult<PinnedSession<'a>> {
        match self {
            #[cfg(feature = "snowflake")]
            Self::Snowflake(backend, selection) => {
                let session = tokio::select! {
                    biased;
                    _ = &mut *cancel => return Err(ExecutionError::cancelled()),
                    session = backend.open(settings, true) => session?,
                };
                Ok(PinnedSession::Snowflake {
                    backend,
                    selection,
                    session,
                })
            }
            Self::Postgres(route) => {
                let lease = tokio::select! {
                    biased;
                    _ = &mut *cancel => return Err(ExecutionError::cancelled()),
                    lease = route.acquire(settings.queue_timeout()) => lease?,
                };
                let pinned = tokio::select! {
                    biased;
                    _ = cancel => return Err(ExecutionError::cancelled()),
                    pinned = lease.begin(settings.query_timeout(), Duration::from_millis(u64::from(settings.read_timeout_ms))) => pinned?,
                };
                Ok(PinnedSession::Postgres(Box::new(pinned)))
            }
        }
    }

    pub(crate) fn selection(&self) -> &crate::hooks::BackendSelection {
        match self {
            Self::Postgres(route) => route.selection(),
            #[cfg(feature = "snowflake")]
            Self::Snowflake(_, selected) => selected,
        }
    }

    pub(crate) fn identity(&self) -> [u8; 32] {
        match self {
            Self::Postgres(route) => route.preferred().execution_identity(),
            #[cfg(feature = "snowflake")]
            Self::Snowflake(backend, _) => backend.execution_identity(),
        }
    }

    pub(crate) fn validate_session(&self, session: &SessionSettings) -> ExecutionResult<()> {
        match self {
            Self::Postgres(_) => {
                let _ = session;
                Ok(())
            }
            #[cfg(feature = "snowflake")]
            Self::Snowflake(_, _) => super::snowflake::validate_session(session),
        }
    }

    /// Return the actual source identity only after a complete response and
    /// verified cleanup. A successful but interrupted source is not reusable.
    pub(crate) async fn execute<S>(
        &self,
        request: ExecutionRequest<'_, '_>,
        output: &mut S,
        cancel: oneshot::Receiver<()>,
    ) -> ExecutionResult<Option<[u8; 32]>>
    where
        S: Sink<ResultEvent, Error = ExecutionError> + Unpin + Send,
    {
        match self {
            #[cfg(feature = "snowflake")]
            Self::Snowflake(backend, _) => backend.execute_query(request, output, cancel).await,
            Self::Postgres(route) => route
                .execute_query(request, &mut postgres::Output(output), cancel)
                .await
                .map_err(Into::into),
        }
    }
}

impl PinnedSession<'_> {
    pub(crate) fn usable(&self) -> bool {
        match self {
            Self::Postgres(pinned) => pinned.usable(),
            #[cfg(feature = "snowflake")]
            Self::Snowflake { session, .. } => session.usable(),
        }
    }
    pub(crate) async fn finish(self, commit: bool) -> ExecutionResult<()> {
        match self {
            Self::Postgres(pinned) => pinned.finish(commit).await.map_err(Into::into),
            #[cfg(feature = "snowflake")]
            Self::Snowflake { session, .. } => session.finish(commit).await,
        }
    }
    pub(crate) async fn execute<S>(
        &mut self,
        request: SessionRequest<'_>,
        output: &mut S,
        cancel: oneshot::Receiver<()>,
    ) -> ExecutionResult<ResultEvent>
    where
        S: Sink<ResultEvent, Error = ExecutionError> + Unpin + Send,
    {
        let SessionRequest {
            query,
            settings,
            session,
            client_id,
        } = request;
        match self {
            #[cfg(feature = "snowflake")]
            Self::Snowflake {
                backend,
                session: native,
                ..
            } => {
                backend.validate_input(query)?;
                backend.log_query(query, client_id);
                native
                    .execute(query, output, settings, session, cancel)
                    .await
            }
            Self::Postgres(pinned) => {
                tracing::debug!(query_id = query.query_id(), client_id,
                    database = ?pinned.backend().target(), database_user = %pinned.backend().database_user(),
                    sql = query.sql(), "query requested on pinned PostgreSQL source");
                pinned
                    .execute(
                        query,
                        &mut postgres::Output(output),
                        settings.query_timeout(),
                        settings.write_timeout(),
                        Some(cancel),
                        Some(session),
                    )
                    .await
                    .map(Into::into)
                    .map_err(Into::into)
            }
        }
    }
}

/// Fingerprint the effective source and its fixed execution settings without
/// retaining credentials in cache metadata.
pub(crate) fn identity(values: &[&str]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut hash = Sha256::new();
    for value in values {
        hash.update((value.len() as u64).to_be_bytes());
        hash.update(value.as_bytes());
    }
    hash.finalize().into()
}
