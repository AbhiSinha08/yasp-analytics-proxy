//! Resolve application routing decisions to configured source pools.
//! Source connections remain owned by backend adapters.

use super::protocol_error;
use crate::{
    backend::{
        PostgresBackend, PostgresBackends, discovery::DiscoverySource, postgres::BackendRoute,
    },
    hooks::{RoutedSelector, SelectionError, SelectionRequest},
};
use pgwire::error::PgWireResult;
use std::sync::Arc;

pub(super) enum Execution {
    Fixed(Arc<PostgresBackend>),
    Routed {
        backends: Arc<PostgresBackends>,
        selector: RoutedSelector,
        discovery: crate::hooks::BackendSelection,
        discovery_compute: Option<String>,
    },
}

impl Execution {
    pub(super) fn discovery(&self) -> DiscoverySource<'_> {
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
        };
        backend.discovery()
    }
    pub(super) async fn select(
        &self,
        request: &SelectionRequest<'_>,
    ) -> PgWireResult<BackendRoute<'_>> {
        match self {
            Self::Fixed(backend) => Ok(backend.route()),
            Self::Routed {
                backends, selector, ..
            } => {
                let decision = selector
                    .select(request)
                    .await
                    .map_err(|error| match error {
                        SelectionError::Denied => {
                            protocol_error("ERROR", "42501", "backend selection denied")
                        }
                        SelectionError::Failed => {
                            protocol_error("ERROR", "XX000", "backend selection failed")
                        }
                    })?;
                let route = backends.resolve(&decision).map_err(|_| {
                    protocol_error("ERROR", "42501", "backend selection is not configured")
                })?;
                tracing::debug!(
                    query_id = request.query_id,
                    client_id = request.client_id,
                    target = %route.selection().target,
                    backend_login = %route.selection().backend_login,
                    "backend selected"
                );
                Ok(route)
            }
        }
    }

    pub(super) fn close(&self) {
        match self {
            Self::Fixed(backend) => backend.close(),
            Self::Routed {
                backends, selector, ..
            } => {
                selector.shutdown();
                backends.close();
            }
        }
    }

    pub(super) fn stop_hooks(&self) {
        if let Self::Routed { selector, .. } = self {
            selector.shutdown();
        }
    }
}
