//! Resolve application routing decisions to configured source pools.
//! Source connections remain owned by backend adapters.

use super::protocol_error;
use crate::{
    backend::{PostgresBackend, PostgresBackends, discovery::DiscoverySource},
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
    },
}

impl Execution {
    pub(super) fn discovery(&self) -> DiscoverySource<'_> {
        let backend = match self {
            Self::Fixed(backend) => backend,
            Self::Routed {
                backends,
                discovery,
                ..
            } => backends.get(discovery).expect("validated discovery pair"),
        };
        backend.discovery()
    }
    pub(super) async fn select(
        &self,
        request: &SelectionRequest<'_>,
    ) -> PgWireResult<&PostgresBackend> {
        match self {
            Self::Fixed(backend) => Ok(backend),
            Self::Routed {
                backends, selector, ..
            } => {
                let route = selector
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
                let backend = backends.get(&route).ok_or_else(|| {
                    protocol_error("ERROR", "42501", "backend selection is not configured")
                })?;
                tracing::debug!(
                    query_id = request.query_id,
                    client_id = request.client_id,
                    target = %route.target,
                    backend_login = %route.backend_login,
                    "backend selected"
                );
                Ok(backend)
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
