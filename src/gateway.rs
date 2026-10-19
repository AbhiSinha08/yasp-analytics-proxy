//! Shared pgwire frontend entry points for PostgreSQL and Redshift profiles.
//! Execution policy and source-native sessions/results have separate owners.

mod frontend;
mod metadata;
mod query;
mod result;
mod server;
mod session;
mod transport;

pub use frontend::FrontendProfile;

use crate::backend::execution::Execution;
use crate::transport::FrameGuard;
use crate::{
    backend::{PostgresBackend, PostgresBackends},
    config::GatewayConfig,
    hooks::RoutedSelector,
};
use pgwire::{
    error::{ErrorInfo, PgWireError},
    tokio::server::PgWireMessageServerCodec,
};
use server::serve_sessions;
use std::{future::Future, io, sync::Arc};
use tokio::net::TcpListener;
use tokio_util::codec::Framed;

/// Serve on a host-owned listener with the configured TLS policy.
/// Stop accepting on shutdown, then close sessions after the configured drain timeout.
pub async fn serve(
    listener: TcpListener,
    config: GatewayConfig,
    backend: Arc<PostgresBackend>,
    shutdown: impl Future<Output = ()>,
) -> io::Result<()> {
    serve_sessions(listener, config, Execution::Fixed(backend), shutdown).await
}

/// Serve with an application-registered selector and configured route pools.
/// Decisions are checked on every query before a pool connection is acquired.
pub async fn serve_routed(
    listener: TcpListener,
    config: GatewayConfig,
    backends: Arc<PostgresBackends>,
    selector: impl Into<RoutedSelector>,
    shutdown: impl Future<Output = ()>,
) -> io::Result<()> {
    let discovery = backends.discovery_route(config.schema_discovery.as_ref())?;
    let discovery_compute = config.schema_discovery_compute.clone();
    if backends
        .get_discovery(&discovery, discovery_compute.as_deref())
        .is_none()
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "discovery requires a configured compute instance",
        ));
    }
    serve_sessions(
        listener,
        config,
        Execution::Routed {
            backends,
            selector: selector.into(),
            discovery,
            discovery_compute,
        },
        shutdown,
    )
    .await
}

type FrontendSocket = Framed<FrameGuard, PgWireMessageServerCodec<String>>;

/// Serve PostgreSQL and Snowflake bindings through one frontend and selection
/// hook. This initial mixed-engine path uses a PostgreSQL discovery authority.
#[cfg(feature = "snowflake")]
pub async fn serve_backends(
    listener: TcpListener,
    config: GatewayConfig,
    backends: Arc<crate::backend::Backends>,
    selector: impl Into<RoutedSelector>,
    shutdown: impl Future<Output = ()>,
) -> io::Result<()> {
    let discovery = backends
        .postgres
        .discovery_route(config.schema_discovery.as_ref())?;
    let discovery_compute = config.schema_discovery_compute.clone();
    if backends
        .postgres
        .get_discovery(&discovery, discovery_compute.as_deref())
        .is_none()
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "discovery requires a configured PostgreSQL binding",
        ));
    }
    serve_sessions(
        listener,
        config,
        Execution::Mixed {
            backends,
            selector: selector.into(),
            discovery,
            discovery_compute,
        },
        shutdown,
    )
    .await
}

fn protocol_error(severity: &str, code: &str, message: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        severity.into(),
        code.into(),
        message.into(),
    )))
}
