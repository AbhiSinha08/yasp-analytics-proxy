//! Dispatch PostgreSQL client metadata requests to their configured source engine.
//! Source handlers own forwarding, translation, and PostgreSQL response encoding.

use super::{
    PostgresBackend,
    postgres::{self, BackendRoute},
};
use crate::{
    discovery::{DiscoveryKind, SessionSettings},
    query::ParsedQuery,
};
use futures::{Sink, channel::oneshot};
use pgwire::{
    error::{PgWireError, PgWireResult},
    messages::PgWireBackendMessage,
};
use std::{fmt, time::Duration};

/// The frontend SQL and session state remain immutable across source handling.
pub(crate) struct DiscoveryRequest<'a> {
    pub query: &'a ParsedQuery,
    pub kind: DiscoveryKind,
    pub session: &'a SessionSettings,
    pub client_id: u64,
    pub query_timeout: Duration,
    pub write_timeout: Duration,
}

/// Engine identity comes from the configured backend, independently of client SQL.
pub(crate) enum DiscoverySource<'a> {
    Postgres(&'a PostgresBackend),
}

impl<'a> DiscoverySource<'a> {
    pub(crate) fn route(&self) -> BackendRoute<'a> {
        match self {
            Self::Postgres(backend) => backend.route(),
        }
    }

    pub(crate) fn prepare(&self, query: &ParsedQuery) -> PgWireResult<ParsedQuery> {
        match self {
            Self::Postgres(_) => postgres::prepare_discovery(query)
                .map_err(|_| postgres::error_response("54000", "metadata query exceeds its bound")),
        }
    }
    /// The source handler supplies the PostgreSQL version advertised to clients.
    pub(crate) async fn server_version(&self) -> PgWireResult<String> {
        match self {
            Self::Postgres(backend) => backend.server_version().await,
        }
    }

    /// A handler may serve metadata locally or use its source executor. Responses
    /// remain PostgreSQL wire messages, regardless of the source engine's dialect.
    pub(crate) async fn execute<S>(
        &self,
        request: DiscoveryRequest<'_>,
        output: &mut S,
        cancel: oneshot::Receiver<()>,
    ) -> PgWireResult<()>
    where
        S: Sink<PgWireBackendMessage> + Unpin + Send,
        S::Error: fmt::Debug,
        PgWireError: From<S::Error>,
    {
        match self {
            Self::Postgres(backend) => postgres::discover(backend, request, output, cancel).await,
        }
    }
}
