//! Dispatch classified metadata requests to their configured source engine.
//! Source handlers own forwarding, translation, and native response values.

use super::{
    PostgresBackend,
    execution::Route,
    postgres,
    result::{ExecutionError, ExecutionResult, ResultEvent},
};
use crate::{
    discovery::{DiscoveryKind, SessionSettings},
    query::ParsedQuery,
};
use futures::{Sink, channel::oneshot};
use std::time::Duration;
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
    pub(crate) fn catalog_identity(&self) -> crate::catalog::SourceIdentity {
        match self {
            Self::Postgres(source) => crate::catalog::SourceIdentity {
                dialect: crate::query::SqlDialect::Postgresql,
                endpoint: source.target().into(),
                principal: source.database_user().into(),
                role: None,
                warehouse: None,
            },
        }
    }
    pub(crate) fn route(&self) -> Route<'a> {
        match self {
            Self::Postgres(backend) => Route::Postgres(backend.route()),
        }
    }

    pub(crate) fn prepare(&self, query: &ParsedQuery) -> ExecutionResult<ParsedQuery> {
        match self {
            Self::Postgres(_) => postgres::prepare_discovery(query)
                .map_err(|_| ExecutionError::rejected("54000", "metadata query exceeds its bound")),
        }
    }
    /// The source handler supplies the PostgreSQL version advertised to clients.
    pub(crate) async fn server_version(&self) -> ExecutionResult<String> {
        match self {
            Self::Postgres(backend) => backend.server_version().await.map_err(Into::into),
        }
    }

    /// A handler may serve metadata locally or use its source executor. Responses
    /// cross shared orchestration as typed events; source values stay adapter-owned.
    pub(crate) async fn execute<S>(
        &self,
        request: DiscoveryRequest<'_>,
        output: &mut S,
        cancel: oneshot::Receiver<()>,
    ) -> ExecutionResult<()>
    where
        S: Sink<ResultEvent, Error = ExecutionError> + Unpin + Send,
    {
        match self {
            Self::Postgres(backend) => {
                postgres::discover(backend, request, &mut postgres::Output(output), cancel)
                    .await
                    .map_err(Into::into)
            }
        }
    }
}
