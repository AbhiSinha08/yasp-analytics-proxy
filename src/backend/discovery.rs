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
    #[cfg(feature = "snowflake")]
    Snowflake(
        &'a super::snowflake::SnowflakeBackend,
        &'a crate::hooks::BackendSelection,
    ),
}

impl<'a> DiscoverySource<'a> {
    pub(crate) fn catalog_identity(&self) -> crate::catalog::SourceIdentity {
        match self {
            #[cfg(feature = "snowflake")]
            Self::Snowflake(source, _) => source.catalog_identity(),
            Self::Postgres(source) => crate::catalog::SourceIdentity {
                dialect: crate::query::SqlDialect::Postgresql,
                endpoint: source.target().into(),
                principal: source.database_user().into(),
                role: None,
                warehouse: None,
                default_schema: None,
            },
        }
    }
    pub(crate) fn route(&self) -> Route<'a> {
        match self {
            #[cfg(feature = "snowflake")]
            Self::Snowflake(backend, selection) => Route::Snowflake(backend, selection),
            Self::Postgres(backend) => Route::Postgres(backend.route()),
        }
    }

    pub(crate) fn prepare(&self, query: &ParsedQuery) -> ExecutionResult<ParsedQuery> {
        match self {
            #[cfg(feature = "snowflake")]
            Self::Snowflake(_, _) => Err(ExecutionError::rejected(
                "0A000",
                "metadata query is outside the virtual catalog compatibility surface",
            )),
            Self::Postgres(_) => postgres::prepare_discovery(query)
                .map_err(|_| ExecutionError::rejected("54000", "metadata query exceeds its bound")),
        }
    }
    /// PostgreSQL preserves its source version; foreign sources use the frontend profile.
    pub(crate) async fn server_version(&self) -> ExecutionResult<String> {
        match self {
            #[cfg(feature = "snowflake")]
            Self::Snowflake(_, _) => Ok("18.0".into()),
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
            #[cfg(feature = "snowflake")]
            Self::Snowflake(_, _) => Err(ExecutionError::rejected(
                "0A000",
                "metadata query is outside the virtual catalog compatibility surface",
            )),
            Self::Postgres(backend) => {
                postgres::discover(backend, request, &mut postgres::Output(output), cancel)
                    .await
                    .map_err(Into::into)
            }
        }
    }
}
