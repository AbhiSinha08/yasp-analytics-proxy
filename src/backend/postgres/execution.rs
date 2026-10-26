//! PostgreSQL execution and physical source admission.
use super::{BackendRoute, PostgresBackend, error_response as protocol_error};
use crate::backend::execution::ExecutionRequest;
use futures::{Sink, channel::oneshot};
use pgwire::{
    error::{PgWireError, PgWireResult},
    messages::PgWireBackendMessage,
};
use std::fmt;

impl PostgresBackend {
    pub(crate) fn execution_identity(&self) -> [u8; 32] {
        crate::backend::execution::identity(&[
            "postgresql:text:v1",
            self.target(),
            self.database_user(),
            self.compute_type().unwrap_or(""),
            self.compute(),
            super::SESSION_DEFAULTS,
        ])
    }
}
impl BackendRoute<'_> {
    pub(crate) fn validate_input(&self, query: &crate::query::ParsedQuery) -> PgWireResult<()> {
        validate_input(query)
    }
    pub(crate) async fn execute_query<S>(
        &self,
        execution: ExecutionRequest<'_, '_>,
        socket: &mut S,
        mut cancel: oneshot::Receiver<()>,
    ) -> PgWireResult<Option<[u8; 32]>>
    where
        S: Sink<PgWireBackendMessage> + Unpin + Send,
        S::Error: fmt::Debug,
        PgWireError: From<S::Error>,
    {
        let ExecutionRequest {
            selection: request,
            settings,
            session,
        } = execution;
        let query = request.query;
        let selected = self.selection();
        let lease = tokio::select! {
            biased;
            _ = &mut cancel => return Err(protocol_error("57014", "query cancelled")),
            result = self.acquire(settings.queue_timeout()) => result?,
        };
        let backend = lease.backend();
        let identity = backend.execution_identity();
        tracing::debug!(
            query_id = request.query_id, client_id = request.client_id,
            database = ?backend.target(), database_user = %backend.database_user(),
            target = %selected.target, backend_login = %selected.backend_login,
            sql = ?crate::hooks::builtin::extractors::sql_without_observed_postgres_remark(query.sql()),
            "query sent to backend"
        );
        let reusable = lease
            .execute_with_session(
                query,
                socket,
                settings.query_timeout(),
                settings.write_timeout(),
                Some(cancel),
                Some(session),
            )
            .await?;
        Ok(reusable.then_some(identity))
    }
}

pub(super) fn validate_input(query: &crate::query::ParsedQuery) -> PgWireResult<()> {
    query
        .validate_read_only()
        .map_err(|_| protocol_error("0A000", "only read-only queries are supported"))
}

/// Replay only the bounded frontend settings; protected text settings stay fixed.
pub(super) fn session_sql(session: &crate::discovery::SessionSettings) -> String {
    format!(
        "SELECT pg_catalog.set_config('search_path', '{}', true); SET LOCAL application_name = '{}'",
        session.search_path.replace('\'', "''"),
        session.application_name.replace('\'', "''"),
    )
}
