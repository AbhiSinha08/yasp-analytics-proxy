//! PostgreSQL execution, text-response caching, and physical source admission.

use super::{
    BackendRoute,
    cache::{self as response_cache, CaptureSink},
    error_response as protocol_error,
};
use crate::backend::execution::ExecutionRequest;
use futures::{Sink, channel::oneshot};
use pgwire::{
    error::{PgWireError, PgWireResult},
    messages::PgWireBackendMessage,
};
use std::fmt;

impl BackendRoute<'_> {
    pub(crate) fn validate_input(&self, query: &crate::query::ParsedQuery) -> PgWireResult<()> {
        validate_input(query)
    }

    pub(crate) async fn execute_query<S>(
        &self,
        execution: ExecutionRequest<'_, '_>,
        socket: &mut S,
        mut cancel: oneshot::Receiver<()>,
    ) -> PgWireResult<()>
    where
        S: Sink<PgWireBackendMessage> + Unpin + Send,
        S::Error: fmt::Debug,
        PgWireError: From<S::Error>,
    {
        let ExecutionRequest {
            selection: request,
            frontend_database,
            settings,
            session,
            cache,
        } = execution;
        let query = request.query;
        let query_id = request.query_id;
        let client_id = request.client_id;
        let authenticated_user = request.authenticated_user;
        let write_timeout = settings.write_timeout();
        let selected = self.selection();
        let backend = self.preferred();
        let mut buffer = None;
        if let Some(cache) = cache {
            let eligible = tokio::select! {
                biased;
                _ = &mut cancel => return Err(protocol_error("57014", "query cancelled")),
                eligible = cache.eligible(request) => eligible,
            };
            if eligible {
                let scope = response_cache::scope(
                    backend,
                    authenticated_user,
                    frontend_database,
                    settings,
                    session,
                );
                buffer = cache.reserve(&scope);
                if let Some(entry) = &mut buffer {
                    let hit = tokio::select! {
                        biased;
                        _ = &mut cancel => return Err(protocol_error("57014", "query cancelled")),
                        hit = cache.get(&selected.target, &selected.backend_login, query_id, &scope, entry) => hit,
                    };
                    if hit
                        && response_cache::valid_response(&entry.bytes, cache.max_rows_per_query())
                    {
                        tracing::debug!(query_id, client_id, database = ?backend.target(), database_user = %backend.database_user(), sql = ?crate::hooks::builtin::extractors::sql_without_observed_postgres_remark(query.sql()), "query served from cache");
                        return response_cache::replay(entry, socket, write_timeout, &mut cancel)
                            .await;
                    }
                    if hit {
                        // A corrupt payload must not be extended with a fresh response.
                        drop(buffer.take());
                        buffer = cache.reserve(&scope);
                    }
                }
                tracing::debug!(query_id, client_id, "cache miss or buffer limit reached");
            }
        }
        let lease = tokio::select! {
            biased;
            _ = &mut cancel => return Err(protocol_error("57014", "query cancelled")),
            result = self.acquire() => result?,
        };
        let backend = lease.backend();
        if buffer.is_some() {
            // Capture under the actual binding if overflow changed the cache probe's source.
            drop(buffer.take());
            let scope = response_cache::scope(
                backend,
                authenticated_user,
                frontend_database,
                settings,
                session,
            );
            buffer = cache.and_then(|cache| cache.reserve(&scope));
        }
        tracing::debug!(
            query_id,
            client_id,
            database = ?backend.target(),
            database_user = %backend.database_user(),
            target = %selected.target,
            backend_login = %selected.backend_login,
            sql = ?crate::hooks::builtin::extractors::sql_without_observed_postgres_remark(query.sql()),
            "query sent to backend"
        );
        let mut output = CaptureSink::new(socket, buffer, cache);
        let cacheable_completion = lease
            .execute_with_session(
                query,
                &mut output,
                settings.query_timeout(),
                write_timeout,
                Some(cancel),
                Some(session),
            )
            .await?;
        if cacheable_completion && let (Some(cache), Some(buffer)) = (cache, output.captured()) {
            cache
                .put(&selected.target, &selected.backend_login, query_id, buffer)
                .await;
        }
        Ok(())
    }
}

pub(super) fn validate_input(query: &crate::query::ParsedQuery) -> PgWireResult<()> {
    let options = query.input_options();
    if options.native && options.dialect != crate::query::SqlDialect::Postgresql {
        return Err(protocol_error(
            "0A000",
            "native SQL dialect does not match the selected PostgreSQL backend",
        ));
    }
    Ok(())
}

/// Replay only the bounded frontend settings; protected text settings stay fixed.
pub(super) fn session_sql(session: &crate::discovery::SessionSettings) -> String {
    format!(
        "SELECT pg_catalog.set_config('search_path', '{}', true); SET LOCAL application_name = '{}'",
        session.search_path.replace('\'', "''"),
        session.application_name.replace('\'', "''"),
    )
}
