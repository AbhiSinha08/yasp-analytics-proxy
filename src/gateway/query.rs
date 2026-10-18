//! Ordered simple-query stages: setup, metadata, context, routing, cache, execution.

use super::{
    FrontendSocket, protocol_error, routing::Execution, server::Credentials,
    transaction::Transaction,
};
use crate::{
    backend::{
        discovery::DiscoveryRequest,
        postgres::cache::{self as response_cache, CaptureSink},
    },
    config::GatewaySettings,
    discovery::{self, SessionSettings, SetupResponse},
    hooks::{ContextRequest, SelectionRequest},
    query::{ParsedQuery, QueryError},
};
use futures::{SinkExt, channel::oneshot};
use pgwire::{
    error::PgWireResult,
    messages::{
        PgWireBackendMessage,
        data::{DataRow, FieldDescription, RowDescription},
        response::{CommandComplete, EmptyQueryResponse},
        startup::ParameterStatus,
    },
};
use serde_json::{Map, Value};
use std::{io, time::Duration};
use tokio::time::timeout;

/// Borrow one frontend's execution services and mutable local setup state.
/// A query cannot replace the authenticated identity or the configured limits.
pub(super) struct QuerySession<'a, 'backend> {
    pub(super) execution: &'backend Execution,
    pub(super) settings: &'a GatewaySettings,
    pub(super) client_id: u64,
    pub(super) source: &'a Credentials,
    pub(super) session: &'a mut SessionSettings,
    pub(super) transaction: &'a mut Transaction<'backend>,
}

impl<'backend> QuerySession<'_, 'backend> {
    pub(super) async fn execute(
        &mut self,
        socket: &mut FrontendSocket,
        sql: &str,
        cancel: oneshot::Receiver<()>,
    ) -> PgWireResult<()> {
        let query = match ParsedQuery::parse_with_limit(sql, self.settings.max_sql_bytes) {
            Ok(query) => query,
            Err(QueryError::Empty) => {
                timeout(
                    self.settings.write_timeout(),
                    socket.send(PgWireBackendMessage::EmptyQueryResponse(
                        EmptyQueryResponse::new(),
                    )),
                )
                .await
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::TimedOut, "empty response timed out")
                })??;
                return Ok(());
            }
            Err(error) => {
                tracing::error!(client_id = self.client_id, "query parsing failed");
                let code = match error {
                    QueryError::Syntax => "42601",
                    QueryError::TooLarge | QueryError::TooComplex => "54000",
                    _ => "0A000",
                };
                return Err(protocol_error("ERROR", code, &error.to_string()));
            }
        };
        let query_id = query.query_id();
        let client_id = self.client_id;
        tracing::debug!(query_id, client_id, sql = ?query.sql(), "query received");
        let result = self.execute_parsed(socket, &query, cancel).await;
        if result.is_err() {
            tracing::error!(query_id, client_id, "query failed");
        }
        result
    }

    async fn execute_parsed(
        &mut self,
        socket: &mut FrontendSocket,
        query: &ParsedQuery,
        mut cancel: oneshot::Receiver<()>,
    ) -> PgWireResult<()> {
        let write_timeout = self.settings.write_timeout();
        let application_name = self.session.application_name.clone();
        let control = self
            .transaction
            .control(query.statement(), self.session)
            .await;
        if application_name != self.session.application_name {
            timeout(
                write_timeout,
                socket.send(PgWireBackendMessage::ParameterStatus(ParameterStatus::new(
                    "application_name".into(),
                    self.session.application_name.clone(),
                ))),
            )
            .await
            .map_err(|_| {
                io::Error::new(io::ErrorKind::TimedOut, "frontend setup response timed out")
            })??;
        }
        if let Some(tag) = control? {
            return send_setup_response(socket, SetupResponse::Command(tag), write_timeout).await;
        }
        self.transaction.check()?;
        if let Some(response) = self
            .session
            .handle(query.statement())
            .map_err(|message| protocol_error("ERROR", "0A000", message))?
        {
            return send_setup_response(socket, response, write_timeout).await;
        }
        query
            .validate_read_only()
            .map_err(|error| protocol_error("ERROR", "0A000", &error.to_string()))?;
        if let Some(kind) = discovery::classify(query) {
            if self.transaction.active() {
                let execution: &'backend Execution = self.execution;
                let discovery = execution.discovery();
                return self
                    .transaction
                    .execute(
                        &discovery.route(),
                        &discovery.prepare(query)?,
                        socket,
                        self.session,
                        self.settings,
                        cancel,
                    )
                    .await;
            }
            return self
                .execution
                .discovery()
                .execute(
                    DiscoveryRequest {
                        query,
                        kind,
                        session: self.session,
                        client_id: self.client_id,
                        query_timeout: self.settings.query_timeout(),
                        write_timeout,
                    },
                    socket,
                    cancel,
                )
                .await;
        }

        // Ordinary queries derive context and select a route even on cache hits.
        let context = self.derive_context(query, &mut cancel).await?;
        let query_id = query.query_id();
        let client_id = self.client_id;
        let authenticated_user = self.source.username.as_str();
        let frontend_database = self.source.database.as_str();
        let request = SelectionRequest {
            authenticated_user,
            client_id,
            query_id,
            query,
            context: &context,
            datasets: self.source.datasets.as_ref(),
            async_hook_timeout_ms: self.settings.async_hook_timeout_ms,
        };
        let execution: &'backend Execution = self.execution;
        let route = tokio::select! {
            biased;
            _ = &mut cancel => return Err(protocol_error("ERROR", "57014", "query cancelled")),
            result = execution.select(&request) => result?,
        };
        if self.transaction.active() {
            return self
                .transaction
                .execute(&route, query, socket, self.session, self.settings, cancel)
                .await;
        }
        let selected = route.selection();
        let backend = route.preferred();
        let cache = self.source.cache.as_deref();
        let mut buffer = None;
        if let Some(cache) = cache {
            let eligible = tokio::select! {
                biased;
                _ = &mut cancel => return Err(protocol_error("ERROR", "57014", "query cancelled")),
                eligible = cache.eligible(&request) => eligible,
            };
            if eligible {
                let scope = response_cache::scope(
                    backend,
                    authenticated_user,
                    frontend_database,
                    self.settings,
                    self.session,
                );
                buffer = cache.reserve(&scope);
                if let Some(entry) = &mut buffer {
                    let hit = tokio::select! {
                        biased;
                        _ = &mut cancel => return Err(protocol_error("ERROR", "57014", "query cancelled")),
                        hit = cache.get(&selected.target, &selected.backend_login, query_id, &scope, entry) => hit,
                    };
                    if hit
                        && response_cache::valid_response(&entry.bytes, cache.max_rows_per_query())
                    {
                        tracing::debug!(query_id, client_id, database = ?backend.target(), database_user = %backend.database_user(), sql = ?query.sql(), "query served from cache");
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
            _ = &mut cancel => return Err(protocol_error("ERROR", "57014", "query cancelled")),
            result = route.acquire() => result?,
        };
        let backend = lease.backend();
        if buffer.is_some() {
            // Capture under the actual binding if overflow changed the cache probe's source.
            drop(buffer.take());
            let scope = response_cache::scope(
                backend,
                authenticated_user,
                frontend_database,
                self.settings,
                self.session,
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
            sql = ?query.sql(),
            "query sent to backend"
        );
        let mut output = CaptureSink::new(socket, buffer, cache);
        let cacheable_completion = lease
            .execute_with_setup(
                query,
                &mut output,
                self.settings.query_timeout(),
                write_timeout,
                Some(cancel),
                Some(&self.session.setup_sql()),
            )
            .await?;
        if cacheable_completion && let (Some(cache), Some(buffer)) = (cache, output.captured()) {
            cache
                .put(&selected.target, &selected.backend_login, query_id, buffer)
                .await;
        }
        Ok(())
    }

    async fn derive_context(
        &self,
        query: &ParsedQuery,
        cancel: &mut oneshot::Receiver<()>,
    ) -> PgWireResult<Map<String, Value>> {
        let Some(hook) = &self.source.context_hook else {
            return Ok(Map::new());
        };
        let mut stopped = self.source.stopped.clone();
        let request = ContextRequest {
            authenticated_user: &self.source.username,
            frontend_database: &self.source.database,
            client_id: self.client_id,
            query_id: query.query_id(),
            query,
            datasets: self.source.datasets.as_ref(),
        };
        let deadline = Duration::from_millis(u64::from(self.settings.context_timeout_ms));
        tokio::select! {
            biased;
            _ = cancel => Err(protocol_error("ERROR", "57014", "query cancelled")),
            _ = stopped.wait_for(|stopped| *stopped) => Err(protocol_error("ERROR", "57014", "query cancelled")),
            context = hook.derive(request, deadline, self.settings.max_context_bytes) => Ok(context),
        }
    }
}

async fn send_setup_response(
    socket: &mut FrontendSocket,
    response: SetupResponse,
    write_timeout: Duration,
) -> PgWireResult<()> {
    let messages = match response {
        SetupResponse::Command(tag) => vec![PgWireBackendMessage::CommandComplete(
            CommandComplete::new(tag.into()),
        )],
        SetupResponse::Set { parameter } => {
            let mut messages = Vec::new();
            if let Some((name, value)) = parameter {
                messages.push(PgWireBackendMessage::ParameterStatus(ParameterStatus::new(
                    name.into(),
                    value,
                )));
            }
            messages.push(PgWireBackendMessage::CommandComplete(CommandComplete::new(
                "SET".into(),
            )));
            messages
        }
        SetupResponse::Show { name, value } => {
            let mut row = bytes::BytesMut::new();
            row.extend_from_slice(&(value.len() as i32).to_be_bytes());
            row.extend_from_slice(value.as_bytes());
            vec![
                PgWireBackendMessage::RowDescription(RowDescription::new(vec![
                    FieldDescription::new(name, 0, 0, 25, -1, -1, 0),
                ])),
                PgWireBackendMessage::DataRow(DataRow::new(row, 1)),
                PgWireBackendMessage::CommandComplete(CommandComplete::new("SHOW".into())),
            ]
        }
    };
    for message in messages {
        timeout(write_timeout, socket.send(message))
            .await
            .map_err(|_| {
                io::Error::new(io::ErrorKind::TimedOut, "frontend setup response timed out")
            })??;
    }
    Ok(())
}
