//! Ordered simple-query stages: metadata/setup, context, parsing, modification,
//! effective-query validation and IDs, then shared lineage/routing/cache/execution.
//!
//! Recognized BI metadata requests read the catalog; supported setup/control SQL
//! changes frontend state. Both bypass application data-query callbacks. Ordinary
//! SQL first derives its incoming dialect, then builds an AST (a structured SQL
//! tree). A replacement from the modification hook is reparsed and validated
//! before downstream code sees it, keeping SQL, query IDs, and lineage consistent.

use super::{FrontendSocket, protocol_error, server::Credentials};
use crate::{
    backend::execution::Execution,
    backend::result::{ColumnType, ResultColumn, ResultEvent},
    config::GatewaySettings,
    discovery::{SessionSettings, SetupResponse},
    execution::Transaction,
    hooks::{
        QueryContextOutput, QueryContextRequest, QueryModificationError, QueryModificationRequest,
    },
    query::{ParsedQuery, QueryError},
};
use futures::{SinkExt, channel::oneshot};
use pgwire::{
    api::ClientInfo,
    error::{PgWireError, PgWireResult},
    messages::{
        PgWireBackendMessage,
        data::{DataRow, FieldDescription, RowDescription},
        response::{CommandComplete, EmptyQueryResponse},
        startup::ParameterStatus,
    },
};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
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
        mut cancel: oneshot::Receiver<()>,
    ) -> PgWireResult<()> {
        if sql.len() > self.settings.max_sql_bytes {
            return Err(protocol_error(
                "ERROR",
                "54000",
                &QueryError::TooLarge.to_string(),
            ));
        }
        let catalog = &self.source.catalog;
        let request_id = || {
            Sha256::digest(sql.as_bytes())
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        };
        if let Some(request) = super::metadata::Request::recognize_snapshot(
            sql,
            self.source.profile,
            catalog.status().source_dialect == crate::query::SqlDialect::Postgresql,
        )
        .or_else(|| super::metadata::Request::recognize(sql, self.source.profile))
        {
            let query_id = request_id();
            let result = async {
                self.transaction
                    .check()
                    .map_err(pgwire::error::PgWireError::from)?;
                let snapshot = catalog.snapshot().map_err(|_| {
                    protocol_error(
                        "ERROR",
                        "55000",
                        "catalog snapshot is unavailable or expired",
                    )
                })?;
                tracing::debug!(
                    query_id,
                    client_id = self.client_id,
                    catalog_generation = snapshot.generation,
                    backend_target = %snapshot.source.endpoint,
                    database_user = %snapshot.source.principal,
                    sql = ?crate::hooks::builtin::extractors::sql_without_observed_postgres_remark(sql),
                    "metadata served from catalog snapshot"
                );
                request
                    .execute(
                        &snapshot,
                        self.source.profile,
                        &mut super::result::output(socket),
                        self.settings,
                        cancel,
                        self.session,
                    )
                    .await
                    .map_err(Into::into)
            }.await;
            if result.is_err() {
                tracing::error!(
                    query_id,
                    client_id = self.client_id,
                    "catalog metadata query failed"
                );
            }
            return result;
        }
        // Only frontend controls bypass context here. Ordinary data is parsed
        // with the callback's dialect and never re-enters metadata recognition.
        let frontend = self
            .source
            .profile
            .parse_query(sql, self.settings.max_sql_bytes);
        let (parsed, query_context, frontend_request) = match frontend {
            Ok(query) if self.source.profile.is_frontend_request(&query) => {
                (Ok(query), QueryContextOutput::default(), true)
            }
            Err(QueryError::Empty) => (Err(QueryError::Empty), QueryContextOutput::default(), true),
            frontend_data => {
                self.transaction
                    .check()
                    .map_err(pgwire::error::PgWireError::from)?;
                let context = self.derive_query_context(sql, &mut cancel).await?;
                let dialect = context.dialect.unwrap_or(self.source.profile.dialect());
                (
                    if dialect == self.source.profile.dialect() {
                        frontend_data
                    } else {
                        ParsedQuery::parse_with_dialect(sql, self.settings.max_sql_bytes, dialect)
                    },
                    context,
                    false,
                )
            }
        };
        let query = match parsed {
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
                tracing::debug!(
                    query_id = %request_id(),
                    client_id = self.client_id,
                    sql = ?crate::hooks::builtin::extractors::sql_without_observed_postgres_remark(sql),
                    "query rejected during parsing"
                );
                tracing::error!(client_id = self.client_id, "query parsing failed");
                return Err(query_error(error));
            }
        };
        let query = if frontend_request {
            query
        } else {
            let query = self
                .modify_query(query, &query_context.values, &mut cancel)
                .await?;
            query.validate_read_only().map_err(query_error)?;
            query
        };
        let query_id = query.query_id();
        let client_id = self.client_id;
        if tracing::enabled!(tracing::Level::DEBUG) {
            let bi_remark =
                crate::hooks::builtin::extractors::observed_postgres_remark(query.sql())
                    .map(Value::Object)
                    .unwrap_or(Value::Null);
            let sql = crate::hooks::builtin::extractors::sql_without_observed_postgres_remark(
                query.sql(),
            );
            tracing::debug!(
                query_id,
                client_id,
                sql = ?sql,
                bi_remark = %bi_remark,
                "query received"
            );
        } else {
            tracing::debug!(query_id, client_id, sql = ?crate::hooks::builtin::extractors::sql_without_observed_postgres_remark(query.sql()), "query received");
        }
        let mut shared = crate::execution::QuerySession {
            execution: self.execution,
            settings: self.settings,
            client_id: self.client_id,
            session: self.session,
            transaction: self.transaction,
            source: crate::execution::ExecutionServices {
                username: &self.source.username,
                database: &self.source.database,
                profile: self.source.profile,
                cache: self.source.cache.as_deref(),
                decision_model: self.source.decision_model.as_deref(),
                ast: &self.source.ast,
                catalog: &self.source.catalog,
                result_processing: self.source.result_processing.as_deref(),
                datasets: self.source.datasets.as_ref(),
            },
        };
        let outcome = shared
            .execute(
                &mut super::result::output(socket),
                &query,
                &query_context.values,
                frontend_request,
                cancel,
            )
            .await;
        let result = match outcome {
            Ok(Some(response)) => {
                send_setup_response(socket, response, self.settings.write_timeout()).await
            }
            Ok(None) => Ok(()),
            Err(error) => Err(error.into()),
        };
        if result.is_err() {
            tracing::error!(query_id, client_id, "query failed");
        }
        result
    }

    async fn modify_query(
        &self,
        query: ParsedQuery,
        query_context: &Map<String, Value>,
        cancel: &mut oneshot::Receiver<()>,
    ) -> PgWireResult<ParsedQuery> {
        if cancel.try_recv().ok().flatten().is_some() {
            return Err(protocol_error("ERROR", "57014", "query cancelled"));
        }
        let Some(hook) = &self.source.query_modification_hook else {
            return Ok(query);
        };
        let mut stopped = self.source.stopped.clone();
        let request = QueryModificationRequest {
            authenticated_user: &self.source.username,
            frontend_database: &self.source.database,
            client_id: self.client_id,
            sql: query.sql(),
            statement: query.statement(),
            dialect: query.dialect(),
            gateway_dialect: self.source.profile.dialect(),
            default_target_dialect: self.execution.default_target_dialect(),
            query_context,
            datasets: self.source.datasets.as_ref(),
        };
        let replacement = tokio::select! {
            biased;
            _ = cancel => return Err(protocol_error("ERROR", "57014", "query cancelled")),
            _ = stopped.wait_for(|stopped| *stopped) => return Err(protocol_error("ERROR", "57014", "query cancelled")),
            result = hook.modify(
                request,
                Duration::from_millis(u64::from(self.settings.async_hook_timeout_ms)),
                self.settings.max_sql_bytes,
            ) => result.map_err(|error| {
                let code = match error {
                    QueryModificationError::Denied => "42501",
                    QueryModificationError::PayloadTooLarge => "54000",
                    _ => "XX000",
                };
                protocol_error("ERROR", code, "query modification unavailable")
            })?,
        };
        match replacement {
            None => Ok(query),
            Some(sql) => {
                ParsedQuery::parse_with_dialect(&sql, self.settings.max_sql_bytes, query.dialect())
                    .map_err(query_error)
            }
        }
    }

    async fn derive_query_context(
        &self,
        sql: &str,
        cancel: &mut oneshot::Receiver<()>,
    ) -> PgWireResult<QueryContextOutput> {
        if cancel.try_recv().ok().flatten().is_some() {
            return Err(protocol_error("ERROR", "57014", "query cancelled"));
        }
        let Some(hook) = &self.source.query_context_hook else {
            return Ok(QueryContextOutput::default());
        };
        let mut stopped = self.source.stopped.clone();
        let request = QueryContextRequest {
            authenticated_user: &self.source.username,
            frontend_database: &self.source.database,
            client_id: self.client_id,
            sql,
            gateway_dialect: self.source.profile.dialect(),
            default_target_dialect: self.execution.default_target_dialect(),
            datasets: self.source.datasets.as_ref(),
        };
        tokio::select! {
            biased;
            _ = cancel => Err(protocol_error("ERROR", "57014", "query cancelled")),
            _ = stopped.wait_for(|stopped| *stopped) => Err(protocol_error("ERROR", "57014", "query cancelled")),
            result = hook.derive(request, Duration::from_millis(u64::from(self.settings.query_context_timeout_ms)), self.settings.max_context_bytes) => {
                result.map_err(|_| protocol_error("ERROR", "XX000", "query context unavailable"))
            }
        }
    }
}

fn query_error(error: QueryError) -> PgWireError {
    let code = match error {
        QueryError::Syntax => "42601",
        QueryError::TooLarge | QueryError::TooComplex => "54000",
        _ => "0A000",
    };
    protocol_error("ERROR", code, &error.to_string())
}

async fn send_setup_response(
    socket: &mut FrontendSocket,
    response: SetupResponse,
    write_timeout: Duration,
) -> PgWireResult<()> {
    let messages =
        match response {
            SetupResponse::BackendPid => [
                ResultEvent::Columns(vec![ResultColumn {
                    name: "pg_backend_pid".into(),
                    kind: ColumnType::Int32,
                }]),
                ResultEvent::Row(vec![Some(bytes::Bytes::from(
                    socket.pid_and_secret_key().0.to_string(),
                ))]),
                ResultEvent::Complete(1),
            ]
            .into_iter()
            .map(|event| crate::wire::encode(event).into_wire())
            .collect(),
            SetupResponse::EmptyInteger { name } => vec![
                PgWireBackendMessage::RowDescription(RowDescription::new(vec![
                    FieldDescription::new(name.into(), 0, 0, 23, 4, -1, 0),
                ])),
                PgWireBackendMessage::CommandComplete(CommandComplete::new("SELECT 0".into())),
            ],
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
            SetupResponse::Show { name, value } => row_response(name, value, "SHOW"),
            SetupResponse::Row { name, value, tag } => row_response(name, value, tag),
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

fn row_response(name: String, value: String, tag: &str) -> Vec<PgWireBackendMessage> {
    let mut row = bytes::BytesMut::new();
    row.extend_from_slice(&(value.len() as i32).to_be_bytes());
    row.extend_from_slice(value.as_bytes());
    vec![
        PgWireBackendMessage::RowDescription(RowDescription::new(vec![FieldDescription::new(
            name, 0, 0, 25, -1, -1, 0,
        )])),
        PgWireBackendMessage::DataRow(DataRow::new(row, 1)),
        PgWireBackendMessage::CommandComplete(CommandComplete::new(tag.into())),
    ]
}
