//! Ordered simple-query stages: setup, metadata, context, routing, cache, execution.

use super::{FrontendSocket, protocol_error, server::Credentials};
use crate::{
    backend::execution::Execution,
    config::GatewaySettings,
    discovery::{SessionSettings, SetupResponse},
    execution::Transaction,
    hooks::InputRequest,
    query::{InputOptions, QueryError},
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
use serde_json::Value;
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
        cancel: oneshot::Receiver<()>,
    ) -> PgWireResult<()> {
        if sql.len() > self.settings.max_sql_bytes {
            return Err(protocol_error(
                "ERROR",
                "54000",
                &QueryError::TooLarge.to_string(),
            ));
        }
        let dialect = self.source.profile.dialect();
        let options = self
            .source
            .input_options
            .as_ref()
            .map(|resolve| {
                resolve(InputRequest {
                    sql,
                    frontend_dialect: dialect,
                })
            })
            .unwrap_or(InputOptions::frontend(dialect));
        // A generated query cannot opt into a different backend grammar.
        if !options.native && options.dialect != dialect {
            return Err(protocol_error(
                "ERROR",
                "0A000",
                "generated SQL must use the frontend dialect",
            ));
        }
        if !options.native
            && let Some(catalog) = &self.source.catalog
            && let Some(request) = (catalog.status().source_dialect
                != crate::query::SqlDialect::Postgresql)
                .then(|| super::metadata::Request::recognize_foreign(sql, self.source.profile))
                .flatten()
                .or_else(|| super::metadata::Request::recognize(sql, self.source.profile))
        {
            let query_id: String = Sha256::digest(sql.as_bytes())
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect();
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
        let query = match self
            .source
            .profile
            .parse_query(sql, self.settings.max_sql_bytes, options)
        {
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
            source: crate::execution::QueryContext {
                username: &self.source.username,
                database: &self.source.database,
                profile: self.source.profile,
                cache: self.source.cache.as_deref(),
                context_hook: self.source.context_hook.as_deref(),
                datasets: self.source.datasets.as_ref(),
                foreign_catalog: self.source.catalog.as_ref().is_some_and(|catalog| {
                    catalog.status().source_dialect != crate::query::SqlDialect::Postgresql
                }),
                stopped: self.source.stopped.clone(),
            },
        };
        let outcome = shared
            .execute(&mut super::result::output(socket), &query, cancel)
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
}

async fn send_setup_response(
    socket: &mut FrontendSocket,
    response: SetupResponse,
    write_timeout: Duration,
) -> PgWireResult<()> {
    let messages =
        match response {
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
