//! Frontend transaction state and affinity; native execution stays in the adapter.

use super::{FrontendSocket, protocol_error};
use crate::{
    backend::postgres::{BackendRoute, PostgresTransaction},
    config::GatewaySettings,
    discovery::SessionSettings,
    query::ParsedQuery,
};
use futures::{SinkExt, channel::oneshot};
use pgwire::{error::PgWireResult, messages::response::TransactionStatus};
use sqlparser::ast::{
    Statement, TransactionAccessMode, TransactionIsolationLevel, TransactionMode,
};
use std::{io, time::Duration};
use tokio::time::timeout;

#[derive(Default)]
pub(super) struct Transaction<'a> {
    original: Option<SessionSettings>,
    pinned: Option<PostgresTransaction<'a>>,
    failed: bool,
}

impl<'a> Transaction<'a> {
    pub(super) fn active(&self) -> bool {
        self.original.is_some()
    }

    pub(super) fn status(&self) -> TransactionStatus {
        if !self.active() {
            TransactionStatus::Idle
        } else if self.failed {
            TransactionStatus::Error
        } else {
            TransactionStatus::Transaction
        }
    }

    pub(super) fn fail(&mut self) {
        if self.active() {
            self.failed = true;
        }
    }

    pub(super) fn check(&self) -> PgWireResult<()> {
        if self.failed {
            Err(protocol_error(
                "ERROR",
                "25P02",
                "current transaction is aborted; roll back before executing queries",
            ))
        } else {
            Ok(())
        }
    }

    pub(super) async fn control(
        &mut self,
        statement: &Statement,
        session: &mut SessionSettings,
    ) -> PgWireResult<Option<&'static str>> {
        let commit = match statement {
            Statement::StartTransaction {
                modes,
                modifier,
                statements,
                exception: None,
                has_end_keyword: false,
                ..
            } if modifier.is_none()
                && statements.is_empty()
                && modes.iter().all(|mode| {
                    matches!(
                        mode,
                        TransactionMode::AccessMode(TransactionAccessMode::ReadOnly)
                            | TransactionMode::IsolationLevel(
                                TransactionIsolationLevel::ReadCommitted
                            )
                    )
                }) =>
            {
                if self.active() {
                    return Err(protocol_error(
                        "ERROR",
                        "25001",
                        "a transaction is already in progress",
                    ));
                }
                self.original = Some(session.clone());
                return Ok(Some("BEGIN"));
            }
            Statement::Commit {
                chain: false,
                modifier: None,
                ..
            } => true,
            Statement::Rollback {
                chain: false,
                savepoint: None,
            } => false,
            Statement::StartTransaction { .. }
            | Statement::Commit { .. }
            | Statement::Rollback { .. } => {
                return Err(protocol_error(
                    "ERROR",
                    "0A000",
                    "unsupported transaction options",
                ));
            }
            _ => return Ok(None),
        };
        let rollback = !commit || self.failed;
        let original = self.original.take();
        let pinned = self.pinned.take();
        self.failed = false;
        let result = match pinned {
            Some(pinned) => pinned.finish(!rollback).await,
            None => Ok(()),
        };
        if (rollback || result.is_err())
            && let Some(original) = original
        {
            *session = original;
        }
        result?;
        Ok(Some(if rollback { "ROLLBACK" } else { "COMMIT" }))
    }

    pub(super) async fn execute(
        &mut self,
        route: &BackendRoute<'a>,
        query: &ParsedQuery,
        socket: &mut FrontendSocket,
        session: &SessionSettings,
        settings: &GatewaySettings,
        mut cancel: oneshot::Receiver<()>,
    ) -> PgWireResult<()> {
        if let Some(pinned) = &self.pinned {
            if !route.contains(pinned.backend()) {
                return Err(protocol_error(
                    "ERROR",
                    "25000",
                    "query route conflicts with the current transaction",
                ));
            }
        } else {
            let lease = tokio::select! {
                biased;
                _ = &mut cancel => return Err(protocol_error("ERROR", "57014", "query cancelled")),
                lease = route.acquire() => lease?,
            };
            let pinned = tokio::select! {
                biased;
                _ = &mut cancel => return Err(protocol_error("ERROR", "57014", "query cancelled")),
                pinned = lease.begin(settings.query_timeout(), Duration::from_millis(u64::from(settings.read_timeout_ms))) => pinned?,
            };
            self.pinned = Some(pinned);
        }
        let pinned = self.pinned.as_mut().unwrap();
        let result = pinned
            .execute(
                query,
                socket,
                settings.query_timeout(),
                settings.write_timeout(),
                Some(cancel),
                Some(&session.setup_sql()),
            )
            .await;
        if !pinned.usable() {
            self.pinned.take();
            return match result {
                Err(error) => Err(error),
                Ok(_) => Err(protocol_error("ERROR", "57014", "transaction interrupted")),
            };
        }
        let completion = result?;
        timeout(settings.write_timeout(), socket.send(completion))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "frontend write timed out"))??;
        Ok(())
    }
}
