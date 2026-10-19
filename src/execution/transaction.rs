//! Frontend transaction state and affinity; native execution stays in the adapter.

use crate::{
    backend::{
        execution::{PinnedSession, Route, SessionRequest},
        result::{ExecutionError, ExecutionResult, ResultEvent},
    },
    discovery::SessionSettings,
};
use futures::{Sink, SinkExt, channel::oneshot};
use sqlparser::ast::{
    Statement, TransactionAccessMode, TransactionIsolationLevel, TransactionMode,
};
use std::io;
use tokio::time::timeout;

#[derive(Clone, Copy)]
pub(crate) enum TransactionStatus {
    Idle,
    Transaction,
    Error,
}

#[derive(Default)]
pub(crate) struct Transaction<'a> {
    original: Option<SessionSettings>,
    pinned: Option<PinnedSession<'a>>,
    failed: bool,
}

impl<'a> Transaction<'a> {
    pub(crate) fn active(&self) -> bool {
        self.original.is_some()
    }

    pub(crate) fn status(&self) -> TransactionStatus {
        if !self.active() {
            TransactionStatus::Idle
        } else if self.failed {
            TransactionStatus::Error
        } else {
            TransactionStatus::Transaction
        }
    }

    pub(crate) fn fail(&mut self) {
        if self.active() {
            self.failed = true;
        }
    }

    pub(crate) fn check(&self) -> ExecutionResult<()> {
        if self.failed {
            Err(ExecutionError::rejected(
                "25P02",
                "current transaction is aborted; roll back before executing queries",
            ))
        } else {
            Ok(())
        }
    }

    pub(crate) async fn control(
        &mut self,
        statement: &Statement,
        session: &mut SessionSettings,
    ) -> ExecutionResult<Option<&'static str>> {
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
                    return Err(ExecutionError::rejected(
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
                return Err(ExecutionError::rejected(
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

    pub(crate) async fn execute<S>(
        &mut self,
        route: &Route<'a>,
        request: SessionRequest<'_>,
        output: &mut S,
        mut cancel: oneshot::Receiver<()>,
    ) -> ExecutionResult<()>
    where
        S: Sink<ResultEvent, Error = ExecutionError> + Unpin + Send,
    {
        if let Some(pinned) = &self.pinned {
            if !route.accepts(pinned) {
                return Err(ExecutionError::rejected(
                    "25000",
                    "query route conflicts with the current transaction",
                ));
            }
        } else {
            let pinned = route.begin(request.settings, &mut cancel).await?;
            self.pinned = Some(pinned);
        }
        let pinned = self.pinned.as_mut().unwrap();
        let result = pinned.execute(request, output, cancel).await;
        if !pinned.usable() {
            self.pinned.take();
            return match result {
                Err(error) => Err(error),
                Ok(_) => Err(ExecutionError::rejected("57014", "transaction interrupted")),
            };
        }
        let completion = result?;
        timeout(request.settings.write_timeout(), output.send(completion))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "frontend write timed out"))??;
        Ok(())
    }
}
