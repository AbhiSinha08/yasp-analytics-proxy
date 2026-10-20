//! A read-only transaction retains its physical lease until verified cleanup.

use super::{
    BackendLease, CLEANUP_TIMEOUT, CONNECT_TIMEOUT, PostgresBackend, SESSION_DEFAULTS,
    error_response, pool::Lease,
};
use crate::query::ParsedQuery;
use futures::{Sink, channel::oneshot};
use pgwire::{
    error::{PgWireError, PgWireResult},
    messages::{PgWireBackendMessage, response::TransactionStatus},
};
use std::{fmt, time::Duration};
use tokio::time::timeout;

pub(crate) struct PostgresTransaction<'a> {
    connection: Lease,
    // Admission remains reserved for the lifetime of the transaction.
    owner: BackendLease<'a>,
    usable: bool,
}

impl<'a> BackendLease<'a> {
    pub(crate) async fn begin(
        mut self,
        query_timeout: Duration,
        idle_timeout: Duration,
    ) -> PgWireResult<PostgresTransaction<'a>> {
        let mut transaction = PostgresTransaction {
            connection: Lease(self.object.take()),
            owner: self,
            usable: false,
        };
        let connection = transaction.connection.0.as_mut().unwrap();
        connection.clean = false;
        let sql = format!(
            "BEGIN READ ONLY; SET LOCAL statement_timeout = '{}ms'; SET LOCAL idle_in_transaction_session_timeout = '{}ms'",
            query_timeout.as_millis(),
            idle_timeout.as_millis(),
        );
        timeout(CONNECT_TIMEOUT, connection.control(&sql))
            .await
            .map_err(|_| error_response("08006", "PostgreSQL transaction setup timed out"))??;
        if connection.status != TransactionStatus::Transaction {
            return Err(error_response(
                "08006",
                "unexpected PostgreSQL transaction state",
            ));
        }
        transaction.usable = true;
        Ok(transaction)
    }
}

impl PostgresTransaction<'_> {
    /// All provider statements observe one source metadata snapshot.
    pub(super) async fn catalog_isolation(&mut self) -> PgWireResult<()> {
        timeout(
            CONNECT_TIMEOUT,
            self.connection
                .0
                .as_mut()
                .unwrap()
                .control("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"),
        )
        .await
        .map_err(|_| error_response("08006", "catalog transaction setup timed out"))?
    }

    pub(crate) fn backend(&self) -> &PostgresBackend {
        self.owner.backend
    }

    pub(crate) fn usable(&self) -> bool {
        self.usable
    }

    pub(crate) async fn execute<S>(
        &mut self,
        query: &ParsedQuery,
        output: &mut S,
        query_timeout: Duration,
        write_timeout: Duration,
        cancel: Option<oneshot::Receiver<()>>,
        session: Option<&crate::discovery::SessionSettings>,
    ) -> PgWireResult<PgWireBackendMessage>
    where
        S: Sink<PgWireBackendMessage> + Unpin + Send,
        S::Error: fmt::Debug,
        PgWireError: From<S::Error>,
    {
        super::execution::validate_input(query)?;
        query
            .validate_read_only()
            .map_err(|error| error_response("0A000", &error.to_string()))?;
        let connection = self.connection.0.as_mut().unwrap();
        connection.query_complete = false;
        connection.cancel_requested = false;
        self.usable = false;
        if let Some(session) = session {
            let setup = super::execution::session_sql(session);
            let result = timeout(CONNECT_TIMEOUT, connection.control(&setup))
                .await
                .map_err(|_| error_response("08006", "PostgreSQL session setup timed out"))?;
            if let Err(error) = result {
                self.usable = connection.status == TransactionStatus::Error;
                return Err(error);
            }
        }
        let result = connection
            .stream_query(query.sql(), output, query_timeout, write_timeout, cancel)
            .await;
        self.usable = connection.query_complete
            && !connection.cancel_requested
            && matches!(
                connection.status,
                TransactionStatus::Transaction | TransactionStatus::Error
            );
        if connection.query_complete
            && !matches!(
                connection.status,
                TransactionStatus::Transaction | TransactionStatus::Error
            )
        {
            return Err(error_response(
                "08006",
                "unexpected PostgreSQL transaction state",
            ));
        }
        result
    }

    pub(crate) async fn finish(mut self, commit: bool) -> PgWireResult<()> {
        if !self.usable {
            return Ok(());
        }
        let connection = self.connection.0.as_mut().unwrap();
        timeout(CLEANUP_TIMEOUT, async {
            connection
                .control(if commit { "COMMIT" } else { "ROLLBACK" })
                .await?;
            connection.control("DISCARD ALL").await?;
            connection.control(SESSION_DEFAULTS).await?;
            if connection.status != TransactionStatus::Idle {
                return Err(error_response(
                    "08006",
                    "PostgreSQL cleanup could not be verified",
                ));
            }
            connection.control("SELECT 1").await?;
            connection.clean = true;
            Ok(())
        })
        .await
        .map_err(|_| error_response("08006", "PostgreSQL cleanup timed out"))??;
        self.connection.release();
        Ok(())
    }
}
