//! Native calls run outside Tokio. Capacity remains owned through stream release
//! and native cleanup, including when a frontend future is abandoned.

use super::{
    Inner,
    connection::{CancelHandle, Connection},
    result, validate_session,
};
use crate::{
    backend::result::{ExecutionError, ExecutionResult, ResultEvent},
    config::GatewaySettings,
    discovery::SessionSettings,
    query::ParsedQuery,
};
use futures::{Sink, SinkExt, channel::oneshot};
use std::{
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::{
    sync::{OwnedSemaphorePermit, mpsc},
    time::{Instant, timeout, timeout_at},
};

enum Command {
    Query {
        sql: String,
        events: mpsc::Sender<ExecutionResult<ResultEvent>>,
        state: Arc<Mutex<Operation>>,
        bound: usize,
    },
    Finish {
        commit: bool,
        reply: oneshot::Sender<ExecutionResult<()>>,
    },
}
#[derive(Default)]
struct Operation {
    cancelled: bool,
    handle: Option<CancelHandle>,
}

struct CancelOnDrop {
    state: Arc<Mutex<Operation>>,
    permit: Arc<OwnedSemaphorePermit>,
    armed: bool,
}
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        let mut state = self.state.lock().expect("operation state is not poisoned");
        state.cancelled = self.armed;
        let handle = state.handle.take();
        if self.armed
            && let Some(handle) = handle
        {
            let permit = self.permit.clone();
            tokio::task::spawn_blocking(move || {
                let _ = handle.request_cancel();
                drop(handle);
                drop(permit);
            });
        }
    }
}

pub(crate) struct Session {
    commands: mpsc::Sender<Command>,
    permit: Arc<OwnedSemaphorePermit>,
    usable: bool,
}
impl Session {
    pub(super) async fn open(
        inner: Arc<Inner>,
        settings: &GatewaySettings,
        transaction: bool,
    ) -> ExecutionResult<Self> {
        let permit = timeout(inner.acquire_timeout, inner.permits.clone().acquire_owned())
            .await
            .map_err(|_| ExecutionError::rejected("53300", "Snowflake capacity wait timed out"))?
            .map_err(|_| ExecutionError::rejected("08006", "Snowflake binding is closed"))?;
        let permit = Arc::new(permit);
        let owned_permit = permit.clone();
        let (commands, mut receiver) = mpsc::channel::<Command>(1);
        let (ready, opened) = oneshot::channel();
        tokio::task::spawn_blocking(move || {
            let options: Vec<_> = inner
                .options
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();
            let cleanup_capacity = owned_permit.clone();
            let cleanup_backend = inner.clone();
            let cleanup_failed = Arc::new(AtomicBool::new(false));
            let cleanup_signal = cleanup_failed.clone();
            let connection = inner
                .driver
                .open(
                    &options,
                    Some(Box::new(move || {
                        cleanup_signal.store(true, Ordering::Release);
                        cleanup_backend.permits.close();
                        // Failed native cleanup cannot return this slot for reuse.
                        std::mem::forget(cleanup_capacity);
                        tracing::error!("Snowflake native cleanup failed; binding retired");
                    })),
                )
                .map_err(|_| {
                    ExecutionError::rejected("08001", "cannot establish Snowflake session")
                });
            let mut connection = match connection {
                Ok(c) => c,
                Err(e) => {
                    let _ = ready.send(Err(e));
                    return;
                }
            };
            let result = if transaction {
                connection.autocommit(false).map_err(Into::into)
            } else {
                Ok(())
            };
            let ok = result.is_ok();
            if ready.send(result).is_ok() && ok {
                while let Some(command) = receiver.blocking_recv() {
                    match command {
                        Command::Query {
                            sql,
                            events,
                            state,
                            bound,
                        } => {
                            if let Err(error) = execute(
                                &mut connection,
                                &sql,
                                &events,
                                state,
                                bound,
                                inner.max_batch_bytes,
                            ) {
                                let _ = events.blocking_send(Err(error));
                            }
                        }
                        Command::Finish { commit, reply } => {
                            let result = if transaction {
                                if commit {
                                    connection.commit()
                                } else {
                                    connection.rollback()
                                }
                                .map_err(Into::into)
                            } else {
                                Ok(())
                            };
                            drop(connection);
                            let result = result.and_then(|()| {
                                if cleanup_failed.load(Ordering::Acquire) {
                                    Err(ExecutionError::rejected(
                                        "08006",
                                        "Snowflake native cleanup failed; binding retired",
                                    ))
                                } else {
                                    Ok(())
                                }
                            });
                            let _ = reply.send(result);
                            return;
                        }
                    }
                }
            }
            // No pool reuse: disconnect and transaction abandonment close this
            // identity's connection. Native parents outlive every child handle.
            drop(connection);
            drop(owned_permit);
        });
        timeout(settings.query_timeout(), opened)
            .await
            .map_err(|_| ExecutionError::rejected("57014", "Snowflake session startup timed out"))?
            .map_err(|_| ExecutionError::rejected("08006", "Snowflake worker stopped"))??;
        Ok(Self {
            commands,
            permit,
            usable: true,
        })
    }

    pub(crate) fn usable(&self) -> bool {
        self.usable && !self.commands.is_closed()
    }

    pub(crate) async fn finish(self, commit: bool) -> ExecutionResult<()> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(Command::Finish { commit, reply })
            .await
            .map_err(|_| stopped())?;
        timeout(std::time::Duration::from_secs(10), result)
            .await
            .map_err(|_| {
                ExecutionError::rejected("57014", "Snowflake transaction cleanup timed out")
            })?
            .map_err(|_| stopped())?
    }

    pub(crate) async fn execute<S>(
        &mut self,
        query: &ParsedQuery,
        output: &mut S,
        settings: &GatewaySettings,
        session: &SessionSettings,
        cancel: oneshot::Receiver<()>,
    ) -> ExecutionResult<ResultEvent>
    where
        S: Sink<ResultEvent, Error = ExecutionError> + Send + Unpin,
    {
        self.execute_sql(query.sql(), output, settings, session, cancel)
            .await
    }

    /// SQL constructed by this adapter's metadata provider also uses the same
    /// worker, cancellation, conversion, and capacity lifetime as data reads.
    pub(super) async fn execute_sql<S>(
        &mut self,
        sql: &str,
        output: &mut S,
        settings: &GatewaySettings,
        session: &SessionSettings,
        mut cancel: oneshot::Receiver<()>,
    ) -> ExecutionResult<ResultEvent>
    where
        S: Sink<ResultEvent, Error = ExecutionError> + Send + Unpin,
    {
        validate_session(session)?;
        let state = Arc::new(Mutex::new(Operation::default()));
        let mut guard = CancelOnDrop {
            state: state.clone(),
            permit: self.permit.clone(),
            armed: true,
        };
        let (events, mut received) = mpsc::channel(1);
        self.commands
            .send(Command::Query {
                sql: sql.into(),
                events,
                state,
                bound: settings.max_message_bytes,
            })
            .await
            .map_err(|_| stopped())?;
        let deadline = Instant::now() + settings.query_timeout();
        loop {
            let event = tokio::select! {
                biased;
                _ = &mut cancel => { self.usable = false; return Err(ExecutionError::cancelled()); }
                result = timeout_at(deadline, received.recv()) => result.map_err(|_| { self.usable = false; ExecutionError::rejected("57014", "Snowflake query timed out") })?,
            };
            match event.ok_or_else(stopped)? {
                Ok(completion @ ResultEvent::Complete(_)) => {
                    guard.armed = false;
                    return Ok(completion);
                }
                Ok(event) => timeout(settings.write_timeout(), output.send(event))
                    .await
                    .map_err(|_| {
                        io::Error::new(io::ErrorKind::TimedOut, "frontend write timed out")
                    })??,
                Err(error) => {
                    guard.armed = false;
                    return Err(error);
                }
            }
        }
    }
}

fn stopped() -> ExecutionError {
    ExecutionError::rejected("08006", "Snowflake worker stopped")
}

fn execute(
    connection: &mut Connection,
    sql: &str,
    events: &mpsc::Sender<ExecutionResult<ResultEvent>>,
    state: Arc<Mutex<Operation>>,
    bound: usize,
    batch_bound: usize,
) -> ExecutionResult<()> {
    let mut statement = connection.statement(sql)?;
    {
        let mut operation = state.lock().expect("operation state is not poisoned");
        let handle = statement.cancel_handle();
        if operation.cancelled {
            handle.request_cancel()?;
        }
        operation.handle = Some(handle);
    }
    let mut rows = statement.execute()?;
    let schema = rows.schema();
    events
        .blocking_send(Ok(ResultEvent::Columns(result::columns(&schema, bound)?)))
        .map_err(|_| stopped())?;
    let mut count = 0u64;
    for batch in &mut rows {
        let batch = batch?;
        if batch.get_array_memory_size() > batch_bound {
            return Err(ExecutionError::rejected(
                "54000",
                "Snowflake Arrow batch exceeds the configured bound",
            ));
        }
        for index in 0..batch.num_rows() {
            events
                .blocking_send(Ok(ResultEvent::Row(result::row(&batch, index, bound)?)))
                .map_err(|_| stopped())?;
            count = count
                .checked_add(1)
                .ok_or_else(|| ExecutionError::rejected("54000", "result row count overflow"))?;
        }
    }
    drop(rows);
    drop(statement);
    events
        .blocking_send(Ok(ResultEvent::Complete(count)))
        .map_err(|_| stopped())
}
