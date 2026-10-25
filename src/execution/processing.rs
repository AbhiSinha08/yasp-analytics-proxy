//! One post-capture batch stage. Source values and cache sidecars stay raw.
use crate::{
    backend::result::{ExecutionError, ExecutionResult, ResultEvent},
    config::GatewaySettings,
    decision_model::Classifications,
    hooks::{
        BackendSelection, ResultBatch, ResultDataContexts, ResultHook, ResultHookError,
        ResultRequest, SelectionRequest,
        result::{Reservation, ResultProcessing},
    },
    wire::{self, ProcessingEvent, Response},
};
use futures::{Sink, SinkExt};
use std::{mem::size_of, sync::Arc, time::Duration};
use tokio::time::{Instant, timeout};

impl From<ResultHookError> for ExecutionError {
    fn from(error: ResultHookError) -> Self {
        let code = match error {
            ResultHookError::TimedOut => "57014",
            ResultHookError::Limit => "54000",
            #[cfg(feature = "python")]
            ResultHookError::Python(crate::hooks::python::PythonHookError::TimedOut) => "57014",
            _ => "38000",
        };
        Self::rejected(code, &error.to_string())
    }
}

#[derive(Clone, Copy)]
pub(crate) struct ProcessingRequest<'a, 'query> {
    pub platform: &'a ResultProcessing,
    pub selection: &'a SelectionRequest<'query>,
    pub database: &'a str,
    pub route: &'a BackendSelection,
    pub settings: &'a GatewaySettings,
}

pub(crate) struct Processor<'a, 'query> {
    request: ProcessingRequest<'a, 'query>,
    columns: Arc<[crate::hooks::ResultColumn]>,
    ast: Option<crate::lineage::AstDataContext>,
    metadata: Reservation,
    batch: Option<ResultBatch>,
    deadline: Option<Instant>,
    emitted: u64,
    called: bool,
    described: bool,
    #[cfg(feature = "python")]
    python_request: Option<Arc<crate::hooks::python::result::PreparedRequest>>,
}
impl<'a, 'query> Processor<'a, 'query> {
    pub fn new(request: ProcessingRequest<'a, 'query>) -> Self {
        Self {
            columns: Arc::from([]),
            ast: None,
            metadata: Reservation::new(request.platform.budget.clone()),
            request,
            batch: None,
            deadline: None,
            emitted: 0,
            called: false,
            described: false,
            #[cfg(feature = "python")]
            python_request: None,
        }
    }
    pub fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    pub async fn send<S>(
        &mut self,
        response: Response,
        output: &mut S,
        decisions: Option<&Classifications>,
        input: Option<Reservation>,
    ) -> ExecutionResult<()>
    where
        S: Sink<ResultEvent, Error = ExecutionError> + Unpin,
    {
        match wire::processing_event(response) {
            ProcessingEvent::Columns(columns, response) => {
                if self.described {
                    return Err(ResultHookError::InvalidOutput.into());
                }
                self.metadata.charge(
                    columns
                        .iter()
                        .map(|c| {
                            size_of::<crate::hooks::ResultColumn>()
                                + c.name.capacity()
                                + c.data_type.capacity()
                        })
                        .sum(),
                )?;
                let names: Vec<_> = columns.iter().map(|c| c.name.as_str()).collect();
                self.ast = self
                    .request
                    .selection
                    .data_contexts
                    .ast
                    .as_ref()
                    .map(|ast| ast.for_columns(&names));
                if let Some(ast) = &self.ast {
                    self.metadata.charge(
                        serde_json::to_vec(ast)
                            .map_err(|_| ResultHookError::InvalidOutput)?
                            .len()
                            * 8,
                    )?;
                }
                self.columns = columns.into();
                self.described = true;
                self.write(output, ResultEvent::Wire(response)).await?;
            }
            ProcessingEvent::Row(row) => {
                if !self.described {
                    return Err(ResultHookError::InvalidOutput.into());
                }
                let target = self.request.platform.settings.target_batch_bytes;
                if self.batch.as_ref().is_some_and(|batch| {
                    !batch.is_empty() && row.len() > target.saturating_sub(batch.input_bytes)
                }) {
                    self.flush(output, decisions, false).await?;
                }
                let batch = self.batch.get_or_insert_with(|| {
                    ResultBatch::new(self.columns.clone(), self.request.platform.budget.clone())
                });
                batch.push(row, input)?;
                self.deadline
                    .get_or_insert_with(|| Instant::now() + Duration::from_millis(2));
                if batch.len() >= self.request.platform.settings.max_rows_per_batch
                    || batch.input_bytes >= target
                {
                    self.flush(output, decisions, false).await?;
                }
            }
            ProcessingEvent::Complete => {
                self.flush(output, decisions, !self.called).await?;
                self.write(output, ResultEvent::Complete(self.emitted))
                    .await?;
            }
            ProcessingEvent::Other(response) => {
                self.flush(output, decisions, false).await?;
                self.write(output, ResultEvent::Wire(response)).await?;
            }
        }
        Ok(())
    }
    pub async fn flush<S>(
        &mut self,
        output: &mut S,
        decisions: Option<&Classifications>,
        empty: bool,
    ) -> ExecutionResult<()>
    where
        S: Sink<ResultEvent, Error = ExecutionError> + Unpin,
    {
        self.deadline = None;
        let Some(mut batch) = self.batch.take().or_else(|| {
            empty.then(|| {
                ResultBatch::new(self.columns.clone(), self.request.platform.budget.clone())
            })
        }) else {
            return Ok(());
        };
        let selection = self.request.selection;
        let (datasets, _guard) = selection
            .datasets
            .map(crate::datasets::DatasetReader::invocation)
            .map_or((None, None), |(reader, guard)| (Some(reader), Some(guard)));
        let request = ResultRequest {
            authenticated_user: selection.authenticated_user,
            frontend_database: self.request.database,
            client_id: selection.client_id,
            query_id: selection.query_id,
            query: selection.query,
            target: &self.request.route.target,
            backend_login: &self.request.route.backend_login,
            query_context: selection.query_context,
            data_contexts: ResultDataContexts {
                ast: self.ast.as_ref(),
                decision_model: decisions,
            },
            datasets: datasets.as_ref(),
        };
        let deadline =
            Duration::from_millis(u64::from(self.request.settings.async_hook_timeout_ms));
        match &self.request.platform.hook {
            ResultHook::Rust(callback) => timeout(deadline, callback(request, &mut batch))
                .await
                .map_err(|_| ResultHookError::TimedOut)??,
            #[cfg(feature = "python")]
            ResultHook::Python(callback) => {
                let prepared = match &self.python_request {
                    Some(request) => request.clone(),
                    None => {
                        let prepared = callback
                            .prepare_result(
                                request,
                                batch.columns(),
                                self.request.platform.budget.clone(),
                            )
                            .map_err(ResultHookError::from)?;
                        self.python_request = Some(prepared.clone());
                        prepared
                    }
                };
                batch = timeout(deadline, callback.call_result(prepared, batch, datasets))
                    .await
                    .map_err(|_| ResultHookError::TimedOut)?
                    .map_err(ResultHookError::from)?;
            }
        }
        drop(_guard);
        self.called = true;
        let (rows, _reservation) = batch.finish(
            self.request.settings.max_message_bytes,
            self.request.platform.output_limit(),
        )?;
        let output_rows = rows.len();
        self.emitted = self
            .emitted
            .checked_add(output_rows as u64)
            .ok_or(ResultHookError::Limit)?;
        // One chunk has one socket deadline, independent of callback/queue time.
        timeout(self.request.settings.write_timeout(), async {
            for row in rows {
                output.feed(ResultEvent::Wire(row)).await?;
            }
            output.flush().await
        })
        .await
        .map_err(|_| ExecutionError::rejected("57014", "frontend write timed out"))??;
        Ok(())
    }
    async fn write<S>(&self, output: &mut S, event: ResultEvent) -> ExecutionResult<()>
    where
        S: Sink<ResultEvent, Error = ExecutionError> + Unpin,
    {
        timeout(self.request.settings.write_timeout(), output.send(event))
            .await
            .map_err(|_| ExecutionError::rejected("57014", "frontend write timed out"))?
    }
}
