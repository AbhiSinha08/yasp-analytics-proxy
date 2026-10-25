//! Shared bounded classification and result-processing coordinator.
//!
//! Hold a bounded prefix of raw rows while gathering column samples. Resolve model
//! outcomes (or load their cache association) before optional result callbacks see
//! those rows. A small channel and frontend write deadlines propagate backpressure
//! to the source instead of collecting the whole result. Cancellation still waits
//! for adapter-owned source cleanup.
use super::processing::{ProcessingRequest, Processor};
use crate::{
    backend::result::{ExecutionError, ExecutionResult, ResultEvent},
    decision_model::{Classifications, ColumnIdentity, DecisionModel, PREFIX_BYTES, PREFIX_ROWS},
    hooks::result::Reservation,
    query::ParsedQuery,
    wire::{self, Response},
};
use futures::{
    Sink, SinkExt, StreamExt,
    channel::{mpsc, oneshot},
};
use pgwire::messages::{Message, PgWireBackendMessage};
use std::{future::Future, io, sync::Mutex};
use tokio::{
    sync::watch,
    time::{sleep_until, timeout},
};

pub(crate) struct StreamOptions<'a, 'query> {
    pub model: Option<&'a DecisionModel>,
    pub cached: Option<Classifications>,
    pub processing: Option<ProcessingRequest<'a, 'query>>,
}

pub(crate) fn source_settings(
    settings: &crate::config::GatewaySettings,
    model: Option<&DecisionModel>,
    processing: bool,
) -> crate::config::GatewaySettings {
    let mut source = settings.clone();
    // Internal channel waits include processing; socket writes keep their own deadline.
    source.write_timeout_ms = settings
        .write_timeout_ms
        .saturating_add(model.map_or(0, |model| model.settings().timeout_ms))
        .saturating_add(if processing {
            settings.async_hook_timeout_ms.saturating_add(2)
        } else {
            0
        })
        .min(i32::MAX as u32);
    source
}

pub(crate) struct StreamEvent {
    response: Response,
    reservation: Option<Reservation>,
}

pub(crate) fn channel(
    budget: Option<std::sync::Arc<tokio::sync::Semaphore>>,
) -> (
    impl Sink<ResultEvent, Error = ExecutionError> + Unpin + Send,
    mpsc::Receiver<StreamEvent>,
) {
    let (sender, receiver) = mpsc::channel(0);
    let sink = sender.sink_map_err(|_| {
        ExecutionError::from(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "result processing stopped",
        ))
    });
    (
        sink.with(move |event| {
            let response = wire::encode(event);
            let result = (|| {
                let reservation = if let Some(budget) = &budget {
                    let mut reservation = Reservation::new(budget.clone());
                    reservation.charge(response.buffer_bytes())?;
                    Some(reservation)
                } else {
                    None
                };
                Ok::<_, ExecutionError>(StreamEvent {
                    response,
                    reservation,
                })
            })();
            futures::future::ready(result)
        }),
        receiver,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn run<T, S>(
    source: impl Future<Output = T>,
    receiver: mpsc::Receiver<StreamEvent>,
    output: &mut S,
    options: StreamOptions<'_, '_>,
    query: &ParsedQuery,
    mut cancel: oneshot::Receiver<()>,
    source_cancel: oneshot::Sender<()>,
    write_timeout: std::time::Duration,
) -> ExecutionResult<(T, Classifications)>
where
    S: Sink<ResultEvent, Error = ExecutionError> + Unpin,
{
    let (stopping, mut stopped) = watch::channel(false);
    let source_cancel = Mutex::new(Some(source_cancel));
    let consumer = async {
        let result = tokio::select! {
            biased;
            _ = stopped.wait_for(|s| *s) => Err(ExecutionError::cancelled()),
            result = forward(receiver, output, options, query, write_timeout) => result,
        };
        if result.is_err()
            && let Some(cancel) = source_cancel.lock().unwrap().take()
        {
            let _ = cancel.send(());
        }
        result
    };
    let work = async { futures::join!(source, consumer) };
    tokio::pin!(work);
    let (source, classifications) = tokio::select! {
        biased;
        _ = &mut cancel => {
            stopping.send_replace(true);
            if let Some(cancel) = source_cancel.lock().unwrap().take() { let _ = cancel.send(()); }
            // The source owns rollback/cancel cleanup and its connection lease.
            let _ = work.await;
            return Err(ExecutionError::cancelled());
        },
        result = &mut work => result,
    };
    Ok((source, classifications?))
}

async fn emit<S>(
    event: StreamEvent,
    output: &mut S,
    processor: &mut Option<Processor<'_, '_>>,
    decisions: Option<&Classifications>,
    write_timeout: std::time::Duration,
) -> ExecutionResult<()>
where
    S: Sink<ResultEvent, Error = ExecutionError> + Unpin,
{
    if let Some(processor) = processor {
        processor
            .send(event.response, output, decisions, event.reservation)
            .await
    } else {
        timeout(
            write_timeout,
            output.send(ResultEvent::Wire(event.response)),
        )
        .await
        .map_err(|_| ExecutionError::rejected("57014", "frontend write timed out"))?
    }
}

async fn forward<S>(
    mut receiver: mpsc::Receiver<StreamEvent>,
    output: &mut S,
    options: StreamOptions<'_, '_>,
    query: &ParsedQuery,
    write_timeout: std::time::Duration,
) -> ExecutionResult<Classifications>
where
    S: Sink<ResultEvent, Error = ExecutionError> + Unpin,
{
    let has_model = options.model.is_some();
    let mut processor = options.processing.map(Processor::new);
    let mut columns: Vec<ColumnIdentity> = Vec::new();
    let mut samples: Vec<Vec<String>> = Vec::new();
    let mut prefix: Vec<StreamEvent> = Vec::new();
    let mut bytes = 0usize;
    let mut rows = 0usize;
    let mut decisions = options
        .cached
        .or_else(|| options.model.is_none().then(Classifications::default));
    loop {
        let deadline = processor.as_ref().and_then(Processor::deadline);
        let next = tokio::select! {
            biased;
            _ = async { match deadline { Some(deadline) => sleep_until(deadline).await,
                None => std::future::pending().await } } => {
                if let Some(processor) = &mut processor {
                    processor.flush(output, decisions.as_ref().filter(|_| has_model), false).await?;
                }
                continue;
            },
            next = receiver.next() => next,
        };
        let Some(event) = next else {
            break;
        };
        let response = &event.response;
        if decisions.is_some()
            || (columns.is_empty()
                && matches!(response.message(), PgWireBackendMessage::NoticeResponse(_)))
        {
            emit(
                event,
                output,
                &mut processor,
                decisions.as_ref().filter(|_| has_model),
                write_timeout,
            )
            .await?;
            continue;
        }
        if let Some(metadata) = wire::sample_columns(response) {
            samples = vec![Vec::new(); metadata.len()];
            columns = metadata;
        }
        let size = match response.message() {
            PgWireBackendMessage::DataRow(v) => v.message_length() + 1,
            PgWireBackendMessage::RowDescription(v) => v.message_length() + 1,
            PgWireBackendMessage::NoticeResponse(v) => v.message_length() + 1,
            PgWireBackendMessage::CommandComplete(v) => v.message_length() + 1,
            _ => return Err(ExecutionError::rejected("08006", "unexpected result event")),
        };
        let overflow = size > PREFIX_BYTES.saturating_sub(bytes);
        if matches!(response.message(), PgWireBackendMessage::DataRow(_)) && !overflow {
            let values = wire::sample_row(response)
                .ok_or_else(|| ExecutionError::rejected("08006", "invalid result row"))?;
            if values.len() != columns.len() {
                return Err(ExecutionError::rejected(
                    "08006",
                    "invalid result column count",
                ));
            }
            rows += 1;
            let model = options.model.expect("classification gate requires a model");
            for (samples, value) in samples.iter_mut().zip(values) {
                if samples.len() < model.settings().sample_size
                    && let Some(value) = value
                {
                    samples.push(value);
                }
            }
        }
        bytes = bytes.saturating_add(size);
        let complete = matches!(response.message(), PgWireBackendMessage::CommandComplete(_));
        prefix.push(event);
        let model = options.model.expect("classification gate requires a model");
        if overflow
            || rows >= PREFIX_ROWS
            || bytes >= PREFIX_BYTES
            || complete
            || (rows > 0
                && samples
                    .iter()
                    .all(|s| s.len() >= model.settings().sample_size))
        {
            let results = model
                .classify_columns(&query.statement().to_string(), &columns, &samples)
                .await;
            model.check(&results, columns.len())?;
            decisions = Some(results);
            for event in prefix.drain(..) {
                emit(
                    event,
                    output,
                    &mut processor,
                    decisions.as_ref(),
                    write_timeout,
                )
                .await?;
            }
        }
    }
    // An incomplete source never flushes its pending prefix or last batch.
    Ok(decisions.unwrap_or_default())
}
