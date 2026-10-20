//! One bounded prefix gate shared by source engines and transaction paths.
use crate::{
    backend::result::{ExecutionError, ExecutionResult, ResultEvent},
    decision_model::{Classifications, ColumnIdentity, DecisionModel, PREFIX_BYTES, PREFIX_ROWS},
    query::ParsedQuery,
    wire::{self, Response},
};
use futures::{
    Sink, SinkExt, StreamExt,
    channel::{mpsc, oneshot},
};
use pgwire::messages::{Message, PgWireBackendMessage};
use std::{future::Future, io};
use tokio::{sync::watch, time::timeout};

pub(crate) fn source_settings(
    settings: &crate::config::GatewaySettings,
    model: &DecisionModel,
) -> crate::config::GatewaySettings {
    let mut source = settings.clone();
    // Adapters write to an internal channel here. Its backpressure can include
    // inference; actual frontend writes still use the original write deadline.
    source.write_timeout_ms = settings
        .write_timeout_ms
        .saturating_add(model.settings().timeout_ms)
        .min(i32::MAX as u32);
    source
}

pub(crate) fn channel() -> (
    impl Sink<ResultEvent, Error = ExecutionError> + Unpin + Send,
    mpsc::Receiver<ResultEvent>,
) {
    // One pending event: source adapters retain their existing write bounds and
    // cleanup ownership while model inference pauses frontend output.
    let (sender, receiver) = mpsc::channel(0);
    (
        sender.sink_map_err(|_| {
            io::Error::new(io::ErrorKind::BrokenPipe, "result processing stopped").into()
        }),
        receiver,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn run<T, S>(
    source: impl Future<Output = T>,
    receiver: mpsc::Receiver<ResultEvent>,
    output: &mut S,
    model: &DecisionModel,
    query: &ParsedQuery,
    mut cancel: oneshot::Receiver<()>,
    source_cancel: oneshot::Sender<()>,
    write_timeout: std::time::Duration,
) -> ExecutionResult<(T, Classifications)>
where
    S: Sink<ResultEvent, Error = ExecutionError> + Unpin,
{
    let (stopping, mut stopped) = watch::channel(false);
    let consumer = async {
        tokio::select! {
            biased;
            _ = stopped.wait_for(|s| *s) => Err(ExecutionError::cancelled()),
            result = forward(receiver, output, model, query, write_timeout) => result,
        }
    };
    let work = async { futures::join!(source, consumer) };
    tokio::pin!(work);
    let (source, classifications) = tokio::select! {
        biased;
        _ = &mut cancel => {
            stopping.send_replace(true);
            let _ = source_cancel.send(());
            // Dropping the consumer closes its channel. Await the source's
            // existing rollback/cancellation cleanup before releasing its lease.
            let _ = work.await;
            return Err(ExecutionError::cancelled());
        },
        result = &mut work => result,
    };
    Ok((source, classifications?))
}

async fn forward<S>(
    mut receiver: mpsc::Receiver<ResultEvent>,
    output: &mut S,
    model: &DecisionModel,
    query: &ParsedQuery,
    write_timeout: std::time::Duration,
) -> ExecutionResult<Classifications>
where
    S: Sink<ResultEvent, Error = ExecutionError> + Unpin,
{
    let mut columns: Vec<ColumnIdentity> = Vec::new();
    let mut samples: Vec<Vec<String>> = Vec::new();
    let mut prefix: Vec<Response> = Vec::new();
    let mut bytes = 0usize;
    let mut rows = 0usize;
    let mut decisions = None;
    while let Some(event) = receiver.next().await {
        let response = wire::encode(event);
        if decisions.is_some()
            || (columns.is_empty()
                && matches!(response.message(), PgWireBackendMessage::NoticeResponse(_)))
        {
            timeout(write_timeout, output.send(ResultEvent::Wire(response)))
                .await
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::TimedOut, "frontend write timed out")
                })??;
            continue;
        }
        if let Some(metadata) = wire::sample_columns(&response) {
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
        let row_size =
            matches!(response.message(), PgWireBackendMessage::DataRow(_)).then_some(size);
        if let Some(_size) = row_size.filter(|_| !overflow) {
            let values = wire::sample_row(&response)
                .ok_or_else(|| ExecutionError::rejected("08006", "invalid result row"))?;
            if values.len() != columns.len() {
                return Err(ExecutionError::rejected(
                    "08006",
                    "invalid result column count",
                ));
            }
            rows += 1;

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
        prefix.push(response);
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
            for response in prefix.drain(..) {
                timeout(write_timeout, output.send(ResultEvent::Wire(response)))
                    .await
                    .map_err(|_| {
                        io::Error::new(io::ErrorKind::TimedOut, "frontend write timed out")
                    })??;
            }
            decisions = Some(results);
        }
    }
    // A source error can close the stream without completion. Its caller owns
    // that error; never classify or emit a buffered incomplete prefix here.
    Ok(decisions.unwrap_or_default())
}
