//! Cache policy wraps ordinary execution after routing and before admission.
//!
//! A validated hit bypasses source connections but still runs result processing.
//! On a miss, raw source events are captured before application edits. Publication
//! requires complete source success and cleanup. Partial, failed, and oversized
//! results never become reusable entries.
use super::{
    HEADER_BYTES, MAGIC, RedisCache,
    response::{self, CaptureSink},
};
use crate::backend::{
    execution::{ExecutionRequest, Route},
    result::{ExecutionError, ExecutionResult, ResultEvent},
};
use futures::{Sink, channel::oneshot};
use sha2::{Digest, Sha256};

#[allow(clippy::too_many_arguments)]
pub(crate) async fn execute<S>(
    route: &Route<'_>,
    request: ExecutionRequest<'_, '_>,
    frontend_database: &str,
    cache: Option<&RedisCache>,
    contexts: (
        &crate::lineage::AstSettings,
        Option<&crate::decision_model::DecisionModel>,
    ),
    processing: Option<crate::execution::processing::ProcessingRequest<'_, '_>>,
    output: &mut S,
    mut cancel: oneshot::Receiver<()>,
) -> ExecutionResult<()>
where
    S: Sink<ResultEvent, Error = ExecutionError> + Unpin + Send,
{
    let (ast_settings, model) = contexts;
    // Cache hits must obey the same adapter session restrictions as source reads.
    route.validate_session(request.session)?;
    let selected = route.selection();
    let query_id = request.selection.query_id;
    let client_id = request.selection.client_id;
    let mut buffer = None;
    if let Some(cache) = cache {
        let eligible = tokio::select! {
            biased;
            _ = &mut cancel => return Err(ExecutionError::cancelled()),
            eligible = cache.eligible(request.selection) => eligible,
        };
        if eligible {
            let scope = scope(&request, frontend_database, route.identity());
            buffer = cache.reserve(&scope);
            if let Some(entry) = &mut buffer {
                let hit = tokio::select! {
                    biased;
                    _ = &mut cancel => return Err(ExecutionError::cancelled()),
                    hit = cache.get(&selected.target, &selected.backend_login, query_id, &scope, entry) => hit,
                };
                if hit
                    && response::valid_response(
                        &entry.bytes,
                        cache.max_rows_per_query(),
                        request.settings.max_message_bytes,
                    )
                    && (model.is_none()
                        || entry.reserve_bytes(
                            entry.bytes.capacity()
                                + crate::decision_model::MAX_METADATA_BYTES.min(
                                    cache
                                        .max_entry_bytes()
                                        .saturating_sub(entry.bytes.capacity()),
                                ),
                        ))
                {
                    let cached = if let Some(model) = model {
                        let columns = response::columns(&entry.bytes);
                        let results = tokio::select! {
                            biased;
                            _ = &mut cancel => return Err(ExecutionError::cancelled()),
                            result = cache.classifications(&selected.target, &selected.backend_login, query_id, entry, &columns, model) => result,
                        };
                        model.check(&results, columns.len())?;
                        Some(results)
                    } else {
                        None
                    };
                    tracing::debug!(query_id, client_id, target = %selected.target,
                        backend_login = %selected.backend_login, "query served from cache");
                    if processing.is_some() {
                        let (mut sender, receiver) = crate::execution::classification::channel(
                            processing.map(|p| p.platform.budget.clone()),
                        );
                        let (source_cancel, mut backend_cancel) = oneshot::channel();
                        let settings = crate::execution::classification::source_settings(
                            request.settings,
                            model,
                            true,
                        );
                        let source = async move {
                            response::replay(
                                entry,
                                &mut sender,
                                settings.write_timeout(),
                                &mut backend_cancel,
                            )
                            .await
                        };
                        crate::execution::classification::run(
                            source,
                            receiver,
                            output,
                            crate::execution::classification::StreamOptions {
                                model,
                                cached,
                                processing,
                            },
                            request.selection.query,
                            cancel,
                            source_cancel,
                            request.settings.write_timeout(),
                        )
                        .await?
                        .0?;
                        return Ok(());
                    }
                    return response::replay(
                        entry,
                        output,
                        request.settings.write_timeout(),
                        &mut cancel,
                    )
                    .await;
                }
                // Release lookup/metadata reservations before capturing a miss.
                drop(buffer.take());
                buffer = cache.reserve(&scope);
            }
            tracing::debug!(
                query_id,
                client_id,
                capturing = buffer.is_some(),
                "cache miss"
            );
        }
    }
    let (completed_source, mut buffer, classifications) = if model.is_some() || processing.is_some()
    {
        let (mut sender, receiver) = crate::execution::classification::channel(
            processing.map(|p| p.platform.budget.clone()),
        );
        let (source_cancel, backend_cancel) = oneshot::channel();
        let source_settings = crate::execution::classification::source_settings(
            request.settings,
            model,
            processing.is_some(),
        );
        let source = async move {
            let mut capture = CaptureSink::new(&mut sender, buffer, cache);
            let result = route
                .execute(
                    ExecutionRequest {
                        settings: &source_settings,
                        ..request
                    },
                    &mut capture,
                    backend_cancel,
                )
                .await;
            (result, capture.take_buffer())
        };
        let ((result, buffer), classifications) = crate::execution::classification::run(
            source,
            receiver,
            output,
            crate::execution::classification::StreamOptions {
                model,
                cached: None,
                processing,
            },
            request.selection.query,
            cancel,
            source_cancel,
            request.settings.write_timeout(),
        )
        .await?;
        (result?, buffer, model.map(|_| classifications))
    } else {
        let mut capture = CaptureSink::new(output, buffer, cache);
        let result = route.execute(request, &mut capture, cancel).await?;
        (result, capture.take_buffer(), None)
    };
    if let (Some(cache), Some(identity), Some(buffer)) = (cache, completed_source, buffer.as_mut())
    {
        // Overflow may execute on a different physical source than the cache probe.
        // Publish only a complete, clean result under its actual source identity.
        let scope = scope(&request, frontend_database, identity);
        buffer.bytes[MAGIC.len()..HEADER_BYTES].copy_from_slice(&scope);
        if response::valid_response(
            &buffer.bytes[HEADER_BYTES..],
            cache.max_rows_per_query(),
            request.settings.max_message_bytes,
        ) {
            let ast = request.selection.data_contexts.ast.as_ref().map(|ast| {
                let columns = response::columns(&buffer.bytes[super::HEADER_BYTES..]);
                let names: Vec<_> = columns.iter().map(|c| c.name.as_str()).collect();
                ast.for_columns(&names)
            });
            let decisions = classifications.as_ref().zip(model);
            if decisions.is_some() || ast.is_some() {
                cache
                    .put_contexts(
                        &selected.target,
                        &selected.backend_login,
                        query_id,
                        buffer,
                        decisions,
                        ast.as_ref()
                            .map(|ast| (ast, ast_settings.namespace.as_str())),
                    )
                    .await;
            } else {
                cache
                    .put(&selected.target, &selected.backend_login, query_id, buffer)
                    .await;
            }
        }
    }
    Ok(())
}

fn scope(request: &ExecutionRequest<'_, '_>, database: &str, source: [u8; 32]) -> [u8; 32] {
    let dialect = request.selection.query.dialect();
    let mut hash = Sha256::new();
    for value in [
        "pgwire:text:v2:query-context-dialect:v2",
        &request.settings.protocol,
        &format!("{dialect:?}"),
        request.selection.authenticated_user,
        database,
        &request.session.search_path,
        &request.session.application_name,
    ] {
        hash.update((value.len() as u64).to_be_bytes());
        hash.update(value.as_bytes());
    }
    hash.update(source);
    hash.update(request.settings.query_timeout_ms.to_be_bytes());
    hash.update(request.settings.write_timeout_ms.to_be_bytes());
    hash.update(request.settings.max_message_bytes.to_be_bytes());
    hash.finalize().into()
}
