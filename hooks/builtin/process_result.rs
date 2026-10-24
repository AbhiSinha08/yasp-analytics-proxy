//! Reusable presentation callbacks; organizations own their policy callbacks.
use crate::hooks::{ResultBatch, ResultHookError, ResultRequest};
use futures::future::BoxFuture;

/// Uppercase one exact, unambiguous output name without changing NULLs.
pub fn uppercase<'a>(
    _: ResultRequest<'a>,
    batch: &'a mut ResultBatch,
    column: std::sync::Arc<String>,
) -> BoxFuture<'a, Result<(), ResultHookError>> {
    Box::pin(async move {
        let column = batch.column_index(&column)?;
        if !batch.columns()[column].is_text() {
            return Err(ResultHookError::Type);
        }
        for row in 0..batch.len() {
            if let Some(value) = batch.text(row, column)?.map(str::to_uppercase) {
                batch.set_text(row, column, &value)?;
            }
        }
        Ok(())
    })
}
