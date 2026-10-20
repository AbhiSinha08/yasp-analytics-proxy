//! Autocommit Snowflake execution and source cleanup.
use super::SnowflakeBackend;
use crate::backend::{
    execution::ExecutionRequest,
    result::{ExecutionError, ExecutionResult, ResultEvent},
};
use futures::{Sink, SinkExt, channel::oneshot};
use tokio::time::timeout;
impl SnowflakeBackend {
    pub(crate) fn execution_identity(&self) -> [u8; 32] {
        let source = &self.inner.catalog_identity;
        crate::backend::execution::identity(&[
            "snowflake:sql-api:text:v1",
            &source.endpoint,
            &source.principal,
            source.role.as_deref().unwrap_or(""),
            source.warehouse.as_deref().unwrap_or(""),
            source.default_schema.as_deref().unwrap_or(""),
        ])
    }
    pub(crate) async fn execute_query<S>(
        &self,
        request: ExecutionRequest<'_, '_>,
        output: &mut S,
        mut cancel: oneshot::Receiver<()>,
    ) -> ExecutionResult<Option<[u8; 32]>>
    where
        S: Sink<ResultEvent, Error = ExecutionError> + Unpin + Send,
    {
        let query = request.selection.query;
        self.validate_input(query)?;
        super::validate_session(request.session)?;
        self.log_query(query, request.selection.client_id);
        let mut session = tokio::select! {
            biased;
            _ = &mut cancel => return Err(ExecutionError::cancelled()),
            session = self.open(request.settings, false) => session?,
        };
        let completion = session
            .execute(query, output, request.settings, request.session, cancel)
            .await?;
        session.finish(false).await?;
        timeout(request.settings.write_timeout(), output.send(completion))
            .await
            .map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::TimedOut, "frontend write timed out")
            })??;
        Ok(Some(self.execution_identity()))
    }
}
