//! Admitted SQL API requests. Frontend transactions pin the route while each
//! statement autocommits; commit/rollback do not imply a shared source snapshot.
use super::{Inner, rest, result, validate_session};
use crate::{
    backend::result::{ExecutionError, ExecutionResult, ResultEvent},
    config::GatewaySettings,
    discovery::SessionSettings,
    query::ParsedQuery,
};
use futures::{Sink, SinkExt, channel::oneshot};
use reqwest::Method;
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::{
    sync::OwnedSemaphorePermit,
    time::{Instant, timeout, timeout_at},
};

pub(crate) struct Session {
    inner: Arc<Inner>,
    permit: Arc<OwnedSemaphorePermit>,
}
impl Session {
    pub(super) async fn open(
        inner: Arc<Inner>,
        _: &GatewaySettings,
        _: bool,
    ) -> ExecutionResult<Self> {
        let permit = timeout(inner.acquire_timeout, inner.permits.clone().acquire_owned())
            .await
            .map_err(|_| ExecutionError::rejected("53300", "Snowflake capacity wait timed out"))?
            .map_err(|_| ExecutionError::rejected("08006", "Snowflake binding is closed"))?;
        Ok(Self {
            inner,
            permit: Arc::new(permit),
        })
    }
    pub(crate) fn usable(&self) -> bool {
        !self.inner.permits.is_closed()
    }
    pub(crate) async fn finish(self, _: bool) -> ExecutionResult<()> {
        Ok(())
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
        S: Sink<ResultEvent, Error = ExecutionError> + Unpin + Send,
    {
        self.execute_sql(query.sql(), output, settings, session, cancel)
            .await
    }
    pub(crate) async fn execute_sql<S>(
        &mut self,
        sql: &str,
        output: &mut S,
        settings: &GatewaySettings,
        session: &SessionSettings,
        mut cancel: oneshot::Receiver<()>,
    ) -> ExecutionResult<ResultEvent>
    where
        S: Sink<ResultEvent, Error = ExecutionError> + Unpin + Send,
    {
        validate_session(session)?;
        let server_seconds = settings.query_timeout().as_secs().saturating_add(1);
        let mut operation = Operation {
            inner: self.inner.clone(),
            permit: self.permit.clone(),
            handle: None,
            cleanup_timeout: Duration::from_secs(server_seconds + 2),
            armed: true,
        };
        let work = run(&mut operation, sql, output, settings, server_seconds);
        tokio::select! {
            biased;
            _ = &mut cancel => Err(ExecutionError::cancelled()),
            result = timeout(settings.query_timeout(), work) => result.unwrap_or_else(|_| Err(ExecutionError::rejected("57014", "Snowflake query timed out"))),
        }
    }
}

struct Operation {
    inner: Arc<Inner>,
    permit: Arc<OwnedSemaphorePermit>,
    handle: Option<String>,
    cleanup_timeout: Duration,
    armed: bool,
}
impl Drop for Operation {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let inner = self.inner.clone();
        let permit = self.permit.clone();
        let handle = self.handle.take();
        let deadline = Instant::now() + self.cleanup_timeout;
        // An abandoned submission may have reached the server without returning
        // its handle. Allow a full server timeout from interruption, because
        // submission might have reached the server just before the disconnect.
        tokio::spawn(async move {
            if let Some(handle) = handle {
                let path = format!("/api/v2/statements/{handle}");
                let _ = timeout_at(
                    deadline,
                    inner.request(Method::POST, &format!("{path}/cancel"), None),
                )
                .await;
                while Instant::now() < deadline {
                    match timeout_at(deadline, inner.request(Method::GET, &path, None)).await {
                        Ok(Ok(response)) if !response.pending() => break,
                        Ok(Err(ExecutionError::Source { .. })) => break,
                        _ => tokio::time::sleep(Duration::from_millis(100)).await,
                    }
                }
            } else {
                tokio::time::sleep_until(deadline).await;
            }
            drop(permit);
        });
    }
}
async fn run<S>(
    operation: &mut Operation,
    sql: &str,
    output: &mut S,
    settings: &GatewaySettings,
    seconds: u64,
) -> ExecutionResult<ResultEvent>
where
    S: Sink<ResultEvent, Error = ExecutionError> + Unpin + Send,
{
    let inner = operation.inner.clone();
    let request = json!({"statement": sql, "timeout": seconds, "database": inner.database,
        "schema": inner.catalog_identity.default_schema, "role": inner.catalog_identity.role, "warehouse": inner.catalog_identity.warehouse,
        "parameters": {"MULTI_STATEMENT_COUNT":"1", "TIMEZONE":"UTC", "BINARY_OUTPUT_FORMAT":"HEX",
        "DATE_OUTPUT_FORMAT":"YYYY-MM-DD", "TIME_OUTPUT_FORMAT":"HH24:MI:SS.FF9",
        "TIMESTAMP_NTZ_OUTPUT_FORMAT":"YYYY-MM-DD HH24:MI:SS.FF9",
        "TIMESTAMP_LTZ_OUTPUT_FORMAT":"YYYY-MM-DD HH24:MI:SS.FF9 TZH:TZM",
        "TIMESTAMP_TZ_OUTPUT_FORMAT":"YYYY-MM-DD HH24:MI:SS.FF9 TZH:TZM"}});
    let path = format!(
        "/api/v2/statements?async=true&requestId={}",
        uuid::Uuid::new_v4()
    );
    let mut response = inner.request(Method::POST, &path, Some(request)).await?;
    loop {
        if !response.statement_handle.is_empty() {
            operation.handle = Some(rest::handle(&response.statement_handle)?.to_owned());
        }
        if let Err(error) = response.check() {
            operation.armed = false;
            return Err(error);
        }
        if !response.pending() {
            break;
        }
        if operation.handle.is_none() {
            return Err(rest::protocol());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        response = inner
            .request(
                Method::GET,
                &format!("/api/v2/statements/{}", operation.handle.as_ref().unwrap()),
                None,
            )
            .await?;
    }
    let metadata = response
        .result_set_meta_data
        .take()
        .ok_or_else(rest::protocol)?;
    if metadata.format != "jsonv2" {
        return Err(rest::protocol());
    }
    let columns = result::columns(&metadata.row_type, settings.max_message_bytes)?;
    send(output, ResultEvent::Columns(columns), settings).await?;
    let partitions = metadata.partition_info.len().max(1);
    if partitions > 100_000 {
        return Err(rest::oversized());
    }
    let mut count = 0u64;
    for index in 0..partitions {
        if index > 0 {
            if metadata.partition_info[index].uncompressed_size > inner.max_response_bytes {
                return Err(rest::oversized());
            }
            response = inner
                .request(
                    Method::GET,
                    &format!(
                        "/api/v2/statements/{}?partition={index}",
                        operation.handle.as_deref().ok_or_else(rest::protocol)?
                    ),
                    None,
                )
                .await?;
            response.check()?;
        }
        if let Some(partition) = metadata.partition_info.get(index)
            && partition.row_count != response.data.len() as u64
        {
            return Err(rest::protocol());
        }
        for row in response.data.drain(..) {
            send(
                output,
                ResultEvent::Row(result::row(
                    &metadata.row_type,
                    row,
                    settings.max_message_bytes,
                )?),
                settings,
            )
            .await?;
            count = count.checked_add(1).ok_or_else(rest::protocol)?;
        }
        if count > metadata.num_rows {
            return Err(rest::protocol());
        }
    }
    if count != metadata.num_rows {
        return Err(rest::protocol());
    }
    operation.armed = false;
    Ok(ResultEvent::Complete(count))
}
async fn send<S>(
    output: &mut S,
    event: ResultEvent,
    settings: &GatewaySettings,
) -> ExecutionResult<()>
where
    S: Sink<ResultEvent, Error = ExecutionError> + Unpin,
{
    timeout(settings.write_timeout(), output.send(event))
        .await
        .map_err(|_| ExecutionError::rejected("08006", "frontend write timed out"))?
}
