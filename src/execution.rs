//! Ordered execution policy shared by the pgwire frontend profiles. Source
//! representation, session SQL, admission, and cleanup belong to backend adapters.

pub(crate) mod classification;
pub(crate) mod processing;
mod transaction;
pub(crate) use transaction::{Transaction, TransactionStatus};

use crate::{
    backend::{
        execution::{Execution, SessionRequest},
        result::{ExecutionError, ExecutionResult, ResultEvent},
    },
    cache::RedisCache,
    config::GatewaySettings,
    datasets::DatasetReader,
    discovery::{SessionSettings, SetupResponse},
    gateway::FrontendProfile,
    hooks::SelectionRequest,
    query::ParsedQuery,
};
use futures::{Sink, SinkExt, channel::oneshot};
use serde_json::{Map, Value};
use std::io;
use tokio::time::timeout;

/// Only authenticated identity and execution services cross this boundary. TLS
/// keys, password material, and frontend sockets remain in the protocol adapter.
pub(crate) struct ExecutionServices<'a> {
    pub username: &'a str,
    pub database: &'a str,
    pub profile: FrontendProfile,
    pub cache: Option<&'a RedisCache>,
    pub decision_model: Option<&'a crate::decision_model::DecisionModel>,
    pub ast: &'a crate::lineage::AstSettings,
    pub catalog: &'a crate::catalog::Catalog,
    pub result_processing: Option<&'a crate::hooks::result::ResultProcessing>,
    pub datasets: Option<&'a DatasetReader>,
}

pub(crate) struct QuerySession<'a, 'backend> {
    pub execution: &'backend Execution,
    pub settings: &'a GatewaySettings,
    pub client_id: u64,
    pub source: ExecutionServices<'a>,
    pub session: &'a mut SessionSettings,
    pub transaction: &'a mut Transaction<'backend>,
}

impl<'backend> QuerySession<'_, 'backend> {
    pub(crate) async fn execute<S>(
        &mut self,
        output: &mut S,
        query: &ParsedQuery,
        query_context: &Map<String, Value>,
        frontend_request: bool,
        mut cancel: oneshot::Receiver<()>,
    ) -> ExecutionResult<Option<SetupResponse>>
    where
        S: Sink<ResultEvent, Error = ExecutionError> + Unpin + Send,
    {
        let write_timeout = self.settings.write_timeout();
        if frontend_request {
            let application_name = self.session.application_name.clone();
            let control = self
                .transaction
                .control(query.statement(), self.session)
                .await;
            if application_name != self.session.application_name {
                timeout(
                    write_timeout,
                    output.send(ResultEvent::Parameter {
                        name: "application_name".into(),
                        value: self.session.application_name.clone(),
                    }),
                )
                .await
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::TimedOut, "frontend setup response timed out")
                })??;
            }
            if let Some(tag) = control? {
                return Ok(Some(SetupResponse::Command(tag)));
            }
            self.transaction.check()?;
            if let Some(response) = self.source.profile.setup(query) {
                return Ok(Some(response));
            }
            if let Some(response) = self
                .session
                .handle(query.statement())
                .map_err(|message| ExecutionError::rejected("0A000", message))?
            {
                return Ok(Some(response));
            }
            if let Some(response) = self.source.profile.fingerprint(query.statement()) {
                return Ok(Some(response));
            }
            return Err(ExecutionError::rejected(
                "0A000",
                "unsupported frontend request",
            ));
        }
        self.transaction.check()?;
        query
            .validate_read_only()
            .map_err(|error| ExecutionError::rejected("0A000", &error.to_string()))?;
        // Ordinary queries select a route with current query context even on cache hits.
        if cancel.try_recv().ok().flatten().is_some() {
            return Err(ExecutionError::cancelled());
        }
        let data_contexts = crate::lineage::DataContexts {
            ast: self.source.ast.enabled.then(|| {
                let snapshot = self.source.catalog.snapshot().ok();
                crate::lineage::AstDataContext::analyze(
                    query,
                    snapshot.as_deref(),
                    self.settings.max_context_bytes,
                )
            }),
        };
        let query_id = query.query_id();
        let client_id = self.client_id;
        let authenticated_user = self.source.username;
        let frontend_database = self.source.database;
        let request = SelectionRequest {
            data_contexts: &data_contexts,
            authenticated_user,
            client_id,
            query_id,
            query,
            query_context,
            datasets: self.source.datasets,
            async_hook_timeout_ms: self.settings.async_hook_timeout_ms,
        };
        let execution: &'backend Execution = self.execution;
        let route = tokio::select! {
            biased;
            _ = &mut cancel => return Err(ExecutionError::rejected("57014", "query cancelled")),
            result = execution.select(&request) => result?,
        };
        route.validate_input(query)?;
        let processing =
            self.source
                .result_processing
                .map(|platform| processing::ProcessingRequest {
                    platform,
                    selection: &request,
                    database: frontend_database,
                    route: route.selection(),
                    settings: self.settings,
                });
        if self.transaction.active() {
            let model = self.source.decision_model;
            if model.is_some() || processing.is_some() {
                let (sender, receiver) =
                    classification::channel(processing.map(|p| p.platform.budget.clone()));
                let (source_cancel, backend_cancel) = oneshot::channel();
                let source_settings =
                    classification::source_settings(self.settings, model, processing.is_some());
                let source = async {
                    let mut sender = sender;
                    self.transaction
                        .execute(
                            &route,
                            SessionRequest {
                                query,
                                session: self.session,
                                settings: &source_settings,
                                client_id: self.client_id,
                            },
                            &mut sender,
                            backend_cancel,
                        )
                        .await
                };
                classification::run(
                    source,
                    receiver,
                    output,
                    classification::StreamOptions {
                        model,
                        cached: None,
                        processing,
                    },
                    query,
                    cancel,
                    source_cancel,
                    write_timeout,
                )
                .await?
                .0?;
                return Ok(None);
            }
            return self
                .transaction
                .execute(
                    &route,
                    SessionRequest {
                        query,
                        session: self.session,
                        settings: self.settings,
                        client_id: self.client_id,
                    },
                    output,
                    cancel,
                )
                .await
                .map(|_| None);
        }
        crate::cache::execute(
            &route,
            crate::backend::execution::ExecutionRequest {
                selection: &request,
                settings: self.settings,
                session: self.session,
            },
            frontend_database,
            self.source.cache,
            (self.source.ast, self.source.decision_model),
            processing,
            output,
            cancel,
        )
        .await
        .map(|_| None)
    }
}
