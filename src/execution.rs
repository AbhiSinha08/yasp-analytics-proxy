//! Ordered execution policy shared by the pgwire frontend profiles. Source
//! representation, session SQL, admission, and cleanup belong to backend adapters.

pub(crate) mod classification;
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
    discovery::{self, SessionSettings, SetupResponse},
    gateway::FrontendProfile,
    hooks::{ContextHook, ContextRequest, SelectionRequest},
    query::ParsedQuery,
};
use futures::{Sink, SinkExt, channel::oneshot};
use serde_json::{Map, Value};
use std::{io, time::Duration};
use tokio::{sync::watch, time::timeout};

/// Only authenticated identity and execution services cross this boundary. TLS
/// keys, password material, and frontend sockets remain in the protocol adapter.
pub(crate) struct QueryContext<'a> {
    pub username: &'a str,
    pub database: &'a str,
    pub profile: FrontendProfile,
    pub cache: Option<&'a RedisCache>,
    pub decision_model: Option<&'a crate::decision_model::DecisionModel>,
    pub ast: &'a crate::lineage::AstSettings,
    pub catalog: &'a crate::catalog::Catalog,
    pub context_hook: Option<&'a ContextHook>,
    pub datasets: Option<&'a DatasetReader>,
    pub stopped: watch::Receiver<bool>,
}

pub(crate) struct QuerySession<'a, 'backend> {
    pub execution: &'backend Execution,
    pub settings: &'a GatewaySettings,
    pub client_id: u64,
    pub source: QueryContext<'a>,
    pub session: &'a mut SessionSettings,
    pub transaction: &'a mut Transaction<'backend>,
}

impl<'backend> QuerySession<'_, 'backend> {
    pub(crate) async fn execute<S>(
        &mut self,
        output: &mut S,
        query: &ParsedQuery,
        mut cancel: oneshot::Receiver<()>,
    ) -> ExecutionResult<Option<SetupResponse>>
    where
        S: Sink<ResultEvent, Error = ExecutionError> + Unpin + Send,
    {
        let write_timeout = self.settings.write_timeout();
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
        query
            .validate_read_only()
            .map_err(|error| ExecutionError::rejected("0A000", &error.to_string()))?;
        if !query.input_options().native
            && let Some(response) = self.source.profile.fingerprint(query.statement())
        {
            return Ok(Some(response));
        }
        if !query.input_options().native
            && (discovery::classify(query).is_some()
                || matches!(
                    query.statement(),
                    sqlparser::ast::Statement::ShowVariable { .. }
                ))
        {
            return Err(ExecutionError::rejected(
                "0A000",
                "metadata query is outside the catalog compatibility surface",
            ));
        }

        // Ordinary queries derive context and select a route even on cache hits.
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
        let context = self
            .derive_context(query, &data_contexts, &mut cancel)
            .await?;
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
            context: &context,
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
        if self.transaction.active() {
            if let Some(model) = self.source.decision_model {
                let (sender, receiver) = classification::channel();
                let (source_cancel, backend_cancel) = oneshot::channel();
                let source_settings = classification::source_settings(self.settings, model);
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
                    model,
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
            output,
            cancel,
        )
        .await
        .map(|_| None)
    }

    async fn derive_context(
        &self,
        query: &ParsedQuery,
        data_contexts: &crate::lineage::DataContexts,
        cancel: &mut oneshot::Receiver<()>,
    ) -> ExecutionResult<Map<String, Value>> {
        let Some(hook) = &self.source.context_hook else {
            return Ok(Map::new());
        };
        let mut stopped = self.source.stopped.clone();
        let request = ContextRequest {
            data_contexts,
            authenticated_user: self.source.username,
            frontend_database: self.source.database,
            client_id: self.client_id,
            query_id: query.query_id(),
            query,
            datasets: self.source.datasets,
        };
        let deadline = Duration::from_millis(u64::from(self.settings.context_timeout_ms));
        tokio::select! {
            biased;
            _ = cancel => Err(ExecutionError::rejected("57014", "query cancelled")),
            _ = stopped.wait_for(|stopped| *stopped) => Err(ExecutionError::rejected("57014", "query cancelled")),
            context = hook.derive(request, deadline, self.settings.max_context_bytes) => Ok(context),
        }
    }
}
