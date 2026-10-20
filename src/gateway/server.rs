//! Listener admission, shared authentication material, and bounded shutdown.

use super::{FrontendProfile, protocol_error, session::connection};
use crate::backend::execution::Execution;
use crate::{
    cache::RedisCache, config::GatewayConfig, datasets::DatasetReader, hooks::ContextHook, logger,
};
use async_trait::async_trait;
use pgwire::{
    api::{
        ConnectionManager, RandomPidSecretKeyGenerator,
        auth::{
            AuthSource, LoginInfo, Password,
            sasl::scram::{SCRAM_ITERATIONS, gen_salted_password, random_nonce},
        },
    },
    error::PgWireResult,
};
use std::{fmt, future::Future, io, sync::Arc, time::Duration};
use tokio::{net::TcpListener, sync::watch, task::JoinSet, time::timeout};

const MAX_CANCEL_CONNECTIONS: usize = 8;

pub(super) async fn serve_sessions(
    listener: TcpListener,
    config: GatewayConfig,
    execution: Execution,
    shutdown: impl Future<Output = ()>,
) -> io::Result<()> {
    let profile =
        FrontendProfile::from_protocol(&config.settings.protocol).map_err(io::Error::other)?;
    let catalog = config.catalog.as_ref().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "schema discovery requires a catalog; attach it with GatewayConfig::with_catalog",
        )
    })?;
    let snapshot = catalog.snapshot().map_err(io::Error::other)?;
    if snapshot.source != execution.discovery().catalog_identity() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "catalog and discovery identities must match",
        ));
    }
    let server_version = match profile {
        FrontendProfile::Postgres => snapshot
            .postgres
            .as_ref()
            .map(|pg| pg.server_version.clone())
            .unwrap_or_else(|| "18.0".into()),
        FrontendProfile::Redshift => "8.0.2".into(),
    };
    drop(snapshot);
    if profile == FrontendProfile::Redshift && config.tls.is_none() {
        return Err(io::Error::other(
            "Redshift password authentication requires TLS",
        ));
    }
    let settings = Arc::new(config.settings);
    let execution = Arc::new(execution);
    let cancel_manager = Arc::new(ConnectionManager::new());
    let salt = random_nonce().into_bytes();
    let password = match profile {
        FrontendProfile::Postgres => Password::new(
            Some(salt.clone()),
            gen_salted_password(&config.password, &salt, SCRAM_ITERATIONS),
        ),
        FrontendProfile::Redshift => Password::new(None, config.password.as_bytes().to_vec()),
    };
    drop(config.password);
    let cache = config.cache;
    let context_hook = config.context_hook;
    let datasets = config.datasets;
    let (stopping, stopped) = watch::channel(false);
    let source = Arc::new(Credentials {
        tls: config.tls,
        username: config.username,
        database: config.database,
        password,
        cache: cache.clone(),
        decision_model: config.decision_model,
        context_hook: context_hook.clone(),
        datasets,
        stopped,
        server_version,
        profile,
        input_options: config.input_options,
        catalog: config
            .catalog
            .expect("catalog was validated before serving"),
        pid_generator: Arc::new(RandomPidSecretKeyGenerator::default()),
    });
    let mut sessions = JoinSet::new();
    tokio::pin!(shutdown);
    let outcome = loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => break Ok(()),
            result = sessions.join_next(), if !sessions.is_empty() => {
                if result.is_some_and(|r| r.is_err()) {
                    tracing::error!("gateway session task failed");
                }
            }
            accepted = listener.accept() => {
                let (socket, peer) = match accepted {
                    Ok(accepted) => accepted,
                    Err(error) => break Err(error),
                };
                let session_limit = settings.max_sessions;
                let cancel_only = sessions.len() >= session_limit;
                // CancelRequest uses a new connection, even when all client slots are occupied.
                if sessions.len() >= session_limit.saturating_add(MAX_CANCEL_CONNECTIONS) {
                    tracing::warn!(%peer, "gateway session limit reached");
                    continue;
                }
                let client_id = logger::next_client_id();
                let source = source.clone();
                let execution = execution.clone();
                let settings = settings.clone();
                let cancel_manager = cancel_manager.clone();
                sessions.spawn(async move {
                    tracing::info!(client_id, %peer, "client connected");
                    if let Err(error) = connection(
                        socket,
                        source,
                        execution,
                        settings,
                        cancel_manager,
                        cancel_only,
                        client_id,
                    )
                    .await
                    {
                        // Protocol errors can contain client data. Log only their category.
                        tracing::error!(client_id, %peer, kind = ?error.kind(), "client session failed");
                    }
                    tracing::info!(client_id, %peer, "client disconnected");
                });
            }
        }
    };
    drop(listener);
    tracing::info!("gateway shutting down");
    stopping.send_replace(true);
    if let Some(hook) = &context_hook {
        hook.shutdown();
    }
    execution.stop_hooks();
    if timeout(
        Duration::from_millis(u64::from(settings.shutdown_timeout_ms)),
        async { while sessions.join_next().await.is_some() {} },
    )
    .await
    .is_err()
    {
        sessions.abort_all();
        while sessions.join_next().await.is_some() {}
    }
    execution.close();
    if let Some(cache) = cache {
        cache.close().await;
    }
    outcome
}

pub(super) struct Credentials {
    pub(super) pid_generator: Arc<RandomPidSecretKeyGenerator>,
    pub(super) catalog: Arc<crate::catalog::Catalog>,
    pub(super) tls: Option<Arc<crate::tls::ServerTls>>,
    pub(super) username: String,
    pub(super) database: String,
    pub(super) password: Password,
    pub(super) cache: Option<Arc<RedisCache>>,
    pub(super) decision_model: Option<Arc<crate::decision_model::DecisionModel>>,
    pub(super) context_hook: Option<Arc<ContextHook>>,
    pub(super) datasets: Option<DatasetReader>,
    pub(super) stopped: watch::Receiver<bool>,
    pub(super) server_version: String,
    pub(super) profile: FrontendProfile,
    pub(super) input_options: Option<Arc<crate::hooks::InputOptionsResolver>>,
}

impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Credentials").finish_non_exhaustive()
    }
}

#[async_trait]
impl AuthSource for CredentialsAuth {
    async fn get_password(&self, login: &LoginInfo) -> PgWireResult<Password> {
        if login.user() != Some(self.0.username.as_str()) {
            return Err(protocol_error("FATAL", "28P01", "authentication failed"));
        }
        Ok(self.0.password.clone())
    }
}

#[derive(Debug)]
pub(super) struct CredentialsAuth(pub(super) Arc<Credentials>);
