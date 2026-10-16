//! Listener admission, shared authentication material, and bounded shutdown.

use super::{protocol_error, routing::Execution, session::connection};
use crate::{cache::RedisCache, config::GatewayConfig, hooks::ContextHook, logger};
use async_trait::async_trait;
use pgwire::{
    api::{
        ConnectionManager,
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
    if !listener.local_addr()?.ip().is_loopback() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "gateway requires a loopback listener",
        ));
    }
    let server_version = execution
        .discovery()
        .server_version()
        .await
        .map_err(|_| io::Error::other("cannot obtain PostgreSQL discovery profile"))?;
    let settings = Arc::new(config.settings);
    let execution = Arc::new(execution);
    let cancel_manager = Arc::new(ConnectionManager::new());
    let salt = random_nonce().into_bytes();
    let password = Password::new(
        Some(salt.clone()),
        gen_salted_password(&config.password, &salt, SCRAM_ITERATIONS),
    );
    drop(config.password);
    let cache = config.cache;
    let context_hook = config.context_hook;
    let (stopping, stopped) = watch::channel(false);
    let source = Arc::new(Credentials {
        username: config.username,
        database: config.database,
        password,
        cache: cache.clone(),
        context_hook: context_hook.clone(),
        stopped,
        server_version,
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
    pub(super) username: String,
    pub(super) database: String,
    pub(super) password: Password,
    pub(super) cache: Option<Arc<RedisCache>>,
    pub(super) context_hook: Option<Arc<ContextHook>>,
    pub(super) stopped: watch::Receiver<bool>,
    pub(super) server_version: String,
}

impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Credentials").finish_non_exhaustive()
    }
}

#[async_trait]
impl AuthSource for Credentials {
    async fn get_password(&self, login: &LoginInfo) -> PgWireResult<Password> {
        if login.user() != Some(self.username.as_str()) {
            return Err(protocol_error("FATAL", "28P01", "authentication failed"));
        }
        Ok(self.password.clone())
    }
}
