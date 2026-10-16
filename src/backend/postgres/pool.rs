//! Physical connection creation, idle checks, and discard-on-drop leases.

use super::connection::PgConnection;
use super::{ACQUIRE_TIMEOUT, CLEANUP_TIMEOUT, CONNECT_TIMEOUT, SESSION_DEFAULTS, error_response};
use crate::{config::BackendConfig, transport::FrameGuard};
use deadpool::{
    Runtime,
    managed::{Manager, Metrics, Object, Pool, RecycleError, RecycleResult, Timeouts},
};
use futures::StreamExt;
use pgwire::{
    api::client::{
        Config, ReadyState, ServerInformation,
        auth::{DefaultStartupHandler, StartupHandler},
    },
    error::{PgWireClientError, PgWireError, PgWireResult},
    messages::{ProtocolVersion, response::TransactionStatus},
    tokio::client::PgWireMessageClientCodec,
};
use std::{io, sync::Arc};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

pub(super) fn new(config: BackendConfig, capacity: usize) -> io::Result<Pool<PgManager>> {
    let mut wire = Config::new();
    wire.user(config.username)
        .password(config.password)
        .dbname(config.database);
    wire.protocol_version(ProtocolVersion::PROTOCOL3_0);
    let manager = PgManager {
        host: config.host,
        port: config.port,
        config: Arc::new(wire),
    };
    let pool = Pool::builder(manager)
        .max_size(capacity)
        .runtime(Runtime::Tokio1)
        .timeouts(Timeouts {
            wait: Some(ACQUIRE_TIMEOUT),
            create: Some(CONNECT_TIMEOUT),
            recycle: Some(CLEANUP_TIMEOUT),
        })
        .build()
        .map_err(|_| io::Error::other("cannot construct PostgreSQL pool"))?;
    Ok(pool)
}

pub(super) struct PgManager {
    host: String,
    port: u16,
    config: Arc<Config>,
}

impl Manager for PgManager {
    type Type = PgConnection;
    type Error = PgWireError;

    async fn create(&self) -> PgWireResult<PgConnection> {
        let socket = TcpStream::connect((self.host.as_str(), self.port)).await?;
        if !socket.peer_addr()?.ip().is_loopback() {
            return Err(error_response(
                "08006",
                "PostgreSQL requires a loopback address",
            ));
        }
        socket.set_nodelay(true)?;
        let address = socket.peer_addr()?;
        let mut connection = PgConnection {
            socket: Framed::new(
                FrameGuard::backend(socket),
                PgWireMessageClientCodec::default(),
            ),
            config: self.config.clone(),
            info: ServerInformation::default(),
            protocol: self.config.get_protocol_version(),
            address,
            status: TransactionStatus::Idle,
            clean: false,
            query_complete: false,
            cancel_requested: false,
        };
        let mut auth = DefaultStartupHandler::new();
        auth.startup(&mut connection).await.map_err(client_error)?;
        loop {
            let message = connection
                .next()
                .await
                .ok_or_else(|| error_response("08006", "PostgreSQL closed during startup"))??;
            if let ReadyState::Ready(info) = auth
                .on_message(&mut connection, message)
                .await
                .map_err(client_error)?
            {
                connection.info = info;
                break;
            }
        }
        connection.control(SESSION_DEFAULTS).await?;
        connection.clean = true;
        Ok(connection)
    }

    async fn recycle(
        &self,
        connection: &mut PgConnection,
        _: &Metrics,
    ) -> RecycleResult<PgWireError> {
        if !connection.clean || connection.status != TransactionStatus::Idle {
            return Err(RecycleError::message("unclean PostgreSQL connection"));
        }
        connection
            .control("SELECT 1")
            .await
            .map_err(RecycleError::Backend)
    }
}

fn client_error(error: PgWireClientError) -> PgWireError {
    match error {
        PgWireClientError::RemoteError(mut info) => {
            info.severity = "ERROR".into();
            PgWireError::UserError(info)
        }
        _ => error_response("08006", "PostgreSQL startup failed"),
    }
}

// Drop is synchronous: interrupted tasks remove and close their physical
// connection rather than scheduling an unbounded background cleanup worker.
pub(super) struct Lease(pub(super) Option<Object<PgManager>>);
impl Lease {
    pub(super) fn release(mut self) {
        drop(self.0.take());
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        if let Some(object) = self.0.take() {
            drop(Object::take(object));
        }
    }
}
