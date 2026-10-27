//! Physical connection creation, idle checks, and discard-on-drop leases.

use super::connection::{PgConnection, connect_transport};
use super::{CLEANUP_TIMEOUT, CONNECT_TIMEOUT, error_response};
use crate::{
    backend::{
        ConnectionBudget,
        budget::{CapacityError, CapacityGroup},
    },
    config::{BackendConfig, PgWireEngine},
    transport::FrameGuard,
};
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
use tokio::sync::Semaphore;
use tokio_util::codec::Framed;

pub(super) struct InstanceState {
    pub(super) max_connections: usize,
    pub(super) max_in_flight: usize,
    pub(super) in_flight: Arc<Semaphore>,
    pub(super) capacity: CapacityGroup,
}
impl InstanceState {
    pub(super) fn new(
        budget: &Arc<ConnectionBudget>,
        max_connections: usize,
        max_in_flight: usize,
    ) -> io::Result<Self> {
        Ok(Self {
            max_connections,
            max_in_flight,
            in_flight: Arc::new(Semaphore::new(max_in_flight)),
            capacity: budget.group(max_connections)?,
        })
    }
}

pub(super) fn new(
    config: BackendConfig,
    capacity: usize,
    instance: Arc<InstanceState>,
) -> io::Result<Pool<PgManager>> {
    let mut wire = Config::new();
    wire.user(config.username)
        .password(config.password)
        .dbname(config.database);
    wire.protocol_version(ProtocolVersion::PROTOCOL3_0);
    let manager = PgManager {
        engine: config.engine,
        tls: config.tls,
        host: config.host,
        port: config.port,
        config: Arc::new(wire),
        instance,
    };
    let pool = Pool::builder(manager)
        .max_size(capacity)
        .runtime(Runtime::Tokio1)
        .timeouts(Timeouts {
            wait: None,
            create: Some(CONNECT_TIMEOUT),
            recycle: Some(CLEANUP_TIMEOUT),
        })
        .build()
        .map_err(|_| io::Error::other("cannot construct PostgreSQL pool"))?;
    let weak = pool.weak();
    pool.manager()
        .instance
        .capacity
        .register_reclaimer(move || {
            let Some(pool) = weak.upgrade() else {
                return false;
            };
            let mut removed_one = false;
            drop(
                pool.retain(|_, _| {
                    if removed_one {
                        true
                    } else {
                        removed_one = true;
                        false
                    }
                })
                .removed,
            );
            removed_one
        });
    Ok(pool)
}

pub(super) struct PgManager {
    engine: PgWireEngine,
    tls: Option<Arc<crate::tls::ClientTls>>,
    host: String,
    port: u16,
    config: Arc<Config>,
    instance: Arc<InstanceState>,
}

impl Manager for PgManager {
    type Type = PgConnection;
    type Error = PgWireError;

    async fn create(&self) -> PgWireResult<PgConnection> {
        let physical = self
            .instance
            .capacity
            .try_acquire()
            .map_err(|error| match error {
                CapacityError::Closed => error_response("08006", "PostgreSQL binding is closed"),
                _ => error_response("53300", "PostgreSQL connection capacity is full"),
            })?;
        let socket = TcpStream::connect((self.host.as_str(), self.port)).await?;
        socket.set_nodelay(true)?;
        let address = socket.peer_addr()?;
        let socket = connect_transport(socket, &self.host, self.tls.as_deref()).await?;
        let mut connection = PgConnection {
            tls: self.tls.clone(),
            host: self.host.clone(),
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
            _physical: physical,
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
        connection.control(self.engine.session_defaults()).await?;
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
        if let Some(object) = self.0.take() {
            let budget = object._physical.budget();
            drop(object);
            budget.changed.notify_waiters();
        }
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        if let Some(object) = self.0.take() {
            drop(Object::take(object));
        }
    }
}
