//! Physical connection creation, idle checks, and discard-on-drop leases.

use super::connection::{PgConnection, connect_transport};
use super::{CLEANUP_TIMEOUT, CONNECT_TIMEOUT, SESSION_DEFAULTS, error_response};
use crate::{config::BackendConfig, transport::FrameGuard};
use deadpool::{
    Runtime,
    managed::{Manager, Metrics, Object, Pool, RecycleError, RecycleResult, Timeouts, WeakPool},
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
use std::{
    collections::HashMap,
    io,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::net::TcpStream;
use tokio::sync::{Notify, Semaphore};
use tokio_util::codec::Framed;

pub(super) struct InstanceState {
    pub(super) id: String,
    pub(super) max_connections: usize,
    pub(super) max_in_flight: usize,
    pub(super) in_flight: Arc<Semaphore>,
}

impl InstanceState {
    pub(super) fn new(id: String, max_connections: usize, max_in_flight: usize) -> Self {
        Self {
            id,
            max_connections,
            max_in_flight,
            in_flight: Arc::new(Semaphore::new(max_in_flight)),
        }
    }
}

struct BudgetState {
    total: usize,
    instances: HashMap<String, usize>,
}

pub(super) struct ConnectionBudget {
    max_total: usize,
    state: Mutex<BudgetState>,
    pools: Mutex<Vec<(String, WeakPool<PgManager>)>>,
    pub(super) changed: Notify,
}

impl ConnectionBudget {
    pub(super) fn new(max_total: usize) -> Arc<Self> {
        Arc::new(Self {
            max_total,
            state: Mutex::new(BudgetState {
                total: 0,
                instances: HashMap::new(),
            }),
            pools: Mutex::new(Vec::new()),
            changed: Notify::new(),
        })
    }

    fn register(&self, instance_id: &str, pool: &Pool<PgManager>) {
        self.pools
            .lock()
            .unwrap()
            .push((instance_id.to_owned(), pool.weak()));
    }

    pub(super) fn may_open(&self, instance: &InstanceState) -> bool {
        if self.has_room(instance) {
            return true;
        }
        self.reclaim(&instance.id, instance.max_connections);
        self.has_room(instance)
    }

    fn has_room(&self, instance: &InstanceState) -> bool {
        let state = self.state.lock().unwrap();
        state.total < self.max_total
            && state.instances.get(&instance.id).copied().unwrap_or(0) < instance.max_connections
    }

    fn reserve(self: &Arc<Self>, instance: &InstanceState) -> PgWireResult<PhysicalPermit> {
        self.reclaim(&instance.id, instance.max_connections);
        let mut state = self.state.lock().unwrap();
        let instance_count = state.instances.get(&instance.id).copied().unwrap_or(0);
        if state.total >= self.max_total || instance_count >= instance.max_connections {
            return Err(error_response(
                "53300",
                "PostgreSQL connection capacity is full",
            ));
        }
        state.total += 1;
        *state.instances.entry(instance.id.clone()).or_default() += 1;
        Ok(PhysicalPermit {
            budget: self.clone(),
            instance_id: instance.id.clone(),
        })
    }

    fn reclaim(&self, instance_id: &str, instance_cap: usize) {
        let (total_full, instance_full) = {
            let state = self.state.lock().unwrap();
            (
                state.total >= self.max_total,
                state.instances.get(instance_id).copied().unwrap_or(0) >= instance_cap,
            )
        };
        if !total_full && !instance_full {
            return;
        }
        let mut pools = self.pools.lock().unwrap();
        let mut live = Vec::new();
        pools.retain(|(id, weak)| {
            if let Some(pool) = weak.upgrade() {
                if !instance_full || id == instance_id {
                    live.push(pool);
                }
                true
            } else {
                false
            }
        });
        // Remove one idle session per blocked creation. This gives active
        // sessions priority while avoiding needless churn in unrelated pools.
        for pool in live {
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
            if removed_one {
                break;
            }
        }
    }
}

pub(super) struct PhysicalPermit {
    budget: Arc<ConnectionBudget>,
    instance_id: String,
}

impl PhysicalPermit {
    pub(super) fn budget(&self) -> Arc<ConnectionBudget> {
        self.budget.clone()
    }
}

impl Drop for PhysicalPermit {
    fn drop(&mut self) {
        let mut state = self.budget.state.lock().unwrap();
        state.total -= 1;
        let count = state.instances.get_mut(&self.instance_id).unwrap();
        *count -= 1;
        if *count == 0 {
            state.instances.remove(&self.instance_id);
        }
        drop(state);
        self.budget.changed.notify_waiters();
    }
}

pub(super) fn new(
    config: BackendConfig,
    capacity: usize,
    budget: Arc<ConnectionBudget>,
    instance: Arc<InstanceState>,
    acquire_timeout: Duration,
) -> io::Result<Pool<PgManager>> {
    let mut wire = Config::new();
    wire.user(config.username)
        .password(config.password)
        .dbname(config.database);
    wire.protocol_version(ProtocolVersion::PROTOCOL3_0);
    let manager = PgManager {
        tls: config.tls,
        host: config.host,
        port: config.port,
        config: Arc::new(wire),
        budget: budget.clone(),
        instance,
    };
    let pool = Pool::builder(manager)
        .max_size(capacity)
        .runtime(Runtime::Tokio1)
        .timeouts(Timeouts {
            wait: Some(acquire_timeout),
            create: Some(CONNECT_TIMEOUT),
            recycle: Some(CLEANUP_TIMEOUT),
        })
        .build()
        .map_err(|_| io::Error::other("cannot construct PostgreSQL pool"))?;
    budget.register(&pool.manager().instance.id, &pool);
    Ok(pool)
}

pub(super) struct PgManager {
    tls: Option<Arc<crate::tls::ClientTls>>,
    host: String,
    port: u16,
    config: Arc<Config>,
    budget: Arc<ConnectionBudget>,
    instance: Arc<InstanceState>,
}

impl Manager for PgManager {
    type Type = PgConnection;
    type Error = PgWireError;

    async fn create(&self) -> PgWireResult<PgConnection> {
        let physical = self.budget.reserve(&self.instance)?;
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
