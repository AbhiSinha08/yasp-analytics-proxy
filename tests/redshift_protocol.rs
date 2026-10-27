//! Native wire/session contracts independent of the local emulator's SQL coverage.
use futures::{Sink, task::AtomicWaker};
use pgwire::{error::PgWireError, messages::PgWireBackendMessage};
use std::{
    io,
    net::SocketAddr,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Notify,
};
use yasp::{
    backend::{ConnectionBudget, PostgresBackend},
    config::{BackendConfig, PgWireEngine},
    query::{ParsedQuery, SqlDialect},
};

const DEFAULTS: &str = "SET client_encoding = 'UTF8'; SET DateStyle = 'ISO, MDY'; SET TimeZone = 'UTC'; SET extra_float_digits = 2";

#[derive(Default)]
struct State {
    queries: Mutex<Vec<String>>,
    startups: AtomicUsize,
    cancelled: AtomicBool,
    cancel: Notify,
    finish_cancel: Notify,
    reject_reset: AtomicBool,
}

struct Fixture {
    address: SocketAddr,
    state: Arc<State>,
    task: tokio::task::JoinHandle<()>,
}

impl Fixture {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let state = Arc::new(State::default());
        let shared = state.clone();
        let task = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (socket, _) = accepted.unwrap();
                        let state = shared.clone();
                        connections.spawn(async move { serve(socket, state).await });
                    }
                    completed = connections.join_next(), if !connections.is_empty() => {
                        if let Err(error) = completed.unwrap().unwrap() {
                            assert!(matches!(error.kind(), io::ErrorKind::UnexpectedEof | io::ErrorKind::ConnectionReset | io::ErrorKind::BrokenPipe), "source fixture: {error}");
                        }
                    }
                }
            }
        });
        Self {
            address,
            state,
            task,
        }
    }

    fn backend(&self, budget: Arc<ConnectionBudget>) -> Arc<PostgresBackend> {
        Arc::new(
            PostgresBackend::with_budget(
                BackendConfig::new(
                    self.address.ip().to_string(),
                    self.address.port(),
                    "reader".into(),
                    "test".into(),
                    "analytics".into(),
                )
                .unwrap()
                .with_engine(PgWireEngine::Redshift),
                budget,
            )
            .unwrap(),
        )
    }

    async fn observed(&self, sql: &str) {
        until(|| self.state.queries.lock().unwrap().iter().any(|q| q == sql)).await;
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn until(ready: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !ready() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("protocol fixture made progress");
}

async fn frame(socket: &mut TcpStream, tag: u8, body: &[u8]) -> io::Result<()> {
    socket.write_all(&[tag]).await?;
    socket
        .write_all(&((body.len() + 4) as u32).to_be_bytes())
        .await?;
    socket.write_all(body).await
}

async fn complete(socket: &mut TcpStream, tag: &str, status: u8) -> io::Result<()> {
    frame(socket, b'C', format!("{tag}\0").as_bytes()).await?;
    frame(socket, b'Z', &[status]).await
}

async fn error(socket: &mut TcpStream, code: &str, status: u8) -> io::Result<()> {
    frame(
        socket,
        b'E',
        format!("SERROR\0C{code}\0Msource failure\0\0").as_bytes(),
    )
    .await?;
    frame(socket, b'Z', &[status]).await
}

async fn columns(socket: &mut TcpStream, oid: i32) -> io::Result<()> {
    let mut body = Vec::new();
    body.extend_from_slice(&1i16.to_be_bytes());
    body.extend_from_slice(b"value\0");
    body.extend_from_slice(&0i32.to_be_bytes());
    body.extend_from_slice(&0i16.to_be_bytes());
    body.extend_from_slice(&oid.to_be_bytes());
    body.extend_from_slice(&(-1i16).to_be_bytes());
    body.extend_from_slice(&(-1i32).to_be_bytes());
    body.extend_from_slice(&0i16.to_be_bytes());
    frame(socket, b'T', &body).await
}

async fn row(socket: &mut TcpStream, value: &[u8]) -> io::Result<()> {
    let mut body = Vec::new();
    body.extend_from_slice(&1i16.to_be_bytes());
    body.extend_from_slice(&(value.len() as i32).to_be_bytes());
    body.extend_from_slice(value);
    frame(socket, b'D', &body).await
}

async fn serve(mut socket: TcpStream, state: Arc<State>) -> io::Result<()> {
    socket.set_nodelay(true)?;
    let length = socket.read_u32().await? as usize;
    assert!((8..8192).contains(&length));
    let mut startup = vec![0; length - 4];
    socket.read_exact(&mut startup).await?;
    if startup.starts_with(&80877102u32.to_be_bytes()) {
        assert_eq!(startup[4..8], 1234i32.to_be_bytes());
        assert_eq!(startup[8..12], 5678i32.to_be_bytes());
        state.cancelled.store(true, Ordering::SeqCst);
        state.cancel.notify_one();
        return Ok(());
    }
    assert!(startup.starts_with(&196608u32.to_be_bytes()));
    assert!(
        startup
            .windows(b"user\0reader\0".len())
            .any(|part| part == b"user\0reader\0")
    );
    assert!(
        startup
            .windows(b"database\0analytics\0".len())
            .any(|part| part == b"database\0analytics\0")
    );
    state.startups.fetch_add(1, Ordering::SeqCst);
    let mut challenge = 5i32.to_be_bytes().to_vec();
    challenge.extend_from_slice(&[1, 2, 3, 4]);
    frame(&mut socket, b'R', &challenge).await?;
    assert_eq!(socket.read_u8().await?, b'p');
    let length = socket.read_u32().await? as usize;
    let mut password = vec![0; length - 4];
    socket.read_exact(&mut password).await?;
    assert_eq!(password, b"md5219a62c741476d472f81c5f243c4d229\0");
    frame(&mut socket, b'R', &0i32.to_be_bytes()).await?;
    for (name, value) in [
        ("server_version", "8.0.2"),
        ("client_encoding", "UTF8"),
        ("DateStyle", "ISO, MDY"),
        ("TimeZone", "UTC"),
        ("standard_conforming_strings", "on"),
    ] {
        frame(&mut socket, b'S', format!("{name}\0{value}\0").as_bytes()).await?;
    }
    let mut key = 1234i32.to_be_bytes().to_vec();
    key.extend_from_slice(&5678i32.to_be_bytes());
    frame(&mut socket, b'K', &key).await?;
    frame(&mut socket, b'Z', b"I").await?;
    let mut status = b'I';
    loop {
        let tag = match socket.read_u8().await {
            Ok(tag) => tag,
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(error) => return Err(error),
        };
        let length = socket.read_u32().await? as usize;
        let mut body = vec![0; length - 4];
        socket.read_exact(&mut body).await?;
        if tag == b'X' {
            return Ok(());
        }
        assert_eq!(tag, b'Q');
        let sql = std::str::from_utf8(&body[..body.len() - 1]).unwrap();
        state.queries.lock().unwrap().push(sql.to_owned());
        match sql {
            "SELECT source_error" => {
                status = b'E';
                error(&mut socket, "42601", status).await?;
            }
            "SELECT cancel_me" => {
                state.cancel.notified().await;
                state.finish_cancel.notified().await;
                status = b'E';
                error(&mut socket, "57014", status).await?;
            }
            "SELECT stall" => {
                let mut byte = [0];
                assert_eq!(
                    socket.read(&mut byte).await?,
                    0,
                    "timed-out source is disconnected"
                );
                return Ok(());
            }
            "SELECT native_super" => {
                columns(&mut socket, 4000).await?;
                row(
                    &mut socket,
                    br#"{"n":12345678901234567890.123456789,"v":[null,true]}"#,
                )
                .await?;
                complete(&mut socket, "SELECT 1", status).await?;
            }
            "SELECT stream" => {
                columns(&mut socket, 23).await?;
                for i in 0..100 {
                    row(&mut socket, i.to_string().as_bytes()).await?;
                }
                complete(&mut socket, "SELECT 100", status).await?;
            }
            "SELECT 1" => {
                columns(&mut socket, 23).await?;
                row(&mut socket, b"1").await?;
                complete(&mut socket, "SELECT 1", status).await?;
            }
            "ROLLBACK" | "COMMIT" => {
                status = b'I';
                complete(&mut socket, sql, status).await?;
            }
            "RESET ALL" if state.reject_reset.swap(false, Ordering::SeqCst) => {
                error(&mut socket, "XX000", status).await?
            }
            "RESET ALL" => complete(&mut socket, "RESET", status).await?,
            sql if sql.starts_with("BEGIN READ ONLY;") => {
                status = b'T';
                complete(&mut socket, "SET", status).await?;
            }
            sql if sql == DEFAULTS || sql.starts_with("SELECT set_config(") => {
                complete(&mut socket, "SET", status).await?
            }
            other => panic!("unexpected source command: {other}"),
        }
    }
}

#[derive(Default)]
struct Gate {
    open: AtomicBool,
    blocked: AtomicBool,
    waker: AtomicWaker,
}
impl Gate {
    fn release(&self) {
        self.open.store(true, Ordering::SeqCst);
        self.waker.wake();
    }
}

#[derive(Default)]
struct Output {
    messages: Arc<Mutex<Vec<PgWireBackendMessage>>>,
    gate: Option<Arc<Gate>>,
    row_sent: bool,
}
impl Sink<PgWireBackendMessage> for Output {
    type Error = PgWireError;
    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn start_send(
        mut self: Pin<&mut Self>,
        message: PgWireBackendMessage,
    ) -> Result<(), Self::Error> {
        self.row_sent |= matches!(message, PgWireBackendMessage::DataRow(_));
        self.messages.lock().unwrap().push(message);
        Ok(())
    }
    fn poll_flush(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        if let Some(gate) = &self.gate {
            gate.waker.register(context.waker());
            if self.row_sent && !gate.open.load(Ordering::SeqCst) {
                gate.blocked.store(true, Ordering::SeqCst);
                return Poll::Pending;
            }
        }
        Poll::Ready(Ok(()))
    }
    fn poll_close(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        self.poll_flush(context)
    }
}

fn query(sql: &str) -> ParsedQuery {
    ParsedQuery::parse_with_dialect(sql, 4096, SqlDialect::Redshift).unwrap()
}
fn code(error: PgWireError) -> String {
    match error {
        PgWireError::UserError(info) => info.code.clone(),
        other => panic!("expected source SQLSTATE, got {other:?}"),
    }
}

#[tokio::test]
async fn native_super_and_source_errors_preserve_session_cleanup_and_recovery() {
    let fixture = Fixture::start().await;
    let backend = fixture.backend(ConnectionBudget::new(1).unwrap());
    let mut output = Output::default();
    backend
        .execute(
            &query("SELECT native_super"),
            &mut output,
            Duration::from_secs(2),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
    {
        let messages = output.messages.lock().unwrap();
        assert!(
            matches!(&messages[0], PgWireBackendMessage::RowDescription(row) if row.fields[0].type_id == 4000)
        );
        assert!(
            matches!(&messages[1], PgWireBackendMessage::DataRow(row) if &row.data[4..] == br#"{"n":12345678901234567890.123456789,"v":[null,true]}"#)
        );
        assert!(matches!(
            messages.last(),
            Some(PgWireBackendMessage::CommandComplete(_))
        ));
    }
    let error = backend
        .execute(
            &query("SELECT source_error"),
            &mut Output::default(),
            Duration::from_secs(2),
            Duration::from_secs(1),
        )
        .await
        .unwrap_err();
    assert_eq!(code(error), "42601");
    backend
        .execute(
            &query("SELECT 1"),
            &mut Output::default(),
            Duration::from_secs(2),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert_eq!(fixture.state.startups.load(Ordering::SeqCst), 1);
    let queries = fixture.state.queries.lock().unwrap();
    assert_eq!(queries[0], DEFAULTS);
    assert_eq!(
        queries[1],
        "BEGIN READ ONLY; SET LOCAL statement_timeout = 2000"
    );
    assert_eq!(
        &queries[3..7],
        &["ROLLBACK", "RESET ALL", DEFAULTS, "SELECT 1"]
    );
    assert_eq!(
        queries.iter().filter(|q| q.as_str() == "RESET ALL").count(),
        3
    );
    assert!(
        !queries
            .iter()
            .any(|q| q.contains("DISCARD") || q.contains("idle_in_transaction_session_timeout"))
    );
}

#[tokio::test]
async fn cancellation_keeps_capacity_until_the_source_finishes() {
    let fixture = Fixture::start().await;
    let budget = ConnectionBudget::new(1).unwrap();
    let backend = fixture.backend(budget.clone());
    let sibling = fixture.backend(budget);
    let (cancel, cancelled) = futures::channel::oneshot::channel();
    let active = tokio::spawn(async move {
        let route = backend.route();
        route
            .acquire(Duration::from_secs(1))
            .await
            .unwrap()
            .execute(
                &query("SELECT cancel_me"),
                &mut Output::default(),
                Duration::from_secs(2),
                Duration::from_secs(1),
                Some(cancelled),
            )
            .await
    });
    fixture.observed("SELECT cancel_me").await;
    cancel.send(()).unwrap();
    until(|| fixture.state.cancelled.load(Ordering::SeqCst)).await;
    assert!(!active.is_finished());
    assert!(
        sibling
            .route()
            .acquire(Duration::from_millis(30))
            .await
            .is_err()
    );
    fixture.state.finish_cancel.notify_one();
    assert_eq!(code(active.await.unwrap().unwrap_err()), "57014");
    sibling
        .execute(
            &query("SELECT 1"),
            &mut Output::default(),
            Duration::from_secs(2),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn deadlines_and_failed_cleanup_discard_the_physical_connection() {
    let fixture = Fixture::start().await;
    let backend = fixture.backend(ConnectionBudget::new(1).unwrap());
    let mut output = Output::default();
    assert_eq!(
        code(
            backend
                .execute(
                    &query("SELECT stall"),
                    &mut output,
                    Duration::from_millis(30),
                    Duration::from_secs(1)
                )
                .await
                .unwrap_err()
        ),
        "57014"
    );
    assert!(output.messages.lock().unwrap().is_empty());
    backend
        .execute(
            &query("SELECT 1"),
            &mut output,
            Duration::from_secs(2),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert_eq!(fixture.state.startups.load(Ordering::SeqCst), 2);
    fixture.state.reject_reset.store(true, Ordering::SeqCst);
    let mut failed = Output::default();
    assert_eq!(
        code(
            backend
                .execute(
                    &query("SELECT 1"),
                    &mut failed,
                    Duration::from_secs(2),
                    Duration::from_secs(1)
                )
                .await
                .unwrap_err()
        ),
        "XX000"
    );
    assert!(
        !failed
            .messages
            .lock()
            .unwrap()
            .iter()
            .any(|m| matches!(m, PgWireBackendMessage::CommandComplete(_)))
    );
    backend
        .execute(
            &query("SELECT 1"),
            &mut Output::default(),
            Duration::from_secs(2),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert_eq!(fixture.state.startups.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn stream_backpressure_holds_admission_and_write_timeout_discards_the_source() {
    let fixture = Fixture::start().await;
    let budget = ConnectionBudget::new(1).unwrap();
    let backend = fixture.backend(budget.clone());
    let sibling = fixture.backend(budget);
    let gate = Arc::new(Gate::default());
    let mut output = Output {
        gate: Some(gate.clone()),
        ..Output::default()
    };
    let messages = output.messages.clone();
    let selected = backend.clone();
    let active = tokio::spawn(async move {
        selected
            .execute(
                &query("SELECT stream"),
                &mut output,
                Duration::from_secs(2),
                Duration::from_secs(2),
            )
            .await
    });
    until(|| gate.blocked.load(Ordering::SeqCst)).await;
    assert_eq!(
        messages
            .lock()
            .unwrap()
            .iter()
            .filter(|m| matches!(m, PgWireBackendMessage::DataRow(_)))
            .count(),
        1
    );
    assert!(
        sibling
            .route()
            .acquire(Duration::from_millis(30))
            .await
            .is_err()
    );
    gate.release();
    active.await.unwrap().unwrap();
    assert_eq!(
        messages
            .lock()
            .unwrap()
            .iter()
            .filter(|m| matches!(m, PgWireBackendMessage::DataRow(_)))
            .count(),
        100
    );

    let mut blocked = Output {
        gate: Some(Arc::new(Gate::default())),
        ..Output::default()
    };
    assert!(
        backend
            .execute(
                &query("SELECT stream"),
                &mut blocked,
                Duration::from_secs(2),
                Duration::from_millis(20)
            )
            .await
            .is_err()
    );
    sibling
        .execute(
            &query("SELECT 1"),
            &mut Output::default(),
            Duration::from_secs(2),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn writes_are_rejected_before_source_admission() {
    let fixture = Fixture::start().await;
    let backend = fixture.backend(ConnectionBudget::new(1).unwrap());
    for sql in [
        "DELETE FROM orders",
        "WITH changed AS (DELETE FROM orders RETURNING *) SELECT * FROM changed",
        "SELECT * INTO copied FROM orders",
    ] {
        assert_eq!(
            code(
                backend
                    .execute(
                        &query(sql),
                        &mut Output::default(),
                        Duration::from_secs(2),
                        Duration::from_secs(1)
                    )
                    .await
                    .unwrap_err()
            ),
            "0A000"
        );
    }
    assert_eq!(fixture.state.startups.load(Ordering::SeqCst), 0);
}
