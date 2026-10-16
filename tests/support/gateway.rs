use super::cleanup_result;
use std::{
    net::SocketAddr,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, mpsc},
    thread,
    time::{Duration, Instant},
};
use tokio::{net::TcpListener, runtime::Builder, sync::oneshot};
use yasp::{
    backend::{PostgresBackend, PostgresBackends},
    config::GatewayConfig,
    hooks::RoutedSelector,
};

pub struct GatewayServer {
    pub address: SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    thread: Option<thread::JoinHandle<()>>,
    completed: mpsc::Receiver<bool>,
}

const STOP_TIMEOUT: Duration = Duration::from_secs(15);

impl GatewayServer {
    pub fn routed(
        gateway: GatewayConfig,
        backends: Arc<PostgresBackends>,
        selector: impl Into<RoutedSelector> + Send + 'static,
    ) -> Self {
        Self::start(move |listener, shutdown| async move {
            yasp::gateway::serve_routed(listener, gateway, backends, selector, async move {
                let _ = shutdown.await;
            })
            .await
            .expect("routed gateway stops cleanly");
        })
    }

    pub fn fixed(gateway: GatewayConfig, backend: Arc<PostgresBackend>) -> Self {
        Self::start(move |listener, shutdown| async move {
            yasp::gateway::serve(listener, gateway, backend, async move {
                let _ = shutdown.await;
            })
            .await
            .expect("fixed gateway stops cleanly");
        })
    }

    pub(crate) fn start<F, Fut>(serve: F) -> Self
    where
        F: FnOnce(TcpListener, oneshot::Receiver<()>) -> Fut + Send + 'static,
        Fut: Future<Output = ()>,
    {
        let (startup_tx, startup_rx) = mpsc::channel();
        let (completed_tx, completed) = mpsc::channel();
        let thread = thread::spawn(move || {
            let result = catch_unwind(AssertUnwindSafe(|| {
                Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("test runtime starts")
                    .block_on(async move {
                        let listener = TcpListener::bind("127.0.0.1:0")
                            .await
                            .expect("gateway listener binds");
                        let address = listener.local_addr().unwrap();
                        let (shutdown, stopped) = oneshot::channel();
                        if startup_tx.send((address, shutdown)).is_ok() {
                            serve(listener, stopped).await;
                        }
                    });
            }));
            let _ = completed_tx.send(result.is_ok());
        });
        let (address, shutdown) = startup_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("gateway starts within five seconds");
        Self {
            address,
            shutdown: Some(shutdown),
            thread: Some(thread),
            completed,
        }
    }

    pub fn stop(self) {
        self.stop_with_timeout(STOP_TIMEOUT);
    }

    pub fn stop_with_timeout(mut self, timeout: Duration) {
        self.finish(timeout);
    }

    fn finish(&mut self, timeout: Duration) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(thread) = self.thread.take() {
            let started = Instant::now();
            let success = match self.completed.recv_timeout(timeout) {
                Ok(success) => success,
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    cleanup_result(false, "gateway thread shutdown timed out");
                    return;
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    cleanup_result(false, "gateway thread completion was lost");
                    return;
                }
            };
            while !thread.is_finished() {
                if started.elapsed() >= timeout {
                    cleanup_result(false, "gateway thread did not exit after completion");
                    return;
                }
                thread::sleep(Duration::from_millis(1));
            }
            cleanup_result(thread.join().is_ok() && success, "join gateway thread");
        }
    }
}

impl Drop for GatewayServer {
    fn drop(&mut self) {
        self.finish(STOP_TIMEOUT);
    }
}
