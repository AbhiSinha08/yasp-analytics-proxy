//! Deterministic HTTP decision service. No model packages or weights are used.
#![allow(dead_code)]
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    io::{Read, Write},
    net::TcpListener,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};
use yasp::decision_model::{DecisionModel, DecisionModelSettings};

pub struct MockModel {
    pub endpoint: String,
    pub requests: Arc<Mutex<Vec<Value>>>,
    pub response: Arc<Mutex<Option<String>>>,
    pub response_by_ordinal: Arc<Mutex<BTreeMap<usize, String>>>,
    pub headers: Arc<Mutex<Vec<String>>>,
    pub delay_ms: Arc<AtomicU64>,
    pub peak_active: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<()>>,
}
impl MockModel {
    pub fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = format!("http://{}/v1/systemone", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let response = Arc::new(Mutex::new(None::<String>));
        let response_by_ordinal = Arc::new(Mutex::new(BTreeMap::<usize, String>::new()));
        let headers = Arc::new(Mutex::new(Vec::new()));
        let (per_column, captured_headers) = (response_by_ordinal.clone(), headers.clone());
        let delay_ms = Arc::new(AtomicU64::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let peak_active = Arc::new(AtomicUsize::new(0));
        let peak = peak_active.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let (r, body, delay, stopping) = (
            requests.clone(),
            response.clone(),
            delay_ms.clone(),
            stop.clone(),
        );
        let worker = thread::spawn(move || {
            let mut clients = Vec::new();
            while !stopping.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((mut socket, _)) => {
                        let (requests, body, delay) = (r.clone(), body.clone(), delay.clone());
                        let (active, peak) = (active.clone(), peak.clone());
                        let (per_column, captured_headers) =
                            (per_column.clone(), captured_headers.clone());
                        clients.push(thread::spawn(move || {
                            socket.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
                            let mut bytes = Vec::new();
                            let mut chunk = [0; 4096];
                            let (offset, length) = loop {
                                let Ok(n) = socket.read(&mut chunk) else { return; };
                                if n == 0 { return; }
                                bytes.extend_from_slice(&chunk[..n]);
                                if let Some(end) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
                                    let headers = String::from_utf8_lossy(&bytes[..end]);
                                    let length = headers.lines().find_map(|line| line.to_ascii_lowercase().strip_prefix("content-length: ").and_then(|n| n.parse::<usize>().ok())).unwrap();
                                    if bytes.len() >= end + 4 + length { break (end + 4, length); }
                                }
                            };
                            captured_headers.lock().unwrap().push(String::from_utf8_lossy(&bytes[..offset]).into_owned());
                            let request: Value = serde_json::from_slice(&bytes[offset..offset + length]).unwrap();
                            requests.lock().unwrap().push(request.clone());
                            let count = active.fetch_add(1, Ordering::SeqCst) + 1;
                            peak.fetch_max(count, Ordering::SeqCst);
                            thread::sleep(Duration::from_millis(delay.load(Ordering::SeqCst)));
                            let specific = request["state"]["column"]["ordinal"].as_u64().and_then(|ordinal| per_column.lock().unwrap().get(&(ordinal as usize)).cloned());
                            let response = specific.or_else(|| body.lock().unwrap().clone()).unwrap_or_else(|| {
                                let answers: BTreeMap<_,_> = request["questions"].as_object().unwrap().keys().map(|key| (key, json!({"type":"noul", "noul":0.75, "confidence":0.8}))).collect();
                                json!({"answers":answers,"usage":{"truncated":false}}).to_string()
                            });
                            active.fetch_sub(1, Ordering::SeqCst);
                            let _ = write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", response.len(), response);
                        }));
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2))
                    }
                    Err(e) => panic!("mock accept: {e}"),
                }
            }
            for client in clients {
                client.join().unwrap();
            }
        });
        Self {
            endpoint,
            requests,
            response,
            response_by_ordinal,
            headers,
            delay_ms,
            peak_active,
            stop,
            worker: Some(worker),
        }
    }
    pub fn settings(&self, fail_on_error: bool) -> DecisionModelSettings {
        DecisionModelSettings {
            endpoint: self.endpoint.clone(),
            namespace: "yasp:decision_cache".into(),
            sample_size: 5,
            timeout_ms: 2000,
            fail_on_error,
            headers_env: BTreeMap::new(),
            questions: BTreeMap::from([(
                "email".into(),
                "Do these values contain email addresses?".into(),
            )]),
        }
    }
    pub fn model(&self, fail_on_error: bool) -> Arc<DecisionModel> {
        Arc::new(DecisionModel::new(self.settings(fail_on_error), BTreeMap::new()).unwrap())
    }
    pub fn count(&self) -> usize {
        self.requests.lock().unwrap().len()
    }
    pub fn clear(&self) {
        self.requests.lock().unwrap().clear();
    }
}
impl Drop for MockModel {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        self.worker.take().unwrap().join().unwrap();
    }
}
