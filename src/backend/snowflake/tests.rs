use super::*;
use crate::backend::result::ResultEvent;
use futures::{Sink, channel::oneshot};
use serde_json::json;
use std::{
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn config(address: std::net::SocketAddr) -> SnowflakeConfig {
    SnowflakeConfig {
        account: "org-account".into(),
        username: "reader".into(),
        private_key_file: concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/snowflake-test-key.p8"
        )
        .into(),
        private_key_passphrase: None,
        database: "DEMO".into(),
        schema: "PUBLIC".into(),
        role: "ANALYST".into(),
        warehouse: "COMPUTE".into(),
        emulator: Some(address),
        max_connections: 1,
        acquire_timeout: Duration::from_millis(30),
        max_response_bytes: 4096,
    }
}
#[derive(Default)]
struct Output {
    rows: Vec<Vec<Option<bytes::Bytes>>>,
}
impl Sink<ResultEvent> for Output {
    type Error = ExecutionError;
    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<ExecutionResult<()>> {
        Poll::Ready(Ok(()))
    }
    fn start_send(mut self: Pin<&mut Self>, event: ResultEvent) -> ExecutionResult<()> {
        if let ResultEvent::Row(row) = event {
            self.rows.push(row);
        }
        Ok(())
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<ExecutionResult<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<ExecutionResult<()>> {
        Poll::Ready(Ok(()))
    }
}
async fn fixture(
    responses: Vec<String>,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<Vec<String>>) {
    fixture_bytes(responses.into_iter().map(String::into_bytes).collect()).await
}
async fn fixture_bytes(
    responses: Vec<Vec<u8>>,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<Vec<String>>) {
    fixture_responses(responses.into_iter().map(|body| (200, body)).collect()).await
}
async fn fixture_responses(
    responses: Vec<(u16, Vec<u8>)>,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<Vec<String>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let mut requests = Vec::new();
        for (status, response) in responses {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut data = Vec::new();
            loop {
                let mut buffer = [0; 4096];
                let count = socket.read(&mut buffer).await.unwrap();
                assert_ne!(count, 0);
                data.extend_from_slice(&buffer[..count]);
                let text = String::from_utf8_lossy(&data);
                if let Some(end) = text.find("\r\n\r\n") {
                    let length = text[..end]
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length: ")
                                .and_then(|s| s.parse::<usize>().ok())
                        })
                        .unwrap_or(0);
                    if data.len() >= end + 4 + length {
                        break;
                    }
                }
            }
            requests.push(String::from_utf8(data).unwrap());
            let encoding = if response.starts_with(&[0x1f, 0x8b]) {
                "Content-Encoding: gzip\r\n"
            } else {
                ""
            };
            socket.write_all(format!("HTTP/1.1 {status} Response\r\nContent-Type: application/json\r\nConnection: close\r\n{encoding}Content-Length: {}\r\n\r\n", response.len()).as_bytes()).await.unwrap();
            socket.write_all(&response).await.unwrap();
        }
        requests
    });
    (address, task)
}
fn result(data: serde_json::Value, fields: serde_json::Value, rows: u64) -> serde_json::Value {
    json!({"statementHandle":"abc-123","code":"090001","resultSetMetaData":{"format":"jsonv2","numRows":rows,"rowType":fields},"data":data})
}
#[tokio::test]
async fn public_api_polls_partitions_and_signs_fixed_identity_without_losing_values() {
    let fields = json!([{"name":"D","type":"fixed","precision":38,"scale":9},{"name":"B","type":"binary"},{"name":"T","type":"timestamp_ntz"},{"name":"Z","type":"text"}]);
    let mut first = result(
        json!([[
            "12345678901234567890.123456789",
            "00ff",
            "1969-12-31 23:59:59.999999000",
            null
        ]]),
        fields,
        2,
    );
    first["resultSetMetaData"]["partitionInfo"] =
        json!([{"rowCount":1,"uncompressedSize":100},{"rowCount":1,"uncompressedSize":100}]);
    let pending = json!({"code":"333334","statementHandle":"abc-123","statementStatusUrl":"https://untrusted.invalid/steal"});
    let (address, task) = fixture(vec![
        pending.to_string(),
        first.to_string(),
        json!({"data":[["0.000000001","","2026-01-01 00:00:00.000000000",null]]}).to_string(),
    ])
    .await;
    let backend = SnowflakeBackend::new(config(address)).unwrap();
    let settings = GatewaySettings::default();
    let mut session = backend.open(&settings, false).await.unwrap();
    let mut output = Output::default();
    let (_sender, cancel) = oneshot::channel();
    assert!(matches!(
        session
            .execute_sql(
                "SELECT test",
                &mut output,
                &settings,
                &SessionSettings::default(),
                cancel
            )
            .await
            .unwrap(),
        ResultEvent::Complete(2)
    ));
    assert_eq!(
        output.rows[0][0].as_deref(),
        Some(&b"12345678901234567890.123456789"[..])
    );
    assert_eq!(output.rows[0][1].as_deref(), Some(&b"\\x00ff"[..]));
    assert_eq!(
        output.rows[0][2].as_deref(),
        Some(&b"1969-12-31 23:59:59.999999"[..])
    );
    assert!(output.rows[0][3].is_none());
    let requests = task.await.unwrap();
    assert!(requests[0].starts_with("POST /api/v2/statements?async=true&requestId="));
    assert!(requests[1].starts_with("GET /api/v2/statements/abc-123 "));
    assert!(requests[2].starts_with("GET /api/v2/statements/abc-123?partition=1 "));
    use rsa::pkcs8::{DecodePrivateKey, EncodePublicKey};
    let key = rsa::RsaPrivateKey::from_pkcs8_pem(include_str!(
        "../../../tests/fixtures/snowflake-test-key.p8"
    ))
    .unwrap();
    let public = key
        .to_public_key()
        .to_public_key_pem(rsa::pkcs8::LineEnding::LF)
        .unwrap();
    let decoder = jsonwebtoken::DecodingKey::from_rsa_pem(public.as_bytes()).unwrap();
    let mut validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::RS256);
    validation.sub = Some("ORG-ACCOUNT.READER".into());
    for request in &requests {
        let token = request
            .lines()
            .find_map(|line| line.strip_prefix("authorization: Bearer "))
            .unwrap();
        let claims = jsonwebtoken::decode::<serde_json::Value>(token, &decoder, &validation)
            .unwrap()
            .claims;
        assert!(
            claims["iss"]
                .as_str()
                .unwrap()
                .starts_with("ORG-ACCOUNT.READER.SHA256:")
        );
        assert_eq!(
            claims["exp"].as_u64().unwrap() - claims["iat"].as_u64().unwrap(),
            300
        );
    }
    let body: serde_json::Value =
        serde_json::from_str(requests[0].split("\r\n\r\n").nth(1).unwrap()).unwrap();
    assert_eq!(body["role"], "ANALYST");
    assert_eq!(body["warehouse"], "COMPUTE");
    assert_eq!(
        body["parameters"]["TIMESTAMP_NTZ_OUTPUT_FORMAT"],
        "YYYY-MM-DD HH24:MI:SS.FF9"
    );
}
#[test]
fn values_reject_precision_loss_and_invalid_wire_values() {
    let field = |kind: &str, precision, scale| rest::Field {
        name: "N".into(),
        kind: kind.into(),
        precision,
        scale,
    };
    for (f, value) in [
        (field("fixed", 1, 0), "99"),
        (field("fixed", 38, 9), "1.0000000001"),
        (
            field("timestamp_ntz", 0, 0),
            "2026-01-01 00:00:00.000000001",
        ),
        (field("date", 0, 0), "0000-01-01"),
        (field("binary", 0, 0), "not hex"),
        (field("text", 0, 0), "NUL\0"),
    ] {
        assert!(
            result::row(&[f], vec![Some(value.into())], 4096).is_err(),
            "{value}"
        );
    }
    assert!(result::row(&[field("text", 0, 0)], vec![Some("too large".into())], 8).is_err());
    assert!(result::columns(&[field("text", 0, 0)], 8).is_err());
}
#[tokio::test]
async fn errors_limits_and_incomplete_results_never_produce_completion() {
    for response in [
        json!({"code":"002003","sqlState":"42S02","message":"missing table","statementHandle":"abc-123"}).to_string(),
        " ".repeat(4097),
        result(json!([]),json!([{"name":"N","type":"fixed","precision":38,"scale":0}]),1).to_string(),
        json!({"code":"333334","statementHandle":"../../escape"}).to_string(),
    ] {
        let (address,task)=fixture(vec![response]).await;
        let backend=SnowflakeBackend::new(config(address)).unwrap();
        let settings=GatewaySettings { query_timeout_ms:100, ..Default::default() };
        let mut session=backend.open(&settings,false).await.unwrap();
        let (_sender,cancel)=oneshot::channel();
        assert!(session.execute_sql("SELECT test",&mut Output::default(),&settings,&SessionSettings::default(),cancel).await.is_err());
        task.await.unwrap();
    }
}
#[tokio::test]
async fn frontend_compatibility_transaction_does_not_submit_transaction_sql() {
    let (address, task) = fixture(vec![
        result(
            json!([["1"]]),
            json!([{"name":"N","type":"fixed","precision":38,"scale":0}]),
            1,
        )
        .to_string(),
    ])
    .await;
    let backend = SnowflakeBackend::new(config(address)).unwrap();
    let settings = GatewaySettings::default();
    let mut session = backend.open(&settings, true).await.unwrap();
    let (_sender, cancel) = oneshot::channel();
    session
        .execute_sql(
            "SELECT 1",
            &mut Output::default(),
            &settings,
            &SessionSettings::default(),
            cancel,
        )
        .await
        .unwrap();
    assert!(matches!(
        backend.open(&settings, false).await,
        Err(ExecutionError::Rejected { code: "53300", .. })
    ));
    session.finish(false).await.unwrap();
    assert!(task.await.unwrap()[0].contains("SELECT 1"));
    assert!(backend.open(&settings, false).await.is_ok());
}

#[tokio::test]
async fn cancellation_keeps_admission_until_known_handle_cleanup_finishes() {
    let pending = json!({"code":"333334","statementHandle":"abc-123"}).to_string();
    let (address, task) = fixture(vec![
        pending,
        json!({"code":"000604","statementHandle":"abc-123"}).to_string(),
        json!({"code":"000604","statementHandle":"abc-123"}).to_string(),
    ])
    .await;
    let backend = SnowflakeBackend::new(config(address)).unwrap();
    let settings = GatewaySettings::default();
    let mut session = backend.open(&settings, false).await.unwrap();
    let (sender, cancel) = oneshot::channel();
    let mut output = Output::default();
    let frontend = SessionSettings::default();
    let mut run =
        Box::pin(session.execute_sql("SELECT slow", &mut output, &settings, &frontend, cancel));
    tokio::select! {
        result=&mut run => panic!("completed before cancellation: {}", result.is_ok()),
        _=tokio::time::sleep(Duration::from_millis(20))=>{}
    }
    sender.send(()).unwrap();
    assert!(matches!(
        run.await,
        Err(ExecutionError::Rejected { code: "57014", .. })
    ));
    session.finish(false).await.unwrap();
    let requests = task.await.unwrap();
    assert!(requests[1].starts_with("POST /api/v2/statements/abc-123/cancel "));
    assert!(requests[2].starts_with("GET /api/v2/statements/abc-123 "));
    assert!(backend.open(&settings, false).await.is_ok());
}

#[test]
fn encrypted_pkcs8_keys_produce_the_same_identity_and_reject_wrong_passphrases() {
    let pem = include_str!("../../../tests/fixtures/snowflake-test-key-encrypted.p8");
    let path = std::env::temp_dir().join(format!("yasp-rest-key-{}.p8", uuid::Uuid::new_v4()));
    std::fs::write(&path, pem.as_bytes()).unwrap();
    let mut plain = config("127.0.0.1:1".parse().unwrap());
    let expected = auth::KeyPair::load(&plain).unwrap().token().unwrap();
    plain.private_key_file = path.clone();
    plain.private_key_passphrase = Some("test passphrase".into());
    let actual = auth::KeyPair::load(&plain).unwrap().token().unwrap();
    // Tokens may cross an issue-time second; compare identity rather than bytes.
    use base64::Engine;
    let claims = |token: &str| -> serde_json::Value {
        serde_json::from_slice(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(token.split('.').nth(1).unwrap())
                .unwrap(),
        )
        .unwrap()
    };
    assert_eq!(claims(&actual)["iss"], claims(&expected)["iss"]);
    plain.private_key_passphrase = Some("wrong".into());
    assert!(auth::KeyPair::load(&plain).is_err());
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn gzip_response_bound_applies_after_decompression() {
    use base64::Engine;
    let gzip = base64::engine::general_purpose::STANDARD
        .decode("H4sIAAAAAAACA+3BAQ0AAADCoCrvn9IcbkABAAAAAAAAAADAvwFU1gY3ECcAAA==")
        .unwrap();
    let (address, task) = fixture_bytes(vec![gzip]).await;
    let backend = SnowflakeBackend::new(config(address)).unwrap();
    let settings = GatewaySettings::default();
    let mut session = backend.open(&settings, false).await.unwrap();
    let (_sender, cancel) = oneshot::channel();
    let result = session
        .execute_sql(
            "SELECT large",
            &mut Output::default(),
            &settings,
            &SessionSettings::default(),
            cancel,
        )
        .await;
    assert!(matches!(
        result,
        Err(ExecutionError::Rejected { code: "54000", .. })
    ));
    task.await.unwrap();
}

#[tokio::test]
async fn slow_frontend_blocks_fetching_the_next_partition() {
    struct Blocked {
        row: bool,
    }
    impl Sink<ResultEvent> for Blocked {
        type Error = ExecutionError;
        fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<ExecutionResult<()>> {
            Poll::Ready(Ok(()))
        }
        fn start_send(mut self: Pin<&mut Self>, event: ResultEvent) -> ExecutionResult<()> {
            self.row |= matches!(event, ResultEvent::Row(_));
            Ok(())
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<ExecutionResult<()>> {
            if self.row {
                Poll::Pending
            } else {
                Poll::Ready(Ok(()))
            }
        }
        fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<ExecutionResult<()>> {
            Poll::Ready(Ok(()))
        }
    }
    let mut first = result(
        json!([["1"]]),
        json!([{"name":"N","type":"fixed","precision":38,"scale":0}]),
        2,
    );
    first["resultSetMetaData"]["partitionInfo"] =
        json!([{"rowCount":1,"uncompressedSize":100},{"rowCount":1,"uncompressedSize":100}]);
    let (address, task) =
        fixture(vec![first.to_string(), json!({"data":[["2"]]}).to_string()]).await;
    let backend = SnowflakeBackend::new(config(address)).unwrap();
    let settings = GatewaySettings {
        write_timeout_ms: 30,
        ..Default::default()
    };
    let mut session = backend.open(&settings, false).await.unwrap();
    let mut blocked = Blocked { row: false };
    let (_sender, cancel) = oneshot::channel();
    let result = session
        .execute_sql(
            "SELECT many",
            &mut blocked,
            &settings,
            &SessionSettings::default(),
            cancel,
        )
        .await;
    assert!(result.is_err());
    assert!(blocked.row);
    assert!(
        !task.is_finished(),
        "partition 1 must not download while partition 0 is blocked"
    );
    task.abort();
}

#[tokio::test]
async fn failed_statement_http_422_releases_capacity_and_preserves_sqlstate() {
    let body=json!({"statementHandle":"abc-123","code":"002003","sqlState":"42S02","message":"missing table"}).to_string().into_bytes();
    let (address, task) = fixture_responses(vec![(422, body)]).await;
    let backend = SnowflakeBackend::new(config(address)).unwrap();
    let settings = GatewaySettings::default();
    let mut session = backend.open(&settings, false).await.unwrap();
    let (_sender, cancel) = oneshot::channel();
    let result = session
        .execute_sql(
            "SELECT missing",
            &mut Output::default(),
            &settings,
            &SessionSettings::default(),
            cancel,
        )
        .await;
    assert!(matches!(result,Err(ExecutionError::Source { code, .. }) if code == "42S02"));
    session.finish(false).await.unwrap();
    assert!(
        backend.open(&settings, false).await.is_ok(),
        "a terminal SQL error must return its slot immediately"
    );
    task.await.unwrap();
}
