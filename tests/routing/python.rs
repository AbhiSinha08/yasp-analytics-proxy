use super::*;

#[allow(clippy::too_many_arguments)]
pub(super) fn python_routing_contract(
    backends: std::sync::Arc<yasp::backend::PostgresBackends>,
    route_one: yasp::hooks::BackendSelection,
    route_two: yasp::hooks::BackendSelection,
    username: &str,
    password: &str,
    database: &str,
    marker_one: &str,
    marker_two: &str,
) {
    use futures::SinkExt;
    use pgwire::{
        api::client::{
            ClientInfo, Config, auth::DefaultStartupHandler, query::DefaultSimpleQueryHandler,
        },
        error::PgWireClientError,
        messages::{PgWireFrontendMessage, cancel::CancelRequest},
        tokio::client::{PgWireClient, PgWireMessageClientCodec},
    };
    use std::{fs, sync::Arc, time::Duration};
    use tokio::{
        net::TcpStream,
        time::{sleep, timeout},
    };
    use tokio_util::codec::Framed;
    use yasp::{
        config::{GatewayConfig, GatewaySettings, PythonSettings},
        hooks::python::PythonRuntime,
    };

    let temporary = support::TempDirectory::new("yasp_python_route");
    let directory = &temporary.0;
    let path = directory.join("application.py");
    let entered = directory.join("entered");
    let release = directory.join("release");
    let finished = directory.join("finished");
    fs::write(
        &path,
        r#"
import time
from pathlib import Path

def application_route(request, params):
    assert set(request) == {'authenticated_user', 'client_id', 'query_id', 'sql', 'context', 'query_ast', 'data_contexts'}
    assert 'Query' in request['query_ast']
    assert request['data_contexts'] == {}
    assert request['authenticated_user'] == params['username']
    assert len(request['query_id']) == 64
    assert all(character in '0123456789abcdef' for character in request['query_id'])
    sql = request['sql']
    if 'denied' in sql:
        return None
    if 'failed' in sql:
        raise RuntimeError('private Python failure')
    if 'unknown' in sql:
        return {'target': 'absent', 'backend_login': 'reader'}
    if 'malformed' in sql:
        return {'target': []}
    if 'partial' in sql:
        return {'target': params['one']['target']}
    if 'sequence' in sql:
        return [params['one']['target'], params['one']['backend_login']]
    if 'slow' in sql:
        Path(params['entered']).touch()
        deadline = time.monotonic() + 5
        while not Path(params['release']).exists():
            if time.monotonic() > deadline:
                raise RuntimeError('test synchronization failed')
            time.sleep(0.005)
        Path(params['finished']).touch()
    return params['two'] if 'route_two' in sql else params['one']
"#,
    )
    .unwrap();
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
        let runtime = PythonRuntime::new(PythonSettings {
            queue_capacity: 1, execution_timeout_ms: 500,
            ..PythonSettings::default()
        }).await.unwrap();
        let params = serde_json::json!({"one": route_one, "two": route_two, "username": username,
                                         "entered": entered, "release": release, "finished": finished});
        let selector = runtime.register(&path, "application_route", params.as_object().unwrap().clone()).await.unwrap();
        let gateway = GatewayConfig::new(username.into(), password.into(), database.into()).unwrap()
            .with_settings(GatewaySettings { shutdown_timeout_ms: 50, ..GatewaySettings::default() }).unwrap()
            .with_schema_discovery(route_one.clone());
        let server = GatewayServer::routed(gateway, backends, selector);
        let address = server.address;
        let mut config = Config::new();
        config.host(address.ip().to_string()).port(address.port()).user(username).password(password).dbname(database);
        let mut client = PgWireClient::connect(Arc::new(config), DefaultStartupHandler::new(), None).await.unwrap();

        async fn marker(client: &mut PgWireClient, sql: &str) -> String {
            let mut response = client.simple_query(DefaultSimpleQueryHandler::new(), sql).await.unwrap()
                .remove(0).into_data_rows_reader();
            response.next_row().unwrap().next_value::<String>().unwrap().unwrap()
        }
        fn code(error: PgWireClientError) -> String {
            match error {
                PgWireClientError::RemoteError(info) => {
                    assert!(!info.message.contains("private Python"));
                    info.code
                }
                other => panic!("unexpected frontend error: {other:?}"),
            }
        }
        for (tag, expected) in [("route_one", marker_one), ("route_two", marker_two), ("partial", marker_one)] {
            assert_eq!(marker(&mut client, &format!("SELECT current_database() || ':' || current_user /* {tag} */")).await, expected);
        }
        for (tag, expected) in [("denied", "42501"), ("failed", "XX000"), ("unknown", "42501"), ("malformed", "XX000"), ("sequence", "XX000")] {
            let error = client.simple_query(DefaultSimpleQueryHandler::new(), &format!("SELECT 1 /* {tag} */")).await.unwrap_err();
            assert_eq!(code(error), expected);
            assert_eq!(marker(&mut client, "SELECT current_database() || ':' || current_user").await, marker_one);
        }
        for cancel in [true, false] {
            let pid = client.process_id();
            let secret = client.secret_key().clone();
            let mut query = Box::pin(client.simple_query(DefaultSimpleQueryHandler::new(), "SELECT 1 /* slow */"));
            timeout(Duration::from_secs(2), async {
                while !entered.exists() {
                    tokio::select! {
                        result = &mut query => panic!("selector completed before synchronization: {result:?}"),
                        _ = sleep(Duration::from_millis(5)) => {}
                    }
                }
            }).await.unwrap();
            if cancel {
                let socket = TcpStream::connect(address).await.unwrap();
                Framed::new(socket, PgWireMessageClientCodec::default())
                    .send(PgWireFrontendMessage::CancelRequest(CancelRequest::new(pid, secret))).await.unwrap();
            }
            assert_eq!(code(query.await.unwrap_err()), if cancel { "57014" } else { "XX000" });
            // Neither cancellation nor timeout releases the executing worker.
            assert_eq!(code(client.simple_query(DefaultSimpleQueryHandler::new(), "SELECT 1").await.unwrap_err()), "XX000");
            fs::write(&release, "").unwrap();
            timeout(Duration::from_secs(2), async {
                while !finished.exists() { sleep(Duration::from_millis(5)).await; }
            }).await.unwrap();
            timeout(Duration::from_secs(2), async {
                loop {
                    match client.simple_query(DefaultSimpleQueryHandler::new(), "SELECT current_database() || ':' || current_user").await {
                        Ok(mut response) => {
                            let mut rows = response.remove(0).into_data_rows_reader();
                            assert_eq!(rows.next_row().unwrap().next_value::<String>().unwrap().unwrap(), marker_one);
                            break;
                        }
                        Err(error) => {
                            assert_eq!(code(error), "XX000");
                            sleep(Duration::from_millis(5)).await;
                        }
                    }
                }
            }).await.unwrap();
            for path in [&entered, &release, &finished] { fs::remove_file(path).unwrap(); }
        }
        drop(client);
        server.stop();
        runtime.shutdown();
    });
}
