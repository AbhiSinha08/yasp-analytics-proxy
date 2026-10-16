use super::*;

pub(super) fn routed_cancellation_contract(
    address: std::net::SocketAddr,
    username: &str,
    password: &str,
    database: &str,
) {
    use std::sync::Arc;
    use tokio::runtime::Builder;

    Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("cancellation test runtime starts")
        .block_on(async {
            use pgwire::{
                api::client::{
                    ClientInfo, Config, auth::DefaultStartupHandler,
                    query::DefaultSimpleQueryHandler,
                },
                error::PgWireClientError,
                messages::{
                    PgWireFrontendMessage, cancel::CancelRequest, startup::SecretKey,
                },
                tokio::client::{PgWireClient, PgWireMessageClientCodec},
            };
            use futures::SinkExt;
            use tokio::{net::TcpStream, time::sleep};
            use tokio_util::codec::Framed;

            async fn connect(
                address: std::net::SocketAddr,
                username: &str,
                password: &str,
                database: &str,
            ) -> PgWireClient {
                let mut config = Config::new();
                config
                    .host(address.ip().to_string())
                    .port(address.port())
                    .user(username)
                    .password(password)
                    .dbname(database);
                PgWireClient::connect(
                    Arc::new(config),
                    DefaultStartupHandler::new(),
                    None,
                )
                .await
                .expect("frontend authenticates with the gateway")
            }

            async fn cancel(
                address: std::net::SocketAddr,
                pid: i32,
                secret_key: SecretKey,
            ) {
                let socket = TcpStream::connect(address)
                    .await
                    .expect("cancel connection reaches the gateway");
                let mut socket = Framed::new(socket, PgWireMessageClientCodec::default());
                socket
                    .send(PgWireFrontendMessage::CancelRequest(CancelRequest::new(
                        pid, secret_key,
                    )))
                    .await
                    .expect("cancel request is sent");
            }

            let mut client = connect(address, username, password, database).await;
            let client_pid = client.process_id();
            let client_key = client.secret_key().clone();

            // A valid key with no active query is harmless.
            cancel(address, client_pid, client_key.clone()).await;
            sleep(std::time::Duration::from_millis(50)).await;
            client
                .simple_query(DefaultSimpleQueryHandler::new(), "SELECT 1 /* route_two */")
                .await
                .expect("idle cancellation does not affect the next query");

            let mut unknown_key_query = Box::pin(client.simple_query(
                DefaultSimpleQueryHandler::new(),
                "SELECT pg_sleep(0.5) /* route_two */",
            ));
            tokio::select! {
                result = &mut unknown_key_query => panic!("query completed before the unknown cancel: {result:?}"),
                _ = sleep(std::time::Duration::from_millis(100)) => {}
            }
            cancel(address, i32::MIN, SecretKey::I32(0)).await;
            unknown_key_query
                .await
                .expect("unknown cancellation key does not interrupt an active query");

            let mut other = connect(address, username, password, database).await;
            let mut cancelled_query = Box::pin(client.simple_query(
                DefaultSimpleQueryHandler::new(),
                "SELECT pg_sleep(8) /* route_two */",
            ));
            let mut other_query = Box::pin(other.simple_query(
                DefaultSimpleQueryHandler::new(),
                "SELECT pg_sleep(2) /* route_one */",
            ));

            tokio::select! {
                result = &mut cancelled_query => panic!("long query completed before cancellation: {result:?}"),
                result = &mut other_query => panic!("other route completed before cancellation: {result:?}"),
                _ = sleep(std::time::Duration::from_millis(200)) => {}
            }
            cancel(address, client_pid, client_key.clone()).await;

            let error = cancelled_query
                .await
                .expect_err("active routed query is cancelled");
            match error {
                PgWireClientError::RemoteError(info) => assert_eq!(info.code, "57014"),
                other => panic!("cancellation returned an unexpected error: {other:?}"),
            }
            other_query
                .await
                .expect("cancelling one routed query leaves the other route running");

            client
                .simple_query(DefaultSimpleQueryHandler::new(), "SELECT 1 /* route_two */")
                .await
                .expect("frontend session is usable after query cancellation");

            // A late request from the first session cannot cancel another client's work.
            let mut other_query = Box::pin(other.simple_query(
                DefaultSimpleQueryHandler::new(),
                "SELECT pg_sleep(1) /* route_one */",
            ));
            tokio::select! {
                result = &mut other_query => panic!("other route completed before the stale cancel: {result:?}"),
                _ = sleep(std::time::Duration::from_millis(100)) => {}
            }
            cancel(address, client_pid, client_key).await;
            other_query
                .await
                .expect("late cancellation cannot interrupt another client");
        });
}

pub(super) fn cancellation_discards_backend(
    client: &dyn Fn(&[&str]) -> Output,
    address: std::net::SocketAddr,
    identity: &Identity,
) {
    let route_two_pid_before = client(&["SELECT pg_backend_pid() /* route_two */"]);
    assert!(route_two_pid_before.status.success());
    let route_two_pid_before = String::from_utf8_lossy(&route_two_pid_before.stdout)
        .trim()
        .parse::<i32>()
        .expect("route query returns the PostgreSQL backend PID");
    routed_cancellation_contract(
        address,
        &identity.user,
        &identity.password,
        &identity.database,
    );
    let route_two_pid_after = client(&["SELECT pg_backend_pid() /* route_two */"]);
    assert!(route_two_pid_after.status.success());
    let route_two_pid_after = String::from_utf8_lossy(&route_two_pid_after.stdout)
        .trim()
        .parse::<i32>()
        .expect("route query returns the PostgreSQL backend PID");
    assert_ne!(
        route_two_pid_before, route_two_pid_after,
        "a backend whose cancellation may still be in flight is discarded"
    );
}
