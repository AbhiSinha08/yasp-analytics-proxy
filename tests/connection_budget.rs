//! One occupied-capacity ceiling across actual PostgreSQL and Snowflake adapters.
#![cfg(feature = "snowflake")]
mod support;
use std::{sync::Arc, time::Duration};
use support::{GatewayServer, RedisNamespace, TestDatabase, TestEnvironment, assert_success};
use yasp::{
    backend::{
        Backends, ConnectionBudget, PostgresBackends, PostgresBinding,
        snowflake::{SnowflakeBackend, SnowflakeConfig},
    },
    cache::RedisCache,
    catalog::{CatalogError, CatalogSettings},
    config::{BackendSettings, CacheSettings},
    hooks::{BackendSelection, bind_select_backend},
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mixed_targets_share_capacity_reclaim_idle_and_preserve_cache_and_catalog_reads() {
    let environment = TestEnvironment::load();
    let database = TestDatabase::create(&environment.source(None), "yasp_mixed_budget");
    database.populate();
    let identity = environment.gateway();
    let pg_selection = BackendSelection {
        target: "postgres".into(),
        backend_login: "reader".into(),
    };
    let sf_selection = BackendSelection {
        target: "snowflake".into(),
        backend_login: "reader".into(),
    };
    let settings = BackendSettings {
        max_connections_per_pool: 2,
        max_connections_total: 1,
        acquire_timeout_ms: 120,
    };
    let binding = || PostgresBinding {
        selection: pg_selection.clone(),
        compute_type: None,
        compute: "default".into(),
        default_compute_type: true,
        max_connections: 2,
        max_in_flight: 2,
        config: database.connection.backend_config(),
    };
    assert!(
        PostgresBackends::with_bindings_and_budget(
            vec![binding()],
            settings.clone(),
            ConnectionBudget::new(2).unwrap()
        )
        .is_err()
    );
    let pg = Arc::new(
        PostgresBackends::with_bindings_and_budget(
            vec![binding()],
            settings,
            ConnectionBudget::new(1).unwrap(),
        )
        .unwrap(),
    );
    let sf = Arc::new(
        SnowflakeBackend::new(SnowflakeConfig {
            private_key_file: std::env::var("YASP_SNOWFLAKE_PRIVATE_KEY_FILE")
                .unwrap()
                .into(),
            private_key_passphrase: None,
            account: "local".into(),
            username: "fake".into(),
            database: "YASP_DEMO".into(),
            schema: "PUBLIC".into(),
            role: "ANALYST".into(),
            warehouse: "TEST".into(),
            emulator: Some(
                std::env::var("YASP_SNOWFLAKE_ENDPOINT")
                    .unwrap()
                    .parse()
                    .unwrap(),
            ),
            max_connections: 2,
            acquire_timeout: Duration::from_millis(120),
            max_response_bytes: 8 * 1024 * 1024,
        })
        .unwrap(),
    );
    let backends = Arc::new(
        Backends::new(pg.clone())
            .with_snowflake(sf_selection.clone(), sf)
            .unwrap(),
    );
    let mut catalog_settings = CatalogSettings::new(vec!["public".into()]);
    catalog_settings.refresh_interval = Duration::from_secs(60);
    catalog_settings.refresh_timeout = Duration::from_secs(2);
    let catalog = backends
        .catalog(&pg_selection, None, catalog_settings)
        .await
        .unwrap();
    let generation = catalog.snapshot().unwrap().generation;
    let redis_url = std::env::var("YASP_TEST_REDIS_URL").unwrap();
    let namespace = RedisNamespace::new(&redis_url, "yasp_mixed_budget");
    let cache = Arc::new(
        RedisCache::new(
            CacheSettings {
                enabled: true,
                namespace: namespace.name.clone(),
                max_rows_per_query: 10,
                max_buffered_bytes_total: 196608,
                ..CacheSettings::default()
            },
            &redis_url,
            None,
        )
        .unwrap(),
    );
    let config = identity
        .gateway_config()
        .with_schema_discovery(pg_selection.clone())
        .with_catalog(catalog.clone())
        .with_cache(cache);
    let selector = bind_select_backend((pg_selection.clone(), sf_selection), |request, routes| {
        Ok(if request.query.sql().contains("route_postgres") {
            routes.0.clone()
        } else {
            routes.1.clone()
        })
    });
    let server = GatewayServer::start(move |listener, stopped| async move {
        yasp::gateway::serve_backends(listener, config, backends, selector, async {
            let _ = stopped.await;
        })
        .await
        .unwrap();
    });
    let client = identity.connection(server.address);
    assert_success(
        &client.query("SELECT 41 /* route_postgres */"),
        "warm PostgreSQL cache",
    );
    let route = pg.resolve(&pg_selection.clone().into()).unwrap();
    let held = route.acquire().await.unwrap();
    let blocked = client.query("SELECT 42");
    assert!(
        String::from_utf8_lossy(&blocked.stderr).contains("53300"),
        "{}",
        String::from_utf8_lossy(&blocked.stderr)
    );
    drop(held);
    assert_success(
        &client.query("SELECT 42"),
        "Snowflake reclaims idle PostgreSQL capacity",
    );

    use pgwire::{
        api::client::{Config, auth::DefaultStartupHandler, query::DefaultSimpleQueryHandler},
        tokio::client::PgWireClient,
    };
    let mut login = Config::new();
    login
        .host("127.0.0.1")
        .port(server.address.port())
        .user(&identity.user)
        .password(&identity.password)
        .dbname(&identity.database);
    let mut transaction =
        PgWireClient::connect(Arc::new(login), DefaultStartupHandler::new(), None)
            .await
            .unwrap();
    transaction
        .simple_query(DefaultSimpleQueryHandler::new(), "BEGIN")
        .await
        .unwrap();
    transaction
        .simple_query(DefaultSimpleQueryHandler::new(), "SELECT 43")
        .await
        .unwrap();
    assert!(
        route.acquire().await.is_err(),
        "Snowflake's retained session must block PostgreSQL"
    );
    assert_eq!(
        catalog.refresh().await,
        Err(CatalogError::SourceUnavailable)
    );
    assert_eq!(catalog.snapshot().unwrap().generation, generation);
    assert_success(
        &client.query("SELECT 41 /* route_postgres */"),
        "cache hits need no backend slot",
    );
    transaction
        .simple_query(DefaultSimpleQueryHandler::new(), "ROLLBACK")
        .await
        .unwrap();
    let recovered = route.acquire().await.unwrap();
    drop(recovered);
    catalog.refresh().await.unwrap();
    assert!(catalog.snapshot().unwrap().generation > generation);
    catalog.stop_refreshing().await;
    drop(transaction);
    let held = route.acquire().await.unwrap();
    let waiting = route.acquire();
    tokio::pin!(waiting);
    assert!(futures::poll!(&mut waiting).is_pending());
    server.stop();
    assert!(
        waiting.await.is_err(),
        "closing the binding must wake its waiters"
    );
    drop(held);

    // A failed physical creation must release its global reservation.
    let recovery_budget = ConnectionBudget::new(1).unwrap();
    let mut invalid = database.connection.clone();
    invalid.identity.password = "incorrect-source-password".into();
    let failed = yasp::backend::PostgresBackend::with_budget(
        invalid.backend_config(),
        recovery_budget.clone(),
    )
    .unwrap();
    assert!(failed.route().acquire().await.is_err());
    let healthy = yasp::backend::PostgresBackend::with_budget(
        database.connection.backend_config(),
        recovery_budget,
    )
    .unwrap();
    let route = healthy.route();
    let lease = route.acquire().await.unwrap();
    drop(lease);
}
