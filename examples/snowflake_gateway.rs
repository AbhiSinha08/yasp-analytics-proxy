//! Native Snowflake charts with PostgreSQL metadata through one pgwire listener.
use std::{collections::BTreeMap, env, sync::Arc, time::Duration};
use yasp::{
    backend::{
        Backends, PostgresBackends,
        snowflake::{SnowflakeBackend, SnowflakeConfig},
    },
    config::{BackendConfig, BackendSettings, GatewayConfig},
    hooks::{BackendSelection, bind_select_backend, builtin::query_context::metabase},
    query::SqlDialect,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    yasp::logger::init()?;
    let catalog = BackendSelection {
        target: "catalog".into(),
        backend_login: "reader".into(),
    };
    let selected = BackendSelection {
        target: "snowflake".into(),
        backend_login: "analyst".into(),
    };
    let postgres = Arc::new(PostgresBackends::new(
        BTreeMap::from([(
            catalog.clone(),
            BackendConfig::new(
                env::var("PGHOST")?,
                env::var("PGPORT")?.parse()?,
                env::var("PGUSER")?,
                env::var("PGPASSWORD")?,
                env::var("PGDATABASE")?,
            )?,
        )]),
        BackendSettings::default(),
    )?);
    let snowflake = Arc::new(SnowflakeBackend::new(SnowflakeConfig {
        private_key_file: env::var("SNOWFLAKE_PRIVATE_KEY_FILE")?.into(),
        private_key_passphrase: env::var("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE").ok(),
        account: env::var("SNOWFLAKE_ACCOUNT")?,
        username: env::var("SNOWFLAKE_USER")?,
        database: env::var("SNOWFLAKE_DATABASE")?,
        schema: env::var("SNOWFLAKE_SCHEMA")?,
        role: env::var("SNOWFLAKE_ROLE")?,
        warehouse: env::var("SNOWFLAKE_WAREHOUSE")?,
        emulator: None,
        max_connections: 4,
        acquire_timeout: Duration::from_secs(60),
        max_response_bytes: 8 * 1024 * 1024,
    })?);
    let backends = Arc::new(Backends::new(postgres).with_snowflake(selected.clone(), snowflake)?);
    let snapshot = backends
        .catalog(
            &catalog,
            None,
            yasp::catalog::CatalogSettings::new(vec!["public".into()]),
        )
        .await?;
    let selector = bind_select_backend(selected, |_, route| Ok(route.clone()));
    let config = GatewayConfig::new(
        env::var("YASP_GATEWAY_USERNAME")?,
        env::var("YASP_GATEWAY_PASSWORD")?,
        env::var("YASP_GATEWAY_DATABASE")?,
    )?
    .with_catalog(snapshot.clone())
    .with_schema_discovery(catalog)
    // This registry includes two target dialects; declare native input explicitly.
    .with_query_context_hook(yasp::hooks::bind_query_context((), |request, _| {
        Box::pin(async move { metabase(request, Some(SqlDialect::Snowflake)) })
    }));
    let listener = tokio::net::TcpListener::bind(config.settings().listen).await?;
    let result = yasp::gateway::serve_backends(listener, config, backends, selector, async {
        let _ = tokio::signal::ctrl_c().await;
    })
    .await;
    snapshot.stop_refreshing().await;
    result?;
    Ok(())
}
