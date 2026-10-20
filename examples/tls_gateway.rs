//! Minimal consuming application. See docs/tls.md for certificates and environment.
use std::{collections::BTreeMap, env, sync::Arc};
use tokio::net::TcpListener;
use yasp::{
    backend::{PostgresBackend, PostgresBackends},
    config::{
        BackendConfig, BackendSettings, ClientTlsSettings, GatewayConfig, GatewaySettings,
        GatewayTls,
    },
    gateway,
    hooks::{BackendSelection, bind_select_backend},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    yasp::logger::init()?;
    let optional_path = |name: &str| env::var_os(name).map(std::path::PathBuf::from);
    let backend = BackendConfig::new_with_tls(
        env::var("PGHOST")?,
        env::var("PGPORT")?.parse()?,
        env::var("PGUSER")?,
        env::var("PGPASSWORD")?,
        env::var("PGDATABASE")?,
        ClientTlsSettings {
            ca_file: optional_path("PGSSLROOTCERT"),
            cert_file: optional_path("PGSSLCERT"),
            key_file: optional_path("PGSSLKEY"),
        },
    )?;
    let config = GatewayConfig::new(
        env::var("YASP_GATEWAY_USERNAME")?,
        env::var("YASP_GATEWAY_PASSWORD")?,
        env::var("YASP_GATEWAY_DATABASE")?,
    )?
    .with_settings(GatewaySettings {
        listen: env::var("YASP_LISTEN")
            .unwrap_or_else(|_| "127.0.0.1:6432".into())
            .parse()?,
        tls: GatewayTls {
            mode: "enable".into(),
            cert_file: Some(env::var("YASP_TLS_CERT")?.into()),
            key_file: Some(env::var("YASP_TLS_KEY")?.into()),
        },
        ..Default::default()
    })?;
    let listener = TcpListener::bind(config.settings().listen).await?;
    let shutdown = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    match env::args().nth(1).as_deref() {
        None | Some("fixed") => {
            let backend = Arc::new(PostgresBackend::new(backend)?);
            let catalog = yasp::catalog::Catalog::postgres(
                backend.clone(),
                yasp::catalog::CatalogSettings::new(vec!["public".into()]),
            )
            .await?;
            let result = gateway::serve(
                listener,
                config.with_catalog(catalog.clone()),
                backend,
                shutdown,
            )
            .await;
            catalog.stop_refreshing().await;
            result?;
        }
        Some("routed") => {
            let route = BackendSelection {
                target: "primary".into(),
                backend_login: "reader".into(),
            };
            let selector = bind_select_backend(route.clone(), |_, route| Ok(route.clone()));
            let backends = Arc::new(PostgresBackends::new(
                BTreeMap::from([(route.clone(), backend)]),
                BackendSettings::default(),
            )?);
            let source = backends
                .get_discovery(&route, None)
                .ok_or("missing discovery source")?;
            let catalog = yasp::catalog::Catalog::postgres(
                Arc::new(source.clone()),
                yasp::catalog::CatalogSettings::new(vec!["public".into()]),
            )
            .await?;
            let result = gateway::serve_routed(
                listener,
                config
                    .with_schema_discovery(route)
                    .with_catalog(catalog.clone()),
                backends,
                selector,
                shutdown,
            )
            .await;
            catalog.stop_refreshing().await;
            result?;
        }
        Some(_) => return Err("usage: tls_gateway [fixed|routed]".into()),
    }
    Ok(())
}
