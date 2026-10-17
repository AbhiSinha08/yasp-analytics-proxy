//! Validated certificate material shared by the protocol adapters.

use crate::config::{ConfigError, GatewayTls};
use pgwire::tokio::{TlsAcceptor, TlsConnector};
use rustls::{
    ClientConfig, RootCertStore, ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer, ServerName, pem::PemObject},
};
use serde::{Deserialize, Serialize};
use std::{
    io,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::net::TcpStream;

/// Outbound trust and optional client identity. An explicit CA bundle replaces system roots.
#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ClientTlsSettings {
    pub ca_file: Option<PathBuf>,
    pub cert_file: Option<PathBuf>,
    pub key_file: Option<PathBuf>,
}

impl ClientTlsSettings {
    pub(crate) fn validate(&self) -> Result<(), ConfigError> {
        if self.cert_file.is_some() != self.key_file.is_some() {
            return Err(ConfigError::InvalidTls);
        }
        Ok(())
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.ca_file.is_none() && self.cert_file.is_none() && self.key_file.is_none()
    }

    pub(crate) fn resolve(&mut self, base: &Path) {
        for path in [&mut self.ca_file, &mut self.cert_file, &mut self.key_file]
            .into_iter()
            .flatten()
        {
            if path.is_relative() {
                *path = base.join(&*path);
            }
        }
    }
}

/// PostgreSQL transport mode and optional outbound certificate material.
#[derive(Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct TargetTls {
    pub mode: String,
    #[serde(flatten)]
    pub client: ClientTlsSettings,
}

impl Default for TargetTls {
    fn default() -> Self {
        Self {
            mode: "disable".into(),
            client: ClientTlsSettings::default(),
        }
    }
}

impl TargetTls {
    pub(crate) fn validate(&self) -> Result<(), ConfigError> {
        self.client.validate()?;
        match self.mode.as_str() {
            "enable" => Ok(()),
            "disable" if self.client.is_empty() => Ok(()),
            _ => Err(ConfigError::InvalidTls),
        }
    }
}

impl GatewayTls {
    pub(crate) fn validate(&self) -> Result<(), ConfigError> {
        match self.mode.as_str() {
            "enable" if self.cert_file.is_some() && self.key_file.is_some() => Ok(()),
            "disable" if self.cert_file.is_none() && self.key_file.is_none() => Ok(()),
            _ => Err(ConfigError::InvalidTls),
        }
    }
}

fn read(path: &Path) -> Result<Vec<u8>, ConfigError> {
    std::fs::read(path).map_err(|_| ConfigError::InvalidTls)
}

fn certificates(pem: &[u8]) -> Result<Vec<CertificateDer<'static>>, ConfigError> {
    let certs = CertificateDer::pem_slice_iter(pem)
        .map(|cert| {
            cert.map(|cert| cert.into_owned())
                .map_err(|_| ConfigError::InvalidTls)
        })
        .collect::<Result<Vec<_>, _>>()?;
    if certs.is_empty() {
        return Err(ConfigError::InvalidTls);
    }
    Ok(certs)
}

pub(crate) struct ServerTls {
    pub acceptor: TlsAcceptor,
    pub certificate: Vec<u8>,
}

impl ServerTls {
    pub fn load(settings: &GatewayTls) -> Result<Option<Arc<Self>>, ConfigError> {
        settings.validate()?;
        if settings.mode == "disable" {
            return Ok(None);
        }
        let certificate = read(settings.cert_file.as_ref().ok_or(ConfigError::InvalidTls)?)?;
        let key = read(settings.key_file.as_ref().ok_or(ConfigError::InvalidTls)?)?;
        let key = PrivateKeyDer::from_pem_slice(&key).map_err(|_| ConfigError::InvalidTls)?;
        let config =
            ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()
                .map_err(|_| ConfigError::InvalidTls)?
                .with_no_client_auth()
                .with_single_cert(certificates(&certificate)?, key)
                .map_err(|_| ConfigError::InvalidTls)?;
        // Validate channel-binding support at startup, before any clients are admitted.
        pgwire::api::auth::sasl::scram::ScramServerAuth::new()
            .configure_certificate(&certificate)
            .map_err(|_| ConfigError::InvalidTls)?;
        Ok(Some(Arc::new(Self {
            acceptor: TlsAcceptor::from(Arc::new(config)),
            certificate,
        })))
    }
}

pub(crate) struct ClientTls {
    connector: TlsConnector,
}

fn client_material(
    settings: &ClientTlsSettings,
) -> Result<(ClientConfig, redis::TlsCertificates), ConfigError> {
    settings.validate()?;
    let root_cert = settings.ca_file.as_deref().map(read).transpose()?;
    let mut roots = RootCertStore::empty();
    if let Some(pem) = &root_cert {
        for cert in certificates(pem)? {
            roots.add(cert).map_err(|_| ConfigError::InvalidTls)?;
        }
    } else {
        let native = rustls_native_certs::load_native_certs();
        roots.add_parsable_certificates(native.certs);
        if roots.is_empty() {
            return Err(ConfigError::InvalidTls);
        }
    }
    let builder =
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .map_err(|_| ConfigError::InvalidTls)?
            .with_root_certificates(roots);
    let client_tls = settings
        .cert_file
        .as_deref()
        .zip(settings.key_file.as_deref())
        .map(|(cert, key)| {
            Ok::<_, ConfigError>(redis::ClientTlsConfig {
                client_cert: read(cert)?,
                client_key: read(key)?,
            })
        })
        .transpose()?;
    let config = if let Some(identity) = &client_tls {
        builder
            .with_client_auth_cert(
                certificates(&identity.client_cert)?,
                PrivateKeyDer::from_pem_slice(&identity.client_key)
                    .map_err(|_| ConfigError::InvalidTls)?,
            )
            .map_err(|_| ConfigError::InvalidTls)?
    } else {
        builder.with_no_client_auth()
    };
    Ok((
        config,
        redis::TlsCertificates {
            client_tls,
            root_cert,
        },
    ))
}

impl ClientTls {
    pub fn load(settings: &ClientTlsSettings) -> Result<Self, ConfigError> {
        let (config, _) = client_material(settings)?;
        Ok(Self {
            connector: TlsConnector::from(Arc::new(config)),
        })
    }

    pub async fn connect(
        &self,
        socket: TcpStream,
        host: &str,
    ) -> io::Result<crate::transport::Socket> {
        let name = ServerName::try_from(host.to_owned())
            .map_err(|_| io::Error::other("invalid TLS server name"))?;
        let socket = self
            .connector
            .connect(name, socket)
            .await
            .map_err(|_| io::Error::other("TLS verification failed"))?;
        Ok(Box::new(socket))
    }
}

/// Redis URL parsing and transport policy are identical for cache and datasets.
pub(crate) fn redis_client(url: &str, settings: &ClientTlsSettings) -> io::Result<redis::Client> {
    let invalid = || {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid Redis transport configuration",
        )
    };
    let client = redis::Client::open(url).map_err(|_| invalid())?;
    match client.get_connection_info().addr() {
        redis::ConnectionAddr::Tcp(_, _) if settings.is_empty() => Ok(client),
        redis::ConnectionAddr::TcpTls {
            insecure: false, ..
        } => {
            // Redis constructs its own rustls configuration from this validated material.
            // Respect a provider already selected by the consuming application.
            let _ = rustls::crypto::ring::default_provider().install_default();
            let (_, certs) = client_material(settings).map_err(|_| invalid())?;
            redis::Client::build_with_tls(url, certs).map_err(|_| invalid())
        }
        _ => Err(invalid()),
    }
}
