use std::{fs, net::SocketAddr, path::Path, sync::Arc, time::Duration};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use rustls::{
    RootCertStore,
    server::{WebPkiClientVerifier, danger::ClientCertVerifier},
};
use rustls_pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use tokio::net::TcpStream;
use tokio_rustls::TlsAcceptor;
use tracing::debug;

use crate::command::server::{
    ServerContext,
    error::Error,
    listeners::{Connector, HandshakeResult, accept_loop},
};
pub use crate::configuration::listeners::{ServerTlsConfig, TlsListenerConfig};

fn load_certificate_bundle(
    path: &Path,
    description: &str,
) -> Result<Vec<CertificateDer<'static>>, Error> {
    let pem = fs::read(path)
        .map_err(|e| Error::Initialization(format!("Failed to read {description}: {e}")))?;
    CertificateDer::pem_slice_iter(&pem)
        .collect::<Result<_, _>>()
        .map_err(|e| Error::Initialization(format!("Failed to build {description}: {e}")))
}

fn load_private_key(path: &Path, description: &str) -> Result<PrivateKeyDer<'static>, Error> {
    let pem = fs::read(path)
        .map_err(|e| Error::Initialization(format!("Failed to read {description}: {e}")))?;
    PrivateKeyDer::from_pem_slice(&pem)
        .map_err(|e| Error::Initialization(format!("Failed to build {description}: {e}")))
}

fn build_client_verifier(client_ca_bundle: &Path) -> Result<Arc<dyn ClientCertVerifier>, Error> {
    let client_certs = load_certificate_bundle(client_ca_bundle, "client certificates bundle")?;

    let mut client_cert_store = RootCertStore::empty();
    for client_cert in client_certs {
        client_cert_store.add(client_cert).map_err(|e| {
            Error::Initialization(format!(
                "Failed to add client CA certificate to root cert store: {e}"
            ))
        })?;
    }

    WebPkiClientVerifier::builder(Arc::new(client_cert_store))
        .allow_unauthenticated()
        .build()
        .map_err(|e| {
            Error::Initialization(format!(
                "Failed to create TLS client certificate verifier: {e}"
            ))
        })
}

struct TlsConnector<'a> {
    tls_acceptor: &'a ArcSwap<TlsAcceptor>,
}

#[async_trait]
impl Connector for TlsConnector<'_> {
    type Stream = tokio_rustls::server::TlsStream<TcpStream>;

    async fn handshake(
        &self,
        tcp: TcpStream,
        remote_address: SocketAddr,
    ) -> Option<HandshakeResult<Self::Stream>> {
        let acceptor = Arc::clone(&self.tls_acceptor.load());
        let result = acceptor.accept(tcp).await;

        let tls = match result {
            Ok(tls) => tls,
            Err(e) => {
                debug!("TLS handshake failed from {remote_address}: {e}");
                return None;
            }
        };

        let (_, session) = tls.get_ref();
        let peer_certificate = session
            .peer_certificates()
            .and_then(|certs| certs.first())
            .map(|cert| cert.to_vec());

        Some(HandshakeResult {
            stream: tls,
            peer_certificate,
        })
    }

    fn label(&self) -> &'static str {
        "mTLS"
    }
}

pub struct TlsListener {
    binding_address: SocketAddr,
    tls_acceptor: ArcSwap<TlsAcceptor>,
    context: ArcSwap<ServerContext>,
    timeouts: ArcSwap<[Duration; 2]>,
}

impl TlsListener {
    pub fn new(config: &TlsListenerConfig, context: ServerContext) -> Result<Self, Error> {
        let binding_address = SocketAddr::new(config.base.bind_address, config.base.port);
        let tls_acceptor = ArcSwap::from_pointee(Self::build_tls_acceptor(&config.tls)?);
        let timeouts = [
            Duration::from_secs(config.base.query_timeout_secs.get()),
            Duration::from_secs(config.base.query_timeout_grace_period_secs.get()),
        ];

        Ok(Self {
            binding_address,
            tls_acceptor,
            context: ArcSwap::from_pointee(context),
            timeouts: ArcSwap::from_pointee(timeouts),
        })
    }

    pub async fn shutdown_with_timeout(&self, timeout: Duration) {
        self.context.load().shutdown_with_timeout(timeout).await;
    }

    pub fn notify_config_change(
        &self,
        config: &TlsListenerConfig,
        context: ServerContext,
    ) -> Result<(), Error> {
        let acceptor = Arc::new(Self::build_tls_acceptor(&config.tls)?);
        self.tls_acceptor.store(acceptor);

        let timeouts = [
            Duration::from_secs(config.base.query_timeout_secs.get()),
            Duration::from_secs(config.base.query_timeout_grace_period_secs.get()),
        ];

        self.timeouts.store(Arc::new(timeouts));
        self.context.store(Arc::new(context));

        Ok(())
    }

    pub fn notify_tls_config_change(&self, config: &ServerTlsConfig) -> Result<(), Error> {
        let acceptor = Arc::new(Self::build_tls_acceptor(config)?);
        self.tls_acceptor.store(acceptor);

        Ok(())
    }

    fn build_tls_acceptor(tls_config: &ServerTlsConfig) -> Result<TlsAcceptor, Error> {
        debug!("Detected TLS configuration");
        let server_certs = load_certificate_bundle(
            &tls_config.server_certificate_bundle,
            "server certificates bundle",
        )?;
        let server_key = load_private_key(&tls_config.server_private_key, "server private key")?;

        let server_config = if let Some(client_ca_bundle) = tls_config.client_ca_bundle.as_ref() {
            debug!("Client CA bundle detected");
            let client_cert_verifier = build_client_verifier(client_ca_bundle)?;
            rustls::ServerConfig::builder()
                .with_client_cert_verifier(client_cert_verifier)
                .with_single_cert(server_certs, server_key)
        } else {
            debug!("No client CA bundle detected");
            rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(server_certs, server_key)
        };

        let server_config = server_config.map_err(|e| {
            Error::Initialization(format!("Failed to create TLS server config: {e}"))
        })?;

        Ok(TlsAcceptor::from(Arc::new(server_config)))
    }

    pub async fn serve(&self) -> Result<(), Error> {
        let connector = TlsConnector {
            tls_acceptor: &self.tls_acceptor,
        };
        accept_loop(
            self.binding_address,
            &connector,
            &self.context,
            &self.timeouts,
        )
        .await
    }
}

#[cfg(test)]
pub mod tests;
