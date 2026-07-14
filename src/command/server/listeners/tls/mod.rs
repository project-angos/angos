use std::{fs, net::SocketAddr, path::Path, sync::Arc};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use rustls::{
    RootCertStore, ServerConfig,
    server::{WebPkiClientVerifier, danger::ClientCertVerifier},
};
use rustls_pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use tokio::net::TcpStream;
use tokio_rustls::TlsAcceptor;
use tracing::debug;

use crate::command::server::{
    ServerContext,
    error::Error,
    listeners::{Connector, HandshakeResult, Listener},
};
pub use crate::configuration::listeners::{ClientAuth, ServerTlsConfig, TlsListenerConfig};

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

fn build_client_verifier(
    client_ca_bundle: &Path,
    mode: ClientAuth,
) -> Result<Arc<dyn ClientCertVerifier>, Error> {
    let client_certs = load_certificate_bundle(client_ca_bundle, "client certificates bundle")?;

    let mut client_cert_store = RootCertStore::empty();
    for client_cert in client_certs {
        client_cert_store.add(client_cert).map_err(|e| {
            Error::Initialization(format!(
                "Failed to add client CA certificate to root cert store: {e}"
            ))
        })?;
    }

    let builder = WebPkiClientVerifier::builder(Arc::new(client_cert_store));

    let verifier = match mode {
        // Accept anonymous clients alongside clients with a valid cert; policy can gate on identity.
        ClientAuth::Optional => builder.allow_unauthenticated(),
        ClientAuth::Required => builder,
    }
    .build()
    .map_err(|e| {
        Error::Initialization(format!(
            "Failed to create TLS client certificate verifier: {e}"
        ))
    })?;

    Ok(verifier)
}

/// Builds a [`TlsAcceptor`] from the server certificate/key and optional client
/// CA bundle. Shared by the connector's initial construction and every reload.
pub fn build_tls_acceptor(tls_config: &ServerTlsConfig) -> Result<TlsAcceptor, Error> {
    debug!("Detected TLS configuration");
    let server_certs = load_certificate_bundle(
        &tls_config.server_certificate_bundle,
        "server certificates bundle",
    )?;
    let server_key = load_private_key(&tls_config.server_private_key, "server private key")?;

    let server_config = if let Some(ca_bundle) = tls_config.client_ca_bundle.as_ref() {
        debug!(
            "Client CA bundle detected (client_auth = {:?})",
            tls_config.client_auth
        );
        let client_cert_verifier = build_client_verifier(ca_bundle, tls_config.client_auth)?;
        ServerConfig::builder()
            .with_client_cert_verifier(client_cert_verifier)
            .with_single_cert(server_certs, server_key)
    } else {
        debug!("No client CA bundle detected; client_auth setting is ignored");
        ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(server_certs, server_key)
    };

    let server_config = server_config
        .map_err(|e| Error::Initialization(format!("Failed to create TLS server config: {e}")))?;

    Ok(TlsAcceptor::from(Arc::new(server_config)))
}

/// The mTLS handshake: it owns the hot-swappable [`TlsAcceptor`] so a config
/// reload re-terminates TLS without rebuilding the listener.
pub struct TlsConnector {
    tls_acceptor: ArcSwap<TlsAcceptor>,
}

impl TlsConnector {
    pub fn new(tls_config: &ServerTlsConfig) -> Result<Self, Error> {
        Ok(Self {
            tls_acceptor: ArcSwap::from_pointee(build_tls_acceptor(tls_config)?),
        })
    }

    /// Swap in an acceptor built from `tls_config`.
    pub fn reload(&self, tls_config: &ServerTlsConfig) -> Result<(), Error> {
        self.tls_acceptor
            .store(Arc::new(build_tls_acceptor(tls_config)?));
        Ok(())
    }
}

#[async_trait]
impl Connector for TlsConnector {
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

/// A TLS/mTLS listener: the shared shell over the [`TlsConnector`].
pub type TlsListener = Listener<TlsConnector>;

impl TlsListener {
    pub fn new(config: &TlsListenerConfig, context: ServerContext) -> Result<Self, Error> {
        let connector = TlsConnector::new(&config.tls)?;
        Ok(Self::build(&config.base, connector, context))
    }

    /// Apply a full listener config reload: rebuild the acceptor, then swap in
    /// the new timeouts and context.
    pub fn notify_config_change(
        &self,
        config: &TlsListenerConfig,
        context: ServerContext,
    ) -> Result<(), Error> {
        self.connector().reload(&config.tls)?;
        self.store_timeouts(&config.base);
        self.store_context(context);
        Ok(())
    }

    /// Apply a TLS-only reload (certificate rotation): rebuild the acceptor.
    pub fn notify_tls_config_change(&self, config: &ServerTlsConfig) -> Result<(), Error> {
        self.connector().reload(config)
    }
}

#[cfg(test)]
pub mod tests;
