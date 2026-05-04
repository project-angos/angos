use std::{net::SocketAddr, path::Path, sync::Arc, time::Duration};

use arc_swap::ArcSwap;
use hyper_util::rt::TokioIo;
use rustls::{
    RootCertStore,
    server::{WebPkiClientVerifier, danger::ClientCertVerifier},
};
use rustls_pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use tokio_rustls::TlsAcceptor;
use tracing::{debug, info};

use crate::command::server::{
    ServerContext,
    error::Error,
    listeners::{accept, build_listener},
    serve_request,
};
pub use crate::configuration::listeners::tls::{ServerTlsConfig, TlsListenerConfig};

fn build_client_verifier(client_ca_bundle: &Path) -> Result<Arc<dyn ClientCertVerifier>, Error> {
    let client_certs: Vec<CertificateDer> = CertificateDer::pem_file_iter(client_ca_bundle)
        .map_err(|e| {
            Error::Initialization(format!("Failed to read client certificates bundle: {e}"))
        })?
        .collect::<Result<_, _>>()
        .map_err(|e| Error::Initialization(format!("Failed to build client certs: {e}")))?;

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

pub struct TlsListener {
    binding_address: SocketAddr,
    tls_acceptor: ArcSwap<TlsAcceptor>,
    context: ArcSwap<ServerContext>,
    timeouts: ArcSwap<[Duration; 2]>,
}

impl TlsListener {
    pub fn new(config: &TlsListenerConfig, context: ServerContext) -> Result<Self, Error> {
        let binding_address = SocketAddr::new(config.bind_address, config.port);
        let tls_acceptor = ArcSwap::from_pointee(Self::build_tls_acceptor(&config.tls)?);
        let timeouts = [
            Duration::from_secs(config.query_timeout),
            Duration::from_secs(config.query_timeout_grace_period),
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
            Duration::from_secs(config.query_timeout),
            Duration::from_secs(config.query_timeout_grace_period),
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
        let server_certs = CertificateDer::pem_file_iter(&tls_config.server_certificate_bundle)
            .map_err(|e| {
                Error::Initialization(format!("Failed to read server certificates bundle: {e}"))
            })?
            .collect::<Result<_, _>>()
            .map_err(|e| Error::Initialization(format!("Failed to build server certs: {e}")))?;
        let server_key =
            PrivateKeyDer::from_pem_file(&tls_config.server_private_key).map_err(|e| {
                Error::Initialization(format!("Failed to read server private key: {e}"))
            })?;

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
        info!("Listening on {} (mTLS)", self.binding_address);
        let listener = build_listener(self.binding_address).await?;

        loop {
            let (tcp, remote_address) = accept(&listener).await?;

            let tls_acceptor = self.tls_acceptor.load();
            let tls_stream = tls_acceptor.accept(tcp).await;
            drop(tls_acceptor);

            let tls = match tls_stream {
                Ok(tls) => tls,
                Err(e) => {
                    debug!("TLS handshake failed from {remote_address}: {e}");
                    continue;
                }
            };

            let (_, session) = tls.get_ref();
            let peer_certificate = session
                .peer_certificates()
                .and_then(|certs| certs.first())
                .map(|cert| cert.to_vec());

            debug!("Accepted connection from {remote_address}");
            let stream = TokioIo::new(tls);
            let context = Arc::clone(&self.context.load());
            let timeouts = Arc::clone(&self.timeouts.load());

            tokio::spawn(Box::pin(serve_request(
                stream,
                context,
                peer_certificate,
                timeouts,
                remote_address,
            )));
        }
    }
}

#[cfg(test)]
pub mod tests {
    use std::{
        io::Write,
        net::{IpAddr, Ipv6Addr},
        path::PathBuf,
        sync::Once,
    };

    use tempfile::NamedTempFile;

    use super::*;
    use crate::{
        command::server::server_context::tests::create_test_server_context,
        test_fixtures::tls::{server_cert_pem, server_key_pem},
    };

    static INIT: Once = Once::new();

    fn init_crypto_provider() {
        INIT.call_once(|| {
            rustls::crypto::aws_lc_rs::default_provider()
                .install_default()
                .ok();
        });
    }

    fn create_temp_cert_file(content: &str) -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(content.as_bytes()).unwrap();
        file.flush().unwrap();
        file
    }

    pub fn build_config(
        client_tls: bool,
    ) -> (
        ServerTlsConfig,
        (NamedTempFile, NamedTempFile, NamedTempFile),
    ) {
        let cert_file = create_temp_cert_file(server_cert_pem());
        let key_file = create_temp_cert_file(server_key_pem());
        let ca_file = create_temp_cert_file(server_cert_pem());

        let server_certificate_bundle = cert_file.path().to_path_buf();
        let server_private_key = key_file.path().to_path_buf();
        let client_ca_bundle = if client_tls {
            Some(ca_file.path().to_path_buf())
        } else {
            None
        };

        (
            ServerTlsConfig {
                server_certificate_bundle,
                server_private_key,
                client_ca_bundle,
            },
            (cert_file, key_file, ca_file),
        )
    }

    #[test]
    fn test_config_default_values() {
        let cert_file = create_temp_cert_file(server_cert_pem());
        let key_file = create_temp_cert_file(server_key_pem());

        let toml = format!(
            r#"
            bind_address = "0.0.0.0"
            [tls]
            server_certificate_bundle = "{}"
            server_private_key = "{}"
        "#,
            cert_file.path().display(),
            key_file.path().display()
        );

        let config: TlsListenerConfig = toml::from_str(&toml).unwrap();

        assert_eq!(config.port, 8000);
        assert_eq!(config.query_timeout, 3600);
        assert_eq!(config.query_timeout_grace_period, 60);
    }

    #[test]
    fn test_config_custom_values() {
        let cert_file = create_temp_cert_file(server_cert_pem());
        let key_file = create_temp_cert_file(server_key_pem());

        let toml = format!(
            r#"
            bind_address = "192.168.1.100"
            port = 9000
            query_timeout = 7200
            query_timeout_grace_period = 120
            [tls]
            server_certificate_bundle = "{}"
            server_private_key = "{}"
        "#,
            cert_file.path().display(),
            key_file.path().display()
        );

        let config: TlsListenerConfig = toml::from_str(&toml).unwrap();

        assert_eq!(config.port, 9000);
        assert_eq!(config.query_timeout, 7200);
        assert_eq!(config.query_timeout_grace_period, 120);
        assert_eq!(
            config.bind_address,
            "192.168.1.100".parse::<IpAddr>().unwrap()
        );
    }

    #[test]
    fn test_config_ipv6_address() {
        let cert_file = create_temp_cert_file(server_cert_pem());
        let key_file = create_temp_cert_file(server_key_pem());

        let toml = format!(
            r#"
            bind_address = "::1"
            port = 8443
            [tls]
            server_certificate_bundle = "{}"
            server_private_key = "{}"
        "#,
            cert_file.path().display(),
            key_file.path().display()
        );

        let config: TlsListenerConfig = toml::from_str(&toml).unwrap();

        assert_eq!(config.bind_address, IpAddr::from(Ipv6Addr::LOCALHOST));
        assert_eq!(config.port, 8443);
    }

    #[test]
    fn test_build_tls_acceptor_success() {
        init_crypto_provider();
        let (tls_config, _tmp_files) = build_config(false);
        let result = TlsListener::build_tls_acceptor(&tls_config);

        assert!(result.is_ok());
    }

    #[test]
    fn test_build_tls_acceptor_with_client_ca() {
        init_crypto_provider();
        let (tls_config, _tmp_files) = build_config(true);

        let result = TlsListener::build_tls_acceptor(&tls_config);

        assert!(result.is_ok());
    }

    #[test]
    fn test_build_tls_acceptor_missing_cert_file() {
        let (mut tls_config, _tmp_files) = build_config(true);
        tls_config.server_certificate_bundle = PathBuf::from("/invalid/path.pem");

        let result = TlsListener::build_tls_acceptor(&tls_config);

        assert!(result.is_err());
        if let Err(Error::Initialization(msg)) = result {
            assert!(msg.contains("Failed to read server certificates bundle"));
        } else {
            panic!("Expected Initialization error");
        }
    }

    #[test]
    fn test_build_tls_acceptor_missing_key_file() {
        let (mut tls_config, _tmp_files) = build_config(true);
        tls_config.server_private_key = PathBuf::from("/invalid/path.pem");

        let result = TlsListener::build_tls_acceptor(&tls_config);

        assert!(result.is_err());
        if let Err(Error::Initialization(msg)) = result {
            assert!(msg.contains("Failed to read server private key"));
        } else {
            panic!("Expected Initialization error");
        }
    }

    #[test]
    fn test_build_tls_acceptor_invalid_cert_format() {
        init_crypto_provider();
        let cert_file = create_temp_cert_file("invalid cert data");

        let (mut tls_config, _tmp_files) = build_config(true);
        tls_config.server_certificate_bundle = cert_file.path().to_path_buf();

        let result = TlsListener::build_tls_acceptor(&tls_config);

        assert!(result.is_err());
    }

    #[test]
    fn test_build_tls_acceptor_invalid_key_format() {
        init_crypto_provider();
        let key_file = create_temp_cert_file("invalid key data");

        let (mut tls_config, _tmp_files) = build_config(true);
        tls_config.server_private_key = key_file.path().to_path_buf();

        let result = TlsListener::build_tls_acceptor(&tls_config);

        assert!(result.is_err());
    }

    #[test]
    fn test_build_tls_acceptor_missing_client_ca_file() {
        let (mut tls_config, _tmp_files) = build_config(false);
        tls_config.client_ca_bundle = Some(PathBuf::from("/nonexistent/ca.pem"));

        let result = TlsListener::build_tls_acceptor(&tls_config);

        assert!(result.is_err());
        if let Err(Error::Initialization(msg)) = result {
            assert!(msg.contains("Failed to read client certificates bundle"));
        } else {
            panic!("Expected Initialization error");
        }
    }

    #[tokio::test]
    async fn test_tls_listener_new() {
        init_crypto_provider();

        let (tls, _temp_files) = build_config(false);

        let config = TlsListenerConfig {
            bind_address: "127.0.0.1".parse().unwrap(),
            port: 8443,
            query_timeout: 3600,
            query_timeout_grace_period: 60,
            tls,
        };

        let context = create_test_server_context().await;
        let result = TlsListener::new(&config, context);

        assert!(result.is_ok());
        let listener = result.unwrap();
        assert_eq!(
            listener.binding_address,
            SocketAddr::from(([127, 0, 0, 1], 8443))
        );
    }

    #[tokio::test]
    async fn test_tls_listener_new_with_ipv6() {
        init_crypto_provider();
        let (tls, _temp_files) = build_config(false);

        let config = TlsListenerConfig {
            bind_address: "::1".parse().unwrap(),
            port: 9443,
            query_timeout: 3600,
            query_timeout_grace_period: 60,
            tls,
        };

        let context = create_test_server_context().await;
        let result = TlsListener::new(&config, context);

        assert!(result.is_ok());
        let listener = result.unwrap();
        assert_eq!(
            listener.binding_address.ip(),
            "::1".parse::<IpAddr>().unwrap()
        );
        assert_eq!(listener.binding_address.port(), 9443);
    }

    #[tokio::test]
    async fn test_tls_listener_new_with_invalid_certs() {
        init_crypto_provider();
        let cert_file = create_temp_cert_file("invalid");
        let key_file = create_temp_cert_file("invalid");

        let (mut tls, _temp_files) = build_config(false);
        tls.server_certificate_bundle = cert_file.path().to_path_buf();
        tls.server_private_key = key_file.path().to_path_buf();

        let config = TlsListenerConfig {
            bind_address: "127.0.0.1".parse().unwrap(),
            port: 8443,
            query_timeout: 3600,
            query_timeout_grace_period: 60,
            tls,
        };

        let context = create_test_server_context().await;
        let result = TlsListener::new(&config, context);

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_tls_listener_notify_config_change() {
        init_crypto_provider();
        let (tls, _temp_files) = build_config(false);

        let config = TlsListenerConfig {
            bind_address: "127.0.0.1".parse().unwrap(),
            port: 8443,
            query_timeout: 3600,
            query_timeout_grace_period: 60,
            tls,
        };

        let context1 = create_test_server_context().await;
        let listener = TlsListener::new(&config, context1).unwrap();

        let context2 = create_test_server_context().await;
        let result = listener.notify_config_change(&config, context2);

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_tls_listener_notify_tls_config_change() {
        init_crypto_provider();
        let (tls, _temp_files) = build_config(false);

        let config = TlsListenerConfig {
            bind_address: "127.0.0.1".parse().unwrap(),
            port: 8443,
            query_timeout: 3600,
            query_timeout_grace_period: 60,
            tls,
        };

        let context = create_test_server_context().await;
        let listener = TlsListener::new(&config, context).unwrap();

        let (tls, _temp_files) = build_config(false);
        let result = listener.notify_tls_config_change(&tls);

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_tls_listener_notify_tls_config_change_with_invalid_certs() {
        init_crypto_provider();

        let (tls, _temp_files) = build_config(false);
        let config = TlsListenerConfig {
            bind_address: "127.0.0.1".parse().unwrap(),
            port: 8443,
            query_timeout: 3600,
            query_timeout_grace_period: 60,
            tls,
        };

        let context = create_test_server_context().await;
        let listener = TlsListener::new(&config, context).unwrap();

        let (mut tls_config, _tmp_files) = build_config(true);
        tls_config.server_certificate_bundle = PathBuf::from("/invalid/path.pem");

        let result = listener.notify_tls_config_change(&tls_config);

        assert!(result.is_err());
    }
}
