use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use hyper_util::rt::TokioIo;
use rustls::RootCertStore;
use rustls::server::WebPkiClientVerifier;
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use serde::Deserialize;
use tokio_rustls::TlsAcceptor;
use tracing::{debug, info};

use crate::command::server::ServerContext;
use crate::command::server::error::Error;
use crate::command::server::listeners::{accept, build_listener};
use crate::command::server::serve_request;

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    pub bind_address: IpAddr,
    #[serde(default = "Config::default_port")]
    pub port: u16,
    #[serde(default = "Config::default_query_timeout")]
    pub query_timeout: u64,
    #[serde(default = "Config::default_query_timeout_grace_period")]
    pub query_timeout_grace_period: u64,
    pub tls: ServerTlsConfig,
}

impl Config {
    fn default_port() -> u16 {
        8000
    }

    fn default_query_timeout() -> u64 {
        3600
    }

    fn default_query_timeout_grace_period() -> u64 {
        60
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct ServerTlsConfig {
    pub server_certificate_bundle: PathBuf,
    pub server_private_key: PathBuf,
    pub client_ca_bundle: Option<PathBuf>,
}

pub struct TlsListener {
    binding_address: SocketAddr,
    tls_acceptor: ArcSwap<TlsAcceptor>,
    context: ArcSwap<ServerContext>,
    timeouts: ArcSwap<[Duration; 2]>,
}

impl TlsListener {
    pub fn new(config: &Config, context: ServerContext) -> Result<Self, Error> {
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
        config: &Config,
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
                let msg = format!("Failed to read server certificates bundle: {e}");
                Error::Initialization(msg)
            })?
            .collect::<Result<_, _>>()
            .map_err(|e| {
                let msg = format!("Failed to build server certs: {e}");
                Error::Initialization(msg)
            })?;
        let server_key =
            PrivateKeyDer::from_pem_file(&tls_config.server_private_key).map_err(|e| {
                let msg = format!("Failed to read server private key: {e}");
                Error::Initialization(msg)
            })?;

        let server_config = if let Some(client_ca_bundle) = tls_config.client_ca_bundle.as_ref() {
            debug!("Client CA bundle detected (will serve with TLS client authentication)");
            let client_certs: Vec<CertificateDer> = CertificateDer::pem_file_iter(client_ca_bundle)
                .map_err(|e| {
                    let msg = format!("Failed to read client certificates bundle: {e}");
                    Error::Initialization(msg)
                })?
                .collect::<Result<_, _>>()
                .map_err(|e| {
                    let msg = format!("Failed to build client certs: {e}");
                    Error::Initialization(msg)
                })?;

            let mut client_cert_store = RootCertStore::empty();
            for client_cert in client_certs {
                if let Err(err) = client_cert_store.add(client_cert) {
                    let msg =
                        format!("Failed to add client CA certificate to root cert store: {err}");
                    return Err(Error::Initialization(msg));
                }
            }

            let Ok(client_cert_verifier) =
                WebPkiClientVerifier::builder(Arc::new(client_cert_store))
                    .allow_unauthenticated()
                    .build()
            else {
                let msg = "Failed to create TLS client certificate verifier".to_string();
                return Err(Error::Initialization(msg));
            };

            rustls::ServerConfig::builder()
                .with_client_cert_verifier(client_cert_verifier)
                .with_single_cert(server_certs, server_key)
        } else {
            debug!("No client CA bundle detected (will serve without TLS client authentication)");

            rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(server_certs, server_key)
        };

        let server_config = match server_config {
            Ok(cfg) => cfg,
            Err(err) => {
                let msg = format!("Failed to create TLS server config: {err}");
                return Err(Error::Initialization(msg));
            }
        };

        let server_config = Arc::new(server_config);
        let tls_acceptor = TlsAcceptor::from(server_config);
        Ok(tls_acceptor)
    }

    pub async fn serve(&self) -> Result<(), Error> {
        info!("Listening on {} (mTLS)", self.binding_address);
        let listener = build_listener(self.binding_address).await?;

        loop {
            let (tcp, remote_address) = accept(&listener).await?;

            let tls_acceptor = self.tls_acceptor.load();
            let tls_stream = tls_acceptor.accept(tcp).await;
            drop(tls_acceptor);

            let Ok(tls) = tls_stream else {
                continue;
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
    use std::io::Write;
    use std::net::Ipv6Addr;
    use std::sync::Once;

    use tempfile::NamedTempFile;

    use super::*;
    use crate::command::server::server_context::tests::create_test_server_context;

    static INIT: Once = Once::new();

    fn init_crypto_provider() {
        INIT.call_once(|| {
            rustls::crypto::aws_lc_rs::default_provider()
                .install_default()
                .ok();
        });
    }

    const TEST_SERVER_CERT: &str = r"-----BEGIN CERTIFICATE-----
MIIDgjCCAmqgAwIBAgIUFCYlDkKrxnJCnCtYXKvA9BaXnfowDQYJKoZIhvcNAQEL
BQAwWDELMAkGA1UEBhMCTFUxCzAJBgNVBAgMAkxVMRMwEQYDVQQHDApMdXhlbWJv
dXJnMRMwEQYDVQQKDApNeSBDb21wYW55MRIwEAYDVQQDDAlTZXJ2ZXIgQ0EwHhcN
MjUxMDA5MTcxNjIyWhcNMjYxMDA5MTcxNjIyWjBaMQswCQYDVQQGEwJMVTELMAkG
A1UECAwCTFUxEzARBgNVBAcMCkx1eGVtYm91cmcxEzARBgNVBAoMCk15IENvbXBh
bnkxFDASBgNVBAMMC2V4YW1wbGUuY29tMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8A
MIIBCgKCAQEAp4FMkW8y3+ZJDM1gZTSGpYk7WPHzv+eQOnWxcVif++j4EKxyIX3y
fKm11GokU7eKIbbUGcDEzBgh5V+VQoiweBC/S4mag86JCESX5dFv1jQ+KnjP6BkW
4bATqWwUwqUX/tXn3Oe/gTue64cU3nl7y6xOgX/jUF93GzVNS69Rz9E5DszeN1kw
zmh8dq88CZrReZ+nrQNFNmxFooqi/6bgnV8YlFfYT5ide+8LY+8Yho3ZcJ9cv530
TCCpX2xMfhqGFhfnVyR+Raj0/EU6PArIM+bXCw5a9llnU4ZQJBiaG6N0gSrPTHw6
kZyi9UE5KA4TwOtFcscFC/Rhm7pqY4z7mQIDAQABo0IwQDAdBgNVHQ4EFgQUgSvE
fVmU14s8Z4zAx3zv0x09pQMwHwYDVR0jBBgwFoAUCRVUTFXrNWkUWA8CwKljxF4R
FlgwDQYJKoZIhvcNAQELBQADggEBAFYCZiW1zpZAty9YFg/yNL2xw4XuDxJyvapT
4yd9LVhdIhNLSJo5dOsZynEFXOmvLpjEgfSRMAI0MhdqdqAjaDr2Wfg0P4VqfkC5
3BoRkwZ4sFDu9r7jiKvZplBO9qln+LxS20YFme1TpjzWzzCy1v/40xVF0PGONmiq
fTmTCQdUw11s7r6NwQPgrpJuyAX5iAY0MKccHMej5cnMy3HyjeCsByKdBqxOb+X4
IBcx+tr+Vvs6YWA7pd2UB6GbRbMgmELwVqkMFi6P7mzJv2PXsabzLzdSD41Xh/rL
pJ1J56iviNUViU6cY4Yy/Q9qe8aifhXXgaRgu5r8oBARAWo5LiE=
-----END CERTIFICATE-----";

    const TEST_SERVER_KEY: &str = r"-----BEGIN PRIVATE KEY-----
MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCngUyRbzLf5kkM
zWBlNIaliTtY8fO/55A6dbFxWJ/76PgQrHIhffJ8qbXUaiRTt4ohttQZwMTMGCHl
X5VCiLB4EL9LiZqDzokIRJfl0W/WND4qeM/oGRbhsBOpbBTCpRf+1efc57+BO57r
hxTeeXvLrE6Bf+NQX3cbNU1Lr1HP0TkOzN43WTDOaHx2rzwJmtF5n6etA0U2bEWi
iqL/puCdXxiUV9hPmJ177wtj7xiGjdlwn1y/nfRMIKlfbEx+GoYWF+dXJH5FqPT8
RTo8Csgz5tcLDlr2WWdThlAkGJobo3SBKs9MfDqRnKL1QTkoDhPA60VyxwUL9GGb
umpjjPuZAgMBAAECggEAM+nzaJmdm138Yu7eAs/KMVC8Z38pa98hSLUERxxQDRtx
kn1XrACkYDWa6Jfy5U1bjsJirw6bD6QlETlUAbhqdPO/zfH6RQKEXt8fcrc74Zf0
cmZaDVWFTXIsTdR1BLR1IOJ7x2+93xgy001czuy4zPF8rU8NDPJhcDnPWHcgNHdj
QK4gz9pGARHyj68tW2nPnBdq1pPQcIelJ2BRM23R+uG/a2JWYztk5Rj0sbL7O83U
ly0i4aMqvndFV1QqGMLK45NUcaQ1132VPtGK8uXLDPcx59BnZhsDWKdL4BSnc8j6
Zv5EJipdxcBb6hDptlxru48KrDiV2Tr0LK2+1Q11TwKBgQDi/g72bTQiyss1wFWj
FYPlDQRFzpSxI8zq6edrYr0X6y+7kqpzHtScKdY9Y9vd2ajT5ygCJHFgHy0JSvf0
C2+CjbNjTd9wOi6RLsQc8wP3mLV2+ksw1GY9y+7NDD2oAfh1xoFAUfkEkszvyWP4
GMoPAYikIntU7S4QmcHSuqEI6wKBgQC86SR7EGcptnSDowz8QcH5bRq+WTvxXgXl
T6yhmCYUyc7KPB4QBsQ+Ny7/AgBTEpFdrK803GXNFGFswz+wLUk7gExd6Q8eF9ZC
89Qc2EowRxKcey8gCxGms4BW/pDI/nuPBEJu6mYFg4UyhSHTzRAnNNRRoTiCtIJT
taNFhI5siwKBgAs1SD/avIxLPyV0TcYztcOFlQRtYuIsRl4DFq4yveuEbWLxpwDw
MMdVOYLmf2DA8pkj/NG+QurgHzUqQnwGZIcpwAXTPokoFkyM5poXVcbP/4XUbgoH
MtUyUKRHSnQVRNNr7c3jPkx/gycD5q/FaZS8GqcgHL7gxzmCnhNtq63pAoGAOLIs
HbcljxJQU7WazxaZNde14Az9/Ym/cTwBTppQS4rpwi5aw2qUeSussiISoNwAvsF1
8AJ5lxwXxUGwGprs0KvHv6OTwu9agcuWDHYpheW+wzIBSbeou4RB71oFcB1YTer0
WT+GEP+Q+UGVjnCL+YhUdI9TW377Yk4wS0vmRtMCgYBIj9PI5TKYtafptgLpga3t
o7QwZTWY2S2Al2epdI8+Zx3O7qAVVdN5ynoRnXuhSV4zL6U0wYqo6K2C/Jr1ErDF
MyMgL1gcqJFWmtfn1TE+IPz1HEN3GXgDI9PYOV63PrTQg3ZU9ixvj2wJYbT7xNWy
PpGhlTQXVV6Evtahtp+cRw==
-----END PRIVATE KEY-----";

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
        let cert_file = create_temp_cert_file(TEST_SERVER_CERT);
        let key_file = create_temp_cert_file(TEST_SERVER_KEY);
        let ca_file = create_temp_cert_file(TEST_SERVER_CERT);

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
        let cert_file = create_temp_cert_file(TEST_SERVER_CERT);
        let key_file = create_temp_cert_file(TEST_SERVER_KEY);

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

        let config: Config = toml::from_str(&toml).unwrap();

        assert_eq!(config.port, 8000);
        assert_eq!(config.query_timeout, 3600);
        assert_eq!(config.query_timeout_grace_period, 60);
    }

    #[test]
    fn test_config_custom_values() {
        let cert_file = create_temp_cert_file(TEST_SERVER_CERT);
        let key_file = create_temp_cert_file(TEST_SERVER_KEY);

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

        let config: Config = toml::from_str(&toml).unwrap();

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
        let cert_file = create_temp_cert_file(TEST_SERVER_CERT);
        let key_file = create_temp_cert_file(TEST_SERVER_KEY);

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

        let config: Config = toml::from_str(&toml).unwrap();

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

    #[test]
    fn test_tls_listener_new() {
        init_crypto_provider();

        let (tls, _temp_files) = build_config(false);

        let config = Config {
            bind_address: "127.0.0.1".parse().unwrap(),
            port: 8443,
            query_timeout: 3600,
            query_timeout_grace_period: 60,
            tls,
        };

        let context = create_test_server_context();
        let result = TlsListener::new(&config, context);

        assert!(result.is_ok());
        let listener = result.unwrap();
        assert_eq!(
            listener.binding_address,
            SocketAddr::from(([127, 0, 0, 1], 8443))
        );
    }

    #[test]
    fn test_tls_listener_new_with_ipv6() {
        init_crypto_provider();
        let (tls, _temp_files) = build_config(false);

        let config = Config {
            bind_address: "::1".parse().unwrap(),
            port: 9443,
            query_timeout: 3600,
            query_timeout_grace_period: 60,
            tls,
        };

        let context = create_test_server_context();
        let result = TlsListener::new(&config, context);

        assert!(result.is_ok());
        let listener = result.unwrap();
        assert_eq!(
            listener.binding_address.ip(),
            "::1".parse::<IpAddr>().unwrap()
        );
        assert_eq!(listener.binding_address.port(), 9443);
    }

    #[test]
    fn test_tls_listener_new_with_invalid_certs() {
        init_crypto_provider();
        let cert_file = create_temp_cert_file("invalid");
        let key_file = create_temp_cert_file("invalid");

        let (mut tls, _temp_files) = build_config(false);
        tls.server_certificate_bundle = cert_file.path().to_path_buf();
        tls.server_private_key = key_file.path().to_path_buf();

        let config = Config {
            bind_address: "127.0.0.1".parse().unwrap(),
            port: 8443,
            query_timeout: 3600,
            query_timeout_grace_period: 60,
            tls,
        };

        let context = create_test_server_context();
        let result = TlsListener::new(&config, context);

        assert!(result.is_err());
    }

    #[test]
    fn test_tls_listener_notify_config_change() {
        init_crypto_provider();
        let (tls, _temp_files) = build_config(false);

        let config = Config {
            bind_address: "127.0.0.1".parse().unwrap(),
            port: 8443,
            query_timeout: 3600,
            query_timeout_grace_period: 60,
            tls,
        };

        let context1 = create_test_server_context();
        let listener = TlsListener::new(&config, context1).unwrap();

        let context2 = create_test_server_context();
        let result = listener.notify_config_change(&config, context2);

        assert!(result.is_ok());
    }

    #[test]
    fn test_tls_listener_notify_tls_config_change() {
        init_crypto_provider();
        let (tls, _temp_files) = build_config(false);

        let config = Config {
            bind_address: "127.0.0.1".parse().unwrap(),
            port: 8443,
            query_timeout: 3600,
            query_timeout_grace_period: 60,
            tls,
        };

        let context = create_test_server_context();
        let listener = TlsListener::new(&config, context).unwrap();

        let (tls, _temp_files) = build_config(false);
        let result = listener.notify_tls_config_change(&tls);

        assert!(result.is_ok());
    }

    #[test]
    fn test_tls_listener_notify_tls_config_change_with_invalid_certs() {
        init_crypto_provider();

        let (tls, _temp_files) = build_config(false);
        let config = Config {
            bind_address: "127.0.0.1".parse().unwrap(),
            port: 8443,
            query_timeout: 3600,
            query_timeout_grace_period: 60,
            tls,
        };

        let context = create_test_server_context();
        let listener = TlsListener::new(&config, context).unwrap();

        let (mut tls_config, _tmp_files) = build_config(true);
        tls_config.server_certificate_bundle = PathBuf::from("/invalid/path.pem");

        let result = listener.notify_tls_config_change(&tls_config);

        assert!(result.is_err());
    }
}
