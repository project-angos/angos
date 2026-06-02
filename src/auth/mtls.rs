use std::sync::Arc;

use async_trait::async_trait;
use hyper::http::request::Parts;
use tracing::{debug, error, instrument};
use x509_parser::{certificate::X509Certificate, prelude::FromDer};

use crate::{
    auth::{AuthMiddleware, AuthResult},
    command::server::Error,
    identity::{ClientCertificate, ClientIdentity},
};

/// Extension type for passing peer certificate data from TLS layer
#[derive(Clone)]
pub struct PeerCertificate(pub Arc<Vec<u8>>);

/// mTLS certificate-based authentication validator
///
/// Note: Certificate validation (expiry, CA trust chain, etc.) is performed by the TLS layer
/// during the handshake. This middleware only extracts identity from already-validated certificates.
/// Invalid certificates are rejected at the TLS layer before reaching this middleware.
pub struct MtlsValidator;

impl MtlsValidator {
    pub fn new() -> Self {
        Self
    }

    /// Extract certificate identity information from X509 certificate
    #[instrument(skip(cert))]
    fn extract_certificate_identity(cert: &X509Certificate) -> Result<ClientCertificate, Error> {
        let subject = cert.subject();

        let organizations = subject
            .iter_organization()
            .filter_map(|o| o.as_str().ok().map(String::from))
            .collect::<Vec<_>>();

        let common_names = subject
            .iter_common_name()
            .filter_map(|cn| cn.as_str().ok().map(String::from))
            .collect::<Vec<_>>();

        Ok(ClientCertificate {
            organizations,
            common_names,
        })
    }
}

impl Default for MtlsValidator {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl AuthMiddleware for MtlsValidator {
    #[instrument(skip(self, parts, identity))]
    async fn authenticate(
        &self,
        parts: &Parts,
        identity: &mut ClientIdentity,
    ) -> Result<AuthResult, Error> {
        let Some(peer_cert) = parts.extensions.get::<PeerCertificate>() else {
            return Ok(AuthResult::NoCredentials);
        };

        let (_, cert) = X509Certificate::from_der(&peer_cert.0).map_err(|e| {
            error!(
                error = ?e,
                certificate_len = peer_cert.0.len(),
                "Failed to parse client certificate"
            );
            Error::Unauthorized("Invalid certificate".to_string())
        })?;

        debug!("Extracting identity from client certificate");
        let cert_info = Self::extract_certificate_identity(&cert)
            .inspect_err(|e| debug!("Failed to extract identity from certificate: {e}"))?;

        identity.certificate = cert_info;
        Ok(AuthResult::Authenticated)
    }
}

#[cfg(test)]
pub mod tests {
    use std::{
        io::{self, Write},
        sync::{Arc, Mutex},
    };

    use hyper::{Request, StatusCode};
    use tracing::Level;
    use tracing_subscriber::fmt::{MakeWriter, format::FmtSpan};

    use super::*;
    use crate::test_fixtures::mtls::{cert_der, minimal_cert_der};

    #[derive(Clone, Default)]
    struct LogCapture(Arc<Mutex<Vec<u8>>>);

    struct LogWriter(Arc<Mutex<Vec<u8>>>);

    impl LogCapture {
        fn contents(&self) -> String {
            let bytes = self.0.lock().unwrap().clone();
            String::from_utf8(bytes).unwrap()
        }
    }

    impl Write for LogWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for LogCapture {
        type Writer = LogWriter;

        fn make_writer(&'a self) -> Self::Writer {
            LogWriter(Arc::clone(&self.0))
        }
    }

    #[tokio::test]
    async fn test_authenticate_no_certificate() {
        let validator = MtlsValidator::new();
        let request = Request::builder().body(()).unwrap();
        let (parts, ()) = request.into_parts();
        let mut identity = ClientIdentity::new(None);

        let result = validator.authenticate(&parts, &mut identity).await;

        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), AuthResult::NoCredentials));
        assert!(identity.certificate.common_names.is_empty());
        assert!(identity.certificate.organizations.is_empty());
    }

    #[tokio::test]
    async fn test_authenticate_with_valid_certificate() {
        let validator = MtlsValidator::new();
        let peer_cert = PeerCertificate(Arc::new(cert_der()));

        let mut request = Request::builder().body(()).unwrap();
        request.extensions_mut().insert(peer_cert);
        let (parts, ()) = request.into_parts();

        let mut identity = ClientIdentity::new(None);

        let result = validator.authenticate(&parts, &mut identity).await;

        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), AuthResult::Authenticated));
        assert_eq!(identity.certificate.common_names, vec!["test-user"]);
        assert_eq!(identity.certificate.organizations, vec!["TestOrg"]);
    }

    #[tokio::test]
    async fn test_authenticate_with_minimal_certificate() {
        let validator = MtlsValidator::new();
        let peer_cert = PeerCertificate(Arc::new(minimal_cert_der()));

        let mut request = Request::builder().body(()).unwrap();
        request.extensions_mut().insert(peer_cert);
        let (parts, ()) = request.into_parts();

        let mut identity = ClientIdentity::new(None);

        let result = validator.authenticate(&parts, &mut identity).await;

        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), AuthResult::Authenticated));
        assert!(identity.certificate.common_names.is_empty());
        assert!(identity.certificate.organizations.is_empty());
    }

    #[tokio::test]
    async fn test_authenticate_with_malformed_certificate() {
        let validator = MtlsValidator::new();
        let invalid_cert = vec![0u8; 100];
        let peer_cert = PeerCertificate(Arc::new(invalid_cert));

        let mut request = Request::builder().body(()).unwrap();
        request.extensions_mut().insert(peer_cert);
        let (parts, ()) = request.into_parts();

        let mut identity = ClientIdentity::new(None);

        let result = validator.authenticate(&parts, &mut identity).await;

        assert!(result.is_err());
        match result.unwrap_err() {
            Error::Unauthorized(msg) => {
                let error = Error::Unauthorized(msg);
                assert_eq!(error.status_code(), StatusCode::UNAUTHORIZED);
                let body = error.as_json(None).to_string();
                assert!(body.contains("Invalid certificate"));
                assert!(!body.contains("UnexpectedTag"));
                assert!(!body.contains("Malformed client certificate"));
            }
            err => panic!("expected Unauthorized, got {err:?}"),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn malformed_certificate_logs_details_without_exposing_them_to_client() {
        let log_capture = LogCapture::default();
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(Level::TRACE)
            .with_writer(log_capture.clone())
            .with_span_events(FmtSpan::ENTER)
            .with_ansi(false)
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let validator = MtlsValidator::new();
        let peer_cert = PeerCertificate(Arc::new(vec![0u8; 100]));

        let mut request = Request::builder().body(()).unwrap();
        request.extensions_mut().insert(peer_cert);
        let (parts, ()) = request.into_parts();

        let mut identity = ClientIdentity::new(None);
        let result = validator.authenticate(&parts, &mut identity).await;

        match result.unwrap_err() {
            Error::Unauthorized(msg) => assert_eq!(msg, "Invalid certificate"),
            err => panic!("expected Unauthorized, got {err:?}"),
        }

        let logs = log_capture.contents();
        assert!(
            logs.contains("Failed to parse client certificate"),
            "logs were: {logs}"
        );
        assert!(logs.contains("error="), "logs were: {logs}");
        assert!(logs.contains("authenticate"), "logs were: {logs}");
        assert!(!logs.contains("Malformed client certificate"));
    }

    #[test]
    fn test_extract_certificate_identity() {
        let der = cert_der();
        let (_, cert) = X509Certificate::from_der(&der).unwrap();

        let result = MtlsValidator::extract_certificate_identity(&cert);

        assert!(result.is_ok());
        let cert_info = result.unwrap();
        assert_eq!(cert_info.common_names, vec!["test-user"]);
        assert_eq!(cert_info.organizations, vec!["TestOrg"]);
    }

    #[test]
    fn test_extract_certificate_identity_minimal() {
        let der = minimal_cert_der();
        let (_, cert) = X509Certificate::from_der(&der).unwrap();

        let result = MtlsValidator::extract_certificate_identity(&cert);

        assert!(result.is_ok());
        let cert_info = result.unwrap();
        assert!(cert_info.common_names.is_empty());
        assert!(cert_info.organizations.is_empty());
    }
}
