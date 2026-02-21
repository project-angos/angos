use std::sync::Arc;

use async_trait::async_trait;
use hyper::http::request::Parts;
use tracing::{debug, instrument};
use x509_parser::{certificate::X509Certificate, prelude::FromDer};

use super::{AuthMiddleware, AuthResult};
use crate::{
    command::server::error::Error,
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
            debug!("Failed to parse client certificate: {:?}", e);
            Error::Unauthorized(format!("Malformed client certificate: {e:?}"))
        })?;

        debug!("Extracting identity from client certificate");
        let cert_info = Self::extract_certificate_identity(&cert)
            .inspect_err(|e| debug!("Failed to extract identity from certificate: {e}"))?;

        identity.certificate = cert_info;
        Ok(AuthResult::Authenticated)
    }
}

#[cfg(test)]
mod tests {
    use hyper::Request;

    use super::*;

    fn generate_test_certificate() -> Vec<u8> {
        use std::process::Command;

        let output = Command::new("openssl")
            .args([
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-keyout",
                "/dev/null",
                "-out",
                "/dev/stdout",
                "-days",
                "1",
                "-subj",
                "/CN=test-user/O=TestOrg/O=SecondOrg",
                "-outform",
                "DER",
            ])
            .output()
            .expect("Failed to generate test certificate");

        assert!(
            output.status.success(),
            "OpenSSL failed to generate certificate"
        );
        output.stdout
    }

    fn generate_minimal_certificate() -> Vec<u8> {
        use std::process::Command;

        let output = Command::new("openssl")
            .args([
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-keyout",
                "/dev/null",
                "-out",
                "/dev/stdout",
                "-days",
                "1",
                "-subj",
                "/",
                "-outform",
                "DER",
            ])
            .output()
            .expect("Failed to generate minimal certificate");

        assert!(
            output.status.success(),
            "OpenSSL failed to generate certificate"
        );
        output.stdout
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
        let cert_der = generate_test_certificate();
        let peer_cert = PeerCertificate(Arc::new(cert_der));

        let mut request = Request::builder().body(()).unwrap();
        request.extensions_mut().insert(peer_cert);
        let (parts, ()) = request.into_parts();

        let mut identity = ClientIdentity::new(None);

        let result = validator.authenticate(&parts, &mut identity).await;

        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), AuthResult::Authenticated));
        assert_eq!(identity.certificate.common_names, vec!["test-user"]);
        assert_eq!(
            identity.certificate.organizations,
            vec!["TestOrg", "SecondOrg"]
        );
    }

    #[tokio::test]
    async fn test_authenticate_with_minimal_certificate() {
        let validator = MtlsValidator::new();
        let cert_der = generate_minimal_certificate();
        let peer_cert = PeerCertificate(Arc::new(cert_der));

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
        assert!(matches!(result.unwrap_err(), Error::Unauthorized(_)));
    }

    #[test]
    fn test_extract_certificate_identity() {
        let cert_der = generate_test_certificate();
        let (_, cert) = X509Certificate::from_der(&cert_der).unwrap();

        let result = MtlsValidator::extract_certificate_identity(&cert);

        assert!(result.is_ok());
        let cert_info = result.unwrap();
        assert_eq!(cert_info.common_names, vec!["test-user"]);
        assert_eq!(cert_info.organizations, vec!["TestOrg", "SecondOrg"]);
    }

    #[test]
    fn test_extract_certificate_identity_minimal() {
        let cert_der = generate_minimal_certificate();
        let (_, cert) = X509Certificate::from_der(&cert_der).unwrap();

        let result = MtlsValidator::extract_certificate_identity(&cert);

        assert!(result.is_ok());
        let cert_info = result.unwrap();
        assert!(cert_info.common_names.is_empty());
        assert!(cert_info.organizations.is_empty());
    }
}
