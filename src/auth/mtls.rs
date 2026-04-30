use std::sync::Arc;

use async_trait::async_trait;
use hyper::http::request::Parts;
use tracing::{debug, instrument};
use x509_parser::{certificate::X509Certificate, prelude::FromDer};

use super::{AuthMiddleware, AuthResult};
use crate::{
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
    use base64::{Engine, engine::general_purpose::STANDARD};
    use hyper::Request;

    use super::*;

    // Pre-generated RSA-2048 self-signed certificate, base64-encoded DER. Valid 100 years.
    // Subject: CN=test-user, O=TestOrg, O=SecondOrg. Regenerate with:
    //   openssl req -x509 -newkey rsa:2048 -nodes -keyout /dev/null -days 36500 \
    //       -subj "/CN=test-user/O=TestOrg/O=SecondOrg" -outform DER | base64
    const TEST_CERT_DER_B64: &str = "MIIDVzCCAj+gAwIBAgIUHmULkmU2Hbcp1bbIQiXTZ6pv+CYwDQYJKoZIhvcNAQELBQAwOjESMBAGA1UEAwwJdGVzdC11c2VyMRAwDgYDVQQKDAdUZXN0T3JnMRIwEAYDVQQKDAlTZWNvbmRPcmcwIBcNMjYwNDMwMTkyMjQ5WhgPMjEyNjA0MDYxOTIyNDlaMDoxEjAQBgNVBAMMCXRlc3QtdXNlcjEQMA4GA1UECgwHVGVzdE9yZzESMBAGA1UECgwJU2Vjb25kT3JnMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAxxRce9HTV6iOzLVFbvO7i49pJNG4N/wE9wVuON6Y5Rg6mofcObQlYq1wO+u/46Al9JWYAaFDARP/NEXsKIxc2ehnpH4SJQc5Vf9bQS+mOnuSEKKzmjUU1vz0jyF+Rxur514CSpZvU6rvTlx+cn9BWUH33qgk6aYt/gUDgHNJlRFXovYw4C2xMNhr9uG4tlE9mYUsiWyVYV0116JyEp8gjBEl7p6dwljb1YGQZQtrRbxgLkFSKllRv1xLVnEP9yaihTn03jznF34VJpmvPPcvdIRT/MnRqDXkA13VhVshAhcgwD+jS+C1SGGkzDmi+8r1EHmPj96fbB1mwjqfJPnfAwIDAQABo1MwUTAdBgNVHQ4EFgQUB2RMQ0tmakSMOjYxJUnAqy87EjIwHwYDVR0jBBgwFoAUB2RMQ0tmakSMOjYxJUnAqy87EjIwDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEAVzO6ne+HaF7Cx8TEyXo6ZRte7+MR/Ajp+7FJqOtWUESMQwSaYq7u/XqR2ZxCs7MjFrQXmJ9EZbqgWJi1rGjHe1ych+6cA4aZGPl91MqFOs8GH2QZV+yUh68fDiFFJ4FUZ2vHCc7na3QUUyMAavApzKJ8Nc5yYAYa1o3wbGlHKYGjktWjllYM3O4Ekq4UFwH3/Iye6+UaZQa/0p2Bs2OAFQEPBqie8B+cXQ3y+WUoFHaMozrUGgd31Fpf2jV0Mp46vDashe/wQXKGL8Yipi54vicvIpZX4hEUb/umDSLvwhX9Aeqb4nKHPLTm1mW23sJHCBhg26RlH+sDNf/dURxlkA==";

    // Same recipe but with `-subj "/"` for an empty-subject certificate.
    const TEST_MINIMAL_CERT_DER_B64: &str = "MIIC4zCCAcugAwIBAgIUVrZ4zPRzLjG5S1tXP/3QFEncOVEwDQYJKoZIhvcNAQELBQAwADAgFw0yNjA0MzAxOTIyNTNaGA8yMTI2MDQwNjE5MjI1M1owADCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAMy4pZXVXfaXsnTAA093TCoW+FS85mwKSaql97+BqzQhgq0EYSXMnNwLFIaYacuDtTCeTUZlMwNsfdCY7J5dTaTJEIgeaa1vXllXAy+iLaIh3ZPIQLn3HnW51HHGw1BlHxlEwyYfHSocxtXH8GQdgEt4HjBWpJPGl/VC4PP6nD1f4xVRVTtVpUpc01+3D8eQw2WJ4nP8s6wOgdG5Q4RAllLGo/Pmcg3BJmlz0mf+GpGGb+zYYSar9oWVKL9Ddsj8cQLvzswTmSAW7NF5PwfhPg6J6x2QLqhXolDHWd10g4uAWpYDzEbFyNOpxZ3S7hLJYqY+VgJZa1815rkD5ouEaDsCAwEAAaNTMFEwHQYDVR0OBBYEFKXr4W+le2mTkghIGKW81H71hobKMB8GA1UdIwQYMBaAFKXr4W+le2mTkghIGKW81H71hobKMA8GA1UdEwEB/wQFMAMBAf8wDQYJKoZIhvcNAQELBQADggEBAEmaD9queRFSzopsVie+d6cQGUqcnxG3aZHqda0ebMQfZjDJ2KpffhWqf6Nw9qdi6zNuwknGrgAuUUz/sgmF/13OaQnAXredzmymZKrbN/qSz/x05rHm63tcHV0jIVseKS6cmEdY7jMjpaTBxDMRf3621sa/4QJkeOHsv5mf92lZjPDCz5CVwFNFQ4vF9BvYesNvmqxIij1l6TCX8mJjZ9pF45j/4l+kmxo25cr+vSslIQcXkOZbAkGEzBzAMzDb1CWOngFltV4T0DpKsXLqnNIx18kbtDcBYJi9aX8iHElHNPEF6qc2qFOuerbhI0r3NvN2uG7ElkfZzWGgS8EV1Pg=";

    fn test_cert_der() -> Vec<u8> {
        STANDARD.decode(TEST_CERT_DER_B64).expect("valid base64")
    }

    fn test_minimal_cert_der() -> Vec<u8> {
        STANDARD
            .decode(TEST_MINIMAL_CERT_DER_B64)
            .expect("valid base64")
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
        let peer_cert = PeerCertificate(Arc::new(test_cert_der()));

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
        let peer_cert = PeerCertificate(Arc::new(test_minimal_cert_der()));

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
        let der = test_cert_der();
        let (_, cert) = X509Certificate::from_der(&der).unwrap();

        let result = MtlsValidator::extract_certificate_identity(&cert);

        assert!(result.is_ok());
        let cert_info = result.unwrap();
        assert_eq!(cert_info.common_names, vec!["test-user"]);
        assert_eq!(cert_info.organizations, vec!["TestOrg", "SecondOrg"]);
    }

    #[test]
    fn test_extract_certificate_identity_minimal() {
        let der = test_minimal_cert_der();
        let (_, cert) = X509Certificate::from_der(&der).unwrap();

        let result = MtlsValidator::extract_certificate_identity(&cert);

        assert!(result.is_ok());
        let cert_info = result.unwrap();
        assert!(cert_info.common_names.is_empty());
        assert!(cert_info.organizations.is_empty());
    }
}
