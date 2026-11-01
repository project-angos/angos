use std::sync::Arc;

use async_trait::async_trait;
use http::request::Parts;
use tracing::{debug, error, instrument};
use x509_parser::certificate::X509Certificate;
use x509_parser::prelude::FromDer;

use crate::auth::{AuthMiddleware, AuthResult, Error};
use crate::identity::{ClientCertificate, ClientIdentity};

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
    /// The organizations and common names of a certificate subject. Entries
    /// that are not valid UTF-8 are dropped, so this cannot fail.
    #[instrument(skip(cert))]
    fn extract_certificate_identity(cert: &X509Certificate) -> ClientCertificate {
        let subject = cert.subject();

        let organizations = subject
            .iter_organization()
            .filter_map(|o| o.as_str().ok().map(String::from))
            .collect::<Vec<_>>();

        let common_names = subject
            .iter_common_name()
            .filter_map(|cn| cn.as_str().ok().map(String::from))
            .collect::<Vec<_>>();

        ClientCertificate {
            organizations,
            common_names,
        }
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

        // The TLS layer already accepted this certificate, so a parse failure is
        // two parsers disagreeing rather than a bad credential. Carrying on
        // without certificate identity leaves the refusal to a policy that needs
        // one, which is where it belongs.
        let cert = match X509Certificate::from_der(&peer_cert.0) {
            Ok((_, cert)) => cert,
            Err(e) => {
                error!(
                    error = ?e,
                    certificate_len = peer_cert.0.len(),
                    "Failed to parse client certificate"
                );
                return Ok(AuthResult::NoCredentials);
            }
        };

        debug!("Extracting identity from client certificate");
        let cert_info = Self::extract_certificate_identity(&cert);

        identity.certificate = cert_info;
        Ok(AuthResult::Authenticated)
    }
}

#[cfg(test)]
pub mod tests {
    use std::sync::Arc;

    use tokio::runtime::Builder;
    use tracing::Level;

    use super::*;
    use crate::test_fixtures::logging::LogCapture;
    use crate::test_fixtures::mtls::{cert_der, minimal_cert_der};
    use crate::test_fixtures::requests::empty_parts;

    /// This must remain the ONLY test driving the parse-error branch: `tracing` caches callsite
    /// interest process-globally, so a second test under a non-capturing
    /// subscriber could cache the `error!` as disabled and make the log
    /// assertion flaky.
    #[test]
    fn malformed_certificate_is_logged_and_yields_no_identity() {
        let capture = LogCapture::default();
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(Level::DEBUG)
            .with_writer(capture.clone())
            .with_ansi(false)
            .finish();

        let (result, identity) = tracing::subscriber::with_default(subscriber, || {
            let runtime = Builder::new_current_thread().build().unwrap();
            runtime.block_on(async {
                let validator = MtlsValidator;
                let mut parts = empty_parts();
                parts
                    .extensions
                    .insert(PeerCertificate(Arc::new(vec![0u8; 100])));
                let mut identity = ClientIdentity::new(None);
                let result = validator.authenticate(&parts, &mut identity).await;
                (result, identity)
            })
        });

        assert!(matches!(result.unwrap(), AuthResult::NoCredentials));
        assert!(
            identity.certificate.common_names.is_empty(),
            "{:?}",
            identity.certificate.common_names
        );
        assert!(
            identity.certificate.organizations.is_empty(),
            "{:?}",
            identity.certificate.organizations
        );

        let logs = capture.contents();
        assert!(
            logs.contains("Failed to parse client certificate"),
            "the parse error must be logged server-side; logs were: {logs}"
        );
        assert!(logs.contains("error="), "logs were: {logs}");
        assert!(logs.contains("certificate_len="), "logs were: {logs}");
    }

    #[tokio::test]
    async fn test_authenticate_no_certificate() {
        let validator = MtlsValidator;
        let parts = empty_parts();
        let mut identity = ClientIdentity::new(None);

        let result = validator.authenticate(&parts, &mut identity).await;

        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), AuthResult::NoCredentials));
        assert!(
            identity.certificate.common_names.is_empty(),
            "{:?}",
            identity.certificate.common_names
        );
        assert!(
            identity.certificate.organizations.is_empty(),
            "{:?}",
            identity.certificate.organizations
        );
    }

    #[tokio::test]
    async fn test_authenticate_with_valid_certificate() {
        let validator = MtlsValidator;
        let mut parts = empty_parts();
        parts
            .extensions
            .insert(PeerCertificate(Arc::new(cert_der())));

        let mut identity = ClientIdentity::new(None);

        let result = validator.authenticate(&parts, &mut identity).await;

        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), AuthResult::Authenticated));
        assert_eq!(identity.certificate.common_names, vec!["test-user"]);
        assert_eq!(identity.certificate.organizations, vec!["TestOrg"]);
    }

    #[tokio::test]
    async fn test_authenticate_with_minimal_certificate() {
        let validator = MtlsValidator;
        let mut parts = empty_parts();
        parts
            .extensions
            .insert(PeerCertificate(Arc::new(minimal_cert_der())));

        let mut identity = ClientIdentity::new(None);

        let result = validator.authenticate(&parts, &mut identity).await;

        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), AuthResult::Authenticated));
        assert!(
            identity.certificate.common_names.is_empty(),
            "{:?}",
            identity.certificate.common_names
        );
        assert!(
            identity.certificate.organizations.is_empty(),
            "{:?}",
            identity.certificate.organizations
        );
    }
}
