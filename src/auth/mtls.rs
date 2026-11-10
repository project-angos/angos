use std::sync::Arc;

use http::request::Parts;
use tracing::{error, instrument};
use x509_parser::certificate::X509Certificate;
use x509_parser::prelude::FromDer;

use crate::identity::ClientCertificate;

/// The peer certificate, set by the TLS layer as a request extension.
#[derive(Clone)]
pub struct PeerCertificate(pub Arc<Vec<u8>>);

/// The organizations and common names of the peer certificate, `None` when
/// the request has none. The TLS handshake already validated it (expiry, CA
/// chain); subject entries that are not UTF-8 are dropped.
#[instrument(skip(parts))]
pub fn authenticate(parts: &Parts) -> Option<ClientCertificate> {
    let peer_cert = parts.extensions.get::<PeerCertificate>()?;

    // The TLS layer already accepted this certificate, so a parse failure is
    // two parsers disagreeing rather than a bad credential; a policy needing
    // certificate identity refuses the request instead.
    let cert = match X509Certificate::from_der(&peer_cert.0) {
        Ok((_, cert)) => cert,
        Err(e) => {
            error!(
                error = ?e,
                certificate_len = peer_cert.0.len(),
                "Failed to parse client certificate"
            );
            return None;
        }
    };

    let subject = cert.subject();
    Some(ClientCertificate {
        organizations: subject
            .iter_organization()
            .filter_map(|o| o.as_str().ok().map(String::from))
            .collect(),
        common_names: subject
            .iter_common_name()
            .filter_map(|cn| cn.as_str().ok().map(String::from))
            .collect(),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tracing::Level;

    use super::*;
    use crate::test_fixtures::logging::LogCapture;
    use crate::test_fixtures::mtls::{cert_der, minimal_cert_der};
    use crate::test_fixtures::requests::empty_parts;

    fn parts_with_certificate(der: Vec<u8>) -> Parts {
        let mut parts = empty_parts();
        parts.extensions.insert(PeerCertificate(Arc::new(der)));
        parts
    }

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

        let certificate = tracing::subscriber::with_default(subscriber, || {
            authenticate(&parts_with_certificate(vec![0u8; 100]))
        });

        assert!(certificate.is_none());
        let logs = capture.contents();
        assert!(
            logs.contains("Failed to parse client certificate"),
            "the parse error must be logged server-side; logs were: {logs}"
        );
        assert!(logs.contains("error="), "logs were: {logs}");
        assert!(logs.contains("certificate_len="), "logs were: {logs}");
    }

    #[test]
    fn extracts_the_certificate_subject() {
        assert!(authenticate(&empty_parts()).is_none());

        let certificate = authenticate(&parts_with_certificate(cert_der())).unwrap();
        assert_eq!(certificate.common_names, vec!["test-user"]);
        assert_eq!(certificate.organizations, vec!["TestOrg"]);

        let minimal = authenticate(&parts_with_certificate(minimal_cert_der())).unwrap();
        assert!(minimal.common_names.is_empty() && minimal.organizations.is_empty());
    }
}
