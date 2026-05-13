use std::{fs, path::Path, time::Duration};

use reqwest::{Certificate, Client, ClientBuilder, Identity, header::HeaderMap, redirect::Policy};

pub struct HttpClientBuilder {
    builder: ClientBuilder,
}

impl HttpClientBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self {
            builder: Client::builder(),
        }
    }

    #[must_use]
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.builder = self.builder.timeout(timeout);
        self
    }

    #[must_use]
    pub fn redirect(mut self, policy: Policy) -> Self {
        self.builder = self.builder.redirect(policy);
        self
    }

    #[must_use]
    pub fn rustls_tls(mut self) -> Self {
        self.builder = self.builder.use_rustls_tls();
        self
    }

    #[must_use]
    pub fn default_headers(mut self, headers: HeaderMap) -> Self {
        self.builder = self.builder.default_headers(headers);
        self
    }

    /// Adds optional CA and client certificate files to the client builder.
    ///
    /// # Errors
    ///
    /// Returns an error when a configured certificate/key file cannot be read or parsed.
    pub fn tls_files(
        mut self,
        server_ca_bundle: Option<&Path>,
        client_certificate: Option<&Path>,
        client_private_key: Option<&Path>,
    ) -> Result<Self, String> {
        if let Some(path) = server_ca_bundle {
            for certificate in load_certificate_bundle(path)? {
                self.builder = self.builder.add_root_certificate(certificate);
            }
        }

        if let Some(identity) = load_identity(client_certificate, client_private_key)? {
            self.builder = self.builder.identity(identity);
        }

        Ok(self)
    }

    /// Builds the configured HTTP client.
    ///
    /// # Errors
    ///
    /// Returns an error when the underlying reqwest client cannot be built.
    pub fn build(self) -> Result<Client, String> {
        self.builder
            .build()
            .map_err(|e| format!("Failed to create HTTP client: {e}"))
    }
}

impl Default for HttpClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}

fn load_certificate_bundle(path: &Path) -> Result<Vec<Certificate>, String> {
    let certificate_pem =
        fs::read(path).map_err(|e| format!("Failed to read server CA bundle: {e}"))?;
    Certificate::from_pem_bundle(&certificate_pem)
        .map_err(|e| format!("Failed to parse server CA bundle: {e}"))
}

fn load_identity(
    cert_path: Option<&Path>,
    key_path: Option<&Path>,
) -> Result<Option<Identity>, String> {
    let (Some(cert_path), Some(key_path)) = (cert_path, key_path) else {
        return Ok(None);
    };

    let cert_pem = fs::read(cert_path)
        .map_err(|e| format!("Failed to read client certificate bundle: {e}"))?;
    let key_pem =
        fs::read(key_path).map_err(|e| format!("Failed to read client private key: {e}"))?;
    Identity::from_pem(&[cert_pem, key_pem].concat())
        .map(Some)
        .map_err(|e| format!("Failed to create identity from PEM: {e}"))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use crate::{
        http_client::{HttpClientBuilder, load_certificate_bundle, load_identity},
        test_fixtures::webhook::{ca_bundle_pem, client_cert_pem, client_key_pem},
    };

    #[test]
    fn load_certificate_bundle_parses_pem_bundle() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let file_path = tmp_dir.path().join("bundle.pem");
        fs::write(&file_path, ca_bundle_pem()).unwrap();

        let loaded_certificates = load_certificate_bundle(&file_path).unwrap();
        assert_eq!(loaded_certificates.len(), 2);
    }

    #[test]
    fn load_certificate_bundle_rejects_invalid_pem() {
        let content = "-----BEGIN INVALID CERTIFICATE-----LOLNOP-----END CERTIFICATE-----";
        let tmp_dir = tempfile::tempdir().unwrap();
        let file_path = tmp_dir.path().join("test.txt");
        fs::write(&file_path, content).unwrap();

        let invalid_certificates = load_certificate_bundle(&file_path);
        assert!(invalid_certificates.is_err());
    }

    #[test]
    fn rustls_tls_builds_with_and_without_ca_bundle() {
        assert!(HttpClientBuilder::new().rustls_tls().build().is_ok());

        let tmp_dir = tempfile::tempdir().unwrap();
        let file_path = tmp_dir.path().join("bundle.pem");
        fs::write(&file_path, ca_bundle_pem()).unwrap();

        let client = HttpClientBuilder::new()
            .rustls_tls()
            .tls_files(Some(&file_path), None, None)
            .and_then(HttpClientBuilder::build);
        assert!(client.is_ok());
    }

    #[test]
    fn load_identity_parses_certificate_and_key() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let cert_file_path = tmp_dir.path().join("certificate.pem");
        fs::write(&cert_file_path, client_cert_pem()).unwrap();

        let key_file_path = tmp_dir.path().join("private-key.pem");
        fs::write(&key_file_path, client_key_pem()).unwrap();

        let identity = load_identity(Some(&cert_file_path), Some(&key_file_path));
        assert!(matches!(identity, Ok(Some(_))));

        fs::write(&key_file_path, ca_bundle_pem()).unwrap();
        let identity = load_identity(Some(&cert_file_path), Some(&key_file_path));
        assert!(identity.is_err());

        let identity = load_identity(None, None);
        assert!(matches!(identity, Ok(None)));
    }
}
