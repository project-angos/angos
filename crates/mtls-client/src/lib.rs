use std::{
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

use reqwest::{Certificate, Client, ClientBuilder, Identity, redirect::Policy};

/// Decorates a [`reqwest::ClientBuilder`] with an optional server CA bundle and
/// client certificate loaded from files, then builds the [`Client`].
///
/// Starts from a rustls builder; redirect and timeout policy are set through
/// the `with_*` methods, and the mTLS material is layered on at [`Self::build`].
pub struct MtlsClientBuilder {
    builder: ClientBuilder,
    server_ca_bundle: Option<PathBuf>,
    client_identity: Option<(PathBuf, PathBuf)>,
}

impl Default for MtlsClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl MtlsClientBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self {
            builder: Client::builder().use_rustls_tls(),
            server_ca_bundle: None,
            client_identity: None,
        }
    }

    /// Sets the redirect policy the built client follows.
    #[must_use]
    pub fn with_redirect_policy(mut self, policy: Policy) -> Self {
        self.builder = self.builder.redirect(policy);
        self
    }

    /// Sets the whole-request timeout the built client applies to each request.
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.builder = self.builder.timeout(timeout);
        self
    }

    /// Bounds establishing the connection (TCP plus TLS handshake).
    #[must_use]
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.builder = self.builder.connect_timeout(timeout);
        self
    }

    /// Bounds inactivity between reads during a transfer, without capping a
    /// long but progressing transfer by a total deadline.
    #[must_use]
    pub fn with_read_timeout(mut self, timeout: Duration) -> Self {
        self.builder = self.builder.read_timeout(timeout);
        self
    }

    /// Trusts the PEM CA bundle at `path` for server verification; `None` leaves
    /// the platform roots in place.
    #[must_use]
    pub fn with_server_ca_bundle(mut self, path: Option<&Path>) -> Self {
        self.server_ca_bundle = path.map(Path::to_path_buf);
        self
    }

    /// Presents the PEM client certificate and its private key, which are
    /// required together; `None` presents no client identity.
    #[must_use]
    pub fn with_client_certificate(mut self, identity: Option<(&Path, &Path)>) -> Self {
        self.client_identity =
            identity.map(|(certificate, key)| (certificate.to_path_buf(), key.to_path_buf()));
        self
    }

    /// Loads the configured files and builds the client.
    ///
    /// # Errors
    ///
    /// Returns an error when a configured certificate or key file cannot be read
    /// or parsed, or when the HTTP client cannot be built.
    pub fn build(self) -> Result<Client, String> {
        let mut builder = self.builder;
        if let Some(path) = &self.server_ca_bundle {
            for certificate in load_certificate_bundle(path)? {
                builder = builder.add_root_certificate(certificate);
            }
        }
        if let Some(identity) = load_identity(
            self.client_identity
                .as_ref()
                .map(|(certificate, key)| (certificate.as_path(), key.as_path())),
        )? {
            builder = builder.identity(identity);
        }
        builder
            .build()
            .map_err(|e| format!("Failed to create HTTP client: {e}"))
    }
}

fn load_certificate_bundle(path: &Path) -> Result<Vec<Certificate>, String> {
    let certificate_pem =
        fs::read(path).map_err(|e| format!("Failed to read server CA bundle: {e}"))?;
    Certificate::from_pem_bundle(&certificate_pem)
        .map_err(|e| format!("Failed to parse server CA bundle: {e}"))
}

fn load_identity(identity: Option<(&Path, &Path)>) -> Result<Option<Identity>, String> {
    let Some((cert_path, key_path)) = identity else {
        return Ok(None);
    };

    let cert_pem = fs::read(cert_path)
        .map_err(|e| format!("Failed to read client certificate bundle: {e}"))?;
    let key_pem =
        fs::read(key_path).map_err(|e| format!("Failed to read client private key: {e}"))?;
    // The separator is unconditional: a certificate file that does not end in a
    // newline would otherwise run its END marker into the key's BEGIN marker,
    // and the parser skips the key.
    Identity::from_pem(&[cert_pem.as_slice(), b"\n", key_pem.as_slice()].concat())
        .map(Some)
        .map_err(|e| format!("Failed to create identity from PEM: {e}"))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use std::sync::LazyLock;

    use rcgen::{
        BasicConstraints, CertificateParams, DistinguishedName, DnType, IsCa, Issuer, KeyPair,
    };

    use super::{MtlsClientBuilder, load_certificate_bundle, load_identity};

    // Self-contained mTLS fixtures: a CA-signed server leaf followed by the CA
    // (the two-certificate bundle the loader expects), and a self-signed
    // client identity.
    struct Fixtures {
        ca_bundle: String,
        client_cert: String,
        client_key: String,
    }

    static FIXTURES: LazyLock<Fixtures> = LazyLock::new(|| {
        let ca_kp = KeyPair::generate().expect("ca key");
        let mut ca_params = CertificateParams::new(vec![]).expect("ca params");
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let mut ca_dn = DistinguishedName::new();
        ca_dn.push(DnType::CommonName, "Server CA");
        ca_params.distinguished_name = ca_dn;
        let ca_cert = ca_params.self_signed(&ca_kp).expect("ca self-sign");
        let ca_issuer = Issuer::from_params(&ca_params, &ca_kp);

        let leaf_kp = KeyPair::generate().expect("leaf key");
        let mut leaf_params =
            CertificateParams::new(vec!["example.com".to_string()]).expect("leaf params");
        let mut leaf_dn = DistinguishedName::new();
        leaf_dn.push(DnType::CommonName, "example.com");
        leaf_params.distinguished_name = leaf_dn;
        let leaf_cert = leaf_params
            .signed_by(&leaf_kp, &ca_issuer)
            .expect("ca-signed leaf");

        let client_kp = KeyPair::generate().expect("client key");
        let mut client_params = CertificateParams::default();
        let mut client_dn = DistinguishedName::new();
        client_dn.push(DnType::CommonName, "philippe");
        client_dn.push(DnType::OrganizationName, "admins");
        client_params.distinguished_name = client_dn;
        let client_cert = client_params
            .self_signed(&client_kp)
            .expect("client self-sign");

        Fixtures {
            ca_bundle: format!("{}{}", leaf_cert.pem(), ca_cert.pem()),
            client_cert: client_cert.pem(),
            client_key: client_kp.serialize_pem(),
        }
    });

    fn ca_bundle_pem() -> &'static str {
        &FIXTURES.ca_bundle
    }

    fn client_cert_pem() -> &'static str {
        &FIXTURES.client_cert
    }

    fn client_key_pem() -> &'static str {
        &FIXTURES.client_key
    }

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
        assert!(MtlsClientBuilder::new().build().is_ok());

        let tmp_dir = tempfile::tempdir().unwrap();
        let file_path = tmp_dir.path().join("bundle.pem");
        fs::write(&file_path, ca_bundle_pem()).unwrap();

        let client = MtlsClientBuilder::new()
            .with_server_ca_bundle(Some(&file_path))
            .build();
        assert!(client.is_ok());
    }

    #[test]
    fn load_identity_parses_certificate_and_key() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let cert_file_path = tmp_dir.path().join("certificate.pem");
        fs::write(&cert_file_path, client_cert_pem()).unwrap();

        let key_file_path = tmp_dir.path().join("private-key.pem");
        fs::write(&key_file_path, client_key_pem()).unwrap();

        let identity = load_identity(Some((cert_file_path.as_path(), key_file_path.as_path())));
        assert!(matches!(identity, Ok(Some(_))));

        fs::write(&key_file_path, ca_bundle_pem()).unwrap();
        let identity = load_identity(Some((cert_file_path.as_path(), key_file_path.as_path())));
        assert!(identity.is_err());

        let identity = load_identity(None);
        assert!(matches!(identity, Ok(None)));
    }

    /// A certificate file with no trailing newline still yields an identity:
    /// its `END` marker would otherwise run into the key's `BEGIN` marker and
    /// the key would be skipped.
    #[test]
    fn load_identity_accepts_a_certificate_without_a_trailing_newline() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let cert_file_path = tmp_dir.path().join("certificate.pem");
        fs::write(&cert_file_path, client_cert_pem().trim_end()).unwrap();

        let key_file_path = tmp_dir.path().join("private-key.pem");
        fs::write(&key_file_path, client_key_pem()).unwrap();

        let identity = load_identity(Some((cert_file_path.as_path(), key_file_path.as_path())));
        assert!(
            matches!(identity, Ok(Some(_))),
            "an unterminated certificate must not lose the key: {identity:?}"
        );
    }
}
