use std::{
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

use reqwest::{Certificate, Client, ClientBuilder, Identity, redirect::Policy};
use rustls::RootCertStore;
use rustls_native_certs::CertificateResult;
use serde::Deserialize;
use tracing::warn;

use angos_secret::Secret;

/// The TLS settings of an outbound client, flattened into its config section:
/// a CA bundle trusted for the server, and the identity presented for mTLS.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq, Hash)]
#[serde(try_from = "ClientTlsFields")]
pub struct ClientTls {
    pub server_ca_bundle: Option<PathBuf>,
    pub identity: Option<MtlsIdentity>,
}

/// A client certificate with its private key; holding both together is what
/// makes one without the other unrepresentable.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MtlsIdentity {
    pub certificate: PathBuf,
    pub private_key: PathBuf,
}

#[derive(Deserialize)]
struct ClientTlsFields {
    server_ca_bundle: Option<PathBuf>,
    /// `client_certificate` is the registry clients' spelling of the key.
    #[serde(alias = "client_certificate")]
    client_certificate_bundle: Option<PathBuf>,
    client_private_key: Option<PathBuf>,
}

impl TryFrom<ClientTlsFields> for ClientTls {
    type Error = String;

    fn try_from(fields: ClientTlsFields) -> Result<Self, Self::Error> {
        let identity = match (fields.client_certificate_bundle, fields.client_private_key) {
            (Some(certificate), Some(private_key)) => Some(MtlsIdentity {
                certificate,
                private_key,
            }),
            (None, None) => None,
            _ => {
                return Err(
                    "both client_certificate_bundle (or client_certificate) and \
                            client_private_key are required for mTLS"
                        .to_string(),
                );
            }
        };
        Ok(Self {
            server_ca_bundle: fields.server_ca_bundle,
            identity,
        })
    }
}

/// A username with its password, which a config section sets together or not
/// at all.
#[derive(Clone, Debug, PartialEq)]
pub struct BasicAuth {
    pub username: String,
    pub password: Secret<String>,
}

impl BasicAuth {
    /// The credentials a config section set, refusing one half without the
    /// other rather than running anonymous.
    ///
    /// # Errors
    ///
    /// Returns an error when only one of `username` and `password` is set.
    pub fn from_pair(
        username: Option<String>,
        password: Option<Secret<String>>,
    ) -> Result<Option<Self>, String> {
        match (username, password) {
            (Some(username), Some(password)) => Ok(Some(Self { username, password })),
            (None, None) => Ok(None),
            _ => Err("both username and password are required for basic auth".to_string()),
        }
    }
}

/// Decorates a [`reqwest::ClientBuilder`] with an optional server CA bundle and
/// client certificate loaded from files, then builds the [`Client`].
///
/// Starts from a rustls builder; redirect and timeout policy are set through
/// the `with_*` methods, and the mTLS material is layered on at [`Self::build`].
pub struct MtlsClientBuilder {
    builder: ClientBuilder,
    tls: ClientTls,
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
            tls: ClientTls::default(),
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

    /// Trusts `tls`'s PEM CA bundle for server verification, beside the
    /// system roots, and presents its client identity.
    #[must_use]
    pub fn with_tls(mut self, tls: &ClientTls) -> Self {
        self.tls = tls.clone();
        self
    }

    /// Loads the configured files and builds the client.
    ///
    /// # Errors
    ///
    /// Returns an error when a configured certificate or key file cannot be read
    /// or parsed, or when the HTTP client cannot be built.
    pub fn build(self) -> Result<Client, String> {
        let mut builder = self
            .builder
            .tls_certs_only(trusted_roots(self.tls.server_ca_bundle.as_deref())?);
        if let Some(identity) = &self.tls.identity {
            builder = builder.identity(load_identity(identity)?);
        }
        builder
            .build()
            .map_err(|e| format!("Failed to create HTTP client: {e}"))
    }
}

/// The roots every outbound client trusts: the system CA bundle
/// (`SSL_CERT_FILE`/`SSL_CERT_DIR`, else the platform's standard location) plus
/// `server_ca_bundle`. Given to [`ClientBuilder::tls_certs_only`], they are the
/// only roots reqwest consults.
///
/// # Errors
///
/// Returns an error when `server_ca_bundle` cannot be read or parsed, or when
/// no root is left to trust at all.
pub fn trusted_roots(server_ca_bundle: Option<&Path>) -> Result<Vec<Certificate>, String> {
    let configured = server_ca_bundle
        .map(load_certificate_bundle)
        .transpose()?
        .unwrap_or_default();
    collect_roots(rustls_native_certs::load_native_certs(), configured)
}

/// The parsable system roots followed by `configured`: one bad system entry is
/// skipped rather than failing every client.
fn collect_roots(
    system: CertificateResult,
    configured: Vec<Certificate>,
) -> Result<Vec<Certificate>, String> {
    for error in &system.errors {
        warn!("Failed to load system CA certificates: {error}");
    }
    let found = system.certs.len();
    let mut parsed = RootCertStore::empty();
    let mut roots: Vec<Certificate> = system
        .certs
        .into_iter()
        .filter(|der| parsed.add(der.clone()).is_ok())
        .filter_map(|der| Certificate::from_der(&der).ok())
        .collect();
    if roots.len() < found {
        warn!(
            "Ignored {} unparsable system CA certificates",
            found - roots.len()
        );
    }
    roots.extend(configured);
    if roots.is_empty() {
        return Err(
            "No CA certificate to trust: none loaded from the system bundle \
             (see SSL_CERT_FILE) and no server_ca_bundle configured"
                .to_string(),
        );
    }
    Ok(roots)
}

fn load_certificate_bundle(path: &Path) -> Result<Vec<Certificate>, String> {
    let certificate_pem =
        fs::read(path).map_err(|e| format!("Failed to read server CA bundle: {e}"))?;
    Certificate::from_pem_bundle(&certificate_pem)
        .map_err(|e| format!("Failed to parse server CA bundle: {e}"))
}

fn load_identity(identity: &MtlsIdentity) -> Result<Identity, String> {
    let cert_pem = fs::read(&identity.certificate)
        .map_err(|e| format!("Failed to read client certificate bundle: {e}"))?;
    let key_pem = fs::read(&identity.private_key)
        .map_err(|e| format!("Failed to read client private key: {e}"))?;
    // The separator is unconditional: a certificate file that does not end in a
    // newline would otherwise run its END marker into the key's BEGIN marker,
    // and the parser skips the key.
    Identity::from_pem(&[cert_pem.as_slice(), b"\n", key_pem.as_slice()].concat())
        .map_err(|e| format!("Failed to create identity from PEM: {e}"))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use std::sync::LazyLock;

    use rcgen::{
        BasicConstraints, CertificateParams, DistinguishedName, DnType, IsCa, Issuer, KeyPair,
    };
    use reqwest::Certificate;
    use rustls::pki_types::CertificateDer;
    use rustls_native_certs::CertificateResult;

    use angos_secret::Secret;

    use super::{
        BasicAuth, ClientTls, ClientTlsFields, MtlsClientBuilder, MtlsIdentity, collect_roots,
        load_certificate_bundle, load_identity,
    };

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
            .with_tls(&ClientTls {
                server_ca_bundle: Some(file_path),
                identity: None,
            })
            .build();
        assert!(client.is_ok());
    }

    fn self_signed_ca_der(common_name: &str) -> CertificateDer<'static> {
        let key = KeyPair::generate().expect("ca key");
        let mut params = CertificateParams::new(vec![]).expect("ca params");
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(DnType::CommonName, common_name);
        params
            .self_signed(&key)
            .expect("ca self-sign")
            .der()
            .clone()
    }

    /// One system certificate rustls cannot parse is skipped rather than
    /// costing every client its TLS, and a configured bundle is trusted beside
    /// the system roots.
    #[test]
    fn roots_skip_an_unparsable_system_certificate_and_add_configured_ones() {
        let mut system = CertificateResult::default();
        system.certs = vec![
            self_signed_ca_der("System CA"),
            CertificateDer::from(vec![0u8; 16]),
        ];
        let configured = Certificate::from_pem_bundle(ca_bundle_pem().as_bytes()).unwrap();

        let roots = collect_roots(system, configured).expect("roots must be collected");

        assert_eq!(
            roots.len(),
            3,
            "the parsable system root and both configured certificates"
        );
    }

    /// A configured bundle is enough where the system has none; with neither,
    /// no TLS connection could ever verify, so the client is not built.
    #[test]
    fn roots_need_a_system_or_a_configured_certificate() {
        let configured = Certificate::from_pem_bundle(ca_bundle_pem().as_bytes()).unwrap();
        assert_eq!(
            collect_roots(CertificateResult::default(), configured)
                .unwrap()
                .len(),
            2
        );
        assert!(collect_roots(CertificateResult::default(), Vec::new()).is_err());
    }

    #[test]
    fn load_identity_parses_certificate_and_key() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let cert_file_path = tmp_dir.path().join("certificate.pem");
        fs::write(&cert_file_path, client_cert_pem()).unwrap();

        let key_file_path = tmp_dir.path().join("private-key.pem");
        fs::write(&key_file_path, client_key_pem()).unwrap();

        let identity = MtlsIdentity {
            certificate: cert_file_path,
            private_key: key_file_path.clone(),
        };
        assert!(load_identity(&identity).is_ok());

        fs::write(&key_file_path, ca_bundle_pem()).unwrap();
        assert!(load_identity(&identity).is_err());
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

        let identity = load_identity(&MtlsIdentity {
            certificate: cert_file_path,
            private_key: key_file_path,
        });
        assert!(
            identity.is_ok(),
            "an unterminated certificate must not lose the key: {identity:?}"
        );
    }

    /// A certificate without its key, or a key without its certificate, would
    /// otherwise connect without a client identity and fail later as an
    /// unauthorized client.
    #[test]
    fn a_half_identity_is_refused() {
        let half = ClientTls::try_from(ClientTlsFields {
            server_ca_bundle: None,
            client_certificate_bundle: Some("cert.pem".into()),
            client_private_key: None,
        });
        assert!(half.is_err());
        let none = ClientTls::try_from(ClientTlsFields {
            server_ca_bundle: None,
            client_certificate_bundle: None,
            client_private_key: None,
        });
        assert_eq!(none, Ok(ClientTls::default()));
    }

    /// A username without its password would otherwise run anonymous.
    #[test]
    fn a_half_credential_pair_is_refused() {
        assert!(BasicAuth::from_pair(Some("ci".to_string()), None).is_err());
        assert!(BasicAuth::from_pair(None, Some(Secret::new("pw".to_string()))).is_err());
        assert_eq!(BasicAuth::from_pair(None, None), Ok(None));
    }
}
