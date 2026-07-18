use std::{fs, path::Path};

use reqwest::{Certificate, ClientBuilder, Identity};

/// Adds optional CA and client certificate files to `builder`.
///
/// # Errors
///
/// Returns an error when a configured certificate/key file cannot be read or
/// parsed.
pub fn apply_tls_files(
    mut builder: ClientBuilder,
    server_ca_bundle: Option<&Path>,
    client_certificate: Option<&Path>,
    client_private_key: Option<&Path>,
) -> Result<ClientBuilder, String> {
    if let Some(path) = server_ca_bundle {
        for certificate in load_certificate_bundle(path)? {
            builder = builder.add_root_certificate(certificate);
        }
    }

    if let Some(identity) = load_identity(client_certificate, client_private_key)? {
        builder = builder.identity(identity);
    }

    Ok(builder)
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

    use reqwest::Client;

    use crate::{
        http_client::{apply_tls_files, load_certificate_bundle, load_identity},
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
        assert!(Client::builder().use_rustls_tls().build().is_ok());

        let tmp_dir = tempfile::tempdir().unwrap();
        let file_path = tmp_dir.path().join("bundle.pem");
        fs::write(&file_path, ca_bundle_pem()).unwrap();

        let client = apply_tls_files(
            Client::builder().use_rustls_tls(),
            Some(&file_path),
            None,
            None,
        )
        .unwrap()
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

        let identity = load_identity(Some(&cert_file_path), Some(&key_file_path));
        assert!(matches!(identity, Ok(Some(_))));

        fs::write(&key_file_path, ca_bundle_pem()).unwrap();
        let identity = load_identity(Some(&cert_file_path), Some(&key_file_path));
        assert!(identity.is_err());

        let identity = load_identity(None, None);
        assert!(matches!(identity, Ok(None)));
    }
}
