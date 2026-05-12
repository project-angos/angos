//! TLS, mTLS, CA-bundle, redirect policy, and timeout configuration for the upstream HTTP client.

use std::{fs, time::Duration};

use reqwest::{Certificate, Client, Identity, redirect::Policy};

use super::RegistryClientConfig;
use crate::registry::Error;

pub fn build_http_client(config: &RegistryClientConfig) -> Result<Client, Error> {
    let mut client_builder = Client::builder()
        .redirect(Policy::limited(config.max_redirect as usize))
        .timeout(Duration::from_mins(5));

    if let Some(ca_bundle) = &config.server_ca_bundle {
        let cert_pem = fs::read(ca_bundle)
            .map_err(|e| Error::Initialization(format!("Failed to read CA bundle: {e}")))?;
        let cert = Certificate::from_pem(&cert_pem)
            .map_err(|e| Error::Initialization(format!("Failed to parse CA bundle: {e}")))?;
        client_builder = client_builder.add_root_certificate(cert);
    } else {
        client_builder = client_builder.use_rustls_tls();
    }

    if let (Some(cert_path), Some(key_path)) =
        (&config.client_certificate, &config.client_private_key)
    {
        let cert_pem = fs::read(cert_path).map_err(|e| {
            Error::Initialization(format!("Failed to read client certificate: {e}"))
        })?;
        let key_pem = fs::read(key_path)
            .map_err(|e| Error::Initialization(format!("Failed to read client key: {e}")))?;
        let identity = Identity::from_pem(&[cert_pem, key_pem].concat())
            .map_err(|e| Error::Initialization(format!("Failed to create client identity: {e}")))?;
        client_builder = client_builder.identity(identity);
    }

    client_builder
        .build()
        .map_err(|e| Error::Initialization(format!("Failed to build HTTP client: {e}")))
}
