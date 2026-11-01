//! Fixtures for [`RegistryClientConfig`]-based tests.

use angos_mtls_client::ClientTls;
use angos_oci_client::RegistryClientConfig;

/// The modal test client config: `url` plus defaults everywhere (no TLS
/// material, no credentials, `max_redirect` 5). Tests needing a variant
/// mutate the returned value.
#[must_use]
pub fn test_client_config(url: String) -> RegistryClientConfig {
    RegistryClientConfig {
        url,
        max_redirect: 5,
        connect_timeout_secs: 30,
        read_timeout_secs: 300,
        tls: ClientTls::default(),
        basic_auth: None,
    }
}
