//! Fixtures for [`RegistryClientConfig`]-based tests.

use crate::registry_client::RegistryClientConfig;

/// The modal test client config: `url` plus defaults everywhere (no TLS
/// material, no credentials, `max_redirect` 5). Tests needing a variant
/// mutate the returned value.
pub fn test_client_config(url: impl Into<String>) -> RegistryClientConfig {
    RegistryClientConfig {
        url: url.into(),
        max_redirect: 5,
        server_ca_bundle: None,
        client_certificate: None,
        client_private_key: None,
        username: None,
        password: None,
    }
}
