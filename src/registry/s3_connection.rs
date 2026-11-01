//! The S3 connection fields shared by `blob_store`, `metadata_store`, and
//! `job_store`. Credentials are wrapped in [`angos_secret::Secret`] for
//! debug-redaction and zeroize-on-drop.

use serde::Deserialize;

use angos_s3_client::BackendConfig as S3TransportConfig;
use angos_secret::Secret;

/// `User-Agent` the S3 transport advertises
const USER_AGENT: &str = concat!("angos/", env!("CARGO_PKG_VERSION"));

/// Connection-level parameters for an S3-compatible backend. Every field but
/// `key_prefix` is required when this type is deserialized through
/// `#[serde(flatten)]`.
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct S3ConnectionConfig {
    pub access_key_id: Secret<String>,
    pub secret_key: Secret<String>,
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    /// Key prefix prepended to every object path.
    #[serde(default)]
    pub key_prefix: String,
}

impl S3ConnectionConfig {
    /// An [`S3TransportConfig`] carrying these connection fields, with every
    /// other field left at its crate default for the caller to override.
    pub fn to_client_config(&self) -> S3TransportConfig {
        S3TransportConfig {
            access_key_id: self.access_key_id.expose().clone(),
            secret_key: self.secret_key.expose().clone(),
            endpoint: self.endpoint.clone(),
            bucket: self.bucket.clone(),
            region: self.region.clone(),
            key_prefix: self.key_prefix.clone(),
            user_agent: Some(USER_AGENT.to_string()),
            ..S3TransportConfig::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_config_advertises_the_angos_product_version() {
        let transport = S3ConnectionConfig::default().to_client_config();
        assert_eq!(
            transport.user_agent,
            Some(format!("angos/{}", env!("CARGO_PKG_VERSION"))),
            "the S3 client must advertise the angos version, not the transport crate's",
        );
    }
}
