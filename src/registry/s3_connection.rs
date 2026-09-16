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

    #[test]
    fn deserialize_flat_toml_keys() {
        let toml = r#"
            access_key_id = "AKIAIOSFODNN7EXAMPLE"
            secret_key    = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
            endpoint      = "https://s3.example.com"
            bucket        = "my-bucket"
            region        = "eu-west-1"
            key_prefix    = "_registry"
        "#;
        let cfg: S3ConnectionConfig = toml::from_str(toml).expect("deserialize");
        assert_eq!(cfg.access_key_id.expose(), "AKIAIOSFODNN7EXAMPLE");
        assert_eq!(
            cfg.secret_key.expose(),
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        );
        assert_eq!(cfg.endpoint, "https://s3.example.com");
        assert_eq!(cfg.bucket, "my-bucket");
        assert_eq!(cfg.region, "eu-west-1");
        assert_eq!(cfg.key_prefix, "_registry");
    }

    #[test]
    fn region_is_required_when_absent() {
        let toml = r#"
            access_key_id = "A"
            secret_key    = "S"
            endpoint      = "http://localhost:9000"
            bucket        = "b"
        "#;
        let err = toml::from_str::<S3ConnectionConfig>(toml)
            .expect_err("region must be required, not silently defaulted");
        assert!(
            err.to_string().contains("region"),
            "error should mention the missing `region` field, got: {err}"
        );
    }

    #[test]
    fn key_prefix_defaults_to_empty_when_absent() {
        let toml = r#"
            access_key_id = "A"
            secret_key    = "S"
            endpoint      = "http://localhost:9000"
            bucket        = "b"
            region        = "us-east-1"
        "#;
        let cfg: S3ConnectionConfig = toml::from_str(toml).expect("deserialize");
        assert_eq!(cfg.key_prefix, "");
    }

    #[test]
    fn to_client_config_propagates_connection_fields() {
        let cfg = S3ConnectionConfig {
            access_key_id: Secret::new("AKID".to_string()),
            secret_key: Secret::new("sekrit".to_string()),
            endpoint: "https://s3.example.com".to_string(),
            bucket: "bucket".to_string(),
            region: "ap-northeast-1".to_string(),
            key_prefix: "_prefix".to_string(),
        };
        let transport = cfg.to_client_config();
        assert_eq!(transport.access_key_id, "AKID");
        assert_eq!(transport.secret_key, "sekrit");
        assert_eq!(transport.endpoint, "https://s3.example.com");
        assert_eq!(transport.bucket, "bucket");
        assert_eq!(transport.region, "ap-northeast-1");
        assert_eq!(transport.key_prefix, "_prefix");
    }

    #[test]
    fn to_client_config_leaves_transport_fields_at_defaults() {
        let cfg = S3ConnectionConfig::default();
        let transport = cfg.to_client_config();
        let default_transport = S3TransportConfig::default();
        assert_eq!(
            transport.operation_timeout_secs,
            default_transport.operation_timeout_secs
        );
        assert_eq!(transport.max_attempts, default_transport.max_attempts);
        assert_eq!(
            transport.multipart_copy_threshold,
            default_transport.multipart_copy_threshold
        );
    }

    #[test]
    fn debug_output_redacts_secrets() {
        let cfg = S3ConnectionConfig {
            access_key_id: Secret::new("super-secret-key-id".to_string()),
            secret_key: Secret::new("super-secret-key".to_string()),
            ..S3ConnectionConfig::default()
        };
        let debug = format!("{cfg:?}");
        assert!(!debug.contains("super-secret-key-id"));
        assert!(!debug.contains("super-secret-key"));
        assert!(debug.contains("[REDACTED]"));
    }
}
