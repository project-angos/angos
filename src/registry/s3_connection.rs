//! Shared S3 connection config used across `blob_store`, `metadata_store`, and `job_store`.
//!
//! Consolidates the six connection fields (`access_key_id`, `secret_key`,
//! `endpoint`, `bucket`, `region`, `key_prefix`) that were previously
//! copy-pasted into each module's own config. Credentials are wrapped in
//! [`crate::secret::Secret`] for debug-redaction and zeroize-on-drop.

use serde::Deserialize;

use crate::secret::Secret;
use angos_s3_client::BackendConfig as S3TransportConfig;
use angos_tx_engine::lock::S3LockConfig;

/// Connection-level parameters for an S3-compatible backend.
///
/// All credential, endpoint, bucket and region fields are required when this
/// type is deserialized via `#[serde(flatten)]`; `key_prefix` is the only
/// optional field and defaults to the empty string. Modules that need their
/// own per-section defaults implement a custom `Deserialize` and provide their
/// own field defaults. (The job store has no S3 config of its own; it inherits
/// the `[metadata_store]` backend and writes under a hardcoded `_jobs/` prefix.)
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct S3ConnectionConfig {
    pub access_key_id: Secret<String>,
    pub secret_key: Secret<String>,
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    /// Key prefix prepended to every object path. Defaults to empty string.
    #[serde(default)]
    pub key_prefix: String,
}

impl S3ConnectionConfig {
    /// Produce a transport-level [`S3TransportConfig`] from the connection
    /// fields, filling all non-connection fields with their crate defaults.
    ///
    /// Callers that need custom timeout or multipart settings should override
    /// those fields after calling this method.
    pub fn to_client_config(&self) -> S3TransportConfig {
        S3TransportConfig {
            access_key_id: self.access_key_id.expose().clone(),
            secret_key: self.secret_key.expose().clone(),
            endpoint: self.endpoint.clone(),
            bucket: self.bucket.clone(),
            region: self.region.clone(),
            key_prefix: self.key_prefix.clone(),
            ..S3TransportConfig::default()
        }
    }

    /// Produce a transport-level [`S3TransportConfig`] tuned for the per-
    /// `lock_key` execution lock: same connection/credentials/bucket as
    /// [`Self::to_client_config`], but with the three timing fields
    /// (`operation_timeout_secs`, `operation_attempt_timeout_secs`,
    /// `max_attempts`) taken from `lock_config`.
    ///
    /// Lock ops want tighter timeouts than blob/metadata ops so a single
    /// stuck request can't consume the entire heartbeat interval; this
    /// helper is the single source of truth for that tuning, shared by
    /// the metadata-store and job-store S3 backends.
    pub fn to_lock_client_config(&self, lock_config: &S3LockConfig) -> S3TransportConfig {
        S3TransportConfig {
            operation_timeout_secs: lock_config.operation_timeout_secs,
            operation_attempt_timeout_secs: lock_config.operation_attempt_timeout_secs,
            max_attempts: lock_config.max_attempts,
            ..self.to_client_config()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    /// Credential values must not appear in `Debug` output.
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
