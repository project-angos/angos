use serde::{Deserialize, Deserializer};

use crate::registry::{
    metadata_store::{ConditionalCapabilities, LockConfig, LockStrategy, lock},
    s3_connection::S3ConnectionConfig,
};
use angos_s3_client::BackendConfig as S3TransportConfig;

#[derive(Clone, Debug, PartialEq)]
pub struct BackendConfig {
    pub connection: S3ConnectionConfig,
    pub lock_strategy: LockStrategy,
    pub link_cache_ttl: u64,
    pub access_time_debounce_secs: u64,
    /// Explicitly declare which conditional S3 operations the provider supports.
    /// Each boolean flag corresponds to a distinct HTTP conditional header:
    /// - `put_if_none_match`: `PutObject` with If-None-Match: * (create-only)
    /// - `put_if_match`: `PutObject` with If-Match: <etag> (update-only, enables CAS optimizations)
    /// - `delete_if_match`: `DeleteObject` with If-Match: <etag> (atomic lock release)
    ///
    /// When set, the startup probe is skipped entirely and your declared values are used.
    /// When absent, the probe runs automatically for S3 metadata storage to auto-detect
    /// capabilities. With `lock_strategy = "memory"` or `"redis"`, set every flag to
    /// `false` to skip the probe and force blob-index updates through the configured
    /// lock backend instead of S3 CAS.
    ///
    /// Set explicitly to avoid startup latency from probing, or to handle S3-compatible
    /// providers where probe results may be inaccurate. Both `put_if_none_match` and
    /// `put_if_match` must be true for compare-and-swap (CAS) operations to be used.
    pub capabilities: Option<ConditionalCapabilities>,
}

impl Default for BackendConfig {
    fn default() -> Self {
        Self {
            connection: S3ConnectionConfig::default(),
            lock_strategy: LockStrategy::Memory,
            link_cache_ttl: default_link_cache_ttl(),
            access_time_debounce_secs: default_access_time_debounce(),
            capabilities: None,
        }
    }
}

impl<'de> Deserialize<'de> for BackendConfig {
    // Custom impl because `lock_strategy` must be resolved from optional
    // `redis` / `lock_strategy` keys via `lock::resolve_lock_strategy`.
    // The connection fields come in flat alongside the metadata-specific
    // keys; flattening `S3ConnectionConfig` preserves its required/optional
    // contract (all required except `key_prefix`).
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Raw {
            #[serde(flatten)]
            connection: S3ConnectionConfig,
            #[serde(default)]
            redis: Option<LockConfig>,
            #[serde(default)]
            lock_strategy: Option<LockStrategy>,
            #[serde(default = "default_link_cache_ttl")]
            link_cache_ttl: u64,
            #[serde(default = "default_access_time_debounce")]
            access_time_debounce_secs: u64,
            #[serde(default)]
            capabilities: Option<ConditionalCapabilities>,
        }

        let raw = Raw::deserialize(deserializer)?;
        let lock_strategy = lock::resolve_lock_strategy(raw.lock_strategy, raw.redis, true)?;

        Ok(BackendConfig {
            connection: raw.connection,
            lock_strategy,
            link_cache_ttl: raw.link_cache_ttl,
            access_time_debounce_secs: raw.access_time_debounce_secs,
            capabilities: raw.capabilities,
        })
    }
}

fn default_link_cache_ttl() -> u64 {
    30
}

fn default_access_time_debounce() -> u64 {
    60
}

impl BackendConfig {
    pub fn to_lock_store_config(&self, lock_config: &lock::s3::S3LockConfig) -> S3TransportConfig {
        S3TransportConfig {
            operation_timeout_secs: lock_config.operation_timeout_secs,
            operation_attempt_timeout_secs: lock_config.operation_attempt_timeout_secs,
            max_attempts: lock_config.max_attempts,
            ..self.connection.to_client_config()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `[metadata_store.s3]` round-trip: flat TOML deserialises into a
    /// `BackendConfig` whose `connection` carries the right values and whose
    /// metadata-specific keys override their defaults.
    #[test]
    fn s3_backend_config_toml_round_trip() {
        let toml = r#"
            access_key_id            = "meta-key"
            secret_key               = "meta-secret"
            endpoint                 = "https://meta.s3.example.com"
            bucket                   = "meta-bucket"
            region                   = "eu-central-1"
            key_prefix               = "_meta"
            link_cache_ttl           = 60
            access_time_debounce_secs = 120
        "#;

        let cfg: BackendConfig = toml::from_str(toml).expect("deserialize");
        assert_eq!(cfg.connection.access_key_id.expose(), "meta-key");
        assert_eq!(cfg.connection.secret_key.expose(), "meta-secret");
        assert_eq!(cfg.connection.endpoint, "https://meta.s3.example.com");
        assert_eq!(cfg.connection.bucket, "meta-bucket");
        assert_eq!(cfg.connection.region, "eu-central-1");
        assert_eq!(cfg.connection.key_prefix, "_meta");
        assert_eq!(cfg.link_cache_ttl, 60);
        assert_eq!(cfg.access_time_debounce_secs, 120);
    }

    /// Regression: `region` must be required, matching the documented schema
    /// and the behaviour before consolidation.
    #[test]
    fn s3_backend_config_requires_region() {
        let toml = r#"
            access_key_id = "k"
            secret_key    = "s"
            endpoint      = "http://localhost:9000"
            bucket        = "b"
        "#;
        let err = toml::from_str::<BackendConfig>(toml).expect_err("region must be required");
        assert!(
            err.to_string().contains("region"),
            "error should mention the missing `region` field, got: {err}"
        );
    }
}
