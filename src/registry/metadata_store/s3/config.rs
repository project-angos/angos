use serde::{Deserialize, Deserializer};

use crate::registry::{
    data_store,
    metadata_store::{ConditionalCapabilities, LockConfig, LockStrategy, lock},
};

#[derive(Clone, Debug, PartialEq)]
pub struct BackendConfig {
    pub access_key_id: String,
    pub secret_key: String,
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub key_prefix: String,
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
    /// When absent, the probe runs automatically if `lock_strategy = "s3"` to auto-detect
    /// capabilities (and capabilities default to all-false for other lock strategies).
    ///
    /// Set explicitly to avoid startup latency from probing, or to handle S3-compatible
    /// providers where probe results may be inaccurate. Both `put_if_none_match` and
    /// `put_if_match` must be true for compare-and-swap (CAS) operations to be used.
    pub capabilities: Option<ConditionalCapabilities>,
}

impl Default for BackendConfig {
    fn default() -> Self {
        Self {
            access_key_id: String::new(),
            secret_key: String::new(),
            endpoint: String::new(),
            bucket: String::new(),
            region: String::new(),
            key_prefix: String::new(),
            lock_strategy: LockStrategy::Memory,
            link_cache_ttl: default_link_cache_ttl(),
            access_time_debounce_secs: default_access_time_debounce(),
            capabilities: None,
        }
    }
}

impl<'de> Deserialize<'de> for BackendConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Raw {
            access_key_id: String,
            secret_key: String,
            endpoint: String,
            bucket: String,
            region: String,
            #[serde(default)]
            key_prefix: String,
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
            access_key_id: raw.access_key_id,
            secret_key: raw.secret_key,
            endpoint: raw.endpoint,
            bucket: raw.bucket,
            region: raw.region,
            key_prefix: raw.key_prefix,
            lock_strategy,
            link_cache_ttl: raw.link_cache_ttl,
            access_time_debounce_secs: raw.access_time_debounce_secs,
            capabilities: raw.capabilities,
        })
    }
}

pub(super) fn default_link_cache_ttl() -> u64 {
    30
}

pub(super) fn default_access_time_debounce() -> u64 {
    60
}

impl BackendConfig {
    pub fn to_data_store_config(&self) -> data_store::s3::BackendConfig {
        data_store::s3::BackendConfig {
            access_key_id: self.access_key_id.clone(),
            secret_key: self.secret_key.clone(),
            endpoint: self.endpoint.clone(),
            bucket: self.bucket.clone(),
            region: self.region.clone(),
            key_prefix: self.key_prefix.clone(),
            ..Default::default()
        }
    }

    pub fn to_lock_store_config(
        &self,
        lock_config: &lock::s3::S3LockConfig,
    ) -> data_store::s3::BackendConfig {
        data_store::s3::BackendConfig {
            operation_timeout_secs: lock_config.operation_timeout_secs,
            operation_attempt_timeout_secs: lock_config.operation_attempt_timeout_secs,
            max_attempts: lock_config.max_attempts,
            ..self.to_data_store_config()
        }
    }
}
