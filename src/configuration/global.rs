use std::num::NonZeroUsize;

use bytesize::ByteSize;
use serde::{Deserialize, Deserializer, de::Error as _};

use crate::{
    configuration::RegexPattern,
    policy::{AccessPolicyConfig, RetentionPolicyConfig},
    registry::job_store::JobQueueConfig,
};

/// Default cap on concurrent in-process cache-fill jobs. Evaluated at
/// compile time: any failure would be a const-eval error at build time, never
/// a runtime panic.
pub const DEFAULT_MAX_CONCURRENT_CACHE_JOBS: NonZeroUsize = NonZeroUsize::new(4).unwrap();

/// Default replication-worker concurrency. Mirrors
/// [`DEFAULT_MAX_CONCURRENT_CACHE_JOBS`]; `unwrap` is const-evaluated.
pub const DEFAULT_MAX_CONCURRENT_REPLICATION_JOBS: NonZeroUsize = NonZeroUsize::new(4).unwrap();

#[derive(Clone, Debug, Deserialize)]
pub struct GlobalConfig {
    #[serde(default = "default_max_concurrent_requests")]
    pub max_concurrent_requests: usize,
    #[serde(
        default = "default_max_concurrent_cache_jobs",
        deserialize_with = "deserialize_max_concurrent_cache_jobs"
    )]
    pub max_concurrent_cache_jobs: NonZeroUsize,
    // Concurrency for the replication queue, consumed by both the in-process
    // drain and `angos worker --queue replication`. Mirrors
    // `max_concurrent_cache_jobs` (sibling worker-concurrency knob).
    #[serde(
        default = "default_max_concurrent_replication_jobs",
        deserialize_with = "deserialize_max_concurrent_replication_jobs"
    )]
    pub max_concurrent_replication_jobs: NonZeroUsize,
    #[serde(default = "default_max_manifest_size")]
    pub max_manifest_size: ByteSize,
    #[serde(default = "default_update_pull_time")]
    pub update_pull_time: bool,
    #[serde(default)]
    pub enable_redirect: Option<bool>,
    #[serde(default)]
    pub enable_blob_redirect: Option<bool>,
    #[serde(default)]
    pub enable_manifest_redirect: Option<bool>,
    #[serde(default)]
    pub access_policy: AccessPolicyConfig,
    #[serde(default)]
    pub retention_policy: RetentionPolicyConfig,
    #[serde(default)]
    pub immutable_tags: bool,
    #[serde(default)]
    pub immutable_tags_exclusions: Vec<RegexPattern>,
    pub authorization_webhook: Option<String>,
    #[serde(default)]
    pub event_webhooks: Vec<String>,
    #[serde(default)]
    pub job_queue: Option<JobQueueConfig>,
}

fn default_max_concurrent_requests() -> usize {
    64
}

fn default_max_concurrent_cache_jobs() -> NonZeroUsize {
    DEFAULT_MAX_CONCURRENT_CACHE_JOBS
}

/// Shared validation core for `NonZeroUsize` concurrency fields: deserializes
/// a `usize` and rejects zero, naming the field in the error message.
fn deserialize_positive_nonzero<'de, D>(
    deserializer: D,
    field: &str,
) -> Result<NonZeroUsize, D::Error>
where
    D: Deserializer<'de>,
{
    let value = usize::deserialize(deserializer)?;
    NonZeroUsize::new(value).ok_or_else(|| D::Error::custom(format!("{field} must be > 0")))
}

fn deserialize_max_concurrent_cache_jobs<'de, D>(deserializer: D) -> Result<NonZeroUsize, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_positive_nonzero(deserializer, "max_concurrent_cache_jobs")
}

fn default_max_concurrent_replication_jobs() -> NonZeroUsize {
    DEFAULT_MAX_CONCURRENT_REPLICATION_JOBS
}

fn deserialize_max_concurrent_replication_jobs<'de, D>(
    deserializer: D,
) -> Result<NonZeroUsize, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_positive_nonzero(deserializer, "max_concurrent_replication_jobs")
}

fn default_max_manifest_size() -> ByteSize {
    ByteSize::mib(5)
}

fn default_update_pull_time() -> bool {
    false
}

impl Default for GlobalConfig {
    fn default() -> Self {
        GlobalConfig {
            max_concurrent_requests: default_max_concurrent_requests(),
            max_concurrent_cache_jobs: default_max_concurrent_cache_jobs(),
            max_concurrent_replication_jobs: default_max_concurrent_replication_jobs(),
            max_manifest_size: default_max_manifest_size(),
            update_pull_time: default_update_pull_time(),
            enable_redirect: None,
            enable_blob_redirect: None,
            enable_manifest_redirect: None,
            access_policy: AccessPolicyConfig::default(),
            retention_policy: RetentionPolicyConfig::default(),
            immutable_tags: false,
            immutable_tags_exclusions: Vec::new(),
            authorization_webhook: None,
            event_webhooks: Vec::new(),
            job_queue: None,
        }
    }
}

impl GlobalConfig {
    pub fn resolved_enable_blob_redirect(&self) -> bool {
        self.enable_blob_redirect
            .or(self.enable_redirect)
            .unwrap_or(true)
    }

    pub fn resolved_enable_manifest_redirect(&self) -> bool {
        self.enable_manifest_redirect
            .or(self.enable_redirect)
            .unwrap_or(true)
    }

    pub fn max_manifest_size_bytes(&self) -> usize {
        usize::try_from(self.max_manifest_size.as_u64()).unwrap_or(usize::MAX)
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use bytesize::ByteSize;

    use crate::configuration::GlobalConfig;

    #[test]
    fn default_values_match_configuration_defaults() {
        let config = GlobalConfig::default();

        assert_eq!(config.max_concurrent_requests, 64);
        assert_eq!(config.max_concurrent_cache_jobs.get(), 4);
        assert_eq!(config.max_concurrent_replication_jobs.get(), 4);
        assert_eq!(config.max_manifest_size, ByteSize::mib(5));
        assert!(!config.update_pull_time);
        assert!(!config.immutable_tags);
        assert!(config.immutable_tags_exclusions.is_empty());
        assert!(config.authorization_webhook.is_none());
    }

    #[test]
    fn custom_values_parse() {
        let config = toml::from_str::<GlobalConfig>(
            r#"
            max_concurrent_requests = 10
            max_concurrent_cache_jobs = 8
            max_concurrent_replication_jobs = 6
            max_manifest_size = "7MiB"
            update_pull_time = true
            immutable_tags = true
            immutable_tags_exclusions = ["latest", "dev"]
            authorization_webhook = "my-webhook"
            "#,
        )
        .unwrap();

        assert_eq!(config.max_concurrent_requests, 10);
        assert_eq!(
            config.max_concurrent_cache_jobs,
            NonZeroUsize::new(8).unwrap()
        );
        assert_eq!(
            config.max_concurrent_replication_jobs,
            NonZeroUsize::new(6).unwrap()
        );
        assert_eq!(config.max_manifest_size, ByteSize::mib(7));
        assert!(config.update_pull_time);
        assert!(config.immutable_tags);
        assert_eq!(config.immutable_tags_exclusions.len(), 2);
        assert_eq!(config.immutable_tags_exclusions[0].as_source(), "latest");
        assert_eq!(config.immutable_tags_exclusions[1].as_source(), "dev");
        assert_eq!(config.authorization_webhook.as_deref(), Some("my-webhook"));
    }

    #[test]
    fn max_concurrent_cache_jobs_zero_is_rejected() {
        let result = toml::from_str::<GlobalConfig>("max_concurrent_cache_jobs = 0\n");
        assert!(result.is_err(), "zero must be rejected at deserialization");
    }

    #[test]
    fn max_concurrent_replication_jobs_zero_is_rejected() {
        let result = toml::from_str::<GlobalConfig>("max_concurrent_replication_jobs = 0\n");
        assert!(result.is_err(), "zero must be rejected at deserialization");
    }

    #[test]
    fn max_manifest_size_bytes_returns_usize() {
        let config = GlobalConfig {
            max_manifest_size: ByteSize::mib(6),
            ..GlobalConfig::default()
        };

        assert_eq!(config.max_manifest_size_bytes(), 6 * 1024 * 1024);
    }
}
