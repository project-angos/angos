use std::num::NonZeroUsize;

use bytesize::ByteSize;
use serde::{Deserialize, Deserializer, de::Error as _};

use crate::{
    configuration::{RegexPattern, TrustedProxy},
    jobs::store::JobQueueConfig,
    policy::{AccessPolicyConfig, RetentionPolicyConfig},
};

/// Default cap on concurrent in-process cache-fill jobs. Evaluated at
/// compile time: any failure would be a const-eval error at build time, never
/// a runtime panic.
pub const DEFAULT_MAX_CONCURRENT_CACHE_JOBS: NonZeroUsize = NonZeroUsize::new(4).unwrap();

/// Default replication-worker concurrency; the `unwrap` is const-evaluated.
pub const DEFAULT_MAX_CONCURRENT_REPLICATION_JOBS: NonZeroUsize = NonZeroUsize::new(4).unwrap();

// A config struct is naturally flag-heavy; the bool count is not an API smell.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Deserialize)]
pub struct GlobalConfig {
    #[serde(default = "default_max_concurrent_requests")]
    pub max_concurrent_requests: usize,
    #[serde(
        default = "default_max_concurrent_cache_jobs",
        deserialize_with = "deserialize_max_concurrent_cache_jobs"
    )]
    pub max_concurrent_cache_jobs: NonZeroUsize,
    /// Worker concurrency for the replication queue, used by the server,
    /// `angos worker`, and `angos replicate` drains.
    #[serde(
        default = "default_max_concurrent_replication_jobs",
        deserialize_with = "deserialize_max_concurrent_replication_jobs"
    )]
    pub max_concurrent_replication_jobs: NonZeroUsize,
    #[serde(default = "default_max_manifest_size")]
    pub max_manifest_size: ByteSize,
    #[serde(default = "default_max_blob_size")]
    pub max_blob_size: ByteSize,
    #[serde(default = "default_update_pull_time")]
    pub update_pull_time: bool,
    #[serde(default = "default_redirect_enabled")]
    pub enable_blob_redirect: bool,
    #[serde(default = "default_redirect_enabled")]
    pub enable_manifest_redirect: bool,
    #[serde(default)]
    pub access_policy: AccessPolicyConfig,
    #[serde(default)]
    pub retention_policy: RetentionPolicyConfig,
    #[serde(default)]
    pub immutable_tags: bool,
    #[serde(default)]
    pub immutable_tags_exclusions: Vec<RegexPattern>,
    /// When `true` (the default), a manifest push is accepted even if some of
    /// the blobs or child manifests it references are not yet present in or
    /// owned by the target namespace. The unowned references are not granted to
    /// the namespace, so they resolve as unknown on a later pull until their
    /// content is pushed. This restores pre-1.2.0 compatibility with `docker
    /// buildx`/`bake` index and attestation pushes whose children are not
    /// namespace-local at validation time.
    ///
    /// When `false`, the registry instead rejects such pushes outright with
    /// `MANIFEST_BLOB_UNKNOWN`.
    ///
    /// Either way a namespace never gains read access to content it did not push,
    /// and `subject` references are always exempt.
    #[serde(default = "default_allow_missing_manifest_references")]
    pub allow_missing_manifest_references: bool,
    pub authorization_webhook: Option<String>,
    #[serde(default)]
    pub event_webhooks: Vec<String>,
    #[serde(default)]
    pub job_queue: Option<JobQueueConfig>,
    /// Proxy IPs or CIDR networks whose `X-Forwarded-For`/`X-Real-IP` headers
    /// are honored as the client IP. From any other peer (the default, since
    /// the list is empty) those headers are ignored and the socket address is
    /// used, so clients cannot spoof IP-gated policies.
    #[serde(default)]
    pub trusted_proxies: Vec<TrustedProxy>,
}

fn default_max_concurrent_requests() -> usize {
    64
}

fn default_max_concurrent_cache_jobs() -> NonZeroUsize {
    DEFAULT_MAX_CONCURRENT_CACHE_JOBS
}

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

fn default_max_blob_size() -> ByteSize {
    ByteSize::gib(100)
}

fn default_update_pull_time() -> bool {
    false
}

fn default_redirect_enabled() -> bool {
    true
}

fn default_allow_missing_manifest_references() -> bool {
    true
}

impl Default for GlobalConfig {
    fn default() -> Self {
        GlobalConfig {
            max_concurrent_requests: default_max_concurrent_requests(),
            max_concurrent_cache_jobs: default_max_concurrent_cache_jobs(),
            max_concurrent_replication_jobs: default_max_concurrent_replication_jobs(),
            max_manifest_size: default_max_manifest_size(),
            max_blob_size: default_max_blob_size(),
            update_pull_time: default_update_pull_time(),
            enable_blob_redirect: true,
            enable_manifest_redirect: true,
            access_policy: AccessPolicyConfig::default(),
            retention_policy: RetentionPolicyConfig::default(),
            immutable_tags: false,
            immutable_tags_exclusions: Vec::new(),
            allow_missing_manifest_references: default_allow_missing_manifest_references(),
            authorization_webhook: None,
            event_webhooks: Vec::new(),
            job_queue: None,
            trusted_proxies: Vec::new(),
        }
    }
}

impl GlobalConfig {
    pub fn max_manifest_size_bytes(&self) -> usize {
        usize::try_from(self.max_manifest_size.as_u64()).unwrap_or(usize::MAX)
    }

    pub fn max_blob_size_bytes(&self) -> u64 {
        self.max_blob_size.as_u64()
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
        assert_eq!(config.max_blob_size, ByteSize::gib(100));
        assert!(!config.update_pull_time);
        assert!(!config.immutable_tags);
        assert!(config.immutable_tags_exclusions.is_empty());
        assert!(
            config.allow_missing_manifest_references,
            "manifest-reference validation is permissive by default (pre-1.2.0 behavior)"
        );
        assert!(config.authorization_webhook.is_none());
        assert!(
            config.trusted_proxies.is_empty(),
            "forwarded headers must be ignored by default"
        );
    }

    #[test]
    fn custom_values_parse() {
        let config = toml::from_str::<GlobalConfig>(
            r#"
            max_concurrent_requests = 10
            max_concurrent_cache_jobs = 8
            max_concurrent_replication_jobs = 6
            max_manifest_size = "7MiB"
            max_blob_size = "10GiB"
            update_pull_time = true
            immutable_tags = true
            immutable_tags_exclusions = ["latest", "dev"]
            allow_missing_manifest_references = false
            authorization_webhook = "my-webhook"
            trusted_proxies = ["127.0.0.1", "10.0.0.0/8"]
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
        assert_eq!(config.max_blob_size, ByteSize::gib(10));
        assert!(config.update_pull_time);
        assert!(config.immutable_tags);
        assert!(!config.allow_missing_manifest_references);
        assert_eq!(config.immutable_tags_exclusions.len(), 2);
        assert_eq!(config.immutable_tags_exclusions[0].as_source(), "latest");
        assert_eq!(config.immutable_tags_exclusions[1].as_source(), "dev");
        assert_eq!(config.authorization_webhook.as_deref(), Some("my-webhook"));
        assert_eq!(config.trusted_proxies.len(), 2);
    }

    #[test]
    fn invalid_trusted_proxy_is_rejected() {
        let result = toml::from_str::<GlobalConfig>(r#"trusted_proxies = ["not-an-ip"]"#);
        assert!(result.is_err(), "invalid entries must fail at parse time");
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

    #[test]
    fn max_blob_size_bytes_returns_u64() {
        let config = GlobalConfig {
            max_blob_size: ByteSize::gib(6),
            ..GlobalConfig::default()
        };

        assert_eq!(config.max_blob_size_bytes(), 6 * 1024 * 1024 * 1024);
    }
}
