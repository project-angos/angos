use std::num::NonZeroUsize;

use bytesize::ByteSize;
use serde::Deserialize;

use crate::{
    configuration::{RegexPattern, TrustedProxy},
    jobs::store::JobQueueConfig,
    policy::{AccessMode, PolicyConfig, RetentionPolicyConfig},
    registry::metadata_store::{DEFAULT_GC_GRACE_SECS, PullHistoryConfig},
    registry::pagination::{LISTING_READ_CONCURRENCY, NAMESPACE_WALK_CONCURRENCY},
};
use crate::{layer::IndexAction, scan::ScanConfig};

/// Default Tokio worker-thread count; the `unwrap` is const-evaluated.
const DEFAULT_MAX_CONCURRENT_REQUESTS: NonZeroUsize = NonZeroUsize::new(64).unwrap();

/// Default cap on concurrent in-process cache-fill jobs; the `unwrap` is
/// const-evaluated.
pub const DEFAULT_MAX_CONCURRENT_CACHE_JOBS: NonZeroUsize = NonZeroUsize::new(4).unwrap();

/// Default replication-worker concurrency; the `unwrap` is const-evaluated.
pub const DEFAULT_MAX_CONCURRENT_REPLICATION_JOBS: NonZeroUsize = NonZeroUsize::new(4).unwrap();
/// Worker concurrency for the scan queue; a scan is minutes of scanner work.
pub const DEFAULT_MAX_CONCURRENT_SCAN_JOBS: NonZeroUsize = NonZeroUsize::new(2).unwrap();
/// Worker concurrency for the index queue: opening an image asks for all its
/// layers at once, each job inflating one on a core.
pub const DEFAULT_MAX_CONCURRENT_INDEX_JOBS: NonZeroUsize = NonZeroUsize::new(4).unwrap();

// A config struct is naturally flag-heavy; the bool count is not an API smell.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Deserialize)]
pub struct GlobalConfig {
    #[serde(default = "default_max_concurrent_requests")]
    pub max_concurrent_requests: NonZeroUsize,
    #[serde(default = "default_max_concurrent_cache_jobs")]
    pub max_concurrent_cache_jobs: NonZeroUsize,
    /// Worker concurrency for the replication queue.
    #[serde(default = "default_max_concurrent_replication_jobs")]
    pub max_concurrent_replication_jobs: NonZeroUsize,
    #[serde(default = "default_max_concurrent_scan_jobs")]
    pub max_concurrent_scan_jobs: NonZeroUsize,
    #[serde(default = "default_max_concurrent_index_jobs")]
    pub max_concurrent_index_jobs: NonZeroUsize,
    #[serde(default = "default_max_manifest_size")]
    pub max_manifest_size: ByteSize,
    #[serde(default = "default_max_blob_size")]
    pub max_blob_size: ByteSize,
    /// Read buffer each frame of a streamed blob response is filled from.
    /// Larger frames cost fewer allocations and body writes per blob served,
    /// at one buffer per in-flight response.
    #[serde(default = "default_blob_stream_frame_size")]
    pub blob_stream_frame_size: ByteSize,
    #[serde(default = "default_update_pull_time")]
    pub update_pull_time: bool,
    #[serde(default = "default_redirect_enabled")]
    pub enable_blob_redirect: bool,
    #[serde(default = "default_redirect_enabled")]
    pub enable_manifest_redirect: bool,
    #[serde(default)]
    pub access_policy: PolicyConfig<AccessMode>,
    #[serde(default)]
    pub retention_policy: RetentionPolicyConfig,
    #[serde(default)]
    pub immutable_tags: bool,
    #[serde(default)]
    pub immutable_tags_exclusions: Vec<RegexPattern>,
    /// When `true` (the default), a manifest push is accepted even if blobs or
    /// child manifests it references are not owned by the target namespace, and
    /// those references resolve as unknown on a later pull until their content
    /// is pushed. When `false` such a push is rejected with
    /// `MANIFEST_BLOB_UNKNOWN`; either way the namespace gains no read access to
    /// content it did not push.
    #[serde(default = "default_allow_missing_manifest_references")]
    pub allow_missing_manifest_references: bool,
    pub authorization_webhook: Option<String>,
    #[serde(default)]
    pub event_webhooks: Vec<String>,
    #[serde(default)]
    pub job_queue: Option<JobQueueConfig>,
    /// The scanner service pushes to scanning repositories are sent to, and
    /// how their reports are refreshed.
    #[serde(default)]
    pub scan: Option<ScanConfig>,
    /// The index policy every repository without an `index` table of its own
    /// follows.
    #[serde(default)]
    pub index: Option<PolicyConfig<IndexAction>>,
    /// Seconds to keep draining in-flight work on shutdown before forcing exit.
    /// Align this with the orchestrator's termination grace period.
    #[serde(default = "default_shutdown_drain_secs")]
    pub shutdown_drain_secs: u64,
    /// Concurrent directory scans a catalog / upload-namespace walk keeps in
    /// flight, hiding per-request backend latency on S3.
    #[serde(default = "default_namespace_walk_concurrency")]
    pub namespace_walk_concurrency: NonZeroUsize,
    /// Concurrent reads an admin listing behind the web UI keeps in flight
    /// per request: revision records, referrer descriptors, job records.
    #[serde(default = "default_listing_read_concurrency")]
    pub listing_read_concurrency: NonZeroUsize,
    /// Reclamation grace period in seconds: scrub leaves unreferenced blobs,
    /// dangling reference keys and stale index entries younger than this alone,
    /// so it cannot race an in-flight push or upload. Lower it only for offline
    /// maintenance against a store with no live traffic.
    #[serde(default = "default_gc_grace_secs")]
    pub gc_grace_secs: u64,
    /// How each tag's and revision's pull history is kept and compacted.
    #[serde(default)]
    pub pull_history: PullHistoryConfig,
    /// Proxy IPs or CIDR networks whose `X-Forwarded-For`/`X-Real-IP` headers
    /// are honored as the client IP. From any other peer those headers are
    /// ignored and the socket address is used, so clients cannot spoof IP-gated
    /// policies.
    #[serde(default)]
    pub trusted_proxies: Vec<TrustedProxy>,
}

fn default_shutdown_drain_secs() -> u64 {
    30
}

fn default_namespace_walk_concurrency() -> NonZeroUsize {
    NAMESPACE_WALK_CONCURRENCY
}

fn default_listing_read_concurrency() -> NonZeroUsize {
    LISTING_READ_CONCURRENCY
}

fn default_gc_grace_secs() -> u64 {
    DEFAULT_GC_GRACE_SECS
}

fn default_max_concurrent_requests() -> NonZeroUsize {
    DEFAULT_MAX_CONCURRENT_REQUESTS
}

fn default_max_concurrent_cache_jobs() -> NonZeroUsize {
    DEFAULT_MAX_CONCURRENT_CACHE_JOBS
}

fn default_max_concurrent_scan_jobs() -> NonZeroUsize {
    DEFAULT_MAX_CONCURRENT_SCAN_JOBS
}

fn default_max_concurrent_index_jobs() -> NonZeroUsize {
    DEFAULT_MAX_CONCURRENT_INDEX_JOBS
}

fn default_max_concurrent_replication_jobs() -> NonZeroUsize {
    DEFAULT_MAX_CONCURRENT_REPLICATION_JOBS
}

fn default_max_manifest_size() -> ByteSize {
    ByteSize::mib(5)
}

fn default_blob_stream_frame_size() -> ByteSize {
    ByteSize::kib(128)
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
            max_concurrent_scan_jobs: default_max_concurrent_scan_jobs(),
            max_concurrent_index_jobs: default_max_concurrent_index_jobs(),
            max_manifest_size: default_max_manifest_size(),
            max_blob_size: default_max_blob_size(),
            blob_stream_frame_size: default_blob_stream_frame_size(),
            update_pull_time: default_update_pull_time(),
            enable_blob_redirect: default_redirect_enabled(),
            enable_manifest_redirect: default_redirect_enabled(),
            access_policy: PolicyConfig::default(),
            retention_policy: RetentionPolicyConfig::default(),
            immutable_tags: false,
            immutable_tags_exclusions: Vec::new(),
            allow_missing_manifest_references: default_allow_missing_manifest_references(),
            authorization_webhook: None,
            event_webhooks: Vec::new(),
            job_queue: None,
            scan: None,
            index: None,
            shutdown_drain_secs: default_shutdown_drain_secs(),
            namespace_walk_concurrency: default_namespace_walk_concurrency(),
            listing_read_concurrency: default_listing_read_concurrency(),
            gc_grace_secs: default_gc_grace_secs(),
            pull_history: PullHistoryConfig::default(),
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

    pub fn blob_stream_frame_size_bytes(&self) -> usize {
        usize::try_from(self.blob_stream_frame_size.as_u64()).unwrap_or(usize::MAX)
    }
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU32, NonZeroUsize};

    use bytesize::ByteSize;

    use crate::{configuration::GlobalConfig, registry::metadata_store::PullHistoryConfig};

    #[test]
    fn default_values_match_configuration_defaults() {
        let config = GlobalConfig::default();

        assert_eq!(
            config.max_concurrent_requests,
            NonZeroUsize::new(64).unwrap()
        );
        assert_eq!(config.max_concurrent_cache_jobs.get(), 4);
        assert_eq!(config.max_concurrent_replication_jobs.get(), 4);
        assert_eq!(config.max_manifest_size, ByteSize::mib(5));
        assert_eq!(config.max_blob_size, ByteSize::gib(100));
        assert_eq!(config.blob_stream_frame_size, ByteSize::kib(128));
        assert!(!config.update_pull_time);
        assert!(!config.immutable_tags);
        assert!(config.immutable_tags_exclusions.is_empty());
        assert!(
            config.allow_missing_manifest_references,
            "manifest-reference validation is permissive by default"
        );
        assert_eq!(config.pull_history, PullHistoryConfig::default());
        assert_eq!(config.pull_history.compaction_age_secs(), Some(3600));
        assert!(config.authorization_webhook.is_none());
        assert!(
            config.trusted_proxies.is_empty(),
            "forwarded headers must be ignored by default"
        );
    }

    /// A count gate alone replaces the default age gate rather than adding
    /// to it.
    #[test]
    fn a_lone_count_gate_drops_the_default_age_gate() {
        let config =
            toml::from_str::<GlobalConfig>("[pull_history]\ncompact_after_pulls = 50").unwrap();
        assert_eq!(config.pull_history.compact_after_pulls, NonZeroU32::new(50));
        assert_eq!(config.pull_history.compaction_age_secs(), None);
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

            [pull_history]
            max_pulls = 5000
            max_age_secs = 31536000
            compact_after_secs = 86400
            "#,
        )
        .unwrap();

        assert_eq!(
            config.max_concurrent_requests,
            NonZeroUsize::new(10).unwrap()
        );
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
        assert_eq!(
            config.pull_history,
            PullHistoryConfig {
                max_pulls: NonZeroU32::new(5000).unwrap(),
                max_age_secs: Some(31_536_000),
                compact_after_secs: Some(86400),
                compact_after_pulls: None,
            }
        );
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
    fn max_concurrent_requests_zero_is_rejected() {
        let result = toml::from_str::<GlobalConfig>("max_concurrent_requests = 0\n");
        assert!(
            result.is_err(),
            "zero must be rejected at deserialization, not asserted inside the runtime builder"
        );
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
