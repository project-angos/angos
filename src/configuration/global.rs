use std::num::{NonZeroU64, NonZeroUsize};

use bytesize::ByteSize;
use serde::{Deserialize, Deserializer, de::Error as _};

use crate::{
    configuration::RegexPattern,
    policy::{AccessPolicyConfig, RetentionPolicyConfig},
    registry::job_store::JobQueueConfig,
};

/// Default cap on concurrent in-process cache-fill jobs.
pub const DEFAULT_MAX_CONCURRENT_CACHE_JOBS: NonZeroUsize = match NonZeroUsize::new(4) {
    Some(value) => value,
    None => unreachable!(),
};

/// Default replication-worker concurrency.
pub const DEFAULT_MAX_CONCURRENT_REPLICATION_JOBS: NonZeroUsize = match NonZeroUsize::new(4) {
    Some(value) => value,
    None => unreachable!(),
};

/// Default per-node fan-out budget for `scrub` / `policy` / `replication`.
pub const DEFAULT_MAX_CONCURRENT_SCRUB_TASKS: NonZeroUsize = match NonZeroUsize::new(16) {
    Some(value) => value,
    None => unreachable!(),
};

/// Default maximum wall-clock one maintenance run may hold the maintenance lock
/// before it self-cancels (24h).
pub const DEFAULT_MAINTENANCE_LOCK_MAX_HOLD_SECS: NonZeroU64 = match NonZeroU64::new(86_400) {
    Some(value) => value,
    None => unreachable!(),
};

/// Default maintenance grace (48h): the minimum quiescence age before scrub's
/// GC categories may reap an object.
pub const DEFAULT_MAINTENANCE_GRACE_SECS: u64 = 172_800;

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
    /// `angos worker`, and `scrub --replicate` drains.
    #[serde(
        default = "default_max_concurrent_replication_jobs",
        deserialize_with = "deserialize_max_concurrent_replication_jobs"
    )]
    pub max_concurrent_replication_jobs: NonZeroUsize,
    /// Per-node fan-out budget for `scrub` / `policy` / `replication`: how many
    /// namespaces may be enumerated in-flight at once. The shared sink still
    /// serializes the actual mutations, so this never changes the emitted actions.
    #[serde(
        default = "default_max_concurrent_scrub_tasks",
        deserialize_with = "deserialize_max_concurrent_scrub_tasks"
    )]
    pub max_concurrent_scrub_tasks: NonZeroUsize,
    /// Maximum wall-clock (seconds) one maintenance run (`scrub` / `policy` /
    /// `replication`) may hold the shared maintenance lock before it
    /// self-cancels. Size above the worst-case scrub duration.
    #[serde(
        default = "default_maintenance_lock_max_hold_secs",
        deserialize_with = "deserialize_maintenance_lock_max_hold_secs"
    )]
    pub maintenance_lock_max_hold_secs: NonZeroU64,
    /// Maintenance grace (seconds): how long an object must have been quiescent
    /// before scrub's GC categories may reap it. `0` disables the grace.
    #[serde(
        default = "default_maintenance_grace_secs",
        deserialize_with = "deserialize_maintenance_grace_secs"
    )]
    pub maintenance_grace_secs: u64,
    #[serde(default = "default_max_manifest_size")]
    pub max_manifest_size: ByteSize,
    #[serde(default = "default_max_blob_size")]
    pub max_blob_size: ByteSize,
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
    /// When `true` (the default), a manifest push is accepted even if some
    /// referenced blobs or child manifests are not yet present in or owned by the
    /// target namespace; the unowned references are not granted, so they resolve
    /// as unknown on a later pull until pushed. This restores pre-1.2.0
    /// compatibility with `docker buildx`/`bake` index and attestation pushes.
    /// When `false`, such pushes are rejected with `MANIFEST_BLOB_UNKNOWN`. Either
    /// way a namespace never gains read access to content it did not push, and
    /// `subject` references are always exempt.
    #[serde(default = "default_allow_missing_manifest_references")]
    pub allow_missing_manifest_references: bool,
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

fn default_max_concurrent_scrub_tasks() -> NonZeroUsize {
    DEFAULT_MAX_CONCURRENT_SCRUB_TASKS
}

fn deserialize_max_concurrent_scrub_tasks<'de, D>(deserializer: D) -> Result<NonZeroUsize, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_positive_nonzero(deserializer, "max_concurrent_scrub_tasks")
}

fn default_maintenance_lock_max_hold_secs() -> NonZeroU64 {
    DEFAULT_MAINTENANCE_LOCK_MAX_HOLD_SECS
}

fn deserialize_maintenance_lock_max_hold_secs<'de, D>(
    deserializer: D,
) -> Result<NonZeroU64, D::Error>
where
    D: Deserializer<'de>,
{
    let value = u64::deserialize(deserializer)?;
    NonZeroU64::new(value)
        .ok_or_else(|| D::Error::custom("maintenance_lock_max_hold_secs must be > 0"))
}

fn default_maintenance_grace_secs() -> u64 {
    DEFAULT_MAINTENANCE_GRACE_SECS
}

/// Largest grace chrono's duration arithmetic can represent (`i64::MAX`
/// milliseconds in seconds); a larger configured value is a config error
/// instead of a runtime panic.
pub const MAX_MAINTENANCE_GRACE_SECS: u64 = i64::MAX as u64 / 1000;

fn deserialize_maintenance_grace_secs<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let value = u64::deserialize(deserializer)?;
    if value > MAX_MAINTENANCE_GRACE_SECS {
        return Err(D::Error::custom(format!(
            "maintenance_grace_secs must be at most {MAX_MAINTENANCE_GRACE_SECS}"
        )));
    }
    Ok(value)
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

fn default_allow_missing_manifest_references() -> bool {
    true
}

impl Default for GlobalConfig {
    fn default() -> Self {
        GlobalConfig {
            max_concurrent_requests: default_max_concurrent_requests(),
            max_concurrent_cache_jobs: default_max_concurrent_cache_jobs(),
            max_concurrent_replication_jobs: default_max_concurrent_replication_jobs(),
            max_concurrent_scrub_tasks: default_max_concurrent_scrub_tasks(),
            maintenance_lock_max_hold_secs: default_maintenance_lock_max_hold_secs(),
            maintenance_grace_secs: default_maintenance_grace_secs(),
            max_manifest_size: default_max_manifest_size(),
            max_blob_size: default_max_blob_size(),
            update_pull_time: default_update_pull_time(),
            enable_redirect: None,
            enable_blob_redirect: None,
            enable_manifest_redirect: None,
            access_policy: AccessPolicyConfig::default(),
            retention_policy: RetentionPolicyConfig::default(),
            immutable_tags: false,
            immutable_tags_exclusions: Vec::new(),
            allow_missing_manifest_references: default_allow_missing_manifest_references(),
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

    pub fn max_blob_size_bytes(&self) -> u64 {
        self.max_blob_size.as_u64()
    }
}

#[cfg(test)]
mod tests {
    use bytesize::ByteSize;

    use super::MAX_MAINTENANCE_GRACE_SECS;
    use crate::configuration::GlobalConfig;

    #[test]
    fn default_values_match_configuration_defaults() {
        let config = GlobalConfig::default();

        assert_eq!(config.max_concurrent_requests, 64);
        assert_eq!(config.max_concurrent_cache_jobs.get(), 4);
        assert_eq!(config.max_concurrent_replication_jobs.get(), 4);
        assert_eq!(config.max_concurrent_scrub_tasks.get(), 16);
        assert_eq!(config.maintenance_lock_max_hold_secs.get(), 86_400);
        assert_eq!(config.maintenance_grace_secs, 172_800);
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
    }

    #[test]
    fn custom_values_parse() -> Result<(), toml::de::Error> {
        let config = toml::from_str::<GlobalConfig>(
            r#"
            max_concurrent_requests = 10
            max_concurrent_cache_jobs = 8
            max_concurrent_replication_jobs = 6
            max_concurrent_scrub_tasks = 32
            maintenance_lock_max_hold_secs = 7200
            maintenance_grace_secs = 3600
            max_manifest_size = "7MiB"
            max_blob_size = "10GiB"
            update_pull_time = true
            immutable_tags = true
            immutable_tags_exclusions = ["latest", "dev"]
            allow_missing_manifest_references = false
            authorization_webhook = "my-webhook"
            "#,
        )?;

        assert_eq!(config.max_concurrent_requests, 10);
        assert_eq!(config.max_concurrent_cache_jobs.get(), 8);
        assert_eq!(config.max_concurrent_replication_jobs.get(), 6);
        assert_eq!(config.max_concurrent_scrub_tasks.get(), 32);
        assert_eq!(config.maintenance_lock_max_hold_secs.get(), 7200);
        assert_eq!(config.maintenance_grace_secs, 3600);
        assert_eq!(config.max_manifest_size, ByteSize::mib(7));
        assert_eq!(config.max_blob_size, ByteSize::gib(10));
        assert!(config.update_pull_time);
        assert!(config.immutable_tags);
        assert!(!config.allow_missing_manifest_references);
        assert_eq!(config.immutable_tags_exclusions.len(), 2);
        assert_eq!(config.immutable_tags_exclusions[0].as_source(), "latest");
        assert_eq!(config.immutable_tags_exclusions[1].as_source(), "dev");
        assert_eq!(config.authorization_webhook.as_deref(), Some("my-webhook"));

        Ok(())
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
    fn max_concurrent_scrub_tasks_zero_is_rejected() {
        let result = toml::from_str::<GlobalConfig>("max_concurrent_scrub_tasks = 0\n");
        assert!(result.is_err(), "zero must be rejected at deserialization");
    }

    #[test]
    fn maintenance_lock_max_hold_secs_zero_is_rejected() {
        let result = toml::from_str::<GlobalConfig>("maintenance_lock_max_hold_secs = 0\n");
        assert!(result.is_err(), "zero must be rejected at deserialization");
    }

    #[test]
    fn maintenance_grace_secs_zero_is_accepted() {
        let config = toml::from_str::<GlobalConfig>("maintenance_grace_secs = 0\n")
            .expect("zero disables the grace");
        assert_eq!(config.maintenance_grace_secs, 0);
    }

    /// A grace beyond chrono's representable range is a clear config error, not
    /// a startup panic.
    #[test]
    fn maintenance_grace_secs_beyond_chrono_range_is_rejected() {
        let over = MAX_MAINTENANCE_GRACE_SECS + 1;
        let result = toml::from_str::<GlobalConfig>(&format!("maintenance_grace_secs = {over}\n"));
        let error = result.expect_err("an unrepresentable grace must be rejected");
        assert!(
            error.to_string().contains("maintenance_grace_secs"),
            "the error must name the field: {error}"
        );

        let config = toml::from_str::<GlobalConfig>(&format!(
            "maintenance_grace_secs = {MAX_MAINTENANCE_GRACE_SECS}\n"
        ))
        .expect("the bound itself is accepted");
        assert_eq!(config.maintenance_grace_secs, MAX_MAINTENANCE_GRACE_SECS);
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
