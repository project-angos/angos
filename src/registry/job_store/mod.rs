use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

pub mod durable;
pub mod fs;
pub mod in_process;
pub mod s3;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("job queue initialization failed: {0}")]
    Initialization(String),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("not found")]
    NotFound,
}

/// Durable job-queue configuration. Setting `[global.job_queue]` always selects
/// durable mode; omit the section to keep the legacy in-process `TaskQueue`.
#[derive(Clone, Debug, Deserialize)]
pub struct JobQueueConfig {
    #[serde(
        default = "default_lease_ttl_secs",
        deserialize_with = "deserialize_lease_ttl_secs"
    )]
    pub default_lease_ttl_secs: u64,
    #[serde(
        default = "default_pending_refresh_interval_secs",
        deserialize_with = "deserialize_pending_refresh_interval_secs"
    )]
    pub pending_refresh_interval_secs: u64,
    #[serde(default = "default_pending_ready_horizon_secs")]
    pub pending_ready_horizon_secs: u64,
    #[serde(flatten)]
    pub backend: JobQueueBackend,
}

fn deserialize_lease_ttl_secs<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
    let value = u64::deserialize(deserializer)?;
    if value < 9 {
        return Err(serde::de::Error::custom(
            "default_lease_ttl_secs must be at least 9 (heartbeat runs at ttl/3)",
        ));
    }
    Ok(value)
}

/// Floor on `pending_refresh_interval_secs`. The same value is the documented
/// S3 floor (sub-5s ticks induce LIST storms when multiple server replicas
/// each refresh in parallel); applying it as a hard config-time guard prevents
/// operators from accidentally configuring `0` or `1` and discovering the
/// consequences in production.
const MIN_PENDING_REFRESH_INTERVAL_SECS: u64 = 5;

fn deserialize_pending_refresh_interval_secs<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<u64, D::Error> {
    let value = u64::deserialize(deserializer)?;
    if value < MIN_PENDING_REFRESH_INTERVAL_SECS {
        return Err(serde::de::Error::custom(format!(
            "pending_refresh_interval_secs must be at least {MIN_PENDING_REFRESH_INTERVAL_SECS} \
             (sub-{MIN_PENDING_REFRESH_INTERVAL_SECS}s refresh ticks induce LIST storms on S3)",
        )));
    }
    Ok(value)
}

#[derive(Clone, Debug, Deserialize)]
pub enum JobQueueBackend {
    #[serde(rename = "fs")]
    Fs(fs::BackendConfig),
    #[serde(rename = "s3")]
    S3(s3::BackendConfig),
}

impl JobQueueConfig {
    pub fn to_backend(&self) -> Result<Arc<dyn JobStore>, Error> {
        match &self.backend {
            JobQueueBackend::Fs(config) => Ok(Arc::new(fs::Backend::new(config))),
            JobQueueBackend::S3(config) => Ok(Arc::new(s3::Backend::new(config)?)),
        }
    }
}

fn default_lease_ttl_secs() -> u64 {
    30
}

fn default_pending_refresh_interval_secs() -> u64 {
    15
}

fn default_pending_ready_horizon_secs() -> u64 {
    600
}

/// Envelope that travels through every queue backend. The `payload` field is
/// untyped so the envelope shape stays stable across payload churn; handlers
/// deserialize it into a concrete type.
///
/// Note: `not_before` is **not** stored on the envelope. It is encoded in the
/// storage key (the filename stem) as a sortable hex unix-millis prefix; see
/// [`make_storage_key`]. The filename is the single source of truth so a
/// claim loop can decide readiness from a LIST result alone.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobEnvelope {
    pub id: String,
    /// Logical queue name; selects the storage prefix and the worker's
    /// `--queue` filter.
    pub queue: String,
    /// Job type identifier (e.g. `"cache.fetch_blob"`). Handlers reject
    /// envelopes whose `kind` they do not recognize.
    pub kind: String,
    /// Per-key serialization token: at most one worker holds a lease per `lock_key`.
    pub lock_key: String,
    pub created_at: DateTime<Utc>,
    pub attempts: u32,
    pub max_attempts: u32,
    pub payload: serde_json::Value,
}

impl JobEnvelope {
    /// Build an envelope with a new UUID v4, default retry budget, and a typed
    /// payload that will be serialized to JSON. Returns `Err` only if the
    /// payload type cannot be serialized.
    pub fn new<P: Serialize>(
        queue: impl Into<String>,
        kind: impl Into<String>,
        lock_key: impl Into<String>,
        payload: &P,
    ) -> Result<Self, serde_json::Error> {
        Ok(Self {
            id: Uuid::new_v4().to_string(),
            queue: queue.into(),
            kind: kind.into(),
            lock_key: lock_key.into(),
            created_at: Utc::now(),
            attempts: 0,
            max_attempts: 5,
            payload: serde_json::to_value(payload)?,
        })
    }
}

/// Width of the hex unix-millis prefix in a storage key. 16 hex chars cover
/// `u64::MAX` milliseconds (well past year 5 billion), so the prefix is
/// fixed-width and lexicographic sort always matches time order.
pub const STORAGE_KEY_PREFIX_LEN: usize = 16;

/// Build a storage key (filename stem) encoding `not_before` as a sortable
/// hex unix-millis prefix followed by `-<id>`. Lexicographic sort of these
/// keys matches time order, so [`JobStore::list_pending`] returns ready
/// envelopes first and the claim loop can stop scanning as soon as it sees
/// a prefix in the future.
///
/// Negative timestamps (pre-1970) clamp to 0 — the queue is not meaningful
/// before unix epoch.
pub fn make_storage_key(not_before: DateTime<Utc>, id: &str) -> String {
    let millis = u64::try_from(not_before.timestamp_millis()).unwrap_or(0);
    format!("{millis:016x}-{id}")
}

/// Parse the `not_before` instant encoded in a storage-key prefix. Returns
/// `None` for malformed keys (missing prefix, bad hex). Used by the claim
/// loop to skip backed-off envelopes without reading their bodies.
pub fn parse_not_before(storage_key: &str) -> Option<DateTime<Utc>> {
    let bytes = storage_key.as_bytes();
    if bytes.len() <= STORAGE_KEY_PREFIX_LEN || bytes[STORAGE_KEY_PREFIX_LEN] != b'-' {
        return None;
    }
    let hex = &storage_key[..STORAGE_KEY_PREFIX_LEN];
    let millis = u64::from_str_radix(hex, 16).ok()?;
    DateTime::<Utc>::from_timestamp_millis(i64::try_from(millis).ok()?)
}

/// Producer-side interface: enqueue a job. Implementations should deduplicate
/// by `lock_key` when a matching pending job already exists.
#[async_trait]
pub trait JobQueue: Send + Sync {
    async fn enqueue(&self, envelope: JobEnvelope) -> Result<(), Error>;
}

/// Executor for a single job kind. Returns a stringified error on failure;
/// the queue runtime turns that into a retry or dead-letter outcome.
///
/// # Idempotency
///
/// Implementations **must** be idempotent. Lease release and pending-entry
/// removal happen in two separate storage ops at the end of `complete`, and a
/// crash (or a stolen lease after heartbeat loss) can leave the pending entry
/// behind. A subsequent worker will then re-execute the same envelope; the
/// handler is responsible for detecting "already done" and returning `Ok(())`
/// in that case.
#[async_trait]
pub trait JobHandler: Send + Sync {
    async fn execute(&self, envelope: &JobEnvelope) -> Result<(), String>;
}

/// Maximum number of pending envelopes inspected per scan. Deeper queues fall
/// back to lease-based serialization, which is the actual correctness primitive.
pub const MAX_SCAN: u16 = 1000;

/// Maximum value reported by [`JobStore::count_pending`]. The gauge feeds KEDA
/// autoscaling, which only needs ordinal granularity at high queue depths —
/// once the queue exceeds 10× the worker pool size you're at max scale anyway.
/// Capping here bounds S3 `LIST` cost per refresh tick to ~10 paginated calls
/// regardless of how deep the queue actually is. Operators reading the gauge
/// should treat the cap value as "≥ this many".
pub const MAX_REPORTED_PENDING: u64 = 10_000;

/// Hex unix-millis prefix (same format as [`make_storage_key`]) marking the
/// upper bound of the ready window for the autoscaler gauge. Storage keys
/// whose 16-hex prefix compares lexicographically greater are scheduled
/// past the horizon and excluded from the count.
///
/// `horizon_secs` is sourced from
/// [`JobQueueConfig::pending_ready_horizon_secs`].
pub fn pending_ready_cutoff_prefix(horizon_secs: u64) -> String {
    let cutoff =
        Utc::now() + chrono::Duration::seconds(i64::try_from(horizon_secs).unwrap_or(i64::MAX));
    let millis = u64::try_from(cutoff.timestamp_millis()).unwrap_or(u64::MAX);
    format!("{millis:016x}")
}

/// On-disk shape of a dead-letter record.
#[derive(Debug, Serialize)]
struct DeadLetterRecord<'a> {
    #[serde(flatten)]
    envelope: &'a JobEnvelope,
    last_error: &'a str,
    failed_at: DateTime<Utc>,
}

pub fn serialize_dead_letter(envelope: &JobEnvelope, last_error: &str) -> Result<Vec<u8>, Error> {
    serde_json::to_vec(&DeadLetterRecord {
        envelope,
        last_error,
        failed_at: Utc::now(),
    })
    .map_err(|e| Error::Storage(format!("failed to serialize dead-letter: {e}")))
}

/// Storage interface for the durable claim/heartbeat/retry logic. Backends
/// (`fs` and `s3`) implement the storage mechanics only — no claim algorithm
/// lives here.
///
/// **Storage keys.** Pending and failed entries are addressed by *storage
/// keys* (filename stems) that encode `not_before` as a sortable hex
/// unix-millis prefix; see [`make_storage_key`]. [`list_pending`] returns
/// keys in time order so the claim loop can identify ready envelopes from
/// the LIST result alone, without GET-ing each body.
#[async_trait]
pub trait JobStore: Send + Sync {
    /// Write a new pending envelope at the given `not_before`. Returns the
    /// storage key (filename stem) the envelope landed at — callers use it
    /// for subsequent [`read_pending`] / [`remove_pending`] /
    /// [`move_to_failed`] operations.
    ///
    /// On retry, the consumer writes a *new* file at a later `not_before` and
    /// deletes the old one; envelope identity (`envelope.id`) stays stable
    /// across retries but the storage key changes.
    async fn put_pending(
        &self,
        queue: &str,
        envelope: &JobEnvelope,
        not_before: DateTime<Utc>,
    ) -> Result<String, Error>;

    /// List up to `n` pending storage keys in ascending order. Storage keys
    /// start with a sortable hex-millis prefix, so callers can stop scanning
    /// at the first key whose prefix is in the future.
    async fn list_pending(&self, queue: &str, n: u16) -> Result<Vec<String>, Error>;

    async fn read_pending(&self, queue: &str, storage_key: &str) -> Result<JobEnvelope, Error>;

    /// Attempt to create a lease atomically (`create-if-not-exists`). Returns
    /// `Some(token)` when the lease was created (or an expired one was stolen);
    /// `None` when another worker holds a valid lease for `lock_key`.
    async fn try_create_lease(
        &self,
        lock_key: &str,
        worker_id: &str,
        ttl_secs: u64,
    ) -> Result<Option<String>, Error>;

    /// Refresh the expiry on an existing lease, returning a new lease token.
    async fn heartbeat_lease(
        &self,
        lock_key: &str,
        token: &str,
        worker_id: &str,
        ttl_secs: u64,
    ) -> Result<String, Error>;

    async fn remove_pending(&self, queue: &str, storage_key: &str) -> Result<(), Error>;

    /// Remove a lease, but only when the stored token matches.
    async fn remove_lease(&self, lock_key: &str, token: &str) -> Result<(), Error>;

    async fn move_to_failed(
        &self,
        queue: &str,
        storage_key: &str,
        envelope: &JobEnvelope,
        last_error: &str,
    ) -> Result<(), Error>;

    /// Count pending envelopes ready for handling within
    /// `[..., now + ready_horizon_secs]`. Capped at [`MAX_REPORTED_PENDING`].
    /// Backends use the storage-key prefix to early-stop once they cross
    /// the horizon, so cost is bounded regardless of total queue depth.
    async fn count_pending(&self, queue: &str, ready_horizon_secs: u64) -> Result<u64, Error>;

    /// `true` when any pending job in `queue` carries `lock_key`. Scans at
    /// most [`MAX_SCAN`] envelopes; this is best-effort dedup, not a
    /// correctness primitive.
    async fn find_pending_with_lock_key(&self, queue: &str, lock_key: &str) -> Result<bool, Error> {
        for storage_key in self.list_pending(queue, MAX_SCAN).await? {
            match self.read_pending(queue, &storage_key).await {
                Ok(env) if env.lock_key == lock_key => return Ok(true),
                Ok(_) | Err(Error::NotFound) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone as _;

    use super::*;

    #[test]
    fn storage_key_roundtrips_through_parse_not_before() {
        let when = Utc
            .timestamp_millis_opt(1_700_000_000_123)
            .single()
            .unwrap();
        let key = make_storage_key(when, "abc-123");
        assert_eq!(parse_not_before(&key), Some(when));
        assert!(key.ends_with("-abc-123"));
        assert_eq!(&key[..STORAGE_KEY_PREFIX_LEN], "0000018bcfe5687b");
    }

    #[test]
    fn storage_key_prefix_sorts_by_time() {
        let earlier = Utc
            .timestamp_millis_opt(1_700_000_000_000)
            .single()
            .unwrap();
        let later = Utc
            .timestamp_millis_opt(1_700_000_001_000)
            .single()
            .unwrap();
        let id = "uuid";
        assert!(make_storage_key(earlier, id) < make_storage_key(later, id));
    }

    #[test]
    fn parse_not_before_rejects_malformed_keys() {
        assert!(parse_not_before("").is_none());
        assert!(parse_not_before("not-a-storage-key").is_none());
        assert!(
            parse_not_before("zzzzzzzzzzzzzzzz-uuid").is_none(),
            "non-hex prefix"
        );
        assert!(
            parse_not_before("0000000000000000uuid").is_none(),
            "missing separator"
        );
    }

    #[test]
    fn negative_timestamp_clamps_to_zero() {
        let pre_epoch = Utc.timestamp_millis_opt(-1).single().unwrap();
        let key = make_storage_key(pre_epoch, "id");
        assert!(key.starts_with("0000000000000000-"));
    }

    /// Sub-floor `pending_refresh_interval_secs` must be rejected at config
    /// load. Zero is the obvious foot-gun; the floor (5) catches values that
    /// would induce LIST storms on S3.
    #[test]
    fn pending_refresh_interval_below_floor_is_rejected() {
        let toml_with_zero = r#"
            default_lease_ttl_secs = 30
            pending_refresh_interval_secs = 0
            pending_ready_horizon_secs = 600

            [fs]
            root_dir = "/tmp/jobs"
        "#;
        let err = toml::from_str::<JobQueueConfig>(toml_with_zero)
            .expect_err("pending_refresh_interval_secs = 0 must be rejected");
        assert!(
            err.to_string().contains("pending_refresh_interval_secs"),
            "error must name the field: {err}"
        );

        let toml_with_four = r#"
            default_lease_ttl_secs = 30
            pending_refresh_interval_secs = 4
            pending_ready_horizon_secs = 600

            [fs]
            root_dir = "/tmp/jobs"
        "#;
        toml::from_str::<JobQueueConfig>(toml_with_four)
            .expect_err("pending_refresh_interval_secs = 4 must be rejected");

        let toml_with_five = r#"
            default_lease_ttl_secs = 30
            pending_refresh_interval_secs = 5
            pending_ready_horizon_secs = 600

            [fs]
            root_dir = "/tmp/jobs"
        "#;
        let cfg = toml::from_str::<JobQueueConfig>(toml_with_five)
            .expect("the floor value itself must parse");
        assert_eq!(cfg.pending_refresh_interval_secs, 5);
    }
}
