//! [`JobStore`]: durable job-queue storage, producer, and consumer over the
//! shared object store.
//!
//! Every mutation is an ordered sequence of idempotent object writes: enqueue
//! dedup and claims rest on atomic `create_if_absent` (degrading to advisory
//! put-settle-verify claims where the backend cannot enforce it), and each
//! crash window re-runs work instead of losing it.

use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use futures_util::TryStreamExt;
use serde::{Deserialize, Deserializer, Serialize, de::DeserializeOwned};
use tokio::{
    select, spawn,
    task::JoinHandle,
    time::{Interval, MissedTickBehavior, interval, sleep},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};
use uuid::Uuid;

use angos_backoff::{Backoff, jitter_below};
use angos_storage::{Error as StorageError, ObjectStore, Page};

use crate::{
    jobs::{JobState, Queue},
    metrics_provider::metrics_provider,
    registry::{Error as RegistryError, metadata_store::MetadataStore},
};

pub const JOBS_ROOT: &str = "_jobs";

fn job_pending_dir(queue: &str) -> String {
    format!("{JOBS_ROOT}/pending/{queue}")
}

pub fn job_pending_path(queue: &str, id: &str) -> String {
    format!("{JOBS_ROOT}/pending/{queue}/{id}.json")
}

fn job_failed_dir(queue: &str) -> String {
    format!("{JOBS_ROOT}/failed/{queue}")
}

pub fn job_failed_path(queue: &str, id: &str) -> String {
    format!("{JOBS_ROOT}/failed/{queue}/{id}.json")
}

/// Path to the `lock_key` to `storage_key` dedup index file, read for an O(1)
/// lookup instead of scanning every pending body.
pub fn job_lock_key_index_path(queue: &str, lock_key: &LockKey) -> String {
    format!("{JOBS_ROOT}/index/{queue}/{}.json", lock_key.encode())
}

/// Path of the claim key serialising execution of one `lock_key` across
/// workers: created with `create_if_absent`, leased, refreshed by the holder,
/// and deleted on release; a lapsed lease is taken over by deletion.
fn job_claim_path(lock_key: &LockKey) -> String {
    format!("{JOBS_ROOT}/claims/{}.json", lock_key.encode())
}

/// A job's per-key serialization token: at most one worker executes a given
/// lock key at a time. Valid keys are non-empty and free of `%`, which
/// [`Self::encode`] reserves as its escape character; allowing it would let two
/// distinct keys encode to one index path and falsely dedup.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize)]
#[serde(try_from = "String")]
pub struct LockKey(String);

impl LockKey {
    /// # Errors
    ///
    /// Returns [`Error::InvalidLockKey`] when `key` is empty or contains `%`.
    pub fn new(key: impl Into<String>) -> Result<Self, Error> {
        let key = key.into();
        if key.is_empty() {
            return Err(Error::InvalidLockKey("lock key is empty".to_string()));
        }
        if key.contains('%') {
            return Err(Error::InvalidLockKey(format!(
                "lock key '{key}' contains a reserved '%'"
            )));
        }
        Ok(Self(key))
    }

    /// Percent-encode characters unsafe in a filename or an S3 key component so
    /// a key lands on the same path on every backend. Injective, since `%`
    /// cannot appear in a [`LockKey`].
    fn encode(&self) -> String {
        self.0
            .chars()
            .map(|c| match c {
                '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => {
                    format!("%{:02X}", c as u32)
                }
                c => c.to_string(),
            })
            .collect()
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for LockKey {
    type Error = Error;

    fn try_from(key: String) -> Result<Self, Error> {
        Self::new(key)
    }
}

impl Serialize for LockKey {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl fmt::Display for LockKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("job queue initialization failed: {0}")]
    Initialization(String),
    #[error("storage error: {0}")]
    Storage(String),
    /// A handler failed to do its work, kept distinct from [`Self::Storage`] so
    /// a handler fault is not mislabelled as a queue storage error.
    #[error("job execution failed: {0}")]
    Execution(String),
    #[error("invalid lock key: {0}")]
    InvalidLockKey(String),
    #[error("not found")]
    NotFound,
    /// A stored record's body will not deserialize, kept distinct from
    /// [`Self::Storage`] so one poison object is skipped instead of aborting the
    /// scan: it will not fix itself, where a transient fault must stop the scan.
    #[error("corrupt record: {0}")]
    Corrupt(String),
    /// The job can never succeed; the runner dead-letters it on the spot
    /// instead of burning its retry budget.
    #[error("terminal failure: {0}")]
    Terminal(String),
}

/// A registry outcome as a job failure: an authorization denial cannot change,
/// so it dead-letters on the spot; anything else is retried.
impl From<RegistryError> for Error {
    fn from(error: RegistryError) -> Self {
        match error {
            RegistryError::Denied(msg) => Error::Terminal(msg),
            other => Error::Execution(other.to_string()),
        }
    }
}

impl From<StorageError> for Error {
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::NotFound => Error::NotFound,
            StorageError::Backend(msg) => Error::Storage(msg),
        }
    }
}

/// How claim keys serialise workers on this backend, per
/// [`ensure_claim_support`]'s startup probe.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClaimMode {
    /// The backend's `create_if_absent` is honest: one atomic create wins.
    Atomic,
    /// The backend cannot enforce the atomic create; claims fall back to a
    /// put-settle-verify sequence that admits transient claim races.
    Advisory,
}

/// Probe claim serialisation by creating a scratch key twice: a backend whose
/// second create fails is [`ClaimMode::Atomic`], one that accepts it degrades to
/// [`ClaimMode::Advisory`], since claims are an efficiency mechanism and
/// correctness rests on handler idempotency. Probe IO errors fail startup.
pub async fn ensure_claim_support(metadata_store: &MetadataStore) -> Result<ClaimMode, Error> {
    let store = metadata_store.object_store();
    let key = format!("{JOBS_ROOT}/claims/.probe-{}", Uuid::new_v4());
    let first = store
        .create_if_absent(&key, Bytes::from_static(b"probe"))
        .await;
    let second = store
        .create_if_absent(&key, Bytes::from_static(b"probe"))
        .await;
    let _ = store.delete(&key).await;
    match (first, second) {
        (Ok(true), Ok(false)) => Ok(ClaimMode::Atomic),
        (Ok(_), Ok(_)) => {
            warn!(
                "job queue: the metadata store's backend did not enforce atomic \
                 create-if-absent; falling back to advisory claims: claim races admit \
                 multiple workers, so idempotent jobs may execute more than once until \
                 the losing claimants self-cancel within a refresh period"
            );
            Ok(ClaimMode::Advisory)
        }
        (Err(e), _) | (_, Err(e)) => Err(Error::Storage(format!(
            "[global.job_queue] claim-support probe failed: {e}"
        ))),
    }
}

/// Job-queue tunables. `[global.job_queue]` does not change durability (jobs
/// persist under the store's `_jobs/` prefix either way); it switches draining
/// to separate `angos worker` processes and enables the queue-depth gauge.
#[derive(Clone, Debug, Deserialize)]
pub struct JobQueueConfig {
    /// Lease on a job claim, in seconds: a crashed worker's jobs are taken over
    /// after this long, and the holder refreshes at a third of it.
    #[serde(
        default = "default_claim_ttl_secs",
        deserialize_with = "at_least::<_, { MIN_CLAIM_TTL_SECS }>"
    )]
    pub claim_ttl_secs: u64,
    #[serde(
        default = "default_pending_refresh_interval_secs",
        deserialize_with = "at_least::<_, { MIN_PENDING_REFRESH_INTERVAL_SECS }>"
    )]
    pub pending_refresh_interval_secs: u64,
    #[serde(default = "default_pending_ready_horizon_secs")]
    pub pending_ready_horizon_secs: u64,
    #[serde(default = "default_job_max_attempts")]
    pub max_attempts: u32,
    #[serde(default = "default_retry_backoff_min_ms")]
    pub retry_backoff_min_ms: u64,
    #[serde(default = "default_retry_backoff_max_ms")]
    pub retry_backoff_max_ms: u64,
}

impl JobQueueConfig {
    #[must_use]
    pub fn retry_policy(&self) -> JobRetryPolicy {
        JobRetryPolicy {
            max_attempts: self.max_attempts,
            backoff_min_ms: self.retry_backoff_min_ms,
            backoff_max_ms: self.retry_backoff_max_ms,
            claim_ttl_secs: self.claim_ttl_secs,
        }
    }
}

/// The retry budget, backoff bounds, and claim-lease TTL a [`JobStore`]
/// applies, sourced from [`JobQueueConfig`] for the durable queue.
#[derive(Clone, Copy, Debug)]
pub struct JobRetryPolicy {
    pub max_attempts: u32,
    pub backoff_min_ms: u64,
    pub backoff_max_ms: u64,
    pub claim_ttl_secs: u64,
}

impl Default for JobRetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: default_job_max_attempts(),
            backoff_min_ms: default_retry_backoff_min_ms(),
            backoff_max_ms: default_retry_backoff_max_ms(),
            claim_ttl_secs: default_claim_ttl_secs(),
        }
    }
}

fn default_job_max_attempts() -> u32 {
    5
}

fn default_retry_backoff_min_ms() -> u64 {
    100
}

fn default_retry_backoff_max_ms() -> u64 {
    10_000
}

fn default_claim_ttl_secs() -> u64 {
    CLAIM_TTL_SECS
}

/// Floor on `claim_ttl_secs`. The holder refreshes at a third of the lease,
/// so a shorter lease lapses before its first refresh can land.
const MIN_CLAIM_TTL_SECS: u64 = 3;

/// Floor on `pending_refresh_interval_secs`: sub-5s ticks induce LIST storms
/// when several server replicas refresh in parallel.
const MIN_PENDING_REFRESH_INTERVAL_SECS: u64 = 5;

/// Rejects a value below the `MIN` floor.
fn at_least<'de, D: Deserializer<'de>, const MIN: u64>(deserializer: D) -> Result<u64, D::Error> {
    let value = u64::deserialize(deserializer)?;
    if value < MIN {
        return Err(serde::de::Error::custom(format!("must be at least {MIN}")));
    }
    Ok(value)
}

fn default_pending_refresh_interval_secs() -> u64 {
    15
}

fn default_pending_ready_horizon_secs() -> u64 {
    600
}

/// Envelope that travels through the queue, with an untyped `payload` handlers
/// deserialize into a concrete type.
///
/// `not_before` is not a field: it is encoded in the storage key by
/// [`make_storage_key`], so the claim loop decides readiness from a LIST result
/// alone.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobEnvelope {
    pub id: String,
    pub queue: Queue,
    /// Job type identifier (e.g. `"cache.fetch_blob"`); handlers reject
    /// envelopes whose `kind` they do not recognize.
    pub kind: String,
    pub lock_key: LockKey,
    pub created_at: DateTime<Utc>,
    pub attempts: u32,
    /// Retry budget, `None` until [`JobStore::enqueue`] stamps the queue's
    /// configured one so a caller can pin its own, zero included.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_attempts: Option<u32>,
    pub payload: serde_json::Value,
}

impl JobEnvelope {
    /// Build an envelope with a new UUID v4 and a JSON-serialized payload.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] when `lock_key` is invalid or the payload cannot be
    /// serialized.
    pub fn new<P: Serialize>(
        queue: Queue,
        kind: impl Into<String>,
        lock_key: impl Into<String>,
        payload: &P,
    ) -> Result<Self, Error> {
        Ok(Self {
            id: Uuid::new_v4().to_string(),
            queue,
            kind: kind.into(),
            lock_key: LockKey::new(lock_key)?,
            created_at: Utc::now(),
            attempts: 0,
            max_attempts: None,
            payload: serde_json::to_value(payload).map_err(|e| Error::Execution(e.to_string()))?,
        })
    }

    /// The payload, for a handler accepting `kinds`. Neither a foreign kind nor
    /// a payload that does not decode can change on retry, so both are
    /// terminal.
    pub fn payload<T: DeserializeOwned>(&self, kinds: &[&str]) -> Result<T, Error> {
        if !kinds.contains(&self.kind.as_str()) {
            return Err(Error::Terminal(format!(
                "unsupported job kind '{}'; expected one of {kinds:?}",
                self.kind
            )));
        }
        serde_json::from_value(self.payload.clone())
            .map_err(|e| Error::Terminal(format!("failed to deserialize job payload: {e}")))
    }
}

/// Width of the hex unix-millis prefix in a storage key. 16 hex chars cover
/// `u64::MAX` milliseconds, so the prefix is fixed-width and lexicographic sort
/// always matches time order.
const STORAGE_KEY_PREFIX_LEN: usize = 16;

/// Build a storage key encoding `not_before` as a sortable hex unix-millis
/// prefix followed by `-<id>`, so a lexicographic listing returns ready
/// envelopes first and the claim loop stops at the first prefix in the future.
/// Pre-1970 timestamps clamp to 0.
fn make_storage_key(not_before: DateTime<Utc>, id: &str) -> String {
    let millis = u64::try_from(not_before.timestamp_millis()).unwrap_or(0);
    format!("{millis:016x}-{id}")
}

/// Parse the `not_before` instant encoded in a storage-key prefix, `None` for a
/// malformed key.
pub fn parse_not_before(storage_key: &str) -> Option<DateTime<Utc>> {
    let bytes = storage_key.as_bytes();
    if bytes.len() <= STORAGE_KEY_PREFIX_LEN || bytes[STORAGE_KEY_PREFIX_LEN] != b'-' {
        return None;
    }
    let hex = &storage_key[..STORAGE_KEY_PREFIX_LEN];
    let millis = u64::from_str_radix(hex, 16).ok()?;
    DateTime::<Utc>::from_timestamp_millis(i64::try_from(millis).ok()?)
}

/// Hex unix-millis prefix marking the upper bound of the ready window: storage
/// keys comparing lexicographically greater are scheduled past the horizon and
/// excluded from the gauge count. Keys hold the same millis a `u64` does, so a
/// horizon too far out to represent saturates into counting all of them.
fn pending_ready_cutoff_prefix(horizon_secs: u64) -> String {
    let now_millis = u64::try_from(Utc::now().timestamp_millis()).unwrap_or(0);
    let millis = horizon_secs.saturating_mul(1000).saturating_add(now_millis);
    format!("{millis:016x}")
}

/// On-disk shape of the per-`lock_key` dedup index file, holding the
/// `storage_key` of the most-recent pending file for that key.
///
/// The index is best-effort: a crash before it is written costs a missed dedup,
/// and a crash that leaves it orphaned is self-healed by the next enqueue, which
/// sees the pending file absent.
#[derive(Debug, Serialize, Deserialize)]
pub struct LockKeyIndex {
    pub storage_key: String,
}

fn serialize_lock_key_index(storage_key: &str) -> Result<Vec<u8>, Error> {
    serde_json::to_vec(&LockKeyIndex {
        storage_key: storage_key.to_string(),
    })
    .map_err(|e| Error::Storage(format!("failed to serialize lock-key index: {e}")))
}

fn parse_lock_key_index(bytes: &[u8]) -> Result<LockKeyIndex, Error> {
    serde_json::from_slice(bytes)
        .map_err(|e| Error::Storage(format!("failed to parse lock-key index: {e}")))
}

/// On-disk shape of a dead-letter record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeadLetterRecord {
    #[serde(flatten)]
    pub envelope: JobEnvelope,
    pub last_error: String,
    pub failed_at: DateTime<Utc>,
}

/// Executor for a single job kind: `Ok(())` reports success, `Err` fails the
/// job over to the retry/dead-letter policy.
///
/// # Idempotency
///
/// Handlers MUST be idempotent: a lost claim or a crash between execution and
/// cleanup re-runs the job, so a duplicate run is possible but a lost job is not.
#[async_trait]
pub trait JobHandler: Send + Sync {
    async fn execute(&self, envelope: &JobEnvelope) -> Result<(), Error>;
}

/// Maximum number of pending envelopes inspected per scan; deeper queues rely on
/// claim serialization, which is the actual correctness primitive.
const MAX_SCAN: u16 = 1000;

/// Cap on the pending/failed gauges. The gauge feeds autoscaling, which needs
/// only ordinal granularity at high depth, and the cap bounds S3 `LIST` cost per
/// refresh tick; read the cap value as "at least this many".
const MAX_REPORTED_PENDING: u64 = 10_000;

/// Default lease on a claim key, overridden by
/// `[global.job_queue].claim_ttl_secs`.
const CLAIM_TTL_SECS: u64 = 60;

/// Advisory-claim settle floor: how long a racing claimant's PUT gets to land
/// before the verify read decides the winner.
const ADVISORY_SETTLE_BASE_MS: u64 = 100;

/// Upper bound on the random extra settle, decorrelating claimants that PUT in
/// the same instant so their verify reads do not tie.
const ADVISORY_SETTLE_JITTER_MS: u64 = 200;

/// The stored body of a claim key.
#[derive(Serialize, Deserialize)]
struct ClaimRecord {
    instance: String,
    expires_at: DateTime<Utc>,
}

/// A held claim: the key, the instance token proving ownership, the token the
/// refresh task raises when the claim is lost, and the refresh task itself.
pub struct JobClaim {
    key: String,
    instance: String,
    lost: CancellationToken,
    refresher: Option<JoinHandle<()>>,
}

impl JobClaim {
    /// Whether the lease lapsed or another worker took the key over, in which
    /// case the holder must stop touching queue state it no longer owns.
    fn lost(&self) -> bool {
        self.lost.is_cancelled()
    }
}

impl Drop for JobClaim {
    fn drop(&mut self) {
        if let Some(refresher) = self.refresher.take() {
            refresher.abort();
        }
    }
}

/// A job claimed by a worker, ready to execute. `storage_key` identifies the
/// pending file the envelope came from so `complete`/`fail` can delete or
/// rewrite it, and the claim is released by those same calls.
pub struct ClaimedJob {
    pub envelope: JobEnvelope,
    pub storage_key: String,
    claim: JobClaim,
}

impl ClaimedJob {
    /// Cancelled the moment the claim's lease is lost, so the runner can abort a
    /// handler mid-execution.
    pub fn lock_lost(&self) -> CancellationToken {
        self.claim.lost.clone()
    }

    /// Test-only: a claimed job over a hand-built claim, so runner tests can
    /// drive the lost-claim branch without a backend.
    #[cfg(test)]
    pub fn for_test(envelope: JobEnvelope, storage_key: String, lost: CancellationToken) -> Self {
        let claim = JobClaim {
            key: job_claim_path(&envelope.lock_key),
            instance: String::new(),
            lost,
            refresher: None,
        };
        Self {
            envelope,
            storage_key,
            claim,
        }
    }
}

pub enum FailOutcome {
    Retried { next_at: DateTime<Utc> },
    MovedToDeadLetter,
}

/// Outcome of [`JobStore::complete`]. A cleanup failure is not surfaced as an
/// error: the job is failed over instead, so a persistently failing cleanup
/// cannot leave the pending file re-claimable in a hot loop.
pub enum CompleteOutcome {
    Completed,
    /// The cleanup failed and the job was failed over via [`JobStore::fail`].
    FailedOver(FailOutcome),
}

/// Outcome of one `claim_one` attempt. When nothing was claimed, `next_ready`
/// carries the soonest `not_before` seen so the caller can sleep until then
/// instead of polling through unchanged backed-off envelopes.
pub struct ClaimOutcome {
    pub claimed: Option<ClaimedJob>,
    pub next_ready: Option<DateTime<Utc>>,
}

impl ClaimOutcome {
    /// How long to idle before the next `claim_one`: the soonest `not_before`,
    /// clamped to `[poll_interval, max(poll_interval, 1 min)]` so the worker
    /// still picks up newly-enqueued ready jobs promptly.
    pub fn idle_sleep(&self, poll_interval: Duration) -> Duration {
        let max_sleep = poll_interval.max(Duration::from_mins(1));
        self.next_ready.map_or(poll_interval, |t| {
            (t - Utc::now())
                .to_std()
                .unwrap_or_default()
                .clamp(poll_interval, max_sleep)
        })
    }
}

/// Job-queue producer (`enqueue`), consumer (`claim_one` / `complete` / `fail`),
/// and storage primitives.
///
/// All writes go straight to the `ObjectStore`: atomicity rests on
/// `create_if_absent` for dedup and claims, and on ordered idempotent writes
/// everywhere else.
pub struct JobStore {
    store: Arc<dyn ObjectStore>,
    worker_id: String,
    claim_mode: ClaimMode,
    retry_backoff: Backoff,
    claim_error_backoff: Backoff,
    consecutive_claim_errors: AtomicU32,
    max_attempts: u32,
    /// Claim-lease seconds, clamped at construction so chrono arithmetic on it
    /// cannot overflow.
    claim_ttl_secs: i64,
}

impl JobStore {
    /// Construct a `JobStore` over the metadata store's backend, which the
    /// queue always shares, with the default retry policy. `worker_id` is a
    /// structured-log tag (empty for producer-only instances) and `claim_mode`
    /// comes from [`ensure_claim_support`].
    pub fn new(
        metadata_store: &MetadataStore,
        worker_id: impl Into<String>,
        claim_mode: ClaimMode,
    ) -> Self {
        Self::with_retry_policy(
            metadata_store,
            worker_id,
            claim_mode,
            JobRetryPolicy::default(),
        )
    }

    /// [`Self::new`] with an operator-configured retry policy.
    pub fn with_retry_policy(
        metadata_store: &MetadataStore,
        worker_id: impl Into<String>,
        claim_mode: ClaimMode,
        retry: JobRetryPolicy,
    ) -> Self {
        let backoff = || {
            Backoff::exponential(
                Duration::from_millis(retry.backoff_min_ms),
                Duration::from_millis(retry.backoff_max_ms),
            )
        };
        Self {
            store: metadata_store.object_store().clone(),
            worker_id: worker_id.into(),
            claim_mode,
            retry_backoff: backoff(),
            claim_error_backoff: backoff(),
            consecutive_claim_errors: AtomicU32::new(0),
            max_attempts: retry.max_attempts,
            claim_ttl_secs: i64::try_from(retry.claim_ttl_secs)
                .unwrap_or(i64::MAX)
                .clamp(1, 86_400),
        }
    }

    /// List up to `n` pending storage keys in ascending (readiness) order.
    pub async fn list_pending(&self, queue: Queue, n: u16) -> Result<Vec<String>, Error> {
        let prefix = job_pending_dir(queue.as_str());
        let page = self.store.list(&prefix, n, None).await?;

        Ok(page
            .items
            .into_iter()
            .filter_map(|name| name.strip_suffix(".json").map(str::to_string))
            .collect())
    }

    pub async fn read_pending(
        &self,
        queue: Queue,
        storage_key: &str,
    ) -> Result<JobEnvelope, Error> {
        let key = job_pending_path(queue.as_str(), storage_key);
        let data = self.store.get(&key).await?;
        serde_json::from_slice(&data)
            .map_err(|e| Error::Corrupt(format!("failed to parse envelope: {e}")))
    }

    /// Read a dead-letter record, [`Error::NotFound`] for a stale key.
    pub async fn read_failed(
        &self,
        queue: Queue,
        storage_key: &str,
    ) -> Result<DeadLetterRecord, Error> {
        let key = job_failed_path(queue.as_str(), storage_key);
        let data = self.store.get(&key).await?;
        serde_json::from_slice(&data)
            .map_err(|e| Error::Corrupt(format!("failed to parse dead-letter: {e}")))
    }

    /// One keyset page of pending storage keys in ascending (time) order; pass
    /// `after = Some(last_key)` from a previous page to resume.
    pub async fn list_pending_page(
        &self,
        queue: Queue,
        n: u16,
        after: Option<&str>,
    ) -> Result<Page<String>, Error> {
        self.list_page(&job_pending_dir(queue.as_str()), n, after)
            .await
    }

    /// One keyset page of dead-letter storage keys in ascending (failure-time)
    /// order. See [`Self::list_pending_page`] for the cursor contract.
    pub async fn list_failed_page(
        &self,
        queue: Queue,
        n: u16,
        after: Option<&str>,
    ) -> Result<Page<String>, Error> {
        self.list_page(&job_failed_dir(queue.as_str()), n, after)
            .await
    }

    /// Shared keyset pager over a job directory. The cursor is suffixed with
    /// `.json` because `start_after` matches the stored child name.
    async fn list_page(
        &self,
        dir: &str,
        n: u16,
        after: Option<&str>,
    ) -> Result<Page<String>, Error> {
        let start_after = after.map(|k| format!("{k}.json"));
        let page = self.store.list_children(dir, n, None, start_after).await?;
        let keys: Vec<String> = page
            .objects
            .into_iter()
            .filter_map(|name| name.strip_suffix(".json").map(str::to_string))
            .collect();
        // The backend's `next_token` says whether more entries exist, but the
        // cursor we hand back is our own last storage key.
        let next_token = page
            .next_token
            .is_some()
            .then(|| keys.last().cloned())
            .flatten();
        Ok(Page {
            items: keys,
            next_token,
        })
    }

    /// Count pending envelopes ready for handling within
    /// `[..., now + ready_horizon_secs]`. Capped at `MAX_REPORTED_PENDING`.
    pub async fn count_pending(&self, queue: Queue, ready_horizon_secs: u64) -> Result<u64, Error> {
        let prefix = job_pending_dir(queue.as_str());
        let cutoff_prefix = pending_ready_cutoff_prefix(ready_horizon_secs);
        let mut names = self.store.list_in_order(&prefix);
        let mut count: u64 = 0;
        while count < MAX_REPORTED_PENDING
            && let Some(name) = names.try_next().await?
        {
            let Some(stem) = name.strip_suffix(".json") else {
                continue;
            };
            // Lex order equals `not_before` order, so the first key past the
            // cutoff ends the count.
            if let Some(p) = stem.get(..STORAGE_KEY_PREFIX_LEN)
                && p > cutoff_prefix.as_str()
            {
                break;
            }
            count += 1;
        }
        Ok(count)
    }

    /// Count dead-lettered envelopes in `queue`, capped at
    /// `MAX_REPORTED_PENDING`.
    pub async fn count_failed(&self, queue: Queue) -> Result<u64, Error> {
        let prefix = job_failed_dir(queue.as_str());
        let mut names = self.store.list_in_order(&prefix);
        let mut count: u64 = 0;
        while count < MAX_REPORTED_PENDING
            && let Some(name) = names.try_next().await?
        {
            count += u64::from(name.strip_suffix(".json").is_some());
        }
        Ok(count)
    }

    /// `true` when any pending job in `queue` carries `lock_key`, via the O(1)
    /// [`LockKeyIndex`] rather than a body scan.
    ///
    /// A failure to clear an orphan index is returned as an error rather than
    /// `false`, so the caller never proceeds to an enqueue that the lingering
    /// index would silently coalesce away.
    pub async fn find_pending_with_lock_key(
        &self,
        queue: Queue,
        lock_key: &LockKey,
    ) -> Result<bool, Error> {
        let index_path = job_lock_key_index_path(queue.as_str(), lock_key);
        let data = match self.store.get(&index_path).await {
            Ok(d) => d,
            Err(StorageError::NotFound) => return Ok(false),
            Err(e) => return Err(Error::from(e)),
        };
        let index = parse_lock_key_index(&data)?;

        let pending_key = job_pending_path(queue.as_str(), &index.storage_key);
        if self.store.exists(&pending_key).await? {
            return Ok(true);
        }
        // Orphan index: remove it so the caller's enqueue does not collide on
        // the atomic create and drop a distinct job as a false dedup hit.
        if let Err(e) = self.store.delete(&index_path).await {
            warn!(
                lock_key = %lock_key,
                error = %e,
                "Failed to remove orphan lock-key index",
            );
            return Err(Error::from(e));
        }
        Ok(false)
    }

    /// Retire the dedup index of a just-claimed job so a same-`lock_key` enqueue
    /// arriving mid-execution starts a fresh pending file instead of coalescing
    /// into the running job and dropping its write.
    ///
    /// `false` means the index may still point at `storage_key`, so the claim
    /// must not proceed.
    async fn retire_claimed_index(
        &self,
        queue: Queue,
        lock_key: &LockKey,
        storage_key: &str,
    ) -> bool {
        match self
            .cleanup_index_if_ours(queue, lock_key, storage_key)
            .await
        {
            Ok(()) => true,
            Err(e) => {
                warn!(%lock_key, error = %e, "Failed to retire dedup index at claim");
                false
            }
        }
    }

    /// Delete the dedup index when it still points at `storage_key` (or does not
    /// parse). Read-then-delete, so a producer re-pointing the index in the
    /// window loses its entry: a missed dedup, never a lost job, since pending
    /// files are claimed off the scan and not off the index.
    async fn cleanup_index_if_ours(
        &self,
        queue: Queue,
        lock_key: &LockKey,
        storage_key: &str,
    ) -> Result<(), Error> {
        let index_path = job_lock_key_index_path(queue.as_str(), lock_key);
        let ours = match self.store.get(&index_path).await {
            Ok(body) => {
                parse_lock_key_index(&body).map_or(true, |index| index.storage_key == storage_key)
            }
            Err(StorageError::NotFound) => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        if ours {
            self.store.delete(&index_path).await?;
        }
        Ok(())
    }

    /// Try to claim `lock_key`: a held unexpired claim yields `None`, a lapsed
    /// one is deleted and re-created. The takeover is deliberately unfenced, so
    /// racing claimants may both execute until each loser's refresher notices,
    /// costing a duplicate run of an idempotent handler and never a lost job.
    async fn try_claim(&self, lock_key: &LockKey) -> Result<Option<JobClaim>, Error> {
        let key = job_claim_path(lock_key);
        let instance = Uuid::new_v4().to_string();
        let mut stamped_until = self.acquire_claim(&key, &instance).await?;
        if stamped_until.is_none() {
            if !self.claim_is_stale(&key).await? {
                return Ok(None);
            }
            self.store.delete(&key).await?;
            stamped_until = self.acquire_claim(&key, &instance).await?;
        }
        let Some(stamped_until) = stamped_until else {
            return Ok(None);
        };
        let lost = CancellationToken::new();
        let refresher = spawn(refresh_claim_loop(
            self.store.clone(),
            key.clone(),
            instance.clone(),
            stamped_until,
            self.claim_ttl_secs,
            lost.clone(),
        ));
        Ok(Some(JobClaim {
            key,
            instance,
            lost,
            refresher: Some(refresher),
        }))
    }

    /// Acquire the claim record per the probed [`ClaimMode`]. Advisory
    /// acquisition can let two claimants transiently coexist, which stays safe
    /// because every downstream path detects a foreign instance in the record
    /// and stands down.
    async fn acquire_claim(
        &self,
        key: &str,
        instance: &str,
    ) -> Result<Option<DateTime<Utc>>, Error> {
        match self.claim_mode {
            ClaimMode::Atomic => self.put_claim_if_absent(key, instance).await,
            ClaimMode::Advisory => self.put_claim_advisory(key, instance).await,
        }
    }

    /// Advisory acquisition: lose to a fresh foreign record, otherwise put our
    /// record, settle, and win only if the read-back still carries this instance.
    async fn put_claim_advisory(
        &self,
        key: &str,
        instance: &str,
    ) -> Result<Option<DateTime<Utc>>, Error> {
        match self.store.get(key).await {
            Ok(raw) => {
                if let Ok(record) = serde_json::from_slice::<ClaimRecord>(&raw)
                    && record.instance != instance
                    && record.expires_at >= Utc::now()
                {
                    return Ok(None);
                }
            }
            Err(StorageError::NotFound) => {}
            Err(e) => return Err(Error::from(e)),
        }
        let (_, body) = self.claim_body(instance)?;
        self.store.put(key, body).await?;
        sleep(Duration::from_millis(
            ADVISORY_SETTLE_BASE_MS + jitter_below(ADVISORY_SETTLE_JITTER_MS + 1),
        ))
        .await;
        match self.store.get(key).await {
            Ok(raw) => Ok(serde_json::from_slice::<ClaimRecord>(&raw)
                .ok()
                .filter(|read_back| read_back.instance == instance)
                .map(|read_back| read_back.expires_at)),
            Err(StorageError::NotFound) => Ok(None),
            Err(e) => Err(Error::from(e)),
        }
    }

    /// Atomically create the claim record; returns its `expires_at` when the
    /// create won, `None` when the key already exists.
    async fn put_claim_if_absent(
        &self,
        key: &str,
        instance: &str,
    ) -> Result<Option<DateTime<Utc>>, Error> {
        let (expires_at, body) = self.claim_body(instance)?;
        let created = self.store.create_if_absent(key, body).await?;
        Ok(created.then_some(expires_at))
    }

    /// A fresh claim record for `instance`: its expiry and the bytes to store.
    fn claim_body(&self, instance: &str) -> Result<(DateTime<Utc>, Bytes), Error> {
        let record = ClaimRecord {
            instance: instance.to_string(),
            expires_at: Utc::now() + ChronoDuration::seconds(self.claim_ttl_secs),
        };
        let body = serde_json::to_vec(&record)
            .map_err(|e| Error::Storage(format!("claim serialization failed: {e}")))?;
        Ok((record.expires_at, Bytes::from(body)))
    }

    /// Whether the claim at `key` is safe to take over: its lease lapsed, or
    /// it is unreadable and old enough that no live refresher can own it.
    async fn claim_is_stale(&self, key: &str) -> Result<bool, Error> {
        match self.store.get(key).await {
            Ok(raw) => {
                if let Ok(record) = serde_json::from_slice::<ClaimRecord>(&raw) {
                    return Ok(record.expires_at < Utc::now());
                }
            }
            Err(StorageError::NotFound) => return Ok(true),
            Err(e) => return Err(Error::from(e)),
        }
        // Unreadable: age it on its mtime, with double the lease as the margin,
        // so a corrupt claim cannot wedge the lock key forever.
        let meta = match self.store.head(key).await {
            Ok(meta) => meta,
            Err(StorageError::NotFound) => return Ok(true),
            Err(e) => return Err(Error::from(e)),
        };
        // With no timestamp there is nothing to age against, so read it as
        // stale: duplicate execution is tolerated, a wedged lock key is not.
        Ok(meta.last_modified.is_none_or(|modified| {
            Utc::now().signed_duration_since(modified).num_seconds() > self.claim_ttl_secs * 2
        }))
    }

    /// Release a claim the holder still owns; a lost or taken-over claim is
    /// left for its new owner.
    async fn release_claim(&self, mut claim: JobClaim) {
        if let Some(refresher) = claim.refresher.take() {
            refresher.abort();
        }
        if claim.lost() {
            return;
        }
        match self.store.get(&claim.key).await {
            Ok(raw) => {
                if serde_json::from_slice::<ClaimRecord>(&raw)
                    .is_ok_and(|record| record.instance == claim.instance)
                    && let Err(e) = self.store.delete(&claim.key).await
                {
                    warn!("job queue: failed to release claim '{}': {e}", claim.key);
                }
            }
            Err(StorageError::NotFound) => {}
            Err(e) => warn!(
                "job queue: failed to read claim '{}' at release: {e}",
                claim.key
            ),
        }
    }

    /// Enqueue a job, deduplicating on its `lock_key`: the index is checked
    /// first, then created atomically ahead of the pending file. When two
    /// replicas race and both see absence, only one create wins and the loser is
    /// a dedup hit.
    pub async fn enqueue(&self, mut envelope: JobEnvelope) -> Result<(), Error> {
        envelope.max_attempts.get_or_insert(self.max_attempts);

        // A lookup error (including a failed orphan self-heal) is propagated
        // rather than read as a miss, so a lingering orphan index cannot make
        // the slow path coalesce this distinct job away.
        if self
            .find_pending_with_lock_key(envelope.queue, &envelope.lock_key)
            .await?
        {
            metrics_provider()
                .job_queue_enqueued_total
                .with_label_values(&[envelope.queue.as_str(), "hit"])
                .inc();
            return Ok(());
        }

        let storage_key = make_storage_key(Utc::now(), &envelope.id);
        let pending_path = job_pending_path(envelope.queue.as_str(), &storage_key);
        let index_path = job_lock_key_index_path(envelope.queue.as_str(), &envelope.lock_key);

        let pending_body = Bytes::from(
            serde_json::to_vec(&envelope)
                .map_err(|e| Error::Storage(format!("envelope serialization failed: {e}")))?,
        );
        let index_body = Bytes::from(serialize_lock_key_index(&storage_key)?);

        let objects = &self.store;
        let outcome = if objects.create_if_absent(&index_path, index_body).await? {
            objects
                .create_if_absent(&pending_path, pending_body)
                .await?;
            "miss"
        } else {
            "hit"
        };
        metrics_provider()
            .job_queue_enqueued_total
            .with_label_values(&[envelope.queue.as_str(), outcome])
            .inc();
        Ok(())
    }

    /// Claim the next available job from `queue`, sleeping a capped exponential
    /// backoff on a storage error so the caller can loop without managing retry
    /// timing itself.
    pub async fn claim_one(&self, queue: Queue) -> Result<ClaimOutcome, Error> {
        match self.try_claim_one(queue).await {
            Ok(outcome) => {
                self.consecutive_claim_errors.store(0, Ordering::Relaxed);
                Ok(outcome)
            }
            Err(error) => {
                let attempt = self
                    .consecutive_claim_errors
                    .fetch_add(1, Ordering::Relaxed);
                sleep(self.claim_error_backoff.delay(attempt)).await;
                Err(error)
            }
        }
    }

    /// Drop a pending record whose body will not parse: the scan walks keys in
    /// ascending time order, so leaving it would strand every job queued after
    /// it. Both queues converge without it, since a cache fill re-enqueues on
    /// the next pull-through and a replication push on the next write or sweep.
    async fn discard_poison_pending(&self, queue: Queue, storage_key: &str, reason: &str) {
        let path = job_pending_path(queue.as_str(), storage_key);
        match self.store.delete(&path).await {
            Ok(()) => {
                warn!("job queue: discarded unreadable pending job '{storage_key}': {reason}");
            }
            Err(e) => {
                warn!(
                    "job queue: unreadable pending job '{storage_key}' ({reason}) not deleted: {e}"
                );
            }
        }
    }

    /// Walk pending keys in ascending `not_before` order and claim the first
    /// ready one, stopping at the first key scheduled in the future and
    /// reporting that instant as `next_ready`.
    async fn try_claim_one(&self, queue: Queue) -> Result<ClaimOutcome, Error> {
        let now = Utc::now();
        let mut next_ready: Option<DateTime<Utc>> = None;
        for storage_key in self.list_pending(queue, MAX_SCAN).await? {
            // The filename prefix is the readiness signal, so a backed-off entry
            // never costs a body GET.
            if let Some(not_before) = parse_not_before(&storage_key)
                && not_before > now
            {
                next_ready = Some(not_before);
                break;
            }
            let envelope = match self.read_pending(queue, &storage_key).await {
                Ok(e) => e,
                Err(Error::NotFound) => continue,
                Err(Error::Corrupt(e)) => {
                    self.discard_poison_pending(queue, &storage_key, &e).await;
                    continue;
                }
                Err(e) => return Err(e),
            };
            if let Some(claim) = self.try_claim(&envelope.lock_key).await? {
                // Re-read under the claim: another worker may have completed this
                // job between the first read and the claim, and re-running the
                // stale envelope could dead-letter a finished job.
                let envelope = match self.read_pending(queue, &storage_key).await {
                    Ok(envelope) => envelope,
                    Err(Error::NotFound) => {
                        self.release_claim(claim).await;
                        continue;
                    }
                    Err(Error::Corrupt(e)) => {
                        self.discard_poison_pending(queue, &storage_key, &e).await;
                        self.release_claim(claim).await;
                        continue;
                    }
                    Err(e) => {
                        self.release_claim(claim).await;
                        return Err(e);
                    }
                };
                // Claiming while the index still points here would coalesce every
                // same-`lock_key` enqueue into this job for the whole execution
                // and drop its write, so leave the job for the next scan.
                if !self
                    .retire_claimed_index(queue, &envelope.lock_key, &storage_key)
                    .await
                {
                    self.release_claim(claim).await;
                    continue;
                }
                debug!(
                    lock_key = envelope.lock_key.as_str(),
                    worker_id = self.worker_id.as_str(),
                    "Claimed job"
                );
                return Ok(ClaimOutcome {
                    claimed: Some(ClaimedJob {
                        envelope,
                        storage_key,
                        claim,
                    }),
                    next_ready: None,
                });
            }
        }
        Ok(ClaimOutcome {
            claimed: None,
            next_ready,
        })
    }

    /// Mark a claimed job complete: delete its pending file, retire the dedup
    /// index when it still points at it, and release the claim. A lost claim
    /// skips the cleanup, since the key's new holder owns that state now.
    pub async fn complete(&self, claimed: ClaimedJob) -> Result<CompleteOutcome, Error> {
        let ClaimedJob {
            envelope,
            storage_key,
            claim,
        } = claimed;
        if claim.lost() {
            warn!(
                lock_key = envelope.lock_key.as_str(),
                "job queue: claim lapsed during execution; leaving cleanup to the new holder"
            );
            return Ok(CompleteOutcome::Completed);
        }
        let pending_path = job_pending_path(envelope.queue.as_str(), &storage_key);
        let cleanup = async {
            self.store.delete(&pending_path).await?;
            self.cleanup_index_if_ours(envelope.queue, &envelope.lock_key, &storage_key)
                .await
        };
        match cleanup.await {
            Ok(()) => {
                self.release_claim(claim).await;
                Ok(CompleteOutcome::Completed)
            }
            Err(e) => {
                // Fail the job over rather than leave the pending file
                // re-claimable in a hot loop; the claim is still held.
                let claimed = ClaimedJob {
                    envelope,
                    storage_key,
                    claim,
                };
                self.fail(claimed, &format!("complete: cleanup failed: {e}"))
                    .await
                    .map(CompleteOutcome::FailedOver)
            }
        }
    }

    /// Record a failure: re-queue with backoff, or dead-letter once the retry
    /// budget is exhausted.
    pub async fn fail(&self, claimed: ClaimedJob, err: &str) -> Result<FailOutcome, Error> {
        let new_attempts = claimed.envelope.attempts.saturating_add(1);

        // The queue's budget stands in for an envelope that reached here without
        // going through `enqueue`.
        if new_attempts >= claimed.envelope.max_attempts.unwrap_or(self.max_attempts) {
            return self.fail_terminal(claimed, err).await;
        }

        let ClaimedJob {
            envelope,
            storage_key,
            claim,
        } = claimed;
        let delay = self.retry_backoff.delay(new_attempts);
        let next_at = Utc::now() + ChronoDuration::from_std(delay).unwrap_or_default();
        let updated = JobEnvelope {
            attempts: new_attempts,
            ..envelope
        };

        self.fail_retry(claim, updated, storage_key, next_at).await
    }

    /// Rewrite the pending file under a new storage key encoding the bumped
    /// `not_before`, then delete the old one; a crash between the two re-runs the
    /// old envelope, which handler idempotency covers.
    async fn fail_retry(
        &self,
        claim: JobClaim,
        updated: JobEnvelope,
        old_storage_key: String,
        next_at: DateTime<Utc>,
    ) -> Result<FailOutcome, Error> {
        // A lost claim must not touch queue state the key's new holder now owns.
        if claim.lost() {
            warn!(
                lock_key = updated.lock_key.as_str(),
                "job queue: claim lapsed during execution; leaving the retry to the new holder"
            );
            return Ok(FailOutcome::Retried { next_at });
        }
        let new_storage_key = make_storage_key(next_at, &updated.id);
        let new_pending_path = job_pending_path(updated.queue.as_str(), &new_storage_key);
        let old_pending_path = job_pending_path(updated.queue.as_str(), &old_storage_key);
        let index_path = job_lock_key_index_path(updated.queue.as_str(), &updated.lock_key);

        let pending_body = Bytes::from(
            serde_json::to_vec(&updated)
                .map_err(|e| Error::Storage(format!("envelope serialization failed: {e}")))?,
        );
        let index_body = Bytes::from(serialize_lock_key_index(&new_storage_key)?);

        let rewrite = async {
            self.store.put(&new_pending_path, pending_body).await?;
            self.store.delete(&old_pending_path).await?;
            // Index the rescheduled job only while the entry is absent: a
            // producer that indexed a fresh job meanwhile keeps its entry, since
            // a missed dedup costs a duplicate job while clobbering the entry
            // would strand that producer's pending file.
            let _ = self.store.create_if_absent(&index_path, index_body).await?;
            Ok::<_, StorageError>(())
        };
        let result = rewrite.await;
        self.release_claim(claim).await;
        result?;

        Ok(FailOutcome::Retried { next_at })
    }

    /// Dead-letter a job immediately, bypassing the retry budget: write the
    /// record, then drop the pending file and the dedup index when it still
    /// points at this job. Record first, so a crash duplicates into the dead
    /// letter rather than losing the failure.
    pub async fn fail_terminal(
        &self,
        claimed: ClaimedJob,
        err: &str,
    ) -> Result<FailOutcome, Error> {
        let ClaimedJob {
            envelope,
            storage_key,
            claim,
        } = claimed;
        // A lost claim must not touch queue state the key's new holder now owns.
        if claim.lost() {
            warn!(
                lock_key = envelope.lock_key.as_str(),
                "job queue: claim lapsed during execution; leaving the dead-letter to the new holder"
            );
            return Ok(FailOutcome::MovedToDeadLetter);
        }
        let failed_path = job_failed_path(envelope.queue.as_str(), &storage_key);
        let pending_path = job_pending_path(envelope.queue.as_str(), &storage_key);

        let record = DeadLetterRecord {
            envelope,
            last_error: err.to_string(),
            failed_at: Utc::now(),
        };
        let failed_body = Bytes::from(
            serde_json::to_vec(&record)
                .map_err(|e| Error::Storage(format!("failed to serialize dead-letter: {e}")))?,
        );
        let envelope = &record.envelope;

        let bury = async {
            self.store.put(&failed_path, failed_body).await?;
            self.store.delete(&pending_path).await?;
            self.cleanup_index_if_ours(envelope.queue, &envelope.lock_key, &storage_key)
                .await
        };
        let result = bury.await;
        self.release_claim(claim).await;
        result?;

        Ok(FailOutcome::MovedToDeadLetter)
    }

    /// Move a dead-letter record back to pending with its retry budget reset to
    /// zero, [`Error::NotFound`] for a stale key.
    ///
    /// The dedup index is deliberately not re-established, so a concurrent
    /// producer may create a second pending file for the same `lock_key`; the
    /// per-`lock_key` claim still serialises the two.
    pub async fn retry_failed(&self, queue: Queue, storage_key: &str) -> Result<(), Error> {
        let failed_path = job_failed_path(queue.as_str(), storage_key);
        // Surface a stale key as `NotFound` before touching any state; a
        // double-retry collides on the atomic create below.
        self.store.head(&failed_path).await?;

        let mut envelope = self.read_failed(queue, storage_key).await?.envelope;
        envelope.attempts = 0;

        let new_storage_key = make_storage_key(Utc::now(), &envelope.id);
        let pending_path = job_pending_path(queue.as_str(), &new_storage_key);
        let pending_body = Bytes::from(
            serde_json::to_vec(&envelope)
                .map_err(|e| Error::Storage(format!("envelope serialization failed: {e}")))?,
        );

        // Pending first, so a crash duplicates into a re-runnable job rather
        // than losing the record.
        let _ = self
            .store
            .create_if_absent(&pending_path, pending_body)
            .await?;
        self.store.delete(&failed_path).await?;
        Ok(())
    }

    /// Delete a job by `state`/`storage_key`, [`Error::NotFound`] for a stale
    /// key. Deleting a pending job is best-effort against a worker that may be
    /// mid-execution: the operator holds no claim, so an in-flight execution
    /// still commits its handler effect.
    pub async fn delete_job(
        &self,
        queue: Queue,
        state: JobState,
        storage_key: &str,
    ) -> Result<(), Error> {
        match state {
            JobState::Failed => {
                let failed_path = job_failed_path(queue.as_str(), storage_key);
                // Surface a stale key as `NotFound` before the idempotent delete.
                self.store.head(&failed_path).await?;
                self.store.delete(&failed_path).await.map_err(Error::from)
            }
            JobState::Pending => self.delete_pending(queue, storage_key).await,
        }
    }

    async fn delete_pending(&self, queue: Queue, storage_key: &str) -> Result<(), Error> {
        let pending_path = job_pending_path(queue.as_str(), storage_key);
        // Only the `lock_key` needs the body, so a record that will not parse is
        // still deletable: retiring it is the one recovery path, and its index
        // entry is reclaimed by the orphan-job sweep.
        let lock_key = match self.read_pending(queue, storage_key).await {
            Ok(envelope) => Some(envelope.lock_key),
            Err(Error::Corrupt(e)) => {
                warn!("job queue: deleting unreadable pending job '{storage_key}': {e}");
                None
            }
            Err(e) => return Err(e),
        };
        // Surface a stale key as `NotFound` before the idempotent delete.
        self.store.head(&pending_path).await?;
        self.store.delete(&pending_path).await?;
        if let Some(lock_key) = lock_key {
            self.cleanup_index_if_ours(queue, &lock_key, storage_key)
                .await?;
        }
        Ok(())
    }
}

/// What one refresh-tick read of the claim key concluded.
enum ClaimCheck {
    Owned {
        expires_at: DateTime<Utc>,
    },
    /// The record is gone, unparseable, expired, or carries another instance.
    Lost,
    /// The read failed, so ownership is unknown this tick.
    Unverifiable,
}

/// Whether the refresher must raise `lost`: a positive loss always cancels,
/// while an unverifiable read only cancels once the last verified expiry has
/// passed, because the lease itself is the tolerance for transient errors.
fn should_cancel_claim(
    check: &ClaimCheck,
    last_verified_expiry: DateTime<Utc>,
    now: DateTime<Utc>,
) -> bool {
    match check {
        ClaimCheck::Owned { .. } => false,
        ClaimCheck::Lost => true,
        ClaimCheck::Unverifiable => now >= last_verified_expiry,
    }
}

/// The claim holder's lease refresher: re-stamp the record at a third of the
/// lease, raising `lost` when the key no longer carries this instance or when
/// read errors persist past the last verified expiry. The re-stamp is an
/// unconditional put that can overwrite a takeover's record, costing at most a
/// bounded duplicate run of an idempotent handler.
async fn refresh_claim_loop(
    store: Arc<dyn ObjectStore>,
    key: String,
    instance: String,
    stamped_until: DateTime<Utc>,
    claim_ttl_secs: i64,
    lost: CancellationToken,
) {
    let period = Duration::from_secs(u64::try_from(claim_ttl_secs / 3).unwrap_or(20).max(1));
    let mut last_verified_expiry = stamped_until;
    loop {
        sleep(period).await;
        let check = match store.get(&key).await {
            Ok(raw) => match serde_json::from_slice::<ClaimRecord>(&raw) {
                Ok(record) if record.instance == instance && record.expires_at >= Utc::now() => {
                    ClaimCheck::Owned {
                        expires_at: record.expires_at,
                    }
                }
                _ => ClaimCheck::Lost,
            },
            Err(StorageError::NotFound) => ClaimCheck::Lost,
            Err(_) => ClaimCheck::Unverifiable,
        };
        if should_cancel_claim(&check, last_verified_expiry, Utc::now()) {
            lost.cancel();
            return;
        }
        let ClaimCheck::Owned { expires_at } = check else {
            continue;
        };
        last_verified_expiry = expires_at;
        let record = ClaimRecord {
            instance: instance.clone(),
            expires_at: Utc::now() + ChronoDuration::seconds(claim_ttl_secs),
        };
        let Ok(body) = serde_json::to_vec(&record) else {
            lost.cancel();
            return;
        };
        // A missed stamp is survivable inside the lease: the next tick retries,
        // and an actual lapse is what `lost` reports.
        if store.put(&key, Bytes::from(body)).await.is_ok() {
            last_verified_expiry = record.expires_at;
        }
    }
}

/// What the queue-depth gauges are refreshed from: the store to read, how
/// often to read it, and how far ahead an envelope still counts as pending.
/// A configuration reload replaces it wholesale, the way a reload replaces the
/// listener's server context.
pub struct QueueDepthRefresh {
    pub store: Arc<JobStore>,
    pub period: Duration,
    pub ready_horizon_secs: u64,
}

/// A ticker firing every `period`, its immediate first tick consumed so the
/// next refresh is a full period away.
async fn refresh_timer(period: Duration) -> Interval {
    let mut timer = interval(period);
    timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
    timer.tick().await;
    timer
}

/// Refresh the `angos_job_queue_pending` and `angos_job_queue_failed` gauges for
/// `queue` until `shutdown` is cancelled, reading `refresh` on every tick so a
/// configuration reload is picked up on the next one: the store the gauges
/// describe is then the store requests are enqueued into.
pub async fn queue_depth_refresh_loop(
    refresh: Arc<ArcSwap<QueueDepthRefresh>>,
    queue: Queue,
    shutdown: CancellationToken,
) {
    let mut period = refresh.load().period;
    let mut timer = refresh_timer(period).await;

    loop {
        select! {
            () = shutdown.cancelled() => return,
            _ = timer.tick() => {}
        }
        let current = refresh.load_full();
        // Only a changed cadence rebuilds the ticker, so an unrelated reload
        // does not shift the refresh phase.
        if current.period != period {
            period = current.period;
            timer = refresh_timer(period).await;
        }
        let (store, ready_horizon_secs) = (&current.store, current.ready_horizon_secs);
        match store.count_pending(queue, ready_horizon_secs).await {
            Ok(count) => {
                metrics_provider()
                    .job_queue_pending
                    .with_label_values(&[queue.as_str()])
                    .set(i64::try_from(count).unwrap_or(i64::MAX));
            }
            Err(e) => debug!(queue = %queue, error = %e, "Failed to refresh pending gauge"),
        }
        match store.count_failed(queue).await {
            Ok(count) => {
                metrics_provider()
                    .job_queue_failed
                    .with_label_values(&[queue.as_str()])
                    .set(i64::try_from(count).unwrap_or(i64::MAX));
            }
            Err(e) => debug!(queue = %queue, error = %e, "Failed to refresh dead-letter gauge"),
        }
    }
}

#[cfg(test)]
#[path = "store_tests.rs"]
mod tests;
