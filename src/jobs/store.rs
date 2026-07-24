//! [`JobStore`]: unified job-queue storage, producer, and consumer backed by
//! a storage façade (`Store`) carrying the object store and transaction
//! executor.
//!
//! All write operations that require atomicity (enqueue dedup, complete,
//! fail/retry, dead-letter) are handled by the transaction engine and never
//! touch the object-store layer directly.
//!
//! The orphan self-heal inside [`JobStore::find_pending_with_lock_key`] also
//! goes through the engine: it submits a one-mutation transaction whose read
//! fingerprint guards against a concurrent enqueue that refreshes the index
//! between the GET and the delete.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest as _, Sha256};
use tokio::{
    select,
    time::{MissedTickBehavior, interval, sleep},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};
use uuid::Uuid;

use angos_backoff::Backoff;
use angos_tx_engine::{
    StorageError,
    error::Error as TxError,
    lock::{Error as LockError, LockSession},
    store::Store,
    transaction::{Mutation, Read, Transaction},
};

use crate::{
    jobs::{JobState, Queue},
    metrics_provider::metrics_provider,
};

// Storage layout of the durable queues.

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

/// Path to the `lock_key` → `storage_key` dedup index file. Each pending
/// envelope has at most one such file alongside it; `find_pending_with_lock_key`
/// reads it for an O(1) lookup instead of scanning all pending bodies.
pub fn job_lock_key_index_path(queue: &str, lock_key: &str) -> String {
    format!(
        "{JOBS_ROOT}/index/{queue}/{}.json",
        encode_job_lock_key(lock_key)
    )
}

/// Percent-encode characters that are unsafe as a filesystem filename or as
/// part of an S3 key path component, so a `lock_key` lands on the same path
/// regardless of backend.
fn encode_job_lock_key(lock_key: &str) -> String {
    lock_key
        .chars()
        .map(|c| match c {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => {
                format!("%{:02X}", c as u32)
            }
            c => c.to_string(),
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("job queue initialization failed: {0}")]
    Initialization(String),
    #[error("storage error: {0}")]
    Storage(String),
    /// A job handler failed to do its work (deserialize its payload, reach an
    /// upstream, apply its effect). Distinct from [`Self::Storage`] so a handler
    /// fault does not dead-letter mislabelled as a queue storage error.
    #[error("job execution failed: {0}")]
    Execution(String),
    #[error("not found")]
    NotFound,
    #[error("denied: {0}")]
    Denied(String),
}

impl From<StorageError> for Error {
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::NotFound => Error::NotFound,
            other => Error::Storage(other.to_string()),
        }
    }
}

impl From<LockError> for Error {
    fn from(err: LockError) -> Self {
        Error::Storage(err.to_string())
    }
}

/// Precondition for `[global.job_queue]`: the durable queue is drained by
/// separate processes, so the engine lock that serializes job claims must
/// coordinate across processes. Config validation rejects strategies that are
/// statically in-process; this catches an unset S3 lock strategy that fell back
/// to the in-process lock because the probe found no conditional-write support.
pub fn ensure_shared_lock(store: &Store) -> Result<(), Error> {
    if !store.lock_is_process_shared() {
        return Err(Error::Initialization(
            "[global.job_queue] needs a shared lock strategy so workers serialize on the \
             same jobs across processes, but the metadata store's lock resolved to the \
             in-process 'memory' backend because the S3 provider lacks conditional-write \
             support. Set the metadata store's lock_strategy to \"redis\", or remove \
             [global.job_queue] to use the in-process queue."
                .to_string(),
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Job-queue tunables. `[global.job_queue]` does not change durability (jobs
/// persist under the store's `_jobs/` prefix either way); it switches draining
/// to separate `angos worker` processes and enables the queue-depth gauge.
///
/// Storage is inherited from the shared `[metadata_store]` configuration.
/// The per-`lock_key` execution lock TTL is governed by the configured lock
/// backend, so there is no job-queue-level TTL knob.
#[derive(Clone, Debug, Deserialize)]
pub struct JobQueueConfig {
    #[serde(
        default = "default_pending_refresh_interval_secs",
        deserialize_with = "deserialize_pending_refresh_interval_secs"
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
    /// The retry policy an operator configured for the durable queue.
    #[must_use]
    pub fn retry_policy(&self) -> JobRetryPolicy {
        JobRetryPolicy {
            max_attempts: self.max_attempts,
            backoff_min_ms: self.retry_backoff_min_ms,
            backoff_max_ms: self.retry_backoff_max_ms,
        }
    }
}

/// The retry budget and backoff bounds a [`JobStore`] applies to failing jobs.
/// Defaults match the historical in-code constants; the durable queue sources
/// them from [`JobQueueConfig`].
#[derive(Clone, Copy, Debug)]
pub struct JobRetryPolicy {
    pub max_attempts: u32,
    pub backoff_min_ms: u64,
    pub backoff_max_ms: u64,
}

impl Default for JobRetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: default_job_max_attempts(),
            backoff_min_ms: default_retry_backoff_min_ms(),
            backoff_max_ms: default_retry_backoff_max_ms(),
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

fn default_pending_refresh_interval_secs() -> u64 {
    15
}

fn default_pending_ready_horizon_secs() -> u64 {
    600
}

// ---------------------------------------------------------------------------
// JobEnvelope
// ---------------------------------------------------------------------------

/// Envelope that travels through the queue. The `payload` field is untyped so
/// the envelope shape stays stable across payload churn; handlers deserialize
/// it into a concrete type.
///
/// Note: `not_before` is **not** stored on the envelope. It is encoded in the
/// storage key (the filename stem) as a sortable hex unix-millis prefix; see
/// [`make_storage_key`]. The filename is the single source of truth so a
/// claim loop can decide readiness from a LIST result alone.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobEnvelope {
    pub id: String,
    /// Logical queue; selects the storage prefix and the worker's `--queue`
    /// filter.
    pub queue: Queue,
    /// Job type identifier (e.g. `"cache.fetch_blob"`). Handlers reject
    /// envelopes whose `kind` they do not recognize.
    pub kind: String,
    /// Per-key serialization token: at most one worker holds the execution
    /// lock per `lock_key`.
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
        queue: Queue,
        kind: impl Into<String>,
        lock_key: impl Into<String>,
        payload: &P,
    ) -> Result<Self, serde_json::Error> {
        Ok(Self {
            id: Uuid::new_v4().to_string(),
            queue,
            kind: kind.into(),
            lock_key: lock_key.into(),
            created_at: Utc::now(),
            attempts: 0,
            // 0 means "unset": `JobStore::enqueue` stamps the queue's configured
            // budget unless a caller set an explicit per-job value first.
            max_attempts: 0,
            payload: serde_json::to_value(payload)?,
        })
    }
}

// ---------------------------------------------------------------------------
// Storage-key helpers
// ---------------------------------------------------------------------------

/// Width of the hex unix-millis prefix in a storage key. 16 hex chars cover
/// `u64::MAX` milliseconds (well past year 5 billion), so the prefix is
/// fixed-width and lexicographic sort always matches time order.
pub const STORAGE_KEY_PREFIX_LEN: usize = 16;

/// Build a storage key (filename stem) encoding `not_before` as a sortable
/// hex unix-millis prefix followed by `-<id>`. Lexicographic sort of these
/// keys matches time order, so [`JobStore::list_pending`] returns ready
/// envelopes first and the claim loop can stop scanning as soon as it sees a
/// prefix in the future.
///
/// Negative timestamps (pre-1970) clamp to 0; the queue is not meaningful
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

/// Hex unix-millis prefix (same format as [`make_storage_key`]) marking the
/// upper bound of the ready window for the autoscaler gauge. Storage keys
/// whose 16-hex prefix compares lexicographically greater are scheduled
/// past the horizon and excluded from the count.
///
/// `horizon_secs` is sourced from
/// [`JobQueueConfig::pending_ready_horizon_secs`].
pub fn pending_ready_cutoff_prefix(horizon_secs: u64) -> String {
    let cutoff =
        Utc::now() + ChronoDuration::seconds(i64::try_from(horizon_secs).unwrap_or(i64::MAX));
    let millis = u64::try_from(cutoff.timestamp_millis()).unwrap_or(u64::MAX);
    format!("{millis:016x}")
}

// ---------------------------------------------------------------------------
// Dedup-index helpers
// ---------------------------------------------------------------------------

/// On-disk shape of the per-`lock_key` dedup index file written alongside
/// every pending envelope. Holds the `storage_key` of the most-recent
/// pending file for this `lock_key`, which lets `find_pending_with_lock_key`
/// do an O(1) `HEAD <pending>` instead of LIST-scanning bodies.
///
/// The index is best-effort: a crash between writing the pending file and
/// writing the index leaves no index (next enqueue dedup-misses, spurious
/// duplicate eventually idempotently handled). A crash between deleting the
/// pending file and the index leaves an orphan index; the next enqueue
/// detects this via the absent pending and self-heals.
#[derive(Debug, Serialize, Deserialize)]
pub struct LockKeyIndex {
    pub storage_key: String,
}

pub fn serialize_lock_key_index(storage_key: &str) -> Result<Vec<u8>, Error> {
    serde_json::to_vec(&LockKeyIndex {
        storage_key: storage_key.to_string(),
    })
    .map_err(|e| Error::Storage(format!("failed to serialize lock-key index: {e}")))
}

pub fn parse_lock_key_index(bytes: &[u8]) -> Result<LockKeyIndex, Error> {
    serde_json::from_slice(bytes)
        .map_err(|e| Error::Storage(format!("failed to parse lock-key index: {e}")))
}

// ---------------------------------------------------------------------------
// Dead-letter serializer
// ---------------------------------------------------------------------------

/// On-disk shape of a dead-letter record.
#[derive(Debug, Serialize)]
struct DeadLetterRecord<'a> {
    #[serde(flatten)]
    envelope: &'a JobEnvelope,
    last_error: &'a str,
    failed_at: DateTime<Utc>,
}

/// Owned, deserializable view of a dead-letter record. The borrowing
/// [`DeadLetterRecord`] is write-only; this is its read counterpart. The
/// flattened envelope plus `last_error`/`failed_at` round-trip the JSON
/// written by [`serialize_dead_letter`].
#[derive(Debug, Clone, Deserialize)]
pub struct DeadLetterRead {
    #[serde(flatten)]
    pub envelope: JobEnvelope,
    pub last_error: String,
    pub failed_at: DateTime<Utc>,
}

pub fn serialize_dead_letter(envelope: &JobEnvelope, last_error: &str) -> Result<Vec<u8>, Error> {
    serde_json::to_vec(&DeadLetterRecord {
        envelope,
        last_error,
        failed_at: Utc::now(),
    })
    .map_err(|e| Error::Storage(format!("failed to serialize dead-letter: {e}")))
}

// ---------------------------------------------------------------------------
// JobHandler trait
// ---------------------------------------------------------------------------

/// Executor for a single job kind.
///
/// Returns the work-product [`Transaction`] on success; the queue runtime
/// merges it with the pending/index cleanup mutations and submits the whole
/// thing as one engine transaction so the handler's effect and the queue
/// bookkeeping commit atomically together. Return an empty [`Transaction`]
/// when the handler has no storage-side effect to commit (the cleanup
/// mutations still land atomically).
///
/// # Idempotency
///
/// `JobStore::complete` commits the work-product mutations and the
/// pending/index deletes in a single engine transaction, so handlers whose
/// effect is expressible as engine mutations need not be idempotent: each
/// attempt either commits atomically or aborts cleanly.
///
/// Handlers with *external*, non-transactional side-effects (e.g. calls to
/// an outside service) must still be idempotent: the engine cannot roll
/// those back. The only re-execution path is a worker dying between claim
/// and commit, bounded by the execution-lock TTL.
#[async_trait]
pub trait JobHandler: Send + Sync {
    async fn execute(&self, envelope: &JobEnvelope) -> Result<Transaction, Error>;
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum number of pending envelopes inspected per scan. Deeper queues fall
/// back to lock-based serialization, which is the actual correctness primitive.
pub const MAX_SCAN: u16 = 1000;

/// Maximum value reported by [`JobStore::count_pending`]. The gauge feeds KEDA
/// autoscaling, which only needs ordinal granularity at high queue depths:
/// once the queue exceeds 10x the worker pool size you're at max scale anyway.
/// Capping here bounds S3 `LIST` cost per refresh tick to ~10 paginated calls
/// regardless of how deep the queue actually is. Operators reading the gauge
/// should treat the cap value as "at least this many".
pub const MAX_REPORTED_PENDING: u64 = 10_000;

// ---------------------------------------------------------------------------
// Consumer types
// ---------------------------------------------------------------------------

/// Lock key used to serialise per-`lock_key` job execution. Prefixed so it
/// shares a namespace with the rest of the job-store's locked operations
/// (dedup index, etc.) without colliding.
fn job_lock_key(lock_key: &str) -> String {
    format!("job:{lock_key}")
}

/// A job claimed by a worker, ready to execute. `session` holds the
/// distributed lock on the job's `lock_key`; its heartbeat keeps the lock
/// alive, and the lock is released on `complete`/`fail`. `storage_key`
/// identifies the pending file the envelope was loaded from so
/// `complete`/`fail` can delete or rewrite that file.
pub struct ClaimedJob {
    pub envelope: JobEnvelope,
    pub storage_key: String,
    pub session: LockSession,
}

pub enum FailOutcome {
    Retried { next_at: DateTime<Utc> },
    MovedToDeadLetter,
}

/// Outcome of [`JobStore::complete`]. A failure to commit the work-product +
/// cleanup transaction is **not** surfaced as an error the caller must handle
/// out-of-band: the job is failed over (retried with backoff, or dead-lettered
/// once its budget is spent) so a persistently-failing commit cannot leave the
/// pending file re-claimable in a hot loop. The variant tells the caller which
/// happened for logging.
pub enum CompleteOutcome {
    /// The work commit and queue cleanup landed; the job is done.
    Completed,
    /// The commit (or the dedup-index read that precedes it) failed; the job
    /// was failed over via [`JobStore::fail`] instead.
    FailedOver(FailOutcome),
}

/// Outcome of a single `claim_one` attempt. `claimed` is `Some` when a job was
/// claimed; otherwise `next_ready` carries the soonest `not_before` observed
/// across the scan so the caller can sleep until then rather than polling at
/// full cadence through unchanged backed-off envelopes.
pub struct ClaimOutcome {
    pub claimed: Option<ClaimedJob>,
    pub next_ready: Option<DateTime<Utc>>,
}

impl ClaimOutcome {
    /// How long the caller should idle before the next `claim_one` attempt.
    /// When only backed-off envelopes were seen, the sleep extends to the
    /// soonest `not_before`, clamped to `[poll_interval, max(poll_interval, 1 min)]`
    /// so the worker stops re-reading unchanged envelopes every tick while
    /// still picking up newly-enqueued ready jobs promptly.
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

// ---------------------------------------------------------------------------
// Error conversion helper
// ---------------------------------------------------------------------------

fn tx_error_to_job(err: TxError) -> Error {
    match err {
        TxError::Storage(e) => Error::from(e),
        TxError::Lock(e) => Error::from(e),
        TxError::Serde(e) => Error::Storage(format!("serialisation error: {e}")),
        TxError::Conflict | TxError::Precondition | TxError::PartialCommit => {
            Error::Storage("transaction conflict: retry budget exhausted".to_string())
        }
        TxError::Build(msg) => Error::Storage(format!("engine build error: {msg}")),
    }
}

// ---------------------------------------------------------------------------
// JobStore: unified producer + consumer + storage
// ---------------------------------------------------------------------------

/// Unified job-queue store: producer (`enqueue`), consumer
/// (`claim_one` / `complete` / `fail`), and raw storage primitives
/// (`list_pending`, `count_pending`, `find_pending_with_lock_key`).
///
/// All write operations that require atomicity go through the
/// [`TransactionExecutor`]; the raw `ObjectStore` is used for reads and
/// low-level access patterns that do not need CAS.
///
/// `worker_id` is an optional per-process identifier attached to structured
/// log entries from `claim_one`; supply an empty string or any identifier when
/// constructing for producer-only use.
pub struct JobStore {
    store: Arc<Store>,
    worker_id: String,
    retry_backoff: Backoff,
    claim_error_backoff: Backoff,
    consecutive_claim_errors: AtomicU32,
    max_attempts: u32,
}

impl JobStore {
    /// Construct a new `JobStore`.
    ///
    /// `worker_id` is a structured-log tag that makes concurrent workers'
    /// `claim_one` actions distinguishable in aggregated logs. Pass an empty
    /// string for producer-only instances. The retry policy defaults to
    /// [`JobRetryPolicy::default`]; the durable queue overrides it with
    /// [`Self::with_retry_policy`].
    pub fn new(store: Arc<Store>, worker_id: impl Into<String>) -> Self {
        Self::with_retry_policy(store, worker_id, JobRetryPolicy::default())
    }

    /// [`Self::new`] with an operator-configured retry policy (see
    /// [`JobQueueConfig::retry_policy`]).
    pub fn with_retry_policy(
        store: Arc<Store>,
        worker_id: impl Into<String>,
        retry: JobRetryPolicy,
    ) -> Self {
        let backoff = || {
            Backoff::exponential(
                Duration::from_millis(retry.backoff_min_ms),
                Duration::from_millis(retry.backoff_max_ms),
            )
        };
        Self {
            store,
            worker_id: worker_id.into(),
            retry_backoff: backoff(),
            claim_error_backoff: backoff(),
            consecutive_claim_errors: AtomicU32::new(0),
            max_attempts: retry.max_attempts,
        }
    }

    // -----------------------------------------------------------------------
    // Storage primitives
    // -----------------------------------------------------------------------

    /// List up to `n` pending storage keys in ascending order. Storage keys
    /// start with a sortable hex-millis prefix, so callers can stop scanning
    /// at the first key whose prefix is in the future.
    pub async fn list_pending(&self, queue: Queue, n: u16) -> Result<Vec<String>, Error> {
        let prefix = job_pending_dir(queue.as_str());
        let page = self.store.object_store().list(&prefix, n, None).await?;

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
        let data = self.store.object_store().get(&key).await?;
        serde_json::from_slice(&data)
            .map_err(|e| Error::Storage(format!("failed to parse envelope: {e}")))
    }

    /// Read a dead-letter record by `storage_key`. Returns [`Error::NotFound`]
    /// when the record is absent (e.g. a stale key the UI is still showing).
    pub async fn read_failed(
        &self,
        queue: Queue,
        storage_key: &str,
    ) -> Result<DeadLetterRead, Error> {
        let key = job_failed_path(queue.as_str(), storage_key);
        let data = self.store.object_store().get(&key).await?;
        serde_json::from_slice(&data)
            .map_err(|e| Error::Storage(format!("failed to parse dead-letter: {e}")))
    }

    /// One keyset page of pending storage keys in ascending (time) order. Pass
    /// `after = Some(last_key)` from a previous page to resume; the cursor is
    /// the plain storage key (non-opaque). The returned `next` is `Some(key)`
    /// when the backend reports more entries beyond this page.
    pub async fn list_pending_page(
        &self,
        queue: Queue,
        n: u16,
        after: Option<&str>,
    ) -> Result<(Vec<String>, Option<String>), Error> {
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
    ) -> Result<(Vec<String>, Option<String>), Error> {
        self.list_page(&job_failed_dir(queue.as_str()), n, after)
            .await
    }

    /// Shared keyset pager over a job directory. Children are flat `<key>.json`
    /// files, so `list_children` returns them as bare `objects` in lexicographic
    /// (== time) order; `start_after` skips up to and including its argument, so
    /// the cursor is suffixed with `.json` to match the stored child name.
    async fn list_page(
        &self,
        dir: &str,
        n: u16,
        after: Option<&str>,
    ) -> Result<(Vec<String>, Option<String>), Error> {
        let start_after = after.map(|k| format!("{k}.json"));
        let page = self
            .store
            .object_store()
            .list_children(dir, n, None, start_after)
            .await?;
        let keys: Vec<String> = page
            .objects
            .into_iter()
            .filter_map(|name| name.strip_suffix(".json").map(str::to_string))
            .collect();
        // The backend's `next_token` is the accurate "more entries exist"
        // signal; surface our own last storage key as the (non-opaque) cursor.
        let next = page
            .next_token
            .is_some()
            .then(|| keys.last().cloned())
            .flatten();
        Ok((keys, next))
    }

    /// Count pending envelopes ready for handling within
    /// `[..., now + ready_horizon_secs]`. Capped at [`MAX_REPORTED_PENDING`].
    pub async fn count_pending(&self, queue: Queue, ready_horizon_secs: u64) -> Result<u64, Error> {
        let prefix = job_pending_dir(queue.as_str());
        let cutoff_prefix = pending_ready_cutoff_prefix(ready_horizon_secs);
        let mut count: u64 = 0;
        let mut token: Option<String> = None;
        loop {
            let page = self.store.object_store().list(&prefix, 1000, token).await?;
            for name in &page.items {
                let Some(stem) = name.strip_suffix(".json") else {
                    continue;
                };
                // Storage list returns lex-sorted keys, which equals `not_before`
                // order due to the fixed-width hex unix-millis prefix. Stop
                // counting at the first key past the readiness cutoff.
                if let Some(p) = stem.get(..STORAGE_KEY_PREFIX_LEN)
                    && p > cutoff_prefix.as_str()
                {
                    return Ok(count.min(MAX_REPORTED_PENDING));
                }
                count += 1;
                if count >= MAX_REPORTED_PENDING {
                    return Ok(MAX_REPORTED_PENDING);
                }
            }
            match page.next_token {
                Some(t) => token = Some(t),
                None => return Ok(count),
            }
        }
    }

    /// Count dead-lettered envelopes in `queue`, capped at
    /// [`MAX_REPORTED_PENDING`]. Feeds the server-published
    /// `angos_job_queue_failed` gauge so dead-letters stay observable even when
    /// `angos worker` (which has no metrics endpoint) drains the queue.
    pub async fn count_failed(&self, queue: Queue) -> Result<u64, Error> {
        let prefix = job_failed_dir(queue.as_str());
        let mut count: u64 = 0;
        let mut token: Option<String> = None;
        loop {
            let page = self.store.object_store().list(&prefix, 1000, token).await?;
            for name in &page.items {
                if name.strip_suffix(".json").is_none() {
                    continue;
                }
                count += 1;
                if count >= MAX_REPORTED_PENDING {
                    return Ok(MAX_REPORTED_PENDING);
                }
            }
            match page.next_token {
                Some(t) => token = Some(t),
                None => return Ok(count),
            }
        }
    }

    /// `true` when any pending job in `queue` carries `lock_key`. Best-effort
    /// dedup backed by an O(1) index file written alongside each pending
    /// envelope (see [`LockKeyIndex`]).
    ///
    /// When the index references a pending file that has vanished (orphan),
    /// submits a one-mutation engine transaction to delete the stale index,
    /// guarded by a read fingerprint so a concurrent enqueue that refreshes
    /// the index between our GET and the apply is never accidentally deleted.
    /// A transient failure to delete the orphan is returned as an error rather
    /// than `false`, so the caller never proceeds to an enqueue the lingering
    /// index would silently coalesce away.
    pub async fn find_pending_with_lock_key(
        &self,
        queue: Queue,
        lock_key: &str,
    ) -> Result<bool, Error> {
        let index_path = job_lock_key_index_path(queue.as_str(), lock_key);
        let data = match self.store.object_store().get(&index_path).await {
            Ok(d) => d,
            Err(StorageError::NotFound) => return Ok(false),
            Err(e) => return Err(Error::from(e)),
        };
        let index = parse_lock_key_index(&data)?;

        let pending_key = job_pending_path(queue.as_str(), &index.storage_key);
        match self.store.object_store().head(&pending_key).await {
            Ok(_) => Ok(true),
            Err(StorageError::NotFound) => {
                // Orphan: pending file vanished but the index lingers. Submit a
                // one-mutation engine transaction whose Read fingerprint validates
                // the index hasn't been refreshed by a concurrent enqueue between
                // our GET and the apply. If it has, the engine returns Conflict
                // and we don't delete the fresh index. Passing the index's own
                // `storage_key` as the target makes the conditional delete fire
                // for this orphan while reusing the shared fingerprint guard.
                let mut tx = Transaction::builder().build();
                if let Some((read, delete)) = Self::conditional_index_delete(
                    index_path,
                    &data,
                    &index.storage_key,
                    &index.storage_key,
                ) {
                    tx.reads.push(read);
                    tx.mutations.push(delete);
                }
                match self.store.execute(tx).await {
                    Ok(_) | Err(TxError::Conflict | TxError::Precondition) => Ok(false),
                    Err(e) => {
                        // Surface the transient failure instead of swallowing it
                        // as `false`: the un-retired index would otherwise make
                        // the caller's enqueue collide on `PutIfAbsent` and drop
                        // a distinct job as a false dedup hit.
                        warn!(
                            lock_key,
                            error = %e,
                            "Failed to remove orphan lock-key index via engine",
                        );
                        Err(tx_error_to_job(e))
                    }
                }
            }
            Err(e) => Err(Error::from(e)),
        }
    }

    /// Return the raw bytes of the per-`lock_key` dedup index alongside the
    /// parsed `storage_key` it contains, or `None` when the index is absent.
    pub async fn get_lock_key_index_raw(
        &self,
        queue: Queue,
        lock_key: &str,
    ) -> Result<Option<(String, Vec<u8>)>, Error> {
        let index_path = job_lock_key_index_path(queue.as_str(), lock_key);
        match self.get_raw(&index_path).await {
            Ok(data) => {
                let index = parse_lock_key_index(&data)?;
                Ok(Some((index.storage_key, data)))
            }
            Err(Error::NotFound) => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub async fn get_raw(&self, key: &str) -> Result<Vec<u8>, Error> {
        self.store
            .object_store()
            .get(key)
            .await
            .map_err(Error::from)
    }

    /// Build the read dependency and conditional `Delete` that retire a
    /// per-`lock_key` dedup index, but only when the index still points at
    /// `target_storage_key`.
    ///
    /// `index_storage_key` / `index_body` are the parsed `storage_key` and the
    /// raw bytes of the current index (as returned by
    /// [`Self::get_lock_key_index_raw`]). When the index points elsewhere the
    /// returned pair is empty and the caller leaves the index untouched.
    ///
    /// The `Read` carries a fingerprint of `index_body`, so an index refreshed
    /// by a concurrent enqueue between the read and the apply turns the delete
    /// into a no-op conflict rather than clobbering the fresh index. Four
    /// call sites fold this into their own transactions: the orphan self-heal
    /// in `find_pending_with_lock_key`, `retire_claimed_index`, `complete`, and
    /// `fail_dead_letter`.
    fn conditional_index_delete(
        index_path: String,
        index_body: &[u8],
        index_storage_key: &str,
        target_storage_key: &str,
    ) -> Option<(Read, Mutation)> {
        if index_storage_key != target_storage_key {
            return None;
        }
        let read = Read {
            key: index_path.clone(),
            fingerprint: Sha256::digest(index_body).into(),
        };
        let delete = Mutation::Delete {
            key: index_path,
            expected: None,
        };
        Some((read, delete))
    }

    /// Fold a conditional dedup-index delete into `tx`: read the current index
    /// for `lock_key`, and when it still points at `target_storage_key`, add its
    /// fingerprint-guarded delete so a concurrent enqueue that refreshed the
    /// index becomes a no-op conflict instead of a clobber. A missing index or a
    /// mismatched target leaves `tx` untouched.
    async fn fold_index_cleanup(
        &self,
        tx: &mut Transaction,
        queue: Queue,
        lock_key: &str,
        target_storage_key: &str,
    ) -> Result<(), Error> {
        let Some((index_storage_key, body)) = self.get_lock_key_index_raw(queue, lock_key).await?
        else {
            return Ok(());
        };
        let index_path = job_lock_key_index_path(queue.as_str(), lock_key);
        if let Some((read, delete)) = Self::conditional_index_delete(
            index_path,
            &body,
            &index_storage_key,
            target_storage_key,
        ) {
            tx.reads.push(read);
            tx.mutations.push(delete);
        }
        Ok(())
    }

    /// Retire the dedup index of a just-claimed job so a same-`lock_key`
    /// enqueue arriving mid-execution starts a fresh pending file rather than
    /// coalescing into the already-resolved job and silently dropping its write.
    /// The pending file stays as the crash-recovery record, and the delete is
    /// conditional plus fingerprint-guarded so a producer's newer index is
    /// never clobbered.
    ///
    /// Returns whether the index is known not to point at `storage_key` any
    /// more: retired here, already absent, pointing elsewhere, or swung by a
    /// producer between the read and the apply (the guard's `Conflict` /
    /// `Precondition`). `false` means the claim must not proceed, since a stale
    /// index left in place coalesces every same-`lock_key` enqueue into a job
    /// that is already running.
    async fn retire_claimed_index(&self, queue: Queue, lock_key: &str, storage_key: &str) -> bool {
        let mut tx = Transaction::builder().build();
        if let Err(e) = self
            .fold_index_cleanup(&mut tx, queue, lock_key, storage_key)
            .await
        {
            warn!(lock_key, error = %e, "Failed to read dedup index at claim");
            return false;
        }
        if tx.mutations.is_empty() {
            return true;
        }
        match self.store.execute(tx).await {
            Ok(_) | Err(TxError::Conflict | TxError::Precondition) => true,
            Err(e) => {
                warn!(lock_key, error = %tx_error_to_job(e), "Failed to retire dedup index at claim");
                false
            }
        }
    }

    // -----------------------------------------------------------------------
    // Producer: enqueue
    // -----------------------------------------------------------------------

    /// Enqueue a job.
    ///
    /// Fast-path: `find_pending_with_lock_key` checks the dedup index and
    /// self-heals orphans (same semantics as a HEAD index + HEAD pending).
    ///
    /// Slow-path: submit a transaction with `PutIfAbsent` on both the index
    /// and the pending file. If two replicas race and both observe absence,
    /// only one wins at the engine's Prepare/Apply stage; the loser receives
    /// `Conflict` or `Precondition` and we treat that as a dedup hit.
    pub async fn enqueue(&self, mut envelope: JobEnvelope) -> Result<(), Error> {
        // Apply the queue's configured retry budget unless a caller pinned an
        // explicit per-job value (a non-zero `max_attempts`).
        if envelope.max_attempts == 0 {
            envelope.max_attempts = self.max_attempts;
        }
        // Fast path: index present and pending exists, a hit, no writes needed.
        // A lookup error (including a failed orphan self-heal) is propagated
        // rather than swallowed as a miss, so a lingering orphan index cannot
        // make the slow-path `PutIfAbsent` coalesce this distinct job away.
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

        // Slow path: build the transaction.
        let storage_key = make_storage_key(Utc::now(), &envelope.id);
        let pending_path = job_pending_path(envelope.queue.as_str(), &storage_key);
        let index_path = job_lock_key_index_path(envelope.queue.as_str(), &envelope.lock_key);

        let pending_body = Bytes::from(
            serde_json::to_vec(&envelope)
                .map_err(|e| Error::Storage(format!("envelope serialization failed: {e}")))?,
        );
        let index_body = Bytes::from(serialize_lock_key_index(&storage_key)?);

        // Mutation order matters: index first, then pending. The CAS executor
        // aborts cleanly if the first mutation's PutIfAbsent fails, meaning
        // another replica already claimed this lock_key.
        let tx = Transaction::builder()
            .mutation(Mutation::PutIfAbsent {
                key: index_path,
                body: index_body,
            })
            .mutation(Mutation::PutIfAbsent {
                key: pending_path,
                body: pending_body,
            })
            .build();

        // A `Conflict` or `Precondition` here means another replica won the
        // race: treat as a dedup hit, not an error.
        match self.store.execute(tx).await {
            Ok(_) => {
                metrics_provider()
                    .job_queue_enqueued_total
                    .with_label_values(&[envelope.queue.as_str(), "miss"])
                    .inc();
                Ok(())
            }
            Err(TxError::Conflict | TxError::Precondition) => {
                metrics_provider()
                    .job_queue_enqueued_total
                    .with_label_values(&[envelope.queue.as_str(), "hit"])
                    .inc();
                Ok(())
            }
            Err(e) => Err(tx_error_to_job(e)),
        }
    }

    // -----------------------------------------------------------------------
    // Consumer: claim / complete / fail
    // -----------------------------------------------------------------------

    /// Claim the next available job from `queue`, self-throttling on backend
    /// failure: a storage error sleeps an exponential, capped backoff before
    /// returning so the caller can loop without managing retry timing itself.
    /// The backoff resets on the next successful claim.
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

    /// Walk pending storage keys in ascending order (`list_pending` returns
    /// them sorted by the hex unix-millis prefix, i.e. by `not_before`). Stops
    /// at the first key whose prefix is in the future without reading its body:
    /// the prefix is the authoritative readiness signal. When no claim is made,
    /// `next_ready` carries that first future instant so the caller can sleep
    /// until then.
    ///
    /// On a successful claim the job's dedup index is retired (see
    /// [`Self::retire_claimed_index`]) so a same-`lock_key` enqueue arriving
    /// while the job runs is not coalesced into the already-resolved job.
    async fn try_claim_one(&self, queue: Queue) -> Result<ClaimOutcome, Error> {
        let now = Utc::now();
        let mut next_ready: Option<DateTime<Utc>> = None;
        for storage_key in self.list_pending(queue, MAX_SCAN).await? {
            // Read the readiness time off the filename; never GET the body for
            // a backed-off entry. A missing/malformed prefix falls through to
            // the body read so legacy keys (if any) still work.
            if let Some(not_before) = parse_not_before(&storage_key)
                && not_before > now
            {
                next_ready = Some(not_before);
                break;
            }
            let envelope = match self.read_pending(queue, &storage_key).await {
                Ok(e) => e,
                Err(Error::NotFound) => continue,
                Err(e) => return Err(e),
            };
            if let Some(session) = self
                .store
                .try_acquire(&[job_lock_key(&envelope.lock_key)])
                .await
                .map_err(tx_error_to_job)?
            {
                // Re-read under the execution lock: another worker may have
                // completed this job (deleting the pending file) between our
                // first read and the lock acquisition. Without this the stale
                // envelope would be re-run and could dead-letter a finished job.
                let envelope = match self.read_pending(queue, &storage_key).await {
                    Ok(envelope) => envelope,
                    Err(Error::NotFound) => {
                        session.release().await;
                        continue;
                    }
                    Err(e) => {
                        session.release().await;
                        return Err(e);
                    }
                };
                // Claiming while the index still points at this job would let
                // every same-`lock_key` enqueue coalesce into it for the whole
                // execution and drop its write, so leave the job for the next
                // scan instead.
                if !self
                    .retire_claimed_index(queue, &envelope.lock_key, &storage_key)
                    .await
                {
                    session.release().await;
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
                        session,
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

    /// Mark a claimed job as complete and release its execution lock.
    ///
    /// `handler_tx` carries the handler's work-product mutations (empty for
    /// no-op handlers). This method appends the pending-file delete and a
    /// conditional dedup-index delete, submits the merged transaction, and
    /// releases the execution lock once the commit settles. The work commit
    /// and the queue cleanup land atomically; the lock release follows
    /// immediately after Reap so the next worker can claim this `lock_key`
    /// without waiting on TTL.
    pub async fn complete(
        &self,
        claimed: ClaimedJob,
        handler_tx: Transaction,
    ) -> Result<CompleteOutcome, Error> {
        let ClaimedJob {
            envelope,
            storage_key,
            session,
        } = claimed;
        let pending_path = job_pending_path(envelope.queue.as_str(), &storage_key);
        let index_path = job_lock_key_index_path(envelope.queue.as_str(), &envelope.lock_key);

        let mut tx = handler_tx;
        tx.mutations.push(Mutation::Delete {
            key: pending_path,
            expected: None,
        });

        // The execution lock keeps other workers off this `lock_key`, but not
        // producers: `enqueue` writes the index without it. The read
        // fingerprint is what makes the cleanup safe, not decoration, and an
        // index pointing elsewhere is left alone. A corrupt index has no
        // storage key to match, so it is deleted on the strength of its own
        // bytes: a producer that replaces it in the window conflicts instead of
        // losing its entry.
        match self.get_raw(&index_path).await {
            Ok(body) => {
                let cleanup = match parse_lock_key_index(&body) {
                    Ok(index) => Self::conditional_index_delete(
                        index_path,
                        &body,
                        &index.storage_key,
                        &storage_key,
                    ),
                    Err(_) => Some((
                        Read {
                            key: index_path.clone(),
                            fingerprint: Sha256::digest(&body).into(),
                        },
                        Mutation::Delete {
                            key: index_path,
                            expected: None,
                        },
                    )),
                };
                if let Some((read, delete)) = cleanup {
                    tx.reads.push(read);
                    tx.mutations.push(delete);
                }
            }
            Err(Error::NotFound) => {}
            Err(e) => {
                // We could not read the dedup index to build the cleanup. Fail
                // the job over rather than returning an error that would leave
                // the pending file re-claimable in a hot loop; we still hold the
                // execution lock, so no other worker can claim this `lock_key`
                // while `fail` runs.
                let claimed = ClaimedJob {
                    envelope,
                    storage_key,
                    session,
                };
                return self
                    .fail(claimed, &format!("complete: index read failed: {e}"))
                    .await
                    .map(CompleteOutcome::FailedOver);
            }
        }

        match self.store.execute(tx).await {
            Ok(_) => {
                session.release().await;
                Ok(CompleteOutcome::Completed)
            }
            Err(e) => {
                // The work commit + queue cleanup did not land (e.g. a transient
                // backend error, or a mutation referencing storage the executor
                // cannot resolve). Treat it as a failed attempt and fail the job
                // over (backoff then dead-letter) instead of leaving it to be
                // re-claimed immediately, forever. We still hold the lock.
                let err = tx_error_to_job(e);
                let claimed = ClaimedJob {
                    envelope,
                    storage_key,
                    session,
                };
                self.fail(claimed, &format!("complete: commit failed: {err}"))
                    .await
                    .map(CompleteOutcome::FailedOver)
            }
        }
    }

    /// Record a failure. The job is either re-queued with backoff or moved to
    /// the dead-letter store when its retry budget is exhausted. On retry the
    /// envelope is rewritten to a *new* storage key encoding the bumped
    /// `not_before`; the previous key is deleted afterwards. A crash between
    /// the two writes re-runs the previous envelope at its old (already
    /// elapsed) `not_before`, which the handler-side idempotency contract
    /// already covers.
    pub async fn fail(&self, claimed: ClaimedJob, err: &str) -> Result<FailOutcome, Error> {
        let ClaimedJob {
            envelope,
            storage_key,
            session,
        } = claimed;
        let new_attempts = envelope.attempts.saturating_add(1);

        if new_attempts >= envelope.max_attempts {
            return self
                .fail_dead_letter(session, envelope, storage_key, err)
                .await;
        }

        let delay = self.retry_backoff.delay(new_attempts);
        let next_at = Utc::now() + ChronoDuration::from_std(delay).unwrap_or_default();
        let updated = JobEnvelope {
            attempts: new_attempts,
            ..envelope
        };

        self.fail_retry(session, updated, storage_key, next_at)
            .await
    }

    /// Record a non-retryable failure: the job is dead-lettered immediately,
    /// bypassing the retry budget. Used when retrying cannot succeed (an
    /// authorization denial).
    pub async fn fail_terminal(
        &self,
        claimed: ClaimedJob,
        err: &str,
    ) -> Result<FailOutcome, Error> {
        let ClaimedJob {
            envelope,
            storage_key,
            session,
        } = claimed;
        self.fail_dead_letter(session, envelope, storage_key, err)
            .await
    }

    /// Single transaction replaces the pending file and updates the index, then
    /// deletes the old pending. The execution lock is held across the call and
    /// released explicitly afterwards.
    async fn fail_retry(
        &self,
        session: LockSession,
        updated: JobEnvelope,
        old_storage_key: String,
        next_at: DateTime<Utc>,
    ) -> Result<FailOutcome, Error> {
        let new_storage_key = make_storage_key(next_at, &updated.id);
        let new_pending_path = job_pending_path(updated.queue.as_str(), &new_storage_key);
        let old_pending_path = job_pending_path(updated.queue.as_str(), &old_storage_key);
        let index_path = job_lock_key_index_path(updated.queue.as_str(), &updated.lock_key);

        let pending_body = Bytes::from(
            serde_json::to_vec(&updated)
                .map_err(|e| Error::Storage(format!("envelope serialization failed: {e}")))?,
        );
        let index_body = Bytes::from(serialize_lock_key_index(&new_storage_key)?);

        let mut tx = Transaction::builder()
            .mutation(Mutation::Put {
                key: new_pending_path,
                body: pending_body,
                expected: None,
            })
            .mutation(Mutation::Delete {
                key: old_pending_path,
                expected: None,
            })
            .build();

        // The execution lock does not exclude producers: `enqueue` writes the
        // index without ever taking it. Point the index at the rescheduled job
        // only while the claim's retire has left it absent, under a read that
        // fingerprints that absence. A producer that indexed a fresh job in the
        // meantime keeps its entry, and this retry stays unindexed until an
        // enqueue self-heals it: a missed dedup hit costs a duplicate job,
        // clobbering the entry would strand that producer's pending file.
        match self
            .get_lock_key_index_raw(updated.queue, &updated.lock_key)
            .await
        {
            Ok(None) => {
                tx.reads.push(Read {
                    key: index_path.clone(),
                    fingerprint: Sha256::digest([]).into(),
                });
                tx.mutations.push(Mutation::Put {
                    key: index_path,
                    body: index_body,
                    expected: None,
                });
            }
            Ok(Some(_)) => {}
            Err(e) => {
                warn!(
                    lock_key = updated.lock_key.as_str(),
                    error = %e,
                    "Failed to read dedup index while rescheduling; leaving it untouched"
                );
            }
        }

        let result = self.store.execute(tx).await;
        session.release().await;
        result.map_err(tx_error_to_job)?;

        Ok(FailOutcome::Retried { next_at })
    }

    /// Single transaction writes the failed record and removes the pending and
    /// (conditionally) the index. The execution lock is held across the call
    /// and released explicitly afterwards.
    async fn fail_dead_letter(
        &self,
        session: LockSession,
        envelope: JobEnvelope,
        storage_key: String,
        err: &str,
    ) -> Result<FailOutcome, Error> {
        let failed_path = job_failed_path(envelope.queue.as_str(), &storage_key);
        let pending_path = job_pending_path(envelope.queue.as_str(), &storage_key);

        let failed_body = Bytes::from(serialize_dead_letter(&envelope, err)?);

        let mut tx = Transaction::builder()
            .mutation(Mutation::Put {
                key: failed_path,
                body: failed_body,
                expected: None,
            })
            .mutation(Mutation::Delete {
                key: pending_path,
                expected: None,
            })
            .build();

        // Conditionally include the index delete: only when the index still
        // points at our storage_key, guarded by its read fingerprint.
        if let Err(e) = self
            .fold_index_cleanup(&mut tx, envelope.queue, &envelope.lock_key, &storage_key)
            .await
        {
            session.release().await;
            return Err(e);
        }

        let result = self.store.execute(tx).await;
        session.release().await;
        result.map_err(tx_error_to_job)?;

        Ok(FailOutcome::MovedToDeadLetter)
    }

    // -----------------------------------------------------------------------
    // Administrative mutations (operator-driven retry / delete)
    // -----------------------------------------------------------------------

    /// Move a dead-letter record back to pending with its retry budget reset to
    /// zero. Atomic: a single transaction `PutIfAbsent`s a fresh pending file
    /// (new `not_before` = now) and deletes the failed record under an `ETag`
    /// fence. Returns [`Error::NotFound`] when the failed record is already
    /// gone (a stale key).
    ///
    /// The dedup index is deliberately **not** re-established: a retried job has
    /// no `lock_key` index entry, so a concurrent producer enqueuing the same
    /// `lock_key` may create a second pending file. That is safe: the
    /// per-`lock_key` execution lock still serialises the two, and the handler
    /// contract makes a redundant run idempotent.
    pub async fn retry_failed(&self, queue: Queue, storage_key: &str) -> Result<(), Error> {
        let failed_path = job_failed_path(queue.as_str(), storage_key);
        // HEAD first for the fencing ETag (`None` when the backend does not
        // surface ETags, giving an unconditional delete, still safe: the new pending
        // key is fresh so a double-retry collides at `PutIfAbsent`).
        let expected = self.store.object_store().head(&failed_path).await?.etag;

        let mut envelope = self.read_failed(queue, storage_key).await?.envelope;
        envelope.attempts = 0;

        let new_storage_key = make_storage_key(Utc::now(), &envelope.id);
        let pending_path = job_pending_path(queue.as_str(), &new_storage_key);
        let pending_body = Bytes::from(
            serde_json::to_vec(&envelope)
                .map_err(|e| Error::Storage(format!("envelope serialization failed: {e}")))?,
        );

        let tx = Transaction::builder()
            .mutation(Mutation::PutIfAbsent {
                key: pending_path,
                body: pending_body,
            })
            .mutation(Mutation::Delete {
                key: failed_path,
                expected,
            })
            .build();

        // A concurrent retry that won the race surfaces as `Conflict` (its
        // pending key collided) or `Precondition` (the failed record it deleted
        // is gone); either way the job is back in flight, so both join the
        // success arm.
        match self.store.execute(tx).await {
            Ok(_) | Err(TxError::Conflict | TxError::Precondition) => Ok(()),
            Err(e) => Err(tx_error_to_job(e)),
        }
    }

    /// Delete a job by `state`/`storage_key`. Failed records are removed with a
    /// single ETag-fenced delete. Pending deletes additionally fold in the
    /// conditional dedup-index delete (only when the index still points at this
    /// key), mirroring [`Self::fail_dead_letter`].
    ///
    /// Deleting a *pending* job is best-effort against a worker that may be
    /// mid-execution: the operator holds no execution lock, so a claim already
    /// in flight still commits its handler effect. The `ETag` fence prevents
    /// clobbering a file the worker has since rewritten (retry) or removed.
    /// Returns [`Error::NotFound`] for a stale key.
    pub async fn delete_job(
        &self,
        queue: Queue,
        state: JobState,
        storage_key: &str,
    ) -> Result<(), Error> {
        match state {
            JobState::Failed => {
                let failed_path = job_failed_path(queue.as_str(), storage_key);
                let expected = self.store.object_store().head(&failed_path).await?.etag;
                let tx = Transaction::builder()
                    .mutation(Mutation::Delete {
                        key: failed_path,
                        expected,
                    })
                    .build();
                self.store
                    .execute(tx)
                    .await
                    .map(|_| ())
                    .map_err(tx_error_to_job)
            }
            JobState::Pending => self.delete_pending(queue, storage_key).await,
        }
    }

    async fn delete_pending(&self, queue: Queue, storage_key: &str) -> Result<(), Error> {
        let pending_path = job_pending_path(queue.as_str(), storage_key);
        // Read the envelope (for its `lock_key`) and HEAD for the fence; a
        // missing pending file surfaces as `NotFound` for a 404.
        let envelope = self.read_pending(queue, storage_key).await?;
        let expected = self.store.object_store().head(&pending_path).await?.etag;

        let mut tx = Transaction::builder()
            .mutation(Mutation::Delete {
                key: pending_path,
                expected,
            })
            .build();

        // Fold a conditional index delete: only when the index still points at
        // this storage key. The read fingerprint turns a concurrent index
        // refresh into a no-op conflict rather than clobbering a fresh index.
        self.fold_index_cleanup(&mut tx, queue, &envelope.lock_key, storage_key)
            .await?;

        self.store
            .execute(tx)
            .await
            .map(|_| ())
            .map_err(tx_error_to_job)
    }
}

// ---------------------------------------------------------------------------
// Queue-depth gauge refresh loop
// ---------------------------------------------------------------------------

/// Refresh the queue-depth gauges (`angos_job_queue_pending` and
/// `angos_job_queue_failed`) for `queue` on every `period` tick, until
/// `shutdown` is cancelled. Uses `tokio::time::interval` so the cadence stays
/// fixed across slow count calls; missed ticks are coalesced rather than
/// catching up.
///
/// `ready_horizon_secs` is forwarded to `count_pending`: only envelopes ready
/// within that window contribute to the pending gauge. The server runs this
/// loop so both gauges are scrapeable even when `angos worker` drains the queue.
pub async fn queue_depth_refresh_loop(
    store: Arc<JobStore>,
    queue: Queue,
    period: Duration,
    ready_horizon_secs: u64,
    shutdown: CancellationToken,
) {
    let mut timer = interval(period);
    timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
    // Consume the immediate first tick so the first refresh happens after `period`.
    timer.tick().await;

    loop {
        select! {
            () = shutdown.cancelled() => return,
            _ = timer.tick() => {}
        }
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
