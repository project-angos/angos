//! [`JobStore`]: unified job-queue storage, producer, and consumer over a
//! `Store` façade.
//!
//! Every atomic write (enqueue dedup, complete, fail/retry, dead-letter) goes
//! through the transaction engine rather than the object-store layer directly.

use std::{
    fmt,
    fmt::{Display, Formatter},
    str::FromStr,
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
use angos_storage::Etag;
use angos_tx_engine::{
    StorageError,
    error::Error as TxError,
    lock::{Error as LockError, LockSession},
    store::Store,
    transaction::{Mutation, Read, Transaction},
};

use crate::{metrics_provider::metrics_provider, registry::path_builder};

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("job queue initialization failed: {0}")]
    Initialization(String),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("not found")]
    NotFound,
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

// ---------------------------------------------------------------------------
// Queue
// ---------------------------------------------------------------------------

/// The durable job queues. Selects the storage prefix, the worker's `--queue`
/// filter, and the dispatch handler. Serializes as its lowercase name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Queue {
    Cache,
    Replication,
}

impl Queue {
    /// The on-disk / metric-label name (`"cache"` or `"replication"`).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Queue::Cache => "cache",
            Queue::Replication => "replication",
        }
    }
}

impl Display for Queue {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Queue {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "cache" => Ok(Queue::Cache),
            "replication" => Ok(Queue::Replication),
            other => Err(Error::Initialization(format!(
                "unknown queue '{other}'; expected 'cache' or 'replication'"
            ))),
        }
    }
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Job-queue tunables. `[global.job_queue]` switches draining to separate
/// `angos worker` processes and enables the queue-depth gauge; jobs persist
/// under the store's `_jobs/` prefix either way.
#[derive(Clone, Debug, Deserialize)]
pub struct JobQueueConfig {
    #[serde(
        default = "default_pending_refresh_interval_secs",
        deserialize_with = "deserialize_pending_refresh_interval_secs"
    )]
    pub pending_refresh_interval_secs: u64,
    #[serde(default = "default_pending_ready_horizon_secs")]
    pub pending_ready_horizon_secs: u64,
}

/// Floor on `pending_refresh_interval_secs`: sub-5s ticks induce LIST storms
/// when replicas refresh in parallel, so it is enforced at config time.
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

/// Envelope that travels through the queue. `payload` is untyped so the shape
/// stays stable across payload churn; handlers deserialize it into a concrete
/// type. `not_before` is encoded in the storage-key stem (see
/// [`make_storage_key`]), not on the envelope, so a claim loop decides readiness
/// from a LIST result alone.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobEnvelope {
    pub id: String,
    /// Logical queue; selects the storage prefix and the worker's `--queue`
    /// filter.
    pub queue: Queue,
    /// Job type identifier (e.g. `"cache.fetch_blob"`); handlers reject an
    /// unrecognized `kind`.
    pub kind: String,
    /// Per-key serialization token: at most one worker holds the execution lock
    /// per `lock_key`.
    pub lock_key: String,
    pub created_at: DateTime<Utc>,
    pub attempts: u32,
    pub max_attempts: u32,
    pub payload: serde_json::Value,
}

impl JobEnvelope {
    /// Build an envelope with a fresh UUID v4, default retry budget, and a typed
    /// payload serialized to JSON. `Err` only if the payload cannot serialize.
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
            max_attempts: 5,
            payload: serde_json::to_value(payload)?,
        })
    }
}

// ---------------------------------------------------------------------------
// Storage-key helpers
// ---------------------------------------------------------------------------

/// Width of the hex unix-millis prefix in a storage key. 16 hex chars cover
/// `u64::MAX` milliseconds, so lexicographic sort always matches time order.
pub const STORAGE_KEY_PREFIX_LEN: usize = 16;

/// Build a storage-key stem encoding `not_before` as a sortable hex unix-millis
/// prefix followed by `-<id>`, so a lex sort matches time order and the claim
/// loop stops scanning at the first future prefix. Pre-1970 clamps to 0.
pub fn make_storage_key(not_before: DateTime<Utc>, id: &str) -> String {
    let millis = u64::try_from(not_before.timestamp_millis()).unwrap_or(0);
    format!("{millis:016x}-{id}")
}

/// Parse the `not_before` instant from a storage-key prefix, or `None` for a
/// malformed key. Lets the claim loop skip backed-off envelopes without reading
/// their bodies.
pub fn parse_not_before(storage_key: &str) -> Option<DateTime<Utc>> {
    let bytes = storage_key.as_bytes();
    if bytes.len() <= STORAGE_KEY_PREFIX_LEN || bytes[STORAGE_KEY_PREFIX_LEN] != b'-' {
        return None;
    }
    let hex = &storage_key[..STORAGE_KEY_PREFIX_LEN];
    let millis = u64::from_str_radix(hex, 16).ok()?;
    DateTime::<Utc>::from_timestamp_millis(i64::try_from(millis).ok()?)
}

/// Hex unix-millis prefix (as [`make_storage_key`]) marking the ready window's
/// upper bound for the autoscaler gauge: keys whose prefix sorts greater are
/// past the horizon and excluded from the count.
pub fn pending_ready_cutoff_prefix(horizon_secs: u64) -> String {
    let cutoff =
        Utc::now() + ChronoDuration::seconds(i64::try_from(horizon_secs).unwrap_or(i64::MAX));
    let millis = u64::try_from(cutoff.timestamp_millis()).unwrap_or(u64::MAX);
    format!("{millis:016x}")
}

// ---------------------------------------------------------------------------
// Dedup-index helpers
// ---------------------------------------------------------------------------

/// On-disk shape of the per-`lock_key` dedup index. Holds the `storage_key` of
/// the most-recent pending file for this `lock_key`, so
/// `find_pending_with_lock_key` does an O(1) `HEAD` instead of a LIST scan.
///
/// Best-effort: a crash before the index write leaves none (next enqueue
/// dedup-misses); a crash after deleting the pending file leaves an orphan the
/// next enqueue self-heals.
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

/// Classified read of a per-`lock_key` dedup index file. Every reader handles
/// `Corrupt` explicitly (retire or overwrite under its fingerprint) instead of
/// propagating a parse error, so a corrupt index can never permanently
/// suppress enqueues for its `lock_key`.
enum LockKeyIndexRead {
    Absent,
    Valid { storage_key: String, bytes: Vec<u8> },
    Corrupt { bytes: Vec<u8> },
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

/// Owned read counterpart of the write-only [`DeadLetterRecord`], round-tripping
/// the JSON written by [`serialize_dead_letter`].
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
/// Returns the work-product [`Transaction`] on success (empty when there is no
/// storage-side effect); the runtime merges it with the pending/index cleanup
/// and commits both as one engine transaction.
///
/// # Idempotency
///
/// Handlers whose effect is expressible as engine mutations need not be
/// idempotent: each attempt commits atomically or aborts cleanly. Handlers with
/// external, non-transactional side-effects must still be idempotent, since a
/// worker dying between claim and commit (bounded by the execution-lock TTL)
/// re-executes them.
#[async_trait]
pub trait JobHandler: Send + Sync {
    async fn execute(&self, envelope: &JobEnvelope) -> Result<Transaction, Error>;
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum pending envelopes inspected per scan. Deeper queues fall back to
/// lock-based serialization, the actual correctness primitive.
pub const MAX_SCAN: u16 = 1000;

/// Cap on [`JobStore::count_pending`]. The gauge feeds KEDA autoscaling, which
/// only needs ordinal granularity at high depths; capping bounds `LIST` cost per
/// refresh tick. Read the cap value as "at least this many".
pub const MAX_REPORTED_PENDING: u64 = 10_000;

// ---------------------------------------------------------------------------
// Consumer types
// ---------------------------------------------------------------------------

/// Lock key serialising per-`lock_key` job execution, prefixed to avoid
/// colliding with other locked job-store operations.
fn job_lock_key(lock_key: &str) -> String {
    format!("job:{lock_key}")
}

/// A job claimed by a worker, ready to execute. `session` holds the distributed
/// lock on the `lock_key`, released on `complete`/`fail`. `storage_key` is the
/// pending file the envelope was loaded from, for `complete`/`fail` to delete or
/// rewrite.
pub struct ClaimedJob {
    pub envelope: JobEnvelope,
    pub storage_key: String,
    pub session: LockSession,
}

pub enum FailOutcome {
    Retried { next_at: DateTime<Utc> },
    MovedToDeadLetter,
}

/// Outcome of [`JobStore::complete`]. A commit failure is not surfaced as an
/// error: the job is failed over (retry or dead-letter) so a persistently
/// failing commit cannot leave the pending file re-claimable in a hot loop.
pub enum CompleteOutcome {
    /// The work commit and queue cleanup landed; the job is done.
    Completed,
    /// The commit (or the preceding dedup-index read) failed; the job was failed
    /// over via [`JobStore::fail`] instead.
    FailedOver(FailOutcome),
}

/// Which durable partition a job lives in, addressing admin mutations
/// (`retry`/`delete`) at the correct storage prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobState {
    Pending,
    Failed,
}

/// Outcome of one `claim_one` attempt. When nothing was claimed, `next_ready`
/// carries the soonest `not_before` seen so the caller sleeps until then rather
/// than polling through unchanged backed-off envelopes.
pub struct ClaimOutcome {
    pub claimed: Option<ClaimedJob>,
    pub next_ready: Option<DateTime<Utc>>,
}

impl ClaimOutcome {
    /// How long to idle before the next `claim_one`. With only backed-off
    /// envelopes seen, extends to the soonest `not_before` clamped to
    /// `[poll_interval, max(poll_interval, 1 min)]`.
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

/// Unified job-queue store: producer (`enqueue`), consumer (`claim_one` /
/// `complete` / `fail`), and raw storage primitives. Atomic writes go through
/// the transaction executor; reads use the raw object store. `worker_id` tags
/// `claim_one` log entries (empty for producer-only use).
pub struct JobStore {
    store: Arc<Store>,
    worker_id: String,
    retry_backoff: Backoff,
    claim_error_backoff: Backoff,
    consecutive_claim_errors: AtomicU32,
}

impl JobStore {
    /// Construct a `JobStore`. `worker_id` tags `claim_one` log entries; pass an
    /// empty string for producer-only instances.
    pub fn new(store: Arc<Store>, worker_id: impl Into<String>) -> Self {
        Self {
            store,
            worker_id: worker_id.into(),
            retry_backoff: Backoff::exponential(
                Duration::from_millis(100),
                Duration::from_secs(10),
            ),
            claim_error_backoff: Backoff::exponential(
                Duration::from_millis(100),
                Duration::from_secs(10),
            ),
            consecutive_claim_errors: AtomicU32::new(0),
        }
    }

    // -----------------------------------------------------------------------
    // Storage primitives
    // -----------------------------------------------------------------------

    /// List up to `n` pending storage keys in ascending order. The sortable
    /// hex-millis prefix lets callers stop at the first future key.
    pub async fn list_pending(&self, queue: Queue, n: u16) -> Result<Vec<String>, Error> {
        let prefix = path_builder::job_pending_dir(queue.as_str());
        let page = self.store.list(&prefix, 1000, None).await?;

        Ok(page
            .items
            .into_iter()
            .filter_map(|name| name.strip_suffix(".json").map(str::to_string))
            .take(n as usize)
            .collect())
    }

    pub async fn read_pending(
        &self,
        queue: Queue,
        storage_key: &str,
    ) -> Result<JobEnvelope, Error> {
        let key = path_builder::job_pending_path(queue.as_str(), storage_key);
        let data = self.store.get(&key).await?;
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
        let key = path_builder::job_failed_path(queue.as_str(), storage_key);
        let data = self.store.get(&key).await?;
        serde_json::from_slice(&data)
            .map_err(|e| Error::Storage(format!("failed to parse dead-letter: {e}")))
    }

    /// One keyset page of pending storage keys in ascending (time) order. Pass
    /// `after = Some(last_key)` to resume; the cursor is the plain storage key.
    /// `next` is `Some` when the backend reports more entries.
    pub async fn list_pending_page(
        &self,
        queue: Queue,
        n: u16,
        after: Option<&str>,
    ) -> Result<(Vec<String>, Option<String>), Error> {
        self.list_page(&path_builder::job_pending_dir(queue.as_str()), n, after)
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
        self.list_page(&path_builder::job_failed_dir(queue.as_str()), n, after)
            .await
    }

    /// List the queue sub-directory names under `_jobs/<state>/`, one per queue.
    /// Structural scrub diffs these against the known [`Queue`] set to flag an
    /// unknown queue directory without reading job bodies. Returns bare child
    /// names, looping pagination to exhaustion; an absent root yields an empty
    /// list.
    pub async fn list_queue_dirs(&self, state: JobState) -> Result<Vec<String>, Error> {
        let root = match state {
            JobState::Pending => path_builder::job_pending_root_dir(),
            JobState::Failed => path_builder::job_failed_root_dir(),
        };
        let mut names = Vec::new();
        let mut token = None;
        loop {
            let page = self.store.list_children(&root, 1000, token, None).await?;
            names.extend(page.sub_prefixes);
            match page.next_token {
                Some(next) => token = Some(next),
                None => break,
            }
        }
        Ok(names)
    }

    /// One keyset page of dedup-index stems under `_jobs/index/<queue>/`, one
    /// per `lock_key` (`.json` stripped), so the dangling-lock-key reconcile
    /// drains pages and keeps only one page resident. Keyset (not offset) paging
    /// stays correct while the reconcile deletes entries between pages. Pass
    /// `after = Some(last_stem)` to resume; `next` is `Some` when more remain.
    pub async fn list_lock_key_index_keys_page(
        &self,
        queue: Queue,
        after: Option<&str>,
    ) -> Result<(Vec<String>, Option<String>), Error> {
        self.list_page(&path_builder::job_lock_key_index_dir(queue.as_str()), 1000, after)
            .await
    }

    /// Enumerate every dedup-index stem for `queue` by draining
    /// [`Self::list_lock_key_index_keys_page`]; an absent directory yields an
    /// empty list. Test-only convenience that buffers every stem; production
    /// paths use the paginated method.
    #[cfg(test)]
    pub async fn list_lock_key_index_keys(&self, queue: Queue) -> Result<Vec<String>, Error> {
        let mut keys = Vec::new();
        let mut after: Option<String> = None;
        loop {
            let (page, next) = self
                .list_lock_key_index_keys_page(queue, after.as_deref())
                .await?;
            keys.extend(page);
            match next {
                Some(cursor) => after = Some(cursor),
                None => break,
            }
        }
        Ok(keys)
    }

    /// Shared keyset pager over a job directory. Children are flat `<key>.json`
    /// files in lexicographic (== time) order; the cursor is suffixed `.json` to
    /// match the stored child name for `start_after`.
    async fn list_page(
        &self,
        dir: &str,
        n: u16,
        after: Option<&str>,
    ) -> Result<(Vec<String>, Option<String>), Error> {
        let start_after = after.map(|k| format!("{k}.json"));
        let page = self.store.list_children(dir, n, None, start_after).await?;
        let keys: Vec<String> = page
            .objects
            .into_iter()
            .filter_map(|name| name.strip_suffix(".json").map(str::to_string))
            .collect();
        // `next_token` is the "more entries exist" signal; the returned cursor is
        // our own last storage key.
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
        let prefix = path_builder::job_pending_dir(queue.as_str());
        let cutoff_prefix = pending_ready_cutoff_prefix(ready_horizon_secs);
        let mut count: u64 = 0;
        let mut token: Option<String> = None;
        loop {
            let page = self.store.list(&prefix, 1000, token).await?;
            for name in &page.items {
                let Some(stem) = name.strip_suffix(".json") else {
                    continue;
                };
                // Lex-sorted keys equal `not_before` order, so stop at the first
                // key past the readiness cutoff.
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
    /// [`MAX_REPORTED_PENDING`]. Feeds the `angos_job_queue_failed` gauge so
    /// dead-letters stay observable even when `angos worker` drains the queue.
    pub async fn count_failed(&self, queue: Queue) -> Result<u64, Error> {
        let prefix = path_builder::job_failed_dir(queue.as_str());
        let mut count: u64 = 0;
        let mut token: Option<String> = None;
        loop {
            let page = self.store.list(&prefix, 1000, token).await?;
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
    /// dedup backed by an O(1) index file (see [`LockKeyIndex`]). An orphan
    /// index (pending file vanished) or a corrupt one is retired via a
    /// fingerprint-guarded engine delete, so neither counts as a hit.
    pub async fn find_pending_with_lock_key(
        &self,
        queue: Queue,
        lock_key: &str,
    ) -> Result<bool, Error> {
        let index_path = path_builder::job_lock_key_index_path(queue.as_str(), lock_key);
        let (storage_key, data) = match self.read_lock_key_index_at(&index_path).await? {
            LockKeyIndexRead::Absent => return Ok(false),
            LockKeyIndexRead::Corrupt { bytes } => {
                warn!(lock_key, "Corrupt job dedup index; retiring");
                self.retire_corrupt_index(&index_path, &bytes).await;
                return Ok(false);
            }
            LockKeyIndexRead::Valid { storage_key, bytes } => (storage_key, bytes),
        };

        let pending_key = path_builder::job_pending_path(queue.as_str(), &storage_key);
        match self.store.head(&pending_key).await {
            Ok(_) => Ok(true),
            Err(StorageError::NotFound) => {
                // Orphan: pending file vanished but the index lingers. The Read
                // fingerprint aborts the delete (Conflict) if a concurrent
                // enqueue refreshed the index between our GET and the apply.
                let (read, delete) =
                    Self::conditional_index_delete(index_path, &data, &storage_key, &storage_key);
                let mut tx = Transaction::builder().build();
                tx.reads.extend(read);
                tx.mutations.extend(delete);
                match self.store.execute(tx).await {
                    Ok(_) | Err(TxError::Conflict | TxError::Precondition) => Ok(false),
                    Err(e) => {
                        warn!(
                            lock_key,
                            error = %e,
                            "Failed to remove orphan lock-key index via engine",
                        );
                        Ok(false)
                    }
                }
            }
            Err(e) => Err(Error::from(e)),
        }
    }

    /// Reconcile one dedup-index file by its already-encoded stem (as
    /// [`Self::list_lock_key_index_keys`] yields), retiring it when its pending
    /// envelope has vanished or the index does not parse. Returns `true` when
    /// the index was retired, `false` when the pending file still exists or the
    /// index is already gone.
    ///
    /// Unlike [`Self::find_pending_with_lock_key`] it consumes the encoded stem
    /// directly (no double-encoding) and reads the stored `storage_key` rather
    /// than decoding it. The orphan delete is the same fingerprint-guarded engine
    /// transaction the enqueue self-heal runs.
    pub async fn reconcile_orphan_lock_key_index(
        &self,
        queue: Queue,
        encoded_stem: &str,
    ) -> Result<bool, Error> {
        let index_path = format!(
            "{}/{encoded_stem}.json",
            path_builder::job_lock_key_index_dir(queue.as_str())
        );
        let (storage_key, data) = match self.read_lock_key_index_at(&index_path).await? {
            // The index vanished since the listing: nothing to reconcile.
            LockKeyIndexRead::Absent => return Ok(false),
            LockKeyIndexRead::Corrupt { bytes } => {
                warn!(encoded_stem, "Corrupt job dedup index; retiring");
                self.retire_corrupt_index(&index_path, &bytes).await;
                return Ok(true);
            }
            LockKeyIndexRead::Valid { storage_key, bytes } => (storage_key, bytes),
        };

        let pending_key = path_builder::job_pending_path(queue.as_str(), &storage_key);
        match self.store.head(&pending_key).await {
            // Pending file still present: the index is live, leave it.
            Ok(_) => Ok(false),
            Err(StorageError::NotFound) => {
                let (read, delete) =
                    Self::conditional_index_delete(index_path, &data, &storage_key, &storage_key);
                let mut tx = Transaction::builder().build();
                tx.reads.extend(read);
                tx.mutations.extend(delete);
                match self.store.execute(tx).await {
                    Ok(_) | Err(TxError::Conflict | TxError::Precondition) => Ok(true),
                    Err(e) => {
                        warn!(
                            encoded_stem,
                            error = %e,
                            "Failed to remove orphan lock-key index via engine",
                        );
                        Ok(true)
                    }
                }
            }
            Err(e) => Err(Error::from(e)),
        }
    }

    /// Read-only orphan probe: `true` when the index at `encoded_stem` points at
    /// a pending envelope that no longer exists, or does not parse. Mirrors
    /// [`Self::reconcile_orphan_lock_key_index`] without the delete, so a dry run
    /// counts the indexes it would retire.
    pub async fn is_orphan_lock_key_index(
        &self,
        queue: Queue,
        encoded_stem: &str,
    ) -> Result<bool, Error> {
        let index_path = format!(
            "{}/{encoded_stem}.json",
            path_builder::job_lock_key_index_dir(queue.as_str())
        );
        let storage_key = match self.read_lock_key_index_at(&index_path).await? {
            // The index vanished since the listing: nothing to reconcile.
            LockKeyIndexRead::Absent => return Ok(false),
            LockKeyIndexRead::Corrupt { .. } => return Ok(true),
            LockKeyIndexRead::Valid { storage_key, .. } => storage_key,
        };
        let pending_key = path_builder::job_pending_path(queue.as_str(), &storage_key);
        match self.store.head(&pending_key).await {
            Ok(_) => Ok(false),
            Err(StorageError::NotFound) => Ok(true),
            Err(e) => Err(Error::from(e)),
        }
    }

    /// Whether a present pending body needs its dedup index (re)created: `true`
    /// when no entry exists for its `lock_key`, the entry does not parse, or it
    /// points at an absent pending file (stale), `false` when it already points
    /// at a present body.
    ///
    /// The reverse of [`Self::is_orphan_lock_key_index`]: that keys on the index,
    /// this keys on a present body. Read-only, so a dry run counts the re-indexes
    /// it would perform; unlike [`Self::reindex_orphan_pending_body`] it does not
    /// probe the execution lock (a lock probe writes), so it may count a body a
    /// real run would skip because a worker is executing it. `storage_key` is
    /// the body's stem; a vanished body yields `false`.
    pub async fn needs_reindex_pending(
        &self,
        queue: Queue,
        storage_key: &str,
    ) -> Result<bool, Error> {
        let envelope = match self.read_pending(queue, storage_key).await {
            Ok(envelope) => envelope,
            // The body vanished since the listing: nothing to re-index.
            Err(Error::NotFound) => return Ok(false),
            Err(e) => return Err(e),
        };
        let index_path = path_builder::job_lock_key_index_path(queue.as_str(), &envelope.lock_key);
        match self.read_lock_key_index_at(&index_path).await? {
            LockKeyIndexRead::Absent | LockKeyIndexRead::Corrupt { .. } => Ok(true),
            LockKeyIndexRead::Valid {
                storage_key: index_storage_key,
                ..
            } if index_storage_key == storage_key => Ok(false),
            // Points elsewhere: stale only when that target's body is absent.
            LockKeyIndexRead::Valid {
                storage_key: index_storage_key,
                ..
            } => {
                let other = path_builder::job_pending_path(queue.as_str(), &index_storage_key);
                match self.store.head(&other).await {
                    Ok(_) => Ok(false),
                    Err(StorageError::NotFound) => Ok(true),
                    Err(e) => Err(Error::from(e)),
                }
            }
        }
    }

    /// Re-index a present pending body whose dedup index is missing, corrupt,
    /// or stale, pointing the entry at this body. Returns `true` when a
    /// re-index was performed (or raced a concurrent enqueue that already
    /// refreshed it), `false` when it already pointed at a present body.
    ///
    /// A body whose `job:{lock_key}` execution lock is held is skipped: a
    /// claimed job's pending body stays on disk until `complete`, and its
    /// index was retired at claim time on purpose so a same-`lock_key` enqueue
    /// starts a fresh pending file instead of coalescing into the in-flight
    /// job. The lock is held across the re-index itself, so a claim cannot
    /// slip in between the probe and the write.
    ///
    /// The reverse direction of [`Self::reconcile_orphan_lock_key_index`]: a
    /// missing entry is created with the enqueue `PutIfAbsent`, a corrupt or
    /// stale one is corrected with a fingerprint-guarded conditional `Put`.
    /// Idempotent.
    pub async fn reindex_orphan_pending_body(
        &self,
        queue: Queue,
        storage_key: &str,
    ) -> Result<bool, Error> {
        let envelope = match self.read_pending(queue, storage_key).await {
            Ok(envelope) => envelope,
            // The body vanished since the listing: nothing to re-index.
            Err(Error::NotFound) => return Ok(false),
            Err(e) => return Err(e),
        };
        let Some(session) = self
            .store
            .executor()
            .try_acquire(&[job_lock_key(&envelope.lock_key)])
            .await
            .map_err(tx_error_to_job)?
        else {
            // A worker is executing this lock_key right now; re-indexing would
            // re-enable coalescing against a job that already read its state.
            return Ok(false);
        };
        let result = self
            .reindex_pending_body_locked(queue, storage_key, &envelope)
            .await;
        session.release().await;
        result
    }

    /// The re-index body, run while holding the `job:{lock_key}` execution lock.
    async fn reindex_pending_body_locked(
        &self,
        queue: Queue,
        storage_key: &str,
        envelope: &JobEnvelope,
    ) -> Result<bool, Error> {
        let index_path = path_builder::job_lock_key_index_path(queue.as_str(), &envelope.lock_key);
        let index_body = Bytes::from(serialize_lock_key_index(storage_key)?);

        match self.read_lock_key_index_at(&index_path).await? {
            LockKeyIndexRead::Absent => self.put_if_absent_index(index_path, index_body).await,
            LockKeyIndexRead::Corrupt { bytes } => {
                self.conditional_reindex(index_path, &bytes, index_body)
                    .await
            }
            LockKeyIndexRead::Valid {
                storage_key: index_storage_key,
                ..
            } if index_storage_key == storage_key => Ok(false),
            // Points elsewhere: correct only when that target's body is absent.
            LockKeyIndexRead::Valid {
                storage_key: index_storage_key,
                bytes,
            } => {
                let other = path_builder::job_pending_path(queue.as_str(), &index_storage_key);
                match self.store.head(&other).await {
                    Ok(_) => Ok(false),
                    Err(StorageError::NotFound) => {
                        self.conditional_reindex(index_path, &bytes, index_body)
                            .await
                    }
                    Err(e) => Err(Error::from(e)),
                }
            }
        }
    }

    /// Create a missing dedup index with a `PutIfAbsent`. A concurrent enqueue
    /// that wins surfaces as `Conflict`/`Precondition`; either way a fresh index
    /// now exists, so both join the acted-on arm.
    async fn put_if_absent_index(
        &self,
        index_path: String,
        index_body: Bytes,
    ) -> Result<bool, Error> {
        let tx = Transaction::builder()
            .mutation(Mutation::PutIfAbsent {
                key: index_path,
                body: index_body,
            })
            .build();
        match self.store.execute(tx).await {
            Ok(_) | Err(TxError::Conflict | TxError::Precondition) => Ok(true),
            Err(e) => Err(tx_error_to_job(e)),
        }
    }

    /// Correct a stale dedup index with a fingerprint-guarded conditional `Put`,
    /// so a concurrent enqueue that refreshes the entry turns the write into a
    /// no-op conflict rather than clobbering it.
    async fn conditional_reindex(
        &self,
        index_path: String,
        stale_body: &[u8],
        index_body: Bytes,
    ) -> Result<bool, Error> {
        let read = Read {
            key: index_path.clone(),
            fingerprint: Sha256::digest(stale_body).into(),
        };
        let mut tx = Transaction::builder()
            .mutation(Mutation::Put {
                key: index_path,
                body: index_body,
                expected: None,
            })
            .build();
        tx.reads.push(read);
        match self.store.execute(tx).await {
            Ok(_) | Err(TxError::Conflict | TxError::Precondition) => Ok(true),
            Err(e) => Err(tx_error_to_job(e)),
        }
    }

    /// The `_jobs/<state>/<queue>` directory of a queue name outside the
    /// recognized [`Queue`] set.
    fn unknown_queue_dir(state: JobState, queue_name: &str) -> String {
        match state {
            JobState::Pending => path_builder::job_pending_dir(queue_name),
            JobState::Failed => path_builder::job_failed_dir(queue_name),
        }
    }

    /// Newest `last_modified` across the objects of an unknown queue directory,
    /// or `None` when it holds no objects. An object reporting no
    /// `last_modified` is an error: the directory's age cannot be established,
    /// so the caller must not remove it.
    pub async fn unknown_queue_newest_mtime(
        &self,
        state: JobState,
        queue_name: &str,
    ) -> Result<Option<DateTime<Utc>>, Error> {
        let dir = Self::unknown_queue_dir(state, queue_name);
        let mut newest: Option<DateTime<Utc>> = None;
        let mut token: Option<String> = None;
        loop {
            let page = self.store.list(&dir, 1000, token).await?;
            for name in &page.items {
                let key = format!("{dir}/{name}");
                let meta = match self.store.head(&key).await {
                    Ok(m) => m,
                    // Removed since the listing: nothing to age.
                    Err(StorageError::NotFound) => continue,
                    Err(e) => return Err(Error::from(e)),
                };
                let Some(modified) = meta.last_modified else {
                    return Err(Error::Storage(format!(
                        "object '{key}' reports no last-modified time; \
                         the directory's age cannot be established"
                    )));
                };
                newest = Some(newest.map_or(modified, |t| t.max(modified)));
            }
            match page.next_token {
                Some(t) => token = Some(t),
                None => return Ok(newest),
            }
        }
    }

    /// Remove the `_jobs/<state>/<queue>` subtree for a queue directory whose
    /// name is not a recognized [`Queue`]. A newer replica may operate a queue
    /// this binary does not recognize, so the caller must first establish the
    /// directory is quiescent via [`Self::unknown_queue_newest_mtime`]. An
    /// absent prefix is a no-op.
    pub async fn delete_unknown_queue_dir(
        &self,
        state: JobState,
        queue_name: &str,
    ) -> Result<(), Error> {
        let dir = Self::unknown_queue_dir(state, queue_name);
        self.store.delete_prefix(&dir).await.map_err(Error::from)
    }

    /// Return the raw bytes of the per-`lock_key` dedup index alongside the
    /// parsed `storage_key` it contains, or `None` when the index is absent.
    pub async fn get_lock_key_index_raw(
        &self,
        queue: Queue,
        lock_key: &str,
    ) -> Result<Option<(String, Vec<u8>)>, Error> {
        let index_path = path_builder::job_lock_key_index_path(queue.as_str(), lock_key);
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
        self.store.get(key).await.map_err(Error::from)
    }

    /// Read and classify the dedup index at `index_path`. `Err` only for
    /// storage failures; an unparseable body is returned as `Corrupt` with its
    /// raw bytes so the caller can retire it under a fingerprint guard.
    async fn read_lock_key_index_at(&self, index_path: &str) -> Result<LockKeyIndexRead, Error> {
        let bytes = match self.store.get(index_path).await {
            Ok(d) => d,
            Err(StorageError::NotFound) => return Ok(LockKeyIndexRead::Absent),
            Err(e) => return Err(Error::from(e)),
        };
        match parse_lock_key_index(&bytes) {
            Ok(index) => Ok(LockKeyIndexRead::Valid {
                storage_key: index.storage_key,
                bytes,
            }),
            Err(_) => Ok(LockKeyIndexRead::Corrupt { bytes }),
        }
    }

    /// Build the fingerprint-guarded `Read` + `Delete` pair retiring a corrupt
    /// dedup index: a concurrent valid rewrite turns the delete into a no-op
    /// conflict instead of being clobbered.
    fn corrupt_index_delete(index_path: String, bytes: &[u8]) -> (Read, Mutation) {
        let read = Read {
            key: index_path.clone(),
            fingerprint: Sha256::digest(bytes).into(),
        };
        let delete = Mutation::Delete {
            key: index_path,
            expected: None,
        };
        (read, delete)
    }

    /// Retire a corrupt dedup index through its own fingerprint-guarded
    /// transaction. A concurrent rewrite wins: `Conflict`/`Precondition` are
    /// treated as success, any other failure is logged and swallowed.
    async fn retire_corrupt_index(&self, index_path: &str, bytes: &[u8]) {
        let (read, delete) = Self::corrupt_index_delete(index_path.to_string(), bytes);
        let mut tx = Transaction::builder().mutation(delete).build();
        tx.reads.push(read);
        if let Err(e) = self.store.execute(tx).await
            && !matches!(e, TxError::Conflict | TxError::Precondition)
        {
            warn!(
                index_path,
                error = %tx_error_to_job(e),
                "Failed to retire corrupt lock-key index",
            );
        }
    }

    /// Build the read dependency and conditional `Delete` that retire a
    /// per-`lock_key` dedup index, only when it still points at
    /// `target_storage_key` (else the returned pair is empty).
    ///
    /// `index_storage_key` / `index_body` come from
    /// [`Self::get_lock_key_index_raw`]. The `Read` fingerprints `index_body`, so
    /// an index refreshed by a concurrent enqueue turns the delete into a no-op
    /// conflict. Callers fold this into their own transactions.
    fn conditional_index_delete(
        index_path: String,
        index_body: &[u8],
        index_storage_key: &str,
        target_storage_key: &str,
    ) -> (Option<Read>, Option<Mutation>) {
        if index_storage_key != target_storage_key {
            return (None, None);
        }
        let read = Read {
            key: index_path.clone(),
            fingerprint: Sha256::digest(index_body).into(),
        };
        let delete = Mutation::Delete {
            key: index_path,
            expected: None,
        };
        (Some(read), Some(delete))
    }

    /// Retire the dedup index of a just-claimed job so a same-`lock_key` enqueue
    /// arriving mid-execution starts a fresh pending file instead of coalescing
    /// into the resolved job. The delete is conditional and fingerprint-guarded
    /// so a producer's newer index is never clobbered.
    async fn retire_claimed_index(&self, queue: Queue, lock_key: &str, storage_key: &str) {
        let (index_storage_key, body) = match self.get_lock_key_index_raw(queue, lock_key).await {
            Ok(Some(found)) => found,
            Ok(None) => return,
            Err(e) => {
                warn!(lock_key, error = %e, "Failed to read dedup index at claim");
                return;
            }
        };
        let index_path = path_builder::job_lock_key_index_path(queue.as_str(), lock_key);
        let (read, delete) =
            Self::conditional_index_delete(index_path, &body, &index_storage_key, storage_key);
        if delete.is_none() {
            return;
        }
        let mut tx = Transaction::builder().build();
        tx.reads.extend(read);
        tx.mutations.extend(delete);
        if let Err(e) = self.store.execute(tx).await
            && !matches!(e, TxError::Conflict | TxError::Precondition)
        {
            warn!(lock_key, error = %tx_error_to_job(e), "Failed to retire dedup index at claim");
        }
    }

    // -----------------------------------------------------------------------
    // Producer: enqueue
    // -----------------------------------------------------------------------

    /// Build the two-`PutIfAbsent` enqueue transaction against a fresh
    /// time-keyed storage key. Index first, then pending: the CAS executor
    /// aborts cleanly if the first `PutIfAbsent` fails (another replica
    /// claimed this `lock_key`).
    fn build_enqueue_tx(envelope: &JobEnvelope, pending_body: Bytes) -> Result<Transaction, Error> {
        let storage_key = make_storage_key(Utc::now(), &envelope.id);
        let pending_path = path_builder::job_pending_path(envelope.queue.as_str(), &storage_key);
        let index_path =
            path_builder::job_lock_key_index_path(envelope.queue.as_str(), &envelope.lock_key);
        let index_body = Bytes::from(serialize_lock_key_index(&storage_key)?);
        Ok(Transaction::builder()
            .mutation(Mutation::PutIfAbsent {
                key: index_path,
                body: index_body,
            })
            .mutation(Mutation::PutIfAbsent {
                key: pending_path,
                body: pending_body,
            })
            .build())
    }

    fn record_enqueue(queue: Queue, outcome: &'static str) {
        metrics_provider()
            .job_queue_enqueued_total
            .with_label_values(&[queue.as_str(), outcome])
            .inc();
    }

    /// Enqueue a job. Fast path: `find_pending_with_lock_key` is a dedup hit
    /// and self-heals orphan and corrupt indexes. Slow path: a transaction
    /// `PutIfAbsent`s both the index and the pending file; a `Conflict` /
    /// `Precondition` is re-adjudicated by re-probing (a genuine racing
    /// producer is a dedup hit, a corrupt or orphan index is retired and the
    /// transaction retried once) so a job is never silently dropped.
    pub async fn enqueue(&self, envelope: JobEnvelope) -> Result<(), Error> {
        // Fast path: index and pending both present, a hit with no writes.
        if self
            .find_pending_with_lock_key(envelope.queue, &envelope.lock_key)
            .await
            .unwrap_or(false)
        {
            Self::record_enqueue(envelope.queue, "hit");
            return Ok(());
        }

        let pending_body = Bytes::from(
            serde_json::to_vec(&envelope)
                .map_err(|e| Error::Storage(format!("envelope serialization failed: {e}")))?,
        );

        let tx = Self::build_enqueue_tx(&envelope, pending_body.clone())?;
        match self.store.execute(tx).await {
            Ok(_) => {
                Self::record_enqueue(envelope.queue, "miss");
                Ok(())
            }
            Err(TxError::Conflict | TxError::Precondition) => {
                if self
                    .find_pending_with_lock_key(envelope.queue, &envelope.lock_key)
                    .await?
                {
                    Self::record_enqueue(envelope.queue, "hit");
                    return Ok(());
                }
                // The conflicting index was orphan or corrupt and the re-probe
                // retired it: retry once with a fresh storage key.
                let retry = Self::build_enqueue_tx(&envelope, pending_body)?;
                match self.store.execute(retry).await {
                    Ok(_) => {
                        Self::record_enqueue(envelope.queue, "miss");
                        Ok(())
                    }
                    Err(TxError::Conflict | TxError::Precondition) => Err(Error::Storage(format!(
                        "enqueue for lock_key '{}' lost the dedup race twice or its index is \
                         corrupt; job not enqueued",
                        envelope.lock_key
                    ))),
                    Err(e) => Err(tx_error_to_job(e)),
                }
            }
            Err(e) => Err(tx_error_to_job(e)),
        }
    }

    // -----------------------------------------------------------------------
    // Consumer: claim / complete / fail
    // -----------------------------------------------------------------------

    /// Claim the next available job from `queue`, self-throttling on backend
    /// failure with an exponential capped backoff that resets on the next
    /// successful claim.
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

    /// Walk pending storage keys in ascending (`not_before`) order, stopping at
    /// the first future prefix without reading its body; `next_ready` then
    /// carries that instant. A successful claim retires the job's dedup index
    /// (see [`Self::retire_claimed_index`]).
    async fn try_claim_one(&self, queue: Queue) -> Result<ClaimOutcome, Error> {
        let now = Utc::now();
        let mut next_ready: Option<DateTime<Utc>> = None;
        for storage_key in self.list_pending(queue, MAX_SCAN).await? {
            // Read readiness off the filename; a backed-off entry never GETs its
            // body. A malformed prefix falls through to the body read.
            if let Some(not_before) = parse_not_before(&storage_key)
                && not_before > now
            {
                next_ready = Some(next_ready.map_or(not_before, |t| t.min(not_before)));
                break;
            }
            let envelope = match self.read_pending(queue, &storage_key).await {
                Ok(e) => e,
                Err(Error::NotFound) => continue,
                Err(e) => return Err(e),
            };
            if let Some(session) = self
                .store
                .executor()
                .try_acquire(&[job_lock_key(&envelope.lock_key)])
                .await
                .map_err(tx_error_to_job)?
            {
                // Re-read under the lock: the job may have completed (pending
                // file deleted) between the pre-lock body read and this
                // acquisition, and a finished job must never be re-executed.
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
                debug!(
                    lock_key = envelope.lock_key.as_str(),
                    worker_id = self.worker_id.as_str(),
                    "Claimed job"
                );
                self.retire_claimed_index(queue, &envelope.lock_key, &storage_key)
                    .await;
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

    /// Mark a claimed job complete and release its execution lock. Appends the
    /// pending-file delete and a conditional dedup-index delete to `handler_tx`,
    /// commits the merged transaction atomically, then releases the lock so the
    /// next worker can claim this `lock_key` without waiting on TTL.
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
        let pending_path = path_builder::job_pending_path(envelope.queue.as_str(), &storage_key);
        let index_path =
            path_builder::job_lock_key_index_path(envelope.queue.as_str(), &envelope.lock_key);

        let mut tx = handler_tx;
        tx.mutations.push(Mutation::Delete {
            key: pending_path,
            expected: None,
        });

        // The execution lock excludes other workers but not producers: the
        // conditional read leaves an index pointing at someone else's pending
        // body alone, and a corrupt index is retired under its fingerprint so
        // a producer's concurrent valid rewrite survives.
        match self.read_lock_key_index_at(&index_path).await {
            Ok(LockKeyIndexRead::Valid {
                storage_key: index_storage_key,
                bytes,
            }) => {
                let (read, delete) = Self::conditional_index_delete(
                    index_path,
                    &bytes,
                    &index_storage_key,
                    &storage_key,
                );
                tx.reads.extend(read);
                tx.mutations.extend(delete);
            }
            Ok(LockKeyIndexRead::Corrupt { bytes }) => {
                let (read, delete) = Self::corrupt_index_delete(index_path, &bytes);
                tx.reads.push(read);
                tx.mutations.push(delete);
            }
            Ok(LockKeyIndexRead::Absent) => {}
            Err(e) => {
                // Cannot read the dedup index to build the cleanup: fail the job
                // over rather than leave the pending file re-claimable in a hot
                // loop. We still hold the lock, so `fail` is the sole writer.
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
                // The commit did not land: fail the job over (backoff then
                // dead-letter) instead of leaving it to be re-claimed forever.
                // We still hold the lock.
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

    /// Record a failure: re-queue with backoff, or dead-letter once the retry
    /// budget is spent. A retry rewrites the envelope to a new storage key
    /// encoding the bumped `not_before`, then deletes the old key; a crash
    /// between the two re-runs the envelope, covered by handler idempotency.
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

    /// Fold a fingerprint-guarded index `Put` into `tx`: the write lands only
    /// while the entry still carries the `observed` bytes.
    fn fold_guarded_index_put(
        tx: &mut Transaction,
        index_path: String,
        observed: &[u8],
        index_body: Bytes,
    ) {
        tx.reads.push(Read {
            key: index_path.clone(),
            fingerprint: Sha256::digest(observed).into(),
        });
        tx.mutations.push(Mutation::Put {
            key: index_path,
            body: index_body,
            expected: None,
        });
    }

    /// Build the unconditional pending swap of a retry: write the backed-off
    /// envelope under its new key and delete the old one.
    fn pending_swap_tx(
        new_pending_path: &str,
        pending_body: &Bytes,
        old_pending_path: &str,
    ) -> Transaction {
        Transaction::builder()
            .mutation(Mutation::Put {
                key: new_pending_path.to_string(),
                body: pending_body.clone(),
                expected: None,
            })
            .mutation(Mutation::Delete {
                key: old_pending_path.to_string(),
                expected: None,
            })
            .build()
    }

    /// Single transaction replaces the pending file, deletes the old pending,
    /// and re-points the dedup index at the new key. The execution lock is held
    /// across the call and released explicitly afterwards.
    ///
    /// The execution lock excludes consumers, not producers: a same-`lock_key`
    /// enqueue after the claim-time retire owns the index, so the update is
    /// fingerprint-guarded and skipped when the entry points at someone else's
    /// pending body. A guard conflict re-executes the pending swap alone; the
    /// stale index is healed by the enqueue fast path and the scrub reconcile.
    async fn fail_retry(
        &self,
        session: LockSession,
        updated: JobEnvelope,
        old_storage_key: String,
        next_at: DateTime<Utc>,
    ) -> Result<FailOutcome, Error> {
        let new_storage_key = make_storage_key(next_at, &updated.id);
        let new_pending_path =
            path_builder::job_pending_path(updated.queue.as_str(), &new_storage_key);
        let old_pending_path =
            path_builder::job_pending_path(updated.queue.as_str(), &old_storage_key);
        let index_path =
            path_builder::job_lock_key_index_path(updated.queue.as_str(), &updated.lock_key);

        let pending_body = Bytes::from(
            serde_json::to_vec(&updated)
                .map_err(|e| Error::Storage(format!("envelope serialization failed: {e}")))?,
        );
        let index_body = Bytes::from(serialize_lock_key_index(&new_storage_key)?);

        let mut tx = Self::pending_swap_tx(&new_pending_path, &pending_body, &old_pending_path);
        match self.read_lock_key_index_at(&index_path).await {
            Ok(LockKeyIndexRead::Absent) => {
                tx.mutations.push(Mutation::PutIfAbsent {
                    key: index_path,
                    body: index_body,
                });
            }
            Ok(LockKeyIndexRead::Valid { storage_key, bytes })
                if storage_key == old_storage_key =>
            {
                Self::fold_guarded_index_put(&mut tx, index_path, &bytes, index_body);
            }
            Ok(LockKeyIndexRead::Corrupt { bytes }) => {
                Self::fold_guarded_index_put(&mut tx, index_path, &bytes, index_body);
            }
            // Points at a concurrent producer's fresh pending body: theirs.
            Ok(LockKeyIndexRead::Valid { .. }) => {}
            // The swap must land regardless; leave the index to its healers.
            Err(e) => {
                warn!(
                    lock_key = updated.lock_key.as_str(),
                    error = %e,
                    "Failed to read dedup index at retry; skipping the index update",
                );
            }
        }

        let result = match self.store.execute(tx).await {
            Ok(_) => Ok(()),
            // The index swung between read and execute (a producer's enqueue):
            // it is theirs, so re-execute the pending swap without touching it.
            Err(TxError::Conflict | TxError::Precondition) => {
                let swap =
                    Self::pending_swap_tx(&new_pending_path, &pending_body, &old_pending_path);
                self.store.execute(swap).await.map(|_| ())
            }
            Err(e) => Err(e),
        };
        session.release().await;
        result.map_err(tx_error_to_job)?;

        Ok(FailOutcome::Retried { next_at })
    }

    /// Build the unconditional dead-letter pair: write the failed record and
    /// delete the pending file.
    fn dead_letter_tx(failed_path: &str, failed_body: &Bytes, pending_path: &str) -> Transaction {
        Transaction::builder()
            .mutation(Mutation::Put {
                key: failed_path.to_string(),
                body: failed_body.clone(),
                expected: None,
            })
            .mutation(Mutation::Delete {
                key: pending_path.to_string(),
                expected: None,
            })
            .build()
    }

    /// Single transaction writes the failed record and removes the pending and
    /// (conditionally) the index. The execution lock is held across the call
    /// and released explicitly afterwards. A corrupt index is retired under its
    /// fingerprint; a guard conflict re-executes the dead-letter pair alone.
    async fn fail_dead_letter(
        &self,
        session: LockSession,
        envelope: JobEnvelope,
        storage_key: String,
        err: &str,
    ) -> Result<FailOutcome, Error> {
        let failed_path = path_builder::job_failed_path(envelope.queue.as_str(), &storage_key);
        let pending_path = path_builder::job_pending_path(envelope.queue.as_str(), &storage_key);

        let failed_body = Bytes::from(serialize_dead_letter(&envelope, err)?);

        let mut tx = Self::dead_letter_tx(&failed_path, &failed_body, &pending_path);

        // Include the index delete only when it still points at our
        // storage_key or does not parse.
        let index_path =
            path_builder::job_lock_key_index_path(envelope.queue.as_str(), &envelope.lock_key);
        match self.read_lock_key_index_at(&index_path).await {
            Ok(LockKeyIndexRead::Valid {
                storage_key: index_storage_key,
                bytes,
            }) => {
                let (read, delete) = Self::conditional_index_delete(
                    index_path,
                    &bytes,
                    &index_storage_key,
                    &storage_key,
                );
                tx.reads.extend(read);
                tx.mutations.extend(delete);
            }
            Ok(LockKeyIndexRead::Corrupt { bytes }) => {
                let (read, delete) = Self::corrupt_index_delete(index_path, &bytes);
                tx.reads.push(read);
                tx.mutations.push(delete);
            }
            Ok(LockKeyIndexRead::Absent) => {}
            Err(e) => {
                session.release().await;
                return Err(e);
            }
        }

        let result = match self.store.execute(tx).await {
            Ok(_) => Ok(()),
            // The index swung between read and execute (a producer's enqueue):
            // it is theirs, so re-execute the dead-letter pair without it.
            Err(TxError::Conflict | TxError::Precondition) => {
                let pair = Self::dead_letter_tx(&failed_path, &failed_body, &pending_path);
                self.store.execute(pair).await.map(|_| ())
            }
            Err(e) => Err(e),
        };
        session.release().await;
        result.map_err(tx_error_to_job)?;

        Ok(FailOutcome::MovedToDeadLetter)
    }

    // -----------------------------------------------------------------------
    // Administrative mutations (operator-driven retry / delete)
    // -----------------------------------------------------------------------

    /// Move a dead-letter record back to pending with attempts reset to zero.
    /// Atomic: one transaction `PutIfAbsent`s a fresh pending file and deletes
    /// the failed record under an `ETag` fence. [`Error::NotFound`] for a stale
    /// key.
    ///
    /// The dedup index is not re-established, so a concurrent same-`lock_key`
    /// enqueue may create a second pending file; the execution lock still
    /// serialises the two and the handler contract makes the redundant run
    /// idempotent.
    pub async fn retry_failed(&self, queue: Queue, storage_key: &str) -> Result<(), Error> {
        let failed_path = path_builder::job_failed_path(queue.as_str(), storage_key);
        // Fencing ETag (`None` gives an unconditional delete, still safe: the
        // fresh pending key collides a double-retry at `PutIfAbsent`).
        let expected = self.store.head(&failed_path).await?.etag;

        let mut envelope = self.read_failed(queue, storage_key).await?.envelope;
        envelope.attempts = 0;

        let new_storage_key = make_storage_key(Utc::now(), &envelope.id);
        let pending_path = path_builder::job_pending_path(queue.as_str(), &new_storage_key);
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

        // A concurrent retry that won surfaces as Conflict/Precondition; either
        // way the job is back in flight, so both join the success arm.
        match self.store.execute(tx).await {
            Ok(_) | Err(TxError::Conflict | TxError::Precondition) => Ok(()),
            Err(e) => Err(tx_error_to_job(e)),
        }
    }

    /// Delete a job by `state`/`storage_key`. Failed records use a single
    /// ETag-fenced delete; pending deletes also fold in the conditional
    /// dedup-index delete, mirroring [`Self::fail_dead_letter`].
    ///
    /// A pending delete is best-effort: the operator holds no execution lock, so
    /// an in-flight claim still commits, but the `ETag` fence prevents clobbering
    /// a file the worker rewrote or removed. [`Error::NotFound`] for a stale key.
    pub async fn delete_job(
        &self,
        queue: Queue,
        state: JobState,
        storage_key: &str,
    ) -> Result<(), Error> {
        match state {
            JobState::Failed => {
                let failed_path = path_builder::job_failed_path(queue.as_str(), storage_key);
                let expected = self.store.head(&failed_path).await?.etag;
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

    /// Build the ETag-fenced delete of a pending file alone (no dedup index).
    /// The fence keeps the delete from clobbering a file a worker rewrote; the
    /// guard-conflict retry re-runs it after dropping the index delete.
    fn pending_delete_tx(pending_path: &str, expected: Option<Etag>) -> Transaction {
        Transaction::builder()
            .mutation(Mutation::Delete {
                key: pending_path.to_string(),
                expected,
            })
            .build()
    }

    async fn delete_pending(&self, queue: Queue, storage_key: &str) -> Result<(), Error> {
        let pending_path = path_builder::job_pending_path(queue.as_str(), storage_key);
        // Read the envelope (for its `lock_key`) and HEAD for the fence.
        let envelope = self.read_pending(queue, storage_key).await?;
        let expected = self.store.head(&pending_path).await?.etag;

        let mut tx = Self::pending_delete_tx(&pending_path, expected.clone());

        // Fold the index delete only when it still points at this key or does
        // not parse; a corrupt index is retired under its fingerprint so it can
        // never block the admin delete, mirroring `fail_dead_letter`.
        let index_path = path_builder::job_lock_key_index_path(queue.as_str(), &envelope.lock_key);
        match self.read_lock_key_index_at(&index_path).await? {
            LockKeyIndexRead::Valid {
                storage_key: index_storage_key,
                bytes,
            } => {
                let (read, delete) = Self::conditional_index_delete(
                    index_path,
                    &bytes,
                    &index_storage_key,
                    storage_key,
                );
                tx.reads.extend(read);
                tx.mutations.extend(delete);
            }
            LockKeyIndexRead::Corrupt { bytes } => {
                let (read, delete) = Self::corrupt_index_delete(index_path, &bytes);
                tx.reads.push(read);
                tx.mutations.push(delete);
            }
            LockKeyIndexRead::Absent => {}
        }

        // The index swung between read and execute (a producer's enqueue): it is
        // theirs, so re-run the fenced pending delete alone.
        match self.store.execute(tx).await {
            Ok(_) => Ok(()),
            Err(TxError::Conflict | TxError::Precondition) => {
                let retry = Self::pending_delete_tx(&pending_path, expected);
                self.store
                    .execute(retry)
                    .await
                    .map(|_| ())
                    .map_err(tx_error_to_job)
            }
            Err(e) => Err(tx_error_to_job(e)),
        }
    }
}

// ---------------------------------------------------------------------------
// Queue-depth gauge refresh loop
// ---------------------------------------------------------------------------

/// Refresh the `angos_job_queue_pending` and `angos_job_queue_failed` gauges for
/// `queue` on every `period` tick until `shutdown`. Missed ticks are coalesced.
/// `ready_horizon_secs` forwards to `count_pending`. Run by the server so both
/// gauges stay scrapeable even when `angos worker` drains the queue.
pub async fn queue_depth_refresh_loop(
    store: Arc<JobStore>,
    queue: Queue,
    period: Duration,
    ready_horizon_secs: u64,
    shutdown: CancellationToken,
) {
    let mut timer = interval(period);
    timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
    // Consume the immediate first tick so the first refresh is after `period`.
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
#[path = "job_store_tests.rs"]
mod tests;
