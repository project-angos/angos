//! [`JobStore`] — unified job-queue storage, producer, and consumer backed by
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

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest as _, Sha256};
use tokio::{
    select,
    time::{MissedTickBehavior, interval},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};
use uuid::Uuid;

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
// Configuration
// ---------------------------------------------------------------------------

/// Job-queue tunables. Setting `[global.job_queue]` selects the durable
/// backend (surviving restarts); omitting the section selects the in-process
/// backend over an in-memory store (jobs are discarded on restart).
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
    /// Logical queue name; selects the storage prefix and the worker's
    /// `--queue` filter.
    pub queue: String,
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
/// effect is expressible as engine mutations need not be idempotent — each
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
/// autoscaling, which only needs ordinal granularity at high queue depths —
/// once the queue exceeds 10× the worker pool size you're at max scale anyway.
/// Capping here bounds S3 `LIST` cost per refresh tick to ~10 paginated calls
/// regardless of how deep the queue actually is. Operators reading the gauge
/// should treat the cap value as "≥ this many".
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
// JobStore — unified producer + consumer + storage
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
}

impl JobStore {
    /// Construct a new `JobStore`.
    ///
    /// `worker_id` is a structured-log tag that makes concurrent workers'
    /// `claim_one` actions distinguishable in aggregated logs. Pass an empty
    /// string for producer-only instances.
    pub fn new(store: Arc<Store>, worker_id: impl Into<String>) -> Self {
        Self {
            store,
            worker_id: worker_id.into(),
        }
    }

    // -----------------------------------------------------------------------
    // Storage primitives
    // -----------------------------------------------------------------------

    /// List up to `n` pending storage keys in ascending order. Storage keys
    /// start with a sortable hex-millis prefix, so callers can stop scanning
    /// at the first key whose prefix is in the future.
    pub async fn list_pending(&self, queue: &str, n: u16) -> Result<Vec<String>, Error> {
        let prefix = path_builder::job_pending_dir(queue);
        let page = self.store.list(&prefix, 1000, None).await?;

        Ok(page
            .items
            .into_iter()
            .filter_map(|name| name.strip_suffix(".json").map(str::to_string))
            .take(n as usize)
            .collect())
    }

    pub async fn read_pending(&self, queue: &str, storage_key: &str) -> Result<JobEnvelope, Error> {
        let key = path_builder::job_pending_path(queue, storage_key);
        let data = self.store.get(&key).await?;
        serde_json::from_slice(&data)
            .map_err(|e| Error::Storage(format!("failed to parse envelope: {e}")))
    }

    /// Count pending envelopes ready for handling within
    /// `[..., now + ready_horizon_secs]`. Capped at [`MAX_REPORTED_PENDING`].
    pub async fn count_pending(&self, queue: &str, ready_horizon_secs: u64) -> Result<u64, Error> {
        let prefix = path_builder::job_pending_dir(queue);
        let cutoff_prefix = pending_ready_cutoff_prefix(ready_horizon_secs);
        let mut count: u64 = 0;
        let mut token: Option<String> = None;
        loop {
            let page = self.store.list(&prefix, 1000, token).await?;
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

    /// `true` when any pending job in `queue` carries `lock_key`. Best-effort
    /// dedup backed by an O(1) index file written alongside each pending
    /// envelope (see [`LockKeyIndex`]).
    ///
    /// When the index references a pending file that has vanished (orphan),
    /// submits a one-mutation engine transaction to delete the stale index,
    /// guarded by a read fingerprint so a concurrent enqueue that refreshes
    /// the index between our GET and the apply is never accidentally deleted.
    pub async fn find_pending_with_lock_key(
        &self,
        queue: &str,
        lock_key: &str,
    ) -> Result<bool, Error> {
        let index_path = path_builder::job_lock_key_index_path(queue, lock_key);
        let data = match self.store.get(&index_path).await {
            Ok(d) => d,
            Err(StorageError::NotFound) => return Ok(false),
            Err(e) => return Err(Error::from(e)),
        };
        let index = parse_lock_key_index(&data)?;

        let pending_key = path_builder::job_pending_path(queue, &index.storage_key);
        match self.store.head(&pending_key).await {
            Ok(_) => Ok(true),
            Err(StorageError::NotFound) => {
                // Orphan: pending file vanished but the index lingers. Submit a
                // one-mutation engine transaction whose Read fingerprint validates
                // the index hasn't been refreshed by a concurrent enqueue between
                // our GET and the apply. If it has, the engine returns Conflict —
                // we don't delete the fresh index. Passing the index's own
                // `storage_key` as the target makes the conditional delete fire
                // for this orphan while reusing the shared fingerprint guard.
                let (read, delete) = Self::conditional_index_delete(
                    index_path,
                    &data,
                    &index.storage_key,
                    &index.storage_key,
                );
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

    /// Return the raw bytes of the per-`lock_key` dedup index alongside the
    /// parsed `storage_key` it contains, or `None` when the index is absent.
    pub async fn get_lock_key_index_raw(
        &self,
        queue: &str,
        lock_key: &str,
    ) -> Result<Option<(String, Vec<u8>)>, Error> {
        let index_path = path_builder::job_lock_key_index_path(queue, lock_key);
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
    /// into a no-op conflict rather than clobbering the fresh index. Three
    /// call sites fold this into their own transactions: the orphan self-heal
    /// in `find_pending_with_lock_key`, `complete`, and `fail_dead_letter`.
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

    // -----------------------------------------------------------------------
    // Producer — enqueue
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
    pub async fn enqueue(&self, envelope: JobEnvelope) -> Result<(), Error> {
        // Fast path: index present and pending exists → hit, no writes needed.
        if self
            .find_pending_with_lock_key(&envelope.queue, &envelope.lock_key)
            .await
            .unwrap_or(false)
        {
            metrics_provider()
                .job_queue_enqueued_total
                .with_label_values(&[envelope.queue.as_str(), "hit"])
                .inc();
            return Ok(());
        }

        // Slow path: build the transaction.
        let storage_key = make_storage_key(Utc::now(), &envelope.id);
        let pending_path = path_builder::job_pending_path(&envelope.queue, &storage_key);
        let index_path = path_builder::job_lock_key_index_path(&envelope.queue, &envelope.lock_key);

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
        // race — treat as a dedup hit, not an error.
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
    // Consumer — claim / complete / fail
    // -----------------------------------------------------------------------

    /// Claim the next available job from `queue`. Walks pending storage keys
    /// in ascending order (`list_pending` returns them sorted by the hex
    /// unix-millis prefix, i.e. by `not_before`). Stops at the first key whose
    /// prefix is in the future without reading its body — the prefix is the
    /// authoritative readiness signal. When no claim is made, `next_ready`
    /// carries that first future instant so the caller can sleep until then.
    pub async fn claim_one(&self, queue: &str) -> Result<ClaimOutcome, Error> {
        let now = Utc::now();
        let mut next_ready: Option<DateTime<Utc>> = None;
        for storage_key in self.list_pending(queue, MAX_SCAN).await? {
            // Read the readiness time off the filename; never GET the body for
            // a backed-off entry. A missing/malformed prefix falls through to
            // the body read so legacy keys (if any) still work.
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
    ) -> Result<(), Error> {
        let ClaimedJob {
            envelope,
            storage_key,
            session,
        } = claimed;
        let pending_path = path_builder::job_pending_path(&envelope.queue, &storage_key);
        let index_path = path_builder::job_lock_key_index_path(&envelope.queue, &envelope.lock_key);

        let mut tx = handler_tx;
        tx.mutations.push(Mutation::Delete {
            key: pending_path,
            expected: None,
        });

        // We hold the execution lock, so no concurrent worker can swing the
        // index. The conditional read is a belt-and-braces guard that costs
        // one HEAD; if the index points elsewhere we leave it alone. A corrupt
        // index cannot be matched or fingerprinted, so we delete it
        // unconditionally — safe because we are the unique writer here.
        match self.get_raw(&index_path).await {
            Ok(body) => match parse_lock_key_index(&body) {
                Ok(index) => {
                    let (read, delete) = Self::conditional_index_delete(
                        index_path,
                        &body,
                        &index.storage_key,
                        &storage_key,
                    );
                    tx.reads.extend(read);
                    tx.mutations.extend(delete);
                }
                Err(_) => {
                    tx.mutations.push(Mutation::Delete {
                        key: index_path,
                        expected: None,
                    });
                }
            },
            Err(Error::NotFound) => {}
            Err(e) => {
                session.release().await;
                return Err(e);
            }
        }

        let result = self.store.execute(tx).await;
        session.release().await;
        result.map(|_| ()).map_err(tx_error_to_job)
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

        let delay = backoff(new_attempts);
        let next_at = Utc::now() + ChronoDuration::from_std(delay).unwrap_or_default();
        let updated = JobEnvelope {
            attempts: new_attempts,
            ..envelope
        };

        self.fail_retry(session, updated, storage_key, next_at)
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
        let new_pending_path = path_builder::job_pending_path(&updated.queue, &new_storage_key);
        let old_pending_path = path_builder::job_pending_path(&updated.queue, &old_storage_key);
        let index_path = path_builder::job_lock_key_index_path(&updated.queue, &updated.lock_key);

        let pending_body = Bytes::from(
            serde_json::to_vec(&updated)
                .map_err(|e| Error::Storage(format!("envelope serialization failed: {e}")))?,
        );
        let index_body = Bytes::from(serialize_lock_key_index(&new_storage_key)?);

        // We hold the execution lock, so we are the unique writer for this
        // lock_key. The index update is unconditional (no read fingerprint
        // needed): no concurrent producer can swing the index while we hold the
        // execution lock.
        let tx = Transaction::builder()
            .mutation(Mutation::Put {
                key: new_pending_path,
                body: pending_body,
                expected: None,
            })
            .mutation(Mutation::Put {
                key: index_path,
                body: index_body,
                expected: None,
            })
            .mutation(Mutation::Delete {
                key: old_pending_path,
                expected: None,
            })
            .build();

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
        let failed_path = path_builder::job_failed_path(&envelope.queue, &storage_key);
        let pending_path = path_builder::job_pending_path(&envelope.queue, &storage_key);

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
        // points at our storage_key. Read the index to get the fingerprint.
        let index_path = path_builder::job_lock_key_index_path(&envelope.queue, &envelope.lock_key);
        match self
            .get_lock_key_index_raw(&envelope.queue, &envelope.lock_key)
            .await
        {
            Ok(Some((index_storage_key, body))) => {
                let (read, delete) = Self::conditional_index_delete(
                    index_path,
                    &body,
                    &index_storage_key,
                    &storage_key,
                );
                tx.reads.extend(read);
                tx.mutations.extend(delete);
            }
            Ok(None) => {}
            Err(e) => {
                session.release().await;
                return Err(e);
            }
        }

        let result = self.store.execute(tx).await;
        session.release().await;
        result.map_err(tx_error_to_job)?;

        Ok(FailOutcome::MovedToDeadLetter)
    }
}

// ---------------------------------------------------------------------------
// Backoff
// ---------------------------------------------------------------------------

/// Exponential backoff for retry delays: `min(1 min * 2^n, 10 min)`. `n = 0`
/// means "first failure". The shift is bounded so it cannot overflow.
pub fn backoff(n: u32) -> Duration {
    Duration::from_mins(1 << n.min(4)).min(Duration::from_mins(10))
}

// ---------------------------------------------------------------------------
// Pending-gauge refresh loop
// ---------------------------------------------------------------------------

/// Refresh `angos_job_queue_pending` for `queue` on every `period` tick,
/// until `shutdown` is cancelled. Uses `tokio::time::interval` so the cadence
/// stays fixed across slow `count_pending` calls; missed ticks are coalesced
/// rather than catching up.
///
/// `ready_horizon_secs` is forwarded to `count_pending`: only envelopes ready
/// within that window contribute to the gauge.
pub async fn pending_refresh_loop(
    store: Arc<JobStore>,
    queue: String,
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
        match store.count_pending(&queue, ready_horizon_secs).await {
            Ok(count) => {
                metrics_provider()
                    .job_queue_pending
                    .with_label_values(&[queue.as_str()])
                    .set(i64::try_from(count).unwrap_or(i64::MAX));
            }
            Err(e) => debug!(queue = %queue, error = %e, "Failed to refresh pending gauge"),
        }
    }
}

#[cfg(test)]
#[path = "job_store_tests.rs"]
mod tests;
