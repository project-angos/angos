use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

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
    #[serde(default = "default_lease_ttl_secs")]
    pub default_lease_ttl_secs: u64,
    #[serde(default = "default_pending_refresh_interval_secs")]
    pub pending_refresh_interval_secs: u64,
    #[serde(flatten)]
    pub backend: JobQueueBackend,
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

/// Envelope that travels through every queue backend. The `payload` field is
/// untyped so the envelope shape stays stable across payload churn; handlers
/// deserialize it into a concrete type.
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
    /// Earliest time a worker may claim this job (used for retry backoff).
    pub not_before: DateTime<Utc>,
    pub attempts: u32,
    pub max_attempts: u32,
    pub payload: serde_json::Value,
}

impl JobEnvelope {
    /// Build an envelope with a new ULID, default retry budget, and a typed
    /// payload that will be serialized to JSON. Returns `Err` only if the
    /// payload type cannot be serialized.
    pub fn new<P: Serialize>(
        queue: impl Into<String>,
        kind: impl Into<String>,
        lock_key: impl Into<String>,
        payload: &P,
    ) -> Result<Self, serde_json::Error> {
        let now = Utc::now();
        Ok(Self {
            id: Ulid::new().to_string(),
            queue: queue.into(),
            kind: kind.into(),
            lock_key: lock_key.into(),
            created_at: now,
            not_before: now,
            attempts: 0,
            max_attempts: 5,
            payload: serde_json::to_value(payload)?,
        })
    }
}

/// Producer-side interface: enqueue a job. Implementations should deduplicate
/// by `lock_key` when a matching pending job already exists.
#[async_trait]
pub trait JobQueue: Send + Sync {
    async fn enqueue(&self, envelope: JobEnvelope) -> Result<(), Error>;
}

/// Executor for a single job kind. Returns a stringified error on failure;
/// the queue runtime turns that into a retry or dead-letter outcome.
#[async_trait]
pub trait JobHandler: Send + Sync {
    async fn execute(&self, envelope: &JobEnvelope) -> Result<(), String>;
}

/// Maximum number of pending envelopes inspected per scan. Deeper queues fall
/// back to lease-based serialization, which is the actual correctness primitive.
pub const MAX_SCAN: u16 = 1000;

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
#[async_trait]
pub trait JobStore: Send + Sync {
    /// Write a new pending envelope. Overwrites any previous envelope with the
    /// same `id` (used when rewriting with bumped `attempts` / `not_before`).
    async fn put_pending(&self, queue: &str, id: &str, envelope: &JobEnvelope)
    -> Result<(), Error>;

    /// List up to `n` pending job IDs in ascending ULID order.
    async fn list_pending(&self, queue: &str, n: u16) -> Result<Vec<String>, Error>;

    async fn read_pending(&self, queue: &str, id: &str) -> Result<JobEnvelope, Error>;

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

    async fn remove_pending(&self, queue: &str, id: &str) -> Result<(), Error>;

    /// Remove a lease, but only when the stored token matches.
    async fn remove_lease(&self, lock_key: &str, token: &str) -> Result<(), Error>;

    async fn move_to_failed(
        &self,
        queue: &str,
        id: &str,
        envelope: &JobEnvelope,
        last_error: &str,
    ) -> Result<(), Error>;

    async fn count_pending(&self, queue: &str) -> Result<u64, Error>;

    /// `true` when any pending job in `queue` carries `lock_key`. Scans at
    /// most [`MAX_SCAN`] envelopes; this is best-effort dedup, not a
    /// correctness primitive.
    async fn find_pending_with_lock_key(&self, queue: &str, lock_key: &str) -> Result<bool, Error> {
        for id in self.list_pending(queue, MAX_SCAN).await? {
            match self.read_pending(queue, &id).await {
                Ok(env) if env.lock_key == lock_key => return Ok(true),
                Ok(_) | Err(Error::NotFound) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(false)
    }
}
