#[cfg(test)]
mod tests;

use std::{
    collections::HashMap,
    io::ErrorKind,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures_util::future::join_all;
use serde::{Deserialize, Deserializer, Serialize};
use tracing::{debug, warn};

use crate::{
    metrics_provider::metrics_provider,
    registry::{
        metadata_store::{
            Error,
            lock::{LockBackend, LockGuard},
            simple_jitter,
        },
        path_builder,
    },
    timing::elapsed_ms,
};
use angos_s3_client as s3_client;

const MAX_LOCK_TTL_SECS: u64 = 3600;

fn deserialize_ttl_secs<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
    let value = u64::deserialize(deserializer)?;
    if value < 9 {
        return Err(serde::de::Error::custom(
            "ttl_secs must be at least 9 (heartbeat runs at ttl/3)",
        ));
    }
    Ok(value)
}

fn deserialize_retry_delay_ms<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
    let value = u64::deserialize(deserializer)?;
    if value < 1 {
        return Err(serde::de::Error::custom(
            "retry_delay_ms must be at least 1",
        ));
    }
    Ok(value)
}

fn deserialize_max_hold_secs<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
    let value = u64::deserialize(deserializer)?;
    if value < 10 {
        return Err(serde::de::Error::custom(
            "max_hold_secs must be at least 10",
        ));
    }
    Ok(value)
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct S3LockConfig {
    #[serde(
        default = "S3LockConfig::default_ttl_secs",
        deserialize_with = "deserialize_ttl_secs"
    )]
    pub ttl_secs: u64,
    #[serde(default = "S3LockConfig::default_max_retries")]
    pub max_retries: u32,
    #[serde(
        default = "S3LockConfig::default_retry_delay_ms",
        deserialize_with = "deserialize_retry_delay_ms"
    )]
    pub retry_delay_ms: u64,
    #[serde(
        default = "S3LockConfig::default_max_hold_secs",
        deserialize_with = "deserialize_max_hold_secs"
    )]
    pub max_hold_secs: u64,
    #[serde(default = "S3LockConfig::default_operation_timeout_secs")]
    pub operation_timeout_secs: u64,
    #[serde(default = "S3LockConfig::default_operation_attempt_timeout_secs")]
    pub operation_attempt_timeout_secs: u64,
    #[serde(default = "S3LockConfig::default_max_attempts")]
    pub max_attempts: u32,
}

impl S3LockConfig {
    fn default_ttl_secs() -> u64 {
        30
    }

    fn default_max_retries() -> u32 {
        100
    }

    fn default_retry_delay_ms() -> u64 {
        50
    }

    fn default_max_hold_secs() -> u64 {
        300
    }

    fn default_operation_timeout_secs() -> u64 {
        15
    }

    fn default_operation_attempt_timeout_secs() -> u64 {
        4
    }

    fn default_max_attempts() -> u32 {
        2
    }
}

impl Default for S3LockConfig {
    fn default() -> Self {
        Self {
            ttl_secs: Self::default_ttl_secs(),
            max_retries: Self::default_max_retries(),
            retry_delay_ms: Self::default_retry_delay_ms(),
            max_hold_secs: Self::default_max_hold_secs(),
            operation_timeout_secs: Self::default_operation_timeout_secs(),
            operation_attempt_timeout_secs: Self::default_operation_attempt_timeout_secs(),
            max_attempts: Self::default_max_attempts(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct S3LockPayload {
    instance_id: String,
    refreshed_at: DateTime<Utc>,
    ttl_secs: u64,
}

impl S3LockPayload {
    // Uses S3's LastModified timestamp (set by the S3 server) instead of the embedded
    // refreshed_at field (set by the requesting instance) to eliminate inter-instance
    // clock skew. The refreshed_at field is retained for debugging and logging.
    fn is_expired(&self, last_modified: DateTime<Utc>) -> bool {
        let effective_ttl = self.ttl_secs.min(MAX_LOCK_TTL_SECS);
        let expiry = last_modified + chrono::Duration::seconds(effective_ttl.cast_signed());
        Utc::now() > expiry
    }
}

#[derive(Debug, Clone)]
pub struct S3LockBackend {
    store: Arc<s3_client::Backend>,
    instance_id: String,
    ttl_secs: u64,
    max_hold_secs: u64,
    max_retries: u32,
    retry_delay_ms: u64,
    supports_conditional_delete: bool,
}

impl S3LockBackend {
    pub fn new(
        store: Arc<s3_client::Backend>,
        config: &S3LockConfig,
        supports_conditional_delete: bool,
    ) -> Result<Self, Error> {
        if config.ttl_secs < 9 {
            return Err(Error::InvalidData("ttl_secs must be at least 9".into()));
        }
        if config.retry_delay_ms < 1 {
            return Err(Error::InvalidData(
                "retry_delay_ms must be at least 1".into(),
            ));
        }
        if config.ttl_secs > MAX_LOCK_TTL_SECS {
            return Err(Error::InvalidData(format!(
                "ttl_secs must be at most {MAX_LOCK_TTL_SECS}"
            )));
        }
        if config.max_hold_secs < config.ttl_secs {
            return Err(Error::InvalidData(
                "max_hold_secs must be >= ttl_secs".into(),
            ));
        }
        let heartbeat_interval = config.ttl_secs / 3;
        let worst_case_attempt =
            config.operation_attempt_timeout_secs * u64::from(config.max_attempts);
        if worst_case_attempt >= heartbeat_interval {
            warn!(
                operation_attempt_timeout_secs = config.operation_attempt_timeout_secs,
                max_attempts = config.max_attempts,
                heartbeat_interval_secs = heartbeat_interval,
                worst_case_secs = worst_case_attempt,
                "Lock S3 client worst-case timeout ({worst_case_attempt}s) >= heartbeat interval \
                 ({heartbeat_interval}s). A stuck S3 request could consume an entire heartbeat \
                 tick, reducing the safety margin before TTL expiry. Consider reducing \
                 operation_attempt_timeout_secs or max_attempts."
            );
        }
        Ok(Self {
            store,
            instance_id: uuid::Uuid::new_v4().to_string(),
            ttl_secs: config.ttl_secs,
            max_hold_secs: config.max_hold_secs,
            max_retries: config.max_retries,
            retry_delay_ms: config.retry_delay_ms,
            supports_conditional_delete,
        })
    }

    fn lock_path(key: &str) -> String {
        let shard = path_builder::shard_key(key);
        format!("_locks/{shard}/{key}")
    }

    fn jittered_delay(&self, attempt: u32) -> Duration {
        let max_delay_ms: u64 = 1000;
        let base_ms = self.retry_delay_ms.saturating_mul(1u64 << attempt.min(6));
        let capped_ms = base_ms.min(max_delay_ms);
        let jitter = simple_jitter(capped_ms / 2);
        Duration::from_millis(capped_ms.saturating_add(jitter))
    }

    fn make_payload(&self) -> Result<Vec<u8>, Error> {
        let payload = S3LockPayload {
            instance_id: self.instance_id.clone(),
            refreshed_at: Utc::now(),
            ttl_secs: self.ttl_secs,
        };
        serde_json::to_vec(&payload)
            .map_err(|e| Error::InvalidData(format!("lock payload serialization failed: {e}")))
    }

    fn make_guard(
        &self,
        lock_paths: Vec<String>,
        initial_etags: HashMap<String, String>,
    ) -> LockGuard {
        let valid = Arc::new(AtomicBool::new(true));
        let etag_cache = Arc::new(RwLock::new(initial_etags));
        let heartbeat_handle =
            self.spawn_heartbeat(lock_paths.clone(), valid.clone(), etag_cache.clone());
        let store = self.store.clone();
        let instance_id = self.instance_id.clone();
        let valid_for_release = valid.clone();
        let supports_conditional_delete = self.supports_conditional_delete;
        LockGuard::with_async_release_and_heartbeat(
            move || {
                Box::pin(release_guard(
                    valid_for_release,
                    lock_paths,
                    etag_cache,
                    store,
                    instance_id,
                    supports_conditional_delete,
                ))
            },
            valid,
            heartbeat_handle,
        )
    }

    async fn try_acquire_key(&self, lock_path: &str) -> Result<Option<String>, Error> {
        let payload = self.make_payload()?;
        match self
            .store
            .put_object_if_not_exists(lock_path, payload)
            .await
        {
            Ok(etag) => Ok(etag),
            Err(s3_client::Error::PreconditionFailed) => Ok(None),
            Err(e) => Err(Error::StorageBackend(e.to_string())),
        }
    }

    async fn try_recover_stale_lock(&self, lock_path: &str) -> RecoveryOutcome {
        let (data, etag, last_modified) = match self.store.read_with_metadata(lock_path).await {
            Ok(result) => result,
            Err(e) if e.kind() == ErrorKind::NotFound => {
                metrics_provider()
                    .lock_recoveries
                    .with_label_values(&["s3", "not_stale"])
                    .inc();
                return RecoveryOutcome::Retry;
            }
            Err(e) => {
                metrics_provider()
                    .lock_recoveries
                    .with_label_values(&["s3", "error"])
                    .inc();
                return RecoveryOutcome::Error(e.to_string());
            }
        };

        let payload: S3LockPayload = match serde_json::from_slice(&data) {
            Ok(p) => p,
            Err(e) => {
                metrics_provider()
                    .lock_recoveries
                    .with_label_values(&["s3", "error"])
                    .inc();
                return RecoveryOutcome::Error(format!("corrupt lock payload: {e}"));
            }
        };

        let last_modified = last_modified.unwrap_or(payload.refreshed_at);
        if !payload.is_expired(last_modified) {
            metrics_provider()
                .lock_recoveries
                .with_label_values(&["s3", "not_stale"])
                .inc();
            return RecoveryOutcome::NotStale;
        }

        debug!(
            lock_path,
            instance_id = payload.instance_id,
            refreshed_at = %payload.refreshed_at,
            "Recovering stale lock"
        );

        let Some(etag) = etag else {
            metrics_provider()
                .lock_recoveries
                .with_label_values(&["s3", "error"])
                .inc();
            return RecoveryOutcome::Error("lock object missing ETag".to_string());
        };

        let payload = match self.make_payload() {
            Ok(p) => p,
            Err(e) => {
                metrics_provider()
                    .lock_recoveries
                    .with_label_values(&["s3", "error"])
                    .inc();
                return RecoveryOutcome::Error(e.to_string());
            }
        };

        match self
            .store
            .put_object_if_match(lock_path, &etag, payload)
            .await
        {
            Ok(new_etag) => {
                metrics_provider()
                    .lock_recoveries
                    .with_label_values(&["s3", "acquired"])
                    .inc();
                RecoveryOutcome::Acquired(new_etag)
            }
            Err(s3_client::Error::PreconditionFailed) => {
                metrics_provider()
                    .lock_recoveries
                    .with_label_values(&["s3", "failed"])
                    .inc();
                RecoveryOutcome::Failed
            }
            Err(e) => {
                metrics_provider()
                    .lock_recoveries
                    .with_label_values(&["s3", "error"])
                    .inc();
                RecoveryOutcome::Error(e.to_string())
            }
        }
    }

    async fn release_paths(&self, paths: &[String]) {
        let futs: Vec<_> = paths
            .iter()
            .map(|path| async move {
                if let Err(e) = self.store.delete(path).await {
                    warn!(path, error = %e, "Failed to delete lock path during rollback");
                }
            })
            .collect();
        join_all(futs).await;
    }

    fn spawn_heartbeat(
        &self,
        paths: Vec<String>,
        valid: Arc<AtomicBool>,
        etag_cache: Arc<RwLock<HashMap<String, String>>>,
    ) -> tokio::task::JoinHandle<()> {
        let store = self.store.clone();
        let instance_id = self.instance_id.clone();
        let ttl_secs = self.ttl_secs;
        let max_hold_secs = self.max_hold_secs;
        let tick_deadline = Duration::from_secs(ttl_secs / 3);

        tokio::spawn(async move {
            let started_at = tokio::time::Instant::now();
            let max_hold = Duration::from_secs(max_hold_secs);
            let mut interval_timer = tokio::time::interval(tick_deadline);
            interval_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            interval_timer.tick().await; // consume immediate first tick

            let mut consecutive_failures: u32 = 0;

            loop {
                interval_timer.tick().await;

                if started_at.elapsed() >= max_hold {
                    warn!(
                        max_hold_secs,
                        "Lock held beyond maximum duration, invalidating"
                    );
                    metrics_provider()
                        .lock_invalidations
                        .with_label_values(&["s3", "max_hold"])
                        .inc();
                    valid.store(false, Ordering::Release);
                    return;
                }

                match run_heartbeat_tick(
                    &paths,
                    &store,
                    &instance_id,
                    ttl_secs,
                    tick_deadline,
                    &etag_cache,
                    &mut consecutive_failures,
                )
                .await
                {
                    TickOutcome::Continue => {}
                    TickOutcome::Invalidate(reason) => {
                        metrics_provider()
                            .lock_invalidations
                            .with_label_values(&["s3", reason])
                            .inc();
                        valid.store(false, Ordering::Release);
                        return;
                    }
                }
            }
        })
    }
}

enum TickOutcome {
    Continue,
    Invalidate(&'static str),
}

enum HeartbeatPathResult {
    Ok(Option<String>),
    Invalidate(&'static str),
    Failure,
}

async fn run_heartbeat_tick(
    paths: &[String],
    store: &s3_client::Backend,
    instance_id: &str,
    ttl_secs: u64,
    tick_deadline: Duration,
    etag_cache: &Arc<RwLock<HashMap<String, String>>>,
    consecutive_failures: &mut u32,
) -> TickOutcome {
    let results: Vec<(String, HeartbeatPathResult)> = join_all(paths.iter().map(|path| {
        let cached_etag = etag_cache.read().unwrap().get(path).cloned();
        let store = store.clone();
        let instance_id = instance_id.to_owned();
        async move {
            // Cap each tick to the heartbeat interval. The slow path
            // (no cached ETag) issues two sequential SDK calls that
            // could each consume the full operation timeout, exceeding
            // the heartbeat interval and risking TTL expiry.
            let result = if let Ok(result) = tokio::time::timeout(
                tick_deadline,
                heartbeat_tick_path(&store, path, &instance_id, ttl_secs, cached_etag),
            )
            .await
            {
                result
            } else {
                warn!(path, "Heartbeat tick timed out");
                HeartbeatPathResult::Failure
            };
            (path.clone(), result)
        }
    }))
    .await;

    let mut tick_had_failure = false;
    for (path, result) in results {
        match result {
            HeartbeatPathResult::Ok(new_etag) => {
                let mut cache = etag_cache.write().unwrap();
                if let Some(etag) = new_etag {
                    cache.insert(path, etag);
                } else {
                    cache.remove(&path);
                }
            }
            HeartbeatPathResult::Invalidate(reason) => {
                return TickOutcome::Invalidate(reason);
            }
            HeartbeatPathResult::Failure => {
                etag_cache.write().unwrap().remove(&path);
                tick_had_failure = true;
            }
        }
    }

    if tick_had_failure {
        *consecutive_failures = consecutive_failures.saturating_add(1);
        if u64::from(*consecutive_failures) * (ttl_secs / 3) >= ttl_secs {
            warn!(
                consecutive_failures,
                "Too many consecutive heartbeat tick failures, invalidating lock"
            );
            return TickOutcome::Invalidate("heartbeat_failure");
        }
    } else {
        *consecutive_failures = 0;
    }

    TickOutcome::Continue
}

async fn heartbeat_tick_path(
    store: &s3_client::Backend,
    path: &str,
    instance_id: &str,
    ttl_secs: u64,
    cached_etag: Option<String>,
) -> HeartbeatPathResult {
    let make_payload_bytes = || -> Result<Vec<u8>, String> {
        let payload = S3LockPayload {
            instance_id: instance_id.to_owned(),
            refreshed_at: Utc::now(),
            ttl_secs,
        };
        serde_json::to_vec(&payload).map_err(|e| format!("lock payload serialization failed: {e}"))
    };

    if let Some(etag) = cached_etag {
        let payload = match make_payload_bytes() {
            Ok(p) => p,
            Err(e) => {
                warn!(path, error = %e, "Failed to serialize lock payload");
                return HeartbeatPathResult::Failure;
            }
        };
        match store.put_object_if_match(path, &etag, payload).await {
            Ok(new_etag) => {
                debug!(path, "Refreshed S3 lock heartbeat");
                return HeartbeatPathResult::Ok(new_etag);
            }
            Err(s3_client::Error::PreconditionFailed) => {
                warn!(
                    path,
                    "Lock ETag changed, ownership lost, stopping heartbeat"
                );
                return HeartbeatPathResult::Invalidate("ownership_lost");
            }
            Err(e) => {
                warn!(path, error = %e, "Failed to refresh S3 lock heartbeat");
                return HeartbeatPathResult::Failure;
            }
        }
    }

    let etag = match store.read_with_etag(path).await {
        Ok((body, etag)) => {
            if let Ok(existing) = serde_json::from_slice::<S3LockPayload>(&body)
                && existing.instance_id != instance_id
            {
                warn!(
                    path,
                    expected = instance_id,
                    found = existing.instance_id,
                    "Lock ownership lost, stopping heartbeat"
                );
                return HeartbeatPathResult::Invalidate("ownership_lost");
            }
            etag
        }
        Err(e) if e.kind() == ErrorKind::NotFound => {
            warn!(path, "Lock file disappeared, stopping heartbeat");
            return HeartbeatPathResult::Invalidate("file_disappeared");
        }
        Err(e) => {
            warn!(path, error = %e, "Failed to read lock for heartbeat");
            return HeartbeatPathResult::Failure;
        }
    };

    let Some(etag) = etag else {
        warn!(
            path,
            "Lock ETag unavailable, cannot safely refresh heartbeat, invalidating lock"
        );
        return HeartbeatPathResult::Invalidate("etag_unavailable");
    };

    let payload = match make_payload_bytes() {
        Ok(p) => p,
        Err(e) => {
            warn!(path, error = %e, "Failed to serialize lock payload");
            return HeartbeatPathResult::Failure;
        }
    };

    match store.put_object_if_match(path, &etag, payload).await {
        Ok(new_etag) => {
            debug!(path, "Refreshed S3 lock heartbeat");
            HeartbeatPathResult::Ok(new_etag)
        }
        Err(s3_client::Error::PreconditionFailed) => {
            warn!(
                path,
                "Lock ETag changed, ownership lost, stopping heartbeat"
            );
            HeartbeatPathResult::Invalidate("ownership_lost")
        }
        Err(e) => {
            warn!(path, error = %e, "Failed to refresh S3 lock heartbeat");
            HeartbeatPathResult::Failure
        }
    }
}

async fn release_guard(
    valid: Arc<AtomicBool>,
    paths: Vec<String>,
    etag_cache: Arc<RwLock<HashMap<String, String>>>,
    store: Arc<s3_client::Backend>,
    instance_id: String,
    supports_conditional_delete: bool,
) {
    if !valid.load(Ordering::Acquire) {
        debug!("Lock ownership lost, skipping delete on release");
        return;
    }
    // Read cached ETags outside the futures so the RwLock guard is not
    // held across any await point.
    let futs: Vec<_> = paths
        .iter()
        .map(|path| {
            let cached_etag = etag_cache.read().unwrap().get(path).cloned();
            release_lock_path(
                store.clone(),
                path.clone(),
                instance_id.clone(),
                cached_etag,
                supports_conditional_delete,
            )
        })
        .collect();
    join_all(futs).await;
}

async fn release_lock_path(
    store: Arc<s3_client::Backend>,
    path: String,
    instance_id: String,
    cached_etag: Option<String>,
    supports_conditional_delete: bool,
) {
    // Fast path: use the heartbeat's cached ETag for an atomic conditional delete
    // without a preceding GET. A stale ETag (i.e. another instance took over) returns
    // PreconditionFailed and leaves the new owner's lock intact.
    if supports_conditional_delete && let Some(etag) = cached_etag {
        match store.delete_if_match(&path, &etag).await {
            Ok(()) => {}
            Err(s3_client::Error::PreconditionFailed) => {
                debug!(
                    path,
                    "Lock ETag changed during release, another instance owns it"
                );
            }
            Err(e) => warn!(path, error = %e, "Failed to delete lock on release"),
        }
        return;
    }
    // Slow path: no cached ETag — verify ownership via GET then delete. Covers the
    // case where no heartbeat tick has run yet and the non-conditional-delete fallback.
    match store.read_with_etag(&path).await {
        Ok((data, etag)) => match serde_json::from_slice::<S3LockPayload>(&data) {
            Ok(payload) if payload.instance_id == instance_id => {
                if supports_conditional_delete && let Some(etag) = etag {
                    match store.delete_if_match(&path, &etag).await {
                        Ok(()) => {}
                        Err(s3_client::Error::PreconditionFailed) => {
                            debug!(
                                path,
                                "Lock ETag changed during release, another instance owns it"
                            );
                        }
                        Err(e) => warn!(path, error = %e, "Failed to delete lock on release"),
                    }
                } else if let Err(e) = store.delete(&path).await {
                    warn!(path, error = %e, "Failed to delete lock on release");
                }
            }
            Ok(payload) => {
                debug!(
                    path,
                    expected = instance_id,
                    found = payload.instance_id,
                    "Lock ownership changed, skipping delete"
                );
            }
            Err(e) => {
                warn!(
                    path,
                    error = %e,
                    "Failed to deserialize lock payload on release, skipping delete"
                );
            }
        },
        Err(e) if e.kind() == ErrorKind::NotFound => {
            debug!(path, "Lock already deleted");
        }
        Err(e) => {
            warn!(
                path,
                error = %e,
                "Failed to read lock for ownership check on release, skipping delete"
            );
        }
    }
}

#[cfg_attr(test, derive(Debug))]
enum RecoveryOutcome {
    Acquired(Option<String>),
    NotStale,
    Retry,
    Failed,
    Error(String),
}

#[cfg(test)]
impl S3LockBackend {
    async fn test_try_recover(&self, key: &str) -> RecoveryOutcome {
        self.try_recover_stale_lock(&Self::lock_path(key)).await
    }
}

enum AcquireRoundOutcome {
    AllAcquired(HashMap<String, String>),
    HardError(Error),
    RecoveryError {
        msg: String,
        to_release: Vec<String>,
    },
    Retry {
        acquired: Vec<String>,
        recovered: Vec<String>,
    },
}

struct ClassifiedRound {
    acquired_etags: HashMap<String, String>,
    acquired_paths: Vec<String>,
    failed_indices: Vec<usize>,
}

struct RecoveredRound {
    acquired_etags: HashMap<String, String>,
    acquired_paths: Vec<String>,
    recovered_paths: Vec<String>,
}

impl S3LockBackend {
    /// Walks per-key acquire results into (etags, acquired paths, failed indices).
    /// Returns `Err(HardError(_))` after releasing any partially-acquired paths
    /// when one of the underlying PUTs returned an unrecoverable backend error.
    async fn classify_acquire_results(
        &self,
        lock_paths: &[String],
        results: Vec<(usize, Result<Option<String>, Error>)>,
    ) -> Result<ClassifiedRound, AcquireRoundOutcome> {
        let mut acquired_etags: HashMap<String, String> = HashMap::new();
        let mut acquired_paths = Vec::new();
        let mut failed_indices = Vec::new();
        let mut hard_error: Option<Error> = None;

        for (i, result) in &results {
            match result {
                Ok(Some(etag)) => {
                    let path = lock_paths[*i].clone();
                    acquired_etags.insert(path.clone(), etag.clone());
                    acquired_paths.push(path);
                }
                Ok(None) => failed_indices.push(*i),
                Err(e) => {
                    hard_error = Some(Error::StorageBackend(format!("S3 lock error: {e}")));
                    break;
                }
            }
        }

        if let Some(e) = hard_error {
            self.release_paths(&acquired_paths).await;
            return Err(AcquireRoundOutcome::HardError(e));
        }

        Ok(ClassifiedRound {
            acquired_etags,
            acquired_paths,
            failed_indices,
        })
    }

    /// For each failed index, attempts stale-lock recovery and merges the
    /// outcome. Short-circuits on `RecoveryOutcome::Error` by surfacing
    /// `Err(RecoveryError { ... })` carrying the paths that must be released
    /// by the caller.  When `classified.failed_indices` is empty, the helper
    /// is a no-op fast path.
    async fn run_recoveries(
        &self,
        lock_paths: &[String],
        classified: ClassifiedRound,
    ) -> Result<RecoveredRound, AcquireRoundOutcome> {
        let ClassifiedRound {
            mut acquired_etags,
            acquired_paths,
            failed_indices,
        } = classified;

        if failed_indices.is_empty() {
            return Ok(RecoveredRound {
                acquired_etags,
                acquired_paths,
                recovered_paths: Vec::new(),
            });
        }

        let recovery_futs: Vec<_> = failed_indices
            .iter()
            .map(|&i| {
                let path = lock_paths[i].clone();
                async move { (i, self.try_recover_stale_lock(&path).await) }
            })
            .collect();

        let mut recovered_paths = Vec::new();
        for (i, outcome) in join_all(recovery_futs).await {
            match outcome {
                RecoveryOutcome::Acquired(new_etag) => {
                    let path = lock_paths[i].clone();
                    if let Some(etag) = new_etag {
                        acquired_etags.insert(path.clone(), etag);
                    }
                    recovered_paths.push(path);
                }
                RecoveryOutcome::Error(msg) => {
                    let mut to_release = acquired_paths;
                    to_release.extend(recovered_paths);
                    return Err(AcquireRoundOutcome::RecoveryError { msg, to_release });
                }
                RecoveryOutcome::Failed | RecoveryOutcome::NotStale | RecoveryOutcome::Retry => {}
            }
        }

        Ok(RecoveredRound {
            acquired_etags,
            acquired_paths,
            recovered_paths,
        })
    }

    /// Pure decision: returns `AllAcquired` when every path is held, else `Retry`.
    fn finalize_round(lock_paths: &[String], recovered: RecoveredRound) -> AcquireRoundOutcome {
        let RecoveredRound {
            acquired_etags,
            acquired_paths,
            recovered_paths,
        } = recovered;

        if recovered_paths.len() + acquired_paths.len() == lock_paths.len() {
            return AcquireRoundOutcome::AllAcquired(acquired_etags);
        }

        AcquireRoundOutcome::Retry {
            acquired: acquired_paths,
            recovered: recovered_paths,
        }
    }

    async fn try_acquire_round(&self, lock_paths: &[String]) -> AcquireRoundOutcome {
        // Parallel PUTs with overlapping key sets across instances can cause
        // repeated rollbacks. This parallel round is used only for the first
        // attempt (optimistic fast path). On any failure, `acquire` switches
        // to `try_acquire_sequential` which acquires keys one-by-one in sorted
        // order, eliminating circular wait and preventing livelock. Randomized
        // jitter on retry delays desynchronises retrying instances.
        let futs: Vec<_> = lock_paths
            .iter()
            .enumerate()
            .map(|(i, path)| {
                let path = path.clone();
                async move { (i, self.try_acquire_key(&path).await) }
            })
            .collect();
        let results = join_all(futs).await;

        let classified = match self.classify_acquire_results(lock_paths, results).await {
            Ok(c) => c,
            Err(outcome) => return outcome,
        };
        let recovered = match self.run_recoveries(lock_paths, classified).await {
            Ok(r) => r,
            Err(outcome) => return outcome,
        };
        Self::finalize_round(lock_paths, recovered)
    }

    async fn try_acquire_sequential(&self, lock_paths: &[String]) -> AcquireRoundOutcome {
        let mut acquired_etags: HashMap<String, String> = HashMap::new();
        let mut acquired_paths: Vec<String> = Vec::new();

        for path in lock_paths {
            match self.try_acquire_key(path).await {
                Ok(Some(etag)) => {
                    acquired_etags.insert(path.clone(), etag);
                    acquired_paths.push(path.clone());
                }
                Ok(None) => match self.try_recover_stale_lock(path).await {
                    RecoveryOutcome::Acquired(new_etag) => {
                        if let Some(etag) = new_etag {
                            acquired_etags.insert(path.clone(), etag);
                        }
                        acquired_paths.push(path.clone());
                    }
                    RecoveryOutcome::Error(msg) => {
                        self.release_paths(&acquired_paths).await;
                        return AcquireRoundOutcome::RecoveryError {
                            msg,
                            to_release: Vec::new(),
                        };
                    }
                    _ => {
                        self.release_paths(&acquired_paths).await;
                        return AcquireRoundOutcome::Retry {
                            acquired: Vec::new(),
                            recovered: Vec::new(),
                        };
                    }
                },
                Err(e) => {
                    self.release_paths(&acquired_paths).await;
                    return AcquireRoundOutcome::HardError(e);
                }
            }
        }

        AcquireRoundOutcome::AllAcquired(acquired_etags)
    }
}

#[async_trait]
impl LockBackend for S3LockBackend {
    async fn acquire(&self, keys: &[String]) -> Result<LockGuard, Error> {
        if keys.is_empty() {
            return Ok(LockGuard::with_async_release(|| Box::pin(async {})));
        }

        let start = std::time::Instant::now();

        let mut lock_paths: Vec<String> = keys.iter().map(|k| Self::lock_path(k)).collect();
        lock_paths.sort();
        lock_paths.dedup();

        let mut retries = self.max_retries;
        let mut use_sequential = false;

        loop {
            let round_result = if use_sequential {
                self.try_acquire_sequential(&lock_paths).await
            } else {
                self.try_acquire_round(&lock_paths).await
            };
            match round_result {
                AcquireRoundOutcome::AllAcquired(etags) => {
                    metrics_provider()
                        .lock_acquisition_duration
                        .with_label_values(&["s3"])
                        .observe(elapsed_ms(start));
                    metrics_provider()
                        .lock_acquisitions
                        .with_label_values(&["s3", "success"])
                        .inc();
                    return Ok(self.make_guard(lock_paths, etags));
                }
                AcquireRoundOutcome::HardError(e) => {
                    metrics_provider()
                        .lock_acquisition_duration
                        .with_label_values(&["s3"])
                        .observe(elapsed_ms(start));
                    metrics_provider()
                        .lock_acquisitions
                        .with_label_values(&["s3", "error"])
                        .inc();
                    return Err(e);
                }
                AcquireRoundOutcome::RecoveryError { msg, to_release } => {
                    self.release_paths(&to_release).await;
                    metrics_provider()
                        .lock_acquisition_duration
                        .with_label_values(&["s3"])
                        .observe(elapsed_ms(start));
                    metrics_provider()
                        .lock_acquisitions
                        .with_label_values(&["s3", "error"])
                        .inc();
                    return Err(Error::StorageBackend(format!(
                        "S3 lock recovery error: {msg}"
                    )));
                }
                AcquireRoundOutcome::Retry {
                    acquired,
                    recovered,
                } => {
                    self.release_paths(&acquired).await;
                    if !recovered.is_empty() {
                        self.release_paths(&recovered).await;
                    }
                    if retries == 0 {
                        metrics_provider()
                            .lock_acquisition_duration
                            .with_label_values(&["s3"])
                            .observe(elapsed_ms(start));
                        metrics_provider()
                            .lock_acquisitions
                            .with_label_values(&["s3", "timeout"])
                            .inc();
                        return Err(Error::Lock(format!(
                            "Failed to acquire S3 locks after {} attempts for keys: {:?}",
                            self.max_retries, keys
                        )));
                    }
                    retries -= 1;
                    let attempt = self.max_retries - retries;
                    use_sequential = true;
                    metrics_provider()
                        .lock_retries
                        .with_label_values(&["s3"])
                        .inc();
                    debug!(retries_left = retries, "S3 lock busy, retrying...");
                    tokio::time::sleep(self.jittered_delay(attempt)).await;
                }
            }
        }
    }
}
