#[cfg(test)]
mod tests;

use std::{
    io::ErrorKind,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures_util::future::join_all;
use serde::{Deserialize, Deserializer, Serialize};
use tracing::{debug, warn};

use crate::registry::{
    data_store,
    metadata_store::{
        Error,
        lock::{LockBackend, LockGuard},
    },
};

const MAX_LOCK_TTL_SECS: u64 = 3600;

fn deserialize_ttl_secs<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
    let value = u64::deserialize(deserializer)?;
    if value < 3 {
        return Err(serde::de::Error::custom(
            "ttl_secs must be at least 3 (heartbeat runs at ttl/3)",
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
}

impl Default for S3LockConfig {
    fn default() -> Self {
        Self {
            ttl_secs: Self::default_ttl_secs(),
            max_retries: Self::default_max_retries(),
            retry_delay_ms: Self::default_retry_delay_ms(),
            max_hold_secs: Self::default_max_hold_secs(),
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
    store: Arc<data_store::s3::Backend>,
    instance_id: String,
    ttl_secs: u64,
    max_hold_secs: u64,
    max_retries: u32,
    retry_delay_ms: u64,
}

impl S3LockBackend {
    pub fn new(store: Arc<data_store::s3::Backend>, config: &S3LockConfig) -> Result<Self, Error> {
        if config.ttl_secs < 3 {
            return Err(Error::InvalidData("ttl_secs must be at least 3".into()));
        }
        if config.retry_delay_ms < 1 {
            return Err(Error::InvalidData(
                "retry_delay_ms must be at least 1".into(),
            ));
        }
        if config.max_hold_secs < config.ttl_secs {
            return Err(Error::InvalidData(
                "max_hold_secs must be >= ttl_secs".into(),
            ));
        }
        Ok(Self {
            store,
            instance_id: uuid::Uuid::new_v4().to_string(),
            ttl_secs: config.ttl_secs,
            max_hold_secs: config.max_hold_secs,
            max_retries: config.max_retries,
            retry_delay_ms: config.retry_delay_ms,
        })
    }

    fn lock_path(key: &str) -> String {
        format!("_locks/{key}")
    }

    fn jittered_delay(&self, attempt: u32) -> Duration {
        let max_delay_ms: u64 = 1000;
        let base_ms = self.retry_delay_ms.saturating_mul(1u64 << attempt.min(6));
        let jitter = simple_jitter(base_ms.min(max_delay_ms) / 2);
        let total_ms = base_ms.saturating_add(jitter).min(max_delay_ms);
        Duration::from_millis(total_ms)
    }

    fn make_payload(&self) -> Vec<u8> {
        let payload = S3LockPayload {
            instance_id: self.instance_id.clone(),
            refreshed_at: Utc::now(),
            ttl_secs: self.ttl_secs,
        };
        serde_json::to_vec(&payload).expect("lock payload serialization cannot fail")
    }

    fn make_guard(&self, lock_paths: Vec<String>) -> LockGuard {
        let valid = Arc::new(AtomicBool::new(true));
        let heartbeat_handle = self.spawn_heartbeat(lock_paths.clone(), valid.clone());
        let store = self.store.clone();
        let instance_id = self.instance_id.clone();
        let valid_for_release = valid.clone();
        LockGuard::with_async_release_and_heartbeat(
            move || {
                Box::pin(async move {
                    if !valid_for_release.load(Ordering::Acquire) {
                        debug!("Lock ownership lost, skipping delete on release");
                        return;
                    }
                    let futs: Vec<_> = lock_paths
                        .iter()
                        .map(|path| {
                            let store = store.clone();
                            let path = path.clone();
                            let instance_id = instance_id.clone();
                            async move {
                                match store.read_with_etag(&path).await {
                                    Ok((data, _)) => {
                                        match serde_json::from_slice::<S3LockPayload>(&data) {
                                            Ok(payload) if payload.instance_id == instance_id => {
                                                if let Err(e) = store.delete(&path).await {
                                                    warn!(
                                                        path,
                                                        error = %e,
                                                        "Failed to delete lock on release"
                                                    );
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
                                        }
                                    }
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
                        })
                        .collect();
                    join_all(futs).await;
                })
            },
            valid,
            heartbeat_handle,
        )
    }

    async fn try_acquire_key(&self, lock_path: &str) -> Result<bool, Error> {
        let payload = self.make_payload();
        match self
            .store
            .put_object_if_not_exists(lock_path, payload)
            .await
        {
            Ok(()) => Ok(true),
            Err(data_store::Error::PreconditionFailed) => Ok(false),
            Err(e) => Err(Error::StorageBackend(e.to_string())),
        }
    }

    async fn try_recover_stale_lock(&self, lock_path: &str) -> RecoveryOutcome {
        let (data, etag, last_modified) = match self.store.read_with_metadata(lock_path).await {
            Ok(result) => result,
            Err(e) if e.kind() == ErrorKind::NotFound => return RecoveryOutcome::Retry,
            Err(e) => return RecoveryOutcome::Error(e.to_string()),
        };

        let payload: S3LockPayload = match serde_json::from_slice(&data) {
            Ok(p) => p,
            Err(e) => return RecoveryOutcome::Error(format!("corrupt lock payload: {e}")),
        };

        let last_modified = last_modified.unwrap_or(payload.refreshed_at);
        if !payload.is_expired(last_modified) {
            return RecoveryOutcome::NotStale;
        }

        debug!(
            lock_path,
            instance_id = payload.instance_id,
            refreshed_at = %payload.refreshed_at,
            "Recovering stale lock"
        );

        let Some(etag) = etag else {
            return RecoveryOutcome::Error("lock object missing ETag".to_string());
        };

        match self
            .store
            .put_object_if_match(lock_path, &etag, self.make_payload())
            .await
        {
            Ok(()) => RecoveryOutcome::Acquired,
            Err(data_store::Error::PreconditionFailed) => RecoveryOutcome::Failed,
            Err(e) => RecoveryOutcome::Error(e.to_string()),
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
    ) -> tokio::task::JoinHandle<()> {
        let store = self.store.clone();
        let instance_id = self.instance_id.clone();
        let ttl_secs = self.ttl_secs;
        let max_hold_secs = self.max_hold_secs;
        let interval = Duration::from_secs(ttl_secs / 3);

        tokio::spawn(async move {
            let started_at = tokio::time::Instant::now();
            let max_hold = Duration::from_secs(max_hold_secs);
            let mut interval_timer = tokio::time::interval(interval);
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
                    valid.store(false, Ordering::Release);
                    return;
                }

                let mut tick_had_failure = false;

                for path in &paths {
                    let etag = match store.read_with_etag(path).await {
                        Ok((data, etag)) => {
                            if let Ok(existing) = serde_json::from_slice::<S3LockPayload>(&data)
                                && existing.instance_id != instance_id
                            {
                                warn!(
                                    path,
                                    expected = instance_id,
                                    found = existing.instance_id,
                                    "Lock ownership lost, stopping heartbeat"
                                );
                                valid.store(false, Ordering::Release);
                                return;
                            }
                            etag
                        }
                        Err(e) if e.kind() == ErrorKind::NotFound => {
                            warn!(path, "Lock file disappeared, stopping heartbeat");
                            valid.store(false, Ordering::Release);
                            return;
                        }
                        Err(e) => {
                            warn!(path, error = %e, "Failed to read lock for heartbeat");
                            tick_had_failure = true;
                            continue;
                        }
                    };

                    let Some(etag) = etag else {
                        warn!(
                            path,
                            "Lock ETag unavailable, cannot safely refresh heartbeat, invalidating lock"
                        );
                        valid.store(false, Ordering::Release);
                        return;
                    };

                    let payload = S3LockPayload {
                        instance_id: instance_id.clone(),
                        refreshed_at: Utc::now(),
                        ttl_secs,
                    };
                    let data = serde_json::to_vec(&payload)
                        .expect("lock payload serialization cannot fail");

                    match store.put_object_if_match(path, &etag, data).await {
                        Ok(()) => {
                            debug!(path, "Refreshed S3 lock heartbeat");
                        }
                        Err(data_store::Error::PreconditionFailed) => {
                            warn!(
                                path,
                                "Lock ETag changed, ownership lost, stopping heartbeat"
                            );
                            valid.store(false, Ordering::Release);
                            return;
                        }
                        Err(e) => {
                            warn!(path, error = %e, "Failed to refresh S3 lock heartbeat");
                            tick_had_failure = true;
                        }
                    }
                }

                if tick_had_failure {
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    if u64::from(consecutive_failures) * (ttl_secs / 3) >= ttl_secs {
                        warn!(
                            consecutive_failures,
                            "Too many consecutive heartbeat tick failures, invalidating lock"
                        );
                        valid.store(false, Ordering::Release);
                        return;
                    }
                } else {
                    consecutive_failures = 0;
                }
            }
        })
    }
}

#[cfg_attr(test, derive(Debug))]
enum RecoveryOutcome {
    Acquired,
    NotStale,
    Retry,
    Failed,
    Error(String),
}

fn simple_jitter(max_ms: u64) -> u64 {
    use std::hash::{BuildHasher, Hasher};
    if max_ms == 0 {
        return 0;
    }
    std::collections::hash_map::RandomState::new()
        .build_hasher()
        .finish()
        % max_ms
}

#[cfg(test)]
impl S3LockBackend {
    async fn test_try_recover(&self, key: &str) -> RecoveryOutcome {
        self.try_recover_stale_lock(&Self::lock_path(key)).await
    }
}

#[async_trait]
impl LockBackend for S3LockBackend {
    async fn acquire(&self, keys: &[String]) -> Result<LockGuard, Error> {
        if keys.is_empty() {
            return Ok(LockGuard::with_async_release(|| Box::pin(async {})));
        }

        let mut lock_paths: Vec<String> = keys.iter().map(|k| Self::lock_path(k)).collect();
        lock_paths.sort();
        lock_paths.dedup();

        let mut retries = self.max_retries;

        loop {
            let results: Vec<(usize, Result<bool, Error>)> = {
                let futs: Vec<_> = lock_paths
                    .iter()
                    .enumerate()
                    .map(|(i, path)| {
                        let path = path.clone();
                        async move { (i, self.try_acquire_key(&path).await) }
                    })
                    .collect();
                // Parallel PUTs with overlapping key sets across instances can cause
                // repeated rollbacks (practical livelock). Sorted key order prevents
                // circular wait in sequential protocols but does not prevent collision
                // in a parallel-issue protocol. The retry budget (`max_retries`) bounds
                // total attempts. Randomized jitter (simple_jitter) desynchronises
                // retrying instances to break collision patterns.
                join_all(futs).await
            };

            let mut acquired_paths = Vec::new();
            let mut failed = false;
            let mut hard_error: Option<Error> = None;

            for (i, result) in &results {
                match result {
                    Ok(true) => acquired_paths.push(lock_paths[*i].clone()),
                    Ok(false) => failed = true,
                    Err(e) => {
                        hard_error = Some(Error::StorageBackend(format!("S3 lock error: {e}")));
                        failed = true;
                    }
                }
            }

            if !failed {
                return Ok(self.make_guard(lock_paths));
            }

            if let Some(e) = hard_error {
                self.release_paths(&acquired_paths).await;
                return Err(e);
            }

            let recovery_futs: Vec<_> = results
                .iter()
                .filter(|(_, result)| matches!(result, Ok(false)))
                .map(|(i, _)| {
                    let path = lock_paths[*i].clone();
                    async move { (i, self.try_recover_stale_lock(&path).await) }
                })
                .collect();

            let recovery_results = join_all(recovery_futs).await;

            let mut recovered_paths = Vec::new();
            for (i, outcome) in recovery_results {
                match outcome {
                    RecoveryOutcome::Acquired => {
                        recovered_paths.push(lock_paths[*i].clone());
                    }
                    RecoveryOutcome::Error(msg) => {
                        self.release_paths(&acquired_paths).await;
                        if !recovered_paths.is_empty() {
                            self.release_paths(&recovered_paths).await;
                        }
                        return Err(Error::StorageBackend(format!(
                            "S3 lock recovery error: {msg}"
                        )));
                    }
                    RecoveryOutcome::Failed
                    | RecoveryOutcome::NotStale
                    | RecoveryOutcome::Retry => {}
                }
            }

            if recovered_paths.len() + acquired_paths.len() == lock_paths.len() {
                return Ok(self.make_guard(lock_paths));
            }

            self.release_paths(&acquired_paths).await;
            if !recovered_paths.is_empty() {
                self.release_paths(&recovered_paths).await;
            }

            if retries == 0 {
                return Err(Error::Lock(format!(
                    "Failed to acquire S3 locks after {} attempts for keys: {:?}",
                    self.max_retries, keys
                )));
            }

            retries -= 1;
            let attempt = self.max_retries - retries;
            debug!(retries_left = retries, "S3 lock busy, retrying...");
            tokio::time::sleep(self.jittered_delay(attempt)).await;
        }
    }
}
