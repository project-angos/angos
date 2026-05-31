//! Concrete lock primitive backed by [`LockStorage`].
//!
//! [`Lock`] is the single concrete lock type for the transaction engine.
//! Callers acquire it via [`Lock::acquire`] (blocking retry) or
//! [`Lock::try_acquire`] (non-blocking, single attempt), both of which return
//! a [`LockSession`]; the session is released with [`LockSession::release`].
//!
//! A background heartbeat refreshes the TTL at `ttl_secs/3` intervals.
//! When the heartbeat detects ownership loss (`ETag` mismatch on refresh) or
//! exhausts its retry budget, it fires the session's [`CancellationToken`], so
//! a caller racing its operation against [`LockSession::cancellation`] can
//! observe the loss at its next await point.
//!
//! ## Stale-lock recovery
//!
//! When `put_if_absent` returns `AlreadyExists`, [`Lock`] reads the current
//! lock body and checks whether it is expired (using the server-assigned
//! `last_modified` timestamp when available, falling back to the embedded
//! `refreshed_at` field). If expired, it attempts a conditional replace via
//! `put_if_match` using the current `ETag`. This mirrors the recovery logic
//! of the previous `S3LockBackend`.

use std::{
    collections::HashMap,
    fmt::Debug,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use chrono::Utc;
use tokio::{
    spawn,
    task::JoinHandle,
    time::{Instant as TokioInstant, MissedTickBehavior, interval, sleep, timeout},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::lock::{
    Error, LockSession,
    metrics::{elapsed_ms, lock_metrics},
    simple_jitter,
    storage::{DeleteIfMatchOutcome, LockBody, LockStorage, PutIfAbsentOutcome, PutIfMatchOutcome},
};

// ─── constants ───────────────────────────────────────────────────────────────

const MAX_LOCK_TTL_SECS: u64 = 3600;

// ─── Lock ────────────────────────────────────────────────────────────────────

/// Concrete distributed lock backed by a [`LockStorage`].
///
/// Constructed via [`Lock::builder`]. Safe to clone; all clones share the same
/// configuration and storage handle.
#[derive(Clone)]
pub struct Lock {
    storage: Arc<dyn LockStorage>,
    ttl_secs: u64,
    max_hold_secs: u64,
    max_retries: u32,
    retry_delay_ms: u64,
}

impl Debug for Lock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Lock")
            .field("ttl_secs", &self.ttl_secs)
            .field("storage_label", &self.storage.label())
            .finish_non_exhaustive()
    }
}

// ─── Builder ─────────────────────────────────────────────────────────────────

/// Builder for [`Lock`].
#[derive(Default)]
pub struct LockBuilder {
    storage: Option<Arc<dyn LockStorage>>,
    ttl_secs: Option<u64>,
    max_hold_secs: Option<u64>,
    max_retries: Option<u32>,
    retry_delay_ms: Option<u64>,
}

impl LockBuilder {
    /// Set the lock-object storage backend (required).
    #[must_use]
    pub fn storage(mut self, storage: Arc<dyn LockStorage>) -> Self {
        self.storage = Some(storage);
        self
    }

    /// Set the lock TTL in seconds. Must be ≥ 9 (heartbeat fires at `ttl/3`).
    /// Defaults to 30.
    #[must_use]
    pub fn ttl_secs(mut self, secs: u64) -> Self {
        self.ttl_secs = Some(secs);
        self
    }

    /// Maximum time a single lock session may be held (includes acquire time).
    /// Defaults to 300.
    #[must_use]
    pub fn max_hold_secs(mut self, secs: u64) -> Self {
        self.max_hold_secs = Some(secs);
        self
    }

    /// Maximum retries on contention before `acquire` fails. Defaults to 100.
    #[must_use]
    pub fn max_retries(mut self, retries: u32) -> Self {
        self.max_retries = Some(retries);
        self
    }

    /// Base retry delay in milliseconds (jittered exponential backoff).
    /// Defaults to 50.
    #[must_use]
    pub fn retry_delay_ms(mut self, ms: u64) -> Self {
        self.retry_delay_ms = Some(ms);
        self
    }

    /// Consume the builder and produce a [`Lock`].
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidData`] if no storage was provided, or if TTL /
    /// hold-time constraints are violated.
    pub fn build(self) -> Result<Lock, Error> {
        let storage = self
            .storage
            .ok_or_else(|| Error::InvalidData("Lock requires a storage backend".to_string()))?;
        let ttl_secs = self.ttl_secs.unwrap_or(30);
        let max_hold_secs = self.max_hold_secs.unwrap_or(300);
        let max_retries = self.max_retries.unwrap_or(100);
        let retry_delay_ms = self.retry_delay_ms.unwrap_or(50);

        if ttl_secs < 9 {
            return Err(Error::InvalidData("ttl_secs must be at least 9".into()));
        }
        if ttl_secs > MAX_LOCK_TTL_SECS {
            return Err(Error::InvalidData(format!(
                "ttl_secs must be at most {MAX_LOCK_TTL_SECS}"
            )));
        }
        if max_hold_secs < ttl_secs {
            return Err(Error::InvalidData(
                "max_hold_secs must be >= ttl_secs".into(),
            ));
        }
        if retry_delay_ms < 1 {
            return Err(Error::InvalidData(
                "retry_delay_ms must be at least 1".into(),
            ));
        }

        Ok(Lock {
            storage,
            ttl_secs,
            max_hold_secs,
            max_retries,
            retry_delay_ms,
        })
    }
}

// ─── Lock implementation ──────────────────────────────────────────────────────

impl Lock {
    /// Return a builder for constructing a `Lock`.
    #[must_use]
    pub fn builder() -> LockBuilder {
        LockBuilder::default()
    }

    /// The label of the underlying [`LockStorage`]. Used in startup log lines
    /// so operators can verify which storage flavour is active.
    #[must_use]
    pub fn storage_label(&self) -> &'static str {
        self.storage.label()
    }

    fn make_body(&self) -> Result<Vec<u8>, Error> {
        let body = LockBody {
            refreshed_at: Utc::now(),
            ttl_secs: self.ttl_secs,
        };
        serde_json::to_vec(&body)
            .map_err(|e| Error::InvalidData(format!("lock body serialization failed: {e}")))
    }

    fn jittered_delay(&self, attempt: u32) -> Duration {
        let max_delay_ms: u64 = 1_000;
        let base_ms = self.retry_delay_ms.saturating_mul(1u64 << attempt.min(6));
        let capped_ms = base_ms.min(max_delay_ms);
        let jitter = simple_jitter(capped_ms / 2);
        Duration::from_millis(capped_ms.saturating_add(jitter))
    }

    /// Try once to acquire a single key. Returns `Ok(Some(etag))` on success,
    /// `Ok(None)` on contention, or `Err` on a hard storage error.
    async fn try_acquire_one(&self, key: &str) -> Result<Option<Option<String>>, Error> {
        let body = self.make_body()?;
        match self.storage.put_if_absent(key, body).await? {
            PutIfAbsentOutcome::Created(etag) => Ok(Some(etag)),
            PutIfAbsentOutcome::AlreadyExists => Ok(None),
        }
    }

    /// Attempt to recover a stale lock at `key`. Returns `Ok(Some(etag))` when
    /// the stale lock was claimed, `Ok(None)` when the lock is still fresh or
    /// the race was lost, `Err` on a hard error.
    async fn try_recover_stale(&self, key: &str) -> Result<Option<Option<String>>, Error> {
        let (data, etag, last_modified) = match self.storage.get_with_etag(key).await {
            Ok(t) => t,
            Err(Error::NotFound) => return Ok(None),
            Err(e) => return Err(e),
        };

        let body: LockBody = serde_json::from_slice(&data)
            .map_err(|e| Error::InvalidData(format!("corrupt lock body: {e}")))?;

        if !body.is_expired(last_modified) {
            return Ok(None);
        }

        let Some(etag) = etag else {
            return Err(Error::InvalidData(
                "lock object missing ETag; cannot recover stale lock".to_string(),
            ));
        };

        debug!(
            key,
            refreshed_at = %body.refreshed_at,
            "Lock: recovering stale lock"
        );

        let new_body = self.make_body()?;
        match self.storage.put_if_match(key, &etag, new_body).await? {
            PutIfMatchOutcome::Updated(new_etag) => {
                lock_metrics().record_recovery(self.storage.label(), "success");
                Ok(Some(new_etag))
            }
            PutIfMatchOutcome::Mismatch => {
                lock_metrics().record_recovery(self.storage.label(), "lost_race");
                Ok(None)
            }
        }
    }

    /// Acquire all `keys`, retrying on contention up to `max_retries` times.
    ///
    /// Keys are sorted and de-duplicated before acquisition (deadlock-free).
    /// On each failed attempt any already-acquired paths are released before
    /// sleeping.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Lock`] after exhausting the retry budget, or
    /// [`Error::StorageBackend`] on a hard storage error.
    pub async fn acquire(&self, keys: &[String]) -> Result<LockSession, Error> {
        if keys.is_empty() {
            return Ok(LockSession::sync(Box::new(())));
        }

        let mut sorted: Vec<String> = keys.to_vec();
        sorted.sort();
        sorted.dedup();

        let label = self.storage.label();
        let mut retries = self.max_retries;
        let start = Instant::now();

        loop {
            match self.try_acquire_all_sequential(&sorted).await {
                AcquireAllOutcome::Acquired(etags) => {
                    let metrics = lock_metrics();
                    metrics.observe_acquisition_duration(label, elapsed_ms(start));
                    metrics.record_acquisition(label, "success");
                    return Ok(self.make_session(sorted, etags));
                }
                AcquireAllOutcome::HardError(e) => {
                    lock_metrics().observe_acquisition_duration(label, elapsed_ms(start));
                    lock_metrics().record_acquisition(label, "error");
                    return Err(e);
                }
                AcquireAllOutcome::Retry { acquired } => {
                    self.release_paths(&acquired).await;
                    if retries == 0 {
                        lock_metrics().observe_acquisition_duration(label, elapsed_ms(start));
                        lock_metrics().record_acquisition(label, "timeout");
                        return Err(Error::Lock(format!(
                            "Failed to acquire lock after {} attempts for keys: {keys:?}",
                            self.max_retries
                        )));
                    }
                    retries -= 1;
                    let attempt = self.max_retries - retries;
                    debug!(retries_left = retries, "Lock busy, retrying");
                    lock_metrics().record_retry(label);
                    sleep(self.jittered_delay(attempt)).await;
                }
            }
        }
    }

    /// Non-blocking acquire: single attempt only. Returns `Ok(None)` when any
    /// key is contended.
    ///
    /// # Errors
    ///
    /// Returns [`Error::StorageBackend`] on a hard storage error.
    pub async fn try_acquire(&self, keys: &[String]) -> Result<Option<LockSession>, Error> {
        if keys.is_empty() {
            return Ok(Some(LockSession::sync(Box::new(()))));
        }

        let mut sorted: Vec<String> = keys.to_vec();
        sorted.sort();
        sorted.dedup();

        let label = self.storage.label();
        let start = Instant::now();

        match self.try_acquire_all_sequential(&sorted).await {
            AcquireAllOutcome::Acquired(etags) => {
                let metrics = lock_metrics();
                metrics.observe_acquisition_duration(label, elapsed_ms(start));
                metrics.record_acquisition(label, "success");
                Ok(Some(self.make_session(sorted, etags)))
            }
            AcquireAllOutcome::HardError(e) => {
                lock_metrics().observe_acquisition_duration(label, elapsed_ms(start));
                lock_metrics().record_acquisition(label, "error");
                Err(e)
            }
            AcquireAllOutcome::Retry { acquired } => {
                self.release_paths(&acquired).await;
                lock_metrics().record_acquisition(label, "timeout");
                Ok(None)
            }
        }
    }

    // ─── internal helpers ─────────────────────────────────────────────────

    async fn try_acquire_all_sequential(&self, sorted_keys: &[String]) -> AcquireAllOutcome {
        let mut acquired_paths: Vec<String> = Vec::new();
        let mut etags: HashMap<String, Option<String>> = HashMap::new();

        for key in sorted_keys {
            match self.try_acquire_one(key).await {
                Ok(Some(etag)) => {
                    etags.insert(key.clone(), etag);
                    acquired_paths.push(key.clone());
                }
                Ok(None) => {
                    // Contended. Try stale-lock recovery.
                    match self.try_recover_stale(key).await {
                        Ok(Some(new_etag)) => {
                            etags.insert(key.clone(), new_etag);
                            acquired_paths.push(key.clone());
                        }
                        Ok(None) => {
                            // Still held or lost the recovery race.
                            return AcquireAllOutcome::Retry {
                                acquired: acquired_paths,
                            };
                        }
                        Err(e) => {
                            return AcquireAllOutcome::HardError(e);
                        }
                    }
                }
                Err(e) => {
                    self.release_paths(&acquired_paths).await;
                    return AcquireAllOutcome::HardError(e);
                }
            }
        }

        AcquireAllOutcome::Acquired(etags)
    }

    async fn release_paths(&self, paths: &[String]) {
        for path in paths {
            if let Err(e) = self.storage.delete(path).await {
                warn!(path, error = %e, "Lock: failed to delete lock path during rollback");
            }
        }
    }

    fn make_session(
        &self,
        paths: Vec<String>,
        initial_etags: HashMap<String, Option<String>>,
    ) -> LockSession {
        let cancellation = CancellationToken::new();
        let etag_cache = Arc::new(RwLock::new(initial_etags));
        let heartbeat_handle =
            self.spawn_heartbeat(paths.clone(), cancellation.clone(), etag_cache.clone());
        let storage = self.storage.clone();
        let cancellation_for_release = cancellation.clone();

        LockSession::with_async_release_and_heartbeat(
            move || {
                Box::pin(release_session(
                    cancellation_for_release,
                    paths,
                    etag_cache,
                    storage,
                ))
            },
            cancellation,
            heartbeat_handle,
        )
    }

    fn spawn_heartbeat(
        &self,
        paths: Vec<String>,
        cancellation: CancellationToken,
        etag_cache: Arc<RwLock<HashMap<String, Option<String>>>>,
    ) -> JoinHandle<()> {
        let storage = self.storage.clone();
        let ttl_secs = self.ttl_secs;
        let max_hold_secs = self.max_hold_secs;
        let label = storage.label();
        let tick_interval = Duration::from_secs(ttl_secs / 3);

        spawn(async move {
            let started_at = TokioInstant::now();
            let max_hold = Duration::from_secs(max_hold_secs);
            let mut timer = interval(tick_interval);
            timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
            timer.tick().await; // consume immediate first tick

            let mut consecutive_failures: u32 = 0;

            loop {
                timer.tick().await;

                if started_at.elapsed() >= max_hold {
                    warn!(
                        max_hold_secs,
                        "Lock: held beyond maximum duration, invalidating"
                    );
                    lock_metrics().record_invalidation(label, "max_hold_exceeded");
                    cancellation.cancel();
                    return;
                }

                match run_heartbeat_tick(
                    &paths,
                    storage.as_ref(),
                    ttl_secs,
                    tick_interval,
                    &etag_cache,
                    &mut consecutive_failures,
                    label,
                )
                .await
                {
                    HeartbeatOutcome::Continue => {}
                    HeartbeatOutcome::Invalidate(reason) => {
                        lock_metrics().record_invalidation(label, reason);
                        cancellation.cancel();
                        return;
                    }
                }
            }
        })
    }
}

// ─── Heartbeat ───────────────────────────────────────────────────────────────

enum HeartbeatOutcome {
    Continue,
    Invalidate(&'static str),
}

// reason: all 7 parameters are the narrowly-scoped heartbeat state that callers
// provide individually; bundling them into a struct would obscure ownership and
// complicate borrowing of `consecutive_failures` as `&mut`.
#[allow(clippy::too_many_arguments)]
async fn run_heartbeat_tick(
    paths: &[String],
    storage: &dyn LockStorage,
    ttl_secs: u64,
    tick_deadline: Duration,
    etag_cache: &Arc<RwLock<HashMap<String, Option<String>>>>,
    consecutive_failures: &mut u32,
    _label: &'static str,
) -> HeartbeatOutcome {
    let mut had_failure = false;

    for path in paths {
        let cached_etag = etag_cache
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(path)
            .and_then(Option::as_ref)
            .cloned();

        let outcome = timeout(
            tick_deadline,
            heartbeat_tick_path(storage, path, ttl_secs, cached_etag),
        )
        .await;

        let path_outcome = match outcome {
            Err(_) => {
                warn!(path, "Lock heartbeat: tick timed out");
                PathTickOutcome::Failure
            }
            Ok(p) => p,
        };

        match path_outcome {
            PathTickOutcome::Updated(new_etag) => {
                let mut cache = etag_cache
                    .write()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                cache.insert(path.clone(), new_etag);
            }
            PathTickOutcome::Invalidate(reason) => {
                return HeartbeatOutcome::Invalidate(reason);
            }
            PathTickOutcome::Failure => {
                etag_cache
                    .write()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(path.clone(), None);
                had_failure = true;
            }
        }
    }

    if had_failure {
        *consecutive_failures = consecutive_failures.saturating_add(1);
        let ticks_per_ttl = ttl_secs / (ttl_secs / 3).max(1);
        if u64::from(*consecutive_failures) >= ticks_per_ttl {
            warn!(
                consecutive_failures,
                "Lock: too many consecutive heartbeat failures, invalidating"
            );
            return HeartbeatOutcome::Invalidate("heartbeat_failure");
        }
    } else {
        *consecutive_failures = 0;
    }

    HeartbeatOutcome::Continue
}

enum PathTickOutcome {
    Updated(Option<String>),
    Invalidate(&'static str),
    Failure,
}

async fn heartbeat_tick_path(
    storage: &dyn LockStorage,
    path: &str,
    ttl_secs: u64,
    cached_etag: Option<String>,
) -> PathTickOutcome {
    let make_body = || -> Result<Vec<u8>, String> {
        let body = LockBody {
            refreshed_at: Utc::now(),
            ttl_secs,
        };
        serde_json::to_vec(&body).map_err(|e| e.to_string())
    };

    if let Some(etag) = cached_etag {
        let body = match make_body() {
            Ok(b) => b,
            Err(e) => {
                warn!(path, error = %e, "Lock heartbeat: serialization failed");
                return PathTickOutcome::Failure;
            }
        };
        match storage.put_if_match(path, &etag, body).await {
            Ok(PutIfMatchOutcome::Updated(new_etag)) => {
                debug!(path, "Lock: heartbeat refreshed");
                return PathTickOutcome::Updated(new_etag);
            }
            Ok(PutIfMatchOutcome::Mismatch) => {
                warn!(path, "Lock: ETag changed, ownership lost");
                return PathTickOutcome::Invalidate("ownership_lost");
            }
            Err(e) => {
                warn!(path, error = %e, "Lock heartbeat: put_if_match failed");
                return PathTickOutcome::Failure;
            }
        }
    }

    // No cached ETag — re-read to recover the ETag and refresh via put_if_match.
    // Ownership loss is detected authoritatively by the put_if_match below: a
    // Mismatch means another holder replaced the object.
    let (_data, etag, _) = match storage.get_with_etag(path).await {
        Ok(t) => t,
        Err(Error::NotFound) => {
            warn!(path, "Lock: lock file disappeared");
            return PathTickOutcome::Invalidate("file_disappeared");
        }
        Err(e) => {
            warn!(path, error = %e, "Lock heartbeat: get_with_etag failed");
            return PathTickOutcome::Failure;
        }
    };

    let Some(etag) = etag else {
        warn!(
            path,
            "Lock: ETag unavailable, cannot safely refresh heartbeat"
        );
        return PathTickOutcome::Invalidate("etag_unavailable");
    };

    let body = match make_body() {
        Ok(b) => b,
        Err(e) => {
            warn!(path, error = %e, "Lock heartbeat: serialization failed");
            return PathTickOutcome::Failure;
        }
    };

    match storage.put_if_match(path, &etag, body).await {
        Ok(PutIfMatchOutcome::Updated(new_etag)) => {
            debug!(path, "Lock: heartbeat refreshed (slow path)");
            PathTickOutcome::Updated(new_etag)
        }
        Ok(PutIfMatchOutcome::Mismatch) => {
            warn!(path, "Lock: ETag changed mid-heartbeat, ownership lost");
            PathTickOutcome::Invalidate("ownership_lost")
        }
        Err(e) => {
            warn!(path, error = %e, "Lock heartbeat: put_if_match failed (slow path)");
            PathTickOutcome::Failure
        }
    }
}

// ─── Release ─────────────────────────────────────────────────────────────────

async fn release_session(
    cancellation: CancellationToken,
    paths: Vec<String>,
    etag_cache: Arc<RwLock<HashMap<String, Option<String>>>>,
    storage: Arc<dyn LockStorage>,
) {
    if cancellation.is_cancelled() {
        debug!("Lock: ownership already lost, skipping release");
        return;
    }

    for path in &paths {
        let cached_etag = etag_cache
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(path)
            .and_then(Option::as_ref)
            .cloned();

        release_single_path(storage.as_ref(), path, cached_etag.as_ref()).await;
    }
}

async fn release_single_path(storage: &dyn LockStorage, path: &str, cached_etag: Option<&String>) {
    // Fast path: conditional delete using cached ETag.
    if let Some(etag) = cached_etag {
        match storage.delete_if_match(path, etag).await {
            Ok(DeleteIfMatchOutcome::Deleted) => return,
            Ok(DeleteIfMatchOutcome::Mismatch) => {
                debug!(
                    path,
                    "Lock: ETag changed on release; another instance owns it"
                );
                return;
            }
            Err(e) => {
                warn!(path, error = %e, "Lock: conditional delete failed, attempting plain delete");
            }
        }
    }

    // Slow path: verify ownership then delete.
    match storage.get_with_etag(path).await {
        Ok((data, etag, _)) => {
            if let Ok(body) = serde_json::from_slice::<LockBody>(&data) {
                // Only delete if we still own it.
                if let Some(ref e) = etag {
                    match storage.delete_if_match(path, e).await {
                        Ok(_) => {}
                        Err(err) => {
                            warn!(path, error = %err, "Lock: delete_if_match failed on release slow path");
                        }
                    }
                } else if let Err(err) = storage.delete(path).await {
                    warn!(path, error = %err, "Lock: delete failed on release slow path");
                }
                let _ = body; // ownership confirmed via the GET
            } else {
                warn!(path, "Lock: corrupt lock body on release, deleting anyway");
                if let Err(err) = storage.delete(path).await {
                    warn!(path, error = %err, "Lock: delete failed after corrupt read");
                }
            }
        }
        Err(Error::NotFound) => {
            debug!(path, "Lock: already deleted");
        }
        Err(e) => {
            warn!(path, error = %e, "Lock: get_with_etag failed on release, lock will expire via TTL");
        }
    }
}

// ─── AcquireAllOutcome ───────────────────────────────────────────────────────

enum AcquireAllOutcome {
    Acquired(HashMap<String, Option<String>>),
    HardError(Error),
    Retry { acquired: Vec<String> },
}
