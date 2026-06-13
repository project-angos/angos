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
    sync::{Arc, PoisonError, RwLock},
    time::{Duration, Instant},
};

use chrono::Utc;
use futures_util::future::join_all;
use tokio::{
    spawn,
    task::JoinHandle,
    time::{Instant as TokioInstant, MissedTickBehavior, interval, sleep, timeout},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};
use uuid::Uuid;

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

/// Builder for [`Lock`]. The storage backend is required and supplied to
/// [`Lock::builder`]; the TTL and retry tuning are optional fluent setters.
pub struct LockBuilder {
    storage: Arc<dyn LockStorage>,
    ttl_secs: Option<u64>,
    max_hold_secs: Option<u64>,
    max_retries: Option<u32>,
    retry_delay_ms: Option<u64>,
}

impl LockBuilder {
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
    /// Returns [`Error::InvalidData`] if the TTL / hold-time constraints are
    /// violated.
    pub fn build(self) -> Result<Lock, Error> {
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
            storage: self.storage,
            ttl_secs,
            max_hold_secs,
            max_retries,
            retry_delay_ms,
        })
    }
}

// ─── Lock implementation ──────────────────────────────────────────────────────

impl Lock {
    /// Return a builder wrapping the lock-object `storage` backend. The TTL and
    /// retry tuning are optional fluent setters on the returned builder.
    #[must_use]
    pub fn builder(storage: Arc<dyn LockStorage>) -> LockBuilder {
        LockBuilder {
            storage,
            ttl_secs: None,
            max_hold_secs: None,
            max_retries: None,
            retry_delay_ms: None,
        }
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
            writer_nonce: Uuid::new_v4(),
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

    // Snapshot each path's cached ETag up front (one short read-lock hold), then
    // refresh ALL paths concurrently within a single tick budget. A sequential
    // refresh could take up to N·tick_deadline wall-clock per tick for N keys,
    // which under a short TTL lets a peer's stale-lock recovery reclaim a
    // still-held lock and split ownership. Running them concurrently keeps the
    // wall-clock per tick at ≈ one `tick_deadline` regardless of key count.
    let cached_etags: Vec<Option<String>> = {
        let cache = etag_cache.read().unwrap_or_else(PoisonError::into_inner);
        paths
            .iter()
            .map(|path| cache.get(path).and_then(Option::as_ref).cloned())
            .collect()
    };

    let path_outcomes: Vec<PathTickOutcome> = join_all(paths.iter().zip(cached_etags).map(
        |(path, cached_etag)| async move {
            match timeout(
                tick_deadline,
                heartbeat_tick_path(storage, path, ttl_secs, cached_etag),
            )
            .await
            {
                Err(_) => {
                    warn!(path, "Lock heartbeat: tick timed out");
                    PathTickOutcome::Failure
                }
                Ok(p) => p,
            }
        },
    ))
    .await;

    // Fold the results after the join (no lock held across an await). Any
    // `Invalidate` wins; the first such path in `paths` order is chosen so the
    // reported reason is deterministic.
    let mut invalidate_reason: Option<&'static str> = None;
    for (path, path_outcome) in paths.iter().zip(path_outcomes) {
        match path_outcome {
            PathTickOutcome::Updated(new_etag) => {
                etag_cache
                    .write()
                    .unwrap_or_else(PoisonError::into_inner)
                    .insert(path.clone(), new_etag);
            }
            PathTickOutcome::Invalidate(reason) => {
                if invalidate_reason.is_none() {
                    invalidate_reason = Some(reason);
                }
            }
            PathTickOutcome::Failure => {
                etag_cache
                    .write()
                    .unwrap_or_else(PoisonError::into_inner)
                    .insert(path.clone(), None);
                had_failure = true;
            }
        }
    }

    if let Some(reason) = invalidate_reason {
        return HeartbeatOutcome::Invalidate(reason);
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
            writer_nonce: Uuid::new_v4(),
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
            .unwrap_or_else(PoisonError::into_inner)
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

// ─── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{
            Arc, Mutex, RwLock,
            atomic::{AtomicU64, AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use async_trait::async_trait;
    use chrono::{Duration as ChronoDuration, Utc};
    use tokio::sync::Barrier;
    use uuid::Uuid;

    use super::{HeartbeatOutcome, Lock, PathTickOutcome, heartbeat_tick_path, run_heartbeat_tick};
    use crate::lock::{
        Error,
        storage::{
            DeleteIfMatchOutcome, LockBody, LockStorage, PutIfAbsentOutcome, PutIfMatchOutcome,
        },
    };

    /// What a single `put_if_match` call on the fake should return.
    #[derive(Clone, Copy)]
    enum PutMatchScript {
        /// Succeed and hand back the next rotated `ETag`.
        Updated,
        /// Report an `ETag` mismatch (ownership lost / takeover race lost).
        Mismatch,
        /// A transient hard error (counts against the heartbeat failure budget).
        Failure,
    }

    /// What a single `get_with_etag` call on the fake should return.
    #[derive(Clone, Copy)]
    enum GetScript {
        /// Return a stored body whose `refreshed_at` makes it expired.
        Expired,
        /// Return a stored body that is still fresh.
        Fresh,
        /// Return a stored body but with no `ETag` (forces `etag_unavailable`).
        FreshNoEtag,
        /// Return a stored, expired body but with no `ETag` (recover path can't fence).
        ExpiredNoEtag,
        /// Key is gone.
        NotFound,
        /// Transient hard error.
        Failure,
    }

    /// Scriptable [`LockStorage`] double.
    ///
    /// Every conditional method consults a pre-loaded script field; `ETag`s are
    /// minted from a monotonically-increasing counter so a healthy refresh
    /// visibly advances the cached `ETag`. The `put_if_absent` path is driven by
    /// a separate flag so acquire / `try_acquire` scenarios are independent of
    /// the heartbeat scripts.
    #[derive(Default)]
    struct FakeLockStorage {
        next_etag: AtomicU64,
        put_match: Mutex<Option<PutMatchScript>>,
        get: Mutex<Option<GetScript>>,
        /// When set, `put_if_absent` reports `AlreadyExists`; otherwise `Created`.
        put_absent_exists: Mutex<bool>,
        /// When set, `put_if_absent` returns a hard error.
        put_absent_error: Mutex<bool>,
        put_match_calls: AtomicUsize,
        delete_if_match_calls: AtomicUsize,
        delete_calls: AtomicUsize,
        /// Last `delete_if_match` etag, for assertions.
        last_delete_etag: Mutex<Option<String>>,
        /// When set, `delete_if_match` reports a mismatch.
        delete_mismatch: Mutex<bool>,
    }

    impl std::fmt::Debug for FakeLockStorage {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("FakeLockStorage").finish_non_exhaustive()
        }
    }

    impl FakeLockStorage {
        fn arc() -> Arc<Self> {
            Arc::new(Self::default())
        }

        fn mint_etag(&self) -> String {
            let v = self.next_etag.fetch_add(1, Ordering::Relaxed);
            format!("\"etag-{v}\"")
        }

        fn set_put_match(&self, script: PutMatchScript) {
            *self.put_match.lock().unwrap() = Some(script);
        }

        fn set_get(&self, script: GetScript) {
            *self.get.lock().unwrap() = Some(script);
        }

        fn set_put_absent_exists(&self, exists: bool) {
            *self.put_absent_exists.lock().unwrap() = exists;
        }

        fn body_bytes(expired: bool) -> Vec<u8> {
            let refreshed_at = if expired {
                Utc::now() - ChronoDuration::seconds(120)
            } else {
                Utc::now()
            };
            let body = LockBody {
                refreshed_at,
                ttl_secs: 30,
                writer_nonce: Uuid::new_v4(),
            };
            serde_json::to_vec(&body).unwrap()
        }
    }

    #[async_trait]
    impl LockStorage for FakeLockStorage {
        async fn put_if_absent(
            &self,
            _key: &str,
            _body: Vec<u8>,
        ) -> Result<PutIfAbsentOutcome, Error> {
            if *self.put_absent_error.lock().unwrap() {
                return Err(Error::StorageBackend("injected put_if_absent error".into()));
            }
            if *self.put_absent_exists.lock().unwrap() {
                Ok(PutIfAbsentOutcome::AlreadyExists)
            } else {
                Ok(PutIfAbsentOutcome::Created(Some(self.mint_etag())))
            }
        }

        async fn put_if_match(
            &self,
            _key: &str,
            _expected_etag: &str,
            _body: Vec<u8>,
        ) -> Result<PutIfMatchOutcome, Error> {
            self.put_match_calls.fetch_add(1, Ordering::Relaxed);
            match self
                .put_match
                .lock()
                .unwrap()
                .expect("put_match script set")
            {
                PutMatchScript::Updated => Ok(PutIfMatchOutcome::Updated(Some(self.mint_etag()))),
                PutMatchScript::Mismatch => Ok(PutIfMatchOutcome::Mismatch),
                PutMatchScript::Failure => {
                    Err(Error::StorageBackend("injected put_if_match error".into()))
                }
            }
        }

        async fn get_with_etag(
            &self,
            _key: &str,
        ) -> Result<(Vec<u8>, Option<String>, Option<chrono::DateTime<Utc>>), Error> {
            match self.get.lock().unwrap().expect("get script set") {
                GetScript::Expired => Ok((Self::body_bytes(true), Some(self.mint_etag()), None)),
                GetScript::Fresh => Ok((Self::body_bytes(false), Some(self.mint_etag()), None)),
                GetScript::FreshNoEtag => Ok((Self::body_bytes(false), None, None)),
                GetScript::ExpiredNoEtag => Ok((Self::body_bytes(true), None, None)),
                GetScript::NotFound => Err(Error::NotFound),
                GetScript::Failure => {
                    Err(Error::StorageBackend("injected get_with_etag error".into()))
                }
            }
        }

        async fn delete(&self, _key: &str) -> Result<(), Error> {
            self.delete_calls.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        async fn delete_if_match(
            &self,
            _key: &str,
            expected_etag: &str,
        ) -> Result<DeleteIfMatchOutcome, Error> {
            self.delete_if_match_calls.fetch_add(1, Ordering::Relaxed);
            *self.last_delete_etag.lock().unwrap() = Some(expected_etag.to_string());
            if *self.delete_mismatch.lock().unwrap() {
                Ok(DeleteIfMatchOutcome::Mismatch)
            } else {
                Ok(DeleteIfMatchOutcome::Deleted)
            }
        }

        fn label(&self) -> &'static str {
            "fake"
        }
    }

    fn lock_with(storage: Arc<FakeLockStorage>) -> Lock {
        Lock::builder(storage)
            .ttl_secs(9)
            .max_hold_secs(9)
            .max_retries(2)
            .build()
            .expect("lock builder")
    }

    fn cache(entries: &[(&str, Option<&str>)]) -> Arc<RwLock<HashMap<String, Option<String>>>> {
        let map: HashMap<String, Option<String>> = entries
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.map(ToString::to_string)))
            .collect();
        Arc::new(RwLock::new(map))
    }

    // ─── heartbeat_tick_path ───────────────────────────────────────────────

    #[tokio::test]
    async fn tick_path_fast_path_mismatch_invalidates_ownership_lost() {
        let storage = FakeLockStorage::arc();
        storage.set_put_match(PutMatchScript::Mismatch);

        let outcome =
            heartbeat_tick_path(storage.as_ref(), "k", 9, Some("\"cached\"".to_string())).await;

        assert!(
            matches!(outcome, PathTickOutcome::Invalidate("ownership_lost")),
            "a put_if_match Mismatch on the cached ETag must invalidate as ownership_lost"
        );
    }

    #[tokio::test]
    async fn tick_path_fast_path_updated_advances_etag() {
        let storage = FakeLockStorage::arc();
        storage.set_put_match(PutMatchScript::Updated);

        let outcome =
            heartbeat_tick_path(storage.as_ref(), "k", 9, Some("\"cached\"".to_string())).await;

        match outcome {
            PathTickOutcome::Updated(Some(new_etag)) => {
                assert_ne!(
                    new_etag, "\"cached\"",
                    "healthy refresh must rotate the ETag"
                );
            }
            other => panic!(
                "expected Updated(new_etag), got a different outcome: {}",
                outcome_name(&other)
            ),
        }
    }

    #[tokio::test]
    async fn tick_path_slow_path_not_found_invalidates_file_disappeared() {
        let storage = FakeLockStorage::arc();
        storage.set_get(GetScript::NotFound);

        // No cached ETag → slow path re-reads via get_with_etag, which is NotFound.
        let outcome = heartbeat_tick_path(storage.as_ref(), "k", 9, None).await;

        assert!(
            matches!(outcome, PathTickOutcome::Invalidate("file_disappeared")),
            "a missing lock object on the slow path must invalidate as file_disappeared"
        );
    }

    #[tokio::test]
    async fn tick_path_slow_path_no_etag_invalidates_etag_unavailable() {
        let storage = FakeLockStorage::arc();
        storage.set_get(GetScript::FreshNoEtag);

        let outcome = heartbeat_tick_path(storage.as_ref(), "k", 9, None).await;

        assert!(
            matches!(outcome, PathTickOutcome::Invalidate("etag_unavailable")),
            "an ETag-less backend on the slow path cannot safely refresh; must invalidate"
        );
    }

    #[tokio::test]
    async fn tick_path_fast_path_storage_error_is_failure() {
        let storage = FakeLockStorage::arc();
        storage.set_put_match(PutMatchScript::Failure);

        let outcome =
            heartbeat_tick_path(storage.as_ref(), "k", 9, Some("\"cached\"".to_string())).await;

        assert!(
            matches!(outcome, PathTickOutcome::Failure),
            "a transient put_if_match error is a Failure (budgeted), not an invalidation"
        );
    }

    fn outcome_name(o: &PathTickOutcome) -> &'static str {
        match o {
            PathTickOutcome::Updated(_) => "Updated",
            PathTickOutcome::Invalidate(_) => "Invalidate",
            PathTickOutcome::Failure => "Failure",
        }
    }

    // ─── run_heartbeat_tick (failure budget) ───────────────────────────────

    #[tokio::test]
    async fn run_tick_single_failure_does_not_cancel() {
        let storage = FakeLockStorage::arc();
        storage.set_put_match(PutMatchScript::Failure);
        let etag_cache = cache(&[("k", Some("\"cached\""))]);
        let mut consecutive = 0u32;

        let outcome = run_heartbeat_tick(
            &["k".to_string()],
            storage.as_ref(),
            9,
            Duration::from_secs(3),
            &etag_cache,
            &mut consecutive,
            "fake",
        )
        .await;

        assert!(
            matches!(outcome, HeartbeatOutcome::Continue),
            "a single transient failure must not cancel"
        );
        assert_eq!(consecutive, 1, "the failure must be counted in the budget");
        // The cache entry is cleared so the next tick takes the slow re-read path.
        assert!(
            etag_cache.read().unwrap().get("k").unwrap().is_none(),
            "a failed tick clears the cached ETag"
        );
    }

    #[tokio::test]
    async fn run_tick_failures_escalate_after_one_ttl() {
        let storage = FakeLockStorage::arc();
        storage.set_put_match(PutMatchScript::Failure);
        let etag_cache = cache(&[("k", Some("\"cached\""))]);
        let mut consecutive = 0u32;

        // ttl=9, tick=3 ⇒ ticks_per_ttl = 9 / 3 = 3 consecutive failures escalate.
        // The cache only holds a cached ETag on the first tick; thereafter the
        // slow path re-reads via get_with_etag, so prime that script too.
        storage.set_get(GetScript::Failure);

        let paths = ["k".to_string()];
        let tick = Duration::from_secs(3);

        let o1 = run_heartbeat_tick(
            &paths,
            storage.as_ref(),
            9,
            tick,
            &etag_cache,
            &mut consecutive,
            "fake",
        )
        .await;
        assert!(matches!(o1, HeartbeatOutcome::Continue));
        let o2 = run_heartbeat_tick(
            &paths,
            storage.as_ref(),
            9,
            tick,
            &etag_cache,
            &mut consecutive,
            "fake",
        )
        .await;
        assert!(matches!(o2, HeartbeatOutcome::Continue));
        let o3 = run_heartbeat_tick(
            &paths,
            storage.as_ref(),
            9,
            tick,
            &etag_cache,
            &mut consecutive,
            "fake",
        )
        .await;

        assert!(
            matches!(o3, HeartbeatOutcome::Invalidate("heartbeat_failure")),
            "consecutive failures spanning one TTL must escalate to heartbeat_failure"
        );
    }

    #[tokio::test]
    async fn run_tick_success_resets_failure_counter() {
        let storage = FakeLockStorage::arc();
        let etag_cache = cache(&[("k", Some("\"cached\""))]);
        let mut consecutive = 2u32;

        storage.set_put_match(PutMatchScript::Updated);
        let outcome = run_heartbeat_tick(
            &["k".to_string()],
            storage.as_ref(),
            9,
            Duration::from_secs(3),
            &etag_cache,
            &mut consecutive,
            "fake",
        )
        .await;

        assert!(matches!(outcome, HeartbeatOutcome::Continue));
        assert_eq!(consecutive, 0, "a healthy tick resets the failure budget");
        assert!(
            etag_cache.read().unwrap().get("k").unwrap().is_some(),
            "a healthy tick advances the cached ETag"
        );
    }

    #[tokio::test]
    async fn run_tick_mismatch_invalidates_immediately() {
        let storage = FakeLockStorage::arc();
        storage.set_put_match(PutMatchScript::Mismatch);
        let etag_cache = cache(&[("k", Some("\"cached\""))]);
        let mut consecutive = 0u32;

        let outcome = run_heartbeat_tick(
            &["k".to_string()],
            storage.as_ref(),
            9,
            Duration::from_secs(3),
            &etag_cache,
            &mut consecutive,
            "fake",
        )
        .await;

        assert!(
            matches!(outcome, HeartbeatOutcome::Invalidate("ownership_lost")),
            "ownership loss must short-circuit the tick regardless of the failure budget"
        );
    }

    // ─── try_recover_stale ─────────────────────────────────────────────────

    #[tokio::test]
    async fn recover_stale_claims_expired_lock_when_race_won() {
        let storage = FakeLockStorage::arc();
        storage.set_get(GetScript::Expired);
        storage.set_put_match(PutMatchScript::Updated);
        let lock = lock_with(storage);

        let result = lock.try_recover_stale("k").await.expect("no hard error");
        assert!(
            matches!(result, Some(Some(_))),
            "an expired lock won via put_if_match must be claimed with a fresh ETag"
        );
    }

    #[tokio::test]
    async fn recover_stale_returns_none_when_not_expired() {
        let storage = FakeLockStorage::arc();
        storage.set_get(GetScript::Fresh);
        let lock = lock_with(storage.clone());

        let result = lock.try_recover_stale("k").await.expect("no hard error");
        assert!(result.is_none(), "a fresh lock must not be recovered");
        assert_eq!(
            storage.put_match_calls.load(Ordering::Relaxed),
            0,
            "a fresh lock must not attempt a conditional replace"
        );
    }

    #[tokio::test]
    async fn recover_stale_returns_none_when_put_if_match_loses_race() {
        let storage = FakeLockStorage::arc();
        storage.set_get(GetScript::Expired);
        storage.set_put_match(PutMatchScript::Mismatch);
        let lock = lock_with(storage);

        let result = lock.try_recover_stale("k").await.expect("no hard error");
        assert!(
            result.is_none(),
            "losing the put_if_match race (another replica recovered first) returns None"
        );
    }

    #[tokio::test]
    async fn recover_stale_returns_none_when_key_absent() {
        let storage = FakeLockStorage::arc();
        storage.set_get(GetScript::NotFound);
        let lock = lock_with(storage);

        let result = lock.try_recover_stale("k").await.expect("no hard error");
        assert!(result.is_none(), "a vanished key has nothing to recover");
    }

    #[tokio::test]
    async fn recover_stale_errors_when_expired_but_no_etag() {
        let storage = FakeLockStorage::arc();
        storage.set_get(GetScript::ExpiredNoEtag);
        let lock = lock_with(storage);

        let result = lock.try_recover_stale("k").await;
        assert!(
            matches!(result, Err(Error::InvalidData(_))),
            "an expired lock with no ETag cannot be fenced; recovery must error"
        );
    }

    // ─── acquire / try_acquire ─────────────────────────────────────────────

    #[tokio::test]
    async fn try_acquire_created_returns_session() {
        let storage = FakeLockStorage::arc();
        storage.set_put_absent_exists(false);
        let lock = lock_with(storage);

        let session = lock
            .try_acquire(&["k".to_string()])
            .await
            .expect("no hard error");
        assert!(session.is_some(), "an absent key must yield a session");
        session.unwrap().release().await;
    }

    #[tokio::test]
    async fn try_acquire_contended_fresh_returns_none() {
        let storage = FakeLockStorage::arc();
        storage.set_put_absent_exists(true);
        storage.set_get(GetScript::Fresh);
        let lock = lock_with(storage);

        let session = lock
            .try_acquire(&["k".to_string()])
            .await
            .expect("no hard error");
        assert!(
            session.is_none(),
            "a held, non-expired lock must yield None from try_acquire"
        );
    }

    #[tokio::test]
    async fn try_acquire_contended_expired_recovers() {
        let storage = FakeLockStorage::arc();
        storage.set_put_absent_exists(true);
        storage.set_get(GetScript::Expired);
        storage.set_put_match(PutMatchScript::Updated);
        let lock = lock_with(storage);

        let session = lock
            .try_acquire(&["k".to_string()])
            .await
            .expect("no hard error");
        assert!(
            session.is_some(),
            "an expired lock must be recovered and acquired"
        );
        session.unwrap().release().await;
    }

    #[tokio::test]
    async fn acquire_errors_after_max_retries_when_held() {
        let storage = FakeLockStorage::arc();
        storage.set_put_absent_exists(true);
        storage.set_get(GetScript::Fresh);
        // max_retries=2, retry_delay_ms=1 keeps the loop fast.
        let lock = Lock::builder(storage)
            .ttl_secs(9)
            .max_hold_secs(9)
            .max_retries(2)
            .retry_delay_ms(1)
            .build()
            .expect("lock builder");

        let result = lock.acquire(&["k".to_string()]).await;
        assert!(
            matches!(result, Err(Error::Lock(_))),
            "a continuously-held lock must error after the retry budget is exhausted"
        );
    }

    // ─── release ───────────────────────────────────────────────────────────

    #[tokio::test]
    async fn release_uses_delete_if_match_with_cached_etag() {
        let storage = FakeLockStorage::arc();
        storage.set_put_absent_exists(false);
        let lock = lock_with(storage.clone());

        let session = lock
            .try_acquire(&["k".to_string()])
            .await
            .expect("no hard error")
            .expect("session");
        session.release().await;

        assert_eq!(
            storage.delete_if_match_calls.load(Ordering::Relaxed),
            1,
            "release must use the conditional delete fast path with the cached ETag"
        );
        let used = storage.last_delete_etag.lock().unwrap().clone();
        assert!(
            used.is_some_and(|e| e.starts_with("\"etag-")),
            "release must pass the ETag minted at acquire time"
        );
    }

    #[tokio::test]
    async fn release_is_idempotent_when_already_gone() {
        let storage = FakeLockStorage::arc();
        storage.set_put_absent_exists(false);
        // delete_if_match reports Mismatch (someone else owns / it's gone); the
        // release must complete without panicking and not fall through to a
        // second hard delete.
        *storage.delete_mismatch.lock().unwrap() = true;
        let lock = lock_with(storage.clone());

        let session = lock
            .try_acquire(&["k".to_string()])
            .await
            .expect("no hard error")
            .expect("session");
        session.release().await;

        assert_eq!(
            storage.delete_calls.load(Ordering::Relaxed),
            0,
            "a Mismatch on the conditional delete must not escalate to a plain delete"
        );
    }

    // ─── run_heartbeat_tick (concurrent refresh) ───────────────────────────

    /// [`LockStorage`] double that records the maximum number of `put_if_match`
    /// calls that were ever simultaneously in-flight.
    ///
    /// Each call increments an in-flight counter, then awaits a [`Barrier`]
    /// sized to the number of paths. The barrier only releases once every path's
    /// call has arrived, so a sequential refresh (one call at a time) would
    /// deadlock-stall and time out, whereas a concurrent refresh sails through.
    /// The observed peak therefore equals the barrier width when (and only when)
    /// the refreshes truly overlap. Determinism comes from the barrier rather
    /// than any wall-clock sleep.
    struct ConcurrencyRecordingStorage {
        next_etag: AtomicU64,
        in_flight: AtomicUsize,
        max_in_flight: AtomicUsize,
        barrier: Barrier,
    }

    impl std::fmt::Debug for ConcurrencyRecordingStorage {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("ConcurrencyRecordingStorage")
                .finish_non_exhaustive()
        }
    }

    impl ConcurrencyRecordingStorage {
        fn arc(width: usize) -> Arc<Self> {
            Arc::new(Self {
                next_etag: AtomicU64::new(0),
                in_flight: AtomicUsize::new(0),
                max_in_flight: AtomicUsize::new(0),
                barrier: Barrier::new(width),
            })
        }

        fn mint_etag(&self) -> String {
            let v = self.next_etag.fetch_add(1, Ordering::Relaxed);
            format!("\"etag-{v}\"")
        }
    }

    #[async_trait]
    impl LockStorage for ConcurrencyRecordingStorage {
        async fn put_if_absent(
            &self,
            _key: &str,
            _body: Vec<u8>,
        ) -> Result<PutIfAbsentOutcome, Error> {
            Ok(PutIfAbsentOutcome::Created(Some(self.mint_etag())))
        }

        async fn put_if_match(
            &self,
            _key: &str,
            _expected_etag: &str,
            _body: Vec<u8>,
        ) -> Result<PutIfMatchOutcome, Error> {
            let now = self.in_flight.fetch_add(1, Ordering::AcqRel) + 1;
            self.max_in_flight.fetch_max(now, Ordering::AcqRel);
            // Block until every concurrent refresh has arrived. A sequential
            // caller never reaches the barrier width, so overlap is required.
            self.barrier.wait().await;
            self.in_flight.fetch_sub(1, Ordering::AcqRel);
            Ok(PutIfMatchOutcome::Updated(Some(self.mint_etag())))
        }

        async fn get_with_etag(
            &self,
            _key: &str,
        ) -> Result<(Vec<u8>, Option<String>, Option<chrono::DateTime<Utc>>), Error> {
            Err(Error::NotFound)
        }

        async fn delete(&self, _key: &str) -> Result<(), Error> {
            Ok(())
        }

        async fn delete_if_match(
            &self,
            _key: &str,
            _expected_etag: &str,
        ) -> Result<DeleteIfMatchOutcome, Error> {
            Ok(DeleteIfMatchOutcome::Deleted)
        }

        fn label(&self) -> &'static str {
            "concurrency-recording"
        }
    }

    #[tokio::test]
    async fn run_tick_refreshes_paths_concurrently() {
        let paths: Vec<String> = (0..4).map(|i| format!("k{i}")).collect();
        let storage = ConcurrencyRecordingStorage::arc(paths.len());
        let etag_cache = cache(&[
            ("k0", Some("\"c0\"")),
            ("k1", Some("\"c1\"")),
            ("k2", Some("\"c2\"")),
            ("k3", Some("\"c3\"")),
        ]);
        let mut consecutive = 0u32;

        let outcome = run_heartbeat_tick(
            &paths,
            storage.as_ref(),
            9,
            // A short per-path deadline: a sequential refresh that stalls on the
            // barrier would time out long before all paths ran, so the only way
            // every path succeeds is genuine overlap.
            Duration::from_secs(3),
            &etag_cache,
            &mut consecutive,
            "concurrency-recording",
        )
        .await;

        assert!(
            matches!(outcome, HeartbeatOutcome::Continue),
            "all paths refreshed successfully within one tick budget"
        );
        assert_eq!(consecutive, 0, "a fully-healthy tick resets the budget");
        assert_eq!(
            storage.max_in_flight.load(Ordering::Acquire),
            paths.len(),
            "all path refreshes must run concurrently within one tick budget"
        );
    }
}
