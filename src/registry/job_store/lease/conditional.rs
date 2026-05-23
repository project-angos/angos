//! CAS-based [`LeaseBackend`] for [`ConditionalStore`] (real `If-Match`).
//!
//! Used by the S3 storage backend. The lease token is the storage [`Etag`]
//! threaded through `put_if_absent` / `put_if_match` / `delete_if_match`.
//!
//! Stale-lease recovery is driven by the body's `refreshed_at` (not the
//! storage `last_modified`) — some S3-compatible providers strip
//! `Last-Modified` from `PUT` responses, so the body field is the
//! authoritative readiness signal.

use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde::{Deserialize, Serialize};
use tokio::{
    select, spawn,
    sync::Mutex,
    task::JoinHandle,
    time::{MissedTickBehavior, interval},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::registry::{
    job_store::{
        Error,
        lease::{LeaseBackend, LeaseGuard, MAX_HEARTBEAT_FAILURES},
    },
    path_builder,
};
use angos_storage::{ConditionalStore, Error as StorageError, Etag};

/// Builder for [`Backend`].
pub struct Builder {
    store: Option<Arc<dyn ConditionalStore>>,
    delete_if_match: bool,
}

impl Builder {
    fn new() -> Self {
        Self {
            store: None,
            delete_if_match: true,
        }
    }

    /// The underlying conditional store (required).
    pub fn store(mut self, store: Arc<dyn ConditionalStore>) -> Self {
        self.store = Some(store);
        self
    }

    /// Whether to use `delete_if_match` for lease release. Defaults to
    /// `true`. Set `false` on endpoints that don't honour `DELETE` with
    /// `If-Match`; release falls back to a plain delete with a small race
    /// window during lease theft.
    pub fn delete_if_match(mut self, enabled: bool) -> Self {
        self.delete_if_match = enabled;
        self
    }

    pub fn build(self) -> Result<Backend, Error> {
        let store = self.store.ok_or_else(|| {
            Error::Initialization("lease::conditional::Backend requires a store".to_string())
        })?;
        Ok(Backend {
            store,
            delete_if_match: self.delete_if_match,
        })
    }
}

/// CAS-based [`LeaseBackend`] implementation.
pub struct Backend {
    store: Arc<dyn ConditionalStore>,
    delete_if_match: bool,
}

impl Backend {
    pub fn builder() -> Builder {
        Builder::new()
    }
}

/// On-disk body of a lease object. The lease *token* is the storage
/// [`Etag`], not anything in this struct — `worker_id` is for diagnostics
/// and `refreshed_at`/`ttl_secs` drive stale-lease recovery.
#[derive(Serialize, Deserialize)]
struct LeaseFile {
    #[serde(default)]
    refreshed_at: DateTime<Utc>,
    worker_id: String,
    ttl_secs: u64,
}

fn serialize_lease(worker_id: &str, ttl_secs: u64) -> Result<Bytes, Error> {
    let body = LeaseFile {
        refreshed_at: Utc::now(),
        worker_id: worker_id.to_string(),
        ttl_secs,
    };
    serde_json::to_vec(&body)
        .map(Bytes::from)
        .map_err(|e| Error::Storage(format!("lease serialization failed: {e}")))
}

fn require_etag(etag: Option<Etag>) -> Result<Etag, Error> {
    etag.ok_or_else(|| Error::Storage("storage backend returned no ETag for PUT".to_string()))
}

/// Issue a single heartbeat write. Mutates `current` to the new etag on
/// success. Returns `Err` on ownership loss or storage error.
async fn heartbeat_once(
    store: &dyn ConditionalStore,
    lock_key: &str,
    current: &mut Etag,
    worker_id: &str,
    ttl_secs: u64,
) -> Result<(), Error> {
    let key = path_builder::job_lease_path(lock_key);
    let payload = serialize_lease(worker_id, ttl_secs)?;
    match store.put_if_match(&key, current, payload).await {
        Ok(new_etag) => {
            if let Some(e) = new_etag {
                *current = e;
            }
            Ok(())
        }
        Err(StorageError::PreconditionFailed) => Err(Error::Storage(
            "heartbeat failed: lease ownership changed".to_string(),
        )),
        Err(e) => Err(Error::from(e)),
    }
}

/// Best-effort release. Conditional delete when supported; mismatch is
/// treated as success (someone else owns it now). Falls back to
/// unconditional delete on endpoints that ignore `If-Match` on `DELETE`.
async fn release_once(
    store: &dyn ConditionalStore,
    lock_key: &str,
    token: &Etag,
    delete_if_match: bool,
) {
    let key = path_builder::job_lease_path(lock_key);
    let outcome = if delete_if_match {
        store.delete_if_match(&key, token).await
    } else {
        store.delete(&key).await
    };
    match outcome {
        Ok(()) | Err(StorageError::PreconditionFailed | StorageError::NotFound) => {}
        Err(e) => warn!(lock_key, error = %e, "Lease release failed"),
    }
}

#[async_trait]
impl LeaseBackend for Backend {
    async fn try_acquire(
        &self,
        lock_key: &str,
        worker_id: &str,
        ttl_secs: u64,
    ) -> Result<Option<LeaseGuard>, Error> {
        let key = path_builder::job_lease_path(lock_key);
        let etag = match self
            .store
            .put_if_absent(&key, serialize_lease(worker_id, ttl_secs)?)
            .await
        {
            Ok(etag) => require_etag(etag)?,
            Err(StorageError::PreconditionFailed) => {
                // Someone else holds (or held) the lease. Read it back and
                // try to steal if its body says it's past TTL.
                let (body, stale_etag) = match self.store.get_with_etag(&key).await {
                    Ok(result) => result,
                    Err(StorageError::NotFound) => return Ok(None),
                    Err(e) => return Err(Error::from(e)),
                };
                let existing: LeaseFile = serde_json::from_slice(&body)
                    .map_err(|e| Error::Storage(format!("corrupt lease: {e}")))?;
                let ttl = ChronoDuration::seconds(existing.ttl_secs.min(3600).cast_signed());
                if Utc::now() <= existing.refreshed_at + ttl {
                    return Ok(None);
                }
                let Some(stale_etag) = stale_etag else {
                    return Ok(None);
                };
                debug!(
                    lock_key,
                    worker_id = existing.worker_id,
                    refreshed_at = %existing.refreshed_at,
                    "Stealing stale lease"
                );
                match self
                    .store
                    .put_if_match(&key, &stale_etag, serialize_lease(worker_id, ttl_secs)?)
                    .await
                {
                    Ok(etag) => require_etag(etag)?,
                    Err(StorageError::PreconditionFailed) => return Ok(None),
                    Err(e) => return Err(Error::from(e)),
                }
            }
            Err(e) => return Err(Error::from(e)),
        };

        let lost = CancellationToken::new();
        let current = Arc::new(Mutex::new(etag));
        let heartbeat = spawn_heartbeat(
            self.store.clone(),
            lock_key.to_string(),
            current.clone(),
            worker_id.to_string(),
            ttl_secs,
            lost.clone(),
        );

        let release_store = self.store.clone();
        let release_lock_key = lock_key.to_string();
        let release_current = current;
        let release_delete_if_match = self.delete_if_match;
        let release_fn = move || -> Pin<Box<dyn Future<Output = ()> + Send>> {
            Box::pin(async move {
                let etag = release_current.lock().await.clone();
                release_once(
                    &*release_store,
                    &release_lock_key,
                    &etag,
                    release_delete_if_match,
                )
                .await;
            })
        };

        Ok(Some(LeaseGuard::new(lost, heartbeat, release_fn)))
    }
}

fn spawn_heartbeat(
    store: Arc<dyn ConditionalStore>,
    lock_key: String,
    current: Arc<Mutex<Etag>>,
    worker_id: String,
    ttl_secs: u64,
    lost: CancellationToken,
) -> JoinHandle<()> {
    let tick = Duration::from_secs(ttl_secs / 3);
    spawn(async move {
        let mut timer = interval(tick);
        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
        timer.tick().await;

        let mut consecutive_failures: u32 = 0;
        loop {
            select! {
                () = lost.cancelled() => return,
                _ = timer.tick() => {}
            }
            let mut etag = current.lock().await;
            match heartbeat_once(&*store, &lock_key, &mut etag, &worker_id, ttl_secs).await {
                Ok(()) => consecutive_failures = 0,
                Err(e) => {
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    warn!(lock_key, error = %e, consecutive_failures, "Lease heartbeat failed");
                    if consecutive_failures >= MAX_HEARTBEAT_FAILURES {
                        warn!(lock_key, "Too many heartbeat failures, marking lease lost");
                        lost.cancel();
                        return;
                    }
                }
            }
        }
    })
}
