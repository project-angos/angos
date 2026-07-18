//! Janitors that reap engine bookkeeping prefixes:
//!
//! - [`BodyJanitor`] sweeps `.tx-bodies/<tx-id>/` prefixes whose intent never
//!   landed in `.tx-log/`, bounded by an age threshold.
//! - [`LockJanitor`] sweeps `.tx-locks/` for lock objects whose declared TTL has
//!   elapsed by more than `orphan_age`: the cold-key counterpart to the lock
//!   primitive's acquire-path stale-lock recovery.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use chrono::{Duration as ChronoDuration, Utc};
use futures_util::stream::{self, StreamExt};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use uuid::Uuid;

use angos_storage::{ConditionalStore, Error as StorageError, ObjectStore};

use crate::intent::{INTENT_BODIES_PREFIX, INTENT_LOG_PREFIX};
use crate::lock::storage::{LOCK_OBJECTS_PREFIX, LockBody};
use crate::periodic::run_periodic;

/// Default age after which an orphan body prefix is eligible for deletion.
pub const DEFAULT_ORPHAN_AGE_SECS: u64 = 3600; // 1 hour

/// Fan-out for the per-item probes within one janitor listing page: each item
/// costs a few independent head/get round-trips, run concurrently so a sweep
/// is bound by backend latency, not item count.
const SWEEP_CONCURRENCY: usize = 8;

/// Orphan-body janitor.
///
/// On each tick, lists `.tx-bodies/` children and for each `tx-id` prefix
/// that has no corresponding `.tx-log/<tx-id>.json`, heads the first staged
/// body object inside the prefix and deletes the entire prefix when that
/// object's `last_modified` is older than the configured age.
///
/// Constructed via [`BodyJanitor::builder`].
pub struct BodyJanitor {
    store: Arc<dyn ObjectStore>,
    interval: Duration,
    orphan_age: Duration,
    cancellation: CancellationToken,
}

impl std::fmt::Debug for BodyJanitor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BodyJanitor")
            .field("interval", &self.interval)
            .field("orphan_age", &self.orphan_age)
            .finish_non_exhaustive()
    }
}

/// Builder for [`BodyJanitor`].
pub struct BodyJanitorBuilder {
    store: Arc<dyn ObjectStore>,
    interval: Option<Duration>,
    orphan_age: Option<Duration>,
    cancellation: Option<CancellationToken>,
}

impl BodyJanitorBuilder {
    /// Set the sweep interval. Defaults to 5 minutes.
    #[must_use]
    pub fn interval(mut self, interval: Duration) -> Self {
        self.interval = Some(interval);
        self
    }

    /// Set the minimum age for orphan body prefixes. Defaults to 1 hour.
    #[must_use]
    pub fn orphan_age(mut self, age: Duration) -> Self {
        self.orphan_age = Some(age);
        self
    }

    /// Set the cancellation token.
    #[must_use]
    pub fn cancellation(mut self, token: CancellationToken) -> Self {
        self.cancellation = Some(token);
        self
    }

    /// Consume the builder and produce a [`BodyJanitor`].
    #[must_use]
    pub fn build(self) -> BodyJanitor {
        BodyJanitor {
            store: self.store,
            interval: self.interval.unwrap_or(Duration::from_mins(5)),
            orphan_age: self
                .orphan_age
                .unwrap_or(Duration::from_secs(DEFAULT_ORPHAN_AGE_SECS)),
            cancellation: self.cancellation.unwrap_or_default(),
        }
    }
}

impl BodyJanitor {
    /// Return a builder for constructing a `BodyJanitor`.
    ///
    /// `store` is a required argument; all other settings are optional fluent
    /// setters on the returned builder.
    #[must_use]
    pub fn builder(store: Arc<dyn ObjectStore>) -> BodyJanitorBuilder {
        BodyJanitorBuilder {
            store,
            interval: None,
            orphan_age: None,
            cancellation: None,
        }
    }

    /// Run the janitor until the cancellation token fires.
    pub async fn run(self) {
        run_periodic(self.interval, &self.cancellation, "BodyJanitor", || {
            self.sweep()
        })
        .await;
    }

    /// Run a single sweep of `.tx-bodies/`.
    pub async fn sweep(&self) {
        let mut token: Option<String> = None;
        loop {
            match self
                .store
                .list_children(
                    &format!("{INTENT_BODIES_PREFIX}/"),
                    100,
                    token.clone(),
                    None,
                )
                .await
            {
                Ok(page) => {
                    stream::iter(&page.sub_prefixes)
                        .for_each_concurrent(SWEEP_CONCURRENCY, |sub_prefix| {
                            self.process_prefix(sub_prefix)
                        })
                        .await;
                    if page.next_token.is_none() {
                        break;
                    }
                    token = page.next_token;
                }
                Err(e) => {
                    warn!(error = %e, "BodyJanitor: failed to list .tx-bodies/");
                    break;
                }
            }
        }
    }

    /// Examine one `.tx-bodies/<tx-id>/` prefix. Delete it if the tx-id
    /// corresponds to a v4 UUID that is old enough and has no live intent.
    /// `sub_prefix` is the bare child name from `list_children` (the tx-id).
    async fn process_prefix(&self, sub_prefix: &str) {
        let Ok(tx_id) = sub_prefix.parse::<Uuid>() else {
            // Not a UUID-shaped prefix; skip.
            return;
        };

        // Age estimation: v4 UUIDs are random but we can use object head for
        // last-modified. Fall back to skipping if head is unavailable.
        // Simpler: check whether the intent exists first.
        let intent_key = format!("{INTENT_LOG_PREFIX}/{tx_id}.json");
        match self.store.head(&intent_key).await {
            Ok(_) => {
                // Intent exists, not an orphan; the owner or recovery will reap.
                return;
            }
            Err(StorageError::NotFound) => {
                // No intent. Check how old the prefix is via the head of the
                // first body object. If we can't determine age, be conservative.
            }
            Err(e) => {
                warn!(key = intent_key, error = %e, "BodyJanitor: head check failed");
                return;
            }
        }

        // Determine the orphan's age by heading the first object inside the
        // prefix. We deliberately do NOT `head` the prefix key itself: on S3 a
        // prefix is not an object (HEAD would 404), and on FS heading a
        // directory returns metadata of the directory entry rather than any
        // staged body, which can be misleading. Listing one child gives us a
        // real staged-body key whose `last_modified` reflects when staging
        // began.
        let prefix_key = format!("{INTENT_BODIES_PREFIX}/{tx_id}/");
        let first_child_suffix = match self.store.list(&prefix_key, 1, None).await {
            Ok(page) => page.items.into_iter().next(),
            Err(e) => {
                warn!(prefix = prefix_key, error = %e, "BodyJanitor: list failed");
                return;
            }
        };
        let Some(suffix) = first_child_suffix else {
            // No children: the prefix has nothing to delete; treat as
            // already-gone.
            return;
        };
        // `list` returns keys relative to the prefix; reconstruct the full key
        // so `head` resolves correctly on every backend.
        let first_child = format!("{prefix_key}{suffix}");
        let is_old_enough = match self.store.head(&first_child).await {
            Ok(meta) => meta.last_modified.is_some_and(|t| {
                Utc::now()
                    .signed_duration_since(t)
                    .to_std()
                    .unwrap_or(Duration::ZERO)
                    >= self.orphan_age
            }),
            Err(_) => {
                // Can't determine age; skip to avoid deleting live data.
                false
            }
        };

        if !is_old_enough {
            return;
        }

        info!(
            tx_id = %tx_id,
            prefix = prefix_key,
            "BodyJanitor: deleting orphan body prefix"
        );
        if let Err(e) = self.store.delete_prefix(&prefix_key).await {
            warn!(
                tx_id = %tx_id,
                prefix = prefix_key,
                error = %e,
                "BodyJanitor: failed to delete orphan prefix"
            );
        }
    }
}

/// Default extra grace period beyond a lock object's declared TTL before the
/// janitor reclaims it.
pub const DEFAULT_LOCK_ORPHAN_AGE_SECS: u64 = 300; // 5 minutes

/// Default page size when listing `.tx-locks/`.
const LOCK_LIST_PAGE_SIZE: u16 = 100;

/// Orphan-lock janitor.
///
/// On each tick, paginates through `.tx-locks/` and for each lock object whose
/// body has expired (TTL elapsed against the server-assigned `last_modified`
/// when available, falling back to the embedded `refreshed_at`) by more than
/// the configured `orphan_age`, deletes it conditionally on the `ETag`
/// observed at HEAD, so a fresh re-acquire racing the sweep is never deleted.
///
/// The lock primitive itself does opportunistic stale-lock recovery on the
/// acquire path; this janitor handles cold keys that are never re-acquired.
/// Lock objects only exist on CAS-capable backends, which is why the janitor
/// requires a [`ConditionalStore`].
///
/// `orphan_age` is added on top of the lock's own TTL, so a lock with a
/// 30-second TTL is reaped at most `orphan_age + 30s` after its last refresh.
///
/// Constructed via [`LockJanitor::builder`].
pub struct LockJanitor {
    store: Arc<dyn ConditionalStore>,
    interval: Duration,
    orphan_age: Duration,
    cancellation: CancellationToken,
}

impl std::fmt::Debug for LockJanitor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LockJanitor")
            .field("interval", &self.interval)
            .field("orphan_age", &self.orphan_age)
            .finish_non_exhaustive()
    }
}

/// Builder for [`LockJanitor`].
pub struct LockJanitorBuilder {
    store: Arc<dyn ConditionalStore>,
    interval: Option<Duration>,
    orphan_age: Option<Duration>,
    cancellation: Option<CancellationToken>,
}

impl LockJanitorBuilder {
    /// Set the sweep interval. Defaults to 5 minutes.
    #[must_use]
    pub fn interval(mut self, interval: Duration) -> Self {
        self.interval = Some(interval);
        self
    }

    /// Set the grace period applied on top of each lock's own TTL. Defaults to
    /// 5 minutes.
    #[must_use]
    pub fn orphan_age(mut self, age: Duration) -> Self {
        self.orphan_age = Some(age);
        self
    }

    /// Set the cancellation token.
    #[must_use]
    pub fn cancellation(mut self, token: CancellationToken) -> Self {
        self.cancellation = Some(token);
        self
    }

    /// Consume the builder and produce a [`LockJanitor`].
    #[must_use]
    pub fn build(self) -> LockJanitor {
        LockJanitor {
            store: self.store,
            interval: self.interval.unwrap_or(Duration::from_mins(5)),
            orphan_age: self
                .orphan_age
                .unwrap_or(Duration::from_secs(DEFAULT_LOCK_ORPHAN_AGE_SECS)),
            cancellation: self.cancellation.unwrap_or_default(),
        }
    }
}

impl LockJanitor {
    /// Return a builder for constructing a `LockJanitor`.
    ///
    /// `store` is a required argument; all other settings are optional fluent
    /// setters on the returned builder.
    #[must_use]
    pub fn builder(store: Arc<dyn ConditionalStore>) -> LockJanitorBuilder {
        LockJanitorBuilder {
            store,
            interval: None,
            orphan_age: None,
            cancellation: None,
        }
    }

    /// Run the janitor until the cancellation token fires.
    pub async fn run(self) {
        run_periodic(self.interval, &self.cancellation, "LockJanitor", || {
            self.sweep()
        })
        .await;
    }

    /// Run a single sweep of `.tx-locks/`.
    pub async fn sweep(&self) {
        let mut token: Option<String> = None;
        loop {
            match self
                .store
                .list(
                    &format!("{LOCK_OBJECTS_PREFIX}/"),
                    LOCK_LIST_PAGE_SIZE,
                    token.clone(),
                )
                .await
            {
                Ok(page) => {
                    stream::iter(&page.items)
                        .for_each_concurrent(SWEEP_CONCURRENCY, |suffix| async move {
                            let key = format!(".tx-locks/{suffix}");
                            self.process_key(&key).await;
                        })
                        .await;
                    if page.next_token.is_none() {
                        break;
                    }
                    token = page.next_token;
                }
                Err(e) => {
                    warn!(error = %e, "LockJanitor: failed to list .tx-locks/");
                    break;
                }
            }
        }
    }

    /// Inspect a single lock object and delete it if expired by more than
    /// `orphan_age`.
    async fn process_key(&self, key: &str) {
        let meta = match self.store.head(key).await {
            Ok(m) => m,
            Err(StorageError::NotFound) => return,
            Err(e) => {
                warn!(key, error = %e, "LockJanitor: head failed");
                return;
            }
        };

        let bytes = match self.store.get(key).await {
            Ok(b) => b,
            Err(StorageError::NotFound) => return,
            Err(e) => {
                warn!(key, error = %e, "LockJanitor: get failed");
                return;
            }
        };

        let body: LockBody = match serde_json::from_slice(&bytes) {
            Ok(b) => b,
            Err(e) => {
                warn!(
                    key,
                    error = %e,
                    "LockJanitor: lock body failed to deserialise; leaving in place"
                );
                return;
            }
        };

        if !body.is_expired(meta.last_modified) {
            return;
        }

        let reference = meta.last_modified.unwrap_or(body.refreshed_at);
        let total_grace = ChronoDuration::seconds(body.ttl_secs.cast_signed())
            + ChronoDuration::from_std(self.orphan_age).unwrap_or(ChronoDuration::zero());
        if Utc::now() <= reference + total_grace {
            return;
        }

        let Some(etag) = meta.etag.as_ref() else {
            warn!(
                key,
                "LockJanitor: lock object has no ETag; leaving in place"
            );
            return;
        };
        match self.store.delete_if_match(key, etag).await {
            Ok(()) => {
                info!(key, "LockJanitor: reclaimed expired lock");
            }
            Err(StorageError::PreconditionFailed) => {
                debug!(
                    key,
                    "LockJanitor: lock was re-acquired between head and delete; skipping"
                );
            }
            Err(e) => {
                warn!(key, error = %e, "LockJanitor: conditional delete failed");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use async_trait::async_trait;
    use bytes::Bytes;
    use chrono::{Duration as ChronoDuration, Utc};
    use uuid::Uuid;

    use angos_storage::{
        ConditionalStore, Error as StorageError, MemoryObjectStore, ObjectStore,
        test_util::{HookedStore, StoreHook, StoreOp},
    };

    use crate::intent::{INTENT_BODIES_PREFIX, INTENT_LOG_PREFIX};
    use crate::janitor::{BodyJanitor, LockJanitor};
    use crate::lock::storage::LockBody;

    fn body_janitor(store: Arc<dyn ObjectStore>) -> BodyJanitor {
        BodyJanitor::builder(store)
            .orphan_age(Duration::ZERO)
            .build()
    }

    async fn write_body(store: &MemoryObjectStore, tx_id: Uuid, idx: u32) {
        store
            .put(
                &format!("{INTENT_BODIES_PREFIX}/{tx_id}/{idx}"),
                Bytes::from_static(b"staged"),
            )
            .await
            .expect("put body");
    }

    #[tokio::test]
    async fn orphan_body_prefix_is_reclaimed() {
        let store = Arc::new(MemoryObjectStore::new());
        let tx_id = Uuid::new_v4();
        write_body(&store, tx_id, 0).await;
        write_body(&store, tx_id, 1).await;

        body_janitor(store.clone()).sweep().await;

        for idx in 0..2 {
            let after = store
                .head(&format!("{INTENT_BODIES_PREFIX}/{tx_id}/{idx}"))
                .await;
            assert!(
                matches!(after, Err(StorageError::NotFound)),
                "orphan staged body {idx} must be reclaimed"
            );
        }
    }

    #[tokio::test]
    async fn body_prefix_with_live_intent_is_preserved() {
        let store = Arc::new(MemoryObjectStore::new());
        let tx_id = Uuid::new_v4();
        write_body(&store, tx_id, 0).await;
        store
            .put(
                &format!("{INTENT_LOG_PREFIX}/{tx_id}.json"),
                Bytes::from_static(b"{}"),
            )
            .await
            .expect("put intent");

        body_janitor(store.clone()).sweep().await;

        store
            .head(&format!("{INTENT_BODIES_PREFIX}/{tx_id}/0"))
            .await
            .expect("a body with a live intent must survive");
    }

    #[tokio::test]
    async fn young_orphan_body_is_preserved() {
        let store = Arc::new(MemoryObjectStore::new());
        let tx_id = Uuid::new_v4();
        write_body(&store, tx_id, 0).await;

        BodyJanitor::builder(store.clone() as Arc<dyn ObjectStore>)
            .orphan_age(Duration::from_secs(3600))
            .build()
            .sweep()
            .await;

        store
            .head(&format!("{INTENT_BODIES_PREFIX}/{tx_id}/0"))
            .await
            .expect("a body younger than the orphan age must survive");
    }

    #[tokio::test]
    async fn non_uuid_prefix_is_skipped() {
        let store = Arc::new(MemoryObjectStore::new());
        store
            .put(
                &format!("{INTENT_BODIES_PREFIX}/not-a-uuid/0"),
                Bytes::from_static(b"?"),
            )
            .await
            .expect("put");

        body_janitor(store.clone()).sweep().await;

        store
            .head(&format!("{INTENT_BODIES_PREFIX}/not-a-uuid/0"))
            .await
            .expect("a non-UUID prefix is not the janitor's to reclaim");
    }

    fn lock_key(suffix: &str) -> String {
        format!(".tx-locks/{suffix}")
    }

    async fn write_lock(store: &MemoryObjectStore, suffix: &str, body: &LockBody) {
        let bytes = Bytes::from(serde_json::to_vec(body).expect("serialise"));
        store.put(&lock_key(suffix), bytes).await.expect("put");
        // The janitor ages a lock by its object mtime (heartbeat rewrites
        // bump it); align the fixture's mtime with the body's declared
        // refresh time.
        store
            .backdate(&lock_key(suffix), body.refreshed_at)
            .expect("backdate");
    }

    fn expired_body() -> LockBody {
        LockBody {
            refreshed_at: Utc::now() - ChronoDuration::minutes(10),
            ttl_secs: 30,
            writer_nonce: Uuid::new_v4(),
        }
    }

    fn fresh_body() -> LockBody {
        LockBody {
            refreshed_at: Utc::now(),
            ttl_secs: 30,
            writer_nonce: Uuid::new_v4(),
        }
    }

    fn build_janitor(store: Arc<dyn ConditionalStore>) -> LockJanitor {
        LockJanitor::builder(store)
            .orphan_age(Duration::from_mins(1))
            .build()
    }

    #[tokio::test]
    async fn expired_lock_is_reclaimed() {
        let store = Arc::new(MemoryObjectStore::new());
        write_lock(&store, "00/cold-key", &expired_body()).await;

        let janitor = build_janitor(store.clone() as Arc<dyn ConditionalStore>);
        janitor.sweep().await;

        let after = store.head(&lock_key("00/cold-key")).await;
        assert!(matches!(after, Err(StorageError::NotFound)));
    }

    #[tokio::test]
    async fn fresh_lock_is_preserved() {
        let store = Arc::new(MemoryObjectStore::new());
        write_lock(&store, "00/hot-key", &fresh_body()).await;

        let janitor = build_janitor(store.clone() as Arc<dyn ConditionalStore>);
        janitor.sweep().await;

        store
            .head(&lock_key("00/hot-key"))
            .await
            .expect("fresh lock survives");
    }

    #[tokio::test]
    async fn shard_isolation_only_reclaims_expired() {
        let store = Arc::new(MemoryObjectStore::new());
        write_lock(&store, "00/expired", &expired_body()).await;
        write_lock(&store, "01/fresh", &fresh_body()).await;

        let janitor = build_janitor(store.clone() as Arc<dyn ConditionalStore>);
        janitor.sweep().await;

        assert!(matches!(
            store.head(&lock_key("00/expired")).await,
            Err(StorageError::NotFound)
        ));
        store
            .head(&lock_key("01/fresh"))
            .await
            .expect("fresh sibling untouched");
    }

    #[tokio::test]
    async fn malformed_body_is_skipped() {
        let store = Arc::new(MemoryObjectStore::new());
        store
            .put(&lock_key("00/garbage"), Bytes::from_static(b"not json"))
            .await
            .expect("put");

        let janitor = build_janitor(store.clone() as Arc<dyn ConditionalStore>);
        janitor.sweep().await;

        store
            .head(&lock_key("00/garbage"))
            .await
            .expect("unparseable body left alone");
    }

    /// Answers `PreconditionFailed` for every conditional write, simulating a
    /// fresh acquire that raced in between the janitor's HEAD and DELETE.
    struct AlwaysRaced;

    #[async_trait]
    impl StoreHook for AlwaysRaced {
        async fn before(&self, op: StoreOp<'_>) -> Result<(), StorageError> {
            match op {
                StoreOp::PutIfAbsent { .. }
                | StoreOp::PutIfMatch { .. }
                | StoreOp::DeleteIfMatch { .. } => Err(StorageError::PreconditionFailed),
                _ => Ok(()),
            }
        }
    }

    #[tokio::test]
    async fn conditional_delete_precondition_failed_leaves_lock_in_place() {
        let inner = Arc::new(MemoryObjectStore::new());
        write_lock(&inner, "00/raced", &expired_body()).await;
        let racing: Arc<HookedStore<Arc<dyn ConditionalStore>, AlwaysRaced>> =
            Arc::new(HookedStore::new(inner.clone(), AlwaysRaced));

        let janitor = LockJanitor::builder(racing.clone() as Arc<dyn ConditionalStore>)
            .orphan_age(Duration::from_mins(1))
            .build();
        janitor.sweep().await;

        inner
            .head(&lock_key("00/raced"))
            .await
            .expect("expired lock survives a racing fresh-acquire");
    }
}
