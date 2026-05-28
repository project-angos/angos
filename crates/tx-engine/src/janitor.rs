//! Janitors that reap engine bookkeeping prefixes:
//!
//! - [`BodyJanitor`] sweeps `tx-bodies/<tx-id>/` prefixes whose intent never
//!   landed in `tx-log/`, bounded by an age threshold.
//! - [`LockJanitor`] sweeps `_locks/` for lock objects whose declared TTL has
//!   elapsed by more than `orphan_age` — the cold-key counterpart to the lock
//!   primitive's acquire-path stale-lock recovery.

use std::sync::Arc;
use std::time::Duration;

use chrono::{Duration as ChronoDuration, Utc};
use tokio::{
    select,
    time::{MissedTickBehavior, interval},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use uuid::Uuid;

use angos_storage::{ConditionalStore, Error as StorageError, ObjectStore};

use crate::lock::storage::LockBody;

/// Default age after which an orphan body prefix is eligible for deletion.
pub const DEFAULT_ORPHAN_AGE_SECS: u64 = 3600; // 1 hour

/// Orphan-body janitor.
///
/// On each tick, lists `tx-bodies/` children and for each `tx-id` prefix
/// that has no corresponding `tx-log/<tx-id>.json` and whose UUID timestamp
/// indicates it is older than the configured age, deletes the entire prefix.
///
/// Constructed via [`BodyJanitor::builder`].
pub struct BodyJanitor {
    store: Arc<dyn ObjectStore>,
    interval: Duration,
    orphan_age: Duration,
    cancellation: CancellationToken,
}

impl std::fmt::Debug for BodyJanitor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
        let mut ticker = interval(self.interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            select! {
                biased;
                () = self.cancellation.cancelled() => {
                    debug!("BodyJanitor: cancellation received, stopping");
                    return;
                }
                _ = ticker.tick() => {
                    self.sweep().await;
                }
            }
        }
    }

    /// Run a single sweep of `tx-bodies/`.
    pub async fn sweep(&self) {
        let mut token: Option<String> = None;
        loop {
            match self
                .store
                .list_children("tx-bodies/", 100, token.clone(), None)
                .await
            {
                Ok(page) => {
                    for sub_prefix in &page.sub_prefixes {
                        self.process_prefix(sub_prefix).await;
                    }
                    if page.next_token.is_none() {
                        break;
                    }
                    token = page.next_token;
                }
                Err(e) => {
                    warn!(error = %e, "BodyJanitor: failed to list tx-bodies/");
                    break;
                }
            }
        }
    }

    /// Examine one `tx-bodies/<tx-id>/` prefix. Delete it if the tx-id
    /// corresponds to a v4 UUID that is old enough and has no live intent.
    async fn process_prefix(&self, sub_prefix: &str) {
        // sub_prefix is like "tx-bodies/<tx-id>/" — extract the UUID.
        let tx_id_str = sub_prefix
            .trim_start_matches("tx-bodies/")
            .trim_end_matches('/');

        let Ok(tx_id) = tx_id_str.parse::<Uuid>() else {
            // Not a UUID-shaped prefix; skip.
            return;
        };

        // Age estimation: v4 UUIDs are random but we can use object head for
        // last-modified. Fall back to skipping if head is unavailable.
        // Simpler: check whether the intent exists first.
        let intent_key = format!("tx-log/{tx_id}.json");
        match self.store.head(&intent_key).await {
            Ok(_) => {
                // Intent exists — not an orphan; the owner or recovery will reap.
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
        let prefix_key = sub_prefix.to_owned();
        let first_child_suffix = match self.store.list(&prefix_key, 1, None).await {
            Ok(page) => page.items.into_iter().next(),
            Err(e) => {
                warn!(prefix = prefix_key, error = %e, "BodyJanitor: list failed");
                return;
            }
        };
        let Some(suffix) = first_child_suffix else {
            // No children — the prefix has nothing to delete; treat as
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
                // Can't determine age — skip to avoid deleting live data.
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

/// Default page size when listing `_locks/`.
const LOCK_LIST_PAGE_SIZE: u16 = 100;

/// Orphan-lock janitor.
///
/// On each tick, paginates through `_locks/` and for each lock object whose
/// body has expired (TTL elapsed against the server-assigned `last_modified`
/// when available, falling back to the embedded `refreshed_at`) by more than
/// the configured `orphan_age`, deletes it.
///
/// The lock primitive itself does opportunistic stale-lock recovery on the
/// acquire path; this janitor handles cold keys that are never re-acquired.
///
/// `orphan_age` is added on top of the lock's own TTL, so a lock with a
/// 30-second TTL is reaped at most `orphan_age + 30s` after its last refresh.
///
/// Constructed via [`LockJanitor::builder`].
pub struct LockJanitor {
    store: Arc<dyn ObjectStore>,
    conditional_store: Option<Arc<dyn ConditionalStore>>,
    interval: Duration,
    orphan_age: Duration,
    cancellation: CancellationToken,
}

impl std::fmt::Debug for LockJanitor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LockJanitor")
            .field("interval", &self.interval)
            .field("orphan_age", &self.orphan_age)
            .field("has_conditional_store", &self.conditional_store.is_some())
            .finish_non_exhaustive()
    }
}

/// Builder for [`LockJanitor`].
pub struct LockJanitorBuilder {
    store: Arc<dyn ObjectStore>,
    conditional_store: Option<Arc<dyn ConditionalStore>>,
    interval: Option<Duration>,
    orphan_age: Option<Duration>,
    cancellation: Option<CancellationToken>,
}

impl LockJanitorBuilder {
    /// Wire a `ConditionalStore` so deletes use `delete_if_match` against the
    /// etag observed at HEAD; without it the janitor falls back to plain
    /// `delete`, which has a small race window against a fresh re-acquire of
    /// the same key.
    #[must_use]
    pub fn conditional_store(mut self, cs: Arc<dyn ConditionalStore>) -> Self {
        self.conditional_store = Some(cs);
        self
    }

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
            conditional_store: self.conditional_store,
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
    pub fn builder(store: Arc<dyn ObjectStore>) -> LockJanitorBuilder {
        LockJanitorBuilder {
            store,
            conditional_store: None,
            interval: None,
            orphan_age: None,
            cancellation: None,
        }
    }

    /// Run the janitor until the cancellation token fires.
    pub async fn run(self) {
        let mut ticker = interval(self.interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            select! {
                biased;
                () = self.cancellation.cancelled() => {
                    debug!("LockJanitor: cancellation received, stopping");
                    return;
                }
                _ = ticker.tick() => {
                    self.sweep().await;
                }
            }
        }
    }

    /// Run a single sweep of `_locks/`.
    pub async fn sweep(&self) {
        let mut token: Option<String> = None;
        loop {
            match self
                .store
                .list("_locks/", LOCK_LIST_PAGE_SIZE, token.clone())
                .await
            {
                Ok(page) => {
                    for suffix in &page.items {
                        let key = format!("_locks/{suffix}");
                        self.process_key(&key).await;
                    }
                    if page.next_token.is_none() {
                        break;
                    }
                    token = page.next_token;
                }
                Err(e) => {
                    warn!(error = %e, "LockJanitor: failed to list _locks/");
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

        if let (Some(cs), Some(etag)) = (self.conditional_store.as_ref(), meta.etag.as_ref()) {
            match cs.delete_if_match(key, etag).await {
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
            return;
        }

        // Unconditional path: a fresh acquire may slip in between our HEAD and
        // DELETE and lose its freshly-written object. Acceptable for backends
        // that do not expose CAS; the next acquire on that key reconstructs.
        match self.store.delete(key).await {
            Ok(()) => {
                info!(key, "LockJanitor: reclaimed expired lock");
            }
            Err(e) => {
                warn!(key, error = %e, "LockJanitor: delete failed");
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

    use angos_storage::{
        BoxedReader, ChildrenPage, ConditionalStore, Error as StorageError, Etag,
        MemoryObjectStore, ObjectMeta, ObjectStore, Page,
    };

    use crate::janitor::LockJanitor;
    use crate::lock::storage::LockBody;

    fn lock_key(suffix: &str) -> String {
        format!("_locks/{suffix}")
    }

    async fn write_lock(store: &MemoryObjectStore, suffix: &str, body: &LockBody) {
        let bytes = Bytes::from(serde_json::to_vec(body).expect("serialise"));
        store.put(&lock_key(suffix), bytes).await.expect("put");
    }

    fn expired_body() -> LockBody {
        LockBody {
            refreshed_at: Utc::now() - ChronoDuration::minutes(10),
            ttl_secs: 30,
        }
    }

    fn fresh_body() -> LockBody {
        LockBody {
            refreshed_at: Utc::now(),
            ttl_secs: 30,
        }
    }

    fn build_janitor(store: Arc<dyn ObjectStore>) -> LockJanitor {
        LockJanitor::builder(store)
            .orphan_age(Duration::from_mins(1))
            .build()
    }

    #[tokio::test]
    async fn expired_lock_is_reclaimed() {
        let store = Arc::new(MemoryObjectStore::new());
        write_lock(&store, "00/cold-key", &expired_body()).await;

        let janitor = build_janitor(store.clone() as Arc<dyn ObjectStore>);
        janitor.sweep().await;

        let after = store.head(&lock_key("00/cold-key")).await;
        assert!(matches!(after, Err(StorageError::NotFound)));
    }

    #[tokio::test]
    async fn fresh_lock_is_preserved() {
        let store = Arc::new(MemoryObjectStore::new());
        write_lock(&store, "00/hot-key", &fresh_body()).await;

        let janitor = build_janitor(store.clone() as Arc<dyn ObjectStore>);
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

        let janitor = build_janitor(store.clone() as Arc<dyn ObjectStore>);
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

        let janitor = build_janitor(store.clone() as Arc<dyn ObjectStore>);
        janitor.sweep().await;

        store
            .head(&lock_key("00/garbage"))
            .await
            .expect("unparseable body left alone");
    }

    /// `MemoryObjectStore` returns `last_modified: None` from `head`. A
    /// `ConditionalStore` wrapper that returns a fixed etag from `head` and
    /// always answers `PreconditionFailed` on `delete_if_match` simulates a
    /// fresh acquire that raced in between our HEAD and DELETE.
    #[derive(Debug)]
    struct RacingConditionalStore {
        inner: Arc<MemoryObjectStore>,
        fixed_etag: Etag,
    }

    impl RacingConditionalStore {
        fn new(inner: Arc<MemoryObjectStore>) -> Self {
            Self {
                inner,
                fixed_etag: Etag::new("\"stale\""),
            }
        }
    }

    #[async_trait]
    impl ObjectStore for RacingConditionalStore {
        async fn get(&self, key: &str) -> Result<Vec<u8>, StorageError> {
            self.inner.get(key).await
        }
        async fn get_stream(
            &self,
            key: &str,
            offset: Option<u64>,
        ) -> Result<(BoxedReader, u64), StorageError> {
            self.inner.get_stream(key, offset).await
        }
        async fn put(&self, key: &str, data: Bytes) -> Result<(), StorageError> {
            self.inner.put(key, data).await
        }
        async fn delete(&self, key: &str) -> Result<(), StorageError> {
            self.inner.delete(key).await
        }
        async fn delete_prefix(&self, prefix: &str) -> Result<(), StorageError> {
            self.inner.delete_prefix(prefix).await
        }
        async fn head(&self, key: &str) -> Result<ObjectMeta, StorageError> {
            let mut m = self.inner.head(key).await?;
            m.etag = Some(self.fixed_etag.clone());
            Ok(m)
        }
        async fn list(
            &self,
            prefix: &str,
            n: u16,
            token: Option<String>,
        ) -> Result<Page<String>, StorageError> {
            self.inner.list(prefix, n, token).await
        }
        async fn list_children(
            &self,
            prefix: &str,
            n: u16,
            token: Option<String>,
            start_after: Option<String>,
        ) -> Result<ChildrenPage, StorageError> {
            self.inner
                .list_children(prefix, n, token, start_after)
                .await
        }
        async fn copy(&self, source: &str, destination: &str) -> Result<(), StorageError> {
            self.inner.copy(source, destination).await
        }
    }

    #[async_trait]
    impl ConditionalStore for RacingConditionalStore {
        async fn get_with_etag(&self, key: &str) -> Result<(Vec<u8>, Option<Etag>), StorageError> {
            let body = self.inner.get(key).await?;
            Ok((body, Some(self.fixed_etag.clone())))
        }
        async fn put_if_absent(
            &self,
            _key: &str,
            _data: Bytes,
        ) -> Result<Option<Etag>, StorageError> {
            Err(StorageError::PreconditionFailed)
        }
        async fn put_if_match(
            &self,
            _key: &str,
            _etag: &Etag,
            _data: Bytes,
        ) -> Result<Option<Etag>, StorageError> {
            Err(StorageError::PreconditionFailed)
        }
        async fn delete_if_match(&self, _key: &str, _etag: &Etag) -> Result<(), StorageError> {
            Err(StorageError::PreconditionFailed)
        }
    }

    #[tokio::test]
    async fn conditional_delete_precondition_failed_leaves_lock_in_place() {
        let inner = Arc::new(MemoryObjectStore::new());
        write_lock(&inner, "00/raced", &expired_body()).await;
        let racing = Arc::new(RacingConditionalStore::new(inner.clone()));

        let janitor = LockJanitor::builder(racing.clone() as Arc<dyn ObjectStore>)
            .conditional_store(racing.clone() as Arc<dyn ConditionalStore>)
            .orphan_age(Duration::from_mins(1))
            .build();
        janitor.sweep().await;

        inner
            .head(&lock_key("00/raced"))
            .await
            .expect("expired lock survives a racing fresh-acquire");
    }
}
