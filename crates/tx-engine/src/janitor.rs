//! Janitors that reap engine bookkeeping prefixes:
//!
//! - [`BodyJanitor`] sweeps `.tx-bodies/<tx-id>/` prefixes whose intent never
//!   landed in `.tx-log/`, bounded by an age threshold.
//! - [`LockJanitor`] sweeps `.tx-locks/` for lock objects whose declared TTL has
//!   elapsed by more than `orphan_age`, honoring any wider recovery margin the
//!   body declares: the cold-key counterpart to the lock primitive's
//!   acquire-path stale-lock recovery.

use std::sync::Arc;
use std::time::Duration;

use chrono::{Duration as ChronoDuration, Utc};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use uuid::Uuid;

use angos_storage::{ConditionalStore, Error as StorageError, ObjectStore};

use crate::lock::primitive::MAX_LOCK_TTL_SECS;
use crate::lock::storage::LockBody;
use crate::periodic::run_periodic;

/// Default age after which an orphan body prefix is eligible for deletion.
pub const DEFAULT_ORPHAN_AGE_SECS: u64 = 3600; // 1 hour

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
        run_periodic(self.interval, &self.cancellation, "BodyJanitor", || {
            self.sweep(false)
        })
        .await;
    }

    /// Run a single sweep of `.tx-bodies/`.
    ///
    /// When `dry_run` is true the janitor lists and classifies orphan prefixes
    /// exactly as a real sweep would but logs the would-delete instead of
    /// calling `delete_prefix`, mutating nothing.
    pub async fn sweep(&self, dry_run: bool) {
        let mut token: Option<String> = None;
        loop {
            match self
                .store
                .list_children(".tx-bodies/", 100, token.clone(), None)
                .await
            {
                Ok(page) => {
                    for sub_prefix in &page.sub_prefixes {
                        self.process_prefix(sub_prefix, dry_run).await;
                    }
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
    async fn process_prefix(&self, sub_prefix: &str, dry_run: bool) {
        // `list_children` returns the bare child name (the `<tx-id>` segment,
        // no `.tx-bodies/` prefix and no trailing slash) on every backend, so
        // strip any prefix the caller may have supplied and parse the UUID.
        let tx_id_str = sub_prefix
            .trim_start_matches(".tx-bodies/")
            .trim_end_matches('/');

        let Ok(tx_id) = tx_id_str.parse::<Uuid>() else {
            // Not a UUID-shaped prefix; skip.
            return;
        };

        // Age estimation: v4 UUIDs are random but we can use object head for
        // last-modified. Fall back to skipping if head is unavailable.
        // Simpler: check whether the intent exists first.
        let intent_key = format!(".tx-log/{tx_id}.json");
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
        // Reconstruct the canonical full prefix from the parsed UUID so `list`,
        // `head` and `delete_prefix` resolve regardless of the bare shape
        // `list_children` hands back. Deletion still gates on an absent intent
        // plus the orphan age, so a live push is never touched.
        let prefix_key = format!(".tx-bodies/{tx_id}/");
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

        if dry_run {
            info!(
                tx_id = %tx_id,
                prefix = prefix_key,
                "BodyJanitor: would delete orphan body prefix"
            );
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

/// Upper bound on a body-declared recovery margin the janitor honors, so a
/// malformed body cannot park a dead lock forever.
const MAX_RECOVERY_MARGIN_SECS: u64 = 7 * 24 * 3600;

/// Default page size when listing `.tx-locks/`.
const LOCK_LIST_PAGE_SIZE: u16 = 100;

/// Orphan-lock janitor.
///
/// On each tick, paginates through `.tx-locks/` and for each lock object whose
/// body has expired (TTL elapsed against the server-assigned `last_modified`
/// when available, falling back to the embedded `refreshed_at`) by more than
/// the configured `orphan_age`, deletes it.
///
/// The lock primitive itself does opportunistic stale-lock recovery on the
/// acquire path; this janitor handles cold keys that are never re-acquired.
///
/// The reclaim grace is `max(ttl + orphan_age, recovery_margin_secs)` measured
/// from the last refresh: a lock with a 30-second TTL is reaped at most
/// `orphan_age + 30s` after its last refresh, while a long-hold lock declaring
/// a wide margin in its body (e.g. the maintenance lock's steal margin) is
/// reclaimed only after that margin of silence, on every replica.
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
        run_periodic(self.interval, &self.cancellation, "LockJanitor", || {
            self.sweep(false)
        })
        .await;
    }

    /// Run a single sweep of `.tx-locks/`.
    ///
    /// When `dry_run` is true the janitor heads, reads and expiry-gates each
    /// lock exactly as a real sweep would but logs the would-reclaim instead of
    /// deleting, mutating nothing.
    pub async fn sweep(&self, dry_run: bool) {
        let mut token: Option<String> = None;
        loop {
            match self
                .store
                .list(".tx-locks/", LOCK_LIST_PAGE_SIZE, token.clone())
                .await
            {
                Ok(page) => {
                    for suffix in &page.items {
                        let key = format!(".tx-locks/{suffix}");
                        self.process_key(&key, dry_run).await;
                    }
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
    async fn process_key(&self, key: &str, dry_run: bool) {
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

        // Clamp body-declared durations so a malformed body cannot overflow the
        // arithmetic or park a lock forever.
        let reference = meta.last_modified.unwrap_or(body.refreshed_at);
        let ttl_grace = ChronoDuration::seconds(body.ttl_secs.min(MAX_LOCK_TTL_SECS).cast_signed())
            + ChronoDuration::from_std(self.orphan_age).unwrap_or(ChronoDuration::zero());
        let margin = ChronoDuration::seconds(
            body.recovery_margin_secs
                .min(MAX_RECOVERY_MARGIN_SECS)
                .cast_signed(),
        );
        if Utc::now() <= reference + ttl_grace.max(margin) {
            return;
        }

        if dry_run {
            info!(key, "LockJanitor: would reclaim expired lock");
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
    use uuid::Uuid;

    use angos_storage::{
        BoxedReader, ByteStream, ChildrenPage, ConditionalStore, Error as StorageError, Etag,
        MemoryObjectStore, MultipartUploadPage, ObjectMeta, ObjectStore, Page,
    };

    use crate::janitor::{BodyJanitor, LockJanitor};
    use crate::lock::storage::LockBody;

    fn lock_key(suffix: &str) -> String {
        format!(".tx-locks/{suffix}")
    }

    async fn write_lock(store: &MemoryObjectStore, suffix: &str, body: &LockBody) {
        let bytes = Bytes::from(serde_json::to_vec(body).expect("serialise"));
        store.put(&lock_key(suffix), bytes).await.expect("put");
    }

    fn expired_body() -> LockBody {
        LockBody {
            refreshed_at: Utc::now() - ChronoDuration::minutes(10),
            ttl_secs: 30,
            writer_nonce: Uuid::new_v4(),
            recovery_margin_secs: 0,
        }
    }

    fn fresh_body() -> LockBody {
        LockBody {
            refreshed_at: Utc::now(),
            ttl_secs: 30,
            writer_nonce: Uuid::new_v4(),
            recovery_margin_secs: 0,
        }
    }

    /// A ttl-30 body refreshed `refreshed_secs_ago` seconds ago declaring
    /// `margin_secs` as its recovery margin.
    fn margin_body(refreshed_secs_ago: i64, margin_secs: u64) -> LockBody {
        LockBody {
            refreshed_at: Utc::now() - ChronoDuration::seconds(refreshed_secs_ago),
            ttl_secs: 30,
            writer_nonce: Uuid::new_v4(),
            recovery_margin_secs: margin_secs,
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
        janitor.sweep(false).await;

        let after = store.head(&lock_key("00/cold-key")).await;
        assert!(matches!(after, Err(StorageError::NotFound)));
    }

    /// The dry-run mirror of `expired_lock_is_reclaimed`: an expired lock that a
    /// real sweep would reclaim is left untouched when `sweep(true)` lists only.
    #[tokio::test]
    async fn expired_lock_is_listed_not_reclaimed_under_dry_run() {
        let store = Arc::new(MemoryObjectStore::new());
        write_lock(&store, "00/cold-key", &expired_body()).await;

        let janitor = build_janitor(store.clone() as Arc<dyn ObjectStore>);
        janitor.sweep(true).await;

        store
            .head(&lock_key("00/cold-key"))
            .await
            .expect("dry-run must leave the expired lock in place");
    }

    #[tokio::test]
    async fn fresh_lock_is_preserved() {
        let store = Arc::new(MemoryObjectStore::new());
        write_lock(&store, "00/hot-key", &fresh_body()).await;

        let janitor = build_janitor(store.clone() as Arc<dyn ObjectStore>);
        janitor.sweep(false).await;

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
        janitor.sweep(false).await;

        assert!(matches!(
            store.head(&lock_key("00/expired")).await,
            Err(StorageError::NotFound)
        ));
        store
            .head(&lock_key("01/fresh"))
            .await
            .expect("fresh sibling untouched");
    }

    /// A stalled long-hold holder is protected by its declared margin: 600s of
    /// silence is past ttl + `orphan_age` (30s + 60s) but inside the 3600s margin,
    /// so no replica's janitor may reclaim it yet.
    #[tokio::test]
    async fn margin_protected_lock_survives_past_ttl_and_orphan_age() {
        let store = Arc::new(MemoryObjectStore::new());
        write_lock(&store, "00/maintenance", &margin_body(600, 3600)).await;

        let janitor = build_janitor(store.clone() as Arc<dyn ObjectStore>);
        janitor.sweep(false).await;

        store
            .head(&lock_key("00/maintenance"))
            .await
            .expect("a lock inside its declared recovery margin survives");
    }

    /// Once the declared margin has elapsed the lock is reclaimed: 600s of
    /// silence exceeds max(ttl 30 + `orphan_age` 60, margin 90).
    #[tokio::test]
    async fn margin_elapsed_lock_is_reclaimed() {
        let store = Arc::new(MemoryObjectStore::new());
        write_lock(&store, "00/maintenance", &margin_body(600, 90)).await;

        let janitor = build_janitor(store.clone() as Arc<dyn ObjectStore>);
        janitor.sweep(false).await;

        assert!(matches!(
            store.head(&lock_key("00/maintenance")).await,
            Err(StorageError::NotFound)
        ));
    }

    /// A 1.3-written body has no margin field: it parses via the serde default
    /// and is reclaimed at ttl + `orphan_age` exactly as before.
    #[tokio::test]
    async fn legacy_body_without_margin_reclaimed_at_ttl_plus_orphan_age() {
        let store = Arc::new(MemoryObjectStore::new());
        let refreshed_at = Utc::now() - ChronoDuration::minutes(10);
        let json = format!(
            r#"{{"refreshed_at":"{}","ttl_secs":30,"writer_nonce":"{}"}}"#,
            refreshed_at.to_rfc3339(),
            Uuid::new_v4()
        );
        store
            .put(&lock_key("00/legacy"), Bytes::from(json))
            .await
            .expect("plant legacy body");

        let janitor = build_janitor(store.clone() as Arc<dyn ObjectStore>);
        janitor.sweep(false).await;

        assert!(matches!(
            store.head(&lock_key("00/legacy")).await,
            Err(StorageError::NotFound)
        ));
    }

    /// An absurd margin must neither panic the sweep nor park a dead lock
    /// forever: it is clamped to seven days, past which the lock is reclaimed.
    #[tokio::test]
    async fn absurd_margin_is_clamped_not_honored_forever() {
        let store = Arc::new(MemoryObjectStore::new());
        write_lock(&store, "00/recent", &margin_body(600, u64::MAX)).await;
        write_lock(&store, "00/ancient", &margin_body(8 * 24 * 3600, u64::MAX)).await;

        let janitor = build_janitor(store.clone() as Arc<dyn ObjectStore>);
        janitor.sweep(false).await;

        store
            .head(&lock_key("00/recent"))
            .await
            .expect("a recently-refreshed holder survives even an absurd margin");
        assert!(
            matches!(
                store.head(&lock_key("00/ancient")).await,
                Err(StorageError::NotFound)
            ),
            "eight days of silence exceeds the clamped seven-day margin"
        );
    }

    #[tokio::test]
    async fn malformed_body_is_skipped() {
        let store = Arc::new(MemoryObjectStore::new());
        store
            .put(&lock_key("00/garbage"), Bytes::from_static(b"not json"))
            .await
            .expect("put");

        let janitor = build_janitor(store.clone() as Arc<dyn ObjectStore>);
        janitor.sweep(false).await;

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
        async fn create_upload(&self, key: &str) -> Result<(), StorageError> {
            self.inner.create_upload(key).await
        }
        async fn write_upload(
            &self,
            key: &str,
            body: ByteStream,
            len: Option<u64>,
        ) -> Result<u64, StorageError> {
            self.inner.write_upload(key, body, len).await
        }
        async fn complete_upload(&self, key: &str) -> Result<(), StorageError> {
            self.inner.complete_upload(key).await
        }
        async fn abort_upload(&self, key: &str) -> Result<(), StorageError> {
            self.inner.abort_upload(key).await
        }
        async fn list_multipart_uploads(
            &self,
            key_marker: Option<&str>,
            upload_id_marker: Option<&str>,
        ) -> Result<MultipartUploadPage, StorageError> {
            self.inner
                .list_multipart_uploads(key_marker, upload_id_marker)
                .await
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
        janitor.sweep(false).await;

        inner
            .head(&lock_key("00/raced"))
            .await
            .expect("expired lock survives a racing fresh-acquire");
    }

    /// `MemoryObjectStore` returns `last_modified: None` from `head`, so the
    /// `BodyJanitor` age gate never fires over it. This wrapper backdates every
    /// `head`'s `last_modified` by a fixed amount so a planted orphan body
    /// qualifies as old enough, exercising the real age path without an FS or S3
    /// backend. Every other call delegates unchanged, so the janitor sees the
    /// same bare-named children a real backend returns.
    #[derive(Debug)]
    struct AgedObjectStore {
        inner: Arc<MemoryObjectStore>,
        backdate: ChronoDuration,
    }

    impl AgedObjectStore {
        fn new(inner: Arc<MemoryObjectStore>, backdate: ChronoDuration) -> Self {
            Self { inner, backdate }
        }
    }

    #[async_trait]
    impl ObjectStore for AgedObjectStore {
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
            m.last_modified = Some(Utc::now() - self.backdate);
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
        async fn create_upload(&self, key: &str) -> Result<(), StorageError> {
            self.inner.create_upload(key).await
        }
        async fn write_upload(
            &self,
            key: &str,
            body: ByteStream,
            len: Option<u64>,
        ) -> Result<u64, StorageError> {
            self.inner.write_upload(key, body, len).await
        }
        async fn complete_upload(&self, key: &str) -> Result<(), StorageError> {
            self.inner.complete_upload(key).await
        }
        async fn abort_upload(&self, key: &str) -> Result<(), StorageError> {
            self.inner.abort_upload(key).await
        }
        async fn list_multipart_uploads(
            &self,
            key_marker: Option<&str>,
            upload_id_marker: Option<&str>,
        ) -> Result<MultipartUploadPage, StorageError> {
            self.inner
                .list_multipart_uploads(key_marker, upload_id_marker)
                .await
        }
    }

    /// `BodyJanitor` dry-run lists an old orphan body prefix without deleting it;
    /// a committing sweep over the same planted orphan then reaps it. The orphan
    /// is a `.tx-bodies/<uuid>/staged/0` object with no `.tx-log/<uuid>.json`
    /// intent, backdated past the orphan age via `AgedObjectStore`.
    #[tokio::test]
    async fn body_janitor_dry_run_lists_then_commit_reaps_orphan() {
        let inner = Arc::new(MemoryObjectStore::new());
        let tx_id = Uuid::new_v4();
        let child = format!(".tx-bodies/{tx_id}/staged/0");
        inner
            .put(&child, Bytes::from_static(b"orphan body"))
            .await
            .expect("plant orphan body");

        let aged = Arc::new(AgedObjectStore::new(
            inner.clone(),
            ChronoDuration::hours(2),
        ));
        let janitor = BodyJanitor::builder(aged.clone() as Arc<dyn ObjectStore>)
            .orphan_age(Duration::from_secs(1))
            .build();

        janitor.sweep(true).await;
        inner
            .head(&child)
            .await
            .expect("dry-run must leave the orphan body in place");

        janitor.sweep(false).await;
        assert!(
            matches!(inner.head(&child).await, Err(StorageError::NotFound)),
            "a committing sweep must reap the aged orphan body prefix"
        );
    }
}
