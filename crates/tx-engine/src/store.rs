//! Storage façade: the single seam subsystems use for storage.
//!
//! [`Store`] composes the storage handles a subsystem needs (an
//! [`ObjectStore`], the CRUD floor plus the upload-session lifecycle)
//! together with the [`TransactionExecutor`] that commits coordinated writes.
//! [`Store::new`] is the engine's construction seam: it builds the lock
//! primitive and selects the executor from operator-level inputs, so
//! subsystems (`metadata_store`, `job_store`) hold a single `Arc<Store>` and
//! never instantiate locks or executors directly. [`Store::maintenance`]
//! returns the engine's background loops (recovery, body janitor, lock
//! janitor) wired over the same primitives; the caller spawns it.
//!
//! The façade stays domain-agnostic: it speaks only `String` keys and `Bytes`
//! bodies. Registry domain types (`Digest`, `LinkKind`, OCI hashing, serde)
//! stay in the registry.
//!
//! # Error surface
//!
//! Plain reads, non-transactional writes, and the upload primitives return
//! [`StorageError`] so callers keep matching [`StorageError::NotFound`]
//! directly. The coordinated helpers ([`Store::update`] and
//! [`Store::update_with_payload`]) return the engine [`Error`] (carrying
//! `Conflict`/`Precondition`).

use std::{
    fmt,
    future::Future,
    sync::{Arc, Mutex, PoisonError},
};

use bytes::Bytes;
use tokio_util::sync::CancellationToken;
use tracing::info;

use angos_storage::{
    BoxedReader, ByteStream, ChildrenPage, ConditionalStore, Error as StorageError,
    MultipartUploadPage, ObjectMeta, ObjectStore, Page,
};

#[cfg(feature = "redis")]
use crate::lock::storage::redis::RedisLockStorage;
use crate::{
    error::Error,
    executor::{
        Outcome, TransactionExecutor, cas::CasExecutor, execute_with_retry_payload,
        locked::LockedExecutor,
    },
    janitor::{BodyJanitor, LockJanitor},
    lock::{
        LockStrategy,
        primitive::Lock,
        storage::{LockStorage, memory::MemoryLockStorage, s3::S3LockStorage},
    },
    recovery::RecoveryLoop,
    transaction::{Mutation, Transaction},
};

/// A point-in-time read used to build a read-for-update transaction.
///
/// `body` is the raw bytes observed for `key`; the engine derives a
/// content-fingerprint from them when a *present* snapshot is folded into a
/// transaction's read set by [`Store::update`]. An absent key yields
/// `present: false` with an empty `body` and is *not* added to the read set
/// (the executor treats a read of an absent key as an unconditional conflict);
/// callers express create-only intent with a [`Mutation::PutIfAbsent`] instead.
#[derive(Clone, Debug)]
pub struct Snapshot {
    /// The storage key that was read.
    pub key: String,
    /// The bytes observed at `key` (empty when absent).
    pub body: Bytes,
    /// Whether the key existed at read time.
    pub present: bool,
}

/// Storage façade composing storage capabilities with the transaction
/// executor. Construct via [`Store::new`].
#[derive(Clone)]
pub struct Store {
    object: Arc<dyn ObjectStore>,
    executor: Arc<dyn TransactionExecutor>,
    lock: Arc<Lock>,
    conditional: Option<Arc<dyn ConditionalStore>>,
}

impl fmt::Debug for Store {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Store").finish_non_exhaustive()
    }
}

impl Store {
    /// Build a [`Store`] from operator-level inputs.
    ///
    /// This is the engine's single construction seam. Subsystems call it with
    /// their [`ObjectStore`] (and optional [`ConditionalStore`]) plus the
    /// operator's [`LockStrategy`]; they never instantiate `Lock`,
    /// `LockStorage`, or any executor type directly.
    ///
    /// - `conditional` must be `Some(...)` only when the backend's support for
    ///   the full conditional set (`put_if_absent`, `put_if_match`,
    ///   `delete_if_match`, all surfacing `ETag`s) has been verified, via
    ///   [`crate::probe::probe_cas_support`] or an operator declaration. Its
    ///   presence selects the CAS executor; otherwise the engine falls back to
    ///   a locked executor.
    /// - For [`LockStrategy::S3`] the caller must provide `s3_lock_store` (a
    ///   [`ConditionalStore`] tuned for short-lived lock requests, on a
    ///   provider satisfying the same conditional contract).
    ///
    /// # Errors
    ///
    /// Returns [`Error::Build`] when `LockStrategy::S3` is selected without an
    /// `s3_lock_store`, when the `redis` feature is not enabled and Redis is
    /// selected, or when the underlying lock or executor builder rejects its
    /// inputs.
    pub fn new(
        object: Arc<dyn ObjectStore>,
        conditional: Option<Arc<dyn ConditionalStore>>,
        lock_strategy: LockStrategy,
        s3_lock_store: Option<Arc<dyn ConditionalStore>>,
    ) -> Result<Self, Error> {
        // Each arm yields the lock-object storage plus a `LockBuilder` primed
        // with the per-strategy tuning carried in the strategy config. The
        // tuning is threaded directly into the builder (never stored as a
        // Config field), and the Lock is built once after the match.
        let lock_builder = match lock_strategy {
            LockStrategy::Memory => {
                let storage: Arc<dyn LockStorage> = Arc::new(MemoryLockStorage::new());
                // Memory backend: keep builder defaults.
                Lock::builder(storage)
            }
            #[cfg(feature = "redis")]
            LockStrategy::Redis(config) => {
                let storage: Arc<dyn LockStorage> =
                    Arc::new(RedisLockStorage::new(&config).map_err(|e| {
                        Error::Build(format!("failed to build Redis lock storage: {e}"))
                    })?);
                // Redis TTL is enforced natively by the storage; only the retry
                // tuning is threaded into the lock primitive.
                Lock::builder(storage)
                    .max_retries(config.max_retries)
                    .retry_delay_ms(config.retry_delay_ms)
            }
            LockStrategy::S3(config) => {
                let lock_store = s3_lock_store.ok_or_else(|| {
                    Error::Build("S3 lock strategy requires an S3 conditional store".to_string())
                })?;
                let storage: Arc<dyn LockStorage> = Arc::new(S3LockStorage::new(lock_store));
                Lock::builder(storage)
                    .ttl_secs(config.ttl_secs)
                    .max_retries(config.max_retries)
                    .retry_delay_ms(config.retry_delay_ms)
                    .max_hold_secs(config.max_hold_secs)
            }
        };

        let lock = Arc::new(
            lock_builder
                .build()
                .map_err(|e| Error::Build(format!("failed to build lock: {e}")))?,
        );
        // Logged alongside the executor choice so operators are not misled
        // into reading the lock strategy as the coordination path: both
        // executors share this backend.
        let lock_backend = lock.storage_label();

        // The conditional store is retained so `maintenance` replays stale
        // intents and reaps cold locks with the same primitives the executor
        // used on the healthy path.
        if let Some(cs) = conditional {
            let executor: Arc<dyn TransactionExecutor> =
                Arc::new(CasExecutor::builder(cs.clone(), lock.clone()).build());
            info!(
                executor = "cas",
                lock_backend, "transactional engine executor selected"
            );
            return Ok(Self {
                object,
                executor,
                lock,
                conditional: Some(cs),
            });
        }

        let executor: Arc<dyn TransactionExecutor> =
            Arc::new(LockedExecutor::builder(object.clone(), lock.clone()).build());
        info!(
            executor = "locked",
            lock_backend, "transactional engine executor selected"
        );
        Ok(Self {
            object,
            executor,
            lock,
            conditional: None,
        })
    }

    /// Label of the lock-object backend in use (`"memory"`, `"redis"`, or
    /// `"s3"`). `"memory"` means the lock cannot coordinate across processes.
    #[must_use]
    pub fn lock_backend(&self) -> &'static str {
        self.lock.storage_label()
    }

    /// The engine maintenance loops (recovery, body janitor, lock janitor)
    /// joined into a single future that runs until `cancellation` fires. The
    /// caller decides where it runs (typically `tokio::spawn`); spawn it once
    /// per process per shared [`ObjectStore`].
    ///
    /// The loops use the engine's default intervals and coordinate through
    /// the executor's own primitives: recovery takes ownership of stale
    /// intents via the engine lock and, on CAS deployments, replays with the
    /// same conditional store the healthy path uses. The lock janitor runs
    /// only on CAS deployments: lock objects exist solely on CAS-capable
    /// backends, and it reclaims them with conditional deletes.
    pub fn maintenance(
        &self,
        cancellation: CancellationToken,
    ) -> impl Future<Output = ()> + Send + 'static {
        let mut recovery = RecoveryLoop::builder(self.object.clone())
            .lock(self.lock.clone())
            .cancellation(cancellation.clone());
        if let Some(cs) = &self.conditional {
            recovery = recovery.conditional_store(cs.clone());
        }
        let recovery = recovery.build();

        let body_janitor = BodyJanitor::builder(self.object.clone())
            .cancellation(cancellation.clone())
            .build();

        let lock_janitor = self
            .conditional
            .clone()
            .map(|cs| LockJanitor::builder(cs).cancellation(cancellation).build());

        async move {
            let lock_janitor = async move {
                if let Some(janitor) = lock_janitor {
                    janitor.run().await;
                }
            };
            tokio::join!(recovery.run(), body_janitor.run(), lock_janitor);
        }
    }

    /// The transaction executor backing this store. Subsystems use it for
    /// single-shot transaction execution and engine-lock sessions.
    #[must_use]
    pub fn executor(&self) -> &Arc<dyn TransactionExecutor> {
        &self.executor
    }

    /// The composed object store. Escape hatch for code that must hand an
    /// `Arc<dyn ObjectStore>` to a helper (e.g. concurrent buffered reads, the
    /// upload-session lifecycle, or test wrappers that intercept the storage
    /// seam).
    #[must_use]
    pub fn object_store(&self) -> &Arc<dyn ObjectStore> {
        &self.object
    }

    // Reads (passthrough; surface `StorageError::NotFound` directly)

    /// Read the full object body into memory.
    ///
    /// # Errors
    ///
    /// [`StorageError::NotFound`] when `key` is absent; otherwise the backend
    /// [`StorageError`].
    pub async fn get(&self, key: &str) -> Result<Vec<u8>, StorageError> {
        self.object.get(key).await
    }

    /// Open a streaming reader over the object body. The returned `u64` is the
    /// total object size, not the remaining length after `offset`.
    ///
    /// # Errors
    ///
    /// [`StorageError::NotFound`] when `key` is absent; otherwise the backend
    /// [`StorageError`].
    pub async fn get_stream(
        &self,
        key: &str,
        offset: Option<u64>,
    ) -> Result<(BoxedReader, u64), StorageError> {
        self.object.get_stream(key, offset).await
    }

    /// Return the object's size and (when available) `ETag`/last-modified.
    ///
    /// # Errors
    ///
    /// [`StorageError::NotFound`] when `key` is absent; otherwise the backend
    /// [`StorageError`].
    pub async fn head(&self, key: &str) -> Result<ObjectMeta, StorageError> {
        self.object.head(key).await
    }

    /// Flat-recursive enumeration of up to `n` keys under `prefix`.
    ///
    /// # Errors
    ///
    /// Propagates the backend [`StorageError`].
    pub async fn list(
        &self,
        prefix: &str,
        n: u16,
        token: Option<String>,
    ) -> Result<Page<String>, StorageError> {
        self.object.list(prefix, n, token).await
    }

    /// One-level enumeration of children under `prefix`.
    ///
    /// # Errors
    ///
    /// Propagates the backend [`StorageError`].
    pub async fn list_children(
        &self,
        prefix: &str,
        n: u16,
        token: Option<String>,
        start_after: Option<String>,
    ) -> Result<ChildrenPage, StorageError> {
        self.object
            .list_children(prefix, n, token, start_after)
            .await
    }

    // Non-transactional writes (session records, best-effort cleanup)

    /// Write `data` to `key`, replacing any existing object.
    ///
    /// # Errors
    ///
    /// Propagates the backend [`StorageError`].
    pub async fn put(&self, key: &str, data: Bytes) -> Result<(), StorageError> {
        self.object.put(key, data).await
    }

    /// Delete `key`. Missing key counts as success.
    ///
    /// # Errors
    ///
    /// Propagates the backend [`StorageError`].
    pub async fn delete(&self, key: &str) -> Result<(), StorageError> {
        self.object.delete(key).await
    }

    /// Delete every object whose key starts with `prefix`.
    ///
    /// # Errors
    ///
    /// Propagates the backend [`StorageError`].
    pub async fn delete_prefix(&self, prefix: &str) -> Result<(), StorageError> {
        self.object.delete_prefix(prefix).await
    }

    /// Server-side copy from `source` to `destination`.
    ///
    /// # Errors
    ///
    /// Propagates the backend [`StorageError`].
    pub async fn copy(&self, source: &str, destination: &str) -> Result<(), StorageError> {
        self.object.copy(source, destination).await
    }

    // Transactions

    /// Execute a pre-built [`Transaction`] once (no retry).
    ///
    /// # Errors
    ///
    /// Propagates any [`Error`] from the executor (`Conflict`, `Precondition`,
    /// `Storage`, etc.).
    pub async fn execute(&self, tx: Transaction) -> Result<Outcome, Error> {
        self.executor.execute(tx).await
    }

    // Read-for-update

    /// Read `key` as a [`Snapshot`]. A missing key is not an error: it yields
    /// `present: false` with an empty body.
    ///
    /// # Errors
    ///
    /// Propagates the backend [`StorageError`] (other than `NotFound`, which is
    /// folded into `present: false`).
    pub async fn read_for_update(&self, key: &str) -> Result<Snapshot, StorageError> {
        match self.object.get(key).await {
            Ok(body) => Ok(Snapshot {
                key: key.to_string(),
                body: Bytes::from(body),
                present: true,
            }),
            Err(StorageError::NotFound) => Ok(Snapshot {
                key: key.to_string(),
                body: Bytes::new(),
                present: false,
            }),
            Err(e) => Err(e),
        }
    }

    /// Read several keys as [`Snapshot`]s, preserving input order.
    ///
    /// # Errors
    ///
    /// Propagates the first backend [`StorageError`] encountered.
    pub async fn read_many_for_update(
        &self,
        keys: &[String],
    ) -> Result<Vec<Snapshot>, StorageError> {
        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            out.push(self.read_for_update(key).await?);
        }
        Ok(out)
    }

    /// Read-modify-write helper that owns the re-read + conflict-retry loop.
    ///
    /// On each attempt the store reads fresh [`Snapshot`]s for `keys`, folds
    /// them into the transaction's read set, and calls `map` with those
    /// snapshots to produce the mutations to apply. `map` is async so callers
    /// may perform additional ad-hoc reads (e.g. derived shard keys) while
    /// building the mutation set. The transaction is then committed; on
    /// [`Error::Conflict`] or [`Error::Precondition`] the whole attempt is
    /// retried (up to `max_attempts` extra times) against a fresh read.
    ///
    /// # Errors
    ///
    /// Returns the first non-retriable error from `map` or the executor, or
    /// [`Error::Conflict`] once `max_attempts` retriable conflicts are
    /// exhausted.
    pub async fn update<F, Fut>(
        &self,
        keys: &[String],
        mut map: F,
        max_attempts: u32,
    ) -> Result<Outcome, Error>
    where
        F: FnMut(Vec<Snapshot>) -> Fut + Send,
        Fut: Future<Output = Result<Vec<Mutation>, Error>> + Send,
    {
        let (outcome, ()) = self
            .update_with_payload(
                keys,
                move |snaps| {
                    let fut = map(snaps);
                    async move { fut.await.map(|m| (m, ())) }
                },
                max_attempts,
            )
            .await?;
        Ok(outcome)
    }

    /// Like [`Store::update`], but `map` also threads a per-attempt payload
    /// `T` out of the retry loop alongside the mutations. Used by callers that
    /// need the value they just wrote (e.g. the updated metadata record).
    ///
    /// # Errors
    ///
    /// See [`Store::update`].
    ///
    /// # Panics
    ///
    /// Panics only if the internal mutex guarding `map` is poisoned. The mutex
    /// is created locally, never shared, and the guard is dropped before any
    /// `.await`, so poisoning cannot occur in practice.
    pub async fn update_with_payload<F, Fut, T>(
        &self,
        keys: &[String],
        map: F,
        max_attempts: u32,
    ) -> Result<(Outcome, T), Error>
    where
        F: FnMut(Vec<Snapshot>) -> Fut + Send,
        Fut: Future<Output = Result<(Vec<Mutation>, T), Error>> + Send,
        T: Send,
    {
        // `execute_with_retry_payload` takes a `FnMut() -> Fut` whose `Fut`
        // must not borrow the closure's captures. The caller's `map` needs
        // `&mut`, which would make the build closure `FnMut` and let that
        // mutable borrow escape into the returned future. Sharing `map` through
        // a `Mutex` lets the closure capture it by shared reference (so it is
        // `Fn`); `&Mutex<F>` is `Send + Sync` when `F: Send`, so the future
        // still satisfies the helper's `Fut: Send` bound. The lock guard is
        // dropped before the `.await`, so it is never held across a suspension
        // point.
        let map = Mutex::new(map);
        let build = || async {
            let snaps = self.read_many_for_update(keys).await?;
            let mut builder = Transaction::builder();
            for snap in &snaps {
                // Only present keys enter the read set: the executor treats a
                // read of an absent key as an unconditional conflict, so
                // create-only protection is expressed by the caller's mutation
                // (`PutIfAbsent`), not by a read fingerprint.
                if snap.present {
                    builder = builder.read(snap.key.clone(), snap.body.clone());
                }
            }
            let map_fut = {
                let mut map = map.lock().unwrap_or_else(PoisonError::into_inner);
                map(snaps)
            };
            let (mutations, payload) = map_fut.await?;
            for mutation in mutations {
                builder = builder.mutation(mutation);
            }
            Ok((builder.build(), payload))
        };
        execute_with_retry_payload(self.executor.as_ref(), build, max_attempts).await
    }

    // Upload lifecycle

    /// Begin/clear a fresh upload at `key` (discards any leaked prior upload).
    ///
    /// # Errors
    ///
    /// Any backend failure starting the upload.
    pub async fn create_upload(&self, key: &str) -> Result<(), StorageError> {
        self.object.create_upload(key).await
    }

    /// Append `body` to the upload at `key`, returning the new total uploaded
    /// size. `Some(len)` declares an exact byte count (validated); `None`
    /// streams the body to EOF (a chunked request with no `Content-Length`).
    ///
    /// # Errors
    ///
    /// Any backend failure writing the chunk.
    pub async fn write_upload(
        &self,
        key: &str,
        body: ByteStream,
        len: Option<u64>,
    ) -> Result<u64, StorageError> {
        self.object.write_upload(key, body, len).await
    }

    /// Discard the upload at `key` and any backend state it owns without
    /// producing an object. Idempotent.
    ///
    /// # Errors
    ///
    /// Any backend failure aborting the upload.
    pub async fn abort_upload(&self, key: &str) -> Result<(), StorageError> {
        self.object.abort_upload(key).await
    }

    /// List in-flight multipart uploads store-wide, one page at a time
    /// (`key_marker`/`upload_id_marker` continue a prior page; `None` starts).
    ///
    /// A raw primitive: orphan detection (age thresholds, live-session checks)
    /// is the caller's job. FS/memory backends return an empty page.
    ///
    /// # Errors
    ///
    /// Any backend listing failure.
    pub async fn list_multipart_uploads(
        &self,
        key_marker: Option<&str>,
        upload_id_marker: Option<&str>,
    ) -> Result<MultipartUploadPage, StorageError> {
        self.object
            .list_multipart_uploads(key_marker, upload_id_marker)
            .await
    }

    /// Run only the backend completion step (S3 multipart-complete; FS no-op)
    /// so the assembled object lands at `key`.
    ///
    /// This is a primitive: promotion to the canonical path and deletion of
    /// any session-record keys are left to the caller, which composes them
    /// into an engine [`Transaction`] (via [`Store::execute`]) so the moves
    /// and deletes commit atomically, often merged with other mutations
    /// (e.g. a blob-index grant). The upload orchestration that drives this
    /// lives in the registry, not the engine.
    ///
    /// # Errors
    ///
    /// Any backend completion failure.
    pub async fn complete_upload(&self, key: &str) -> Result<(), StorageError> {
        self.object.complete_upload(key).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;

    use angos_storage::{Error as StorageError, MemoryObjectStore};

    use crate::{
        lock::LockStrategy,
        store::Store,
        transaction::{Mutation, Transaction},
    };

    fn store_over(backend: Arc<MemoryObjectStore>) -> Store {
        Store::new(backend, None, LockStrategy::Memory, None).expect("store")
    }

    #[tokio::test]
    async fn update_commits_mutations() {
        let backend = Arc::new(MemoryObjectStore::new());
        let store = store_over(backend);

        store
            .update(
                &["k".to_string()],
                |_snaps| async {
                    Ok(vec![Mutation::Put {
                        key: "k".to_string(),
                        body: Bytes::from_static(b"v"),
                        expected: None,
                    }])
                },
                3,
            )
            .await
            .expect("update");

        assert_eq!(store.get("k").await.expect("get"), b"v");
    }

    #[tokio::test]
    async fn update_observes_current_snapshot() {
        let backend = Arc::new(MemoryObjectStore::new());
        let store = store_over(backend);
        store
            .put("k", Bytes::from_static(b"v1"))
            .await
            .expect("seed");

        store
            .update(
                &["k".to_string()],
                |snaps| async move {
                    assert_eq!(snaps.len(), 1);
                    assert!(snaps[0].present);
                    assert_eq!(&snaps[0].body[..], b"v1");
                    Ok(vec![Mutation::Put {
                        key: "k".to_string(),
                        body: Bytes::from_static(b"v2"),
                        expected: None,
                    }])
                },
                3,
            )
            .await
            .expect("update");

        assert_eq!(store.get("k").await.expect("get"), b"v2");
    }

    #[tokio::test]
    async fn read_for_update_absent_key() {
        let backend = Arc::new(MemoryObjectStore::new());
        let store = store_over(backend);

        let snap = store.read_for_update("missing").await.expect("snapshot");
        assert!(!snap.present);
        assert!(snap.body.is_empty());
    }

    #[tokio::test]
    async fn update_with_payload_threads_value() {
        let backend = Arc::new(MemoryObjectStore::new());
        let store = store_over(backend);

        let (_outcome, payload) = store
            .update_with_payload(
                &["k".to_string()],
                |_snaps| async {
                    Ok((
                        vec![Mutation::Put {
                            key: "k".to_string(),
                            body: Bytes::from_static(b"v"),
                            expected: None,
                        }],
                        42u32,
                    ))
                },
                3,
            )
            .await
            .expect("update");

        assert_eq!(payload, 42);
        assert_eq!(store.get("k").await.expect("get"), b"v");
    }

    #[tokio::test]
    async fn complete_upload_then_promote_and_delete_via_transaction() {
        let backend = Arc::new(MemoryObjectStore::new());
        let store = store_over(backend);

        // Stand in for a session record + an assembled upload object.
        store.create_upload("upload/u1").await.expect("create");
        store
            .put("upload-sessions/u1.json", Bytes::from_static(b"{}"))
            .await
            .expect("session record");

        // Primitive: backend completion lands the assembled object at the
        // upload key. The caller (registry) then composes the promotion and
        // record cleanup into a single engine transaction.
        store.complete_upload("upload/u1").await.expect("complete");
        store
            .execute(
                Transaction::builder()
                    .mutation(Mutation::Move {
                        src: "upload/u1".to_string(),
                        dst: "blob-data/abc".to_string(),
                    })
                    .mutation(Mutation::Delete {
                        key: "upload-sessions/u1.json".to_string(),
                        expected: None,
                    })
                    .build(),
            )
            .await
            .expect("promote");

        // Assembled object promoted to the canonical key...
        assert!(store.get("blob-data/abc").await.is_ok());
        // ...source upload key gone, and the session record deleted.
        assert!(matches!(
            store.get("upload/u1").await,
            Err(StorageError::NotFound)
        ));
        assert!(matches!(
            store.get("upload-sessions/u1.json").await,
            Err(StorageError::NotFound)
        ));
    }
}
