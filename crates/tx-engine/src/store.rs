//! Storage façade — the single seam subsystems use for storage.
//!
//! [`Store`] composes the storage handles a subsystem needs (the floor
//! [`ObjectStore`], plus optional upload-session and presign capabilities)
//! together with the [`TransactionExecutor`] that commits
//! coordinated writes. Subsystems (`metadata_store`, `blob_store`,
//! `job_store`) hold a single `Arc<Store>` for their per-operation storage
//! access; the concrete backends are constructed from `angos_storage` at the
//! configuration seam.
//!
//! The façade stays domain-agnostic: it speaks only `String` keys and `Bytes`
//! bodies. Registry domain types (`Digest`, `LinkKind`, OCI hashing, serde)
//! stay in the registry.
//!
//! # Error surface
//!
//! Plain reads, non-transactional writes, and the upload primitives return
//! [`StorageError`] so callers keep matching [`StorageError::NotFound`]
//! directly. The coordinated helpers — [`Store::update`] and
//! [`Store::update_with_payload`] — return the engine [`Error`] (carrying
//! `Conflict`/`Precondition`).

use std::{
    fmt,
    future::Future,
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use bytes::Bytes;

use angos_storage::{
    BoxedReader, ByteStream, ChildrenPage, Error as StorageError, MultipartUploadPage, ObjectMeta,
    ObjectStore, Page, PresignedStore, UploadSession,
};

use crate::{
    error::Error,
    executor::{Outcome, TransactionExecutor, execute_with_retry_payload},
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
/// executor. Construct via [`Store::builder`].
#[derive(Clone)]
pub struct Store {
    object: Arc<dyn ObjectStore>,
    presign: Option<Arc<dyn PresignedStore>>,
    executor: Arc<dyn TransactionExecutor>,
}

impl fmt::Debug for Store {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Store").finish_non_exhaustive()
    }
}

impl Store {
    /// Return a builder for constructing a [`Store`].
    #[must_use]
    pub fn builder() -> StoreBuilder {
        StoreBuilder::default()
    }

    /// The transaction executor backing this store. Wiring code uses it to
    /// reach the lock primitive and conditional store for the recovery loop
    /// and lock janitor.
    #[must_use]
    pub fn executor(&self) -> &Arc<dyn TransactionExecutor> {
        &self.executor
    }

    /// The floor object store. Escape hatch for code that must hand an
    /// `Arc<dyn ObjectStore>` to a helper (e.g. concurrent buffered reads, or
    /// test wrappers that intercept the storage seam).
    #[must_use]
    pub fn object_store(&self) -> &Arc<dyn ObjectStore> {
        &self.object
    }

    // ── Reads (passthrough; surface `StorageError::NotFound` directly) ──────

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

    // ── Non-transactional writes (session records, best-effort cleanup) ─────

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

    // ── Transactions ────────────────────────────────────────────────────────

    /// Execute a pre-built [`Transaction`] once (no retry).
    ///
    /// # Errors
    ///
    /// Propagates any [`Error`] from the executor (`Conflict`, `Precondition`,
    /// `Storage`, …).
    pub async fn execute(&self, tx: Transaction) -> Result<Outcome, Error> {
        self.executor.execute(tx).await
    }

    // ── Read-for-update ──────────────────────────────────────────────────────

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

    // ── Upload lifecycle ─────────────────────────────────────────────────────

    /// Begin a fresh upload at `key`.
    ///
    /// # Errors
    ///
    /// Any backend failure starting the session.
    pub async fn create_upload(&self, key: &str) -> Result<UploadSession, StorageError> {
        self.object.create_upload(key).await
    }

    /// Stream `len` bytes from `body` into `session`, parking sub-part
    /// remainders at `staged_dir` (S3 only). The session is mutated in place.
    ///
    /// # Errors
    ///
    /// Any backend failure writing the chunk.
    pub async fn write_upload(
        &self,
        session: &mut UploadSession,
        staged_dir: &str,
        body: ByteStream,
        len: u64,
    ) -> Result<(), StorageError> {
        self.object
            .write_upload(session, staged_dir, body, len)
            .await
    }

    /// Discard `session` and any backend state it owns without producing an
    /// object.
    ///
    /// # Errors
    ///
    /// Any backend failure aborting the session.
    pub async fn abort_upload(
        &self,
        session: UploadSession,
        staged_dir: &str,
    ) -> Result<(), StorageError> {
        self.object.abort_upload(session, staged_dir).await
    }

    /// Best-effort abort of any in-flight backend upload state for `key`.
    ///
    /// # Errors
    ///
    /// Any backend failure during the abort.
    pub async fn abort_pending_uploads(&self, key: &str) -> Result<(), StorageError> {
        self.object.abort_pending_uploads(key).await
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

    /// Abort a single in-flight multipart upload by `key` + `upload_id`.
    ///
    /// # Errors
    ///
    /// Any backend abort failure.
    pub async fn abort_multipart_upload(
        &self,
        key: &str,
        upload_id: &str,
    ) -> Result<(), StorageError> {
        self.object.abort_multipart_upload(key, upload_id).await
    }

    /// Run only the backend completion step (S3 multipart-complete; FS no-op)
    /// so the assembled object lands at `session.key`.
    ///
    /// This is a primitive: promotion to the canonical path and deletion of
    /// any session-record keys are left to the caller, which composes them
    /// into an engine [`Transaction`] (via [`Store::execute`]) so the moves
    /// and deletes commit atomically — often merged with other mutations
    /// (e.g. a blob-index grant). The upload-session orchestration that drives
    /// this lives in the registry, not the engine.
    ///
    /// # Errors
    ///
    /// Any backend completion failure.
    pub async fn complete_upload(
        &self,
        session: UploadSession,
        staged_dir: &str,
    ) -> Result<(), StorageError> {
        self.object.complete_upload(session, staged_dir).await
    }

    // ── Presign / prune ──────────────────────────────────────────────────────

    /// Generate a presigned download URL for `key`, or `Ok(None)` when no
    /// presign backend is wired.
    ///
    /// # Errors
    ///
    /// Propagates the backend [`StorageError`] from the presign operation.
    pub async fn presigned_get(
        &self,
        key: &str,
        ttl: Duration,
        content_type: Option<&str>,
    ) -> Result<Option<String>, StorageError> {
        match &self.presign {
            Some(presign) => Ok(Some(presign.presign_get(key, ttl, content_type).await?)),
            None => Ok(None),
        }
    }

    /// Whether a presign backend is wired (S3 has one; FS does not).
    #[must_use]
    pub fn supports_presign(&self) -> bool {
        self.presign.is_some()
    }
}

/// Builder for [`Store`]. `object` and `executor` are required; the presign
/// capability is optional and reflects what the underlying backend supports.
#[derive(Default)]
pub struct StoreBuilder {
    object: Option<Arc<dyn ObjectStore>>,
    presign: Option<Arc<dyn PresignedStore>>,
    executor: Option<Arc<dyn TransactionExecutor>>,
}

impl StoreBuilder {
    /// Set the floor object store (required). [`ObjectStore`] carries the
    /// upload-session lifecycle too, so this single handle wires both.
    #[must_use]
    pub fn object(mut self, object: Arc<dyn ObjectStore>) -> Self {
        self.object = Some(object);
        self
    }

    /// Set the presign backend. Enables [`Store::presigned_get`].
    #[must_use]
    pub fn presign(mut self, presign: Arc<dyn PresignedStore>) -> Self {
        self.presign = Some(presign);
        self
    }

    /// Set the transaction executor (required).
    #[must_use]
    pub fn executor(mut self, executor: Arc<dyn TransactionExecutor>) -> Self {
        self.executor = Some(executor);
        self
    }

    /// Consume the builder and produce the [`Store`].
    ///
    /// # Errors
    ///
    /// Returns [`Error::Build`] when `store` or `executor` is missing.
    pub fn build(self) -> Result<Store, Error> {
        let object = self
            .object
            .ok_or_else(|| Error::Build("Store requires an object store".to_string()))?;
        let executor = self
            .executor
            .ok_or_else(|| Error::Build("Store requires an executor".to_string()))?;
        Ok(Store {
            object,
            presign: self.presign,
            executor,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;

    use angos_storage::{Error as StorageError, MemoryObjectStore};

    use crate::{
        executor::locked::LockedExecutor,
        lock::{primitive::Lock, storage::memory::MemoryLockStorage},
        store::Store,
        transaction::{Mutation, Transaction},
    };

    fn store_over(backend: Arc<MemoryObjectStore>) -> Store {
        let lock = Arc::new(
            Lock::builder()
                .storage(Arc::new(MemoryLockStorage::new()))
                .build()
                .expect("lock"),
        );
        let executor = Arc::new(
            LockedExecutor::builder()
                .store(backend.clone())
                .lock(lock)
                .build()
                .expect("executor"),
        );
        Store::builder()
            .object(backend)
            .executor(executor)
            .build()
            .expect("store")
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
        let session = store.create_upload("upload/u1").await.expect("create");
        store
            .put("upload-sessions/u1.json", Bytes::from_static(b"{}"))
            .await
            .expect("session record");

        // Primitive: backend completion lands the assembled object at the
        // session key. The caller (registry) then composes the promotion and
        // record cleanup into a single engine transaction.
        store
            .complete_upload(session, "staging/u1")
            .await
            .expect("complete");
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

        // Assembled object promoted to the canonical key…
        assert!(store.get("blob-data/abc").await.is_ok());
        // …source upload key gone, and the session record deleted.
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
