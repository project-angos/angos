//! Storage façade — the single seam subsystems use for storage.
//!
//! [`Store`] composes the storage handles a subsystem needs (the floor
//! [`ObjectStore`], plus optional upload-session, presign, and FS-prune
//! capabilities) together with the [`TransactionExecutor`] that commits
//! coordinated writes. Subsystems (`metadata_store`, `blob_store`,
//! `job_store`) hold a single `Arc<Store>` and never reach for `angos_storage`
//! or `angos_s3_client` directly.
//!
//! The façade stays domain-agnostic: it speaks only `String` keys and `Bytes`
//! bodies. Registry domain types (`Digest`, `LinkKind`, OCI hashing, serde)
//! stay in the registry.
//!
//! # Error surface
//!
//! Plain reads, non-transactional writes, and the upload primitives return
//! [`StorageError`] so callers keep matching [`StorageError::NotFound`]
//! directly. The coordinated helpers — [`Store::update`],
//! [`Store::update_with_payload`], and [`Store::finalize_upload`] — return the
//! engine [`Error`] (carrying `Conflict`/`Precondition`).

use std::{fmt, future::Future, sync::Arc, time::Duration};

use bytes::Bytes;
use tracing::debug;

use angos_storage::{
    BoxedReader, ByteStream, ChildrenPage, Error as StorageError, ObjectMeta, ObjectStore, Page,
    PresignedStore, UploadSession, UploadSessionStore, fs::Backend as StorageFsBackend,
};

use crate::{
    error::Error,
    executor::{Outcome, TransactionExecutor},
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
    upload: Option<Arc<dyn UploadSessionStore>>,
    presign: Option<Arc<dyn PresignedStore>>,
    fs_prune: Option<Arc<StorageFsBackend>>,
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
    /// `Arc<dyn ObjectStore>` to a helper (e.g. concurrent buffered reads).
    #[must_use]
    pub fn object_store(&self) -> &Arc<dyn ObjectStore> {
        &self.object
    }

    /// The upload-session store, when one was wired. Escape hatch for code that
    /// must hand an `Arc<dyn UploadSessionStore>` to a helper (e.g. test
    /// wrappers that intercept the upload seam).
    #[must_use]
    pub fn upload_store(&self) -> Option<&Arc<dyn UploadSessionStore>> {
        self.upload.as_ref()
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
    pub async fn update_with_payload<F, Fut, T>(
        &self,
        keys: &[String],
        mut map: F,
        max_attempts: u32,
    ) -> Result<(Outcome, T), Error>
    where
        F: FnMut(Vec<Snapshot>) -> Fut + Send,
        Fut: Future<Output = Result<(Vec<Mutation>, T), Error>> + Send,
        T: Send,
    {
        let mut attempts = 0u32;
        loop {
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
            let (mutations, payload) = map(snaps).await?;
            for mutation in mutations {
                builder = builder.mutation(mutation);
            }
            match self.executor.execute(builder.build()).await {
                Ok(outcome) => return Ok((outcome, payload)),
                Err(Error::Conflict | Error::Precondition) if attempts < max_attempts => {
                    debug!(attempts, max_attempts, "read-for-update conflict, retrying");
                    attempts += 1;
                }
                Err(e) => return Err(e),
            }
        }
    }

    // ── Upload lifecycle ─────────────────────────────────────────────────────

    /// Begin a fresh upload at `key`.
    ///
    /// # Errors
    ///
    /// [`StorageError::Backend`] when no upload-session store was wired.
    pub async fn create_upload(&self, key: &str) -> Result<UploadSession, StorageError> {
        self.upload()?.create_upload(key).await
    }

    /// Stream `len` bytes from `body` into `session`, parking sub-part
    /// remainders at `staging_key` (S3 only). The session is mutated in place.
    ///
    /// # Errors
    ///
    /// [`StorageError::Backend`] when no upload-session store was wired.
    pub async fn write_upload(
        &self,
        session: &mut UploadSession,
        staging_key: &str,
        body: ByteStream,
        len: u64,
    ) -> Result<(), StorageError> {
        self.upload()?
            .write_upload(session, staging_key, body, len)
            .await
    }

    /// Discard `session` and any backend state it owns without producing an
    /// object.
    ///
    /// # Errors
    ///
    /// [`StorageError::Backend`] when no upload-session store was wired.
    pub async fn abort_upload(
        &self,
        session: UploadSession,
        staging_key: &str,
    ) -> Result<(), StorageError> {
        self.upload()?.abort_upload(session, staging_key).await
    }

    /// Best-effort abort of any in-flight backend upload state for `key`.
    ///
    /// # Errors
    ///
    /// [`StorageError::Backend`] when no upload-session store was wired.
    pub async fn abort_pending_uploads(&self, key: &str) -> Result<(), StorageError> {
        self.upload()?.abort_pending_uploads(key).await
    }

    /// Run only the backend completion step (S3 multipart-complete; FS no-op)
    /// so the assembled object lands at `session.key`. Promotion to the
    /// canonical path is left to the caller — used when those mutations must be
    /// merged into a larger transaction (e.g. upload promotion + blob-index
    /// grant). For the standalone case prefer [`Store::finalize_upload`].
    ///
    /// # Errors
    ///
    /// [`StorageError::Backend`] when no upload-session store was wired, or any
    /// backend completion failure.
    pub async fn complete_upload(
        &self,
        session: UploadSession,
        staging_key: &str,
    ) -> Result<(), StorageError> {
        self.upload()?.complete_upload(session, staging_key).await
    }

    /// Two-phase upload finalization: run the backend completion step so the
    /// assembled object lands at `session.key`, then atomically `Move` it to
    /// `dst_key` and `Delete` the session record at `session_record_key` in a
    /// single engine transaction.
    ///
    /// # Errors
    ///
    /// - [`Error::Storage`] wrapping [`StorageError::Backend`] when no
    ///   upload-session store was wired, or any backend completion failure.
    /// - Any [`Error`] from the promoting transaction.
    pub async fn finalize_upload(
        &self,
        session: UploadSession,
        staging_key: &str,
        dst_key: &str,
        session_record_key: &str,
    ) -> Result<Outcome, Error> {
        let upload = self.upload()?;
        let src_key = session.key.clone();
        upload.complete_upload(session, staging_key).await?;

        let tx = Transaction::builder()
            .mutation(Mutation::Move {
                src: src_key,
                dst: dst_key.to_string(),
            })
            .mutation(Mutation::Delete {
                key: session_record_key.to_string(),
                expected: None,
            })
            .build();
        self.executor.execute(tx).await
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

    /// Prune now-empty ancestor directories of `key`, up to `max_levels`
    /// levels. No-op unless an FS-prune backend is wired (S3 has no directory
    /// concept).
    pub async fn prune_empty_ancestors(&self, key: &str, max_levels: u8) {
        if let Some(fs) = &self.fs_prune {
            fs.prune_empty_ancestors(key, max_levels).await;
        }
    }

    fn upload(&self) -> Result<&Arc<dyn UploadSessionStore>, StorageError> {
        self.upload
            .as_ref()
            .ok_or_else(|| StorageError::Backend("upload-session store not configured".into()))
    }
}

/// Builder for [`Store`]. `store` and `executor` are required; the upload,
/// presign, and FS-prune capabilities are optional and reflect what the
/// underlying backend supports.
#[derive(Default)]
pub struct StoreBuilder {
    object: Option<Arc<dyn ObjectStore>>,
    upload: Option<Arc<dyn UploadSessionStore>>,
    presign: Option<Arc<dyn PresignedStore>>,
    fs_prune: Option<Arc<StorageFsBackend>>,
    executor: Option<Arc<dyn TransactionExecutor>>,
}

impl StoreBuilder {
    /// Set the floor object store (required).
    #[must_use]
    pub fn object(mut self, object: Arc<dyn ObjectStore>) -> Self {
        self.object = Some(object);
        self
    }

    /// Set the upload-session store. Enables the upload lifecycle methods.
    #[must_use]
    pub fn upload(mut self, upload: Arc<dyn UploadSessionStore>) -> Self {
        self.upload = Some(upload);
        self
    }

    /// Set the presign backend. Enables [`Store::presigned_get`].
    #[must_use]
    pub fn presign(mut self, presign: Arc<dyn PresignedStore>) -> Self {
        self.presign = Some(presign);
        self
    }

    /// Set the typed FS backend used for [`Store::prune_empty_ancestors`].
    #[must_use]
    pub fn fs_prune(mut self, fs_prune: Arc<StorageFsBackend>) -> Self {
        self.fs_prune = Some(fs_prune);
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
            upload: self.upload,
            presign: self.presign,
            fs_prune: self.fs_prune,
            executor,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use angos_storage::MemoryObjectStore;

    use crate::{
        executor::locked::LockedExecutor,
        lock::{primitive::Lock, storage::memory::MemoryLockStorage},
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
            .object(backend.clone())
            .upload(backend)
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
    async fn finalize_upload_promotes_and_deletes_session() {
        let backend = Arc::new(MemoryObjectStore::new());
        let store = store_over(backend);

        // Stand in for a session record + an assembled upload object.
        let session = store.create_upload("upload/u1").await.expect("create");
        store
            .put("upload-sessions/u1.json", Bytes::from_static(b"{}"))
            .await
            .expect("session record");

        store
            .finalize_upload(
                session,
                "staging/u1",
                "blob-data/abc",
                "upload-sessions/u1.json",
            )
            .await
            .expect("finalize");

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
