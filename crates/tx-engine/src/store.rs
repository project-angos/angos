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
//! Raw object I/O (reads, non-transactional writes, the upload lifecycle)
//! goes straight through [`Store::object_store`] and returns [`StorageError`],
//! so callers keep matching [`StorageError::NotFound`] directly. The
//! coordinated helpers ([`Store::update`] and [`Store::update_with_payload`])
//! return the engine [`Error`] (carrying `Conflict`/`Precondition`).

use std::{fmt, future::Future, sync::Arc};

use bytes::Bytes;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use angos_storage::{ConditionalStore, Error as StorageError, ObjectStore};

#[cfg(feature = "redis")]
use crate::lock::storage::redis::RedisLockStorage;
use crate::{
    error::Error,
    executor::{Outcome, TransactionExecutor, cas::CasExecutor, locked::LockedExecutor},
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

    /// `true` when writes coordinate through storage-level conditional
    /// operations (the CAS executor), making [`Store::update_advisory`]
    /// available.
    #[must_use]
    pub fn cas_enabled(&self) -> bool {
        self.conditional.is_some()
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

    /// The composed object store: the single surface for raw object I/O
    /// (reads, non-transactional writes, the upload lifecycle). Only the
    /// coordinated paths (`execute`, `update*`, `read_for_update`) live on
    /// the façade itself.
    #[must_use]
    pub fn object_store(&self) -> &Arc<dyn ObjectStore> {
        &self.object
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
    /// The retry loop is written out here rather than routed through
    /// [`execute_with_retry_payload`]: that helper's build closure cannot
    /// borrow `map` mutably across its returned future, which the `&mut self`
    /// receiver of `FnMut` requires.
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
                    debug!(attempts, max_attempts, "Transaction conflict, retrying");
                    attempts += 1;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Single-shot advisory read-modify-write on one key, bypassing the
    /// transaction pipeline: one read with its etag plus one conditional
    /// write. Only available on CAS deployments (see [`Store::cas_enabled`]).
    ///
    /// Advisory state tolerates lost updates, so a concurrent writer winning
    /// the race (etag mismatch) drops this write as a successful no-op, and a
    /// backend that surfaces no etag skips the write entirely rather than
    /// clobbering unconditionally. `map`'s payload is returned either way.
    ///
    /// # Errors
    ///
    /// [`Error::Build`] when the store has no conditional backend, `map`'s
    /// error, or a storage error from the read or write, including
    /// `NotFound` when `key` is absent.
    pub async fn update_advisory<F, T>(&self, key: &str, map: F) -> Result<T, Error>
    where
        F: FnOnce(Bytes) -> Result<(Bytes, T), Error> + Send,
        T: Send,
    {
        let Some(conditional) = &self.conditional else {
            return Err(Error::Build(
                "advisory update requires the CAS executor".to_string(),
            ));
        };
        let (body, etag) = conditional
            .get_with_etag(key)
            .await
            .map_err(Error::Storage)?;
        let (mapped, payload) = map(Bytes::from(body))?;
        if let Some(etag) = etag {
            match conditional.put_if_match(key, &etag, mapped).await {
                Ok(_) | Err(StorageError::PreconditionFailed) => {}
                Err(e) => return Err(Error::Storage(e)),
            }
        }
        Ok(payload)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;

    use angos_storage::{Error as StorageError, MemoryObjectStore};

    use crate::{
        error::Error,
        lock::LockStrategy,
        store::Store,
        transaction::{Mutation, Transaction},
    };

    fn store_over(backend: Arc<MemoryObjectStore>) -> Store {
        Store::new(backend, None, LockStrategy::Memory, None).expect("store")
    }

    fn cas_store_over(backend: Arc<MemoryObjectStore>) -> Store {
        Store::new(backend.clone(), Some(backend), LockStrategy::Memory, None).expect("store")
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

        assert_eq!(store.object_store().get("k").await.expect("get"), b"v");
    }

    #[tokio::test]
    async fn update_observes_current_snapshot() {
        let backend = Arc::new(MemoryObjectStore::new());
        let store = store_over(backend);
        store
            .object_store()
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

        assert_eq!(store.object_store().get("k").await.expect("get"), b"v2");
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
    async fn update_advisory_writes_and_returns_payload() {
        let backend = Arc::new(MemoryObjectStore::new());
        let store = cas_store_over(backend);
        store
            .object_store()
            .put("k", Bytes::from_static(b"v1"))
            .await
            .expect("seed");

        let payload = store
            .update_advisory("k", |body| {
                assert_eq!(&body[..], b"v1");
                Ok((Bytes::from_static(b"v2"), "payload"))
            })
            .await
            .expect("advisory update");

        assert_eq!(payload, "payload");
        assert_eq!(store.object_store().get("k").await.expect("get"), b"v2");
    }

    /// A write landing between the advisory read and its conditional write
    /// must win: the advisory write is dropped as a no-op, not retried.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn update_advisory_lost_race_is_noop() {
        let backend = Arc::new(MemoryObjectStore::new());
        let store = cas_store_over(backend);
        store
            .object_store()
            .put("k", Bytes::from_static(b"v1"))
            .await
            .expect("seed");

        let racer = store.clone();
        let payload = store
            .update_advisory("k", move |body| {
                assert_eq!(&body[..], b"v1");
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current()
                        .block_on(racer.object_store().put("k", Bytes::from_static(b"racer")))
                })
                .expect("racing put");
                Ok((Bytes::from_static(b"stamped"), ()))
            })
            .await;

        assert!(payload.is_ok(), "a lost race must not surface an error");
        assert_eq!(
            store.object_store().get("k").await.expect("get"),
            b"racer",
            "the concurrent write must win; the advisory write is dropped"
        );
    }

    #[tokio::test]
    async fn update_advisory_missing_key_propagates_not_found() {
        let backend = Arc::new(MemoryObjectStore::new());
        let store = cas_store_over(backend);

        let err = store
            .update_advisory("missing", |body| Ok((body, ())))
            .await
            .expect_err("missing key must error");
        assert!(matches!(err, Error::Storage(StorageError::NotFound)));
    }

    #[tokio::test]
    async fn update_advisory_requires_cas() {
        let backend = Arc::new(MemoryObjectStore::new());
        let store = store_over(backend);
        store
            .object_store()
            .put("k", Bytes::from_static(b"v1"))
            .await
            .expect("seed");

        let err = store
            .update_advisory("k", |body| Ok((body, ())))
            .await
            .expect_err("non-CAS store must reject advisory updates");
        assert!(matches!(err, Error::Build(_)));
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
        assert_eq!(store.object_store().get("k").await.expect("get"), b"v");
    }

    #[tokio::test]
    async fn complete_upload_then_promote_and_delete_via_transaction() {
        let backend = Arc::new(MemoryObjectStore::new());
        let store = store_over(backend);

        // Stand in for a session record + an assembled upload object.
        store
            .object_store()
            .create_upload("upload/u1")
            .await
            .expect("create");
        store
            .object_store()
            .put("upload-sessions/u1.json", Bytes::from_static(b"{}"))
            .await
            .expect("session record");

        // Primitive: backend completion lands the assembled object at the
        // upload key. The caller (registry) then composes the promotion and
        // record cleanup into a single engine transaction.
        store
            .object_store()
            .complete_upload("upload/u1")
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

        // Assembled object promoted to the canonical key...
        assert!(store.object_store().get("blob-data/abc").await.is_ok());
        // ...source upload key gone, and the session record deleted.
        assert!(matches!(
            store.object_store().get("upload/u1").await,
            Err(StorageError::NotFound)
        ));
        assert!(matches!(
            store.object_store().get("upload-sessions/u1.json").await,
            Err(StorageError::NotFound)
        ));
    }
}
