//! Locked executor: acquires distributed locks on the full key set, writes the
//! intent, applies mutations under the lock, then reaps.
//!
//! Works on any `ObjectStore` backend (FS or S3) because it never calls
//! backend-native conditional operations; instead it enforces a mutation's
//! `expected` `ETag` itself by HEAD-comparing the stored `ETag` under the lock
//! before writing. Deadlock-freedom is guaranteed by acquiring all locks in
//! sorted order before any write.
//!
//! NOTE: Both executors now honor `expected`. The CAS executor uses
//! backend-native conditional primitives (`put_if_match`/`delete_if_match`);
//! the Locked executor performs an explicit HEAD + `ETag` comparison under the
//! lock, mirroring CAS semantics.

use std::sync::Arc;

use async_trait::async_trait;
use sha2::{Digest as _, Sha256};
use tokio::select;
use tracing::debug;
use uuid::Uuid;

use angos_storage::{Error as StorageError, ObjectStore};

use crate::{
    error::Error,
    executor::{
        Outcome, TransactionExecutor,
        common::{
            ObjectApplyMode, apply_object_store, build_intent, finish, stage_bodies, stamp_applied,
            write_intent,
        },
    },
    intent::{DEFAULT_INTENT_TTL_SECS, IntentRecord, MutationRecord},
    lock::{LockSession, primitive::Lock},
    transaction::Transaction,
};

/// Locked-mode executor.
///
/// Acquires distributed locks on `reads ∪ mutations` in sorted
/// order (deadlock-free), writes the intent record, applies mutations under
/// the lock with unconditional storage operations, stamps each mutation's
/// progress slot to `Applied` as it succeeds, then reaps bodies and intent.
///
/// Constructed via [`LockedExecutor::builder`].
pub struct LockedExecutor {
    store: Arc<dyn ObjectStore>,
    lock: Arc<Lock>,
    ttl_secs: u64,
}

impl std::fmt::Debug for LockedExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LockedExecutor")
            .field("ttl_secs", &self.ttl_secs)
            .finish_non_exhaustive()
    }
}

/// Builder for [`LockedExecutor`]. The object store and lock are required and
/// supplied to [`LockedExecutor::builder`]; the intent TTL is an optional
/// fluent setter.
pub struct LockedExecutorBuilder {
    store: Arc<dyn ObjectStore>,
    lock: Arc<Lock>,
    ttl_secs: Option<u64>,
}

impl LockedExecutorBuilder {
    /// Set the intent TTL in seconds. Defaults to 300.
    #[must_use]
    pub fn ttl_secs(mut self, secs: u64) -> Self {
        self.ttl_secs = Some(secs);
        self
    }

    /// Consume the builder and produce a [`LockedExecutor`].
    #[must_use]
    pub fn build(self) -> LockedExecutor {
        LockedExecutor {
            store: self.store,
            lock: self.lock,
            ttl_secs: self.ttl_secs.unwrap_or(DEFAULT_INTENT_TTL_SECS),
        }
    }
}

impl LockedExecutor {
    /// Return a builder wrapping the object `store` and `lock`. The intent TTL
    /// is an optional fluent setter on the returned builder.
    #[must_use]
    pub fn builder(store: Arc<dyn ObjectStore>, lock: Arc<Lock>) -> LockedExecutorBuilder {
        LockedExecutorBuilder {
            store,
            lock,
            ttl_secs: None,
        }
    }

    /// Apply a single mutation under the pre-acquired lock.
    ///
    /// Delegates to the shared [`apply_object_store`] in `Abort` mode: `expected`
    /// is honored via a HEAD/ETag compare under the lock, mirroring the CAS
    /// executor's `put_if_match` / `delete_if_match` semantics.
    async fn apply_mutation(&self, mutation: &MutationRecord) -> Result<(), Error> {
        apply_object_store(self.store.as_ref(), mutation, ObjectApplyMode::Abort).await
    }

    /// Verify read fingerprints after acquiring the lock.
    ///
    /// Re-fetches each key and checks the SHA-256 of the live body against the
    /// captured hash. A missing key or a hash mismatch returns
    /// [`Error::Conflict`] so the caller retries.
    async fn verify_reads_under_lock(&self, tx: &Transaction) -> Result<(), Error> {
        for read in &tx.reads {
            match self.store.get(&read.key).await {
                Ok(body) => {
                    let actual: [u8; 32] = Sha256::digest(&body).into();
                    if actual != read.fingerprint {
                        debug!(
                            key = read.key,
                            "Locked executor: content hash mismatch, signalling Conflict"
                        );
                        return Err(Error::Conflict);
                    }
                }
                Err(StorageError::NotFound) => {
                    // An absent key matches only a read that recorded absence.
                    if !read.expects_absent() {
                        return Err(Error::Conflict);
                    }
                }
                Err(e) => return Err(Error::Storage(e)),
            }
        }
        Ok(())
    }

    /// Apply all mutations under the pre-acquired lock.
    ///
    /// Returns `Ok(())` on success or `Err` on the first hard error.
    /// The caller is responsible for releasing the lock session in both cases.
    async fn apply_all(&self, intent: &mut IntentRecord) -> Result<(), Error> {
        for idx in 0..intent.mutations.len() {
            let mutation = intent.mutations[idx].clone();
            match self.apply_mutation(&mutation).await {
                Ok(()) => stamp_applied(self.store.as_ref(), intent, idx).await,
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
}

#[async_trait]
impl TransactionExecutor for LockedExecutor {
    /// Drive `tx` through Build → Prepare → Commit-intent → Apply → Reap.
    ///
    /// Acquires the engine-owned lock on `tx.lock_set()` in sorted order,
    /// verifies read fingerprints under the lock, writes the intent, applies
    /// mutations unconditionally, reaps the intent and staged bodies, then
    /// releases the lock. Any caller-held [`LockSession`] is independent of
    /// this call; the caller releases it explicitly after `execute` returns.
    async fn execute(&self, tx: Transaction) -> Result<Outcome, Error> {
        let tx_id = Uuid::new_v4();
        let lock_set = tx.lock_set();

        // Stage bodies before acquiring the lock so lock hold time is minimal.
        let mutation_records = stage_bodies(self.store.as_ref(), &tx, tx_id).await?;

        // Acquire the engine's own locks in sorted order (deadlock-free).
        let session = self.lock.acquire(&lock_set).await.map_err(Error::Lock)?;

        // Verify read fingerprints now that we hold the locks.
        if let Err(e) = self.verify_reads_under_lock(&tx).await {
            session.release().await;
            return Err(e);
        }

        let mut intent = build_intent(
            tx_id,
            self.ttl_secs,
            &tx.reads,
            mutation_records,
            tx.coarse_lock_keys.clone(),
        );
        if let Err(e) = write_intent(self.store.as_ref(), &intent).await {
            session.release().await;
            return Err(e);
        }

        // Fence Apply against lock-loss: the heartbeat fires the session's
        // cancellation token when ownership is lost (ETag mismatch on refresh)
        // or `max_hold_secs` is exceeded. Racing `apply_all` against the token
        // and dropping the future on cancellation stops any further mutations,
        // so the original owner cannot keep writing while a takeover replica
        // also writes (split-brain). The abort is reported as `Error::Conflict`
        // because the caller's retry loop (`execute_with_retry[_payload]`)
        // retries on `Conflict | Precondition`, re-running the whole
        // transaction under a freshly-acquired lock.
        //
        // (The CAS executor needs no such fence: it holds no working-set lock
        // and applies via conditional ops, so a lost lock cannot cause an
        // unconditional overwrite.)
        let cancelled = session.cancellation();
        let apply_result = select! {
            biased;
            () = cancelled.cancelled() => Err(Error::Conflict),
            result = self.apply_all(&mut intent) => result,
        };

        // Reap only when the transaction either fully committed or applied
        // nothing (see `common::finish`). On a mid-apply abort the intent is
        // left in place for recovery.
        finish(self.store.as_ref(), &apply_result, &intent).await;
        session.release().await;

        apply_result?;
        Ok(Outcome { tx_id })
    }

    async fn try_acquire(&self, keys: &[String]) -> Result<Option<LockSession>, Error> {
        self.lock.try_acquire(keys).await.map_err(Error::Lock)
    }

    async fn acquire(&self, keys: &[String]) -> Result<LockSession, Error> {
        self.lock.acquire(keys).await.map_err(Error::Lock)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;

    use angos_storage::{Etag, MemoryObjectStore, ObjectStore};

    use crate::{
        error::Error,
        executor::{Outcome, TransactionExecutor, locked::LockedExecutor},
        lock::{primitive::Lock, storage::memory::MemoryLockStorage},
        transaction::{Mutation, Transaction},
    };

    fn make_executor(store: Arc<MemoryObjectStore>) -> LockedExecutor {
        let lock = Arc::new(
            Lock::builder(Arc::new(MemoryLockStorage::new()))
                .build()
                .unwrap(),
        );
        LockedExecutor::builder(store as Arc<dyn ObjectStore>, lock).build()
    }

    #[tokio::test]
    async fn read_matching_body_commits() {
        let body = b"hello";
        let store = MemoryObjectStore::new();
        store.put("k", Bytes::from_static(body)).await.unwrap();
        let executor = make_executor(Arc::new(store));

        let tx = Transaction::builder()
            .read("k", Bytes::from_static(body))
            .mutation(Mutation::Put {
                key: "out".to_string(),
                body: Bytes::from("x"),
                expected: None,
            })
            .build();

        let result: Result<Outcome, Error> = executor.execute(tx).await;
        assert!(result.is_ok(), "matching body should commit: {result:?}");
    }

    #[tokio::test]
    async fn read_stale_body_returns_conflict() {
        let store = MemoryObjectStore::new();
        store.put("k", Bytes::from_static(b"hello")).await.unwrap();
        let executor = make_executor(Arc::new(store));

        let tx = Transaction::builder()
            .read("k", Bytes::from_static(b"stale-content"))
            .mutation(Mutation::Put {
                key: "out".to_string(),
                body: Bytes::from("x"),
                expected: None,
            })
            .build();

        let result: Result<Outcome, Error> = executor.execute(tx).await;
        assert!(
            matches!(result, Err(Error::Conflict)),
            "stale body should return Conflict, got: {result:?}"
        );
    }

    #[tokio::test]
    async fn read_absent_key_returns_conflict() {
        let executor = make_executor(Arc::new(MemoryObjectStore::new()));

        let tx = Transaction::builder()
            .read("no-such-key", Bytes::from_static(b"something"))
            .mutation(Mutation::Put {
                key: "out".to_string(),
                body: Bytes::from("x"),
                expected: None,
            })
            .build();

        let result: Result<Outcome, Error> = executor.execute(tx).await;
        assert!(
            matches!(result, Err(Error::Conflict)),
            "absent key should return Conflict, got: {result:?}"
        );
    }

    #[tokio::test]
    async fn read_recording_absence_commits_when_key_is_absent() {
        let executor = make_executor(Arc::new(MemoryObjectStore::new()));

        let tx = Transaction::builder()
            .read("no-such-key", Bytes::new())
            .mutation(Mutation::Put {
                key: "out".to_string(),
                body: Bytes::from("x"),
                expected: None,
            })
            .build();

        let result: Result<Outcome, Error> = executor.execute(tx).await;
        assert!(
            result.is_ok(),
            "an empty-body read records absence and must match a missing key: {result:?}"
        );
    }

    #[tokio::test]
    async fn read_recording_absence_conflicts_when_key_appeared() {
        let store = MemoryObjectStore::new();
        store
            .put("k", Bytes::from_static(b"created"))
            .await
            .unwrap();
        let executor = make_executor(Arc::new(store));

        let tx = Transaction::builder()
            .read("k", Bytes::new())
            .mutation(Mutation::Put {
                key: "out".to_string(),
                body: Bytes::from("x"),
                expected: None,
            })
            .build();

        let result: Result<Outcome, Error> = executor.execute(tx).await;
        assert!(
            matches!(result, Err(Error::Conflict)),
            "a key written since the absence was recorded must conflict, got: {result:?}"
        );
    }

    #[tokio::test]
    async fn put_if_absent_on_existing_key_returns_precondition() {
        let store = Arc::new(MemoryObjectStore::new());
        store
            .put("existing", Bytes::from_static(b"bytes"))
            .await
            .unwrap();
        let executor = make_executor(Arc::clone(&store));

        let tx = Transaction::builder()
            .mutation(Mutation::PutIfAbsent {
                key: "existing".to_string(),
                body: Bytes::from_static(b"new-bytes"),
            })
            .build();

        let result: Result<Outcome, Error> = executor.execute(tx).await;
        assert!(
            matches!(result, Err(Error::Precondition)),
            "PutIfAbsent on existing key should return Precondition, got: {result:?}"
        );

        let body = store
            .get("existing")
            .await
            .expect("existing key still present");
        assert_eq!(body.as_slice(), b"bytes", "original body must be untouched");
    }

    #[tokio::test]
    async fn put_with_stale_expected_returns_precondition_and_leaves_object() {
        let store = Arc::new(MemoryObjectStore::new());
        // Seed an object so it has a real (current) ETag.
        store
            .put("k1", Bytes::from_static(b"original"))
            .await
            .unwrap();
        let executor = make_executor(Arc::clone(&store));

        // A conditional put against a stale ETag must fail and not write.
        let stale = Etag::new("\"stale-etag\"");
        let tx = Transaction::builder()
            .mutation(Mutation::Put {
                key: "k1".to_string(),
                body: Bytes::from_static(b"new"),
                expected: Some(stale),
            })
            .build();

        let result: Result<Outcome, Error> = executor.execute(tx).await;
        assert!(
            matches!(result, Err(Error::Precondition)),
            "stale expected etag on Put should return Precondition, got: {result:?}"
        );

        // The existing object must be untouched.
        let body = store.get("k1").await.expect("k1 still present");
        assert_eq!(
            body.as_slice(),
            b"original",
            "existing object must be untouched on precondition failure"
        );
    }

    #[tokio::test]
    async fn delete_with_stale_expected_returns_precondition_and_leaves_object() {
        let store = Arc::new(MemoryObjectStore::new());
        store
            .put("k1", Bytes::from_static(b"original"))
            .await
            .unwrap();
        let executor = make_executor(Arc::clone(&store));

        let stale = Etag::new("\"stale-etag\"");
        let tx = Transaction::builder()
            .mutation(Mutation::Delete {
                key: "k1".to_string(),
                expected: Some(stale),
            })
            .build();

        let result: Result<Outcome, Error> = executor.execute(tx).await;
        assert!(
            matches!(result, Err(Error::Precondition)),
            "stale expected etag on Delete should return Precondition, got: {result:?}"
        );

        // The object must still be present.
        let body = store.get("k1").await.expect("k1 still present");
        assert_eq!(
            body.as_slice(),
            b"original",
            "existing object must survive a failed conditional delete"
        );
    }
}
