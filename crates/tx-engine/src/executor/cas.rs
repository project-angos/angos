//! CAS executor: records etags at Prepare, writes the intent, applies mutations
//! with `put_if_match`/`put_if_absent`/`delete_if_match`, and rolls back on
//! `PreconditionFailed` when no mutations have been applied yet.
//!
//! When a CAS precondition fails after at least one mutation has already been
//! applied (partial-commit case), the executor uses `apply_cas_idempotent` —
//! the same stale-stamp recovery logic the `RecoveryLoop` uses — to distinguish
//! between a healthy-path write that landed without its stamp (the live body
//! matches the staged body → stamp and continue) versus true contention (live
//! body differs → return `Error::PartialCommit` and preserve the intent for
//! the recovery loop).

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use sha2::{Digest as _, Sha256};
use tracing::{debug, warn};
use uuid::Uuid;

use angos_storage::{ConditionalStore, Error as StorageError, Etag};

use crate::{
    error::Error,
    executor::{
        Outcome, TransactionExecutor,
        common::{reap, rollback, stage_bodies, stamp_progress, write_intent},
    },
    intent::{DEFAULT_INTENT_TTL_SECS, IntentRecord, MutationProgress, MutationRecord, ReadRecord},
    lock::{LockSession, primitive::Lock},
    transaction::Transaction,
};

/// CAS-mode executor.
///
/// Available only on backends that implement [`ConditionalStore`] (S3 and
/// compatible endpoints). At Prepare, etags for the read set are re-read and
/// stashed as preconditions. Apply uses `put_if_match`/`put_if_absent`/
/// `delete_if_match`. On `PreconditionFailed` the transaction is rolled back
/// if no mutations have been applied yet; partially-applied transactions are
/// continued forward (each successful mutation stamps its `progress` slot to
/// `Applied`, which switches the recovery loop into replay-forward mode).
///
/// The `lock` is used only by [`TransactionExecutor::try_acquire`] and
/// [`TransactionExecutor::acquire`]; it is not involved in the CAS transaction
/// path. Subsystems that need a distributed execution lock (e.g. the durable
/// job consumer's per-`lock_key` serialisation) go through these methods rather
/// than holding a separate `Arc<Lock>`.
///
/// Constructed via [`CasExecutor::builder`].
pub struct CasExecutor {
    store: Arc<dyn ConditionalStore>,
    lock: Arc<Lock>,
    ttl_secs: u64,
}

impl std::fmt::Debug for CasExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CasExecutor")
            .field("ttl_secs", &self.ttl_secs)
            .finish_non_exhaustive()
    }
}

/// Builder for [`CasExecutor`].
#[derive(Default)]
pub struct CasExecutorBuilder {
    store: Option<Arc<dyn ConditionalStore>>,
    lock: Option<Arc<Lock>>,
    ttl_secs: Option<u64>,
}

impl CasExecutorBuilder {
    /// Set the conditional store.
    #[must_use]
    pub fn store(mut self, store: Arc<dyn ConditionalStore>) -> Self {
        self.store = Some(store);
        self
    }

    /// Set the lock used by [`TransactionExecutor::try_acquire`] /
    /// [`TransactionExecutor::acquire`]. Required.
    #[must_use]
    pub fn lock(mut self, lock: Arc<Lock>) -> Self {
        self.lock = Some(lock);
        self
    }

    /// Set the intent TTL in seconds. Defaults to 300.
    #[must_use]
    pub fn ttl_secs(mut self, secs: u64) -> Self {
        self.ttl_secs = Some(secs);
        self
    }

    /// Consume the builder and produce a [`CasExecutor`].
    ///
    /// # Errors
    ///
    /// Returns [`Error::Build`] if `store` or `lock` was not provided.
    pub fn build(self) -> Result<CasExecutor, Error> {
        Ok(CasExecutor {
            store: self.store.ok_or(Error::Build("store".to_string()))?,
            lock: self.lock.ok_or(Error::Build("lock".to_string()))?,
            ttl_secs: self.ttl_secs.unwrap_or(DEFAULT_INTENT_TTL_SECS),
        })
    }
}

impl CasExecutor {
    /// Return a builder for constructing a `CasExecutor`.
    #[must_use]
    pub fn builder() -> CasExecutorBuilder {
        CasExecutorBuilder::default()
    }

    /// Apply a single mutation using conditional storage operations.
    ///
    /// Returns `Ok(Some(etag))` when a conditional `put_if_match` /
    /// `put_if_absent` returned a fresh etag for the new object, `Ok(None)`
    /// when no etag is available (unconditional put, delete, copy, move, or
    /// a backend that did not surface an etag), or
    /// `Err(Error::Precondition)` when an etag precondition was not met.
    async fn apply_mutation_cas(&self, mutation: &MutationRecord) -> Result<Option<Etag>, Error> {
        match mutation {
            MutationRecord::Put {
                key,
                body_ref,
                expected,
                ..
            } => {
                let body = self.store.get(body_ref).await?;
                let body_bytes = Bytes::from(body);
                if let Some(etag) = expected {
                    let new_etag = self
                        .store
                        .put_if_match(key, etag, body_bytes)
                        .await
                        .map_err(|e| match e {
                            StorageError::PreconditionFailed => Error::Precondition,
                            other => Error::Storage(other),
                        })?;
                    Ok(new_etag)
                } else {
                    self.store.put(key, body_bytes).await?;
                    Ok(None)
                }
            }
            MutationRecord::PutIfAbsent { key, body_ref, .. } => {
                let body = self.store.get(body_ref).await?;
                let new_etag = self
                    .store
                    .put_if_absent(key, Bytes::from(body))
                    .await
                    .map_err(|e| match e {
                        StorageError::PreconditionFailed => Error::Precondition,
                        other => Error::Storage(other),
                    })?;
                Ok(new_etag)
            }
            MutationRecord::Delete { key, expected, .. } => {
                if let Some(etag) = expected {
                    self.store
                        .delete_if_match(key, etag)
                        .await
                        .map_err(|e| match e {
                            StorageError::PreconditionFailed => Error::Precondition,
                            other => Error::Storage(other),
                        })?;
                } else {
                    self.store.delete(key).await?;
                }
                Ok(None)
            }
            MutationRecord::Copy { src, dst, .. } => {
                self.store.copy(src, dst).await?;
                Ok(None)
            }
            MutationRecord::Move { src, dst, .. } => {
                self.store.copy(src, dst).await?;
                self.store.delete(src).await?;
                Ok(None)
            }
        }
    }

    /// Verify the read set: re-read each key, hash the body, and return
    /// `Conflict` if the hash differs from the one recorded at build time.
    async fn prepare_reads(&self, tx: &Transaction) -> Result<(), Error> {
        for read in &tx.reads {
            match self.store.get(&read.key).await {
                Ok(body) => {
                    let actual: [u8; 32] = Sha256::digest(&body).into();
                    if actual != read.fingerprint {
                        debug!(
                            key = read.key,
                            "CAS executor: content hash mismatch at Prepare, signalling Conflict"
                        );
                        return Err(Error::Conflict);
                    }
                }
                Err(StorageError::NotFound) => {
                    return Err(Error::Conflict);
                }
                Err(e) => return Err(Error::Storage(e)),
            }
        }
        Ok(())
    }

    /// Apply all mutations in the intent.
    ///
    /// Returns `Ok(())` after all mutations are applied. On `Precondition`
    /// failure before any mutation has been applied, rolls back and returns
    /// `Err(Error::Precondition)`. On `Precondition` failure mid-Apply (at
    /// least one mutation already stamped `Applied`), uses the stale-stamp
    /// recovery heuristic:
    ///
    /// - If the live body at the failing key matches the staged body, the
    ///   healthy-path write already landed without its stamp. The slot is
    ///   marked `Applied` and the loop continues to the next mutation.
    /// - If the live body does not match, this is true contention. The intent
    ///   is left in `tx-log/` for the recovery loop and
    ///   `Err(Error::PartialCommit)` is returned so the caller skips `reap`.
    async fn apply_all(&self, intent: &mut IntentRecord) -> Result<(), Error> {
        let tx_id = intent.id;
        for idx in 0..intent.mutations.len() {
            let mutation = intent.mutations[idx].clone();
            match self.apply_mutation_cas(&mutation).await {
                Ok(etag) => {
                    if let Err(stamp_err) =
                        stamp_progress(self.store.as_ref(), intent, idx, etag).await
                    {
                        warn!(
                            tx_id = %tx_id,
                            idx,
                            error = %stamp_err,
                            "Failed to stamp mutation progress; recovery will re-apply idempotently"
                        );
                    }
                }
                Err(Error::Precondition) => {
                    if intent.any_applied() {
                        // The transaction is committed (at least one slot is
                        // Applied). Apply the stale-stamp recovery heuristic:
                        // compare the live body against the staged body.
                        match apply_cas_idempotent(self.store.as_ref(), &mutation).await {
                            Ok(etag) => {
                                // Live body matches staged body — the
                                // healthy-path write landed without its stamp.
                                // Stamp and continue forward.
                                if let Err(stamp_err) =
                                    stamp_progress(self.store.as_ref(), intent, idx, etag).await
                                {
                                    warn!(
                                        tx_id = %tx_id,
                                        idx,
                                        error = %stamp_err,
                                        "Failed to stamp stale-stamp-recovered mutation; \
                                         recovery will re-apply idempotently"
                                    );
                                }
                            }
                            Err(Error::PartialCommit) => {
                                // True contention: live body differs from staged
                                // body. Preserve the intent for the recovery loop.
                                warn!(
                                    tx_id = %tx_id,
                                    idx,
                                    "CAS true contention mid-Apply; \
                                     leaving intent for recovery loop"
                                );
                                return Err(Error::PartialCommit);
                            }
                            Err(e) => return Err(e),
                        }
                    } else {
                        rollback(self.store.as_ref(), intent).await;
                        return Err(Error::Precondition);
                    }
                }
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
}

/// Apply a single mutation using conditional storage operations, with
/// stale-stamp recovery for the `PreconditionFailed` case.
///
/// This is the idempotent variant used both by the CAS executor's partial-commit
/// handler and by the `RecoveryLoop`'s replay path. It mirrors the semantics
/// described in the AIP design: on `PreconditionFailed`, compare the live
/// body's SHA-256 hash against the staged body. A match means the healthy-path
/// write already landed (stale stamp); the mutation is treated as applied and
/// `Ok(None)` is returned. A mismatch means true contention: `Err(Error::PartialCommit)`
/// is returned so the caller stops replay and leaves the intent for the next sweep.
///
/// # Errors
///
/// Returns `Err(Error::PartialCommit)` on true contention (live body ≠ staged
/// body), `Err(Error::Storage(...))` on hard storage errors, or `Ok(Some(etag))`
/// / `Ok(None)` on success.
pub async fn apply_cas_idempotent(
    store: &dyn ConditionalStore,
    mutation: &MutationRecord,
) -> Result<Option<Etag>, Error> {
    match mutation {
        MutationRecord::Put {
            key,
            body_ref,
            expected,
        } => {
            let body = match store.get(body_ref).await {
                Ok(b) => b,
                Err(StorageError::NotFound) => return Ok(None),
                Err(e) => return Err(Error::Storage(e)),
            };
            let body_bytes = Bytes::from(body);
            let Some(etag) = expected else {
                store.put(key, body_bytes).await.map_err(Error::Storage)?;
                return Ok(None);
            };
            match store.put_if_match(key, etag, body_bytes.clone()).await {
                Ok(new_etag) => Ok(new_etag),
                Err(StorageError::PreconditionFailed) => {
                    if live_body_matches(store, key, &body_bytes).await? {
                        Ok(None)
                    } else {
                        Err(Error::PartialCommit)
                    }
                }
                Err(e) => Err(Error::Storage(e)),
            }
        }
        MutationRecord::PutIfAbsent { key, body_ref } => {
            let body = match store.get(body_ref).await {
                Ok(b) => b,
                Err(StorageError::NotFound) => return Ok(None),
                Err(e) => return Err(Error::Storage(e)),
            };
            match store.put_if_absent(key, Bytes::from(body)).await {
                Ok(new_etag) => Ok(new_etag),
                Err(StorageError::PreconditionFailed) => Ok(None),
                Err(e) => Err(Error::Storage(e)),
            }
        }
        MutationRecord::Delete { key, expected } => match expected {
            Some(etag) => match store.delete_if_match(key, etag).await {
                Ok(()) | Err(StorageError::PreconditionFailed) => Ok(None),
                Err(e) => Err(Error::Storage(e)),
            },
            None => match store.delete(key).await {
                Ok(()) | Err(StorageError::NotFound) => Ok(None),
                Err(e) => Err(Error::Storage(e)),
            },
        },
        MutationRecord::Copy { src, dst } => {
            store.copy(src, dst).await.map_err(Error::Storage)?;
            Ok(None)
        }
        MutationRecord::Move { src, dst } => {
            store.copy(src, dst).await.map_err(Error::Storage)?;
            match store.delete(src).await {
                Ok(()) | Err(StorageError::NotFound) => Ok(None),
                Err(e) => Err(Error::Storage(e)),
            }
        }
    }
}

/// Return `true` when the live object at `key` has the same SHA-256 hash as
/// `expected_body`. `NotFound` counts as no match (nothing landed).
///
/// Used by both the CAS executor's partial-commit handler and the `RecoveryLoop`
/// to distinguish stale stamps from true contention.
///
/// # Errors
///
/// Returns `Err(Error::Storage(...))` on hard storage errors.
pub async fn live_body_matches(
    store: &dyn ConditionalStore,
    key: &str,
    expected_body: &Bytes,
) -> Result<bool, Error> {
    match store.get(key).await {
        Ok(live) => {
            let live_hash: [u8; 32] = Sha256::digest(&live).into();
            let want_hash: [u8; 32] = Sha256::digest(expected_body).into();
            Ok(live_hash == want_hash)
        }
        Err(StorageError::NotFound) => Ok(false),
        Err(e) => Err(Error::Storage(e)),
    }
}

#[async_trait]
impl TransactionExecutor for CasExecutor {
    /// Drive `tx` through Build → Prepare → Commit-intent → Apply → Reap using
    /// conditional storage operations.
    ///
    /// The CAS executor relies on storage-level conditional operations and
    /// does not acquire a transaction-scoped lock. Any caller-held
    /// [`LockSession`] is independent of this call; the caller releases it
    /// explicitly after `execute` returns.
    async fn execute(&self, tx: Transaction) -> Result<Outcome, Error> {
        let tx_id = Uuid::new_v4();

        self.prepare_reads(&tx).await?;

        let mutation_records = stage_bodies(self.store.as_ref(), &tx, tx_id).await?;

        let read_records: Vec<ReadRecord> = tx
            .reads
            .iter()
            .map(|r| ReadRecord {
                key: r.key.clone(),
                fingerprint: hex::encode(r.fingerprint),
            })
            .collect();

        // CAS executor takes no transaction-scoped lock for its working set.
        // When the caller declares coarse lock keys (e.g. `blob-data:{digest}`),
        // acquire only those, hold across Apply, release at Reap.
        let coarse_session = if tx.coarse_lock_keys.is_empty() {
            None
        } else {
            let mut keys = tx.coarse_lock_keys.clone();
            keys.sort();
            keys.dedup();
            Some(self.lock.acquire(&keys).await.map_err(Error::Lock)?)
        };

        let progress = vec![MutationProgress::Pending; mutation_records.len()];
        let mut intent = IntentRecord {
            id: tx_id,
            created_at: Utc::now(),
            ttl_secs: self.ttl_secs,
            reads: read_records,
            mutations: mutation_records,
            coarse_lock_keys: tx.coarse_lock_keys.clone(),
            progress,
        };
        if let Err(e) = write_intent(self.store.as_ref(), &intent).await {
            if let Some(session) = coarse_session {
                session.release().await;
            }
            return Err(e);
        }

        let apply_result = self.apply_all(&mut intent).await;

        // Only reap when the apply path completed cleanly (all mutations
        // landed) or failed in a way that still left the intent in a consistent
        // state. `PartialCommit` means the intent was partially applied and
        // true contention stopped the loop — the intent MUST stay in `tx-log/`
        // so the recovery loop can converge it.
        let is_partial_commit = matches!(apply_result, Err(Error::PartialCommit));
        if !is_partial_commit {
            reap(self.store.as_ref(), &intent).await;
        }

        if let Some(session) = coarse_session {
            session.release().await;
        }

        apply_result?;
        Ok(Outcome { tx_id })
    }

    async fn try_acquire(&self, keys: &[String]) -> Result<Option<LockSession>, Error> {
        self.lock.try_acquire(keys).await.map_err(Error::Lock)
    }

    async fn acquire(&self, keys: &[String]) -> Result<LockSession, Error> {
        self.lock.acquire(keys).await.map_err(Error::Lock)
    }

    fn lock(&self) -> Arc<Lock> {
        Arc::clone(&self.lock)
    }

    fn conditional_store(&self) -> Option<Arc<dyn ConditionalStore>> {
        Some(Arc::clone(&self.store))
    }
}
