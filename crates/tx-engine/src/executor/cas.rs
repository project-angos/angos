//! CAS executor: records etags at Prepare, writes the intent, applies mutations
//! with `put_if_match`/`put_if_absent`/`delete_if_match`, and rolls back on
//! `PreconditionFailed` when no mutations have been applied yet.
//!
//! When a CAS precondition fails after at least one mutation has already been
//! applied (partial-commit case), the executor uses `apply_cas` in
//! `Reconcile` mode (the same stale-stamp recovery logic the `RecoveryLoop`
//! uses) to distinguish
//! between a healthy-path write that landed without its stamp (the live body
//! matches the staged body, so stamp and continue) versus true contention (live
//! body differs, so return `Error::PartialCommit` and preserve the intent for
//! the recovery loop).

use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::Arc,
};

use async_trait::async_trait;
use bytes::Bytes;
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use tracing::{debug, warn};
use uuid::Uuid;

use angos_storage::{ConditionalStore, Error as StorageError, Etag};

use crate::{
    error::Error,
    executor::{
        Outcome, TransactionExecutor, common,
        common::{
            ApplyMode, build_intent, finish, rollback, stage_bodies, stamp_applied, stamp_progress,
            write_intent,
        },
    },
    intent::{DEFAULT_INTENT_TTL_SECS, IntentRecord, MutationRecord},
    lock::primitive::Lock,
    transaction::{Transaction, lock_key_set},
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
/// The `lock` serialises only the caller-declared coarse lock keys (e.g.
/// `blob-data:{digest}`) across Apply; the working set itself is coordinated
/// purely by the conditional operations.
///
/// Constructed via [`CasExecutor::builder`].
pub struct CasExecutor {
    store: Arc<dyn ConditionalStore>,
    lock: Arc<Lock>,
    ttl_secs: u64,
}

impl std::fmt::Debug for CasExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CasExecutor")
            .field("ttl_secs", &self.ttl_secs)
            .finish_non_exhaustive()
    }
}

/// Builder for [`CasExecutor`]. The conditional store and lock are required and
/// supplied to [`CasExecutor::builder`]; the intent TTL is an optional fluent
/// setter.
pub struct CasExecutorBuilder {
    store: Arc<dyn ConditionalStore>,
    lock: Arc<Lock>,
    ttl_secs: Option<u64>,
}

impl CasExecutorBuilder {
    /// Set the intent TTL in seconds. Defaults to 300.
    #[must_use]
    pub fn ttl_secs(mut self, secs: u64) -> Self {
        self.ttl_secs = Some(secs);
        self
    }

    /// Consume the builder and produce a [`CasExecutor`].
    #[must_use]
    pub fn build(self) -> CasExecutor {
        CasExecutor {
            store: self.store,
            lock: self.lock,
            ttl_secs: self.ttl_secs.unwrap_or(DEFAULT_INTENT_TTL_SECS),
        }
    }
}

impl CasExecutor {
    /// Return a builder wrapping the conditional `store` and the `lock` that
    /// serialises coarse lock keys. The intent TTL is an optional fluent
    /// setter on the returned builder.
    #[must_use]
    pub fn builder(store: Arc<dyn ConditionalStore>, lock: Arc<Lock>) -> CasExecutorBuilder {
        CasExecutorBuilder {
            store,
            lock,
            ttl_secs: None,
        }
    }

    /// Verify the read set, capturing each present read key's live etag and
    /// each key a read confirmed absent.
    ///
    /// Re-reads every read key, checks the content fingerprint recorded at
    /// build time (returning [`Error::Conflict`] on mismatch or if a
    /// present-expecting key has vanished). The etags let the caller turn a
    /// same-key write into a compare-and-swap; the absent set lets it turn a
    /// same-key write into a `PutIfAbsent` so the absence precondition holds
    /// through Apply, not just Prepare.
    async fn prepare_reads(&self, tx: &Transaction) -> Result<PreparedReads, Error> {
        let mut prepared = PreparedReads::default();
        for read in &tx.reads {
            match self.store.get_with_etag(&read.key).await {
                Ok((body, etag)) => {
                    let actual: [u8; 32] = Sha256::digest(&body).into();
                    if actual != read.fingerprint {
                        debug!(
                            key = read.key,
                            "CAS executor: content hash mismatch at Prepare, signalling Conflict"
                        );
                        return Err(Error::Conflict);
                    }
                    if let Some(etag) = etag {
                        prepared.etags.insert(read.key.clone(), etag);
                    }
                }
                Err(StorageError::NotFound) => {
                    // An absent key matches only a read that recorded absence.
                    if !read.expects_absent() {
                        return Err(Error::Conflict);
                    }
                    prepared.absent.insert(read.key.clone());
                }
                Err(e) => return Err(Error::Storage(e)),
            }
        }
        Ok(prepared)
    }

    /// Apply all mutations in the intent.
    ///
    /// Returns `Ok(())` after all mutations are applied. On `Precondition`
    /// failure before any mutation has been applied, rolls back and returns
    /// `Err(Error::Precondition)`. A merge that exhausts its attempt budget
    /// before any mutation has been applied likewise rolls back, returning
    /// `Err(Error::Conflict)` so the caller's retry loop re-runs the whole
    /// transaction. On `Precondition` failure mid-Apply (at
    /// least one mutation already stamped `Applied`), uses the stale-stamp
    /// recovery heuristic:
    ///
    /// - If the live body at the failing key matches the staged body, the
    ///   healthy-path write already landed without its stamp. The slot is
    ///   marked `Applied` and the loop continues to the next mutation.
    /// - If the live body does not match, this is true contention. The intent
    ///   is left in `.tx-log/` for the recovery loop and
    ///   `Err(Error::PartialCommit)` is returned so the caller skips `reap`.
    async fn apply_all(&self, intent: &mut IntentRecord) -> Result<(), Error> {
        for idx in 0..intent.mutations.len() {
            let mutation = intent.mutations[idx].clone();
            match apply_cas(self.store.as_ref(), &mutation, ApplyMode::Abort).await {
                Ok(()) => stamp_applied(self.store.as_ref(), intent, idx).await,
                Err(Error::Precondition) => {
                    if intent.any_applied() {
                        // Committed (a slot is Applied): reconcile this slot,
                        // continuing forward when it recovers and propagating
                        // `PartialCommit` on true contention.
                        self.recover_mid_apply(intent, idx, &mutation).await?;
                    } else {
                        rollback(self.store.as_ref(), intent).await;
                        return Err(Error::Precondition);
                    }
                }
                Err(Error::PartialCommit) if !intent.any_applied() => {
                    // Merge attempt budget exhausted with nothing applied yet:
                    // roll back and signal a retriable conflict instead.
                    rollback(self.store.as_ref(), intent).await;
                    return Err(Error::Conflict);
                }
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Reconcile mutation `idx` after a mid-Apply precondition failure on an
    /// already-committed transaction, applying the stale-stamp recovery
    /// heuristic: a `Reconcile` apply that succeeds means the healthy-path write
    /// landed without its stamp, so stamp the slot and let the caller continue
    /// forward; a `PartialCommit` means true contention (the live body differs
    /// from the staged body), so leave the intent for the recovery loop.
    async fn recover_mid_apply(
        &self,
        intent: &mut IntentRecord,
        idx: usize,
        mutation: &MutationRecord,
    ) -> Result<(), Error> {
        match apply_cas(self.store.as_ref(), mutation, ApplyMode::Reconcile).await {
            Ok(()) => {
                if let Err(stamp_err) = stamp_progress(self.store.as_ref(), intent, idx).await {
                    warn!(
                        tx_id = %intent.id,
                        idx,
                        error = %stamp_err,
                        "Failed to stamp stale-stamp-recovered mutation; \
                         recovery will re-apply idempotently"
                    );
                }
                Ok(())
            }
            Err(Error::PartialCommit) => {
                warn!(
                    tx_id = %intent.id,
                    idx,
                    "CAS true contention mid-Apply; leaving intent for recovery loop"
                );
                Err(Error::PartialCommit)
            }
            Err(e) => Err(e),
        }
    }
}

/// Verified read set: live etags for keys read as present, and the keys a read
/// confirmed absent.
#[derive(Default)]
struct PreparedReads {
    etags: HashMap<String, Etag>,
    absent: HashSet<String>,
}

/// Promote same-key read-modify-write mutations to conditional writes and order
/// them ahead of unconditional mutations.
///
/// A `Put`/`Delete` on a key read as *present* with no explicit precondition is
/// conditioned on the etag captured at Prepare; a `Put` on a key read as
/// *absent* becomes a `PutIfAbsent`. Either way the precondition holds through
/// Apply, so a racing write in the Prepare-to-Apply window aborts the transaction
/// before any sibling mutation commits and the caller's retry loop re-reads.
/// The read-keyed mutations are stably moved to the front so that abort lands
/// clean.
fn apply_read_preconditions(records: &mut [MutationRecord], reads: &PreparedReads) {
    if reads.etags.is_empty() && reads.absent.is_empty() {
        return;
    }
    for record in records.iter_mut() {
        match record {
            MutationRecord::Put { key, expected, .. }
            | MutationRecord::Delete { key, expected }
                if expected.is_none() =>
            {
                if let Some(etag) = reads.etags.get(key) {
                    *expected = Some(etag.clone());
                }
            }
            _ => {}
        }
        if let MutationRecord::Put {
            key,
            body_ref,
            expected: None,
        } = record
            && reads.absent.contains(key)
        {
            *record = MutationRecord::PutIfAbsent {
                key: key.clone(),
                body_ref: body_ref.clone(),
            };
        }
    }
    records.sort_by_key(|record| u8::from(!is_read_keyed(record, reads)));
}

/// `true` when any key the mutation touches was part of the read set.
fn is_read_keyed(record: &MutationRecord, reads: &PreparedReads) -> bool {
    record
        .all_keys()
        .any(|key| reads.etags.contains_key(key) || reads.absent.contains(key))
}

/// Apply a single mutation using conditional storage operations.
///
/// The op dispatch is shared by the CAS executor's healthy apply path
/// ([`ApplyMode::Abort`]) and by both the CAS executor's partial-commit
/// handler and the `RecoveryLoop`'s replay path ([`ApplyMode::Reconcile`]);
/// `mode` selects the per-key precondition-failure semantics (see
/// [`ApplyMode`]).
///
/// In `Reconcile` mode, on a `PreconditionFailed` for a conditional `Put` the
/// live body's SHA-256 hash is compared against the staged body. A match means
/// the healthy-path write already landed (a stale progress stamp); the mutation
/// is treated as applied and `Ok(())` is returned. A mismatch means true
/// contention: `Err(Error::PartialCommit)` is returned so the caller stops
/// replay and leaves the intent for the next sweep.
///
/// # Errors
///
/// In `Abort` mode, returns `Err(Error::Precondition)` when an etag
/// precondition was not met. In `Reconcile` mode, returns
/// `Err(Error::PartialCommit)` on true contention (live body differs from the
/// staged body). Either mode returns `Err(Error::Storage(...))` on hard storage
/// errors.
pub async fn apply_cas(
    store: &dyn ConditionalStore,
    mutation: &MutationRecord,
    mode: ApplyMode,
) -> Result<(), Error> {
    match mutation {
        MutationRecord::Put {
            key,
            body_ref,
            expected,
        } => {
            let Some(body_bytes) = fetch_staged_body(store, body_ref, mode).await? else {
                return Ok(());
            };
            let Some(etag) = expected else {
                store.put(key, body_bytes).await.map_err(Error::Storage)?;
                return Ok(());
            };
            match store.put_if_match(key, etag, body_bytes.clone()).await {
                Ok(_) => Ok(()),
                Err(StorageError::PreconditionFailed) => match mode {
                    ApplyMode::Abort => Err(Error::Precondition),
                    ApplyMode::Reconcile => {
                        if live_body_matches(store, key, &body_bytes).await? {
                            Ok(())
                        } else {
                            Err(Error::PartialCommit)
                        }
                    }
                },
                Err(e) => Err(Error::Storage(e)),
            }
        }
        MutationRecord::PutIfAbsent { key, body_ref } => {
            let Some(body_bytes) = fetch_staged_body(store, body_ref, mode).await? else {
                return Ok(());
            };
            match store.put_if_absent(key, body_bytes).await {
                Ok(_) => Ok(()),
                Err(StorageError::PreconditionFailed) => match mode {
                    ApplyMode::Abort => Err(Error::Precondition),
                    ApplyMode::Reconcile => Ok(()),
                },
                Err(e) => Err(Error::Storage(e)),
            }
        }
        MutationRecord::Delete { key, expected } => match expected {
            Some(etag) => match store.delete_if_match(key, etag).await {
                Ok(()) => Ok(()),
                Err(StorageError::PreconditionFailed) => match mode {
                    ApplyMode::Abort => Err(Error::Precondition),
                    ApplyMode::Reconcile => Ok(()),
                },
                Err(e) => Err(Error::Storage(e)),
            },
            None => match store.delete(key).await {
                Ok(()) => Ok(()),
                Err(StorageError::NotFound) if mode == ApplyMode::Reconcile => Ok(()),
                Err(e) => Err(Error::Storage(e)),
            },
        },
        MutationRecord::Copy { src, dst } => {
            store.copy(src, dst).await.map_err(Error::Storage)?;
            Ok(())
        }
        MutationRecord::Move { src, dst } => match mode {
            ApplyMode::Abort => {
                store.move_object(src, dst).await.map_err(Error::Storage)?;
                Ok(())
            }
            ApplyMode::Reconcile => common::move_idempotent(store, src, dst)
                .await
                .map_err(Error::Storage),
        },
        // Mode-independent: the merge re-reads and recomputes against live state,
        // so it is idempotent on both the healthy apply and recovery replay.
        MutationRecord::MergeSet { key, add, remove } => {
            apply_merge_set_cas(store, key, add, remove).await
        }
    }
}

/// Upper bound on CAS re-reads for a single [`MutationRecord::MergeSet`] apply.
/// On exhaustion the mutation is left `Pending` and `Error::PartialCommit` is
/// returned, deferring to the recovery loop, which retries and converges once
/// contention subsides.
const MERGE_SET_MAX_ATTEMPTS: u32 = 16;

/// Apply a [`MutationRecord::MergeSet`] with conditional operations: read the
/// current set with its etag, merge, and commit with `put_if_match` /
/// `delete_if_match` / `put_if_absent`, re-reading on a precondition miss.
///
/// A backend that surfaces no etag for a present object falls back to an
/// unconditional write (matching how a read-keyed `Put` degrades when no etag is
/// captured).
///
/// # Errors
///
/// Returns [`Error::Serde`] on a malformed set body, [`Error::Storage`] on a hard
/// storage error, or [`Error::PartialCommit`] when the attempt budget is
/// exhausted under sustained contention.
async fn apply_merge_set_cas(
    store: &dyn ConditionalStore,
    key: &str,
    add: &[Value],
    remove: &[Value],
) -> Result<(), Error> {
    for _ in 0..MERGE_SET_MAX_ATTEMPTS {
        let outcome = match store.get_with_etag(key).await {
            Ok((current, Some(etag))) => match common::merge_json_set(&current, add, remove)? {
                Some(body) => store.put_if_match(key, &etag, body).await.map(|_| ()),
                None => store.delete_if_match(key, &etag).await,
            },
            // Present but no etag: no CAS is possible, so write unconditionally.
            Ok((current, None)) => match common::merge_json_set(&current, add, remove)? {
                Some(body) => store.put(key, body).await,
                None => store.delete(key).await,
            },
            Err(StorageError::NotFound) => match common::merge_json_set(&[], add, remove)? {
                Some(body) => store.put_if_absent(key, body).await.map(|_| ()),
                None => return Ok(()),
            },
            Err(e) => return Err(Error::Storage(e)),
        };
        match outcome {
            Ok(()) => return Ok(()),
            // Lost the CAS race: re-read and recompute on the next iteration.
            Err(StorageError::PreconditionFailed) => {}
            Err(e) => return Err(Error::Storage(e)),
        }
    }
    Err(Error::PartialCommit)
}

/// Fetch the staged body for a `Put`/`PutIfAbsent`.
///
/// Returns `Ok(Some(bytes))` with the staged body, or `Ok(None)` when the body
/// is gone and the mutation should be skipped (only in [`ApplyMode::Reconcile`],
/// where a vanished staged body means the canonical write already landed and the
/// prefix was reaped). In [`ApplyMode::Abort`] a missing body propagates as a
/// storage error.
///
/// # Errors
///
/// Returns `Err(Error::Storage(...))` on a hard storage error (and, in
/// `Abort` mode, on a `NotFound` for the staged body).
async fn fetch_staged_body(
    store: &dyn ConditionalStore,
    body_ref: &str,
    mode: ApplyMode,
) -> Result<Option<Bytes>, Error> {
    match store.get(body_ref).await {
        Ok(body) => Ok(Some(Bytes::from(body))),
        Err(StorageError::NotFound) if mode == ApplyMode::Reconcile => Ok(None),
        Err(e) => Err(Error::Storage(e)),
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
    /// Drive `tx` through Build, Prepare, Commit-intent, Apply, and Reap using
    /// conditional storage operations.
    ///
    /// The CAS executor relies on storage-level conditional operations and
    /// does not acquire a transaction-scoped lock. Any caller-held
    /// [`LockSession`](crate::lock::LockSession) is independent of this call; the caller releases it
    /// explicitly after `execute` returns.
    async fn execute(&self, tx: Transaction) -> Result<Outcome, Error> {
        let tx_id = Uuid::new_v4();

        let prepared_reads = self.prepare_reads(&tx).await?;

        let mut mutation_records = stage_bodies(self.store.as_ref(), &tx, tx_id).await?;

        // Linearise read-modify-write: a mutation whose key was read becomes a
        // compare-and-swap conditioned on the etag captured at Prepare, and is
        // ordered ahead of unconditional mutations. A losing writer then fails
        // its CAS before any sibling mutation commits, so the transaction rolls
        // back cleanly (no mutation applied yet) and the caller's retry loop
        // re-reads and converges, with no lost update and no stuck partial intent.
        apply_read_preconditions(&mut mutation_records, &prepared_reads);

        // CAS executor takes no transaction-scoped lock for its working set.
        // When the caller declares coarse lock keys (e.g. `blob-data:{digest}`),
        // acquire only those, hold across Apply, release at Reap.
        let coarse_session = if tx.coarse_lock_keys.is_empty() {
            None
        } else {
            let keys = lock_key_set(tx.coarse_lock_keys.iter().cloned());
            Some(self.lock.acquire(&keys).await.map_err(Error::Lock)?)
        };

        let mut intent = build_intent(
            tx_id,
            self.ttl_secs,
            &tx.reads,
            mutation_records,
            tx.coarse_lock_keys.clone(),
        );
        if let Err(e) = write_intent(self.store.as_ref(), &intent).await {
            if let Some(session) = coarse_session {
                session.release().await;
            }
            return Err(e);
        }

        let apply_result = self.apply_all(&mut intent).await;

        // Reap only when the transaction either fully committed or applied
        // nothing (see `common::finish`). This subsumes the `PartialCommit`
        // case (which always implies `any_applied`); on the nothing-applied
        // `Precondition`/`Conflict` paths `apply_all` already rolled back, so
        // reaping here is a harmless no-op.
        finish(self.store.as_ref(), &apply_result, &intent).await;

        if let Some(session) = coarse_session {
            session.release().await;
        }

        apply_result?;
        Ok(Outcome { tx_id })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put(key: &str) -> MutationRecord {
        MutationRecord::Put {
            key: key.to_string(),
            body_ref: format!("body/{key}"),
            expected: None,
        }
    }

    #[test]
    fn absent_read_promotes_same_key_put_to_put_if_absent() {
        // The Put carries the absence precondition through Apply, so a racing
        // create in the Prepare->Apply window aborts rather than being clobbered.
        let mut reads = PreparedReads::default();
        reads.absent.insert("tag".to_string());

        let mut records = vec![put("tag")];
        apply_read_preconditions(&mut records, &reads);

        assert!(
            matches!(
                &records[0],
                MutationRecord::PutIfAbsent { key, body_ref }
                    if key == "tag" && body_ref == "body/tag"
            ),
            "an absent-read same-key Put must become PutIfAbsent, got {:?}",
            records[0]
        );
    }

    #[test]
    fn present_read_conditions_same_key_put_on_its_etag() {
        let mut reads = PreparedReads::default();
        reads.etags.insert("tag".to_string(), Etag::new("etag-1"));

        let mut records = vec![put("tag")];
        apply_read_preconditions(&mut records, &reads);

        assert!(
            matches!(
                &records[0],
                MutationRecord::Put { expected: Some(e), .. } if *e == Etag::new("etag-1")
            ),
            "a present-read same-key Put must be conditioned on its etag, got {:?}",
            records[0]
        );
    }

    #[test]
    fn unread_key_put_stays_unconditional() {
        let reads = PreparedReads::default();

        let mut records = vec![put("other")];
        apply_read_preconditions(&mut records, &reads);

        assert!(matches!(
            &records[0],
            MutationRecord::Put { expected: None, .. }
        ));
    }
}
