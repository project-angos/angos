//! Shared helpers used by both the CAS and Locked executors.
//!
//! Each function takes `&dyn ObjectStore` (the lowest common trait) so it works
//! with both `ConditionalStore` (CAS executor) and plain `ObjectStore` (Locked
//! executor): both traits are supertraits of `ObjectStore`.

use bytes::Bytes;
use chrono::Utc;
use serde_json::Value;
use tracing::warn;

use angos_storage::{Error as StorageError, Etag, ObjectStore};

use crate::{
    error::Error,
    intent::{IntentRecord, MutationProgress, MutationRecord, ReadRecord, body_ref_key},
    transaction::{Mutation, Read, Transaction},
};

use uuid::Uuid;

/// Serialise `intent` to JSON and PUT it at its canonical log key.
///
/// # Errors
///
/// Returns [`Error::Serde`] if serialisation fails or [`Error::Storage`] if the
/// PUT operation fails.
pub async fn write_intent(store: &dyn ObjectStore, intent: &IntentRecord) -> Result<(), Error> {
    let key = intent.log_key();
    let body = serde_json::to_vec(intent)?;
    store.put(&key, Bytes::from(body)).await?;
    Ok(())
}

/// Mark mutation `idx` as `Applied` and re-PUT the intent record.
///
/// The in-memory mark lands before the write and survives its failure, which
/// [`finish`] depends on: a lost stamp must still leave the transaction looking
/// committed to this process, or an error on a later mutation would reap the
/// intent out from under a canonical write that already applied.
///
/// # Errors
///
/// Propagates any error from [`write_intent`].
pub async fn stamp_progress(
    store: &dyn ObjectStore,
    intent: &mut IntentRecord,
    idx: usize,
) -> Result<(), Error> {
    intent.mark_applied(idx);
    write_intent(store, intent).await
}

/// Mark mutation `idx` `Applied` and re-PUT the intent, warning (not failing)
/// when the stamp write fails. The recovery loop re-applies idempotently if a
/// stamp is lost. Shared by both executors' per-mutation apply loops.
pub async fn stamp_applied(store: &dyn ObjectStore, intent: &mut IntentRecord, idx: usize) {
    let tx_id = intent.id;
    if let Err(stamp_err) = stamp_progress(store, intent, idx).await {
        warn!(
            tx_id = %tx_id,
            idx,
            error = %stamp_err,
            "Failed to stamp mutation progress; recovery will re-apply idempotently"
        );
    }
}

/// Stage `Put`/`PutIfAbsent` bodies at `.tx-bodies/<tx_id>/<idx>` and return
/// the matching [`MutationRecord`]s for the intent.
///
/// # Errors
///
/// Returns [`Error::Storage`] if any body PUT fails.
pub async fn stage_bodies(
    store: &dyn ObjectStore,
    tx: &Transaction,
    tx_id: Uuid,
) -> Result<Vec<MutationRecord>, Error> {
    let mut records = Vec::with_capacity(tx.mutations.len());
    for (idx, mutation) in tx.mutations.iter().enumerate() {
        let record = match mutation {
            Mutation::Put {
                key,
                body,
                expected,
            } => {
                let body_ref = body_ref_key(tx_id, idx);
                store
                    .put(&body_ref, body.clone())
                    .await
                    .map_err(Error::Storage)?;
                MutationRecord::Put {
                    key: key.clone(),
                    body_ref,
                    expected: expected.clone(),
                }
            }
            Mutation::PutIfAbsent { key, body } => {
                let body_ref = body_ref_key(tx_id, idx);
                store
                    .put(&body_ref, body.clone())
                    .await
                    .map_err(Error::Storage)?;
                MutationRecord::PutIfAbsent {
                    key: key.clone(),
                    body_ref,
                }
            }
            Mutation::Delete { key, expected } => MutationRecord::Delete {
                key: key.clone(),
                expected: expected.clone(),
            },
            Mutation::Copy { src, dst } => MutationRecord::Copy {
                src: src.clone(),
                dst: dst.clone(),
            },
            Mutation::Move { src, dst } => MutationRecord::Move {
                src: src.clone(),
                dst: dst.clone(),
            },
            Mutation::MergeSet { key, add, remove } => MutationRecord::MergeSet {
                key: key.clone(),
                add: add.clone(),
                remove: remove.clone(),
            },
        };
        records.push(record);
    }
    Ok(records)
}

/// Assemble the [`IntentRecord`] written at the Commit-intent stage.
///
/// Folds in the read-records mapping (each [`Read`]'s key plus its hex-encoded
/// fingerprint) and initialises a `Pending` progress slot per mutation. Shared
/// by both executors so the intent literal lives once.
#[must_use]
pub fn build_intent(
    tx_id: Uuid,
    ttl_secs: u64,
    reads: &[Read],
    mutations: Vec<MutationRecord>,
    coarse_lock_keys: Vec<String>,
) -> IntentRecord {
    let read_records: Vec<ReadRecord> = reads
        .iter()
        .map(|r| ReadRecord {
            key: r.key.clone(),
            fingerprint: hex::encode(r.fingerprint),
        })
        .collect();
    let progress = vec![MutationProgress::Pending; mutations.len()];
    IntentRecord {
        id: tx_id,
        created_at: Utc::now(),
        ttl_secs,
        reads: read_records,
        mutations,
        coarse_lock_keys,
        progress,
    }
}

/// Apply a `Move` idempotently: copy `src` to `dst`, then delete `src`
/// tolerating a missing source.
///
/// Shared by both appliers' reconcile paths ([`apply_object_store`] and
/// [`super::cas::apply_cas`]) so the idempotent Move shape stays in lock-step.
/// The `copy` still propagates errors; only a `NotFound` on the source delete is
/// swallowed so re-application after a partial Move converges.
///
/// # Errors
///
/// Returns the underlying [`angos_storage::Error`] from `copy`, or from
/// `delete` when it fails with anything other than `NotFound`.
pub async fn move_idempotent(
    store: &dyn ObjectStore,
    src: &str,
    dst: &str,
) -> Result<(), StorageError> {
    store.copy(src, dst).await?;
    match store.delete(src).await {
        Ok(()) | Err(StorageError::NotFound) => Ok(()),
        Err(e) => Err(e),
    }
}

/// Merge `add`/`remove` into the JSON-array set held in `current` (an empty
/// slice for an absent key), returning the re-serialised body or `None` when the
/// set becomes empty and the key should be deleted.
///
/// Members compare by structural JSON equality. `add` and `remove` are assumed
/// disjoint, so apply order does not matter. Shared by both appliers so the
/// CAS retry loop and the locked read-modify-write stay in lock-step.
///
/// # Errors
///
/// Returns [`serde_json::Error`] when `current` is not a JSON array or the
/// merged set fails to serialise.
pub fn merge_json_set(
    current: &[u8],
    add: &[Value],
    remove: &[Value],
) -> Result<Option<Bytes>, serde_json::Error> {
    let mut members: Vec<Value> = if current.is_empty() {
        Vec::new()
    } else {
        serde_json::from_slice(current)?
    };
    members.retain(|member| !remove.contains(member));
    for member in add {
        if !members.contains(member) {
            members.push(member.clone());
        }
    }
    if members.is_empty() {
        return Ok(None);
    }
    Ok(Some(Bytes::from(serde_json::to_vec(&members)?)))
}

/// Apply a [`MutationRecord::MergeSet`] against a plain [`ObjectStore`] under the
/// caller's lock: read the current set, merge, then write or delete. Idempotent
/// in both modes, so recovery replays it unchanged.
///
/// # Errors
///
/// Returns [`Error::Serde`] when the stored body is not a JSON array, or
/// [`Error::Storage`] on a hard storage error.
async fn apply_merge_set_object(
    store: &dyn ObjectStore,
    key: &str,
    add: &[Value],
    remove: &[Value],
) -> Result<(), Error> {
    let current = match store.get(key).await {
        Ok(body) => body,
        Err(StorageError::NotFound) => Vec::new(),
        Err(e) => return Err(Error::Storage(e)),
    };
    match merge_json_set(&current, add, remove)? {
        Some(body) => store.put(key, body).await.map_err(Error::Storage),
        None => match store.delete(key).await {
            Ok(()) | Err(StorageError::NotFound) => Ok(()),
            Err(e) => Err(Error::Storage(e)),
        },
    }
}

/// Per-key precondition-failure semantics shared by [`apply_object_store`] and
/// [`super::cas::apply_cas`]: the op dispatch is identical across both modes,
/// only the handling of a failed precondition (and of an absent target)
/// differs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyMode {
    /// Healthy-path apply: a failed precondition is a hard
    /// `Err(Error::Precondition)` and an absent staged body or delete target
    /// propagates as a storage error.
    Abort,
    /// Recovery reconcile / replay-forward path: already-applied outcomes are
    /// `Ok(())`; only true contention (a live body differing from the staged
    /// body under CAS) is `Err(Error::PartialCommit)`.
    Reconcile,
}

/// Apply one mutation against a plain [`ObjectStore`] (the Locked-executor world,
/// which has no conditional store). Conditional `Put`/`Delete` are honored via a
/// HEAD/ETag compare under the caller's lock. The conditional-store equivalent is
/// [`super::cas::apply_cas`]; keeping both as mode-parameterized appliers stops
/// the `Move`/`PutIfAbsent`/precondition semantics from drifting.
///
/// # Errors
///
/// In `Abort`, returns [`Error::Precondition`] when a conditional `Put`/`Delete`
/// or a `PutIfAbsent` finds the key in the wrong state. Either mode returns
/// [`Error::Storage`] on a hard storage error.
pub async fn apply_object_store(
    store: &dyn ObjectStore,
    mutation: &MutationRecord,
    mode: ApplyMode,
) -> Result<(), Error> {
    match mutation {
        MutationRecord::Put {
            key,
            body_ref,
            expected,
        } => {
            if mode == ApplyMode::Abort
                && let Some(etag) = expected
            {
                check_expected_match(store, key, etag).await?;
            }
            match store.get(body_ref).await {
                Ok(body) => store
                    .put(key, Bytes::from(body))
                    .await
                    .map_err(Error::Storage),
                // Reconcile: a reaped staging body means the canonical write already landed.
                Err(StorageError::NotFound) if mode == ApplyMode::Reconcile => Ok(()),
                Err(e) => Err(Error::Storage(e)),
            }
        }
        MutationRecord::PutIfAbsent { key, body_ref } => match store.head(key).await {
            // Abort: the key exists, so the precondition fails. Reconcile: that is
            // the expected idempotent outcome of a replayed insert.
            Ok(_) => match mode {
                ApplyMode::Abort => Err(Error::Precondition),
                ApplyMode::Reconcile => Ok(()),
            },
            Err(StorageError::NotFound) => match store.get(body_ref).await {
                Ok(body) => store
                    .put(key, Bytes::from(body))
                    .await
                    .map_err(Error::Storage),
                Err(StorageError::NotFound) if mode == ApplyMode::Reconcile => Ok(()),
                Err(e) => Err(Error::Storage(e)),
            },
            Err(e) => Err(Error::Storage(e)),
        },
        MutationRecord::Delete { key, expected } => {
            if mode == ApplyMode::Abort
                && let Some(etag) = expected
            {
                match store.head(key).await {
                    Ok(meta) => {
                        if meta.etag.as_ref() != Some(etag) {
                            return Err(Error::Precondition);
                        }
                    }
                    // Already gone: a conditional delete is a no-op success.
                    Err(StorageError::NotFound) => return Ok(()),
                    Err(e) => return Err(Error::Storage(e)),
                }
            }
            match store.delete(key).await {
                Ok(()) => Ok(()),
                Err(StorageError::NotFound) if mode == ApplyMode::Reconcile => Ok(()),
                Err(e) => Err(Error::Storage(e)),
            }
        }
        MutationRecord::Copy { src, dst } => store.copy(src, dst).await.map_err(Error::Storage),
        MutationRecord::Move { src, dst } => match mode {
            ApplyMode::Abort => store.move_object(src, dst).await.map_err(Error::Storage),
            ApplyMode::Reconcile => move_idempotent(store, src, dst)
                .await
                .map_err(Error::Storage),
        },
        MutationRecord::MergeSet { key, add, remove } => {
            apply_merge_set_object(store, key, add, remove).await
        }
    }
}

/// HEAD `key` and require its current `ETag` to equal `expected`, mirroring the
/// CAS executor's `put_if_match` / `delete_if_match`. A missing key, an `ETag`
/// mismatch, or a backend that cannot surface an `ETag` all yield
/// [`Error::Precondition`] (no write).
async fn check_expected_match(
    store: &dyn ObjectStore,
    key: &str,
    expected: &Etag,
) -> Result<(), Error> {
    match store.head(key).await {
        Ok(meta) if meta.etag.as_ref() == Some(expected) => Ok(()),
        Ok(_) | Err(StorageError::NotFound) => Err(Error::Precondition),
        Err(e) => Err(Error::Storage(e)),
    }
}

/// Which cleanup path is running, selecting the warn labels and the
/// prefix-delete-failure behaviour for [`cleanup`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CleanupMode {
    /// Committed transaction. If the body-prefix delete fails the intent log is
    /// left intact so the recovery loop can retry the full reap on the next
    /// sweep (early return before deleting the log entry).
    Reap,
    /// Failed transaction with no applied mutations. A body-prefix delete
    /// failure is warned but cleanup proceeds to delete the intent log entry.
    Rollback,
}

/// Delete a transaction's staged body objects, then its intent log entry.
///
/// `mode` selects the warn labels and the prefix-delete-failure behaviour:
/// see [`CleanupMode`].
async fn cleanup(store: &dyn ObjectStore, intent: &IntentRecord, mode: CleanupMode) {
    let prefix = intent.bodies_prefix();
    if let Err(e) = store.delete_prefix(&prefix).await {
        match mode {
            CleanupMode::Reap => {
                warn!(
                    tx_id = %intent.id,
                    prefix,
                    error = %e,
                    "Reap: failed to delete body staging objects; intent left for recovery"
                );
                return;
            }
            CleanupMode::Rollback => {
                warn!(
                    tx_id = %intent.id,
                    prefix,
                    error = %e,
                    "Rollback: failed to delete body staging objects"
                );
            }
        }
    }
    let log_key = intent.log_key();
    if let Err(e) = store.delete(&log_key).await {
        match mode {
            CleanupMode::Reap => warn!(
                tx_id = %intent.id,
                key = log_key,
                error = %e,
                "Reap: failed to delete intent log entry"
            ),
            CleanupMode::Rollback => warn!(
                tx_id = %intent.id,
                key = log_key,
                error = %e,
                "Rollback: failed to delete intent log entry"
            ),
        }
    }
}

/// Reap a committed transaction: delete body staging objects, then the intent
/// log entry.
///
/// If deleting the body prefix fails the intent log is left intact so the
/// recovery loop can retry the full reap on the next sweep.
pub async fn reap(store: &dyn ObjectStore, intent: &IntentRecord) {
    cleanup(store, intent, CleanupMode::Reap).await;
}

/// Roll back a failed transaction: delete staged body objects and the intent
/// log entry.
pub async fn rollback(store: &dyn ObjectStore, intent: &IntentRecord) {
    cleanup(store, intent, CleanupMode::Rollback).await;
}

/// Run the Apply-stage reap gate shared by both executors.
///
/// Reap only when the transaction either fully committed (`apply_result` is
/// `Ok`) or applied nothing (`!intent.any_applied()`). Once any mutation has
/// applied but the transaction did not finish, the intent is preserved so the
/// recovery loop can replay-forward idempotently and converge; reaping here
/// would orphan the partial canonical write. On the `Precondition` +
/// nothing-applied path the executor has already rolled back, so reaping
/// deleted objects here is a harmless no-op.
pub async fn finish(
    store: &dyn ObjectStore,
    apply_result: &Result<(), Error>,
    intent: &IntentRecord,
) {
    if apply_result.is_ok() || !intent.any_applied() {
        reap(store, intent).await;
    }
}
