//! Shared helpers used by both the CAS and Locked executors.
//!
//! Each function takes `&dyn ObjectStore` (the lowest common trait) so it works
//! with both `ConditionalStore` (CAS executor) and plain `ObjectStore` (Locked
//! executor): both traits are supertraits of `ObjectStore`.

use bytes::Bytes;
use chrono::Utc;
use tracing::warn;

use angos_storage::ObjectStore;

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
/// Shared by the replay-forward paths (the CAS idempotent applier and the
/// unconditional recovery applier) so the idempotent Move shape stays in
/// lock-step. The `copy` still propagates errors; only a `NotFound` on the
/// source delete is swallowed so re-application after a partial Move converges.
///
/// # Errors
///
/// Returns the underlying [`angos_storage::Error`] from `copy`, or from
/// `delete` when it fails with anything other than `NotFound`.
pub async fn move_idempotent(
    store: &dyn ObjectStore,
    src: &str,
    dst: &str,
) -> Result<(), angos_storage::Error> {
    store.copy(src, dst).await?;
    match store.delete(src).await {
        Ok(()) | Err(angos_storage::Error::NotFound) => Ok(()),
        Err(e) => Err(e),
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
