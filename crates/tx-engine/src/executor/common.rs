//! Shared helpers used by both the CAS and Locked executors.
//!
//! Each function takes `&dyn ObjectStore` (the lowest common trait) so it works
//! with both `ConditionalStore` (CAS executor) and plain `ObjectStore` (Locked
//! executor) — both traits are supertraits of `ObjectStore`.

use bytes::Bytes;
use tracing::warn;

use angos_storage::ObjectStore;

use crate::{
    error::Error,
    intent::{IntentRecord, MutationRecord},
    transaction::{Mutation, Transaction},
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

/// Mark mutation `idx` as `Applied { etag }` and re-PUT the intent record.
///
/// # Errors
///
/// Propagates any error from [`write_intent`].
pub async fn stamp_progress(
    store: &dyn ObjectStore,
    intent: &mut IntentRecord,
    idx: usize,
    etag: Option<angos_storage::Etag>,
) -> Result<(), Error> {
    intent.mark_applied(idx, etag);
    write_intent(store, intent).await
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
                let body_ref = format!(".tx-bodies/{tx_id}/{idx}");
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
                let body_ref = format!(".tx-bodies/{tx_id}/{idx}");
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

/// Reap a committed transaction: delete body staging objects, then the intent
/// log entry.
///
/// If deleting the body prefix fails the intent log is left intact so the
/// recovery loop can retry the full reap on the next sweep.
pub async fn reap(store: &dyn ObjectStore, intent: &IntentRecord) {
    let prefix = intent.bodies_prefix();
    if let Err(e) = store.delete_prefix(&prefix).await {
        warn!(
            tx_id = %intent.id,
            prefix,
            error = %e,
            "Reap: failed to delete body staging objects; intent left for recovery"
        );
        return;
    }
    let log_key = intent.log_key();
    if let Err(e) = store.delete(&log_key).await {
        warn!(
            tx_id = %intent.id,
            key = log_key,
            error = %e,
            "Reap: failed to delete intent log entry"
        );
    }
}

/// Roll back a failed transaction: delete staged body objects and the intent
/// log entry.
pub async fn rollback(store: &dyn ObjectStore, intent: &IntentRecord) {
    let prefix = intent.bodies_prefix();
    if let Err(e) = store.delete_prefix(&prefix).await {
        warn!(
            tx_id = %intent.id,
            prefix,
            error = %e,
            "Rollback: failed to delete body staging objects"
        );
    }
    let log_key = intent.log_key();
    if let Err(e) = store.delete(&log_key).await {
        warn!(
            tx_id = %intent.id,
            key = log_key,
            error = %e,
            "Rollback: failed to delete intent log entry"
        );
    }
}
