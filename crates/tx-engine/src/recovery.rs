//! Recovery loop: periodically scans `.tx-log/` and replays or rolls back
//! intents whose owner's heartbeat has gone stale.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use chrono::Utc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use angos_storage::{ConditionalStore, Error as StorageError, ObjectStore};

use crate::{
    error::Error,
    executor::{
        cas::{CasApplyMode, apply_cas},
        common,
    },
    intent::{IntentRecord, MutationProgress, MutationRecord},
    lock::primitive::Lock,
    periodic::run_periodic,
    transaction::lock_key_set,
};

/// Periodic recovery loop.
///
/// On each tick the loop:
///
/// 1. Lists all entries under `.tx-log/`.
/// 2. Deserialises each intent; skips intents whose owner's heartbeat is fresh.
/// 3. For stale intents:
///    - Acquires the intent's lock set (so a still-alive owner cannot race the
///      recovery loop). If the lock cannot be taken right now, the intent is
///      left for the next sweep.
///    - If any entry in `progress` is `Applied`, the transaction is partially
///      (or fully) committed: re-apply every still-`Pending` mutation
///      idempotently — using conditional storage primitives when a
///      [`ConditionalStore`] is wired — and reap.
///    - If every entry in `progress` is `Pending`, the transaction is rolled
///      back: delete bodies and intent.
///
/// Constructed via [`RecoveryLoop::builder`].
pub struct RecoveryLoop {
    store: Arc<dyn ObjectStore>,
    conditional_store: Option<Arc<dyn ConditionalStore>>,
    lock: Option<Arc<Lock>>,
    interval: Duration,
    cancellation: CancellationToken,
}

impl std::fmt::Debug for RecoveryLoop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecoveryLoop")
            .field("interval", &self.interval)
            .field("has_lock", &self.lock.is_some())
            .field("has_conditional_store", &self.conditional_store.is_some())
            .finish_non_exhaustive()
    }
}

/// Builder for [`RecoveryLoop`].
pub struct RecoveryLoopBuilder {
    store: Arc<dyn ObjectStore>,
    conditional_store: Option<Arc<dyn ConditionalStore>>,
    lock: Option<Arc<Lock>>,
    interval: Option<Duration>,
    cancellation: Option<CancellationToken>,
}

impl RecoveryLoopBuilder {
    /// Provide the engine's lock primitive so recovery can take ownership of a
    /// stale intent before replay or rollback.
    ///
    /// Omitting this is intended only for single-process tests and direct
    /// in-process recovery scenarios; in any multi-replica deployment the lock
    /// must be wired so a still-alive owner cannot race with the takeover.
    #[must_use]
    pub fn lock(mut self, lock: Arc<Lock>) -> Self {
        self.lock = Some(lock);
        self
    }

    /// Provide the deployment's [`ConditionalStore`] so recovery's replay path
    /// can use the same conditional primitives (`put_if_match`,
    /// `put_if_absent`, `delete_if_match`) the CAS executor used on the
    /// healthy path. Omitting this falls back to unconditional ops.
    #[must_use]
    pub fn conditional_store(mut self, store: Arc<dyn ConditionalStore>) -> Self {
        self.conditional_store = Some(store);
        self
    }

    /// Set the scan interval. Defaults to 30 seconds.
    #[must_use]
    pub fn interval(mut self, interval: Duration) -> Self {
        self.interval = Some(interval);
        self
    }

    /// Set the cancellation token used to shut down the loop.
    #[must_use]
    pub fn cancellation(mut self, token: CancellationToken) -> Self {
        self.cancellation = Some(token);
        self
    }

    /// Consume the builder and produce a [`RecoveryLoop`].
    #[must_use]
    pub fn build(self) -> RecoveryLoop {
        RecoveryLoop {
            store: self.store,
            conditional_store: self.conditional_store,
            lock: self.lock,
            interval: self.interval.unwrap_or(Duration::from_secs(30)),
            cancellation: self.cancellation.unwrap_or_default(),
        }
    }
}

/// Reconstruct the lock set for an intent: reads ∪ mutation keys ∪ coarse
/// lock keys, sorted and de-duplicated. Matches what `Transaction::lock_set`
/// produces from the original `Transaction`.
fn intent_lock_set(intent: &IntentRecord) -> Vec<String> {
    lock_key_set(
        intent
            .reads
            .iter()
            .map(|r| r.key.clone())
            .chain(
                intent
                    .mutations
                    .iter()
                    .flat_map(|m| m.all_keys().map(ToOwned::to_owned)),
            )
            .chain(intent.coarse_lock_keys.iter().cloned()),
    )
}

impl RecoveryLoop {
    /// Return a builder for constructing a `RecoveryLoop`.
    ///
    /// `store` is a required argument; all other settings are optional fluent
    /// setters on the returned builder.
    #[must_use]
    pub fn builder(store: Arc<dyn ObjectStore>) -> RecoveryLoopBuilder {
        RecoveryLoopBuilder {
            store,
            conditional_store: None,
            lock: None,
            interval: None,
            cancellation: None,
        }
    }

    /// Run the recovery loop until the cancellation token is fired.
    pub async fn run(self) {
        run_periodic(self.interval, &self.cancellation, "RecoveryLoop", || {
            self.sweep()
        })
        .await;
    }

    /// Run a single sweep: list `.tx-log/`, process each intent.
    pub async fn sweep(&self) {
        let mut token: Option<String> = None;
        loop {
            match self.store.list(".tx-log/", 100, token.clone()).await {
                Ok(page) => {
                    for suffix in page.items {
                        // `list` returns keys relative to the prefix; reconstruct
                        // the full key so `get`/`delete` resolve correctly.
                        let key = format!(".tx-log/{suffix}");
                        self.process_intent(&key).await;
                    }
                    if page.next_token.is_none() {
                        break;
                    }
                    token = page.next_token;
                }
                Err(e) => {
                    warn!(error = %e, "RecoveryLoop: failed to list .tx-log/, will retry next tick");
                    break;
                }
            }
        }
    }

    async fn process_intent(&self, key: &str) {
        let body = match self.store.get(key).await {
            Ok(b) => b,
            Err(StorageError::NotFound) => {
                // Reaped between list and get — normal race, skip.
                return;
            }
            Err(e) => {
                warn!(key, error = %e, "RecoveryLoop: failed to read intent");
                return;
            }
        };

        let mut intent: IntentRecord = match serde_json::from_slice(&body) {
            Ok(i) => i,
            Err(e) => {
                warn!(key, error = %e, "RecoveryLoop: failed to deserialise intent");
                return;
            }
        };

        let now = Utc::now();
        if !intent.is_stale(now) {
            // Owner is still live; skip.
            return;
        }

        info!(
            tx_id = %intent.id,
            "RecoveryLoop: taking over stale transaction"
        );

        // Acquire ownership of the intent's lock set before any apply or
        // rollback so a still-alive owner cannot race the takeover.
        let session = if let Some(lock) = &self.lock {
            let keys = intent_lock_set(&intent);
            match lock.try_acquire(&keys).await {
                Ok(Some(session)) => Some(session),
                Ok(None) => {
                    debug!(
                        tx_id = %intent.id,
                        "RecoveryLoop: intent lock set contended, skipping until next sweep"
                    );
                    return;
                }
                Err(e) => {
                    debug!(
                        tx_id = %intent.id,
                        error = %e,
                        "RecoveryLoop: failed to acquire intent lock set, skipping until next sweep"
                    );
                    return;
                }
            }
        } else {
            None
        };

        if intent.any_applied() {
            self.replay_forward(&mut intent).await;
        } else {
            self.rollback(&intent).await;
        }

        if let Some(session) = session {
            session.release().await;
        }
    }

    /// Re-apply every still-`Pending` mutation of a committed transaction
    /// idempotently, then reap.
    async fn replay_forward(&self, intent: &mut IntentRecord) {
        let len = intent.mutations.len();
        for idx in 0..len {
            if let Err(e) = self.apply_mutation(intent, idx).await {
                warn!(
                    tx_id = %intent.id,
                    idx,
                    error = %e,
                    "RecoveryLoop: failed to replay mutation; will retry next sweep"
                );
                return;
            }
        }
        self.reap(intent).await;
    }

    /// Roll back a transaction that has no applied mutations.
    ///
    /// Delegates to the shared [`common::rollback`] so the recovery and
    /// healthy-path cleanup stay in lock-step.
    async fn rollback(&self, intent: &IntentRecord) {
        common::rollback(self.store.as_ref(), intent).await;
    }

    /// Reap a fully-applied transaction.
    ///
    /// Delegates to the shared [`common::reap`]: deletes the body staging
    /// prefix first, leaving the intent log intact for a later sweep if that
    /// delete fails.
    async fn reap(&self, intent: &IntentRecord) {
        common::reap(self.store.as_ref(), intent).await;
    }

    /// Apply a single mutation during recovery.
    ///
    /// Already-`Applied` mutations are skipped (recovery never overwrites a
    /// successful commit). Pending mutations dispatch to the idempotent CAS
    /// variant when a [`ConditionalStore`] is wired; otherwise they use
    /// unconditional ops. On success the mutation's `progress` slot is stamped
    /// and the intent JSON is re-PUT so subsequent sweeps see the updated state.
    ///
    /// A missing staging body (`NotFound` on `body_ref`) is treated as evidence
    /// that the Reap deleted the body prefix already; the canonical write has
    /// therefore landed and this mutation is a no-op.
    ///
    /// On true contention (`Error::PartialCommit` from the CAS path), the
    /// mutation is left `Pending` and the error is surfaced so the caller stops
    /// replay and leaves the intent for the next sweep.
    async fn apply_mutation(
        &self,
        intent: &mut IntentRecord,
        idx: usize,
    ) -> Result<(), StorageError> {
        if matches!(intent.progress.get(idx), Some(MutationProgress::Applied)) {
            return Ok(());
        }

        let mutation = intent.mutations[idx].clone();
        if let Some(cs) = &self.conditional_store {
            apply_cas(cs.as_ref(), &mutation, CasApplyMode::Reconcile)
                .await
                .map_err(|e| match e {
                    Error::PartialCommit => StorageError::PreconditionFailed,
                    Error::Storage(s) => s,
                    other => StorageError::Backend(other.to_string()),
                })?;
        } else {
            apply_mutation_unconditional(self.store.as_ref(), &mutation).await?;
        }

        self.stamp_progress_during_recovery(intent, idx).await
    }

    /// Mirror the executors' stamp pattern: mark the slot `Applied` and re-PUT
    /// the intent so subsequent recovery iterations skip it.
    async fn stamp_progress_during_recovery(
        &self,
        intent: &mut IntentRecord,
        idx: usize,
    ) -> Result<(), StorageError> {
        intent.mark_applied(idx);
        let body = match serde_json::to_vec(intent) {
            Ok(b) => b,
            Err(e) => {
                warn!(
                    tx_id = %intent.id,
                    idx,
                    error = %e,
                    "RecoveryLoop: failed to serialise intent after stamping progress"
                );
                return Ok(());
            }
        };
        if let Err(e) = self.store.put(&intent.log_key(), Bytes::from(body)).await {
            warn!(
                tx_id = %intent.id,
                idx,
                error = %e,
                "RecoveryLoop: failed to re-PUT intent after stamping progress"
            );
        }
        Ok(())
    }
}

/// Apply a mutation using unconditional storage operations.
///
/// Used when no [`ConditionalStore`] is wired into the recovery loop (the
/// Locked-executor deployment path).
async fn apply_mutation_unconditional(
    store: &dyn ObjectStore,
    mutation: &MutationRecord,
) -> Result<(), StorageError> {
    match mutation {
        MutationRecord::Put { key, body_ref, .. } => match store.get(body_ref).await {
            Ok(body) => store.put(key, Bytes::from(body)).await,
            Err(StorageError::NotFound) => Ok(()),
            Err(e) => Err(e),
        },
        MutationRecord::PutIfAbsent { key, body_ref, .. } => match store.head(key).await {
            Ok(_) => Ok(()),
            Err(StorageError::NotFound) => match store.get(body_ref).await {
                Ok(body) => store.put(key, Bytes::from(body)).await,
                Err(StorageError::NotFound) => Ok(()),
                Err(e) => Err(e),
            },
            Err(e) => Err(e),
        },
        MutationRecord::Delete { key, .. } => match store.delete(key).await {
            Ok(()) | Err(StorageError::NotFound) => Ok(()),
            Err(e) => Err(e),
        },
        MutationRecord::Copy { src, dst, .. } => store.copy(src, dst).await,
        MutationRecord::Move { src, dst, .. } => common::move_idempotent(store, src, dst).await,
    }
}
