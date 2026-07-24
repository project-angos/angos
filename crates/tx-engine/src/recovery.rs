//! Recovery loop: periodically scans `.tx-log/` and replays or rolls back
//! intents whose owner's heartbeat has gone stale.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use angos_storage::{ConditionalStore, Error as StorageError, ObjectStore};

use crate::{
    error::Error,
    executor::{
        cas::apply_cas,
        common::{self, ApplyMode},
    },
    intent::{INTENT_LOG_PREFIX, IntentRecord, MutationProgress},
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
///    - Re-reads the intent under the lock and decides from that copy; an
///      intent its owner reaped in the meantime is left alone.
///    - If any entry in `progress` is `Applied`, the transaction is partially
///      (or fully) committed: re-apply every still-`Pending` mutation
///      idempotently (using conditional storage primitives when a
///      [`ConditionalStore`] is wired) and reap.
///    - If every entry in `progress` is `Pending`, the transaction is rolled
///      back: delete bodies and intent.
///
/// Default grace, used by the serving processes, after which a committed intent
/// the recovery loop still cannot reconcile is abandoned: its intent is reaped
/// so a permanently unsatisfiable mutation stops re-logging every sweep. The
/// blob-index state a reaped shard mutation would have written is reconciled by
/// `angos scrub`.
pub const DEFAULT_ABANDON_AFTER_SECS: u64 = 3600;

/// Constructed via [`RecoveryLoop::builder`].
pub struct RecoveryLoop {
    store: Arc<dyn ObjectStore>,
    conditional_store: Option<Arc<dyn ConditionalStore>>,
    lock: Arc<Lock>,
    interval: Duration,
    cancellation: CancellationToken,
    abandon_after_secs: Option<u64>,
}

impl std::fmt::Debug for RecoveryLoop {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecoveryLoop")
            .field("interval", &self.interval)
            .field("has_conditional_store", &self.conditional_store.is_some())
            .finish_non_exhaustive()
    }
}

/// Builder for [`RecoveryLoop`].
pub struct RecoveryLoopBuilder {
    store: Arc<dyn ObjectStore>,
    conditional_store: Option<Arc<dyn ConditionalStore>>,
    lock: Arc<Lock>,
    interval: Option<Duration>,
    cancellation: Option<CancellationToken>,
    abandon_after_secs: Option<u64>,
}

impl RecoveryLoopBuilder {
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

    /// Abandon a committed intent that still fails to reconcile once it is older
    /// than `secs`, reaping it instead of replaying it forever. Omitting this
    /// (the default) never abandons, which is the right choice for one-shot test
    /// sweeps; serving processes wire [`DEFAULT_ABANDON_AFTER_SECS`].
    #[must_use]
    pub fn abandon_after_secs(mut self, secs: u64) -> Self {
        self.abandon_after_secs = Some(secs);
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
            abandon_after_secs: self.abandon_after_secs,
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
    /// `store` and `lock` are required: a sweep takes ownership of every stale
    /// intent it acts on, so without the engine's lock primitive it would race
    /// a still-alive owner. All other settings are optional fluent setters on
    /// the returned builder.
    #[must_use]
    pub fn builder(store: Arc<dyn ObjectStore>, lock: Arc<Lock>) -> RecoveryLoopBuilder {
        RecoveryLoopBuilder {
            store,
            conditional_store: None,
            lock,
            interval: None,
            cancellation: None,
            abandon_after_secs: None,
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
            match self
                .store
                .list(&format!("{INTENT_LOG_PREFIX}/"), 100, token.clone())
                .await
            {
                Ok(page) => {
                    for suffix in page.items {
                        // `list` returns keys relative to the prefix; reconstruct
                        // the full key so `get`/`delete` resolve correctly.
                        let key = format!("{INTENT_LOG_PREFIX}/{suffix}");
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

    /// Read and deserialise the intent at `key`. `None` when it is gone (reaped
    /// by its owner or by a peer sweep between the list and the read) or
    /// unreadable; both leave the intent for a later sweep.
    async fn read_intent(&self, key: &str) -> Option<IntentRecord> {
        let body = match self.store.get(key).await {
            Ok(b) => b,
            Err(StorageError::NotFound) => return None,
            Err(e) => {
                warn!(key, error = %e, "RecoveryLoop: failed to read intent");
                return None;
            }
        };

        match serde_json::from_slice(&body) {
            Ok(intent) => Some(intent),
            Err(e) => {
                warn!(key, error = %e, "RecoveryLoop: failed to deserialise intent");
                None
            }
        }
    }

    async fn process_intent(&self, key: &str) {
        let Some(intent) = self.read_intent(key).await else {
            return;
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
        let keys = intent_lock_set(&intent);
        let session = match self.lock.try_acquire(&keys).await {
            Ok(Some(session)) => session,
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
        };

        // Decide from the intent as it stands under the lock, never from the
        // pre-lock snapshot: the owner may have finished and reaped it, or
        // stamped another mutation, since it was read. Replaying that snapshot
        // would re-apply committed mutations (an unconditional `Delete` would
        // remove an object a later transaction re-created), and rolling it back
        // would discard a transaction whose first mutation just committed. Only
        // `progress` can have moved on; `created_at` is fixed for an intent's
        // lifetime, so the staleness verdict still holds.
        if let Some(mut fresh) = self.read_intent(key).await {
            if fresh.any_applied() {
                self.replay_forward(&mut fresh).await;
            } else {
                common::rollback(self.store.as_ref(), &fresh).await;
            }
        } else {
            debug!(
                tx_id = %intent.id,
                "RecoveryLoop: intent completed by its owner before the takeover, nothing to recover"
            );
        }

        session.release().await;
    }

    /// Re-apply every still-`Pending` mutation of a committed transaction
    /// idempotently, then reap.
    async fn replay_forward(&self, intent: &mut IntentRecord) {
        let len = intent.mutations.len();
        for idx in 0..len {
            if let Err(e) = self.apply_mutation(intent, idx).await {
                if matches!(e, Error::PartialCommit) && self.abandon_deadline_passed(intent) {
                    warn!(
                        tx_id = %intent.id,
                        idx,
                        "RecoveryLoop: abandoning committed transaction whose mutation stayed \
                         unreconcilable past the grace period; reaping intent so it stops \
                         replaying (blob-index state is reconciled by scrub)"
                    );
                    common::reap(self.store.as_ref(), intent).await;
                    return;
                }
                warn!(
                    tx_id = %intent.id,
                    idx,
                    error = %e,
                    "RecoveryLoop: failed to replay mutation; will retry next sweep"
                );
                return;
            }
        }
        common::reap(self.store.as_ref(), intent).await;
    }

    /// Whether `intent` has stayed unreconcilable long enough to abandon, per
    /// the configured grace. Always `false` when no grace is set.
    fn abandon_deadline_passed(&self, intent: &IntentRecord) -> bool {
        let Some(grace_secs) = self.abandon_after_secs else {
            return false;
        };
        let grace = chrono::Duration::seconds(grace_secs.cast_signed());
        Utc::now() > intent.created_at + grace
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
    async fn apply_mutation(&self, intent: &mut IntentRecord, idx: usize) -> Result<(), Error> {
        if matches!(intent.progress.get(idx), Some(MutationProgress::Applied)) {
            return Ok(());
        }

        let mutation = intent.mutations[idx].clone();
        if let Some(cs) = &self.conditional_store {
            apply_cas(cs.as_ref(), &mutation, ApplyMode::Reconcile).await?;
        } else {
            common::apply_object_store(
                self.store.as_ref(),
                &mutation,
                common::ApplyMode::Reconcile,
            )
            .await?;
        }

        common::stamp_applied(self.store.as_ref(), intent, idx).await;
        Ok(())
    }
}
