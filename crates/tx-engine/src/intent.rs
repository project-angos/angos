//! On-disk shape of a transaction intent record.
//!
//! An intent record is written to `.tx-log/<tx-id>.json` before any canonical
//! keys are mutated. It is the linearisation point: once this write succeeds,
//! the transaction WILL be observed (either by the owning worker finishing
//! Apply/Reap, or by the recovery loop replaying it).
//!
//! Body bytes for `Put` and `PutIfAbsent` mutations are staged at
//! `.tx-bodies/<tx-id>/<idx>` *before* the intent PUT, so the intent JSON
//! stays small (KB-scale). The `body_ref` field records where to find the
//! bytes during Apply and recovery.

/// Default intent TTL in seconds used by both executors when no explicit TTL
/// is provided. Named here so the value is defined once and referenced by both
/// `CasExecutor` and `LockedExecutor` builders.
pub const DEFAULT_INTENT_TTL_SECS: u64 = 300;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::warn;
use uuid::Uuid;

use angos_storage::Etag;

/// The operation variant recorded in an intent record.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "op")]
pub enum MutationRecord {
    /// Write `body_ref` to `key`.
    ///
    /// `expected` is honored by both executors: the CAS executor via
    /// `put_if_match`, and the Locked executor via a HEAD + `ETag` comparison
    /// under the lock.
    Put {
        key: String,
        body_ref: String,
        expected: Option<Etag>,
    },
    /// Write `body_ref` to `key` only if the key is absent.
    PutIfAbsent { key: String, body_ref: String },
    /// Delete `key`.
    ///
    /// `expected` is honored by both executors: the CAS executor via
    /// `delete_if_match`, and the Locked executor via a HEAD + `ETag` comparison
    /// under the lock.
    Delete { key: String, expected: Option<Etag> },
    /// Server-side copy from `src` to `dst`.
    Copy { src: String, dst: String },
    /// Server-side move from `src` to `dst`: copy then delete-src, both
    /// idempotent under replay.
    Move { src: String, dst: String },
}

impl MutationRecord {
    /// Return every canonical key this mutation touches (source and
    /// destination for `Copy`/`Move`; just the target for the rest). Used by
    /// recovery to reconstruct the transaction's lock set without rebuilding
    /// the original `Transaction` value.
    pub fn all_keys(&self) -> impl Iterator<Item = &str> {
        match self {
            MutationRecord::Put { key, .. }
            | MutationRecord::PutIfAbsent { key, .. }
            | MutationRecord::Delete { key, .. } => vec![key.as_str()].into_iter(),
            MutationRecord::Copy { src, dst } | MutationRecord::Move { src, dst } => {
                vec![src.as_str(), dst.as_str()].into_iter()
            }
        }
    }
}

/// Per-mutation apply progress recorded in the intent log.
///
/// Parallel to `IntentRecord::mutations`: one entry per mutation. `Pending`
/// means the mutation has not yet been confirmed applied; `Applied` means
/// the engine observed a successful apply. Only the discriminant matters:
/// recovery and `any_applied` inspect `Applied` to decide replay-forward vs
/// rollback and to skip already-applied slots, but the value carries no
/// payload.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum MutationProgress {
    Pending,
    Applied,
}

/// Staging key for the body of mutation `idx` in transaction `id`.
///
/// Prefix of the intent log: one `<tx-id>.json` record per in-flight
/// transaction. Engine-owned; external maintenance must never touch it.
pub const INTENT_LOG_PREFIX: &str = ".tx-log";

/// Prefix of the staged mutation bodies, one `<tx-id>/<idx>` object per
/// mutation. Engine-owned; the `BodyJanitor` reclaims orphans.
pub const INTENT_BODIES_PREFIX: &str = ".tx-bodies";

/// Single source of truth for the `.tx-bodies/<tx-id>/<idx>` shape, shared by
/// [`IntentRecord::body_ref`] and the body-staging path in the executors.
#[must_use]
pub fn body_ref_key(id: Uuid, idx: usize) -> String {
    format!("{INTENT_BODIES_PREFIX}/{id}/{idx}")
}

/// A read dependency recorded in an intent.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReadRecord {
    pub key: String,
    /// Serialised fingerprint: either an `ETag` string (for CAS) or a
    /// hex-encoded content hash (for Locked).
    pub fingerprint: String,
}

/// The complete intent record written to `.tx-log/<tx-id>.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntentRecord {
    /// Unique transaction identifier.
    pub id: Uuid,
    /// Wall-clock time at which the intent was written.
    pub created_at: DateTime<Utc>,
    /// Seconds after `created_at` at which the intent is considered stale
    /// by the recovery loop.
    pub ttl_secs: u64,
    /// Read dependencies.
    pub reads: Vec<ReadRecord>,
    /// Ordered mutations.
    pub mutations: Vec<MutationRecord>,
    /// Coarse lock keys captured at build time so recovery can reconstruct
    /// the full lock set when reclaiming a stale intent.
    #[serde(default)]
    pub coarse_lock_keys: Vec<String>,
    /// Per-mutation apply progress, length-matched to `mutations`. Initialised
    /// to `Pending` for every mutation when the intent is first written and
    /// stamped to `Applied` after each successful apply. Recovery uses this
    /// vector to decide replay-forward vs rollback (any `Applied` entry implies
    /// the transaction is committed).
    pub progress: Vec<MutationProgress>,
}

impl IntentRecord {
    /// Return the object key under which this intent is stored.
    #[must_use]
    pub fn log_key(&self) -> String {
        format!("{INTENT_LOG_PREFIX}/{}.json", self.id)
    }

    /// Return the `.tx-bodies` prefix for mutation body staging.
    #[must_use]
    pub fn bodies_prefix(&self) -> String {
        format!("{INTENT_BODIES_PREFIX}/{}/", self.id)
    }

    /// Return the staging key for mutation body at index `idx`.
    #[must_use]
    pub fn body_ref(&self, idx: usize) -> String {
        body_ref_key(self.id, idx)
    }

    /// Returns `true` if the owner's heartbeat is considered stale.
    #[must_use]
    pub fn is_stale(&self, now: DateTime<Utc>) -> bool {
        let expiry = self.created_at + chrono::Duration::seconds(self.ttl_secs.cast_signed());
        now > expiry
    }

    /// Returns `true` if at least one mutation in this transaction has been
    /// applied (derived from `progress`).
    #[must_use]
    pub fn any_applied(&self) -> bool {
        self.progress
            .iter()
            .any(|p| matches!(p, MutationProgress::Applied))
    }

    /// Record that mutation `idx` has been applied. A no-op (with a warning)
    /// when `idx` is out of range, defending against a malformed intent without
    /// panicking.
    pub fn mark_applied(&mut self, idx: usize) {
        if idx >= self.progress.len() {
            warn!(
                tx_id = %self.id,
                idx,
                len = self.progress.len(),
                "mark_applied called with out-of-range index; ignoring"
            );
            return;
        }
        self.progress[idx] = MutationProgress::Applied;
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use uuid::Uuid;

    use crate::intent::{IntentRecord, MutationProgress, MutationRecord};

    fn sample_intent(progress: Vec<MutationProgress>) -> IntentRecord {
        let mutations = (0..progress.len())
            .map(|i| MutationRecord::Put {
                key: format!("k{i}"),
                body_ref: format!("b{i}"),
                expected: None,
            })
            .collect();
        IntentRecord {
            id: Uuid::new_v4(),
            created_at: Utc::now(),
            ttl_secs: 300,
            reads: vec![],
            mutations,
            coarse_lock_keys: vec![],
            progress,
        }
    }

    #[test]
    fn progress_round_trips_through_serde() {
        let intent = sample_intent(vec![
            MutationProgress::Pending,
            MutationProgress::Applied,
            MutationProgress::Applied,
        ]);
        let json = serde_json::to_vec(&intent).expect("serialise");
        let back: IntentRecord = serde_json::from_slice(&json).expect("deserialise");
        assert_eq!(back.progress, intent.progress);
    }

    #[test]
    fn mark_applied_sets_correct_slot() {
        let mut intent = sample_intent(vec![MutationProgress::Pending; 3]);
        intent.mark_applied(1);
        assert_eq!(intent.progress[0], MutationProgress::Pending);
        assert_eq!(intent.progress[1], MutationProgress::Applied);
        assert_eq!(intent.progress[2], MutationProgress::Pending);
    }

    #[test]
    fn mark_applied_out_of_range_is_a_no_op() {
        let mut intent = sample_intent(vec![MutationProgress::Pending; 2]);
        intent.mark_applied(5);
        assert!(
            intent
                .progress
                .iter()
                .all(|p| matches!(p, MutationProgress::Pending))
        );
    }

    #[test]
    fn any_applied_reflects_progress() {
        let intent = sample_intent(vec![MutationProgress::Pending; 3]);
        assert!(!intent.any_applied());

        let intent = sample_intent(vec![
            MutationProgress::Pending,
            MutationProgress::Applied,
            MutationProgress::Pending,
        ]);
        assert!(intent.any_applied());
    }
}
