use std::{fmt, sync::Arc, time::Duration};

use tokio::time::sleep;

use crate::registry::metadata_store::{lock::LockBackend, simple_jitter};
use angos_storage::{ConditionalStore, ObjectStore};

// ───────────────────────────────────────────────────────────────────────────
// CAS retry helpers
// ───────────────────────────────────────────────────────────────────────────

pub const MAX_CAS_RETRIES: u32 = 20;
const CAS_RETRY_BASE_MS: u64 = 50;

pub async fn sleep_cas_retry(attempt: u32) {
    let max_ms = CAS_RETRY_BASE_MS.saturating_mul(1u64 << attempt.min(4));
    sleep(Duration::from_millis(simple_jitter(max_ms))).await;
}

// ───────────────────────────────────────────────────────────────────────────
// Coordination mode
// ───────────────────────────────────────────────────────────────────────────

/// How concurrent writes are serialised.
///
/// `Cas` drives blob-index and link updates through optimistic CAS on a
/// `ConditionalStore`; the S3-based lock backend is kept only for the
/// blob-data lock and for fallback on lock-key contention.
///
/// `Locked` runs every write under a `LockBackend` acquire/release; blob-index
/// shards are updated with plain unconditional writes.
pub enum Coordinator {
    Cas {
        /// The CAS-capable store (superset of `ObjectStore`).
        store: Arc<dyn ConditionalStore>,
        /// Lock backend used for `acquire_blob_data_lock` and for fallback on
        /// lock-key contention. The only constructor that builds `Cas` (S3
        /// metadata-store with `lock_strategy = "s3"`) always wires an
        /// `S3LockBackend` here; the trait object is kept to avoid coupling
        /// the coordinator module to the S3 lock implementation.
        lock: Arc<dyn LockBackend + Send + Sync>,
    },
    Locked {
        store: Arc<dyn ObjectStore>,
        lock: Arc<dyn LockBackend + Send + Sync>,
    },
}

impl fmt::Debug for Coordinator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Coordinator::Cas { lock, .. } => f
                .debug_struct("Coordinator::Cas")
                .field("lock", lock)
                .finish_non_exhaustive(),
            Coordinator::Locked { lock, .. } => f
                .debug_struct("Coordinator::Locked")
                .field("lock", lock)
                .finish_non_exhaustive(),
        }
    }
}

impl Coordinator {
    pub fn store(&self) -> &dyn ObjectStore {
        match self {
            Coordinator::Cas { store, .. } => store.as_ref(),
            Coordinator::Locked { store, .. } => store.as_ref(),
        }
    }

    pub fn lock(&self) -> &(dyn LockBackend + Send + Sync) {
        match self {
            Coordinator::Cas { lock, .. } | Coordinator::Locked { lock, .. } => lock.as_ref(),
        }
    }

    pub fn is_cas(&self) -> bool {
        matches!(self, Coordinator::Cas { .. })
    }

    pub fn conditional_store(&self) -> Option<&dyn ConditionalStore> {
        match self {
            Coordinator::Cas { store, .. } => Some(store.as_ref()),
            Coordinator::Locked { .. } => None,
        }
    }
}
