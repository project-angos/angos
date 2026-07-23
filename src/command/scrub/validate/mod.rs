//! Per-key validation for the scrub walk.
//!
//! [`Validator::process`] takes one raw key, categorizes it, and runs the
//! store-appropriate checks: repair derivable state, delete objects whose
//! content is unreadable, quarantine keys that match no known layout. It is
//! infallible per key (failures are warned and counted), so one bad object
//! never aborts a walk.

mod blob;
mod jobs;
mod link;
mod shard;
#[cfg(test)]
mod tests;

use std::{
    collections::{HashMap, HashSet},
    future::Future,
    sync::{Arc, Mutex, atomic::Ordering},
    time::Duration,
};

use angos_backoff::Backoff;
use chrono::{DateTime, TimeDelta, Utc};
use tokio::time::sleep;
use tracing::warn;

use angos_tx_engine::{
    INTENT_LOG_PREFIX, StorageError,
    intent::{IntentRecord, MutationRecord},
};

use crate::{
    command::maintenance::{
        Error,
        action::{Action, WalkedStore},
        categorize::{KeyCategory, categorize},
        executor::ActionSink,
        walk::WalkStats,
    },
    oci::Digest,
    registry::{Error as RegistryError, blob_store::BlobStore, metadata_store::MetadataStore},
};

/// Intent records fetched per `.tx-log/` listing page while confirming a
/// candidate repair.
const INTENT_PAGE: u16 = 100;

/// How many settle re-checks a candidate repair gets before it is left for a
/// later run. Budgeting in attempts rather than wall-clock keeps the check
/// S3-latency-independent: each intent-log scan runs to completion, so a slow
/// scan over a large log no longer burns the whole budget before a single real
/// re-check.
const INTENT_SETTLE_ATTEMPTS: u32 = 8;
/// Jittered backoff between settle re-checks, decorrelating scrub from the
/// in-flight writers it is waiting on. Small because each re-check already
/// includes a full intent-log scan, which dominates the wait on S3.
const INTENT_SETTLE_BACKOFF: Backoff =
    Backoff::exponential(Duration::from_millis(20), Duration::from_millis(200)).with_jitter();

/// One of the three walk passes. Later passes rely on earlier repairs:
/// links are healed and grants reconciled (M1) before shard entries are
/// pruned against them (M2), and the index is healed before blob GC reads
/// it (B).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Pass {
    /// Metadata store, everything except the `v2/blobs/` subtree: links and
    /// job records.
    MetadataLinks,
    /// Metadata store, `v2/blobs/` subtree: blob-index shards.
    MetadataShards,
    /// Blob store: blob data and upload artifacts.
    Blob,
}

/// Shared context of one scrub run: the stores, the action sink, and the
/// run counters. One `Arc<Validator>` serves every concurrent per-key task.
pub struct Validator {
    pub blob_store: Arc<BlobStore>,
    pub metadata_store: Arc<MetadataStore>,
    pub sink: Arc<dyn ActionSink>,
    pub stats: Arc<WalkStats>,
    /// Delete unrecognized keys outright instead of quarantining them
    /// (`--delete-unknown`).
    delete_unknown: bool,
    /// Names already handled by a once-per-container emission (invalid
    /// namespaces, orphan upload sessions), so the per-key walk does not
    /// repeat their prefix-level actions.
    handled: Mutex<HashSet<String>>,
    /// Digests exempted from this run's blob GC (see [`Self::hold_blob_gc`]).
    gc_holds: Mutex<HashSet<Digest>>,
    /// Intent records already fetched this run, keyed by log file name. An
    /// intent's expiry and mutation keys never change once written, so a
    /// cached entry stays valid for the whole run.
    intent_cache: Mutex<HashMap<String, CachedIntent>>,
}

impl Validator {
    pub fn new(
        blob_store: Arc<BlobStore>,
        metadata_store: Arc<MetadataStore>,
        sink: Arc<dyn ActionSink>,
        stats: Arc<WalkStats>,
        delete_unknown: bool,
    ) -> Self {
        Self {
            blob_store,
            metadata_store,
            sink,
            stats,
            delete_unknown,
            handled: Mutex::new(HashSet::new()),
            gc_holds: Mutex::new(HashSet::new()),
            intent_cache: Mutex::new(HashMap::new()),
        }
    }

    /// Validate one key. Never fails: every error is warned and counted, so
    /// the walk continues past individual defects.
    pub async fn process(&self, pass: Pass, key: &str) {
        self.stats.keys.fetch_add(1, Ordering::Relaxed);
        if let Err(e) = self.dispatch(pass, key).await {
            self.stats.failures.fetch_add(1, Ordering::Relaxed);
            warn!("scrub: validation failed for key '{key}': {e}");
        }
    }

    async fn dispatch(&self, pass: Pass, key: &str) -> Result<(), Error> {
        let category = categorize(key);
        // Engine-owned and already-quarantined keys are never touched. A
        // leaked probe object is left alone too: deleting it could race a
        // concurrent server startup's CAS probe and flip its verdict.
        if matches!(
            category,
            KeyCategory::EngineInternal | KeyCategory::LostAndFound | KeyCategory::Probe
        ) {
            return Ok(());
        }

        match (pass, category) {
            (Pass::MetadataLinks, KeyCategory::Link { namespace, link }) => {
                self.validate_link(key, &namespace, link).await
            }
            (Pass::MetadataLinks, KeyCategory::JobRecord { queue, state }) => {
                self.validate_job_record(key, queue, state).await
            }
            (Pass::MetadataLinks, KeyCategory::JobIndex { .. }) => {
                self.validate_job_index(key).await
            }
            (Pass::MetadataShards, KeyCategory::BlobIndexShard { digest, namespace }) => {
                self.validate_shard(key, &digest, &namespace).await
            }
            (Pass::Blob, KeyCategory::BlobData { digest }) => self.validate_blob(&digest).await,
            (
                Pass::Blob,
                KeyCategory::UploadArtifact {
                    namespace,
                    uuid,
                    artifact,
                },
            ) => {
                self.validate_upload_artifact(key, &namespace, &uuid, artifact)
                    .await
            }
            (pass, KeyCategory::Unknown) => self.quarantine(walked_store(pass), key).await,
            // Anything else is a known category owned by another pass or the
            // other store (the two stores may share one physical root).
            _ => Ok(()),
        }
    }

    /// Emit a repair/reclaim action and count it.
    pub async fn emit(&self, action: Action) -> Result<(), Error> {
        self.sink.apply(action).await?;
        self.stats.repairs.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Confirm a cross-key inconsistency before repairing it, so a
    /// transaction caught mid-apply is never mistaken for settled damage.
    ///
    /// A live intent listing any of `evidence_keys` marks the state as still
    /// moving (a server write, or one of this run's own repairs on a
    /// neighbouring key), so the check backs off and retries until it settles.
    /// Once settled, the repair proceeds only when `reverify` still observes
    /// the inconsistency; checking intents again after the re-read closes the
    /// race with a transaction that started in between. A candidate that never
    /// settles is left for the next run.
    pub async fn confirm_repair<F, Fut>(
        &self,
        evidence_keys: &[String],
        reverify: F,
    ) -> Result<bool, Error>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<bool, Error>>,
    {
        for attempt in 0..INTENT_SETTLE_ATTEMPTS {
            if self.live_intent_touches(evidence_keys).await? {
                sleep(INTENT_SETTLE_BACKOFF.delay(attempt)).await;
                continue;
            }
            if !reverify().await? {
                return Ok(false);
            }
            if !self.live_intent_touches(evidence_keys).await? {
                return Ok(true);
            }
            sleep(INTENT_SETTLE_BACKOFF.delay(attempt)).await;
        }
        warn!("scrub: repair candidate on {evidence_keys:?} never settled; leaving to a later run");
        Ok(false)
    }

    /// Whether a live (non-expired) transaction intent lists any of `keys`
    /// among its mutation targets. An unreadable intent record suppresses the
    /// repair too: its transaction is recovery's to settle first.
    ///
    /// The listing is always fresh (reaping must be observed), but records
    /// already fetched this run are answered from the intent cache.
    async fn live_intent_touches(&self, keys: &[String]) -> Result<bool, Error> {
        let store = self.metadata_store.store().object_store();
        let mut token = None;
        loop {
            let page = store
                .list(INTENT_LOG_PREFIX, INTENT_PAGE, token)
                .await
                .map_err(RegistryError::from)?;
            for name in &page.items {
                if let Some(touches) = self.cached_intent_touches(name, keys) {
                    if touches {
                        return Ok(true);
                    }
                    continue;
                }
                let raw = match store.get(&format!("{INTENT_LOG_PREFIX}/{name}")).await {
                    Ok(raw) => raw,
                    // Reaped between the listing and the read: not in flight.
                    Err(StorageError::NotFound) => continue,
                    Err(e) => return Err(RegistryError::from(e).into()),
                };
                // An unreadable record is never cached, so it keeps
                // suppressing repairs until it becomes readable or is reaped.
                let Ok(intent) = serde_json::from_slice::<IntentRecord>(&raw) else {
                    return Ok(true);
                };
                let cached = CachedIntent::from(&intent);
                let touches = cached.touches(keys);
                self.cache_intent(name.clone(), cached);
                if touches {
                    return Ok(true);
                }
            }
            token = page.next_token;
            if token.is_none() {
                return Ok(false);
            }
        }
    }

    /// The cached intent's verdict against `keys`, or `None` when the record
    /// has not been fetched this run.
    fn cached_intent_touches(&self, name: &str, keys: &[String]) -> Option<bool> {
        let cache = match self.intent_cache.lock() {
            Ok(cache) => cache,
            Err(poisoned) => poisoned.into_inner(),
        };
        cache.get(name).map(|cached| cached.touches(keys))
    }

    /// Remember a fetched intent record for the rest of the run.
    fn cache_intent(&self, name: String, cached: CachedIntent) {
        match self.intent_cache.lock() {
            Ok(mut cache) => cache.insert(name, cached),
            Err(poisoned) => poisoned.into_inner().insert(name, cached),
        };
    }

    /// Handle a key that matches no known layout: quarantined by default,
    /// deleted outright under `--delete-unknown`. Either way it counts in
    /// the run's unknown-key tally.
    async fn quarantine(&self, store: WalkedStore, key: &str) -> Result<(), Error> {
        let action = if self.delete_unknown {
            Action::DeleteUnknownKey {
                store,
                key: key.to_string(),
            }
        } else {
            Action::QuarantineKey {
                store,
                key: key.to_string(),
            }
        };
        self.sink.apply(action).await?;
        self.stats.quarantined.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Delete an expected-shape object whose content is unreadable.
    pub async fn delete_corrupt(&self, store: WalkedStore, key: &str) -> Result<(), Error> {
        self.sink
            .apply(Action::DeleteCorruptObject {
                store,
                key: key.to_string(),
            })
            .await?;
        self.stats.corrupt.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Claim a once-per-container emission. Returns `false` when another key
    /// of the same container already claimed it.
    pub fn claim(&self, token: String) -> bool {
        match self.handled.lock() {
            Ok(mut handled) => handled.insert(token),
            Err(poisoned) => poisoned.into_inner().insert(token),
        }
    }

    /// Exempt `digest` from this run's blob GC. Deleting a corrupt shard
    /// removes references the link pass could not re-grant (the grant write
    /// fails against unreadable shard content), so reclaiming the bytes this
    /// run would destroy a still-referenced blob; the next run re-grants from
    /// the manifests and then GC sees the truth.
    pub fn hold_blob_gc(&self, digest: &Digest) {
        match self.gc_holds.lock() {
            Ok(mut holds) => holds.insert(digest.clone()),
            Err(poisoned) => poisoned.into_inner().insert(digest.clone()),
        };
    }

    /// Whether `digest` was exempted from this run's blob GC.
    pub fn blob_gc_held(&self, digest: &Digest) -> bool {
        match self.gc_holds.lock() {
            Ok(holds) => holds.contains(digest),
            Err(poisoned) => poisoned.into_inner().contains(digest),
        }
    }
}

/// The immutable facts of one intent record: when it expires and which keys
/// its mutations touch. Only an intent's progress slots change after it is
/// written, so these never go stale.
struct CachedIntent {
    expires_at: DateTime<Utc>,
    touched_keys: HashSet<String>,
}

impl CachedIntent {
    /// Whether the intent is still live and lists any of `keys`. An expired
    /// intent's leftovers are scrub's to repair rather than recovery's to
    /// replay, so it suppresses nothing.
    fn touches(&self, keys: &[String]) -> bool {
        Utc::now() < self.expires_at && keys.iter().any(|key| self.touched_keys.contains(key))
    }
}

impl From<&IntentRecord> for CachedIntent {
    fn from(intent: &IntentRecord) -> Self {
        let ttl = i64::try_from(intent.ttl_secs).unwrap_or(i64::MAX);
        let expires_at = intent
            .created_at
            .checked_add_signed(TimeDelta::seconds(ttl))
            .unwrap_or(DateTime::<Utc>::MAX_UTC);
        let touched_keys = intent
            .mutations
            .iter()
            .flat_map(MutationRecord::all_keys)
            .map(str::to_string)
            .collect();
        Self {
            expires_at,
            touched_keys,
        }
    }
}

/// The store a pass walks, for raw-key actions.
fn walked_store(pass: Pass) -> WalkedStore {
    match pass {
        Pass::MetadataLinks | Pass::MetadataShards => WalkedStore::Metadata,
        Pass::Blob => WalkedStore::Blob,
    }
}
