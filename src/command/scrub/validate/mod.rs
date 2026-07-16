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
    collections::HashSet,
    sync::{Arc, Mutex, atomic::Ordering},
};

use tracing::warn;

use crate::{
    command::scrub::{
        action::{Action, WalkedStore},
        categorize::{KeyCategory, categorize},
        error::Error,
        executor::ActionSink,
        walk::WalkStats,
    },
    oci::Digest,
    registry::{blob_store::BlobStore, metadata_store::MetadataStore},
};

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
    /// Names already handled by a once-per-container emission (invalid
    /// namespaces, orphan upload sessions), so the per-key walk does not
    /// repeat their prefix-level actions.
    handled: Mutex<HashSet<String>>,
}

impl Validator {
    pub fn new(
        blob_store: Arc<BlobStore>,
        metadata_store: Arc<MetadataStore>,
        sink: Arc<dyn ActionSink>,
        stats: Arc<WalkStats>,
    ) -> Self {
        Self {
            blob_store,
            metadata_store,
            sink,
            stats,
            handled: Mutex::new(HashSet::new()),
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

    /// Quarantine a key that matches no known layout.
    async fn quarantine(&self, store: WalkedStore, key: &str) -> Result<(), Error> {
        self.sink
            .apply(Action::QuarantineKey {
                store,
                key: key.to_string(),
            })
            .await?;
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
        let _ = self.claim(format!("gc-hold:{digest}"));
    }

    /// Whether `digest` was exempted from this run's blob GC.
    pub fn blob_gc_held(&self, digest: &Digest) -> bool {
        let token = format!("gc-hold:{digest}");
        match self.handled.lock() {
            Ok(handled) => handled.contains(&token),
            Err(poisoned) => poisoned.into_inner().contains(&token),
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
