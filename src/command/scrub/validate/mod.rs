//! Per-key validation for the scrub walk.
//!
//! [`Validator::process`] categorizes one raw key and runs the checks its
//! category calls for. It is infallible per key, failures being warned and
//! counted, so one bad object never aborts a walk.

mod blob;
mod jobs;
mod link;
mod reference;
#[cfg(test)]
mod tests;

use std::{
    collections::HashSet,
    sync::{Arc, Mutex, atomic::Ordering},
};

use tracing::warn;

use angos_oci::Digest;

use crate::{
    command::maintenance::{
        Error,
        action::{Action, WalkedStore},
        categorize::{KeyCategory, categorize},
        executor::{ActionSink, object_younger_than_grace},
        walk::WalkStats,
    },
    registry::{Error as RegistryError, blob_store::BlobStore, metadata_store::MetadataStore},
};

/// One of the three walk passes. Later passes rely on earlier repairs: links
/// are healed and grants reconciled before entries are pruned against them,
/// and the index is healed before blob GC reads it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Pass {
    /// Metadata store, everything except the `v2/blobs/` subtree: links and
    /// job records.
    MetadataLinks,
    /// Metadata store, the `v2/ref/` subtree: the blob-index reference keys.
    MetadataReferences,
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
    /// Names a once-per-container emission already handled, so the per-key
    /// walk does not repeat their prefix-level actions.
    handled: Mutex<HashSet<String>>,
    /// Digests the reference walk read live references for (see
    /// [`Self::record_reference_seen`]).
    seen_refs: Mutex<HashSet<Digest>>,
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
            seen_refs: Mutex::new(HashSet::new()),
        }
    }

    /// Validate one key, warning and counting every error so the walk
    /// continues past individual defects.
    pub async fn process(&self, pass: Pass, key: &str) {
        self.stats.keys.fetch_add(1, Ordering::Relaxed);
        if let Err(e) = self.dispatch(pass, key).await {
            self.stats.failures.fetch_add(1, Ordering::Relaxed);
            warn!("scrub: validation failed for key '{key}': {e}");
        }
    }

    async fn dispatch(&self, pass: Pass, key: &str) -> Result<(), Error> {
        let category = categorize(key);
        // Quarantined keys are never touched, nor a leaked probe object:
        // deleting one could race another replica's startup CAS probe and
        // flip its verdict.
        if matches!(category, KeyCategory::LostAndFound | KeyCategory::Probe) {
            return Ok(());
        }

        match (pass, category) {
            (Pass::MetadataLinks, KeyCategory::TagEntry { namespace, tag }) => {
                self.validate_tag_entries(&namespace, &tag).await
            }
            (Pass::MetadataLinks, KeyCategory::TagAtimeEntry { namespace, tag }) => {
                self.collect_tag_atime_entries(&namespace, &tag).await
            }
            (Pass::MetadataLinks, KeyCategory::RevisionAtimeEntry { namespace, digest }) => {
                self.collect_revision_atime_entries(&namespace, &digest)
                    .await
            }
            (Pass::MetadataLinks, KeyCategory::RevisionRecord { namespace, digest }) => {
                self.validate_revision_record(&namespace, &digest).await
            }
            (
                Pass::MetadataLinks,
                KeyCategory::ReferrerRecord {
                    namespace,
                    subject,
                    referrer,
                },
            ) => {
                self.validate_referrer_record(&namespace, &subject, &referrer)
                    .await
            }
            (Pass::MetadataLinks, KeyCategory::JobRecord { queue, state }) => {
                self.validate_job_record(key, queue, state).await
            }
            (Pass::MetadataLinks, KeyCategory::JobIndex { queue }) => {
                self.validate_job_index(key, queue).await
            }
            (
                Pass::MetadataReferences,
                KeyCategory::BlobRef {
                    digest,
                    namespace,
                    link,
                },
            ) => self.validate_ref(key, &digest, &namespace, link).await,
            (Pass::Blob, KeyCategory::BlobData { digest }) => self.validate_blob(&digest).await,
            (
                Pass::Blob,
                KeyCategory::UploadArtifact {
                    namespace,
                    artifact,
                },
            ) => {
                self.validate_upload_artifact(key, &namespace, artifact)
                    .await
            }
            (Pass::MetadataLinks, KeyCategory::CatalogIndex { namespace }) => {
                self.validate_catalog_index(&namespace).await
            }
            (pass, KeyCategory::Unknown) => self.quarantine(walked_store(pass), key).await,
            // A known category needing nothing here: owned by another pass or
            // the other store, write-once tag history, or a live run marker.
            _ => Ok(()),
        }
    }

    /// Emit a repair/reclaim action and count it.
    pub async fn emit(&self, action: Action) -> Result<(), Error> {
        self.sink.apply(action).await?;
        self.stats.repairs.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Whether the metadata key at `key` is younger than the reclamation grace
    /// period. A young key may belong to a push between two of its waves, so no
    /// repair may be derived from it; a missing key is not young, its absence
    /// being the caller's evidence.
    pub async fn younger_than_grace(&self, key: &str) -> Result<bool, Error> {
        let young = object_younger_than_grace(
            self.metadata_store.object_store().as_ref(),
            key,
            self.metadata_store.gc_grace_secs,
        )
        .await
        .map_err(RegistryError::from)?;
        Ok(young == Some(true))
    }

    /// Handle a key matching no known layout: quarantined by default, deleted
    /// under `--delete-unknown`, counted either way.
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

    /// Record that the reference walk read a live reference to `digest`. The
    /// walk enumerates the whole store, so this is an independent witness
    /// against the per-blob listing blob GC decides from.
    pub fn record_reference_seen(&self, digest: &Digest) {
        match self.seen_refs.lock() {
            Ok(mut refs) => refs.insert(digest.clone()),
            Err(poisoned) => poisoned.into_inner().insert(digest.clone()),
        };
    }

    /// Whether the reference walk read a live reference to `digest`.
    pub fn reference_walk_saw(&self, digest: &Digest) -> bool {
        match self.seen_refs.lock() {
            Ok(refs) => refs.contains(digest),
            Err(poisoned) => poisoned.into_inner().contains(digest),
        }
    }
}

/// The store a pass walks, for raw-key actions.
fn walked_store(pass: Pass) -> WalkedStore {
    match pass {
        Pass::MetadataLinks | Pass::MetadataReferences => WalkedStore::Metadata,
        Pass::Blob => WalkedStore::Blob,
    }
}
