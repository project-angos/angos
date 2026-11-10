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

use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, PoisonError};

use tracing::warn;

use angos_oci::Digest;
use angos_storage::Error as StorageError;

use crate::command::maintenance::Error;
use crate::command::maintenance::action::{Action, WalkedStore};
use crate::command::maintenance::categorize::{KeyCategory, categorize};
use crate::command::maintenance::executor::{ActionSink, object_younger_than_grace};
use crate::command::maintenance::walk::WalkStats;
use crate::registry::Error as RegistryError;
use crate::registry::blob_store::BlobStore;
use crate::registry::metadata_store::{LinkKind, MetadataStore};

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
    pub stats: WalkStats,
    /// Delete unrecognized keys outright instead of quarantining them
    /// (`--delete-unknown`).
    delete_unknown: bool,
    /// Names a once-per-container emission already handled, so the per-key
    /// walk does not repeat their prefix-level actions.
    handled: Mutex<HashSet<String>>,
    /// Digests the reference walk read live references for: a witness,
    /// independent of the per-blob listing, against which blob GC decides.
    seen_refs: Mutex<HashSet<Digest>>,
}

impl Validator {
    pub fn new(
        blob_store: Arc<BlobStore>,
        metadata_store: Arc<MetadataStore>,
        sink: Arc<dyn ActionSink>,
        delete_unknown: bool,
    ) -> Self {
        Self {
            blob_store,
            metadata_store,
            sink,
            stats: WalkStats::default(),
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
                self.collect_atime_entries(&namespace, &LinkKind::Tag(tag))
                    .await
            }
            (Pass::MetadataLinks, KeyCategory::RevisionAtimeEntry { namespace, digest }) => {
                self.collect_atime_entries(&namespace, &LinkKind::Digest(digest))
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
                self.validate_referrer_record(namespace, subject, referrer)
                    .await
            }
            (Pass::MetadataLinks, KeyCategory::JobRecord { state }) => {
                self.validate_job_record(key, state).await
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
                self.validate_catalog_index(namespace).await
            }
            (pass, KeyCategory::Unknown) => {
                let store = if pass == Pass::Blob {
                    WalkedStore::Blob
                } else {
                    WalkedStore::Metadata
                };
                self.quarantine(store, key).await
            }
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
            self.metadata_store.settings.gc_grace_secs,
        )
        .await
        .map_err(RegistryError::from)?;
        Ok(young == Some(true))
    }

    /// Handle a key matching no known layout: quarantined by default, deleted
    /// under `--delete-unknown`, counted either way.
    async fn quarantine(&self, store: WalkedStore, key: &str) -> Result<(), Error> {
        let key = key.to_string();
        let action = if self.delete_unknown {
            Action::DeleteUnknownKey { store, key }
        } else {
            Action::QuarantineKey { store, key }
        };
        self.sink.apply(action).await?;
        self.stats.quarantined.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Raw metadata-store read tolerating a concurrent deletion.
    pub async fn read_metadata_object(&self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        match self.metadata_store.object_store().get(key).await {
            Ok(raw) => Ok(Some(raw)),
            Err(StorageError::NotFound) => Ok(None),
            Err(e) => Err(RegistryError::from(e).into()),
        }
    }

    pub async fn metadata_exists(&self, key: &str) -> Result<bool, Error> {
        Ok(self
            .metadata_store
            .object_store()
            .exists(key)
            .await
            .map_err(RegistryError::from)?)
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
        self.handled
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(token)
    }
}
