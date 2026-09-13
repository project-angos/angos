use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use tracing::{debug, info};
use uuid::Uuid;

use angos_oci::{Digest, Namespace, Reference, Tag};
use angos_storage::Error as StorageError;
use angos_storage::ObjectStore;

use crate::registry::keys::{DigestKeys, NamespaceKeys};

#[cfg(test)]
use crate::registry::{
    RegistryConfig, repository_resolver::RepositoryResolver, test_utils::create_test_repositories,
};
use crate::{
    command::maintenance::{
        action::{Action, LOST_AND_FOUND_PREFIX, WalkedStore},
        error::Error,
    },
    event_webhook::event::EventActor,
    jobs::store::{ClaimMode, Error as JobStoreError, JobEnvelope, JobStore, job_pending_path},
    jobs::{JobState, Queue},
    layer::{self, IndexLayerPayload},
    registry::{
        Error as RegistryError, Registry,
        blob_store::BlobStore,
        metadata_store::{BlobIndexOperation, LinkKind, LinkOperation, MetadataStore},
    },
    replication::{
        ReplicationJob, ReplicationTarget, build_envelope, build_prune_delete_envelope,
        record_reconcile_outcome,
    },
    scan::{self, ScanImagePayload},
};

/// Internal-process name stamped on the events retention deletions emit.
pub const RETENTION_ACTOR: &str = "prune";

/// A fresh uniquely-named job store for one maintenance run.
#[must_use]
pub fn run_job_store(metadata_store: &MetadataStore, prefix: &str) -> Arc<JobStore> {
    // Maintenance runs never probe the backend, so claims go through the
    // unconditional `create_if_absent` mode.
    Arc::new(JobStore::new(
        metadata_store.object_store().clone(),
        format!("{prefix}-{}", Uuid::new_v4()),
        ClaimMode::Atomic,
    ))
}

/// A sink that receives `Action` values produced by maintenance checks;
/// `apply` takes `&self` so one sink serves many concurrent producers.
#[async_trait]
pub trait ActionSink: Send + Sync {
    async fn apply(&self, action: Action) -> Result<(), Error>;
}

/// Logs actions as dry-run without applying any mutations to storage.
pub struct DryRunSink;

#[async_trait]
impl ActionSink for DryRunSink {
    async fn apply(&self, action: Action) -> Result<(), Error> {
        info!("DRY RUN: would {action}");
        Ok(())
    }
}

/// Applies scrub actions against live storage backends.
#[allow(clippy::struct_field_names)]
pub struct Executor {
    blob_store: Arc<BlobStore>,
    metadata_store: Arc<MetadataStore>,
    job_store: Arc<JobStore>,
    /// The registry the retention deletions run through, so they take the
    /// standard delete path (locking, blob reclaim, events, replication).
    registry: Option<Arc<Registry>>,
}

impl Executor {
    /// Construct an executor over the stores it mutates and the queue
    /// replication enqueues land on.
    #[must_use]
    pub fn new(
        blob_store: Arc<BlobStore>,
        metadata_store: Arc<MetadataStore>,
        job_store: Arc<JobStore>,
    ) -> Self {
        Self {
            blob_store,
            metadata_store,
            job_store,
            registry: None,
        }
    }

    /// Wire the registry tag and manifest deletions run through; both scrub
    /// and prune construct their executors with one.
    #[must_use]
    pub fn with_registry(mut self, registry: Arc<Registry>) -> Self {
        self.registry = Some(registry);
        self
    }

    fn retention_registry(&self) -> Result<&Registry, Error> {
        self.registry.as_deref().ok_or_else(|| {
            Error::Initialization(
                "retention actions require a registry; construct the executor with one".to_string(),
            )
        })
    }

    /// Test-only constructor synthesizing a `JobStore` and a registry over the
    /// same stores, so the retention arms work out of the box.
    #[cfg(test)]
    #[must_use]
    pub fn new_for_test(blob_store: Arc<BlobStore>, metadata_store: Arc<MetadataStore>) -> Self {
        let job_store = Arc::new(JobStore::new(
            metadata_store.object_store().clone(),
            "scrub-test",
            ClaimMode::Atomic,
        ));
        let resolver = Arc::new(
            RepositoryResolver::new(create_test_repositories())
                .expect("test repositories must not have overlapping prefixes"),
        );
        let registry = Registry::new(
            blob_store.clone(),
            metadata_store.clone(),
            resolver,
            RegistryConfig::new(job_store.clone()),
        );
        Self::new(blob_store, metadata_store, job_store).with_registry(registry)
    }

    /// Lands the envelope on the durable replication queue, coalescing on the
    /// `lock_key` its builder chose.
    async fn enqueue_replication(
        &self,
        envelope: Result<JobEnvelope, JobStoreError>,
    ) -> Result<(), Error> {
        let envelope = envelope.map_err(|e| {
            record_reconcile_outcome("failed");
            Error::Replication(format!("failed to build replication envelope: {e}"))
        })?;
        self.job_store.enqueue(envelope).await.map_err(|e| {
            record_reconcile_outcome("failed");
            Error::Replication(format!("failed to enqueue replication job: {e}"))
        })?;
        record_reconcile_outcome("enqueued");
        Ok(())
    }
}

/// Whether the object at `key` is younger than `grace_secs`, by the
/// backend's own timestamp; `None` when the key is gone. A missing timestamp
/// reads as young, so an unreadable age never justifies a deletion.
pub async fn object_younger_than_grace(
    store: &dyn ObjectStore,
    key: &str,
    grace_secs: u64,
) -> Result<Option<bool>, StorageError> {
    let meta = match store.head(key).await {
        Ok(meta) => meta,
        Err(StorageError::NotFound) => return Ok(None),
        Err(e) => return Err(e),
    };
    let grace = i64::try_from(grace_secs).unwrap_or(i64::MAX);
    Ok(Some(meta.last_modified.is_none_or(|modified| {
        Utc::now().signed_duration_since(modified).num_seconds() < grace
    })))
}

impl Executor {
    /// [`object_younger_than_grace`] bound to this executor's grace period
    /// and error domain.
    async fn key_younger_than_grace(
        &self,
        store: &dyn ObjectStore,
        key: &str,
    ) -> Result<Option<bool>, Error> {
        object_younger_than_grace(store, key, self.metadata_store.gc_grace_secs())
            .await
            .map_err(|e| Error::from(RegistryError::from(e)))
    }

    /// The collector's only irreversible action, fenced by the marker protocol
    /// rather than a lock: age-gate, check liveness, publish a run covering the
    /// digest, re-verify under a refreshed marker, then delete. A writer either
    /// landed its reference before the re-verification or saw the marker in its
    /// own check and backed off.
    async fn delete_orphan_blob(&self, digest: Digest) -> Result<(), Error> {
        // Fresh bytes are unconditionally live: an upload's `_own` key or a
        // push's reference may still be in flight.
        let Some(fresh) = self
            .key_younger_than_grace(self.blob_store.object_store().as_ref(), &digest.blob_path())
            .await?
        else {
            return Ok(());
        };
        if fresh || self.metadata_store.blob_references_live(&digest).await? {
            info!("skipping orphan blob deletion: '{digest}' reads live");
            return Ok(());
        }

        let claim = self.metadata_store.gc_claim(&digest, &digest).await?;
        // Fence, then re-verify under the published marker.
        if !self.metadata_store.gc_refresh(&claim).await?
            || self.metadata_store.blob_references_live(&digest).await?
        {
            info!("skipping orphan blob deletion: '{digest}' became live under the marker");
            return self
                .metadata_store
                .gc_release(claim)
                .await
                .map_err(Error::from);
        }
        match self.blob_store.delete_blob(&digest).await {
            Ok(()) | Err(RegistryError::BlobUnknown | RegistryError::NotFound) => {}
            Err(e) => {
                let _ = self.metadata_store.gc_release(claim).await;
                return Err(Error::from(e));
            }
        }
        if let Err(e) = self.metadata_store.delete_blob_references(&digest).await {
            let _ = self.metadata_store.gc_release(claim).await;
            return Err(Error::from(e));
        }
        // The layer listing is derived from the bytes and goes with them.
        if let Err(e) = self
            .metadata_store
            .object_store()
            .delete_prefix(&digest.layer_dir())
            .await
        {
            let _ = self.metadata_store.gc_release(claim).await;
            return Err(Error::from(RegistryError::from(e)));
        }
        self.metadata_store
            .gc_release(claim)
            .await
            .map_err(Error::from)
    }

    /// Remove a blob-index entry, re-checking at apply time that the removal
    /// is still justified. An entry re-legitimized since classification, by an
    /// upload landing the bytes or a push recreating the link, is kept.
    async fn remove_blob_index_link(
        &self,
        namespace: Namespace,
        blob: Digest,
        link: LinkKind,
    ) -> Result<(), Error> {
        let bytes_exist = match self.blob_store.size(&blob).await {
            Ok(_) => true,
            Err(RegistryError::BlobUnknown | RegistryError::NotFound) => false,
            Err(e) => return Err(Error::from(e)),
        };
        if bytes_exist && self.entry_still_backed(&namespace, &link, &blob).await? {
            info!("skipping blob-index removal: entry for '{namespace}/{blob}' is live again");
            return Ok(());
        }
        // A young reference key may be a concurrent push's re-put between
        // its reference wave and its commit; a gone key needs no removal.
        let ref_key = blob.blob_ref_path(&namespace, &link);
        match self
            .key_younger_than_grace(self.metadata_store.object_store().as_ref(), &ref_key)
            .await?
        {
            None => return Ok(()),
            Some(true) => {
                info!(
                    "skipping blob-index removal: entry for '{namespace}/{blob}' is inside the grace period"
                );
                return Ok(());
            }
            Some(false) => {}
        }
        self.metadata_store
            .update_blob_index(&namespace, &blob, BlobIndexOperation::Remove(link))
            .await?;
        Ok(())
    }

    /// Whether the reference entry for `link` is still backed: a blob
    /// self-grant is its own record, every other kind defers to
    /// [`MetadataStore::reference_backed`].
    async fn entry_still_backed(
        &self,
        namespace: &Namespace,
        link: &LinkKind,
        blob: &Digest,
    ) -> Result<bool, Error> {
        if matches!(link, LinkKind::Blob(_)) {
            return Ok(true);
        }
        self.metadata_store
            .reference_backed(namespace, link, blob)
            .await
            .map_err(Error::from)
    }

    /// Re-add a blob-index grant missing for a still-referenced blob. The
    /// bytes are re-checked first, since granting after a concurrent reclaim
    /// would resurrect a reference to a deleted blob.
    async fn grant_blob_index_link(
        &self,
        namespace: Namespace,
        blob: Digest,
        link: LinkKind,
    ) -> Result<(), Error> {
        match self.blob_store.size(&blob).await {
            Ok(_) => {}
            Err(RegistryError::BlobUnknown | RegistryError::NotFound) => {
                info!("skipping blob-index grant: bytes were reclaimed for '{namespace}/{blob}'");
                return Ok(());
            }
            Err(e) => return Err(Error::from(e)),
        }
        self.metadata_store
            .update_blob_index(&namespace, &blob, BlobIndexOperation::Insert(link.clone()))
            .await?;
        Ok(())
    }

    async fn remove_orphan_blob_grant(
        &self,
        namespace: Namespace,
        blob: Digest,
    ) -> Result<(), Error> {
        // A manifest reference may have appeared since the checker classified
        // the grant as orphaned.
        let links = match self
            .metadata_store
            .read_blob_index_namespace(&namespace, &blob)
            .await
        {
            Ok(links) => links,
            // A concurrent revoke or delete already took the grant.
            Err(RegistryError::NotFound) => return Ok(()),
            Err(e) => return Err(Error::from(e)),
        };
        if links.iter().any(LinkKind::is_tracked) {
            info!(
                "skipping orphan grant revoke: a manifest reference appeared for '{namespace}/{blob}'"
            );
            return Ok(());
        }
        // A young `_own` key may be a concurrent upload completion
        // re-granting ownership; a gone key is already revoked.
        let own_key = blob.blob_ref_own_path(&namespace);
        match self
            .key_younger_than_grace(self.metadata_store.object_store().as_ref(), &own_key)
            .await?
        {
            None => return Ok(()),
            Some(true) => {
                info!(
                    "skipping orphan grant revoke: ownership of '{namespace}/{blob}' is inside the grace period"
                );
                return Ok(());
            }
            Some(false) => {}
        }
        // The bytes stay: they are the collector's to reclaim once every
        // reference is stale.
        self.metadata_store
            .revoke_blob_ownership(&namespace, &blob)
            .await?;
        Ok(())
    }

    async fn recreate_link(
        &self,
        namespace: Namespace,
        link: LinkKind,
        target: Digest,
    ) -> Result<(), Error> {
        self.metadata_store
            .update_links(&namespace, &[LinkOperation::create(link, target)])
            .await?;
        Ok(())
    }

    /// Retention tag deletion through the registry's standard delete path, so
    /// it emits the delete events and, per its internal actor, mirrors only to
    /// `prune = true` downstreams. A `target` is the digest the tag was judged
    /// on, and the tag is left alone when it no longer points there.
    async fn delete_tag(
        &self,
        namespace: Namespace,
        tag: Tag,
        target: Option<Digest>,
    ) -> Result<(), Error> {
        if let Some(target) = target {
            // Uncached, like the digest-delete guard: a re-push landed on
            // another replica within the link-cache TTL must be visible here,
            // which is the whole point of re-reading.
            let current = self
                .metadata_store
                .read_link_reference(&namespace, &LinkKind::Tag(tag.clone()))
                .await?;
            if current.target != target {
                info!(
                    "skipping tag deletion: '{namespace}:{tag}' now points at '{}', not the judged '{target}'",
                    current.target
                );
                return Ok(());
            }
        }

        self.retention_registry()?
            .delete_manifest(
                Some(EventActor::internal(RETENTION_ACTOR)),
                None,
                &namespace,
                &Reference::Tag(tag),
            )
            .await?;
        Ok(())
    }

    /// Move one superseded tag entry to the `!hist/` prefix, re-reading the
    /// body at apply time. Hist key first, then the delete, so an interruption
    /// duplicates rather than loses.
    async fn demote_tag_entry(
        &self,
        namespace: Namespace,
        tag: Tag,
        entry_name: String,
    ) -> Result<(), Error> {
        let store = self.metadata_store.object_store();
        let entry_key = format!("{}/{entry_name}", namespace.tag_entry_dir(&tag));
        let body = match store.get(&entry_key).await {
            Ok(body) => body,
            Err(StorageError::NotFound) => return Ok(()),
            Err(e) => return Err(Error::from(RegistryError::from(e))),
        };
        // An interrupted earlier demotion may already have written the copy;
        // the delete below finishes the move either way.
        store
            .create_if_absent(
                &namespace.tag_hist_path(&tag, &entry_name),
                Bytes::from(body),
            )
            .await
            .map_err(RegistryError::from)?;
        match store.delete(&entry_key).await {
            Ok(()) | Err(StorageError::NotFound) => Ok(()),
            Err(e) => Err(Error::from(RegistryError::from(e))),
        }
    }

    /// Retention orphan-manifest deletion through the registry's standard
    /// delete path, which also reclaims the manifest's bytes once unreferenced.
    /// Age-gated like [`Self::delete_orphan_blob`]: every producer of this
    /// action classifies a revision as orphaned the moment its record exists,
    /// which is before the tag or index that will reference it lands.
    async fn delete_orphan_manifest(
        &self,
        namespace: Namespace,
        digest: Digest,
    ) -> Result<(), Error> {
        // A multi-arch push writes its platform manifests by digest before the
        // index that names them, and a single push has the same window between
        // its record and its tag, so a fresh revision is still being pushed.
        if self
            .key_younger_than_grace(
                self.metadata_store.object_store().as_ref(),
                &namespace.revision_record_path(&digest),
            )
            .await?
            .unwrap_or(false)
        {
            info!("skipping orphan manifest deletion: '{digest}' is younger than the grace period");
            return Ok(());
        }

        self.retention_registry()?
            .delete_manifest(
                Some(EventActor::internal(RETENTION_ACTOR)),
                None,
                &namespace,
                &Reference::Digest(digest),
            )
            .await?;
        Ok(())
    }

    async fn delete_orphan_referrer(
        &self,
        namespace: Namespace,
        subject: Digest,
        referrer: Digest,
    ) -> Result<(), Error> {
        self.metadata_store
            .update_links(
                &namespace,
                &[LinkOperation::delete(LinkKind::Referrer {
                    subject,
                    referrer,
                })],
            )
            .await?;
        Ok(())
    }

    async fn enqueue_replication_push(
        &self,
        downstream: String,
        namespace: Namespace,
        tag: Tag,
        digest: Digest,
    ) -> Result<(), Error> {
        // The handler stamps `source_ts` from the tag's `created_at` at execute
        // time, so the push carries the same last-writer-wins version as the
        // event path.
        let job = ReplicationJob::Push {
            target: ReplicationTarget {
                downstream,
                namespace,
                tag: Some(tag),
                digest: Some(digest),
                source_ts: None,
            },
        };
        self.enqueue_replication(build_envelope(&job)).await
    }

    async fn enqueue_scan(&self, scan: ScanImagePayload) -> Result<(), Error> {
        let envelope = scan::build_envelope(&scan)
            .map_err(|e| Error::JobQueue(format!("failed to build scan envelope: {e}")))?;
        self.job_store
            .enqueue(envelope)
            .await
            .map_err(|e| Error::JobQueue(format!("failed to enqueue scan job: {e}")))
    }

    async fn ensure_catalog_index(&self, namespace: Namespace) -> Result<(), Error> {
        self.metadata_store.ensure_catalog_index(&namespace).await;
        Ok(())
    }

    async fn enqueue_index(&self, index: IndexLayerPayload) -> Result<(), Error> {
        let envelope = layer::build_envelope(&index)
            .map_err(|e| Error::JobQueue(format!("failed to build index envelope: {e}")))?;
        self.job_store
            .enqueue(envelope)
            .await
            .map_err(|e| Error::JobQueue(format!("failed to enqueue index job: {e}")))
    }

    async fn reclaim_listing(&self, digest: Digest) -> Result<(), Error> {
        self.metadata_store
            .object_store()
            .delete_prefix(&digest.layer_dir())
            .await
            .map_err(|e| Error::from(RegistryError::from(e)))
    }

    async fn enqueue_replication_delete(
        &self,
        downstream: String,
        namespace: Namespace,
        tag: Tag,
    ) -> Result<(), Error> {
        // The decision-time `source_ts` lets the receiver preserve a downstream
        // tag dated after it. It does NOT make prune active-active safe: a
        // peer's newer tag created before this run is still deleted.
        let job = ReplicationJob::Delete {
            target: ReplicationTarget {
                downstream,
                namespace,
                tag: Some(tag),
                digest: None,
                source_ts: Some(Utc::now()),
            },
            subject: None,
        };
        // Keyed on the bare reference so repeated runs coalesce instead of
        // stacking one fresh-ts job each.
        self.enqueue_replication(build_prune_delete_envelope(&job))
            .await
    }

    async fn delete_orphan_job(
        &self,
        queue: Queue,
        state: JobState,
        storage_key: String,
    ) -> Result<(), Error> {
        match self.job_store.delete_job(queue, state, &storage_key).await {
            Ok(()) => Ok(()),
            // A stale key means the job completed or was deleted concurrently.
            Err(JobStoreError::NotFound) => {
                debug!("{queue} job '{storage_key}' already gone; nothing to delete");
                Ok(())
            }
            Err(e) => Err(Error::JobQueue(format!(
                "failed to delete {queue} job '{storage_key}': {e}"
            ))),
        }
    }

    /// Delete a dedup index whose pending job never landed, re-checking both
    /// the absence and the index's age so a concurrent enqueue between the
    /// walk and here is never dropped.
    async fn delete_orphan_job_index(
        &self,
        queue: Queue,
        key: String,
        storage_key: String,
    ) -> Result<(), Error> {
        let store = self.metadata_store.object_store();
        let pending = job_pending_path(queue.as_str(), &storage_key);
        if store.exists(&pending).await.map_err(RegistryError::from)? {
            return Ok(());
        }
        match self.key_younger_than_grace(store.as_ref(), &key).await? {
            None => Ok(()),
            Some(true) => {
                info!("skipping job index reclaim: '{key}' is inside the grace period");
                Ok(())
            }
            Some(false) => match store.delete(&key).await {
                Ok(()) | Err(StorageError::NotFound) => Ok(()),
                Err(e) => Err(Error::from(RegistryError::from(e))),
            },
        }
    }

    /// The raw object store behind `store`, for exact-key actions produced by
    /// the walk (quarantine, corrupt-object deletion).
    fn walked_object_store(&self, store: WalkedStore) -> &Arc<dyn ObjectStore> {
        match store {
            WalkedStore::Blob => self.blob_store.object_store(),
            WalkedStore::Metadata => self.metadata_store.object_store(),
        }
    }

    /// Move an unrecognized key under the lost-and-found prefix, preserving its
    /// original path. A missing source counts as success: on a shared physical
    /// root both walks see the alien key, and the second finds it moved.
    async fn quarantine_key(&self, store: WalkedStore, key: String) -> Result<(), Error> {
        let destination = format!("{LOST_AND_FOUND_PREFIX}/{key}");
        if let Err(e) = self
            .walked_object_store(store)
            .move_object(&key, &destination)
            .await
        {
            match RegistryError::from(e) {
                RegistryError::NotFound => {}
                other => return Err(other.into()),
            }
        }
        Ok(())
    }

    /// Delete an exact walked key: an expected-shape object with unreadable
    /// content, or an unrecognized key under `--delete-unknown`.
    async fn delete_walked_key(&self, store: WalkedStore, key: String) -> Result<(), Error> {
        self.walked_object_store(store)
            .delete(&key)
            .await
            .map_err(RegistryError::from)?;
        Ok(())
    }
}

#[async_trait]
impl ActionSink for Executor {
    async fn apply(&self, action: Action) -> Result<(), Error> {
        info!("{action}");

        match action {
            Action::DeleteOrphanBlob(digest) => self.delete_orphan_blob(digest).await,
            Action::RemoveBlobIndexLink {
                namespace,
                blob,
                link,
            } => self.remove_blob_index_link(namespace, blob, link).await,
            Action::GrantBlobIndexLink {
                namespace,
                blob,
                link,
            } => self.grant_blob_index_link(namespace, blob, link).await,
            Action::EnsureCatalogIndex { namespace } => self.ensure_catalog_index(namespace).await,
            Action::RemoveOrphanBlobGrant { namespace, blob } => {
                self.remove_orphan_blob_grant(namespace, blob).await
            }
            Action::RecreateLink {
                namespace,
                link,
                target,
            } => self.recreate_link(namespace, link, target).await,
            Action::DeleteTag {
                namespace,
                tag,
                target,
            } => self.delete_tag(namespace, tag, target).await,
            Action::DemoteTagEntry {
                namespace,
                tag,
                entry_name,
            } => self.demote_tag_entry(namespace, tag, entry_name).await,
            // An upload-only namespace whose name fails `Namespace` validation
            // is reclaimed by removing its upload subtree from the blob store.
            Action::DeleteInvalidUploadNamespace { name } => self
                .blob_store
                .delete_namespace_directory(&name)
                .await
                .map_err(Error::from),
            Action::DeleteOrphanManifest { namespace, digest } => {
                self.delete_orphan_manifest(namespace, digest).await
            }
            Action::DeleteExpiredUpload {
                namespace,
                session_id,
            } => self
                .blob_store
                .delete_upload(&namespace, &session_id)
                .await
                .map_err(Error::from),
            Action::DeleteOrphanReferrer {
                namespace,
                subject,
                referrer,
            } => {
                self.delete_orphan_referrer(namespace, subject, referrer)
                    .await
            }
            Action::AbortMultipartUpload { upload } => self
                .blob_store
                .abort_orphan_multipart_upload(&upload)
                .await
                .map_err(Error::from),
            Action::EnqueueReplicationPush {
                downstream,
                namespace,
                tag,
                digest,
            } => {
                self.enqueue_replication_push(downstream, namespace, tag, digest)
                    .await
            }
            Action::EnqueueScan(scan) => self.enqueue_scan(scan).await,
            Action::EnqueueIndex(index) => self.enqueue_index(index).await,
            Action::ReclaimListing(digest) => self.reclaim_listing(digest).await,
            Action::EnqueueReplicationDelete {
                downstream,
                namespace,
                tag,
            } => {
                self.enqueue_replication_delete(downstream, namespace, tag)
                    .await
            }
            Action::DeleteOrphanJob {
                queue,
                state,
                storage_key,
                ..
            } => self.delete_orphan_job(queue, state, storage_key).await,
            Action::DeleteOrphanJobIndex {
                queue,
                key,
                storage_key,
            } => self.delete_orphan_job_index(queue, key, storage_key).await,
            Action::QuarantineKey { store, key } => self.quarantine_key(store, key).await,
            Action::DeleteCorruptObject { store, key }
            | Action::DeleteUnknownKey { store, key } => self.delete_walked_key(store, key).await,
            Action::RetireAtimeKey { key } => {
                self.delete_walked_key(WalkedStore::Metadata, key).await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use chrono::{DateTime, TimeDelta};
    use tempfile::TempDir;

    use angos_oci::{Digest, UploadSessionId};
    use angos_storage::fs::Backend as StorageFsBackend;

    use crate::command::maintenance::executor::*;
    use crate::{
        cache_fill::{CACHE_FETCH_BLOB_KIND, CacheFetchBlobPayload},
        jobs::store::{ClaimMode, FailOutcome},
        registry::{
            metadata_store::{LinkKind, LinkOperation, MetadataStore, ReferencePolicy},
            test_utils::{FSRegistryTestCase, RegistryTestCase, for_each_backend, put_blob_direct},
        },
        replication::REPLICATION_DELETE_MANIFEST_KIND,
    };

    /// A producer `JobStore` over a private store no worker drains, plus the
    /// temp directory backing it. Tests that assert queue depth must not share
    /// the registry store, whose in-process claim loops would otherwise claim
    /// the job and race the assertion.
    fn standalone_job_store(worker_id: &str) -> (Arc<JobStore>, TempDir) {
        let dir = TempDir::new().expect("temp dir");
        let raw = Arc::new(StorageFsBackend::builder(dir.path()).build());
        (
            Arc::new(JobStore::new(raw, worker_id, ClaimMode::Atomic)),
            dir,
        )
    }

    #[tokio::test]
    async fn executor_dry_run_does_not_delete_blob() {
        for_each_backend(async |test_case| {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            let orphan_content = b"executor dry-run test";
            let orphan_digest =
                put_blob_direct(metadata_store.object_store(), orphan_content).await;

            let sink = DryRunSink;
            sink.apply(Action::DeleteOrphanBlob(orphan_digest.clone()))
                .await
                .unwrap();

            assert!(
                blob_store.read(&orphan_digest).await.is_ok(),
                "dry-run must not delete the blob"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn executor_real_run_deletes_blob() {
        for_each_backend(async |test_case| {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            let orphan_content = b"executor real-run test";
            let orphan_digest =
                put_blob_direct(metadata_store.object_store(), orphan_content).await;

            let executor = Executor::new_for_test(blob_store.clone(), metadata_store);

            executor
                .apply(Action::DeleteOrphanBlob(orphan_digest.clone()))
                .await
                .unwrap();

            assert!(
                blob_store.read(&orphan_digest).await.is_err(),
                "real-run must delete the blob"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn executor_reclaim_listing_deletes_the_layer_directory() {
        for_each_backend(async |test_case| {
            let metadata_store = test_case.metadata_store();
            let store = metadata_store.object_store();
            let layer = Digest::sha256_of_bytes(b"listed layer");
            for key in [layer.layer_entries_path(), layer.layer_checkpoints_path()] {
                store.put(&key, Bytes::from_static(b"{}")).await.unwrap();
            }

            let executor = Executor::new_for_test(test_case.blob_store(), metadata_store.clone());
            executor
                .apply(Action::ReclaimListing(layer.clone()))
                .await
                .unwrap();

            for key in [layer.layer_entries_path(), layer.layer_checkpoints_path()] {
                assert!(store.get(&key).await.is_err(), "{key} must be gone");
            }
        })
        .await;
    }

    /// A cache fill or upload can land bytes for a grant classified as
    /// byteless; the apply-time re-check must then keep the grant.
    #[tokio::test]
    async fn executor_remove_blob_index_link_keeps_byteless_grant_after_bytes_land() {
        for_each_backend(async |test_case| {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();
            let namespace = Namespace::new("test-repo/app").unwrap();

            // Grant first (as an upload does), then the bytes land before the
            // prune-emitted removal is applied.
            let digest = put_blob_direct(metadata_store.object_store(), b"bytes landed late").await;
            metadata_store
                .update_blob_index(
                    &namespace,
                    &digest,
                    BlobIndexOperation::Insert(LinkKind::Blob(digest.clone())),
                )
                .await
                .unwrap();

            let executor = Executor::new_for_test(blob_store, metadata_store.clone());
            executor
                .apply(Action::RemoveBlobIndexLink {
                    namespace: namespace.clone(),
                    blob: digest.clone(),
                    link: LinkKind::Blob(digest.clone()),
                })
                .await
                .unwrap();

            assert!(
                metadata_store
                    .read_blob_index_namespace(&namespace, &digest)
                    .await
                    .is_ok_and(|links| links.contains(&LinkKind::Blob(digest.clone()))),
                "a grant whose bytes exist must survive the stale removal"
            );
        })
        .await;
    }

    /// A genuinely byteless grant past the window is still removed.
    #[tokio::test]
    async fn executor_remove_blob_index_link_removes_byteless_grant() {
        for_each_backend(async |test_case| {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();
            let namespace = Namespace::new("test-repo/app").unwrap();

            let digest = Digest::sha256_of_bytes(b"never uploaded");
            metadata_store
                .update_blob_index(
                    &namespace,
                    &digest,
                    BlobIndexOperation::Insert(LinkKind::Blob(digest.clone())),
                )
                .await
                .unwrap();

            let executor = Executor::new_for_test(blob_store, metadata_store.clone());
            executor
                .apply(Action::RemoveBlobIndexLink {
                    namespace: namespace.clone(),
                    blob: digest.clone(),
                    link: LinkKind::Blob(digest.clone()),
                })
                .await
                .unwrap();

            assert!(
                metadata_store
                    .read_blob_index_namespace(&namespace, &digest)
                    .await
                    .is_err(),
                "a byteless grant must be removed"
            );
        })
        .await;
    }

    /// A push can recreate the revision behind an entry scrub confirmed
    /// dangling; the apply-time re-check must then keep the entry.
    #[tokio::test]
    async fn executor_remove_blob_index_link_keeps_entry_whose_link_reappeared() {
        for_each_backend(async |test_case| {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();
            let namespace = Namespace::new("test-repo/app").unwrap();

            // A pushed layer: its per-referrer entry is backed while the
            // referring manifest's revision resolves.
            let digest = put_blob_direct(metadata_store.object_store(), b"layer re-pushed").await;
            let parent = put_blob_direct(metadata_store.object_store(), b"parent manifest").await;
            metadata_store
                .update_links(
                    &namespace,
                    &[
                        LinkOperation::create(LinkKind::Digest(parent.clone()), parent.clone()),
                        LinkOperation::create_with_referrer(
                            LinkKind::Layer(digest.clone()),
                            digest.clone(),
                            parent.clone(),
                        ),
                    ],
                )
                .await
                .unwrap();

            let executor = Executor::new_for_test(blob_store, metadata_store.clone());
            executor
                .apply(Action::RemoveBlobIndexLink {
                    namespace: namespace.clone(),
                    blob: digest.clone(),
                    link: LinkKind::ReferencedBy(parent.clone()),
                })
                .await
                .unwrap();

            assert!(
                metadata_store
                    .read_blob_index_namespace(&namespace, &digest)
                    .await
                    .is_ok_and(|links| links.contains(&LinkKind::ReferencedBy(parent.clone()))),
                "an entry backed by a live referring revision must survive the stale removal"
            );
        })
        .await;
    }

    /// A dangling entry (link file gone, bytes present) is still removed.
    #[tokio::test]
    async fn executor_remove_blob_index_link_removes_dangling_entry() {
        for_each_backend(async |test_case| {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();
            let namespace = Namespace::new("test-repo/app").unwrap();

            // Shard entry without its link file: the dangling state scrub's
            // shard pass confirms before emitting the removal.
            let digest = put_blob_direct(metadata_store.object_store(), b"dangling entry").await;
            metadata_store
                .update_blob_index(
                    &namespace,
                    &digest,
                    BlobIndexOperation::Insert(LinkKind::Layer(digest.clone())),
                )
                .await
                .unwrap();

            let executor = Executor::new_for_test(blob_store, metadata_store.clone());
            executor
                .apply(Action::RemoveBlobIndexLink {
                    namespace: namespace.clone(),
                    blob: digest.clone(),
                    link: LinkKind::Layer(digest.clone()),
                })
                .await
                .unwrap();

            assert!(
                metadata_store
                    .read_blob_index_namespace(&namespace, &digest)
                    .await
                    .is_err(),
                "a dangling entry must be removed"
            );
        })
        .await;
    }

    /// A reference key inside the grace period may be a concurrent push's
    /// re-put between its reference wave and its commit, so a graced executor
    /// must keep it even when it reads stale.
    #[tokio::test]
    async fn executor_remove_blob_index_link_keeps_young_reference_key() {
        for_each_backend(async |test_case| {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();
            let namespace = Namespace::new("test-repo/app").unwrap();

            // Byteless and unbacked, but freshly written.
            let digest = Digest::sha256_of_bytes(b"re-pushed between waves");
            let link = LinkKind::Layer(digest.clone());
            metadata_store
                .update_blob_index(
                    &namespace,
                    &digest,
                    BlobIndexOperation::Insert(link.clone()),
                )
                .await
                .unwrap();

            // Same stores, but an executor whose grace period is real.
            let graced = Arc::new(
                MetadataStore::builder(metadata_store.object_store().clone())
                    .gc_grace_secs(300)
                    .build(),
            );
            let executor = Executor::new_for_test(blob_store, graced);
            executor
                .apply(Action::RemoveBlobIndexLink {
                    namespace: namespace.clone(),
                    blob: digest.clone(),
                    link: link.clone(),
                })
                .await
                .unwrap();

            assert!(
                metadata_store
                    .read_blob_index_namespace(&namespace, &digest)
                    .await
                    .is_ok_and(|links| links.contains(&link)),
                "a reference key inside the grace period must be kept"
            );
        })
        .await;
    }

    /// An `_own` key inside the grace period may be a concurrent upload
    /// completion re-granting ownership, so a graced executor must keep it.
    #[tokio::test]
    async fn executor_remove_orphan_blob_grant_keeps_young_ownership() {
        for_each_backend(async |test_case| {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();
            let namespace = Namespace::new("test-repo/app").unwrap();

            let digest = put_blob_direct(metadata_store.object_store(), b"grant-only blob").await;
            metadata_store
                .update_blob_index(
                    &namespace,
                    &digest,
                    BlobIndexOperation::Insert(LinkKind::Blob(digest.clone())),
                )
                .await
                .unwrap();

            let graced = Arc::new(
                MetadataStore::builder(metadata_store.object_store().clone())
                    .gc_grace_secs(300)
                    .build(),
            );
            let executor = Executor::new_for_test(blob_store, graced);
            executor
                .apply(Action::RemoveOrphanBlobGrant {
                    namespace: namespace.clone(),
                    blob: digest.clone(),
                })
                .await
                .unwrap();

            assert!(
                metadata_store
                    .read_blob_index_namespace(&namespace, &digest)
                    .await
                    .is_ok_and(|links| links.contains(&LinkKind::Blob(digest.clone()))),
                "an ownership grant inside the grace period must be kept"
            );
        })
        .await;
    }

    /// CI that re-pushes a tag between the retention snapshot and the delete
    /// must not lose the image it just pushed, and a tag still pointing where
    /// it was judged must still go.
    #[tokio::test]
    async fn executor_deletes_a_tag_only_while_it_points_where_it_was_judged() {
        let case = FSRegistryTestCase::new();
        let blob_store = case.blob_store();
        let metadata_store = case.metadata_store();
        let namespace = Namespace::new("test-repo/ci").unwrap();
        let tag = Tag::new("latest").unwrap();

        let judged = put_blob_direct(metadata_store.object_store(), b"what retention judged").await;
        let repushed = put_blob_direct(metadata_store.object_store(), b"what CI just pushed").await;
        // Both in the past, so the delete's own tombstone outranks them.
        let pushed_at = Utc::now() - TimeDelta::seconds(10);
        for (digest, at) in [
            (&judged, pushed_at),
            (&repushed, pushed_at + TimeDelta::seconds(1)),
        ] {
            metadata_store
                .store_manifest(
                    &namespace,
                    &[LinkOperation::create(
                        LinkKind::Tag(tag.clone()),
                        digest.clone(),
                    )],
                    Some(at),
                    ReferencePolicy::Trusted,
                )
                .await
                .unwrap();
        }

        let executor = Executor::new_for_test(blob_store.clone(), metadata_store.clone());
        executor
            .apply(Action::DeleteTag {
                namespace: namespace.clone(),
                tag: tag.clone(),
                target: Some(judged),
            })
            .await
            .unwrap();

        assert_eq!(
            metadata_store
                .read_link_reference(&namespace, &LinkKind::Tag(tag.clone()))
                .await
                .unwrap()
                .target,
            repushed,
            "a tag re-pointed since it was judged must survive"
        );

        executor
            .apply(Action::DeleteTag {
                namespace: namespace.clone(),
                tag: tag.clone(),
                target: Some(repushed),
            })
            .await
            .unwrap();

        assert!(
            metadata_store
                .read_link_reference(&namespace, &LinkKind::Tag(tag))
                .await
                .is_err(),
            "a tag still pointing where it was judged must be deleted"
        );
    }

    /// Every producer of this action treats a revision as orphaned the moment
    /// its record exists, which is before the index or tag that will reference
    /// it lands. A multi-arch push writes its platform manifests first, so
    /// without the gate retention deletes them mid-push.
    #[tokio::test]
    async fn executor_keeps_an_orphan_manifest_younger_than_the_grace() {
        let case = FSRegistryTestCase::new();
        let blob_store = case.blob_store();
        let metadata_store = case.metadata_store();
        let namespace = Namespace::new("test-repo/mid-push").unwrap();

        let content = b"a platform manifest whose index has not landed yet";
        let digest = put_blob_direct(metadata_store.object_store(), content).await;
        metadata_store
            .update_links(
                &namespace,
                &[LinkOperation::create(
                    LinkKind::Digest(digest.clone()),
                    digest.clone(),
                )],
            )
            .await
            .unwrap();

        // The same store, read through a grace period the record cannot have
        // outlived.
        let graced = Arc::new(
            MetadataStore::builder(metadata_store.object_store().clone())
                .gc_grace_secs(300)
                .build(),
        );
        let executor = Executor::new_for_test(blob_store.clone(), graced);

        executor
            .apply(Action::DeleteOrphanManifest {
                namespace: namespace.clone(),
                digest: digest.clone(),
            })
            .await
            .unwrap();

        assert!(
            metadata_store
                .read_link(&namespace, &LinkKind::Digest(digest.clone()))
                .await
                .is_ok(),
            "a revision younger than the grace is still being pushed and must survive"
        );
    }

    #[tokio::test]
    async fn executor_delete_orphan_manifest_missing_blob_still_removes_digest_link() {
        for_each_backend(async |test_case| {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            let namespace = Namespace::new("test-repo/app").unwrap();

            // Write manifest blob and create a digest link, then delete the blob.
            let content = b"orphan manifest content for missing-blob test";
            let digest = put_blob_direct(metadata_store.object_store(), content).await;
            metadata_store
                .update_links(
                    &namespace,
                    &[LinkOperation::create(
                        LinkKind::Digest(digest.clone()),
                        digest.clone(),
                    )],
                )
                .await
                .unwrap();
            blob_store.delete_blob(&digest).await.unwrap();

            let executor = Executor::new_for_test(blob_store.clone(), metadata_store.clone());

            executor
                .apply(Action::DeleteOrphanManifest {
                    namespace: namespace.clone(),
                    digest: digest.clone(),
                })
                .await
                .unwrap();

            assert!(
                metadata_store
                    .read_link(&namespace, &LinkKind::Digest(digest.clone()))
                    .await
                    .is_err(),
                "digest link must be removed even when the blob is missing"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn executor_delete_orphan_manifest_missing_blob_removes_tag_link() {
        for_each_backend(async |test_case| {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            let namespace = Namespace::new("test-repo/app").unwrap();

            let content = b"orphan manifest with tag - missing blob";
            let digest = put_blob_direct(metadata_store.object_store(), content).await;
            metadata_store
                .update_links(
                    &namespace,
                    &[
                        LinkOperation::create(LinkKind::Digest(digest.clone()), digest.clone()),
                        LinkOperation::create(
                            LinkKind::Tag(Tag::new("dangling").unwrap()),
                            digest.clone(),
                        ),
                    ],
                )
                .await
                .unwrap();
            blob_store.delete_blob(&digest).await.unwrap();

            let executor = Executor::new_for_test(blob_store.clone(), metadata_store.clone());

            executor
                .apply(Action::DeleteOrphanManifest {
                    namespace: namespace.clone(),
                    digest: digest.clone(),
                })
                .await
                .unwrap();

            assert!(
                metadata_store
                    .read_link(&namespace, &LinkKind::Tag(Tag::new("dangling").unwrap()))
                    .await
                    .is_err(),
                "tag link pointing at missing-blob digest must be removed"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn executor_delete_orphan_blob_skips_when_reference_appears_between_classification_and_apply()
     {
        for_each_backend(async |test_case| {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            let content = b"blob that got ownership just in time";
            let digest = put_blob_direct(metadata_store.object_store(), content).await;

            metadata_store
                .update_blob_index(
                    &Namespace::new("test-repo/app").unwrap(),
                    &digest,
                    BlobIndexOperation::Insert(LinkKind::Blob(digest.clone())),
                )
                .await
                .unwrap();

            let executor = Executor::new_for_test(blob_store.clone(), metadata_store);

            executor
                .apply(Action::DeleteOrphanBlob(digest.clone()))
                .await
                .unwrap();

            assert!(
                blob_store.read(&digest).await.is_ok(),
                "blob with a reference must not be deleted"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn executor_grant_blob_index_link_inserts_when_bytes_exist() {
        for_each_backend(async |test_case| {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            let namespace = Namespace::new("test-repo/app").unwrap();
            let digest = put_blob_direct(metadata_store.object_store(), b"granted layer").await;

            let executor = Executor::new_for_test(blob_store.clone(), metadata_store.clone());
            executor
                .apply(Action::GrantBlobIndexLink {
                    namespace: namespace.clone(),
                    blob: digest.clone(),
                    link: LinkKind::Layer(digest.clone()),
                })
                .await
                .unwrap();

            let links = metadata_store
                .read_blob_index_namespace(&namespace, &digest)
                .await
                .unwrap();
            assert!(
                links.contains(&LinkKind::Layer(digest.clone())),
                "the grant must insert the layer link into the blob index"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn executor_grant_blob_index_link_skips_when_bytes_were_reclaimed() {
        for_each_backend(async |test_case| {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            // A digest the reconcile classified for a grant whose bytes a
            // concurrent reclaim deleted before this apply: the under-lock
            // re-check must refuse to resurrect a reference to the absent blob.
            let namespace = Namespace::new("test-repo/app").unwrap();
            let digest = Digest::from_str(
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            )
            .unwrap();

            let executor = Executor::new_for_test(blob_store.clone(), metadata_store.clone());
            executor
                .apply(Action::GrantBlobIndexLink {
                    namespace: namespace.clone(),
                    blob: digest.clone(),
                    link: LinkKind::Layer(digest.clone()),
                })
                .await
                .unwrap();

            assert!(
                metadata_store
                    .read_blob_index_namespace(&namespace, &digest)
                    .await
                    .is_err(),
                "no index entry must be created for a blob with no bytes"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn executor_delete_orphan_referrer_removes_referrer_link() {
        for_each_backend(async |test_case| {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            let namespace = Namespace::new("test-repo/referrer-exec").unwrap();

            let subject_digest =
                put_blob_direct(metadata_store.object_store(), b"subject for referrer exec").await;
            let referrer_digest =
                put_blob_direct(metadata_store.object_store(), b"referrer for referrer exec").await;

            metadata_store
                .update_links(
                    &namespace,
                    &[
                        LinkOperation::create(
                            LinkKind::Digest(subject_digest.clone()),
                            subject_digest.clone(),
                        ),
                        LinkOperation::create(
                            LinkKind::Referrer {
                                subject: subject_digest.clone(),
                                referrer: referrer_digest.clone(),
                            },
                            referrer_digest.clone(),
                        ),
                    ],
                )
                .await
                .unwrap();

            assert!(
                metadata_store
                    .read_link(
                        &namespace,
                        &LinkKind::Referrer {
                            subject: subject_digest.clone(),
                            referrer: referrer_digest.clone()
                        }
                    )
                    .await
                    .is_ok(),
                "Referrer link must exist before applying the action"
            );

            let executor = Executor::new_for_test(blob_store.clone(), metadata_store.clone());

            executor
                .apply(Action::DeleteOrphanReferrer {
                    namespace: namespace.clone(),
                    subject: subject_digest.clone(),
                    referrer: referrer_digest.clone(),
                })
                .await
                .unwrap();

            assert!(
                metadata_store
                    .read_link(
                        &namespace,
                        &LinkKind::Referrer {
                            subject: subject_digest.clone(),
                            referrer: referrer_digest.clone()
                        }
                    )
                    .await
                    .is_err(),
                "Referrer link must be removed after applying the action"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn enqueue_replication_delete_stamps_source_ts_for_receiver_lww() {
        for_each_backend(async |test_case| {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            let (job_store, _job_dir) = standalone_job_store("scrub-source-ts");

            let executor = Executor::new(blob_store, metadata_store, job_store.clone());

            executor
                .apply(Action::EnqueueReplicationDelete {
                    downstream: "mirror".to_string(),
                    namespace: Namespace::new("ns/app").unwrap(),
                    tag: Tag::new("stray").unwrap(),
                })
                .await
                .unwrap();

            let claimed = job_store
                .claim_one(Queue::Replication)
                .await
                .unwrap()
                .claimed
                .expect("a delete job must be enqueued");

            assert_eq!(claimed.envelope.kind, REPLICATION_DELETE_MANIFEST_KIND);
            let source_ts = claimed.envelope.payload.get("source_ts").cloned();
            let source_ts = source_ts
                .as_ref()
                .and_then(serde_json::Value::as_str)
                .expect("prune delete payload must carry a source_ts (not None)");
            assert!(
                DateTime::parse_from_rfc3339(source_ts).is_ok(),
                "source_ts must be a valid RFC 3339 timestamp; got {source_ts}"
            );
        })
        .await;
    }

    /// Each apply stamps a fresh decision-time `source_ts`, so this pins that
    /// the prune `lock_key` excludes it: a second run against a still-failing
    /// downstream must coalesce, not stack a second job per tag.
    #[tokio::test]
    async fn prune_delete_enqueues_coalesce_while_one_is_pending() {
        for_each_backend(async |test_case| {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            let (job_store, _job_dir) = standalone_job_store("scrub-prune-coalesce");

            let executor = Executor::new(blob_store, metadata_store, job_store.clone());

            for _ in 0..2 {
                executor
                    .apply(Action::EnqueueReplicationDelete {
                        downstream: "mirror".to_string(),
                        namespace: Namespace::new("ns/app").unwrap(),
                        tag: Tag::new("stray").unwrap(),
                    })
                    .await
                    .unwrap();
            }

            assert_eq!(
                job_store
                    .count_pending(Queue::Replication, 0)
                    .await
                    .unwrap(),
                1,
                "two prune-delete enqueues for the same (downstream, namespace, tag) \
                 must coalesce into a single pending job"
            );
        })
        .await;
    }

    /// Builds an orphan-shaped replication push envelope.
    fn orphan_push_envelope() -> JobEnvelope {
        let job = ReplicationJob::Push {
            target: ReplicationTarget {
                downstream: "removed".to_string(),
                namespace: Namespace::new("ns/app").unwrap(),
                tag: Some(Tag::new("v1").unwrap()),
                digest: None,
                source_ts: None,
            },
        };
        build_envelope(&job).unwrap()
    }

    /// Builds an orphan-shaped pull-through cache-fill envelope.
    fn orphan_cache_envelope() -> JobEnvelope {
        let payload = CacheFetchBlobPayload {
            namespace: Namespace::new("ns/app").unwrap(),
            digest: "sha256:1111111111111111111111111111111111111111111111111111111111111111"
                .parse()
                .unwrap(),
        };
        JobEnvelope::new(
            Queue::Cache,
            CACHE_FETCH_BLOB_KIND,
            "cache.ns/app",
            &payload,
        )
        .unwrap()
    }

    fn delete_orphan_action(queue: Queue, state: JobState, storage_key: String) -> Action {
        Action::DeleteOrphanJob {
            queue,
            state,
            storage_key,
            reason: "configuration no longer resolves this job".to_string(),
        }
    }

    #[tokio::test]
    async fn executor_delete_orphan_job_removes_pending_jobs_on_both_queues() {
        for_each_backend(async |test_case| {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();
            let (job_store, _job_dir) = standalone_job_store("scrub-orphan");

            let executor = Executor::new(blob_store, metadata_store, job_store.clone());

            for (queue, envelope) in [
                (Queue::Replication, orphan_push_envelope()),
                (Queue::Cache, orphan_cache_envelope()),
            ] {
                job_store.enqueue(envelope).await.unwrap();
                let keys = job_store.list_pending(queue, 10).await.unwrap();
                assert_eq!(keys.len(), 1);

                executor
                    .apply(delete_orphan_action(
                        queue,
                        JobState::Pending,
                        keys[0].clone(),
                    ))
                    .await
                    .unwrap();

                assert_eq!(
                    job_store.count_pending(queue, 0).await.unwrap(),
                    0,
                    "the pending orphan job on '{queue}' must be deleted"
                );
            }
        })
        .await;
    }

    #[tokio::test]
    async fn executor_delete_orphan_job_removes_failed_jobs_on_both_queues() {
        for_each_backend(async |test_case| {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();
            let (job_store, _job_dir) = standalone_job_store("scrub-orphan");

            let executor = Executor::new(blob_store, metadata_store, job_store.clone());

            for (queue, mut envelope) in [
                (Queue::Replication, orphan_push_envelope()),
                (Queue::Cache, orphan_cache_envelope()),
            ] {
                // A single-attempt job failed once dead-letters under its
                // original key.
                envelope.max_attempts = Some(1);
                job_store.enqueue(envelope).await.unwrap();
                let claimed = job_store
                    .claim_one(queue)
                    .await
                    .unwrap()
                    .claimed
                    .expect("the job must be claimable");
                let outcome = job_store.fail(claimed, "simulated failure").await.unwrap();
                assert!(matches!(outcome, FailOutcome::MovedToDeadLetter));

                let failed_keys = job_store
                    .list_failed_page(queue, 10, None)
                    .await
                    .unwrap()
                    .items;
                assert_eq!(failed_keys.len(), 1);

                executor
                    .apply(delete_orphan_action(
                        queue,
                        JobState::Failed,
                        failed_keys[0].clone(),
                    ))
                    .await
                    .unwrap();

                let failed_keys = job_store
                    .list_failed_page(queue, 10, None)
                    .await
                    .unwrap()
                    .items;
                assert!(
                    failed_keys.is_empty(),
                    "the dead-lettered orphan job on '{queue}' must be deleted"
                );
            }
        })
        .await;
    }

    #[tokio::test]
    async fn executor_delete_orphan_job_tolerates_stale_key() {
        for_each_backend(async |test_case| {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();
            let (job_store, _job_dir) = standalone_job_store("scrub-orphan");

            let executor = Executor::new(blob_store, metadata_store, job_store);

            for queue in [Queue::Replication, Queue::Cache] {
                for state in [JobState::Pending, JobState::Failed] {
                    executor
                        .apply(delete_orphan_action(
                            queue,
                            state,
                            "0000000000000000-already-gone".to_string(),
                        ))
                        .await
                        .expect("a stale storage_key must be tolerated, not an error");
                }
            }
        })
        .await;
    }

    #[tokio::test]
    async fn vec_sink_captures_actions_without_io() {
        let digest = Digest::from_str(
            "sha256:0000000000000000000000000000000000000000000000000000000000000000",
        )
        .unwrap();

        let sink: std::sync::Mutex<Vec<Action>> = std::sync::Mutex::new(Vec::new());
        sink.apply(Action::DeleteOrphanBlob(digest.clone()))
            .await
            .unwrap();
        sink.apply(Action::DeleteExpiredUpload {
            namespace: Namespace::new("ns").unwrap(),
            session_id: UploadSessionId::generate(),
        })
        .await
        .unwrap();

        assert_eq!(sink.lock().unwrap().len(), 2);
        assert!(matches!(
            sink.lock().unwrap()[0],
            Action::DeleteOrphanBlob(_)
        ));
        assert!(matches!(
            sink.lock().unwrap()[1],
            Action::DeleteExpiredUpload { .. }
        ));
    }

    #[tokio::test]
    async fn quarantine_key_moves_object_under_lost_and_found() {
        for_each_backend(async |test_case| {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();
            let executor = Executor::new_for_test(blob_store.clone(), metadata_store.clone());

            let key = "junk/unexpected-object";
            metadata_store
                .object_store()
                .put(key, bytes::Bytes::from_static(b"alien"))
                .await
                .unwrap();

            executor
                .apply(Action::QuarantineKey {
                    store: WalkedStore::Metadata,
                    key: key.to_string(),
                })
                .await
                .unwrap();

            let objects = metadata_store.object_store();
            assert!(objects.get(key).await.is_err(), "original key must be gone");
            assert_eq!(
                objects
                    .get(&format!("{LOST_AND_FOUND_PREFIX}/{key}"))
                    .await
                    .unwrap(),
                b"alien",
                "bytes must be preserved under the lost-and-found prefix"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn delete_corrupt_object_removes_key_in_named_store() {
        for_each_backend(async |test_case| {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();
            let executor = Executor::new_for_test(blob_store.clone(), metadata_store.clone());

            let key = "v2/repositories/junk-ns/_manifests/tags/x/current/link";
            blob_store
                .object_store()
                .put(key, bytes::Bytes::from_static(b"not json"))
                .await
                .unwrap();

            executor
                .apply(Action::DeleteCorruptObject {
                    store: WalkedStore::Blob,
                    key: key.to_string(),
                })
                .await
                .unwrap();

            assert!(blob_store.object_store().get(key).await.is_err());
        })
        .await;
    }

    #[tokio::test]
    async fn delete_unknown_key_removes_key_without_quarantining() {
        for_each_backend(async |test_case| {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();
            let executor = Executor::new_for_test(blob_store.clone(), metadata_store.clone());

            let key = "junk/unexpected-object";
            let objects = metadata_store.object_store();
            objects
                .put(key, bytes::Bytes::from_static(b"alien"))
                .await
                .unwrap();

            executor
                .apply(Action::DeleteUnknownKey {
                    store: WalkedStore::Metadata,
                    key: key.to_string(),
                })
                .await
                .unwrap();

            assert!(objects.get(key).await.is_err(), "the key must be gone");
            assert!(
                objects
                    .get(&format!("{LOST_AND_FOUND_PREFIX}/{key}"))
                    .await
                    .is_err(),
                "a deleted unknown key must leave no quarantined copy"
            );
        })
        .await;
    }
}
