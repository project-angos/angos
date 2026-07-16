use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use tracing::{debug, info};
use uuid::Uuid;

use crate::{
    command::scrub::{action::Action, error::Error},
    event_webhook::event::EventActor,
    jobs::store::{Error as JobStoreError, JobEnvelope, JobStore},
    jobs::{JobState, Queue},
    metrics_provider::metrics_provider,
    oci::{Digest, Namespace, Reference, Tag},
    registry::{
        Error as RegistryError, Registry,
        blob_store::{self, BlobStore, MultipartCleanup},
        metadata_store::{BlobIndexOperation, LinkKind, LinkOperation, MetadataStore},
    },
    replication::{
        REPLICATION_DELETE_MANIFEST_KIND, REPLICATION_PUSH_MANIFEST_KIND, ReplicationPushPayload,
        build_envelope, build_prune_delete_envelope,
    },
};

#[cfg(test)]
use crate::registry::{
    RegistryConfig, repository_resolver::RepositoryResolver, test_utils::create_test_repositories,
};

/// Internal-process name stamped on the events retention deletions emit.
pub const RETENTION_ACTOR: &str = "prune";

/// A fresh uniquely-named job store for one maintenance run: the executor's
/// replication-enqueue target and, when a drain is wired, its consumer.
#[must_use]
pub fn run_job_store(metadata_store: &MetadataStore, prefix: &str) -> Arc<JobStore> {
    Arc::new(JobStore::new(
        metadata_store.store_arc(),
        format!("{prefix}-{}", Uuid::new_v4()),
    ))
}

/// A sink that receives `Action` values produced by scrub checkers.
#[async_trait]
pub trait ActionSink: Send {
    async fn apply(&mut self, action: Action) -> Result<(), Error>;
}

/// Logs actions as dry-run without applying any mutations to storage.
pub struct DryRunSink;

#[async_trait]
impl ActionSink for DryRunSink {
    async fn apply(&mut self, action: Action) -> Result<(), Error> {
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
    /// Only the retention actions need it; integrity repairs never use it.
    registry: Option<Arc<Registry>>,
}

impl Executor {
    /// Construct an executor from its resolved fields: the `blob_store` it
    /// reads/deletes blobs through, the `metadata_store` it mutates links
    /// through, and the `job_store` replication enqueue actions are landed on.
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

    /// Wire the registry that tag deletions run through. Required by `prune`
    /// (retention deletes) and `scrub --orphan-namespaces` (dangling-tag deletes).
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

    /// Test-only constructor that synthesizes a `JobStore` and a registry
    /// over the same stores, so the retention arms work out of the box.
    #[cfg(test)]
    #[must_use]
    pub fn new_for_test(blob_store: Arc<BlobStore>, metadata_store: Arc<MetadataStore>) -> Self {
        let job_store = Arc::new(JobStore::new(metadata_store.store_arc(), "scrub-test"));
        let resolver = Arc::new(
            RepositoryResolver::new(create_test_repositories())
                .expect("test repositories must not have overlapping prefixes"),
        );
        let registry = Registry::new(
            blob_store.clone(),
            metadata_store.clone(),
            resolver,
            RegistryConfig {
                job_queue: Some(job_store.clone()),
                ..RegistryConfig::default()
            },
        )
        .expect("test registry");
        Self::new(blob_store, metadata_store, job_store).with_registry(registry)
    }

    /// Lands the envelope on the durable replication queue. A reconcile push
    /// shares the event path's `lock_key` (so it coalesces with a pending
    /// event-path push); a prune delete keys on the bare reference, so repeated
    /// runs coalesce and never merge with a timestamped event-path delete.
    async fn enqueue_replication(
        &self,
        envelope: Result<JobEnvelope, serde_json::Error>,
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

/// Records a `replication_reconcile_total` outcome (`enqueued`, `failed`, or
/// `skipped`).
pub fn record_reconcile_outcome(outcome: &str) {
    metrics_provider()
        .replication_reconcile_total
        .with_label_values(&[outcome])
        .inc();
}

impl Executor {
    /// Deletes the orphan's bytes under the `blob-data:{digest}` coarse lock
    /// (the same one manifest pushes and upload completions take), so a
    /// reference a concurrent push is granting cannot be missed and have its
    /// bytes reclaimed underneath it.
    async fn delete_orphan_blob(&self, digest: Digest) -> Result<(), Error> {
        self.metadata_store
            .with_blob_data_lock(&digest, async {
                match self.metadata_store.has_blob_references(&digest).await {
                    Err(e) => Err(Error::from(e)),
                    Ok(true) => {
                        info!("skipping orphan blob deletion: reference appeared for {digest}");
                        Ok(())
                    }
                    Ok(false) => match self.blob_store.delete_blob(&digest).await {
                        Ok(()) | Err(RegistryError::BlobUnknown | RegistryError::NotFound) => {
                            Ok(())
                        }
                        Err(e) => Err(Error::from(e)),
                    },
                }
            })
            .await
    }

    async fn remove_blob_index_link(
        &self,
        namespace: Namespace,
        blob: Digest,
        link: LinkKind,
    ) -> Result<(), Error> {
        self.metadata_store
            .update_blob_index(&namespace, &blob, BlobIndexOperation::Remove(link))
            .await?;
        Ok(())
    }

    /// Re-add a blob-index grant the index is missing for a still-referenced
    /// blob, under the `blob-data:{blob}` lock the orphan-blob reclaim also
    /// holds.
    ///
    /// Re-checks the bytes under the lock: the checker's existence gate ran
    /// before this apply, so a concurrent reclaim may have deleted the bytes in
    /// between. Granting then would resurrect a reference to a deleted blob, so a
    /// vanished blob is skipped. The insert itself is idempotent, so a concurrent
    /// push that re-granted the same link is harmless.
    async fn grant_blob_index_link(
        &self,
        namespace: Namespace,
        blob: Digest,
        link: LinkKind,
    ) -> Result<(), Error> {
        self.metadata_store
            .with_blob_data_lock(&blob, async {
                match self.blob_store.size(&blob).await {
                    Ok(_) => {}
                    Err(RegistryError::BlobUnknown | RegistryError::NotFound) => {
                        info!(
                            "skipping blob-index grant: bytes were reclaimed for '{namespace}/{blob}'"
                        );
                        return Ok(());
                    }
                    Err(e) => return Err(Error::from(e)),
                }
                self.metadata_store
                    .update_blob_index(&namespace, &blob, BlobIndexOperation::Insert(link.clone()))
                    .await?;
                Ok(())
            })
            .await
    }

    async fn remove_orphan_blob_grant(
        &self,
        namespace: Namespace,
        blob: Digest,
    ) -> Result<(), Error> {
        self.metadata_store
            .with_blob_data_lock(&blob, async {
                // Re-check under the lock: a manifest reference may have appeared
                // since the checker classified the grant as orphaned.
                let links = match self
                    .metadata_store
                    .read_blob_index_namespace(&namespace, &blob)
                    .await
                {
                    Ok(links) => links,
                    // The grant vanished since classification (a concurrent revoke
                    // or delete): nothing left to do.
                    Err(RegistryError::NotFound) => return Ok(()),
                    Err(e) => return Err(Error::from(e)),
                };
                if links.iter().any(LinkKind::is_tracked) {
                    info!(
                        "skipping orphan grant revoke: a manifest reference appeared for '{namespace}/{blob}'"
                    );
                    return Ok(());
                }
                // Revoke the grant, then reclaim the now-unreferenced manifest
                // blob-data from the blob store under the same lock.
                if self
                    .metadata_store
                    .revoke_blob_ownership(&namespace, &blob)
                    .await?
                {
                    self.blob_store.delete_blob(&blob).await?;
                }
                Ok(())
            })
            .await
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

    async fn add_referrer(
        &self,
        namespace: Namespace,
        link: LinkKind,
        target: Digest,
        referrer: Digest,
    ) -> Result<(), Error> {
        self.metadata_store
            .update_links(
                &namespace,
                &[LinkOperation::create_with_referrer(link, target, referrer)],
            )
            .await?;
        Ok(())
    }

    /// Retention tag deletion through the registry's standard delete path,
    /// so it emits `tag.delete`/`manifest.delete` events and, per its
    /// internal actor, mirrors only to `prune = true` downstreams.
    async fn delete_tag(&self, namespace: Namespace, tag: Tag) -> Result<(), Error> {
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

    async fn delete_invalid_tag(&self, namespace: Namespace, tag: String) -> Result<(), Error> {
        // An invalid tag name cannot form a typed `LinkKind::Tag`, so the
        // directory is removed by prefix rather than via a link delete.
        self.metadata_store
            .delete_tag_directory(&namespace, &tag)
            .await?;
        Ok(())
    }

    /// Reclaim a manifest namespace whose name fails `Namespace` validation: it
    /// cannot form typed links, so its repository subtree is removed by prefix.
    async fn delete_invalid_namespace(&self, name: String) -> Result<(), Error> {
        self.metadata_store
            .delete_namespace_directory(&name)
            .await?;
        Ok(())
    }

    /// Reclaim an upload-only namespace whose name fails `Namespace` validation
    /// by removing its upload subtree from the blob store.
    async fn delete_invalid_upload_namespace(&self, name: String) -> Result<(), Error> {
        self.blob_store.delete_namespace_directory(&name).await?;
        Ok(())
    }

    /// Retention orphan-manifest deletion through the registry's standard
    /// delete path, which also reclaims the manifest's blob bytes once
    /// unreferenced (a missing blob body deletes metadata-only).
    async fn delete_orphan_manifest(
        &self,
        namespace: Namespace,
        digest: Digest,
    ) -> Result<(), Error> {
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

    async fn delete_expired_upload(&self, namespace: Namespace, uuid: String) -> Result<(), Error> {
        self.blob_store.delete_upload(&namespace, &uuid).await?;
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
                &[LinkOperation::delete(LinkKind::Referrer(subject, referrer))],
            )
            .await?;
        Ok(())
    }

    async fn remove_referrer(
        &self,
        namespace: Namespace,
        link: LinkKind,
        referrer: Digest,
    ) -> Result<(), Error> {
        self.metadata_store
            .update_links(
                &namespace,
                &[LinkOperation::delete_with_referrer(link, referrer)],
            )
            .await?;
        Ok(())
    }

    async fn abort_multipart_upload(&self, key: String, upload_id: String) -> Result<(), Error> {
        self.blob_store
            .abort_orphan_multipart_upload(&blob_store::OrphanMultipartUpload { key, upload_id })
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
        let payload = ReplicationPushPayload {
            downstream,
            namespace,
            tag: Some(tag),
            digest: Some(digest.to_string()),
            kind: REPLICATION_PUSH_MANIFEST_KIND.to_string(),
            // The handler stamps source_ts from the tag's created_at at execute
            // time, so the push carries the same last-writer-wins version as the
            // event path.
            source_ts: None,
        };
        self.enqueue_replication(build_envelope(&payload)).await
    }

    async fn enqueue_replication_delete(
        &self,
        downstream: String,
        namespace: Namespace,
        tag: Tag,
    ) -> Result<(), Error> {
        // Stamp `source_ts` with the prune decision time so the receiver runs
        // last-writer-wins and preserves a downstream tag dated after this
        // decision (clock skew, or a push racing the listing). That does NOT
        // make prune active-active safe: a peer's newer tag created before this
        // run is still deleted, so `prune = true` is one-way-mirror-only.
        let payload = ReplicationPushPayload {
            downstream,
            namespace,
            tag: Some(tag),
            digest: None,
            kind: REPLICATION_DELETE_MANIFEST_KIND.to_string(),
            source_ts: Some(Utc::now().to_rfc3339()),
        };
        // The prune envelope keys on the bare reference so repeated runs
        // coalesce instead of stacking one fresh-ts job per run.
        self.enqueue_replication(build_prune_delete_envelope(&payload))
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
            // A stale key means the job was claimed-and-completed or deleted
            // concurrently; either way the orphan is gone.
            Err(JobStoreError::NotFound) => {
                debug!("{queue} job '{storage_key}' already gone; nothing to delete");
                Ok(())
            }
            Err(e) => Err(Error::JobQueue(format!(
                "failed to delete {queue} job '{storage_key}': {e}"
            ))),
        }
    }
}

#[async_trait]
impl ActionSink for Executor {
    async fn apply(&mut self, action: Action) -> Result<(), Error> {
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
            Action::RemoveOrphanBlobGrant { namespace, blob } => {
                self.remove_orphan_blob_grant(namespace, blob).await
            }
            Action::RecreateLink {
                namespace,
                link,
                target,
            } => self.recreate_link(namespace, link, target).await,
            Action::AddReferrer {
                namespace,
                link,
                target,
                referrer,
            } => self.add_referrer(namespace, link, target, referrer).await,
            Action::DeleteTag { namespace, tag } => self.delete_tag(namespace, tag).await,
            Action::DeleteInvalidTag { namespace, tag } => {
                self.delete_invalid_tag(namespace, tag).await
            }
            Action::DeleteInvalidNamespace { name } => self.delete_invalid_namespace(name).await,
            Action::DeleteInvalidUploadNamespace { name } => {
                self.delete_invalid_upload_namespace(name).await
            }
            Action::DeleteOrphanManifest { namespace, digest } => {
                self.delete_orphan_manifest(namespace, digest).await
            }
            Action::DeleteExpiredUpload { namespace, uuid } => {
                self.delete_expired_upload(namespace, uuid).await
            }
            Action::DeleteOrphanReferrer {
                namespace,
                subject,
                referrer,
            } => {
                self.delete_orphan_referrer(namespace, subject, referrer)
                    .await
            }
            Action::RemoveReferrer {
                namespace,
                link,
                referrer,
            } => self.remove_referrer(namespace, link, referrer).await,
            Action::AbortMultipartUpload { key, upload_id } => {
                self.abort_multipart_upload(key, upload_id).await
            }
            Action::EnqueueReplicationPush {
                downstream,
                namespace,
                tag,
                digest,
            } => {
                self.enqueue_replication_push(downstream, namespace, tag, digest)
                    .await
            }
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
        }
    }
}

/// Captures actions into a `Vec` without performing any I/O.
///
/// Used in tests to assert which actions a checker would produce without
/// touching any real storage backend.
#[async_trait]
impl ActionSink for Vec<Action> {
    async fn apply(&mut self, action: Action) -> Result<(), Error> {
        self.push(action);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use chrono::DateTime;

    use angos_storage::MemoryObjectStore;

    use super::*;
    use crate::{
        jobs::store::FailOutcome,
        oci::Digest,
        registry::{
            cache_job_handler::{CACHE_FETCH_BLOB_KIND, CacheFetchBlobPayload},
            metadata_store::{LinkKind, LinkOperation},
            test_utils::{build_store, for_each_backend, put_blob_direct},
        },
        replication::REPLICATION_DELETE_MANIFEST_KIND,
    };

    /// A producer `JobStore` over a private store no worker drains. Tests that
    /// assert queue depth must not share the registry store, whose in-process
    /// claim loops would otherwise claim the job and race the assertion.
    fn standalone_job_store(worker_id: &str) -> Arc<JobStore> {
        let raw = Arc::new(MemoryObjectStore::new());
        Arc::new(JobStore::new(build_store(raw), worker_id))
    }

    #[tokio::test]
    async fn executor_dry_run_does_not_delete_blob() {
        for_each_backend(async |test_case| {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            let orphan_content = b"executor dry-run test";
            let orphan_digest = put_blob_direct(metadata_store.store(), orphan_content).await;

            let mut sink = DryRunSink;
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
            let orphan_digest = put_blob_direct(metadata_store.store(), orphan_content).await;

            let mut executor = Executor::new_for_test(blob_store.clone(), metadata_store);

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
    async fn executor_delete_orphan_manifest_missing_blob_still_removes_digest_link() {
        for_each_backend(async |test_case| {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            let namespace = Namespace::new("test-repo/app").unwrap();

            // Write manifest blob and create a digest link, then delete the blob.
            let content = b"orphan manifest content for missing-blob test";
            let digest = put_blob_direct(metadata_store.store(), content).await;
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

            let mut executor = Executor::new_for_test(blob_store.clone(), metadata_store.clone());

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
            let digest = put_blob_direct(metadata_store.store(), content).await;
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

            let mut executor = Executor::new_for_test(blob_store.clone(), metadata_store.clone());

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
            let digest = put_blob_direct(metadata_store.store(), content).await;

            metadata_store
                .update_blob_index(
                    &Namespace::new("test-repo/app").unwrap(),
                    &digest,
                    BlobIndexOperation::Insert(LinkKind::Blob(digest.clone())),
                )
                .await
                .unwrap();

            let mut executor = Executor::new_for_test(blob_store.clone(), metadata_store);

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
            let digest = put_blob_direct(metadata_store.store(), b"granted layer").await;

            let mut executor = Executor::new_for_test(blob_store.clone(), metadata_store.clone());
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

            let mut executor = Executor::new_for_test(blob_store.clone(), metadata_store.clone());
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
                put_blob_direct(metadata_store.store(), b"subject for referrer exec").await;
            let referrer_digest =
                put_blob_direct(metadata_store.store(), b"referrer for referrer exec").await;

            metadata_store
                .update_links(
                    &namespace,
                    &[
                        LinkOperation::create(
                            LinkKind::Digest(subject_digest.clone()),
                            subject_digest.clone(),
                        ),
                        LinkOperation::create(
                            LinkKind::Referrer(subject_digest.clone(), referrer_digest.clone()),
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
                        &LinkKind::Referrer(subject_digest.clone(), referrer_digest.clone())
                    )
                    .await
                    .is_ok(),
                "Referrer link must exist before applying the action"
            );

            let mut executor = Executor::new_for_test(blob_store.clone(), metadata_store.clone());

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
                        &LinkKind::Referrer(subject_digest.clone(), referrer_digest.clone())
                    )
                    .await
                    .is_err(),
                "Referrer link must be removed after applying the action"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn executor_remove_referrer_cascades_to_link_delete_when_referenced_by_becomes_empty() {
        for_each_backend(async |test_case| {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            let namespace = Namespace::new("test-repo/remove-referrer-cascade").unwrap();

            // Create a layer blob and the corresponding layer link with exactly
            // one phantom referrer so referenced_by = {phantom}.
            let layer_content = b"layer content for cascade test";
            let layer_digest = put_blob_direct(metadata_store.store(), layer_content).await;
            let phantom_digest = Digest::from_str(
                "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            )
            .unwrap();

            metadata_store
                .update_links(
                    &namespace,
                    &[LinkOperation::create_with_referrer(
                        LinkKind::Layer(layer_digest.clone()),
                        layer_digest.clone(),
                        phantom_digest.clone(),
                    )],
                )
                .await
                .unwrap();

            // Confirm the layer link exists with the phantom referrer.
            let before = metadata_store
                .read_link(&namespace, &LinkKind::Layer(layer_digest.clone()))
                .await
                .unwrap();
            assert!(
                before.referenced_by.contains(&phantom_digest),
                "phantom referrer must be present before the action"
            );

            let mut executor = Executor::new_for_test(blob_store.clone(), metadata_store.clone());

            executor
                .apply(Action::RemoveReferrer {
                    namespace: namespace.clone(),
                    link: LinkKind::Layer(layer_digest.clone()),
                    referrer: phantom_digest.clone(),
                })
                .await
                .unwrap();

            // After removing the only referrer the link itself must be gone.
            assert!(
                metadata_store
                    .read_link(&namespace, &LinkKind::Layer(layer_digest.clone()))
                    .await
                    .is_err(),
                "layer link must be removed when referenced_by becomes empty"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn enqueue_replication_delete_stamps_source_ts_for_receiver_lww() {
        for_each_backend(async |test_case| {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            let job_store = standalone_job_store("scrub-source-ts");

            let mut executor = Executor::new(blob_store, metadata_store, job_store.clone());

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

            let job_store = standalone_job_store("scrub-prune-coalesce");

            let mut executor = Executor::new(blob_store, metadata_store, job_store.clone());

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
        let payload = ReplicationPushPayload {
            downstream: "removed".to_string(),
            namespace: Namespace::new("ns/app").unwrap(),
            tag: Some(Tag::new("v1").unwrap()),
            digest: None,
            kind: REPLICATION_PUSH_MANIFEST_KIND.to_string(),
            source_ts: None,
        };
        build_envelope(&payload).unwrap()
    }

    /// Builds an orphan-shaped pull-through cache-fill envelope.
    fn orphan_cache_envelope() -> JobEnvelope {
        let payload = CacheFetchBlobPayload {
            namespace: Namespace::new("ns/app").unwrap(),
            digest: "sha256:1111111111111111111111111111111111111111111111111111111111111111"
                .to_string(),
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
            let job_store = standalone_job_store("scrub-orphan");

            let mut executor = Executor::new(blob_store, metadata_store, job_store.clone());

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
            let job_store = standalone_job_store("scrub-orphan");

            let mut executor = Executor::new(blob_store, metadata_store, job_store.clone());

            for (queue, mut envelope) in [
                (Queue::Replication, orphan_push_envelope()),
                (Queue::Cache, orphan_cache_envelope()),
            ] {
                // A single-attempt job failed once dead-letters under its
                // original key.
                envelope.max_attempts = 1;
                job_store.enqueue(envelope).await.unwrap();
                let claimed = job_store
                    .claim_one(queue)
                    .await
                    .unwrap()
                    .claimed
                    .expect("the job must be claimable");
                let outcome = job_store.fail(claimed, "simulated failure").await.unwrap();
                assert!(matches!(outcome, FailOutcome::MovedToDeadLetter));

                let (failed_keys, _) = job_store.list_failed_page(queue, 10, None).await.unwrap();
                assert_eq!(failed_keys.len(), 1);

                executor
                    .apply(delete_orphan_action(
                        queue,
                        JobState::Failed,
                        failed_keys[0].clone(),
                    ))
                    .await
                    .unwrap();

                let (failed_keys, _) = job_store.list_failed_page(queue, 10, None).await.unwrap();
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
            let job_store = standalone_job_store("scrub-orphan");

            let mut executor = Executor::new(blob_store, metadata_store, job_store);

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

        let mut sink: Vec<Action> = Vec::new();
        sink.apply(Action::DeleteOrphanBlob(digest.clone()))
            .await
            .unwrap();
        sink.apply(Action::DeleteExpiredUpload {
            namespace: Namespace::new("ns").unwrap(),
            uuid: "uuid".to_string(),
        })
        .await
        .unwrap();

        assert_eq!(sink.len(), 2);
        assert!(matches!(sink[0], Action::DeleteOrphanBlob(_)));
        assert!(matches!(sink[1], Action::DeleteExpiredUpload { .. }));
    }
}
