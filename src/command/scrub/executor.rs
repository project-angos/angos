use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use tracing::{debug, info, warn};

use crate::{
    command::scrub::{action::Action, error::Error},
    metrics_provider::metrics_provider,
    oci::{Manifest, Reference},
    registry::{
        blob_store::{self, MultipartCleanup},
        job_store::{Error as JobStoreError, JobEnvelope, JobStore},
        manifest::link_plan,
        metadata_store::{
            BlobIndexOperation, Error as MetadataError, LinkOperation, MetadataStore,
            link_kind::LinkKind,
        },
    },
    replication::{
        REPLICATION_DELETE_MANIFEST_KIND, REPLICATION_PUSH_MANIFEST_KIND, ReplicationPushPayload,
        build_envelope, build_prune_delete_envelope,
    },
};

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
    blob_store: Arc<blob_store::BlobStore>,
    metadata_store: Arc<MetadataStore>,
    job_store: Arc<JobStore>,
}

impl Executor {
    /// Starts building an executor from individual resolved fields.
    #[must_use]
    pub fn builder() -> ExecutorBuilder {
        ExecutorBuilder::default()
    }

    /// Test-only constructor that synthesizes a `JobStore` so the build cannot fail.
    #[cfg(test)]
    #[must_use]
    pub fn new(blob_store: Arc<blob_store::BlobStore>, metadata_store: Arc<MetadataStore>) -> Self {
        let job_store = Arc::new(JobStore::new(metadata_store.store_arc(), "scrub-test"));
        Self::builder()
            .blob_store(blob_store)
            .metadata_store(metadata_store)
            .job_store(job_store)
            .build()
            .expect("blob_store, metadata_store and job_store provided")
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

/// Builder for [`Executor`]; all three fields are required.
#[allow(clippy::struct_field_names)]
#[derive(Default)]
pub struct ExecutorBuilder {
    blob_store: Option<Arc<blob_store::BlobStore>>,
    metadata_store: Option<Arc<MetadataStore>>,
    job_store: Option<Arc<JobStore>>,
}

impl ExecutorBuilder {
    /// Blob store the executor reads/deletes blobs through (required).
    #[must_use]
    pub fn blob_store(mut self, blob_store: Arc<blob_store::BlobStore>) -> Self {
        self.blob_store = Some(blob_store);
        self
    }

    /// Metadata store the executor mutates links through (required).
    #[must_use]
    pub fn metadata_store(mut self, metadata_store: Arc<MetadataStore>) -> Self {
        self.metadata_store = Some(metadata_store);
        self
    }

    /// Producer queue replication enqueue actions are landed on (required).
    #[must_use]
    pub fn job_store(mut self, job_store: Arc<JobStore>) -> Self {
        self.job_store = Some(job_store);
        self
    }

    /// Builds the [`Executor`].
    ///
    /// # Errors
    ///
    /// Returns [`Error::Initialization`] when a required field is missing.
    pub fn build(self) -> Result<Executor, Error> {
        let blob_store = self.blob_store.ok_or_else(|| {
            Error::Initialization("executor builder requires a blob_store".into())
        })?;
        let metadata_store = self.metadata_store.ok_or_else(|| {
            Error::Initialization("executor builder requires a metadata_store".into())
        })?;
        let job_store = self
            .job_store
            .ok_or_else(|| Error::Initialization("executor builder requires a job_store".into()))?;

        Ok(Executor {
            blob_store,
            metadata_store,
            job_store,
        })
    }
}

#[async_trait]
impl ActionSink for Executor {
    #[allow(clippy::too_many_lines)]
    async fn apply(&mut self, action: Action) -> Result<(), Error> {
        info!("{action}");

        match action {
            Action::MigrateNamespaceRegistry => {
                self.metadata_store.migrate_namespace_registry().await?;
            }
            Action::MigrateBlobIndex(digest) => {
                self.metadata_store.migrate_blob_index(&digest).await?;
            }
            Action::DeleteOrphanBlob(digest) => {
                // Hold the `blob-data:{digest}` coarse lock — the same one
                // manifest pushes and upload completions take — across the
                // reference check and the data delete, so a reference that a
                // concurrent push is granting cannot be missed and have its
                // bytes reclaimed underneath it.
                let session = self.metadata_store.acquire_blob_data_lock(&digest).await?;
                let result = match self.metadata_store.has_blob_references(&digest).await {
                    Err(e) => Err(Error::from(e)),
                    Ok(true) => {
                        info!("skipping orphan blob deletion: reference appeared for {digest}");
                        Ok(())
                    }
                    Ok(false) => match self.blob_store.delete_blob(&digest).await {
                        Ok(())
                        | Err(
                            blob_store::Error::BlobNotFound | blob_store::Error::ReferenceNotFound,
                        ) => Ok(()),
                        Err(e) => Err(Error::from(e)),
                    },
                };
                session.release().await;
                result?;
            }
            Action::RemoveBlobIndexLink {
                namespace,
                blob,
                link,
            } => {
                self.metadata_store
                    .update_blob_index(&namespace, &blob, BlobIndexOperation::Remove(link))
                    .await?;
            }
            Action::RemoveOrphanBlobGrant { namespace, blob } => {
                // Hold the `blob-data:{digest}` coarse lock across the re-check
                // and the revoke, the same one manifest pushes and upload
                // completions take, so a reference a concurrent push is granting
                // is never missed and reclaimed underneath it.
                let session = self.metadata_store.acquire_blob_data_lock(&blob).await?;
                let result = async {
                    // Re-check under the lock: a manifest reference may have
                    // appeared since the checker classified the grant as orphaned.
                    let links = match self
                        .metadata_store
                        .read_blob_index_namespace(&namespace, &blob)
                        .await
                    {
                        Ok(links) => links,
                        // The grant vanished since classification (a concurrent
                        // revoke or delete): nothing left to do.
                        Err(MetadataError::ReferenceNotFound) => return Ok(()),
                        Err(e) => return Err(Error::from(e)),
                    };
                    if links.iter().any(LinkKind::is_tracked) {
                        info!(
                            "skipping orphan grant revoke: a manifest reference appeared for '{namespace}/{blob}'"
                        );
                        return Ok(());
                    }
                    self.metadata_store
                        .revoke_blob_ownership(&namespace, &blob)
                        .await
                        .map_err(Error::from)
                }
                .await;
                session.release().await;
                result?;
            }
            Action::RecreateLink {
                namespace,
                link,
                target,
            } => {
                self.metadata_store
                    .update_links(&namespace, &[LinkOperation::create(link, target)])
                    .await?;
            }
            Action::AddReferrer {
                namespace,
                link,
                target,
                referrer,
            } => {
                self.metadata_store
                    .update_links(
                        &namespace,
                        &[LinkOperation::create_with_referrer(link, target, referrer)],
                    )
                    .await?;
            }
            Action::SetMediaType {
                namespace,
                link,
                target,
                media_type,
                ..
            } => {
                self.metadata_store
                    .update_links(
                        &namespace,
                        &[LinkOperation::create_with_media_type(
                            link,
                            target,
                            Some(media_type),
                        )],
                    )
                    .await?;
            }
            Action::DeleteTag { namespace, tag } => {
                self.metadata_store
                    .update_links(&namespace, &[LinkOperation::delete(LinkKind::Tag(tag))])
                    .await?;
            }
            Action::DeleteOrphanManifest { namespace, digest } => {
                let manifest = match self.blob_store.read(&digest).await {
                    Ok(content) => Manifest::from_slice(&content).ok(),
                    Err(blob_store::Error::BlobNotFound | blob_store::Error::ReferenceNotFound) => {
                        warn!(
                            "Manifest blob missing for {digest}, proceeding with metadata-only deletion"
                        );
                        None
                    }
                    Err(e) => return Err(Error::from(e)),
                };
                let tags = self
                    .metadata_store
                    .find_tags_pointing_at(&namespace, &digest)
                    .await?;
                let ops = link_plan::delete(&Reference::Digest(digest), manifest.as_ref(), &tags);
                self.metadata_store.update_links(&namespace, &ops).await?;
            }
            Action::DeleteExpiredUpload { namespace, uuid } => {
                self.blob_store.delete_upload(&namespace, &uuid).await?;
            }
            Action::DeleteOrphanReferrer {
                namespace,
                subject,
                referrer,
            } => {
                self.metadata_store
                    .update_links(
                        &namespace,
                        &[LinkOperation::delete(LinkKind::Referrer(subject, referrer))],
                    )
                    .await?;
            }
            Action::RemoveReferrer {
                namespace,
                link,
                referrer,
            } => {
                self.metadata_store
                    .update_links(
                        &namespace,
                        &[LinkOperation::delete_with_referrer(link, referrer)],
                    )
                    .await?;
            }
            Action::AbortMultipartUpload { key, upload_id } => {
                self.blob_store
                    .abort_orphan_multipart_upload(&blob_store::OrphanMultipartUpload {
                        key,
                        upload_id,
                    })
                    .await?;
            }
            Action::EnqueueReplicationPush {
                downstream,
                namespace,
                tag,
                digest,
            } => {
                let payload = ReplicationPushPayload {
                    downstream,
                    namespace,
                    tag: Some(tag),
                    digest: Some(digest.to_string()),
                    kind: REPLICATION_PUSH_MANIFEST_KIND.to_string(),
                    // The handler stamps source_ts from the tag's created_at at
                    // execute time, so the push carries the same last-writer-wins
                    // version as the event path.
                    source_ts: None,
                };
                self.enqueue_replication(build_envelope(&payload)).await?;
            }
            Action::EnqueueReplicationDelete {
                downstream,
                namespace,
                tag,
            } => {
                // Stamp `source_ts` with the prune decision time so the receiver
                // runs last-writer-wins and preserves a downstream tag dated after
                // this decision (clock skew, or a push racing the listing). That
                // does NOT make prune active-active safe: a peer's newer tag
                // created before this run is still deleted, so `prune = true` is
                // one-way-mirror-only.
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
                    .await?;
            }
            Action::DeleteOrphanJob {
                queue,
                state,
                storage_key,
                ..
            } => {
                match self.job_store.delete_job(queue, state, &storage_key).await {
                    Ok(()) => {}
                    // A stale key means the job was claimed-and-completed or
                    // deleted concurrently; either way the orphan is gone.
                    Err(JobStoreError::NotFound) => {
                        debug!("{queue} job '{storage_key}' already gone; nothing to delete");
                    }
                    Err(e) => {
                        return Err(Error::JobQueue(format!(
                            "failed to delete {queue} job '{storage_key}': {e}"
                        )));
                    }
                }
            }
        }

        Ok(())
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
        oci::Digest,
        registry::{
            cache_job_handler::{CACHE_FETCH_BLOB_KIND, CACHE_QUEUE, CacheFetchBlobPayload},
            job_store::{FailOutcome, JobState},
            metadata_store::{LinkOperation, link_kind::LinkKind},
            test_utils::{backends, build_store, locked_executor_over, put_blob_direct},
        },
        replication::{REPLICATION_DELETE_MANIFEST_KIND, REPLICATION_QUEUE},
    };

    /// A producer `JobStore` over a private store no worker drains. Tests that
    /// assert queue depth must not share the registry store, whose in-process
    /// claim loops would otherwise claim the job and race the assertion.
    fn standalone_job_store(worker_id: &str) -> Arc<JobStore> {
        let raw = Arc::new(MemoryObjectStore::new());
        Arc::new(JobStore::new(
            build_store(raw.clone(), locked_executor_over(raw)),
            worker_id,
        ))
    }

    #[tokio::test]
    async fn executor_dry_run_does_not_delete_blob() {
        for test_case in backends() {
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
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn executor_real_run_deletes_blob() {
        for test_case in backends() {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            let orphan_content = b"executor real-run test";
            let orphan_digest = put_blob_direct(metadata_store.store(), orphan_content).await;

            let mut executor = Executor::new(blob_store.clone(), metadata_store);

            executor
                .apply(Action::DeleteOrphanBlob(orphan_digest.clone()))
                .await
                .unwrap();

            assert!(
                blob_store.read(&orphan_digest).await.is_err(),
                "real-run must delete the blob"
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn executor_delete_orphan_manifest_missing_blob_still_removes_digest_link() {
        for test_case in backends() {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            let namespace = "test-repo/app";

            // Write manifest blob and create a digest link, then delete the blob.
            let content = b"orphan manifest content for missing-blob test";
            let digest = put_blob_direct(metadata_store.store(), content).await;
            metadata_store
                .update_links(
                    namespace,
                    &[LinkOperation::create(
                        LinkKind::Digest(digest.clone()),
                        digest.clone(),
                    )],
                )
                .await
                .unwrap();
            blob_store.delete_blob(&digest).await.unwrap();

            let mut executor = Executor::new(blob_store.clone(), metadata_store.clone());

            executor
                .apply(Action::DeleteOrphanManifest {
                    namespace: namespace.to_string(),
                    digest: digest.clone(),
                })
                .await
                .unwrap();

            assert!(
                metadata_store
                    .read_link(namespace, &LinkKind::Digest(digest.clone()), false)
                    .await
                    .is_err(),
                "digest link must be removed even when the blob is missing"
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn executor_delete_orphan_manifest_missing_blob_removes_tag_link() {
        for test_case in backends() {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            let namespace = "test-repo/app";

            let content = b"orphan manifest with tag - missing blob";
            let digest = put_blob_direct(metadata_store.store(), content).await;
            metadata_store
                .update_links(
                    namespace,
                    &[
                        LinkOperation::create(LinkKind::Digest(digest.clone()), digest.clone()),
                        LinkOperation::create(
                            LinkKind::Tag("dangling".to_string()),
                            digest.clone(),
                        ),
                    ],
                )
                .await
                .unwrap();
            blob_store.delete_blob(&digest).await.unwrap();

            let mut executor = Executor::new(blob_store.clone(), metadata_store.clone());

            executor
                .apply(Action::DeleteOrphanManifest {
                    namespace: namespace.to_string(),
                    digest: digest.clone(),
                })
                .await
                .unwrap();

            assert!(
                metadata_store
                    .read_link(namespace, &LinkKind::Tag("dangling".to_string()), false)
                    .await
                    .is_err(),
                "tag link pointing at missing-blob digest must be removed"
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn executor_delete_orphan_blob_takes_blob_data_lock() {
        for test_case in backends() {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            let content = b"blob with no ownership entry";
            let digest = put_blob_direct(metadata_store.store(), content).await;

            let mut executor = Executor::new(blob_store.clone(), metadata_store);

            executor
                .apply(Action::DeleteOrphanBlob(digest.clone()))
                .await
                .unwrap();

            assert!(
                blob_store.read(&digest).await.is_err(),
                "unreferenced blob must be deleted"
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn executor_delete_orphan_blob_skips_when_reference_appears_between_classification_and_apply()
     {
        for test_case in backends() {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            let content = b"blob that got ownership just in time";
            let digest = put_blob_direct(metadata_store.store(), content).await;

            metadata_store
                .update_blob_index(
                    "test-repo/app",
                    &digest,
                    BlobIndexOperation::Insert(LinkKind::Blob(digest.clone())),
                )
                .await
                .unwrap();

            let mut executor = Executor::new(blob_store.clone(), metadata_store);

            executor
                .apply(Action::DeleteOrphanBlob(digest.clone()))
                .await
                .unwrap();

            assert!(
                blob_store.read(&digest).await.is_ok(),
                "blob with a reference must not be deleted"
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn executor_delete_orphan_referrer_removes_referrer_link() {
        for test_case in backends() {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            let namespace = "test-repo/referrer-exec";

            let subject_digest =
                put_blob_direct(metadata_store.store(), b"subject for referrer exec").await;
            let referrer_digest =
                put_blob_direct(metadata_store.store(), b"referrer for referrer exec").await;

            metadata_store
                .update_links(
                    namespace,
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
                        namespace,
                        &LinkKind::Referrer(subject_digest.clone(), referrer_digest.clone()),
                        false,
                    )
                    .await
                    .is_ok(),
                "Referrer link must exist before applying the action"
            );

            let mut executor = Executor::new(blob_store.clone(), metadata_store.clone());

            executor
                .apply(Action::DeleteOrphanReferrer {
                    namespace: namespace.to_string(),
                    subject: subject_digest.clone(),
                    referrer: referrer_digest.clone(),
                })
                .await
                .unwrap();

            assert!(
                metadata_store
                    .read_link(
                        namespace,
                        &LinkKind::Referrer(subject_digest.clone(), referrer_digest.clone()),
                        false,
                    )
                    .await
                    .is_err(),
                "Referrer link must be removed after applying the action"
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn executor_remove_referrer_cascades_to_link_delete_when_referenced_by_becomes_empty() {
        for test_case in backends() {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            let namespace = "test-repo/remove-referrer-cascade";

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
                    namespace,
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
                .read_link(namespace, &LinkKind::Layer(layer_digest.clone()), false)
                .await
                .unwrap();
            assert!(
                before.referenced_by.contains(&phantom_digest),
                "phantom referrer must be present before the action"
            );

            let mut executor = Executor::new(blob_store.clone(), metadata_store.clone());

            executor
                .apply(Action::RemoveReferrer {
                    namespace: namespace.to_string(),
                    link: LinkKind::Layer(layer_digest.clone()),
                    referrer: phantom_digest.clone(),
                })
                .await
                .unwrap();

            // After removing the only referrer the link itself must be gone.
            assert!(
                metadata_store
                    .read_link(namespace, &LinkKind::Layer(layer_digest.clone()), false,)
                    .await
                    .is_err(),
                "layer link must be removed when referenced_by becomes empty"
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn enqueue_replication_delete_stamps_source_ts_for_receiver_lww() {
        for test_case in backends() {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            let job_store = standalone_job_store("scrub-source-ts");

            let mut executor = Executor::builder()
                .blob_store(blob_store)
                .metadata_store(metadata_store)
                .job_store(job_store.clone())
                .build()
                .unwrap();

            executor
                .apply(Action::EnqueueReplicationDelete {
                    downstream: "mirror".to_string(),
                    namespace: "ns/app".to_string(),
                    tag: "stray".to_string(),
                })
                .await
                .unwrap();

            let claimed = job_store
                .claim_one(REPLICATION_QUEUE)
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

            test_case.cleanup().await;
        }
    }

    /// Each apply stamps a fresh decision-time `source_ts`, so this pins that
    /// the prune `lock_key` excludes it: a second run against a still-failing
    /// downstream must coalesce, not stack a second job per tag.
    #[tokio::test]
    async fn prune_delete_enqueues_coalesce_while_one_is_pending() {
        for test_case in backends() {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            let job_store = standalone_job_store("scrub-prune-coalesce");

            let mut executor = Executor::builder()
                .blob_store(blob_store)
                .metadata_store(metadata_store)
                .job_store(job_store.clone())
                .build()
                .unwrap();

            for _ in 0..2 {
                executor
                    .apply(Action::EnqueueReplicationDelete {
                        downstream: "mirror".to_string(),
                        namespace: "ns/app".to_string(),
                        tag: "stray".to_string(),
                    })
                    .await
                    .unwrap();
            }

            assert_eq!(
                job_store.count_pending(REPLICATION_QUEUE, 0).await.unwrap(),
                1,
                "two prune-delete enqueues for the same (downstream, namespace, tag) \
                 must coalesce into a single pending job"
            );

            test_case.cleanup().await;
        }
    }

    /// Builds an orphan-shaped replication push envelope.
    fn orphan_push_envelope() -> JobEnvelope {
        let payload = ReplicationPushPayload {
            downstream: "removed".to_string(),
            namespace: "ns/app".to_string(),
            tag: Some("v1".to_string()),
            digest: None,
            kind: REPLICATION_PUSH_MANIFEST_KIND.to_string(),
            source_ts: None,
        };
        build_envelope(&payload).unwrap()
    }

    /// Builds an orphan-shaped pull-through cache-fill envelope.
    fn orphan_cache_envelope() -> JobEnvelope {
        let payload = CacheFetchBlobPayload {
            namespace: "ns/app".to_string(),
            digest: "sha256:1111111111111111111111111111111111111111111111111111111111111111"
                .to_string(),
        };
        JobEnvelope::new(CACHE_QUEUE, CACHE_FETCH_BLOB_KIND, "cache.ns/app", &payload).unwrap()
    }

    fn delete_orphan_action(queue: &'static str, state: JobState, storage_key: String) -> Action {
        Action::DeleteOrphanJob {
            queue,
            state,
            storage_key,
            reason: "configuration no longer resolves this job".to_string(),
        }
    }

    #[tokio::test]
    async fn executor_delete_orphan_job_removes_pending_jobs_on_both_queues() {
        for test_case in backends() {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();
            let job_store = standalone_job_store("scrub-orphan");

            let mut executor = Executor::builder()
                .blob_store(blob_store)
                .metadata_store(metadata_store)
                .job_store(job_store.clone())
                .build()
                .unwrap();

            for (queue, envelope) in [
                (REPLICATION_QUEUE, orphan_push_envelope()),
                (CACHE_QUEUE, orphan_cache_envelope()),
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
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn executor_delete_orphan_job_removes_failed_jobs_on_both_queues() {
        for test_case in backends() {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();
            let job_store = standalone_job_store("scrub-orphan");

            let mut executor = Executor::builder()
                .blob_store(blob_store)
                .metadata_store(metadata_store)
                .job_store(job_store.clone())
                .build()
                .unwrap();

            for (queue, mut envelope) in [
                (REPLICATION_QUEUE, orphan_push_envelope()),
                (CACHE_QUEUE, orphan_cache_envelope()),
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
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn executor_delete_orphan_job_tolerates_stale_key() {
        for test_case in backends() {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();
            let job_store = standalone_job_store("scrub-orphan");

            let mut executor = Executor::builder()
                .blob_store(blob_store)
                .metadata_store(metadata_store)
                .job_store(job_store)
                .build()
                .unwrap();

            for queue in [REPLICATION_QUEUE, CACHE_QUEUE] {
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
            test_case.cleanup().await;
        }
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
            namespace: "ns".to_string(),
            uuid: "uuid".to_string(),
        })
        .await
        .unwrap();

        assert_eq!(sink.len(), 2);
        assert!(matches!(sink[0], Action::DeleteOrphanBlob(_)));
        assert!(matches!(sink[1], Action::DeleteExpiredUpload { .. }));
    }
}
