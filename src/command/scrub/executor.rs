use std::{
    future::Future,
    sync::{Arc, atomic::Ordering},
};

use angos_tx_engine::StorageError;
use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::{
    command::scrub::{
        action::{Action, ActionCategory},
        error::Error,
        report::ActionTally,
    },
    metrics_provider::metrics_provider,
    oci::{Digest, Manifest, Namespace, Reference, Tag},
    registry::{
        blob_store::{self, BlobStore, MultipartCleanup},
        job_store::{Error as JobStoreError, JobEnvelope, JobState, JobStore, Queue},
        manifest::link_plan,
        metadata_store::{
            BlobIndexOperation, Error as MetadataError, LinkKind, LinkOperation, MetadataStore,
        },
        path_builder,
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

/// Per-category mutate gate over the inner sink. A closed gate logs the action
/// as skipped and counts it under [`ActionTally::suppressed`] without touching
/// the inner sink; an open gate forwards unchanged. Built only when at least
/// one gate is open (an all-closed run uses the plain [`DryRunSink`]).
pub struct GatedSink {
    inner: Box<dyn ActionSink + Send>,
    policy_mutate: bool,
    gc_mutate: bool,
    tally: Arc<ActionTally>,
}

impl GatedSink {
    #[must_use]
    pub fn new(
        inner: Box<dyn ActionSink + Send>,
        policy_mutate: bool,
        gc_mutate: bool,
        tally: Arc<ActionTally>,
    ) -> Self {
        Self {
            inner,
            policy_mutate,
            gc_mutate,
            tally,
        }
    }
}

#[async_trait]
impl ActionSink for GatedSink {
    async fn apply(&mut self, action: Action) -> Result<(), Error> {
        let open = match action.category() {
            ActionCategory::Policy => self.policy_mutate,
            ActionCategory::Gc => self.gc_mutate,
        };
        if !open {
            info!("skipped (needs --commit): would {action}");
            self.tally.suppressed.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        self.inner.apply(action).await
    }
}

/// Cancellation backstop over the inner sink: once the run token fires, every
/// apply returns [`Error::Cancelled`] without touching the inner sink.
pub struct CancelSink {
    inner: Box<dyn ActionSink + Send>,
    cancel: CancellationToken,
}

impl CancelSink {
    #[must_use]
    pub fn new(inner: Box<dyn ActionSink + Send>, cancel: CancellationToken) -> Self {
        Self { inner, cancel }
    }
}

#[async_trait]
impl ActionSink for CancelSink {
    async fn apply(&mut self, action: Action) -> Result<(), Error> {
        if self.cancel.is_cancelled() {
            return Err(Error::Cancelled);
        }
        self.inner.apply(action).await
    }
}

/// A per-task handle over the run's shared sink, locking the `Mutex` around each
/// `apply` so fanned-out tasks overlap enumeration while mutations serialize.
/// Cheap to clone.
#[derive(Clone)]
pub struct SharedSink(Arc<Mutex<Box<dyn ActionSink + Send>>>);

impl SharedSink {
    #[must_use]
    pub fn new(inner: Arc<Mutex<Box<dyn ActionSink + Send>>>) -> Self {
        Self(inner)
    }
}

#[async_trait]
impl ActionSink for SharedSink {
    async fn apply(&mut self, action: Action) -> Result<(), Error> {
        self.0.lock().await.apply(action).await
    }
}

/// Applies scrub actions against live storage backends.
#[allow(clippy::struct_field_names)]
pub struct Executor {
    blob_store: Arc<BlobStore>,
    metadata_store: Arc<MetadataStore>,
    job_store: Arc<JobStore>,
}

impl Executor {
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
        }
    }

    /// Test-only constructor that synthesizes a `JobStore`.
    #[cfg(test)]
    #[must_use]
    pub fn new_for_test(blob_store: Arc<BlobStore>, metadata_store: Arc<MetadataStore>) -> Self {
        let job_store = Arc::new(JobStore::new(metadata_store.store_arc(), "scrub-test"));
        Self::new(blob_store, metadata_store, job_store)
    }

    /// Lands the envelope on the durable replication queue.
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

/// Outcome of a locked reap apply: `Applied` mutated storage, `Kept` found the
/// precondition no longer holds under the lock and left everything in place.
/// The sweep tallies only `Applied`, so a raced keep never over-counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyOutcome {
    Applied,
    Kept,
}

/// Whether the blob-data lock session was lost (its cancellation token fired):
/// a lost session means a concurrent writer may have committed unseen, so the
/// caller keeps the object instead of applying its irreversible mutation.
fn session_lost(session_cancellation: &CancellationToken, what: &str) -> bool {
    if session_cancellation.is_cancelled() {
        warn!("blob-data lock session lost before {what}; keeping (a concurrent writer may have committed unseen)");
        return true;
    }
    false
}

/// Run `critical` under the `blob-data:{digest}` lock, owning the shared reap
/// lock lifecycle: acquire the lock, fence `cancel` before entering the critical
/// section, hand the session's own cancellation token to the closure, and
/// release on every path (including errors and the pre-cancel fence). The
/// closure body is the reap's critical section returning its `ApplyOutcome`.
async fn with_blob_data_lock<F, Fut>(
    metadata_store: &MetadataStore,
    digest: &Digest,
    cancel: &CancellationToken,
    critical: F,
) -> Result<ApplyOutcome, Error>
where
    F: FnOnce(CancellationToken) -> Fut,
    Fut: Future<Output = Result<ApplyOutcome, Error>>,
{
    let session = metadata_store.acquire_blob_data_lock(digest).await?;
    if cancel.is_cancelled() {
        session.release().await;
        return Err(Error::Cancelled);
    }
    let result = critical(session.cancellation()).await;
    session.release().await;
    result
}

/// Hard-delete an orphan blob's bytes under the `blob-data:{digest}` lock. Holds
/// the same coarse lock pushes take and re-verifies `has_blob_references` is still
/// false, so a reference that reappeared keeps the blob and a raced delete is a
/// no-op. `cancel` is fenced under the lock, and the lock session's own
/// cancellation is fenced again before the irreversible delete.
/// Free function so the sweep (no `Executor`) reuses the locked dance.
pub async fn delete_orphan_blob_locked(
    metadata_store: &MetadataStore,
    blob_store: &BlobStore,
    digest: &Digest,
    cancel: &CancellationToken,
) -> Result<ApplyOutcome, Error> {
    with_blob_data_lock(metadata_store, digest, cancel, |session_cancellation| async move {
        match metadata_store.has_blob_references(digest).await {
            Err(e) => Err(Error::from(e)),
            Ok(true) => {
                info!("skipping orphan blob deletion: reference appeared for {digest}");
                Ok(ApplyOutcome::Kept)
            }
            Ok(false) => {
                if session_lost(&session_cancellation, "the orphan blob delete") {
                    Ok(ApplyOutcome::Kept)
                } else {
                    match blob_store.delete_blob(digest).await {
                        Ok(()) => Ok(ApplyOutcome::Applied),
                        Err(
                            blob_store::Error::BlobNotFound | blob_store::Error::ReferenceNotFound,
                        ) => Ok(ApplyOutcome::Kept),
                        Err(e) => Err(Error::from(e)),
                    }
                }
            }
        }
    })
    .await
}

/// Remove a stale blob-ownership grant entry under the `blob-data:{digest}`
/// lock. The sweep classifies a grant as stale when its backing link file
/// reads `ReferenceNotFound` (tracked or non-tracked kinds alike); this
/// re-reads the link under the lock (a reappeared link keeps the grant),
/// removes the entry via `update_blob_index`, and reclaims the bytes when that
/// dropped the last reference. `cancel` is fenced under the lock, and the lock
/// session's own cancellation before the irreversible removal. Free function
/// so the sweep reuses the locked dance.
pub async fn remove_stale_grant_locked(
    metadata_store: &MetadataStore,
    blob_store: &BlobStore,
    namespace: &Namespace,
    digest: &Digest,
    link: &LinkKind,
    cancel: &CancellationToken,
) -> Result<ApplyOutcome, Error> {
    with_blob_data_lock(metadata_store, digest, cancel, |session_cancellation| async move {
        // A concurrent push may have re-established the link since classification;
        // if it is back, keep the grant.
        match metadata_store.read_link(namespace, link).await {
            Ok(_) => {
                info!(
                    "skipping stale grant removal: link reappeared for '{namespace}/{digest}': '{link}'"
                );
                return Ok(ApplyOutcome::Kept);
            }
            Err(MetadataError::ReferenceNotFound) => {}
            Err(e) => return Err(Error::from(e)),
        }
        if session_lost(&session_cancellation, "the stale grant removal") {
            return Ok(ApplyOutcome::Kept);
        }
        metadata_store
            .update_blob_index(namespace, digest, BlobIndexOperation::Remove(link.clone()))
            .await?;
        reclaim_if_unreferenced(metadata_store, blob_store, digest, &session_cancellation).await?;
        Ok(ApplyOutcome::Applied)
    })
    .await
}

/// Reclaim the blob's bytes if nothing references it any more; a raced delete
/// is tolerated. Callers hold the `blob-data:{digest}` lock and pass its
/// session cancellation, fenced again before the irreversible byte delete.
async fn reclaim_if_unreferenced(
    metadata_store: &MetadataStore,
    blob_store: &BlobStore,
    digest: &Digest,
    session_cancellation: &CancellationToken,
) -> Result<(), Error> {
    if !metadata_store.has_blob_references(digest).await? {
        if session_lost(session_cancellation, "the byte reclaim") {
            return Ok(());
        }
        match blob_store.delete_blob(digest).await {
            Ok(())
            | Err(blob_store::Error::BlobNotFound | blob_store::Error::ReferenceNotFound) => {}
            Err(e) => return Err(Error::from(e)),
        }
    }
    Ok(())
}

/// Revoke an obsolete bare `Blob` self-grant under the `blob-data:{digest}`
/// lock. The sweep classifies a self-grant as obsolete when the namespace holds
/// no link file to the digest and the grant shard is older than the maintenance
/// grace; this re-probes the shard's freshness under the lock (an upload dedup
/// or cross-repo mount re-confirms a blob by rewriting the shard alone,
/// creating no link file, so a shard rewritten since `run_epoch - grace` keeps
/// the grant) and every link kind that could reference the digest (any live
/// one keeps the grant; a live tag implies a live `Digest` revision link, so
/// the `Digest` probe covers tags transitively given the rebuild ran, and a
/// rebuild-fatal namespace never reaches this pass). It then removes the
/// self-grant entry and reclaims the bytes when that dropped the last
/// reference. `cancel` is fenced under the lock, and the lock session's own
/// cancellation before the irreversible removal.
pub async fn revoke_bare_self_grant_locked(
    metadata_store: &MetadataStore,
    blob_store: &BlobStore,
    namespace: &Namespace,
    digest: &Digest,
    run_epoch: DateTime<Utc>,
    grace: ChronoDuration,
    cancel: &CancellationToken,
) -> Result<ApplyOutcome, Error> {
    with_blob_data_lock(metadata_store, digest, cancel, |session_cancellation| async move {
        if grant_shard_fresh(metadata_store, namespace, digest, run_epoch, grace).await? {
            info!(
                "skipping bare self-grant revocation: the grant shard was rewritten since \
                 classification for '{namespace}/{digest}' (a concurrent upload re-confirmed the \
                 blob)"
            );
            return Ok(ApplyOutcome::Kept);
        }
        for link in [
            LinkKind::Blob(digest.clone()),
            LinkKind::Layer(digest.clone()),
            LinkKind::Config(digest.clone()),
            LinkKind::Digest(digest.clone()),
        ] {
            match metadata_store.read_link(namespace, &link).await {
                Ok(_) => {
                    info!(
                        "skipping bare self-grant revocation: link appeared for \
                         '{namespace}/{digest}': '{link}'"
                    );
                    return Ok(ApplyOutcome::Kept);
                }
                Err(MetadataError::ReferenceNotFound) => {}
                Err(e) => return Err(Error::from(e)),
            }
        }
        if session_lost(&session_cancellation, "the bare self-grant revocation") {
            return Ok(ApplyOutcome::Kept);
        }
        metadata_store
            .update_blob_index(
                namespace,
                digest,
                BlobIndexOperation::Remove(LinkKind::Blob(digest.clone())),
            )
            .await?;
        reclaim_if_unreferenced(metadata_store, blob_store, digest, &session_cancellation).await?;
        Ok(ApplyOutcome::Applied)
    })
    .await
}

/// Whether the grant shard (or its legacy `index.json` fallback) is fresher
/// than the classification window: a `last_modified` younger than
/// `run_epoch - grace` means a writer re-granted since the sweep classified,
/// so the caller keeps the grant. An unknown `last_modified` keeps too (the
/// freshness cannot be established); an absent shard does not (nothing left to
/// keep), and a failed probe surfaces as an error (never reap on a failed
/// read).
async fn grant_shard_fresh(
    metadata_store: &MetadataStore,
    namespace: &Namespace,
    digest: &Digest,
    run_epoch: DateTime<Utc>,
    grace: ChronoDuration,
) -> Result<bool, Error> {
    for key in [
        path_builder::blob_index_shard_path(digest, namespace),
        path_builder::blob_index_path(digest),
    ] {
        match metadata_store.store().head(&key).await {
            Ok(meta) => {
                return Ok(match meta.last_modified {
                    Some(modified) => run_epoch.signed_duration_since(modified) < grace,
                    None => true,
                });
            }
            Err(StorageError::NotFound) => {}
            Err(e) => return Err(Error::from(MetadataError::from(e))),
        }
    }
    Ok(false)
}

/// Remove an orphan derived metadata link under the `blob-data:{referrer}` lock.
/// The sweep classifies a derived link as orphaned when the referrer's `Digest`
/// revision link reads `NotFound`; this re-reads that link under the lock (a
/// reappeared revision keeps the link) and otherwise applies `op`. `referrer` is
/// the manifest whose revision gates the decision; `cancel` is fenced under the
/// lock, and the lock session's own cancellation before the irreversible
/// removal. Only the `Digest` revision link is re-probed: a `Digest` link lost
/// out-of-band mid-run (after the namespace's rebuild, before this apply) is
/// the accepted-risk boundary of hard deletion. Free function so the sweep
/// reuses the locked dance.
pub async fn remove_orphan_link_locked(
    metadata_store: &MetadataStore,
    namespace: &Namespace,
    referrer: &Digest,
    op: LinkOperation,
    cancel: &CancellationToken,
) -> Result<ApplyOutcome, Error> {
    with_blob_data_lock(metadata_store, referrer, cancel, |session_cancellation| async move {
        // A concurrent push may have re-established the referrer's revision since
        // classification; if it is back, keep the link.
        match metadata_store
            .read_link(namespace, &LinkKind::Digest(referrer.clone()))
            .await
        {
            Ok(_) => {
                info!(
                    "skipping orphan link removal: revision reappeared for '{namespace}/{referrer}'"
                );
                return Ok(ApplyOutcome::Kept);
            }
            Err(MetadataError::ReferenceNotFound) => {}
            Err(e) => return Err(Error::from(e)),
        }
        if session_lost(&session_cancellation, "the orphan link removal") {
            return Ok(ApplyOutcome::Kept);
        }
        metadata_store.update_links(namespace, &[op]).await?;
        Ok(ApplyOutcome::Applied)
    })
    .await
}

/// Remove a dangling revision whose manifest body is gone, under the
/// `blob-data:{target}` lock. Re-checks under the lock that the body is still
/// absent (a rewrite keeps the revision), then cascades the `Digest` self-link
/// and every tag pointing at the target. A tag-only entry (self-link already
/// gone, dangling tags remaining) still deletes its tags so state converges;
/// with neither self-link nor tags there is nothing to do (`Kept`). The cascade
/// passes `None` for the manifest, so forward back-links are left, and every
/// tag delete is conditioned on the target (a tag concurrently re-pointed at a
/// new healthy manifest survives; that push serializes on a different
/// blob-data key, so only the conditioned delete protects it). `cancel` is
/// fenced under the lock, and the lock session's own cancellation before the
/// irreversible cascade. Free function so the sweep reuses the locked dance.
pub async fn orphan_missing_body_locked(
    metadata_store: &MetadataStore,
    blob_store: &BlobStore,
    namespace: &Namespace,
    target: &Digest,
    cancel: &CancellationToken,
) -> Result<ApplyOutcome, Error> {
    with_blob_data_lock(metadata_store, target, cancel, |session_cancellation| async move {
        // A concurrent push may have re-written the body since classification; if
        // it is back, keep the revision.
        match blob_store.size(target).await {
            Ok(_) => {
                info!("skipping missing-body removal: body reappeared for '{namespace}/{target}'");
                return Ok(ApplyOutcome::Kept);
            }
            Err(blob_store::Error::BlobNotFound | blob_store::Error::ReferenceNotFound) => {}
            Err(e) => return Err(Error::from(e)),
        }
        let self_link_present = match metadata_store
            .read_link(namespace, &LinkKind::Digest(target.clone()))
            .await
        {
            Ok(_) => true,
            Err(MetadataError::ReferenceNotFound) => false,
            Err(e) => return Err(Error::from(e)),
        };
        let tags = metadata_store
            .find_tags_pointing_at(namespace, target)
            .await?;
        if !self_link_present && tags.is_empty() {
            return Ok(ApplyOutcome::Kept);
        }
        if session_lost(&session_cancellation, "the missing-body cascade") {
            return Ok(ApplyOutcome::Kept);
        }
        // The delete of an absent self-link is a no-op, so a tag-only entry
        // removes exactly its dangling tags.
        let ops = link_plan::delete(&Reference::Digest(target.clone()), None, &tags);
        metadata_store.update_links(namespace, &ops).await?;
        Ok(ApplyOutcome::Applied)
    })
    .await
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
    async fn delete_tag(&self, namespace: Namespace, tag: Tag) -> Result<(), Error> {
        self.metadata_store
            .update_links(&namespace, &[LinkOperation::delete(LinkKind::Tag(tag))])
            .await?;
        Ok(())
    }

    async fn delete_invalid_tag(&self, namespace: Namespace, tag: String) -> Result<(), Error> {
        // An invalid tag name cannot form a typed `LinkKind::Tag`, so remove by
        // prefix.
        self.metadata_store
            .delete_tag_directory(&namespace, &tag)
            .await?;
        Ok(())
    }

    /// Reclaim a manifest namespace whose name fails `Namespace` validation by
    /// removing its repository subtree by prefix. Unlike the de-configured
    /// sweep (marker-subtrees only), the recursive prefix delete is safe here:
    /// an invalid path segment makes every nested child namespace invalid too,
    /// so no valid namespace can live inside the deleted prefix.
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

    async fn delete_orphan_manifest(
        &self,
        namespace: Namespace,
        digest: Digest,
    ) -> Result<(), Error> {
        let manifest = match self.blob_store.read(&digest).await {
            Ok(content) => Manifest::from_slice(&content).ok(),
            Err(blob_store::Error::BlobNotFound | blob_store::Error::ReferenceNotFound) => {
                warn!("Manifest blob missing for {digest}, proceeding with metadata-only deletion");
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
        Ok(())
    }

    async fn delete_expired_upload(&self, namespace: Namespace, uuid: String) -> Result<(), Error> {
        self.blob_store.delete_upload(&namespace, &uuid).await?;
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
            // time, matching the event path's last-writer-wins version.
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
        // Stamp `source_ts` with the decision time so the receiver's
        // last-writer-wins preserves a downstream tag dated after it. This does
        // not make prune active-active safe (a peer's newer tag created before
        // this run is still deleted); `prune = true` is one-way-mirror-only.
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

/// Captures actions into a `Vec` without I/O, for tests asserting which actions a
/// checker produces.
#[cfg(test)]
#[async_trait]
impl ActionSink for Vec<Action> {
    async fn apply(&mut self, action: Action) -> Result<(), Error> {
        self.push(action);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;

    use angos_storage::MemoryObjectStore;

    use super::*;
    use crate::{
        command::scrub::report::counting_sink,
        registry::{
            cache_job_handler::{CACHE_FETCH_BLOB_KIND, CacheFetchBlobPayload},
            job_store::{FailOutcome, JobState, Queue},
            metadata_store::{LinkKind, LinkOperation},
            test_utils::{backends, build_store, locked_executor_over, put_blob_direct},
        },
        replication::REPLICATION_DELETE_MANIFEST_KIND,
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
    async fn delete_orphan_blob_locked_deletes_unreferenced_bytes() {
        for test_case in backends() {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            let orphan_content = b"orphan-blob locked-delete test";
            let orphan_digest = put_blob_direct(metadata_store.store(), orphan_content).await;

            delete_orphan_blob_locked(
                &metadata_store,
                &blob_store,
                &orphan_digest,
                &CancellationToken::new(),
            )
            .await
            .unwrap();

            assert!(
                blob_store.read(&orphan_digest).await.is_err(),
                "the locked orphan-blob delete must remove unreferenced bytes"
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn executor_delete_orphan_manifest_missing_blob_still_removes_digest_link() {
        for test_case in backends() {
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
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn executor_delete_orphan_manifest_missing_blob_removes_tag_link() {
        for test_case in backends() {
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
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn delete_orphan_blob_locked_skips_when_reference_appears_between_classification_and_apply()
     {
        for test_case in backends() {
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

            delete_orphan_blob_locked(
                &metadata_store,
                &blob_store,
                &digest,
                &CancellationToken::new(),
            )
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
    async fn remove_orphan_link_locked_skips_when_revision_reappears_between_classification_and_apply()
     {
        for test_case in backends() {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            let namespace = Namespace::new("test-repo/referrer-race").unwrap();
            let subject =
                put_blob_direct(metadata_store.store(), b"subject for referrer race").await;
            let referrer =
                put_blob_direct(metadata_store.store(), b"referrer for referrer race").await;

            // Seed the subject revision and the referrer link, plus the
            // referrer's OWN revision link, simulating a concurrent push that
            // re-established the referrer between classification and apply.
            metadata_store
                .update_links(
                    &namespace,
                    &[
                        LinkOperation::create(LinkKind::Digest(subject.clone()), subject.clone()),
                        LinkOperation::create(LinkKind::Digest(referrer.clone()), referrer.clone()),
                        LinkOperation::create(
                            LinkKind::Referrer(subject.clone(), referrer.clone()),
                            referrer.clone(),
                        ),
                    ],
                )
                .await
                .unwrap();

            // The sweep would have classified the referrer link as orphaned (the
            // revision was gone at classify time), but the revision is back now.
            remove_orphan_link_locked(
                &metadata_store,
                &namespace,
                &referrer,
                LinkOperation::delete(LinkKind::Referrer(subject.clone(), referrer.clone())),
                &CancellationToken::new(),
            )
            .await
            .unwrap();

            assert!(
                metadata_store
                    .read_link(
                        &namespace,
                        &LinkKind::Referrer(subject.clone(), referrer.clone())
                    )
                    .await
                    .is_ok(),
                "the referrer link must be kept when its revision reappeared under the lock"
            );

            let _ = blob_store;
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn orphan_missing_body_locked_keeps_revision_when_body_reappears() {
        for test_case in backends() {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            let namespace = Namespace::new("test-repo/missing-body-race").unwrap();
            // The body is PRESENT (a concurrent push re-wrote it between the
            // sweep's classify and this apply), and the revision self-link + a tag
            // point at it. The under-lock body re-check must keep the revision.
            let target = put_blob_direct(metadata_store.store(), b"manifest body present").await;
            metadata_store
                .update_links(
                    &namespace,
                    &[
                        LinkOperation::create(LinkKind::Digest(target.clone()), target.clone()),
                        LinkOperation::create(
                            LinkKind::Tag(Tag::new("latest").unwrap()),
                            target.clone(),
                        ),
                    ],
                )
                .await
                .unwrap();

            orphan_missing_body_locked(
                &metadata_store,
                &blob_store,
                &namespace,
                &target,
                &CancellationToken::new(),
            )
            .await
            .unwrap();

            assert!(
                metadata_store
                    .read_link(&namespace, &LinkKind::Digest(target.clone()))
                    .await
                    .is_ok(),
                "the revision must be kept when its body reappeared under the lock"
            );
            assert!(
                metadata_store
                    .read_link(&namespace, &LinkKind::Tag(Tag::new("latest").unwrap()))
                    .await
                    .is_ok(),
                "the tag must be kept when the body reappeared under the lock"
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn orphan_missing_body_locked_removes_revision_and_tags_when_body_absent() {
        for test_case in backends() {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            let namespace = Namespace::new("test-repo/missing-body-absent").unwrap();
            let target = put_blob_direct(metadata_store.store(), b"manifest body to delete").await;
            metadata_store
                .update_links(
                    &namespace,
                    &[
                        LinkOperation::create(LinkKind::Digest(target.clone()), target.clone()),
                        LinkOperation::create(
                            LinkKind::Tag(Tag::new("latest").unwrap()),
                            target.clone(),
                        ),
                        LinkOperation::create(
                            LinkKind::Tag(Tag::new("v1").unwrap()),
                            target.clone(),
                        ),
                    ],
                )
                .await
                .unwrap();

            // The body is gone; both tags and the self-link must cascade away.
            blob_store.delete_blob(&target).await.unwrap();

            orphan_missing_body_locked(
                &metadata_store,
                &blob_store,
                &namespace,
                &target,
                &CancellationToken::new(),
            )
            .await
            .unwrap();

            assert!(
                metadata_store
                    .read_link(&namespace, &LinkKind::Digest(target.clone()))
                    .await
                    .is_err(),
                "the dangling revision self-link must be removed"
            );
            for tag in ["latest", "v1"] {
                assert!(
                    metadata_store
                        .read_link(&namespace, &LinkKind::Tag(Tag::new(tag).unwrap()))
                        .await
                        .is_err(),
                    "the tag '{tag}' pointing at the missing-body digest must be removed"
                );
            }
            test_case.cleanup().await;
        }
    }

    /// A tag-only dangling entry (self-link already gone, tags remaining, body
    /// absent) deletes its tags on the first apply and reports `Kept` on the
    /// second, so the sweep's missing-body tally converges instead of recounting.
    #[tokio::test]
    async fn orphan_missing_body_locked_removes_tag_only_dangling_entry_then_kept() {
        for test_case in backends() {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            let namespace = Namespace::new("test-repo/tag-only").unwrap();
            let target = put_blob_direct(metadata_store.store(), b"body for tag-only entry").await;
            metadata_store
                .update_links(
                    &namespace,
                    &[LinkOperation::create(
                        LinkKind::Tag(Tag::new("dangling").unwrap()),
                        target.clone(),
                    )],
                )
                .await
                .unwrap();
            blob_store.delete_blob(&target).await.unwrap();

            let outcome = orphan_missing_body_locked(
                &metadata_store,
                &blob_store,
                &namespace,
                &target,
                &CancellationToken::new(),
            )
            .await
            .unwrap();
            assert_eq!(
                outcome,
                ApplyOutcome::Applied,
                "the first apply must delete the dangling tag"
            );
            assert!(
                metadata_store
                    .read_link(&namespace, &LinkKind::Tag(Tag::new("dangling").unwrap()))
                    .await
                    .is_err(),
                "the dangling tag must be gone"
            );

            let outcome = orphan_missing_body_locked(
                &metadata_store,
                &blob_store,
                &namespace,
                &target,
                &CancellationToken::new(),
            )
            .await
            .unwrap();
            assert_eq!(
                outcome,
                ApplyOutcome::Kept,
                "with neither self-link nor tags the second apply must be a keep"
            );
            test_case.cleanup().await;
        }
    }

    /// An obsolete bare self-grant (no link file references the digest) is
    /// revoked and, being the last reference, its bytes are reclaimed.
    #[tokio::test]
    async fn revoke_bare_self_grant_locked_revokes_and_reclaims() {
        for test_case in backends() {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            let namespace = Namespace::new("test-repo/bare-grant").unwrap();
            let digest = put_blob_direct(metadata_store.store(), b"bare self-grant bytes").await;
            metadata_store
                .update_blob_index(
                    &namespace,
                    &digest,
                    BlobIndexOperation::Insert(LinkKind::Blob(digest.clone())),
                )
                .await
                .unwrap();

            let outcome = revoke_bare_self_grant_locked(
                &metadata_store,
                &blob_store,
                &namespace,
                &digest,
                // A future run epoch absorbs backend mtime granularity, so the
                // just-written shard reads as older than the classification
                // window and the revoke proceeds.
                Utc::now() + ChronoDuration::minutes(5),
                ChronoDuration::zero(),
                &CancellationToken::new(),
            )
            .await
            .unwrap();

            assert_eq!(outcome, ApplyOutcome::Applied);
            assert!(
                metadata_store
                    .read_blob_index_namespace(&namespace, &digest)
                    .await
                    .is_err(),
                "the bare self-grant must be revoked"
            );
            assert!(
                blob_store.read(&digest).await.is_err(),
                "dropping the last reference must reclaim the bytes"
            );
            test_case.cleanup().await;
        }
    }

    /// The under-lock re-probe keeps a self-grant whose digest gained a link
    /// between classification and apply (any live link keeps grant and bytes).
    #[tokio::test]
    async fn revoke_bare_self_grant_locked_keeps_when_link_appears() {
        for test_case in backends() {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            let namespace = Namespace::new("test-repo/bare-grant-race").unwrap();
            let digest = put_blob_direct(metadata_store.store(), b"self-grant gained a link").await;
            metadata_store
                .update_blob_index(
                    &namespace,
                    &digest,
                    BlobIndexOperation::Insert(LinkKind::Blob(digest.clone())),
                )
                .await
                .unwrap();
            // A concurrent manifest push created a layer link since classification.
            metadata_store
                .update_links(
                    &namespace,
                    &[LinkOperation::create(
                        LinkKind::Layer(digest.clone()),
                        digest.clone(),
                    )],
                )
                .await
                .unwrap();

            let outcome = revoke_bare_self_grant_locked(
                &metadata_store,
                &blob_store,
                &namespace,
                &digest,
                Utc::now() + ChronoDuration::minutes(5),
                ChronoDuration::zero(),
                &CancellationToken::new(),
            )
            .await
            .unwrap();

            assert_eq!(outcome, ApplyOutcome::Kept);
            assert!(
                metadata_store
                    .read_blob_index_namespace(&namespace, &digest)
                    .await
                    .is_ok(),
                "a self-grant whose digest is linked must be kept"
            );
            assert!(blob_store.read(&digest).await.is_ok(), "bytes must survive");
            test_case.cleanup().await;
        }
    }

    /// The under-lock shard freshness re-probe keeps a self-grant re-granted
    /// since classification: an upload dedup or cross-repo mount re-confirms a
    /// blob by rewriting the grant shard alone (no link file), so the
    /// link-file re-probe is blind to it and only the shard age catches it.
    #[tokio::test]
    async fn revoke_bare_self_grant_locked_keeps_when_shard_regranted_under_lock() {
        for test_case in backends() {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            let namespace = Namespace::new("test-repo/regrant-race").unwrap();
            let digest =
                put_blob_direct(metadata_store.store(), b"self-grant re-granted by dedupe").await;
            // The shard rewrite a HEAD/POST dedupe performs: the same bare
            // self-grant insert, bumping the shard's last_modified, with no
            // link file created.
            metadata_store
                .update_blob_index(
                    &namespace,
                    &digest,
                    BlobIndexOperation::Insert(LinkKind::Blob(digest.clone())),
                )
                .await
                .unwrap();

            // The sweep classified the grant against a run epoch BEFORE the
            // rewrite, so the shard now reads fresher than the classification
            // window and the revoke must keep it.
            let outcome = revoke_bare_self_grant_locked(
                &metadata_store,
                &blob_store,
                &namespace,
                &digest,
                Utc::now() - ChronoDuration::minutes(5),
                ChronoDuration::zero(),
                &CancellationToken::new(),
            )
            .await
            .unwrap();

            assert_eq!(
                outcome,
                ApplyOutcome::Kept,
                "a self-grant whose shard was rewritten since classification must be kept"
            );
            assert!(
                metadata_store
                    .read_blob_index_namespace(&namespace, &digest)
                    .await
                    .is_ok(),
                "the re-granted self-grant must survive the reap that classified it stale"
            );
            assert!(
                blob_store.read(&digest).await.is_ok(),
                "the re-confirmed blob's bytes must survive"
            );
            test_case.cleanup().await;
        }
    }

    /// The conditioned tag cascade keeps a tag concurrently re-pointed at a
    /// new healthy manifest: the delete ops are planned against the old
    /// target, so the re-pointed tag survives while the stale self-link
    /// cascade still applies.
    #[tokio::test]
    async fn missing_body_tag_cascade_keeps_repointed_tag() {
        for test_case in backends() {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            let namespace = Namespace::new("test-repo/repointed-tag").unwrap();
            let old_target = put_blob_direct(metadata_store.store(), b"old missing body").await;
            let new_target = put_blob_direct(metadata_store.store(), b"new healthy body").await;
            let tag = LinkKind::Tag(Tag::new("latest").unwrap());
            metadata_store
                .update_links(
                    &namespace,
                    &[
                        LinkOperation::create(
                            LinkKind::Digest(old_target.clone()),
                            old_target.clone(),
                        ),
                        LinkOperation::create(tag.clone(), old_target.clone()),
                    ],
                )
                .await
                .unwrap();

            // The cascade was planned while the tag pointed at old_target...
            let ops = link_plan::delete(
                &Reference::Digest(old_target.clone()),
                None,
                std::slice::from_ref(&tag),
            );
            // ...then a concurrent push re-pointed the tag at a new healthy
            // manifest (serialized on blob-data:{new_target}, a different key).
            metadata_store
                .update_links(
                    &namespace,
                    &[
                        LinkOperation::create(
                            LinkKind::Digest(new_target.clone()),
                            new_target.clone(),
                        ),
                        LinkOperation::create(tag.clone(), new_target.clone()),
                    ],
                )
                .await
                .unwrap();

            metadata_store.update_links(&namespace, &ops).await.unwrap();

            let survived = metadata_store.read_link(&namespace, &tag).await.unwrap();
            assert_eq!(
                survived.target, new_target,
                "a tag re-pointed at a new healthy manifest must survive the cascade"
            );
            assert!(
                metadata_store
                    .read_link(&namespace, &LinkKind::Digest(old_target.clone()))
                    .await
                    .is_err(),
                "the stale revision self-link must still be removed"
            );

            let _ = blob_store;
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn enqueue_replication_delete_stamps_source_ts_for_receiver_lww() {
        for test_case in backends() {
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

            test_case.cleanup().await;
        }
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
        for test_case in backends() {
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
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn executor_delete_orphan_job_removes_failed_jobs_on_both_queues() {
        for test_case in backends() {
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
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn executor_delete_orphan_job_tolerates_stale_key() {
        for test_case in backends() {
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
            test_case.cleanup().await;
        }
    }

    fn policy_action() -> Action {
        Action::DeleteTag {
            namespace: Namespace::new("ns").unwrap(),
            tag: Tag::new("v1").unwrap(),
        }
    }

    fn gc_action() -> Action {
        Action::DeleteInvalidTag {
            namespace: Namespace::new("ns").unwrap(),
            tag: "-bad".to_string(),
        }
    }

    /// `GatedSink` forwards an action iff its category's gate is open; a closed
    /// gate suppresses without touching the inner sink and counts it.
    #[tokio::test]
    async fn gated_sink_forwards_per_category() {
        for (policy_mutate, gc_mutate) in [(true, false), (false, true), (true, true)] {
            let tally = Arc::new(ActionTally::default());
            let inner: Box<dyn ActionSink + Send> = Box::new(Vec::<Action>::new());
            let mut sink = GatedSink::new(inner, policy_mutate, gc_mutate, tally.clone());

            sink.apply(policy_action()).await.unwrap();
            sink.apply(gc_action()).await.unwrap();

            let expected_suppressed = u64::from(!policy_mutate) + u64::from(!gc_mutate);
            assert_eq!(
                tally.suppressed.load(Ordering::Relaxed),
                expected_suppressed,
                "gates ({policy_mutate}, {gc_mutate}) must suppress exactly the closed categories"
            );
        }
    }

    /// A pre-cancelled `CancelSink` rejects every apply without touching the
    /// inner sink, and an open token forwards.
    #[tokio::test]
    async fn cancel_sink_rejects_after_cancellation() {
        let tally = Arc::new(ActionTally::default());
        let inner = counting_sink(Box::new(Vec::<Action>::new()), tally.clone());
        let cancel = CancellationToken::new();
        let mut sink = CancelSink::new(inner, cancel.clone());

        sink.apply(policy_action()).await.unwrap();
        assert_eq!(tally.total.load(Ordering::Relaxed), 1);

        cancel.cancel();
        let result = sink.apply(gc_action()).await;
        assert!(
            matches!(result, Err(Error::Cancelled)),
            "a cancelled sink must reject the apply"
        );
        assert_eq!(
            tally.total.load(Ordering::Relaxed),
            1,
            "the rejected apply must never reach the inner sink"
        );
    }

    /// The reap helpers fence the cancellation under the blob-data lock: a
    /// pre-cancelled token keeps the orphan bytes.
    #[tokio::test]
    async fn delete_orphan_blob_locked_fences_cancellation() {
        for test_case in backends() {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            let digest =
                put_blob_direct(metadata_store.store(), b"orphan kept by the cancel fence").await;

            let cancel = CancellationToken::new();
            cancel.cancel();
            let result =
                delete_orphan_blob_locked(&metadata_store, &blob_store, &digest, &cancel).await;
            assert!(matches!(result, Err(Error::Cancelled)));
            assert!(
                blob_store.read(&digest).await.is_ok(),
                "the cancel fence must keep the bytes"
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn vec_sink_captures_actions_without_io() {
        let mut sink: Vec<Action> = Vec::new();
        sink.apply(Action::DeleteTag {
            namespace: Namespace::new("ns").unwrap(),
            tag: Tag::new("v1").unwrap(),
        })
        .await
        .unwrap();
        sink.apply(Action::DeleteExpiredUpload {
            namespace: Namespace::new("ns").unwrap(),
            uuid: "uuid".to_string(),
        })
        .await
        .unwrap();

        assert_eq!(sink.len(), 2);
        assert!(matches!(sink[0], Action::DeleteTag { .. }));
        assert!(matches!(sink[1], Action::DeleteExpiredUpload { .. }));
    }
}
