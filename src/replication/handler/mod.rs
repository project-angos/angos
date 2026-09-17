//! [`ReplicationJobHandler`]: the [`JobHandler`] that drives the replication
//! push pipeline, kept idempotent by HEAD-before-PUT under the at-least-once
//! queue contract.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::debug;

use angos_oci::{Digest, Namespace, Reference, Tag};
use angos_oci_client::Error as RegistryClientError;

use crate::{
    jobs::Queue,
    jobs::store::{Error, JobEnvelope, JobHandler},
    metrics_provider::metrics_provider,
    registry::{
        Error as MetadataStoreError,
        blob_store::BlobStore,
        metadata_store::{LinkKind, MetadataStore},
        repository_resolver::RepositoryResolver,
    },
    replication::{
        Error as ReplicationError, REPLICATION_DELETE_MANIFEST_KIND,
        REPLICATION_PUSH_MANIFEST_KIND, ReplicationDownstream,
        pipeline::{self, PushContext, PushOutcome},
    },
};

/// Maps a replication error to a job error, dead-lettering a downstream
/// authorization denial and an invalid manifest body instead of retrying an
/// outcome that cannot change.
fn job_error(error: ReplicationError) -> Error {
    match error {
        ReplicationError::Client(RegistryClientError::Denied(msg))
        | ReplicationError::InvalidManifest(msg) => Error::Terminal(msg),
        other => Error::Execution(other.to_string()),
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

/// What a replication job acts on, shared by both operations. The handler
/// re-resolves the current local `tag -> digest` at execute time and the queue
/// only coalesces not-yet-claimed jobs, so a write landing mid-push enqueues its
/// own job instead of being dropped.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicationTarget {
    /// Local identifier of the target downstream (selects the `RegistryClient`).
    pub downstream: String,
    pub namespace: Namespace,
    /// Tag bound to the digest, when the change is tag-scoped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag: Option<Tag>,
    /// Informational for a tag-scoped job, authoritative for a tag-less one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub digest: Option<Digest>,
    /// Event instant carried for receiver-side last-writer-wins.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_ts: Option<DateTime<Utc>>,
}

/// JSON payload for a replication job on [`Queue::Replication`]. The operation
/// is the serde discriminant, serialized as an internal `kind` tag beside the
/// flat target fields, which is the layout the durable queue stores.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind")]
pub enum ReplicationJob {
    #[serde(rename = "replication.push_manifest")]
    Push {
        #[serde(flatten)]
        target: ReplicationTarget,
    },
    #[serde(rename = "replication.delete_manifest")]
    Delete {
        #[serde(flatten)]
        target: ReplicationTarget,
        /// Subject digest of a referrer manifest, captured before the delete so
        /// a retry can still prune the downstream fallback index.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        subject: Option<Digest>,
    },
}

impl ReplicationJob {
    /// The envelope kind this job is queued under.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            ReplicationJob::Push { .. } => REPLICATION_PUSH_MANIFEST_KIND,
            ReplicationJob::Delete { .. } => REPLICATION_DELETE_MANIFEST_KIND,
        }
    }

    #[must_use]
    pub fn target(&self) -> &ReplicationTarget {
        match self {
            ReplicationJob::Push { target } | ReplicationJob::Delete { target, .. } => target,
        }
    }
}

/// Tag-or-digest segment shared by every replication `lock_key`.
fn lock_key_reference(target: &ReplicationTarget) -> String {
    target
        .tag
        .as_ref()
        .map(ToString::to_string)
        .or_else(|| target.digest.as_ref().map(ToString::to_string))
        .unwrap_or_default()
}

/// Base delete `lock_key`,
/// `{Queue::Replication}.delete.{downstream}:{namespace}:{tag_or_digest}`: the
/// event-path delete appends `@{source_ts}` to it, the scrub prune delete keys
/// on it bare.
fn delete_lock_key_base(target: &ReplicationTarget) -> String {
    format!(
        "{}.delete.{}:{}:{}",
        Queue::Replication,
        target.downstream,
        target.namespace,
        lock_key_reference(target)
    )
}

/// Builds the per-job `lock_key`,
/// `{Queue::Replication}.{op}.{downstream}:{namespace}:{tag_or_digest}` plus
/// `@{source_ts}` for deletes, so identical pending work coalesces. The `op`
/// segment keeps a delete from coalescing into a still-pending push, and the
/// delete-only timestamp keeps distinct deletion events apart because a delete
/// cannot re-derive its last-writer-wins timestamp at execute time, while a
/// push re-resolves both digest and timestamp.
#[must_use]
fn replication_lock_key(job: &ReplicationJob) -> String {
    let target = job.target();
    match job {
        ReplicationJob::Delete { .. } => format!(
            "{}@{}",
            delete_lock_key_base(target),
            target
                .source_ts
                .map(|ts| ts.to_rfc3339())
                .unwrap_or_default()
        ),
        ReplicationJob::Push { .. } => format!(
            "{}.push.{}:{}:{}",
            Queue::Replication,
            target.downstream,
            target.namespace,
            lock_key_reference(target)
        ),
    }
}

/// Builds a [`JobEnvelope`] from a [`ReplicationJob`], so the scrub checker and
/// the event path enqueue the same envelope shape.
///
/// # Errors
///
/// Returns an [`Error`] when the lock key is invalid or the payload cannot be
/// serialized.
pub fn build_envelope(job: &ReplicationJob) -> Result<JobEnvelope, Error> {
    JobEnvelope::new(
        Queue::Replication,
        job.kind(),
        replication_lock_key(job),
        job,
    )
}

/// Builds a [`JobEnvelope`] for a scrub prune delete, whose `lock_key` omits
/// the `@{source_ts}` suffix so repeated reconcile runs coalesce on the bare
/// reference instead of stacking one job per run. Coalescing keeps the first
/// (older-ts) envelope, which is the conservative choice under receiver
/// last-writer-wins, and a later scrub run re-enqueues once the pending job
/// clears.
///
/// # Errors
///
/// Returns an [`Error`] when the lock key is invalid or the payload cannot be
/// serialized.
pub fn build_prune_delete_envelope(job: &ReplicationJob) -> Result<JobEnvelope, Error> {
    JobEnvelope::new(
        Queue::Replication,
        job.kind(),
        delete_lock_key_base(job.target()),
        job,
    )
}

/// Mirrors a local repository's state to a configured downstream.
pub struct ReplicationJobHandler {
    resolver: Arc<RepositoryResolver>,
    blob_store: Arc<BlobStore>,
    metadata_store: Arc<MetadataStore>,
}

impl ReplicationJobHandler {
    #[must_use]
    pub fn new(
        resolver: Arc<RepositoryResolver>,
        blob_store: Arc<BlobStore>,
        metadata_store: Arc<MetadataStore>,
    ) -> Self {
        Self {
            resolver,
            blob_store,
            metadata_store,
        }
    }

    /// Resolves the [`ReplicationDownstream`] for a payload, or errors when the
    /// namespace or downstream is not configured.
    fn resolve_downstream<'a>(
        &'a self,
        namespace: &Namespace,
        downstream_name: &str,
    ) -> Result<&'a ReplicationDownstream, Error> {
        let repository = self.resolver.resolve(namespace).ok_or_else(|| {
            Error::Execution(format!(
                "no repository configured for namespace '{namespace}'"
            ))
        })?;
        // Failing loudly on an unknown downstream surfaces stale config: the
        // orphaned jobs dead-letter after max attempts instead of vanishing,
        // and `angos prune`'s orphan-job sweep clears them.
        repository
            .replication
            .iter()
            .find(|d| d.name == downstream_name)
            .ok_or_else(|| {
                Error::Execution(format!(
                    "no downstream '{downstream_name}' configured for namespace '{namespace}'"
                ))
            })
    }

    /// Records the per-outcome counter and, for a converged outcome, the
    /// last-success gauge. `Unsupported` completed without converging, so it
    /// counts but must not advance the staleness gauge.
    fn record_success(downstream: &str, outcome: PushOutcome) {
        let outcome_label = match outcome {
            PushOutcome::Pushed => "pushed",
            PushOutcome::Converged => "converged",
            PushOutcome::Superseded => "superseded",
            PushOutcome::Unsupported => "unsupported",
        };
        metrics_provider()
            .replication_push_total
            .with_label_values(&[downstream, outcome_label])
            .inc();
        if !matches!(outcome, PushOutcome::Unsupported) {
            metrics_provider()
                .replication_last_success_timestamp
                .with_label_values(&[downstream])
                .set(Utc::now().timestamp());
        }
    }

    /// Records a `failed` push for `downstream`: per-attempt, and counted for
    /// every `Err`, including pre-flight and local-read failures.
    fn record_failure(downstream: &str) {
        metrics_provider()
            .replication_push_total
            .with_label_values(&[downstream, "failed"])
            .inc();
    }

    /// Re-resolves the current local target for a push: a tag is re-read at
    /// execute time (latest-wins) for its digest and `created_at`, while a
    /// tag-less job uses the payload digest and only confirms its revision link.
    /// Returns `Ok(None)` when the target is gone locally, which the caller
    /// treats as an already-converged no-op.
    async fn resolve_current_digest(
        &self,
        namespace: &Namespace,
        target: &ReplicationTarget,
    ) -> Result<Option<(Digest, Option<DateTime<Utc>>)>, Error> {
        if let Some(tag) = &target.tag {
            match self
                .metadata_store
                .read_link(namespace, &LinkKind::Tag(tag.clone()))
                .await
            {
                Ok(link) => Ok(Some((link.target, link.created_at))),
                Err(MetadataStoreError::NotFound) => Ok(None),
                Err(e) => Err(Error::Execution(format!(
                    "failed to read tag '{tag}' in '{namespace}': {e}"
                ))),
            }
        } else {
            let digest = target.digest.clone().ok_or_else(|| {
                Error::Execution(format!(
                    "tag-less push for '{namespace}' has no digest to resolve"
                ))
            })?;
            // A content-addressed digest carries no local version timestamp.
            match self
                .metadata_store
                .read_link(namespace, &LinkKind::Digest(digest.clone()))
                .await
            {
                Ok(_) => Ok(Some((digest, None))),
                Err(MetadataStoreError::NotFound) => Ok(None),
                Err(e) => Err(Error::Execution(format!(
                    "failed to read revision '{digest}' in '{namespace}': {e}"
                ))),
            }
        }
    }

    /// Runs the push/delete attempt for a validated payload and records the
    /// success metric. It must not record `failed` itself, since every `Err` is
    /// counted once by the `inspect_err` in [`Self::execute`].
    async fn attempt(&self, job: &ReplicationJob) -> Result<(), Error> {
        let target = job.target();
        let namespace = &target.namespace;
        let downstream = self.resolve_downstream(namespace, &target.downstream)?;
        // `Err` is unreachable for a routed namespace; the map stays defensive.
        let downstream_namespace = downstream.remote(namespace).map_err(|e| {
            Error::Execution(format!(
                "invalid downstream namespace for '{namespace}': {e}"
            ))
        })?;

        match job {
            ReplicationJob::Delete { target, subject } => {
                self.attempt_delete(target, subject.as_ref(), downstream, &downstream_namespace)
                    .await
            }
            ReplicationJob::Push { target } => {
                self.attempt_push(target, downstream, &downstream_namespace)
                    .await
            }
        }
    }

    async fn attempt_delete(
        &self,
        target: &ReplicationTarget,
        subject: Option<&Digest>,
        downstream: &ReplicationDownstream,
        downstream_namespace: &Namespace,
    ) -> Result<(), Error> {
        let namespace = &target.namespace;
        let reference = if let Some(tag) = &target.tag {
            Reference::Tag(tag.clone())
        } else {
            let digest = target.digest.clone().ok_or_else(|| {
                Error::Execution(format!(
                    "tag-less delete for '{namespace}' has no digest reference"
                ))
            })?;
            Reference::Digest(digest)
        };
        let outcome = pipeline::delete_manifest(
            &downstream.registry_client,
            namespace,
            downstream_namespace,
            &reference,
            subject,
            target.source_ts,
        )
        .await
        .map_err(job_error)?;
        Self::record_success(&target.downstream, outcome);
        Ok(())
    }

    async fn attempt_push(
        &self,
        target: &ReplicationTarget,
        downstream: &ReplicationDownstream,
        downstream_namespace: &Namespace,
    ) -> Result<(), Error> {
        let namespace = &target.namespace;
        let Some((digest, created_at)) = self.resolve_current_digest(namespace, target).await?
        else {
            debug!(
                namespace = %namespace,
                tag = ?target.tag,
                digest = ?target.digest,
                "Push target no longer exists locally; treating as converged no-op"
            );
            return Ok(());
        };
        // Prefer the resolved tag's created_at over the payload so receiver-side
        // last-writer-wins compares against the digest actually sent; a tag-less
        // push has no local timestamp, so the payload value carries through.
        let source_ts = created_at.or(target.source_ts);
        let body = self.blob_store.read(&digest).await.map_err(|e| {
            Error::Execution(format!("failed to read local manifest '{digest}': {e}"))
        })?;
        let ctx = PushContext {
            downstream,
            blob_store: &self.blob_store,
            metadata_store: &self.metadata_store,
            namespace,
            downstream_namespace,
            source_ts,
            index_depth: 0,
        };
        let outcome = pipeline::push_manifest(&ctx, &digest, target.tag.as_deref(), body)
            .await
            .map_err(job_error)?;
        Self::record_success(&target.downstream, outcome);
        Ok(())
    }
}

#[async_trait]
impl JobHandler for ReplicationJobHandler {
    async fn execute(&self, envelope: &JobEnvelope) -> Result<(), Error> {
        if envelope.kind != REPLICATION_PUSH_MANIFEST_KIND
            && envelope.kind != REPLICATION_DELETE_MANIFEST_KIND
        {
            return Err(Error::Execution(format!(
                "unsupported job kind '{}'; expected one of '{REPLICATION_PUSH_MANIFEST_KIND}', \
                 '{REPLICATION_DELETE_MANIFEST_KIND}'",
                envelope.kind,
            )));
        }

        let job: ReplicationJob = serde_json::from_value(envelope.payload.clone())
            .map_err(|e| Error::Execution(format!("failed to deserialize job payload: {e}")))?;

        // Mesh cycles terminate at the mutation boundary: a mutation dispatches
        // replication only when local state actually changed, so the handler
        // needs no origin filter of its own.

        // `failed` is counted exactly once here; success metrics land inside the
        // attempt. The boxing is load-bearing: the concrete future chain below
        // `attempt` overflows rustc's layout query depth in release builds.
        Box::pin(self.attempt(&job))
            .await
            .inspect_err(|_| Self::record_failure(&job.target().downstream))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests;
