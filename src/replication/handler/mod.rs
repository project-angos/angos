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

use crate::jobs::Queue;
use crate::jobs::store::{Error, JobEnvelope, JobHandler};
use crate::metrics_provider::metrics_provider;
use crate::registry::Error as MetadataStoreError;
use crate::registry::blob_store::BlobStore;
use crate::registry::metadata_store::{LinkKind, MetadataStore};
use crate::registry::repository_resolver::RepositoryResolver;
use crate::replication::pipeline::{self, PushContext, PushOutcome};
use crate::replication::{
    Error as ReplicationError, REPLICATION_DELETE_MANIFEST_KIND, REPLICATION_PUSH_MANIFEST_KIND,
    ReplicationDownstream,
};

/// Dead-letters a downstream denial and an invalid manifest body instead of
/// retrying an outcome that cannot change.
fn job_error(error: ReplicationError) -> Error {
    match error {
        ReplicationError::Client(RegistryClientError::Denied(msg))
        | ReplicationError::InvalidManifest(msg) => Error::Terminal(msg),
        other => Error::Execution(other.to_string()),
    }
}

/// `outcome` is `enqueued`, `failed` or `skipped`.
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

impl ReplicationTarget {
    /// The tag when set, else the digest.
    pub fn reference(&self) -> Option<Reference> {
        match (&self.tag, &self.digest) {
            (Some(tag), _) => Some(Reference::Tag(tag.clone())),
            (None, Some(digest)) => Some(Reference::Digest(digest.clone())),
            (None, None) => None,
        }
    }
}

/// The `kind` tag beside the flat target fields is the layout the durable queue stores.
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

/// The `op` segment keeps a delete from coalescing into a pending push. The
/// event-path delete appends `@{source_ts}` to this key, the scrub prune delete
/// keys on it bare.
fn lock_key_base(op: &str, target: &ReplicationTarget) -> String {
    let reference = target
        .reference()
        .map(|reference| reference.to_string())
        .unwrap_or_default();
    format!(
        "{}.{op}.{}:{}:{reference}",
        Queue::Replication,
        target.downstream,
        target.namespace,
    )
}

/// Identical pending work coalesces on this key. The delete-only timestamp
/// keeps distinct deletions apart, since a delete cannot re-derive its
/// last-writer-wins timestamp at execute time while a push re-resolves it.
fn replication_lock_key(job: &ReplicationJob) -> String {
    let target = job.target();
    match job {
        ReplicationJob::Delete { .. } => format!(
            "{}@{}",
            lock_key_base("delete", target),
            target
                .source_ts
                .map(|ts| ts.to_rfc3339())
                .unwrap_or_default()
        ),
        ReplicationJob::Push { .. } => lock_key_base("push", target),
    }
}

pub fn build_envelope(job: &ReplicationJob) -> Result<JobEnvelope, Error> {
    JobEnvelope::new(
        Queue::Replication,
        job.kind(),
        &replication_lock_key(job),
        job,
    )
}

/// A scrub prune delete keys on the bare reference so repeated reconcile runs
/// coalesce instead of stacking; keeping the first (older-ts) envelope is the
/// conservative choice under receiver last-writer-wins.
pub fn build_prune_delete_envelope(job: &ReplicationJob) -> Result<JobEnvelope, Error> {
    JobEnvelope::new(
        Queue::Replication,
        job.kind(),
        &lock_key_base("delete", job.target()),
        job,
    )
}

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

    /// `Unsupported` completed without converging, so it must not advance the
    /// staleness gauge.
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

    /// A tag is re-read at execute time (latest-wins); a tag-less job only
    /// confirms its revision link. `Ok(None)` means the target is gone locally.
    async fn resolve_current_digest(
        &self,
        namespace: &Namespace,
        target: &ReplicationTarget,
    ) -> Result<Option<(Digest, Option<DateTime<Utc>>)>, Error> {
        let reference = target.reference().ok_or_else(|| {
            Error::Execution(format!(
                "tag-less push for '{namespace}' has no digest to resolve"
            ))
        })?;
        match self
            .metadata_store
            .read_link(namespace, &LinkKind::from(&reference))
            .await
        {
            // A content-addressed digest carries no local version timestamp.
            Ok(link) => Ok(Some(match reference {
                Reference::Tag(_) => (link.target, link.created_at),
                Reference::Digest(digest) => (digest, None),
            })),
            Err(MetadataStoreError::NotFound) => Ok(None),
            Err(e) => Err(Error::Execution(format!(
                "failed to read '{reference}' in '{namespace}': {e}"
            ))),
        }
    }

    /// Must not record `failed`: [`Self::execute`] counts every `Err` once.
    async fn attempt(&self, job: &ReplicationJob) -> Result<(), Error> {
        let target = job.target();
        let namespace = &target.namespace;
        let downstream = self.resolve_downstream(namespace, &target.downstream)?;
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
        let reference = target.reference().ok_or_else(|| {
            Error::Execution(format!(
                "tag-less delete for '{namespace}' has no digest reference"
            ))
        })?;
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
        let outcome = pipeline::push_manifest(&ctx, &digest, target.tag.as_ref(), body)
            .await
            .map_err(job_error)?;
        Self::record_success(&target.downstream, outcome);
        Ok(())
    }
}

#[async_trait]
impl JobHandler for ReplicationJobHandler {
    async fn execute(&self, envelope: &JobEnvelope) -> Result<(), Error> {
        let job: ReplicationJob = envelope.payload(&[
            REPLICATION_PUSH_MANIFEST_KIND,
            REPLICATION_DELETE_MANIFEST_KIND,
        ])?;

        // No origin filter: a mutation dispatches replication only when local
        // state changed, so mesh cycles terminate there. The boxing is
        // load-bearing: the future chain below `attempt` overflows rustc's
        // layout query depth in release builds.
        Box::pin(self.attempt(&job)).await.inspect_err(|_| {
            metrics_provider()
                .replication_push_total
                .with_label_values(&[job.target().downstream.as_str(), "failed"])
                .inc();
        })
    }
}

#[cfg(test)]
mod tests;
