//! [`ReplicationJobHandler`]: the [`JobHandler`] that drives the replication
//! push pipeline. It returns an empty [`Transaction`] on success (the effect is
//! an external HTTP push the engine cannot roll back), so the pipeline stays
//! idempotent via HEAD-before-PUT under the at-least-once contract.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::debug;

use angos_tx_engine::transaction::Transaction;

use crate::{
    jobs::Queue,
    jobs::store::{Error, JobEnvelope, JobHandler},
    metrics_provider::metrics_provider,
    oci::{Digest, Namespace, Reference, Tag},
    registry::{
        Error as MetadataStoreError,
        blob_store::BlobStore,
        metadata_store::{LinkKind, MetadataStore},
        repository_resolver::RepositoryResolver,
    },
    replication::{
        ReplicationDownstream,
        pipeline::{self, PushContext, PushOutcome},
    },
};

/// Records a `replication_reconcile_total` outcome (`enqueued`, `failed`, or
/// `skipped`).
pub fn record_reconcile_outcome(outcome: &str) {
    metrics_provider()
        .replication_reconcile_total
        .with_label_values(&[outcome])
        .inc();
}

/// Single queue carrying every replication job; the downstream is encoded in the
/// `lock_key` and payload.
/// Push a manifest (and everything it references) to a downstream.
pub const REPLICATION_PUSH_MANIFEST_KIND: &str = "replication.push_manifest";
/// Delete a manifest on a downstream.
pub const REPLICATION_DELETE_MANIFEST_KIND: &str = "replication.delete_manifest";

/// JSON payload for a replication job on [`Queue::Replication`]. The handler
/// re-resolves the current local `tag -> digest` at execute time and the queue
/// only coalesces not-yet-claimed jobs, so a write landing mid-push enqueues
/// its own job instead of being dropped.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicationPushPayload {
    /// Local identifier of the target downstream (selects the `RegistryClient`).
    pub downstream: String,
    pub namespace: Namespace,
    /// Tag bound to the digest, when the change is tag-scoped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag: Option<Tag>,
    /// Serialized OCI digest: informational for pushes, authoritative for
    /// digest deletes and tag-less pushes, absent for a tag delete.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub digest: Option<String>,
    /// Mirrors the envelope `kind` so a payload is self-describing for the
    /// scrub-side builder.
    pub kind: String,
    /// Event timestamp (RFC 3339) carried for receiver-side last-writer-wins.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_ts: Option<String>,
}

/// Tag-or-digest segment shared by every replication `lock_key`.
fn lock_key_reference(payload: &ReplicationPushPayload) -> &str {
    payload
        .tag
        .as_deref()
        .or(payload.digest.as_deref())
        .unwrap_or("")
}

/// Base delete `lock_key`,
/// `{Queue::Replication}.delete.{downstream}:{namespace}:{tag_or_digest}`, shared
/// by the event-path delete (which appends `@{source_ts}`) and the scrub prune
/// delete (which keys on this bare base). Defining it once keeps the invariant
/// "the prune key is the event delete key minus the timestamp suffix" in code.
fn delete_lock_key_base(payload: &ReplicationPushPayload) -> String {
    format!(
        "{}.delete.{}:{}:{}",
        Queue::Replication,
        payload.downstream,
        payload.namespace,
        lock_key_reference(payload)
    )
}

/// Builds the per-job `lock_key`,
/// `{Queue::Replication}.{op}.{downstream}:{namespace}:{tag_or_digest}` plus
/// `@{source_ts}` for deletes, shared by the event path and the scrub push
/// reconcile so identical pending work coalesces. The `op` segment keeps a
/// delete from coalescing into a still-pending push, and the delete-only
/// timestamp keeps distinct deletion events apart because a delete cannot
/// re-derive its last-writer-wins timestamp at execute time (pushes re-resolve
/// digest and timestamp, so they safely coalesce on the bare reference). Scrub
/// prune deletes coalesce differently; see [`build_prune_delete_envelope`].
#[must_use]
fn replication_lock_key(payload: &ReplicationPushPayload) -> String {
    if payload.kind == REPLICATION_DELETE_MANIFEST_KIND {
        // The event-path delete is the bare base plus the `@{source_ts}` suffix.
        format!(
            "{}@{}",
            delete_lock_key_base(payload),
            payload.source_ts.as_deref().unwrap_or("")
        )
    } else {
        format!(
            "{}.push.{}:{}:{}",
            Queue::Replication,
            payload.downstream,
            payload.namespace,
            lock_key_reference(payload)
        )
    }
}

/// Builds a [`JobEnvelope`] from a [`ReplicationPushPayload`]; exposed so the
/// scrub checker enqueues the same envelope shape as the event path.
///
/// # Errors
///
/// Returns a [`serde_json::Error`] when the payload cannot be serialized.
pub fn build_envelope(payload: &ReplicationPushPayload) -> Result<JobEnvelope, serde_json::Error> {
    JobEnvelope::new(
        Queue::Replication,
        payload.kind.clone(),
        replication_lock_key(payload),
        payload,
    )
}

/// Builds a [`JobEnvelope`] for a scrub prune delete, whose `lock_key` omits
/// the `@{source_ts}` suffix so repeated reconcile runs coalesce on the bare
/// reference instead of stacking one job per run. Coalescing keeps the first
/// (older-ts) envelope, which is strictly more conservative under receiver
/// last-writer-wins, and a later scrub run re-enqueues once the pending job
/// clears, so convergence is preserved.
///
/// # Errors
///
/// Returns a [`serde_json::Error`] when the payload cannot be serialized.
pub fn build_prune_delete_envelope(
    payload: &ReplicationPushPayload,
) -> Result<JobEnvelope, serde_json::Error> {
    JobEnvelope::new(
        Queue::Replication,
        payload.kind.clone(),
        delete_lock_key_base(payload),
        payload,
    )
}

/// Mirrors a local repository's state to a configured downstream. Constructed
/// from its resolved dependencies via [`ReplicationJobHandler::new`].
pub struct ReplicationJobHandler {
    resolver: Arc<RepositoryResolver>,
    blob_store: Arc<BlobStore>,
    metadata_store: Arc<MetadataStore>,
}

impl ReplicationJobHandler {
    /// Construct a handler from its resolved dependencies: the namespace ->
    /// repository `resolver`, the `blob_store` the manifest/blob bytes are read
    /// from, and the `metadata_store` used to re-resolve the current
    /// `tag -> digest`.
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
            Error::Storage(format!(
                "no repository configured for namespace '{namespace}'"
            ))
        })?;
        // Failing loudly on an unknown downstream is intentional: it surfaces
        // stale config (a removed or renamed downstream) and the orphaned jobs
        // dead-letter after max attempts instead of vanishing silently;
        // `angos prune`'s orphan-job sweep clears them.
        repository
            .replication
            .iter()
            .find(|d| d.name == downstream_name)
            .ok_or_else(|| {
                Error::Storage(format!(
                    "no downstream '{downstream_name}' configured for namespace '{namespace}'"
                ))
            })
    }

    /// Records the per-outcome counter and, for a converged outcome, the
    /// last-success gauge. `Unsupported` completed without error but did not
    /// converge (the downstream cannot delete this reference), so it counts but
    /// must not advance the staleness gauge.
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

    /// Records a `failed` push for `downstream` before the handler returns `Err`.
    /// `failed` is per-attempt (each retry increments it) and covers every `Err`,
    /// including pre-flight and local-read failures, not only downstream HTTP errors.
    fn record_failure(downstream: &str) {
        metrics_provider()
            .replication_push_total
            .with_label_values(&[downstream, "failed"])
            .inc();
    }

    /// Re-resolves the current local target for a push: a tag is re-read at
    /// execute time (latest-wins) yielding its digest and `created_at`, while a
    /// tag-less job uses the payload digest and only confirms the revision link
    /// exists. Returns `Ok(None)` when the target no longer exists locally,
    /// which the caller treats as already-converged no-op success.
    async fn resolve_current_digest(
        &self,
        namespace: &Namespace,
        payload: &ReplicationPushPayload,
    ) -> Result<Option<(Digest, Option<DateTime<Utc>>)>, Error> {
        // Reads bypass the per-process link cache: a worker's cache can lag a
        // sibling process's write, and a stale resolve would replicate the old
        // digest and complete the job.
        if let Some(tag) = &payload.tag {
            match self
                .metadata_store
                .read_link_reference(namespace, &LinkKind::Tag(tag.clone()))
                .await
            {
                Ok(link) => Ok(Some((link.target, link.created_at))),
                Err(MetadataStoreError::NotFound) => Ok(None),
                Err(e) => Err(Error::Storage(format!(
                    "failed to read tag '{tag}' in '{namespace}': {e}"
                ))),
            }
        } else {
            let digest_str = payload.digest.as_deref().ok_or_else(|| {
                Error::Storage(format!(
                    "tag-less push for '{namespace}' has no digest to resolve"
                ))
            })?;
            let digest: Digest = digest_str
                .parse()
                .map_err(|e| Error::Storage(format!("invalid digest '{digest_str}': {e}")))?;
            // A content-addressed digest carries no local version timestamp.
            match self
                .metadata_store
                .read_link_reference(namespace, &LinkKind::Digest(digest.clone()))
                .await
            {
                Ok(_) => Ok(Some((digest, None))),
                Err(MetadataStoreError::NotFound) => Ok(None),
                Err(e) => Err(Error::Storage(format!(
                    "failed to read revision '{digest}' in '{namespace}': {e}"
                ))),
            }
        }
    }

    /// Runs the push/delete attempt for a validated payload and records the
    /// success metric. It must NOT record `failed` itself: every `Err` is
    /// counted once by the `inspect_err` in [`Self::execute`].
    async fn attempt(
        &self,
        envelope: &JobEnvelope,
        payload: &ReplicationPushPayload,
    ) -> Result<(), Error> {
        let namespace = &payload.namespace;
        let downstream = self.resolve_downstream(namespace, &payload.downstream)?;
        let registry_client = &downstream.registry_client;
        // `Err` is unreachable for a routed namespace; the map stays defensive.
        let downstream_namespace = downstream.remote(namespace).map_err(|e| {
            Error::Storage(format!(
                "invalid downstream namespace for '{namespace}': {e}"
            ))
        })?;
        let max_concurrent_pushes = downstream.max_concurrent_pushes;

        if envelope.kind == REPLICATION_DELETE_MANIFEST_KIND {
            let reference = if let Some(tag) = &payload.tag {
                Reference::Tag(tag.clone())
            } else {
                let digest = payload.digest.as_deref().ok_or_else(|| {
                    Error::Storage(format!(
                        "tag-less delete for '{namespace}' has no digest reference"
                    ))
                })?;
                Reference::Digest(
                    digest
                        .parse()
                        .map_err(|e| Error::Storage(format!("invalid digest '{digest}': {e}")))?,
                )
            };
            let outcome = pipeline::delete_manifest(
                registry_client,
                &self.metadata_store,
                namespace,
                downstream_namespace.as_ref(),
                &reference,
                payload.source_ts.as_deref(),
            )
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
            Self::record_success(&payload.downstream, outcome);
        } else {
            // Re-resolving the digest at execute time gives latest-wins for a
            // coalesced job.
            let Some((digest, created_at)) =
                self.resolve_current_digest(namespace, payload).await?
            else {
                debug!(
                    namespace = %namespace,
                    tag = ?payload.tag,
                    digest = ?payload.digest,
                    "Push target no longer exists locally; treating as converged no-op"
                );
                return Ok(());
            };
            // Re-derive source_ts from the resolved tag's created_at (not the
            // payload) so receiver-side last-writer-wins compares against the
            // digest actually sent, for coalesced and reconcile pushes alike.
            // A tag-less push has no local timestamp, so the payload value
            // carries through (the receiver skips LWW for it).
            let source_ts = created_at
                .map(|ts| ts.to_rfc3339())
                .or_else(|| payload.source_ts.clone());
            let body = self.blob_store.read(&digest).await.map_err(|e| {
                Error::Storage(format!("failed to read local manifest '{digest}': {e}"))
            })?;
            let ctx = PushContext {
                downstream: registry_client,
                blob_store: &self.blob_store,
                metadata_store: &self.metadata_store,
                namespace,
                downstream_namespace: &downstream_namespace,
                downstream_local_namespace: downstream.local_namespace.as_ref(),
                downstream_target_namespace: downstream.target_namespace.as_ref(),
                max_concurrent_pushes,
                source_ts: source_ts.as_deref(),
            };
            let outcome =
                pipeline::push_manifest(&ctx, &digest, None, payload.tag.as_deref(), body)
                    .await
                    .map_err(|e| Error::Storage(e.to_string()))?;
            Self::record_success(&payload.downstream, outcome);
        }

        Ok(())
    }
}

#[async_trait]
impl JobHandler for ReplicationJobHandler {
    async fn execute(&self, envelope: &JobEnvelope) -> Result<Transaction, Error> {
        if envelope.kind != REPLICATION_PUSH_MANIFEST_KIND
            && envelope.kind != REPLICATION_DELETE_MANIFEST_KIND
        {
            return Err(Error::Storage(format!(
                "unsupported job kind '{}'; expected one of '{REPLICATION_PUSH_MANIFEST_KIND}', \
                 '{REPLICATION_DELETE_MANIFEST_KIND}'",
                envelope.kind,
            )));
        }

        let payload: ReplicationPushPayload = serde_json::from_value(envelope.payload.clone())
            .map_err(|e| Error::Storage(format!("failed to deserialize job payload: {e}")))?;

        // Loop prevention is no-op suppression at the mutation boundary: mutation
        // methods only dispatch replication when local state actually changed, so
        // converged replays are never re-dispatched and mesh cycles terminate.
        // The handler itself needs no origin filter.

        // Record `failed` exactly once on any Err; success metrics are recorded
        // inside the attempt.
        self.attempt(envelope, &payload)
            .await
            .inspect_err(|_| Self::record_failure(&payload.downstream))?;

        // Empty transaction: the HTTP push is the side effect.
        Ok(Transaction::builder().build())
    }
}

#[cfg(test)]
mod tests;
