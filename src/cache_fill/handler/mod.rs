//! [`CacheFillJobHandler`]: the [`JobHandler`] that fills the pull-through
//! blob cache. It returns an empty [`Transaction`] on success (bytes and
//! grants commit on their own stores), so the fill stays idempotent under the
//! at-least-once contract.

use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::warn;

use angos_tx_engine::transaction::Transaction;

use crate::{
    event_webhook::{
        dispatcher::EventDispatcher,
        event::{Event, EventActor, EventKind},
    },
    jobs::Queue,
    jobs::store::{Error, JobEnvelope, JobHandler},
    oci::{Digest, Namespace},
    registry::{
        Error as RegistryError, blob::cache_blob, blob_ownership::BlobOwnership,
        blob_store::BlobStore, metadata_store::MetadataStore,
        repository_resolver::RepositoryResolver,
    },
};

/// Maps a registry error to a job error, preserving an upstream authorization
/// denial as the terminal [`Error::Denied`] so the worker dead-letters it
/// instead of retrying against an unchangeable outcome.
fn job_error(error: RegistryError) -> Error {
    match error {
        RegistryError::Denied(msg) => Error::Denied(msg),
        other => Error::Execution(other.to_string()),
    }
}

pub const CACHE_FETCH_BLOB_KIND: &str = "cache.fetch_blob";

/// Internal-process name stamped on the events cache fills emit.
pub const CACHE_ACTOR: &str = "cache";

/// JSON payload for a [`CACHE_FETCH_BLOB_KIND`] job on the `cache` queue.
#[derive(Debug, Serialize, Deserialize)]
pub struct CacheFetchBlobPayload {
    pub namespace: Namespace,
    /// Serialized OCI digest, e.g. `sha256:abc...`.
    pub digest: String,
}

/// Builds a [`JobEnvelope`] for a cache-fill job, with the
/// `{Queue::Cache}.{namespace}:{digest}` lock key so identical pending fills
/// coalesce; exposed so every producer enqueues the same envelope shape.
///
/// # Errors
///
/// Returns a [`serde_json::Error`] when the payload cannot be serialized.
pub fn build_envelope(
    namespace: &Namespace,
    digest: &Digest,
) -> Result<JobEnvelope, serde_json::Error> {
    let payload = CacheFetchBlobPayload {
        namespace: namespace.clone(),
        digest: digest.to_string(),
    };
    JobEnvelope::new(
        Queue::Cache,
        CACHE_FETCH_BLOB_KIND,
        format!("{}.{namespace}:{digest}", Queue::Cache),
        &payload,
    )
}

/// Fills the pull-through blob cache. Constructed from its resolved
/// dependencies via [`CacheFillJobHandler::new`].
pub struct CacheFillJobHandler {
    resolver: Arc<RepositoryResolver>,
    blob_store: Arc<BlobStore>,
    metadata_store: Arc<MetadataStore>,
    event_dispatcher: Option<Arc<EventDispatcher>>,
}

impl CacheFillJobHandler {
    /// Construct a handler from its resolved dependencies: the namespace ->
    /// repository `resolver`, the `blob_store` the fetched bytes land in, the
    /// `metadata_store` recording ownership grants, and the optional
    /// `event_dispatcher` the fill emits its `blob.push` events through.
    #[must_use]
    pub fn new(
        resolver: Arc<RepositoryResolver>,
        blob_store: Arc<BlobStore>,
        metadata_store: Arc<MetadataStore>,
        event_dispatcher: Option<Arc<EventDispatcher>>,
    ) -> Self {
        Self {
            resolver,
            blob_store,
            metadata_store,
            event_dispatcher,
        }
    }

    /// Cache-fill a blob for a pull-through namespace: emits the `blob.push`
    /// intent with the internal `cache` actor, then grants a reference when
    /// the bytes are already present locally, otherwise fetches them from the
    /// upstream and stores them. The emission is best effort: the fill is
    /// idempotent, so a delivery failure must not fail (and re-run) the job.
    async fn fill(&self, namespace: &Namespace, digest: &Digest) -> Result<(), RegistryError> {
        let repository_name = self
            .resolver
            .resolve(namespace)
            .map(|r| r.name.to_string())
            .unwrap_or_default();
        let event = Event::new(EventKind::BlobPush, namespace.clone(), repository_name)
            .digest(Some(digest.to_string()))
            .actor(Some(EventActor::internal(CACHE_ACTOR)));
        if let Some(dispatcher) = &self.event_dispatcher
            && let Err(error) = dispatcher.dispatch(&event).await
        {
            warn!("Cache-fill event delivery failed: {error}");
        }

        // Bytes already present locally (cached by this or another namespace):
        // grant this namespace a reference without re-fetching. Gate on *byte
        // presence*, not on a `can_read` ownership link: a manifest pull records
        // the layer's ownership link before its bytes are fetched, so a
        // link-only short-circuit here would skip the fetch and the blob would
        // never be cached.
        //
        // The grant commits on the metadata store and the bytes on the blob
        // store; neither is folded into the job-completion transaction, because
        // those stores can be separate backends and a single executor cannot
        // commit across both. The work is idempotent, so it is safe to redo on a
        // retry even though it no longer commits atomically with job completion.
        //
        // The presence check runs with the grant under the blob-data lock, so a
        // concurrent reclaim cannot delete the bytes between the two; absent
        // bytes fall through to the fetch path.
        let granted = self
            .metadata_store
            .with_blob_data_lock(digest, async {
                match self.blob_store.size(digest).await {
                    Ok(_) => BlobOwnership::new(self.metadata_store.as_ref())
                        .grant(namespace, digest)
                        .await
                        .map(|()| true),
                    Err(RegistryError::BlobUnknown | RegistryError::NotFound) => Ok(false),
                    Err(error) => Err(error),
                }
            })
            .await?;

        if !granted {
            let repository = self
                .resolver
                .resolve(namespace)
                .ok_or(RegistryError::NameUnknown)?;
            if !repository.is_pull_through() {
                return Err(RegistryError::Internal(
                    "repository is not a pull-through proxy".to_string(),
                ));
            }

            let (content_length, stream) = repository.get_blob(&[], namespace, digest).await?;
            cache_blob(
                &self.blob_store,
                &self.metadata_store,
                namespace,
                digest,
                stream,
                content_length,
            )
            .await?;
        }

        Ok(())
    }
}

#[async_trait]
impl JobHandler for CacheFillJobHandler {
    async fn execute(&self, envelope: &JobEnvelope) -> Result<Transaction, Error> {
        if envelope.kind != CACHE_FETCH_BLOB_KIND {
            return Err(Error::Execution(format!(
                "unsupported job kind '{}'; expected '{CACHE_FETCH_BLOB_KIND}'",
                envelope.kind,
            )));
        }
        let payload: CacheFetchBlobPayload = serde_json::from_value(envelope.payload.clone())
            .map_err(|e| Error::Execution(format!("failed to deserialize job payload: {e}")))?;

        let digest: Digest = payload
            .digest
            .parse()
            .map_err(|e| Error::Execution(format!("invalid digest '{}': {e}", payload.digest)))?;

        self.fill(&payload.namespace, &digest)
            .await
            .map_err(job_error)?;

        Ok(Transaction::builder().build())
    }
}

#[cfg(test)]
mod tests;
