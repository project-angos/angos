//! [`CacheFillJobHandler`]: the [`JobHandler`] that fills the pull-through
//! blob cache. Bytes and grants land on their own stores, so the fill must
//! stay idempotent under the at-least-once queue contract.

use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::warn;

use angos_oci::{Digest, Namespace};

use crate::{
    event_webhook::{
        dispatcher::EventDispatcher,
        event::{Event, EventActor},
    },
    jobs::Queue,
    jobs::store::{Error, JobEnvelope, JobHandler},
    registry::{
        Error as RegistryError, blob::cache_blob, blob_ownership, blob_ownership::GrantOutcome,
        blob_store::BlobStore, metadata_store::MetadataStore,
        repository_resolver::RepositoryResolver,
    },
};

/// Maps a registry error to a job error, dead-lettering an upstream
/// authorization denial instead of retrying an outcome that cannot change.
fn job_error(error: RegistryError) -> Error {
    match error {
        RegistryError::Denied(msg) => Error::Terminal(msg),
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
    pub digest: Digest,
}

/// Builds a [`JobEnvelope`] for a cache-fill job, keyed on
/// `{Queue::Cache}.{namespace}:{digest}` so identical pending fills coalesce.
///
/// # Errors
///
/// Returns an [`Error`] when the lock key is invalid or the payload cannot be
/// serialized.
pub fn build_envelope(namespace: &Namespace, digest: &Digest) -> Result<JobEnvelope, Error> {
    let payload = CacheFetchBlobPayload {
        namespace: namespace.clone(),
        digest: digest.clone(),
    };
    JobEnvelope::new(
        Queue::Cache,
        CACHE_FETCH_BLOB_KIND,
        format!("{}.{namespace}:{digest}", Queue::Cache),
        &payload,
    )
}

/// Fills the pull-through blob cache.
pub struct CacheFillJobHandler {
    resolver: Arc<RepositoryResolver>,
    blob_store: Arc<BlobStore>,
    metadata_store: Arc<MetadataStore>,
    event_dispatcher: Option<Arc<EventDispatcher>>,
}

impl CacheFillJobHandler {
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

    /// Cache-fill a blob for a pull-through namespace: grants a reference when
    /// the bytes are already local, otherwise fetches and stores them. The
    /// `blob.push` emission is best effort, since a delivery failure must not
    /// re-run an idempotent fill.
    async fn fill(&self, namespace: &Namespace, digest: &Digest) -> Result<(), RegistryError> {
        let repository_name = self
            .resolver
            .resolve(namespace)
            .map(|r| r.name.to_string())
            .unwrap_or_default();
        let event = Event::push_blob(
            namespace,
            &repository_name,
            digest,
            Some(&EventActor::internal(CACHE_ACTOR)),
        );
        if let Some(dispatcher) = &self.event_dispatcher
            && let Err(error) = dispatcher.dispatch(&event).await
        {
            warn!("Cache-fill event delivery failed: {error}");
        }

        // Gate on byte presence, not on a `can_read` ownership link: a manifest
        // pull records a layer's ownership link before its bytes are fetched, so
        // a link-only short-circuit would skip the fetch and never cache the blob.
        // The guarded grant catches a reclaim mid-flight; anything but a clean
        // grant falls through to the fetch, whose fresh bytes are grace-protected.
        let granted = match self.blob_store.size(digest).await {
            Ok(_) => {
                blob_ownership::grant_existing(
                    &self.blob_store,
                    &self.metadata_store,
                    namespace,
                    digest,
                )
                .await?
                    == GrantOutcome::Granted
            }
            Err(RegistryError::BlobUnknown | RegistryError::NotFound) => false,
            Err(error) => return Err(error),
        };

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

            let fetched = repository.get_blob(&[], namespace, digest, None).await?;
            cache_blob(
                &self.blob_store,
                &self.metadata_store,
                namespace,
                digest,
                fetched.reader,
                fetched.length,
            )
            .await?;
        }

        Ok(())
    }
}

#[async_trait]
impl JobHandler for CacheFillJobHandler {
    async fn execute(&self, envelope: &JobEnvelope) -> Result<(), Error> {
        if envelope.kind != CACHE_FETCH_BLOB_KIND {
            return Err(Error::Execution(format!(
                "unsupported job kind '{}'; expected '{CACHE_FETCH_BLOB_KIND}'",
                envelope.kind,
            )));
        }
        let payload: CacheFetchBlobPayload = serde_json::from_value(envelope.payload.clone())
            .map_err(|e| Error::Execution(format!("failed to deserialize job payload: {e}")))?;

        self.fill(&payload.namespace, &payload.digest)
            .await
            .map_err(job_error)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests;
