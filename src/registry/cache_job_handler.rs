use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{
    oci::{Digest, Namespace},
    registry::{
        blob::cache_blob,
        blob_ownership::BlobOwnership,
        blob_store::UploadStore,
        job_store::{JobEnvelope, JobHandler},
        metadata_store::MetadataStore,
        repository_resolver::RepositoryResolver,
    },
};

pub const CACHE_QUEUE: &str = "cache";
pub const CACHE_FETCH_BLOB_KIND: &str = "cache.fetch_blob";

/// JSON payload for a [`CACHE_FETCH_BLOB_KIND`] job on [`CACHE_QUEUE`].
#[derive(Debug, Serialize, Deserialize)]
pub struct CacheFetchBlobPayload {
    pub namespace: String,
    /// Serialized OCI digest, e.g. `sha256:abc...`.
    pub digest: String,
}

/// Pulls a blob from the upstream registry and writes it to the local blob
/// store. Skips the fetch when the blob is already locally owned, so multiple
/// replicas can dedup the same miss safely.
pub struct CacheJobHandler {
    resolver: Arc<RepositoryResolver>,
    upload_store: Arc<dyn UploadStore>,
    metadata_store: Arc<dyn MetadataStore>,
}

impl CacheJobHandler {
    pub fn new(
        resolver: Arc<RepositoryResolver>,
        upload_store: Arc<dyn UploadStore>,
        metadata_store: Arc<dyn MetadataStore>,
    ) -> Self {
        Self {
            resolver,
            upload_store,
            metadata_store,
        }
    }
}

#[async_trait]
impl JobHandler for CacheJobHandler {
    async fn execute(&self, envelope: &JobEnvelope) -> Result<(), String> {
        if envelope.kind != CACHE_FETCH_BLOB_KIND {
            return Err(format!(
                "unsupported job kind '{}'; expected '{CACHE_FETCH_BLOB_KIND}'",
                envelope.kind,
            ));
        }
        let payload: CacheFetchBlobPayload = serde_json::from_value(envelope.payload.clone())
            .map_err(|e| format!("failed to deserialize job payload: {e}"))?;

        let namespace = Namespace::new(&payload.namespace)
            .map_err(|e| format!("invalid namespace '{}': {e}", payload.namespace))?;
        let digest: Digest = payload
            .digest
            .parse()
            .map_err(|e| format!("invalid digest '{}': {e}", payload.digest))?;

        // The lease was acquired before `execute`, so no other worker is
        // concurrently writing this blob — a positive ownership check means
        // someone else already finished caching it.
        let already_owned = BlobOwnership::new(self.metadata_store.as_ref())
            .can_read(&namespace, &digest)
            .await
            .map_err(|e| e.to_string())?;
        if already_owned {
            return Ok(());
        }

        let Some(repository) = self.resolver.resolve(&namespace) else {
            return Err(format!(
                "no repository configured for namespace '{namespace}'"
            ));
        };

        if !repository.is_pull_through() {
            return Err("repository is not a pull-through proxy".to_string());
        }

        let (_, stream) = repository
            .get_blob(&[], &namespace, &digest)
            .await
            .map_err(|e| e.to_string())?;

        cache_blob(
            self.upload_store.clone(),
            self.metadata_store.clone(),
            namespace,
            digest,
            stream,
        )
        .await
        .map_err(|e| e.to_string())
    }
}
