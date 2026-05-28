use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use angos_tx_engine::transaction::Transaction;

use crate::{
    oci::{Digest, Namespace},
    registry::{
        blob::cache_blob_mutations,
        blob_ownership::BlobOwnership,
        blob_store::{BlobStore, UploadStore},
        job_store::{Error, JobEnvelope, JobHandler},
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
    blob_store: Arc<dyn BlobStore>,
    metadata_store: Arc<MetadataStore>,
}

impl CacheJobHandler {
    pub fn new(
        resolver: Arc<RepositoryResolver>,
        upload_store: Arc<dyn UploadStore>,
        blob_store: Arc<dyn BlobStore>,
        metadata_store: Arc<MetadataStore>,
    ) -> Self {
        Self {
            resolver,
            upload_store,
            blob_store,
            metadata_store,
        }
    }
}

#[async_trait]
impl JobHandler for CacheJobHandler {
    async fn execute(&self, envelope: &JobEnvelope) -> Result<Transaction, Error> {
        if envelope.kind != CACHE_FETCH_BLOB_KIND {
            return Err(Error::Storage(format!(
                "unsupported job kind '{}'; expected '{CACHE_FETCH_BLOB_KIND}'",
                envelope.kind,
            )));
        }
        let payload: CacheFetchBlobPayload = serde_json::from_value(envelope.payload.clone())
            .map_err(|e| Error::Storage(format!("failed to deserialize job payload: {e}")))?;

        let namespace = Namespace::new(&payload.namespace).map_err(|e| {
            Error::Storage(format!("invalid namespace '{}': {e}", payload.namespace))
        })?;
        let digest: Digest = payload
            .digest
            .parse()
            .map_err(|e| Error::Storage(format!("invalid digest '{}': {e}", payload.digest)))?;

        // The per-`lock_key` lock was acquired before `execute`, so no other
        // worker is concurrently writing this blob — a positive ownership
        // check means someone else already finished caching it. Return an
        // empty transaction so `complete` runs only the queue cleanup.
        let already_owned = BlobOwnership::new(self.metadata_store.as_ref())
            .can_read(&namespace, &digest)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        if already_owned {
            return Ok(Transaction::builder().build());
        }

        // Crash-resume short-circuit: canonical blob already sits at its
        // canonical key (a prior attempt landed the upload but not the grant
        // — or the multipart upload-id is no longer usable). Skip the
        // upstream fetch and just grant ownership.
        if self.blob_store.size(&digest).await.is_ok() {
            let (reads, mutations) = self
                .metadata_store
                .build_grant_mutations(namespace.as_ref(), &digest)
                .await
                .map_err(|e| Error::Storage(e.to_string()))?;
            return Ok(Transaction::from_parts(reads, mutations));
        }

        let Some(repository) = self.resolver.resolve(&namespace) else {
            return Err(Error::Storage(format!(
                "no repository configured for namespace '{namespace}'"
            )));
        };

        if !repository.is_pull_through() {
            return Err(Error::Storage(
                "repository is not a pull-through proxy".to_string(),
            ));
        }

        let (_, stream) = repository
            .get_blob(&[], &namespace, &digest)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        let (reads, mutations) = cache_blob_mutations(
            self.upload_store.clone(),
            self.metadata_store.clone(),
            namespace,
            digest,
            stream,
        )
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        Ok(Transaction::from_parts(reads, mutations))
    }
}
