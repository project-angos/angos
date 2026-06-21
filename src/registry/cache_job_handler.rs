use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use angos_tx_engine::transaction::Transaction;

use crate::{
    oci::{Digest, Namespace},
    registry::{
        blob::cache_blob_mutations,
        blob_store::BlobStore,
        job_store::{Error, JobEnvelope, JobHandler},
        metadata_store::MetadataStore,
        repository_resolver::RepositoryResolver,
    },
};

pub const CACHE_FETCH_BLOB_KIND: &str = "cache.fetch_blob";

/// JSON payload for a [`CACHE_FETCH_BLOB_KIND`] job on the `cache` queue.
#[derive(Debug, Serialize, Deserialize)]
pub struct CacheFetchBlobPayload {
    pub namespace: Namespace,
    /// Serialized OCI digest, e.g. `sha256:abc...`.
    pub digest: String,
}

/// Pulls a blob from the upstream registry and writes it to the local blob
/// store. Skips the upstream fetch when the bytes are already present locally
/// (granting this namespace a reference instead), so concurrent fills of the
/// same blob dedup safely; otherwise it fetches and stores the bytes.
pub struct CacheJobHandler {
    resolver: Arc<RepositoryResolver>,
    blob_store: Arc<BlobStore>,
    metadata_store: Arc<MetadataStore>,
}

impl CacheJobHandler {
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

        let namespace = payload.namespace;
        let digest: Digest = payload
            .digest
            .parse()
            .map_err(|e| Error::Storage(format!("invalid digest '{}': {e}", payload.digest)))?;

        // Bytes already present locally (cached by this or another namespace):
        // grant this namespace a reference without re-fetching. Gate on *byte
        // presence*, not on a `can_read` ownership link: a manifest pull records
        // the layer's ownership link before its bytes are fetched, so a
        // link-only short-circuit here would skip the fetch and the blob would
        // never be cached (see doc/reviews/20260603-in-process-cache-fill-broken.md).
        if self.blob_store.size(&digest).await.is_ok() {
            let (reads, mutations) = self
                .metadata_store
                .build_grant_mutations(&namespace, &digest)
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

        let (content_length, stream) = repository
            .get_blob(&[], &namespace, &digest)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        let (reads, mutations) = cache_blob_mutations(
            self.blob_store.clone(),
            self.metadata_store.clone(),
            namespace,
            digest,
            stream,
            content_length,
        )
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        Ok(Transaction::from_parts(reads, mutations))
    }
}
