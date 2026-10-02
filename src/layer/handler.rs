//! The layer index job handler: walks a layer's tar stream and stores its
//! listing.

use std::{io, sync::Arc};

use async_trait::async_trait;
use tokio::runtime::Handle;
use tokio_util::io::SyncIoBridge;
use tracing::{debug, info};

use angos_inflate::Checkpoint;
use angos_oci::Digest;

use crate::{
    jobs::store::{Error, JobEnvelope, JobHandler},
    layer::{
        CHUNK_ENTRIES, Checkpoints, INDEX_LAYER_KIND, IndexLayerPayload, IndexLimits, index_stream,
    },
    registry::{Error as RegistryError, blob_store::BlobStore, metadata_store::MetadataStore},
};

pub struct IndexLayerJobHandler {
    blob_store: Arc<BlobStore>,
    metadata_store: Arc<MetadataStore>,
    limits: IndexLimits,
}

impl IndexLayerJobHandler {
    pub fn new(
        blob_store: Arc<BlobStore>,
        metadata_store: Arc<MetadataStore>,
        limits: IndexLimits,
    ) -> Self {
        Self {
            blob_store,
            metadata_store,
            limits,
        }
    }

    /// Indexes the layer unless a current listing already exists, or again
    /// when `force`; a layer whose bytes are gone has nothing to index and the
    /// job is done.
    pub async fn index(&self, digest: &Digest, force: bool) -> Result<(), Error> {
        if !force
            && self
                .metadata_store
                .read_listing(digest)
                .await?
                .is_some_and(|listing| !listing.is_outdated())
        {
            return Ok(());
        }
        let (reader, _) = match self.blob_store.reader(digest, None).await {
            Ok(reader) => reader,
            Err(RegistryError::BlobUnknown) => {
                debug!("Index of {digest} skipped: the layer is gone");
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        };
        let limits = self.limits;
        // Each chunk is stored as the walk passes it, so a layer's checkpoints
        // never sit in memory whole.
        let (runtime, store, layer) = (
            Handle::current(),
            self.metadata_store.clone(),
            digest.clone(),
        );
        let store_chunk = move |chunk: usize, checkpoints: Vec<Checkpoint>| {
            let checkpoints = Checkpoints::from_inflater(checkpoints);
            runtime
                .block_on(store.put_checkpoints(&layer, chunk, &checkpoints))
                .map_err(io::Error::other)
        };
        let (listing, entries) = tokio::task::spawn_blocking(move || {
            index_stream(SyncIoBridge::new(reader), limits, store_chunk)
        })
        .await
        .map_err(|e| Error::Execution(format!("index task failed: {e}")))?
        .map_err(|e| match e.kind() {
            // The same bytes pass the same limits on every attempt.
            io::ErrorKind::FileTooLarge => {
                Error::Terminal(format!("layer {digest} is too large to index: {e}"))
            }
            _ => Error::Execution(format!("indexing layer {digest} failed: {e}")),
        })?;
        for (chunk, entries) in entries.chunks(CHUNK_ENTRIES).enumerate() {
            self.metadata_store
                .put_entries(digest, chunk, entries)
                .await?;
        }
        let entries = entries.len();
        self.metadata_store.put_listing(digest, &listing).await?;
        info!(
            "Indexed layer {digest}: {entries} entries, {} checkpoint chunks",
            listing.checkpoints.len()
        );
        Ok(())
    }
}

#[async_trait]
impl JobHandler for IndexLayerJobHandler {
    async fn execute(&self, envelope: &JobEnvelope) -> Result<(), Error> {
        let payload: IndexLayerPayload = envelope.payload(&[INDEX_LAYER_KIND])?;
        self.index(&payload.digest, payload.force).await
    }
}
