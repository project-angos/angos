//! The filesystem endpoints under `<name>/_angos/layers/<digest>/`: the entry
//! listing of a layer, indexed on first request, and one file out of it.
//! Both read under the blob's ownership check, since a listing tells what
//! the bytes hold.

use std::io;
use std::pin::Pin;

use bytes::Bytes;
use futures_util::stream;
use tokio::io::{AsyncRead, AsyncReadExt, empty};
use tokio::sync::mpsc;
use tokio::task::spawn_blocking;
use tokio_util::io::{StreamReader, SyncIoBridge};
use tracing::{instrument, warn};

use angos_extension_service::{LayerEntries, LayerEntriesRequest, LayerFile, LayerFileRequest};
use angos_oci::{Digest, Namespace};

use crate::{
    jobs::Queue,
    layer::{self, Entry, IndexLayerPayload, Kind},
    metrics_provider::metrics_provider,
    registry::{Error, Registry},
};

/// The boxed reader a layer file streams through. The gzip decode path is
/// `!Unpin`, so this is a pinned box rather than the storage `BoxedReader`.
pub type LayerFileReader = Pin<Box<dyn AsyncRead + Send>>;

impl Registry {
    /// The listing, or 202 while the index job it enqueues runs; 404 for a
    /// layer the namespace does not own, like the blob itself.
    #[instrument(skip(self))]
    pub async fn handle_list_layer_entries(
        &self,
        request: LayerEntriesRequest,
    ) -> Result<LayerEntries, Error> {
        let LayerEntriesRequest { namespace, digest } = request;
        let upstream = self
            .get_repository_for_namespace(&namespace)
            .ok()
            .filter(|repository| repository.is_pull_through());
        if !self.metadata_store.can_read(&namespace, &digest).await? {
            // A pull-through namespace can have lost the grant its manifest
            // pull made; the upstream decides, as it does for a blob GET.
            let upstream = upstream.ok_or(Error::BlobUnknown)?;
            upstream.head_blob(&[], &namespace, &digest).await?;
            self.dispatch_cache_fill(&namespace, &digest).await;
            return Ok(LayerEntries::Indexing);
        }
        let Some(listing) = layer::read_listing(&self.metadata_store, &digest).await? else {
            match self.blob_store.size(&digest).await {
                Ok(_) => {}
                // A pull-through manifest pull links its layers before their
                // bytes are fetched; fetch them, and a later poll indexes them.
                Err(Error::BlobUnknown) if upstream.is_some() => {
                    self.dispatch_cache_fill(&namespace, &digest).await;
                    return Ok(LayerEntries::Indexing);
                }
                // Gone bytes are a 404, not an index job that would find none.
                Err(error) => return Err(error),
            }
            let payload = IndexLayerPayload {
                namespace,
                digest,
                force: false,
            };
            self.job_queue
                .enqueue(layer::build_envelope(&payload)?)
                .await?;
            return Ok(LayerEntries::Indexing);
        };
        Ok(LayerEntries::Ready(listing.into()))
    }

    /// One file's bytes out of the layer, decoded from the nearest checkpoint.
    #[instrument(skip(self))]
    pub async fn handle_get_layer_file(
        &self,
        request: LayerFileRequest,
    ) -> Result<LayerFile<LayerFileReader>, Error> {
        let LayerFileRequest {
            namespace,
            digest,
            path,
            download,
        } = request;
        self.readable_layer(&namespace, &digest).await?;
        let listing = layer::read_listing(&self.metadata_store, &digest)
            .await?
            .ok_or(Error::NotFound)?;
        let entry = file_entry(&listing.entries, &path).ok_or(Error::NotFound)?;
        let (offset, size) = (entry.offset, entry.size);

        let content_type = mime_guess::from_path(&path)
            .first_or_octet_stream()
            .to_string();

        let reader: LayerFileReader = if size == 0 {
            Box::pin(empty())
        } else if !listing.compressed {
            let (reader, _) = self.blob_store.reader(&digest, Some(offset)).await?;
            Box::pin(reader.take(size))
        } else {
            let checkpoint = layer::read_checkpoints(&self.metadata_store, &digest)
                .await?
                .before(offset);
            let start = checkpoint
                .as_ref()
                .map_or(0, |checkpoint| checkpoint.in_offset);
            let (reader, _) = self.blob_store.reader(&digest, Some(start)).await?;
            // The inflater is blocking; its output crosses to the response
            // through a bounded channel, so a slow client holds the decode back
            // rather than filling memory, and a gone client ends it.
            let (tx, rx) = mpsc::channel::<io::Result<Bytes>>(4);
            spawn_blocking(move || {
                let result = layer::extract_gzip(
                    SyncIoBridge::new(reader),
                    checkpoint.as_ref(),
                    offset,
                    size,
                    |chunk| {
                        tx.blocking_send(Ok(Bytes::copy_from_slice(chunk)))
                            .map_err(|_| io::Error::other("the client went away"))
                    },
                );
                if let Err(e) = result {
                    let _ = tx.blocking_send(Err(e));
                }
            });
            let chunks = stream::unfold(rx, |mut rx| async move {
                rx.recv().await.map(|chunk| (chunk, rx))
            });
            Box::pin(StreamReader::new(chunks))
        };

        Ok(LayerFile {
            path,
            content_type,
            size,
            download,
            reader,
        })
    }

    async fn readable_layer(&self, namespace: &Namespace, digest: &Digest) -> Result<(), Error> {
        if self.metadata_store.can_read(namespace, digest).await? {
            Ok(())
        } else {
            Err(Error::BlobUnknown)
        }
    }

    /// Fire-and-forget enqueue of a layer's index job after a push into an
    /// repository with an `index` table; a failure is logged and counted, never the
    /// client's problem.
    pub(crate) async fn dispatch_index(&self, namespace: &Namespace, digest: &Digest) {
        let payload = IndexLayerPayload {
            namespace: namespace.clone(),
            digest: digest.clone(),
            force: false,
        };
        let outcome = match layer::build_envelope(&payload) {
            Ok(envelope) => self
                .job_queue
                .enqueue(envelope)
                .await
                .map_err(|e| e.to_string()),
            Err(e) => Err(e.to_string()),
        };
        if let Err(error) = outcome {
            warn!("Failed to dispatch index job for {namespace}@{digest}: {error}");
            metrics_provider()
                .job_queue_enqueue_failures_total
                .with_label_values(&[Queue::Index.as_str()])
                .inc();
        }
    }
}

/// The entry whose bytes `path` names: a file, or the file a hard link
/// points at.
fn file_entry<'a>(entries: &'a [Entry], path: &str) -> Option<&'a Entry> {
    let entry = entries.iter().rev().find(|entry| entry.path == path)?;
    match entry.kind {
        Kind::File => Some(entry),
        Kind::Hardlink => {
            let target = entry
                .link
                .as_deref()?
                .trim_start_matches("./")
                .trim_matches('/');
            entries
                .iter()
                .rev()
                .find(|entry| entry.path == target && entry.kind == Kind::File)
        }
        _ => None,
    }
}
