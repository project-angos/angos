//! The filesystem endpoints under `<name>/_angos/layers/<digest>/`: the entry
//! listing of a layer, indexed on first request, one file out of it, and what
//! a file holds once decoded. All read under the blob's ownership check, since
//! a listing tells what the bytes hold.

use std::io::{self, Write};
use std::pin::Pin;

use bytes::Bytes;
use flate2::{Compression, write::GzEncoder};
use futures_util::stream;
use tokio::io::{AsyncRead, AsyncReadExt, empty};
use tokio::sync::mpsc;
use tokio::task::spawn_blocking;
use tokio_util::io::{StreamReader, SyncIoBridge};
use tracing::{instrument, warn};

use angos_extension_service::{
    LayerEntries, LayerEntriesRequest, LayerFile, LayerFileDetails, LayerFileDetailsRequest,
    LayerFileRequest, LayerListing,
};
use angos_oci::{Digest, Namespace};

use crate::{
    jobs::Queue,
    layer::{self, Checkpoints, Entry, IndexLayerPayload, Kind, Listing, elf, pem},
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
        let LayerEntriesRequest {
            namespace,
            digest,
            gzip,
        } = request;
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
        // An older version's listing serves as it is while a job walks the layer
        // again, unless another did first.
        if listing.is_outdated() {
            self.dispatch_index(&namespace, &digest).await;
        }
        // Megabytes to serialize and compress: off the async threads.
        let body = spawn_blocking(move || encode(&listing.into(), gzip))
            .await
            .map_err(|e| Error::Internal(e.to_string()))??;
        Ok(LayerEntries::Ready { body, gzip })
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
            range,
        } = request;
        self.readable_layer(&namespace, &digest).await?;
        let listing = layer::read_listing(&self.metadata_store, &digest)
            .await?
            .ok_or(Error::NotFound)?;
        let entry = file_entry(&listing.entries, &path).ok_or(Error::NotFound)?;
        let range = match range {
            Some(range) => range.resolve(entry.size)?,
            None => None,
        };
        // A range narrows the bytes read to its own, from the nearest checkpoint.
        let (offset, size) = match range {
            Some(range) => (entry.offset + range.start, range.length()),
            None => (entry.offset, entry.size),
        };

        let content_type = entry.content.as_ref().map_or_else(
            || {
                mime_guess::from_path(&path)
                    .first_or_octet_stream()
                    .to_string()
            },
            |content| content.mime_type.clone(),
        );

        let checkpoints = self.checkpoints(&listing, &digest).await?;
        let reader = self
            .stream_reader(&digest, checkpoints.as_ref(), offset, size)
            .await?;
        Ok(LayerFile {
            path,
            content_type,
            size,
            download,
            range,
            reader,
        })
    }

    /// What one file of the layer holds that its bytes spell only once
    /// decoded: an ELF binary's header and libraries, or a PEM file's
    /// certificates, reading of the file no more than that takes.
    #[instrument(skip(self))]
    pub async fn handle_get_layer_file_details(
        &self,
        request: LayerFileDetailsRequest,
    ) -> Result<LayerFileDetails, Error> {
        let LayerFileDetailsRequest {
            namespace,
            digest,
            path,
        } = request;
        self.readable_layer(&namespace, &digest).await?;
        let listing = layer::read_listing(&self.metadata_store, &digest)
            .await?
            .ok_or(Error::NotFound)?;
        let entry = file_entry(&listing.entries, &path).ok_or(Error::NotFound)?;
        let checkpoints = self.checkpoints(&listing, &digest).await?;
        let read = |offset: u64, length: u64| {
            // As many bytes as the file holds past `offset`, at most `length`.
            let size = length.min(entry.size.saturating_sub(offset));
            let checkpoints = checkpoints.as_ref();
            let digest = &digest;
            async move {
                let mut bytes = Vec::new();
                self.stream_reader(digest, checkpoints, entry.offset + offset, size)
                    .await?
                    .read_to_end(&mut bytes)
                    .await?;
                Ok::<_, Error>(bytes)
            }
        };
        let head = read(0, elf::HEAD_LEN).await?;
        if let Some(details) = elf::describe(&head, &read).await? {
            return Ok(LayerFileDetails {
                elf: Some(details),
                certificates: None,
            });
        }
        if pem::holds_certificates(&head) {
            let text = if entry.size > elf::HEAD_LEN {
                read(0, pem::PEM_LIMIT).await?
            } else {
                head
            };
            return Ok(LayerFileDetails {
                elf: None,
                certificates: Some(pem::blocks(&text)),
            });
        }
        Ok(LayerFileDetails::default())
    }

    /// A gzipped layer's inflater checkpoints; `None` for a plain one, read as
    /// it is.
    async fn checkpoints(
        &self,
        listing: &Listing,
        digest: &Digest,
    ) -> Result<Option<Checkpoints>, Error> {
        if !listing.compressed {
            return Ok(None);
        }
        Ok(Some(
            layer::read_checkpoints(&self.metadata_store, digest).await?,
        ))
    }

    /// `size` bytes of the layer's uncompressed stream from `offset`, decoded
    /// from the nearest checkpoint when the layer is gzipped.
    async fn stream_reader(
        &self,
        digest: &Digest,
        checkpoints: Option<&Checkpoints>,
        offset: u64,
        size: u64,
    ) -> Result<LayerFileReader, Error> {
        if size == 0 {
            return Ok(Box::pin(empty()));
        }
        let Some(checkpoints) = checkpoints else {
            let (reader, _) = self.blob_store.reader(digest, Some(offset)).await?;
            return Ok(Box::pin(reader.take(size)));
        };
        let checkpoint = checkpoints.before(offset);
        let start = checkpoint
            .as_ref()
            .map_or(0, |checkpoint| checkpoint.in_offset);
        let (reader, _) = self.blob_store.reader(digest, Some(start)).await?;
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
        Ok(Box::pin(StreamReader::new(chunks)))
    }

    async fn readable_layer(&self, namespace: &Namespace, digest: &Digest) -> Result<(), Error> {
        if self.metadata_store.can_read(namespace, digest).await? {
            Ok(())
        } else {
            Err(Error::BlobUnknown)
        }
    }

    /// Fire-and-forget enqueue of a layer's index job, after a push into an
    /// image an index policy applies to or to walk an outdated listing again;
    /// a failure is logged and counted, never the client's problem.
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

/// The listing as JSON, gzipped when `gzip`.
fn encode(listing: &LayerListing, gzip: bool) -> Result<Vec<u8>, Error> {
    let json = serde_json::to_vec(listing)?;
    if !gzip {
        return Ok(json);
    }
    // Megabytes of mostly digests: the fastest level saves nearly all a
    // slower one would, at a third of its cost.
    let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
    encoder.write_all(&json)?;
    Ok(encoder.finish()?)
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
