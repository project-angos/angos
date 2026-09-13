//! The filesystem endpoints under `<name>/_angos/layers/<digest>/`: the entry
//! listing of a layer, indexed on first request, and one file out of it.
//! Both read under the blob's ownership check, since a listing tells what
//! the bytes hold.

use std::io;

use bytes::Bytes;
use futures_util::stream;
use http::{HeaderValue, Response, StatusCode, header};
use tokio::io::AsyncReadExt;
use tokio_util::io::{StreamReader, SyncIoBridge};
use tracing::{instrument, warn};

use angos_oci::{Digest, Namespace};

use crate::{
    http_response::{ResponseBody, build_response, json_headers, json_response},
    jobs::Queue,
    layer::{self, Entry, IndexLayerPayload, Kind},
    metrics_provider::metrics_provider,
    registry::{Error, Registry, keys::DigestKeys},
};

#[derive(Debug)]
pub struct LayerEntriesRequest {
    pub namespace: Namespace,
    pub digest: Digest,
}

#[derive(Debug)]
pub struct LayerFileRequest {
    pub namespace: Namespace,
    pub digest: Digest,
    /// The entry's path as the listing spells it.
    pub path: String,
    /// Serve as an attachment named after the file.
    pub download: bool,
}

impl Registry {
    /// The listing, or 202 while the index job it enqueues runs; 404 for a
    /// layer the namespace does not own, like the blob itself.
    #[instrument(skip(self))]
    pub async fn get_layer_entries(
        &self,
        request: LayerEntriesRequest,
    ) -> Result<Response<ResponseBody>, Error> {
        let LayerEntriesRequest { namespace, digest } = request;
        self.readable_layer(&namespace, &digest).await?;
        match self
            .metadata_store
            .object_store()
            .get(&digest.layer_entries_path())
            .await
        {
            Ok(listing) => Ok(build_response(
                StatusCode::OK,
                json_headers(),
                ResponseBody::fixed(listing),
            )?),
            Err(angos_storage::Error::NotFound) => {
                // Gone bytes are a 404, not an index job that would find none.
                self.blob_store.size(&digest).await?;
                let payload = IndexLayerPayload {
                    namespace,
                    digest,
                    force: false,
                };
                self.job_queue
                    .enqueue(layer::build_envelope(&payload)?)
                    .await?;
                json_response(
                    StatusCode::ACCEPTED,
                    &serde_json::json!({ "status": "indexing" }),
                )
            }
            Err(e) => Err(e.into()),
        }
    }

    /// One file's bytes out of the layer, decoded from the nearest checkpoint.
    #[instrument(skip(self))]
    pub async fn get_layer_file(
        &self,
        request: LayerFileRequest,
    ) -> Result<Response<ResponseBody>, Error> {
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

        let mut headers = http::HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_str(
                mime_guess::from_path(&path)
                    .first_or_octet_stream()
                    .as_ref(),
            )?,
        );
        headers.insert(header::CONTENT_LENGTH, HeaderValue::from(size));
        if download {
            let name = path.rsplit('/').next().unwrap_or(&path).replace('"', "");
            headers.insert(
                header::CONTENT_DISPOSITION,
                HeaderValue::from_str(&format!("attachment; filename=\"{name}\""))?,
            );
        }
        if size == 0 {
            return Ok(build_response(
                StatusCode::OK,
                headers,
                ResponseBody::empty(),
            )?);
        }

        let frame = self.blob_stream_frame_size;
        if !listing.compressed {
            let (reader, _) = self.blob_store.reader(&digest, Some(offset)).await?;
            let body = ResponseBody::streaming(reader.take(size), frame);
            return Ok(build_response(StatusCode::OK, headers, body)?);
        }

        let checkpoint = layer::read_checkpoints(&self.metadata_store, &digest)
            .await?
            .before(offset);
        let start = checkpoint
            .as_ref()
            .map_or(0, |checkpoint| checkpoint.in_offset);
        let (reader, _) = self.blob_store.reader(&digest, Some(start)).await?;
        // The inflater is blocking; its output crosses to the response through
        // a bounded channel, so a slow client holds the decode back rather than
        // filling memory, and a gone client ends it.
        let (tx, rx) = tokio::sync::mpsc::channel::<io::Result<Bytes>>(4);
        tokio::task::spawn_blocking(move || {
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
        let body = ResponseBody::streaming(StreamReader::new(chunks), frame);
        Ok(build_response(StatusCode::OK, headers, body)?)
    }

    async fn readable_layer(&self, namespace: &Namespace, digest: &Digest) -> Result<(), Error> {
        if self.metadata_store.can_read(namespace, digest).await? {
            Ok(())
        } else {
            Err(Error::BlobUnknown)
        }
    }

    /// Fire-and-forget enqueue of a layer's index job after a push into an
    /// `index = true` repository; a failure is logged and counted, never the
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
