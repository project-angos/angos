use std::{io, pin::Pin};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{Stream, stream};
use tokio::sync::mpsc;

use crate::{
    error::Error,
    object::ObjectStore,
    types::{Etag, MultipartPage, Part, UploadId},
};

/// Boxed byte stream used as the body of [`MultipartStore::upload_part`].
///
/// `'static` and `Send`, so it can be shipped across `tokio::spawn` boundaries
/// and stored in handles. The stream yields chunks as `Result<Bytes, io::Error>`
/// — the `io::Error` slot lets readers (e.g. file-backed sources) surface I/O
/// failures inline rather than aborting the upload out-of-band.
pub type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + 'static>>;

/// Wrap a [`mpsc::Receiver`] of [`Bytes`] as a [`ByteStream`].
///
/// Use this for producer/consumer uploads where a writer feeds chunks into
/// `tx` and the upload reads them off `rx`. The stream terminates when all
/// senders are dropped.
#[must_use]
pub fn channel_stream(rx: mpsc::Receiver<Bytes>) -> ByteStream {
    Box::pin(stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|chunk| (Ok(chunk), rx))
    }))
}

/// Multipart-upload extension to [`ObjectStore`].
///
/// Models the S3 multipart-upload protocol verbatim — the only backend in
/// the registry that implements this is S3. FS backends keep their existing
/// append-mode-file upload path and do not implement this trait; consumers
/// branch on whether a `MultipartStore` is available rather than emulating
/// multipart on FS.
#[async_trait]
pub trait MultipartStore: ObjectStore {
    /// Initiate a new multipart upload at `key` and return its `UploadId`.
    async fn create_multipart(&self, key: &str) -> Result<UploadId, Error>;

    /// Upload one part by streaming `body` into the backend's HTTP body.
    /// `content_length` must match the total bytes the stream will yield.
    ///
    /// In-memory callers should construct a [`ByteStream`] directly via
    /// `Box::pin(stream::once(async move { Ok(data) }))`;
    /// producer/consumer callers should wrap their channel with
    /// [`channel_stream`]. Other sources (file readers, etc.) can construct
    /// a [`ByteStream`] directly.
    ///
    /// The body is signed as `UNSIGNED-PAYLOAD` regardless of source — S3
    /// integrity for individual parts is provided by the per-part `ETag`.
    async fn upload_part(
        &self,
        key: &str,
        id: &UploadId,
        part_number: u32,
        content_length: u64,
        body: ByteStream,
    ) -> Result<Etag, Error>;

    /// Server-side copy of `source` (or a byte range thereof) as one part of
    /// the multipart upload at `destination`. `range` uses HTTP `Range`
    /// syntax (e.g. `bytes=0-1048575`).
    async fn upload_part_copy(
        &self,
        source: &str,
        destination: &str,
        id: &UploadId,
        part_number: u32,
        range: Option<String>,
    ) -> Result<Etag, Error>;

    /// Finalise the multipart upload, assembling the listed `parts` into a
    /// single object at `key`. `parts` must be sorted by `part_number` and
    /// dense (no gaps).
    async fn complete_multipart(
        &self,
        key: &str,
        id: &UploadId,
        parts: &[Part],
    ) -> Result<(), Error>;

    /// Abandon the multipart upload. Any uploaded parts are discarded by the
    /// backend.
    async fn abort_multipart(&self, key: &str, id: &UploadId) -> Result<(), Error>;

    /// List in-progress multipart uploads under `prefix` (or globally when
    /// `prefix` is `None`). Pass `key_marker`/`upload_id_marker` from the
    /// previous page to resume.
    async fn list_multipart_uploads(
        &self,
        prefix: Option<&str>,
        key_marker: Option<&str>,
        upload_id_marker: Option<&str>,
    ) -> Result<MultipartPage, Error>;

    /// List the parts already uploaded for a given multipart session.
    async fn list_parts(&self, key: &str, id: &UploadId) -> Result<Vec<Part>, Error>;

    /// Walk `list_multipart_uploads` pages looking for the first upload whose
    /// key equals `key` exactly, returning its `UploadId` or `None`.
    async fn search_multipart_upload_id(&self, key: &str) -> Result<Option<UploadId>, Error> {
        let mut key_marker: Option<String> = None;
        let mut upload_id_marker: Option<String> = None;
        loop {
            let page = self
                .list_multipart_uploads(
                    Some(key),
                    key_marker.as_deref(),
                    upload_id_marker.as_deref(),
                )
                .await?;
            if let Some(found) = page.uploads.into_iter().find(|u| u.key == key) {
                return Ok(Some(found.upload_id));
            }
            if page.next_key_marker.is_none() {
                return Ok(None);
            }
            key_marker = page.next_key_marker;
            upload_id_marker = page.next_upload_id_marker;
        }
    }

    /// Repeatedly call [`search_multipart_upload_id`] + [`abort_multipart`]
    /// until no in-progress upload for `key` is found. Used to cancel any
    /// leaked or duplicate sessions before starting a fresh one.
    ///
    /// [`search_multipart_upload_id`]: MultipartStore::search_multipart_upload_id
    /// [`abort_multipart`]: MultipartStore::abort_multipart
    async fn abort_pending_uploads(&self, key: &str) -> Result<(), Error> {
        while let Some(id) = self.search_multipart_upload_id(key).await? {
            self.abort_multipart(key, &id).await?;
        }
        Ok(())
    }
}
