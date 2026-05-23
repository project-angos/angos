use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::mpsc;

use crate::{
    error::Error,
    object::ObjectStore,
    types::{Etag, MultipartPage, Part, UploadId},
};

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

    /// Upload one part by streaming bytes from an mpsc channel into the
    /// backend's HTTP body. Memory in flight is bounded by the channel
    /// capacity the caller chose plus any backend-internal buffering.
    /// `content_length` must match the total bytes sent on `rx`.
    async fn upload_part_streaming(
        &self,
        key: &str,
        id: &UploadId,
        part_number: u32,
        content_length: u64,
        rx: mpsc::Receiver<Bytes>,
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
}
