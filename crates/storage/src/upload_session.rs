//! Value types for the keyed, append-only upload primitive on
//! [`ObjectStore`](crate::ObjectStore). The trait methods live on `ObjectStore`;
//! this module owns only the values they exchange.
//!
//! Uploads are addressed solely by their object key — there is no caller-held
//! session value. The S3 backend recovers all multipart state from S3 itself
//! on each call, so nothing is persisted by the caller between calls.
//!
//! Bodies are streamed end-to-end via [`ByteStream`]; backends never buffer a
//! full upload in memory. Single-shot callers wrap one frame with
//! `stream::once(async move { Ok(bytes) })`; producer/consumer callers wrap
//! a [`tokio::sync::mpsc::Receiver`] with [`channel_stream`].

use std::{io, pin::Pin};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures_util::{Stream, stream};
use tokio::sync::mpsc;

/// Boxed byte stream consumed by [`ObjectStore::write_upload`](crate::ObjectStore::write_upload).
///
/// `'static` and `Send`, so it can be shipped across `tokio::spawn` boundaries
/// and stored in handles. The stream yields chunks as `Result<Bytes, io::Error>`
/// — the `io::Error` slot lets readers (file-backed sources, hashing readers)
/// surface I/O failures inline rather than aborting the upload out-of-band.
pub type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + 'static>>;

/// Wrap a [`mpsc::Receiver`] of [`Bytes`] as a [`ByteStream`].
#[must_use]
pub fn channel_stream(rx: mpsc::Receiver<Bytes>) -> ByteStream {
    Box::pin(stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|chunk| (Ok(chunk), rx))
    }))
}

/// An in-flight S3 multipart upload discovered by
/// [`ObjectStore::list_multipart_uploads`](crate::ObjectStore::list_multipart_uploads).
/// Backends without a multipart protocol (FS, memory) never produce these.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingMultipartUpload {
    /// Object key the multipart upload targets.
    pub key: String,
    /// S3 multipart upload identifier, required to abort it.
    pub upload_id: String,
    /// When the multipart upload was initiated, used by callers to decide
    /// whether it has been abandoned.
    pub initiated_at: DateTime<Utc>,
}

/// One page of [`ObjectStore::list_multipart_uploads`](crate::ObjectStore::list_multipart_uploads).
/// When both markers are `None` the listing is exhausted.
#[derive(Clone, Debug, Default)]
pub struct MultipartUploadPage {
    pub uploads: Vec<PendingMultipartUpload>,
    pub next_key_marker: Option<String>,
    pub next_upload_id_marker: Option<String>,
}

// The upload methods (`create_upload`, `write_upload`, `complete_upload`,
// `abort_upload`, `list_multipart_uploads`) live on the
// [`ObjectStore`](crate::ObjectStore) trait. This module owns only the value
// types those methods exchange.
