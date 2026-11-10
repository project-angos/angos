use std::io;
use std::pin::Pin;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures_util::Stream;

/// An upload body; the `io::Error` slot lets a reader surface I/O failures
/// inline.
pub type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + 'static>>;

/// An in-flight S3 multipart upload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingMultipartUpload {
    pub key: String,
    pub upload_id: String,
    pub initiated_at: DateTime<Utc>,
}

/// One page of multipart uploads; both markers `None` ends the listing.
#[derive(Clone, Debug, Default)]
pub struct MultipartUploadPage {
    pub uploads: Vec<PendingMultipartUpload>,
    pub next_key_marker: Option<String>,
    pub next_upload_id_marker: Option<String>,
}
