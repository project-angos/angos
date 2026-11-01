//! Unified storage abstraction shared by `blob_store`, `metadata_store`, and
//! `job_store`.
//!
//! # Capability traits
//!
//! - [`ObjectStore`]: universal floor: object CRUD, prefix-batch delete,
//!   head, two listing modes (flat-recursive and one-level-children),
//!   server-side copy, and the keyed, append-only streaming upload primitive
//!   (FS opens an append-mode file; S3 wraps its native multipart-upload
//!   protocol, hiding the wire details (upload IDs, parts, staged remainders)
//!   from callers, recovering them from S3 on each call). Every backend
//!   implements this.
//! - [`PresignedStore`]: signed download URLs. Only S3 implements this.
//!
//! # Backends
//!
//! - [`fs::Backend`]: [`ObjectStore`] on top of `tokio::fs`.
//! - [`s3::Backend`]: [`ObjectStore`] + [`PresignedStore`] wrapping
//!   [`angos_s3_client::Backend`].

mod error;
mod object;
mod pagination;
mod presigned;
#[cfg(any(test, feature = "test-util"))]
pub mod test_util;
mod types;
mod upload_session;

pub mod fs;
pub mod s3;

#[cfg(test)]
mod tests;

use tokio::io::AsyncRead;

pub use crate::error::Error;
pub use crate::object::{KeyStream, ObjectStore};
pub use crate::pagination::paginated;
pub use crate::presigned::PresignedStore;
pub use crate::types::{Children, ChildrenPage, ObjectMeta, Page};
pub use crate::upload_session::{
    ByteStream, MultipartUploadPage, PendingMultipartUpload, channel_stream,
};

/// Boxed `AsyncRead` returned by [`ObjectStore::get_stream`]. Matches the
/// shape `blob_store` already uses so future migrations don't need a
/// conversion layer.
pub type BoxedReader = Box<dyn AsyncRead + Unpin + Send + Sync>;
