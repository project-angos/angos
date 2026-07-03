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
//! - [`ConditionalStore`]: CAS extension: `put_if_absent`, `put_if_match`,
//!   `delete_if_match`. S3 implements this; FS does not (consumers fall
//!   back to the transactional engine's `Lock` primitive).
//! - [`PresignedStore`]: signed download URLs. Only S3 implements this.
//!
//! # Backends
//!
//! Each backend is compiled in by its feature flag (both are default):
//!
//! - `fs::Backend` (feature `fs`): [`ObjectStore`] on top of `tokio::fs`.
//! - `s3::Backend` (feature `s3`): [`ObjectStore`] + [`ConditionalStore`]
//!   + [`PresignedStore`] wrapping `angos_s3_client::Backend`.

mod conditional;
mod error;
#[cfg(any(test, feature = "test-util"))]
mod memory;
mod object;
mod presigned;
#[cfg(any(test, feature = "test-util"))]
pub mod test_util;
mod types;
mod upload_session;

#[cfg(feature = "fs")]
pub mod fs;
#[cfg(feature = "s3")]
pub mod s3;

#[cfg(test)]
mod tests;

use tokio::io::AsyncRead;

pub use crate::conditional::ConditionalStore;
pub use crate::error::Error;
#[cfg(any(test, feature = "test-util"))]
pub use crate::memory::MemoryObjectStore;
pub use crate::object::ObjectStore;
pub use crate::presigned::PresignedStore;
pub use crate::types::{ChildrenPage, Etag, ObjectMeta, Page};
pub use crate::upload_session::{
    ByteStream, MultipartUploadPage, PendingMultipartUpload, channel_stream,
};

/// Boxed `AsyncRead` returned by [`ObjectStore::get_stream`]. Matches the
/// shape `blob_store` already uses so future migrations don't need a
/// conversion layer.
pub type BoxedReader = Box<dyn AsyncRead + Unpin + Send + Sync>;
