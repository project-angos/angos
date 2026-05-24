//! Unified storage abstraction shared by `blob_store`, `metadata_store`, and
//! `job_store`. See `doc/storage-convergence.md` for the full plan and the
//! rationale behind the capability-trait split.
//!
//! # Capability traits
//!
//! - [`ObjectStore`] — universal floor: object CRUD, prefix-batch delete,
//!   head, two listing modes (flat-recursive and one-level-children), and
//!   server-side copy. Every backend implements this.
//! - [`ConditionalStore`] — CAS extension: `put_if_absent`, `put_if_match`,
//!   `delete_if_match`. S3 implements this; FS does not (consumers fall
//!   back to a `LockBackend` acquire).
//! - [`MultipartStore`] — S3 multipart-upload protocol. Only S3 implements
//!   this; FS keeps its append-mode-file upload path.
//! - [`PresignedStore`] — signed download URLs. Only S3 implements this.
//!
//! # Backends
//!
//! - [`fs::Backend`] — [`ObjectStore`] on top of `tokio::fs`.
//! - [`s3::Backend`] — [`ObjectStore`] + [`ConditionalStore`] +
//!   [`MultipartStore`] + [`PresignedStore`] wrapping
//!   [`angos_s3_client::Backend`].

mod conditional;
mod error;
mod multipart;
mod object;
mod presigned;
mod types;

pub mod fs;
pub mod s3;

#[cfg(test)]
mod tests;

use tokio::io::AsyncRead;

pub use crate::conditional::ConditionalStore;
pub use crate::error::Error;
pub use crate::multipart::{ByteStream, MultipartStore, channel_stream};
pub use crate::object::ObjectStore;
pub use crate::presigned::PresignedStore;
pub use crate::types::{
    ChildrenPage, Etag, MultipartPage, MultipartUpload, ObjectMeta, Page, Part, UploadId,
};

/// Boxed `AsyncRead` returned by [`ObjectStore::get_stream`]. Matches the
/// shape `blob_store` already uses so future migrations don't need a
/// conversion layer.
pub type BoxedReader = Box<dyn AsyncRead + Unpin + Send + Sync>;
