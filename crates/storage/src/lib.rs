//! Unified storage abstraction shared by `blob_store`, `metadata_store`, and
//! `job_store`.
//!
//! # Capability traits
//!
//! - [`ObjectStore`] — universal floor: object CRUD, prefix-batch delete,
//!   head, two listing modes (flat-recursive and one-level-children), and
//!   server-side copy. Every backend implements this.
//! - [`ConditionalStore`] — CAS extension: `put_if_absent`, `put_if_match`,
//!   `delete_if_match`. S3 implements this; FS does not (consumers fall
//!   back to the transactional engine's `Lock` primitive via `with_lock`).
//! - [`UploadSessionStore`] — resumable streaming upload primitive used by
//!   the blob store. FS opens an append-mode file; S3 wraps its native
//!   multipart-upload protocol. Blob-store callers never deal with the S3
//!   multipart wire details (upload IDs, parts).
//! - [`PresignedStore`] — signed download URLs. Only S3 implements this.
//!
//! # Backends
//!
//! - [`fs::Backend`] — [`ObjectStore`] + [`UploadSessionStore`] on top of
//!   `tokio::fs`.
//! - [`s3::Backend`] — [`ObjectStore`] + [`ConditionalStore`] +
//!   [`UploadSessionStore`] + [`PresignedStore`] wrapping
//!   [`angos_s3_client::Backend`].

mod conditional;
mod error;
mod memory;
mod object;
mod presigned;
mod types;
mod upload_session;

pub mod fs;
pub mod s3;

#[cfg(test)]
mod tests;

use tokio::io::AsyncRead;

pub use crate::conditional::ConditionalStore;
pub use crate::error::Error;
pub use crate::memory::MemoryObjectStore;
pub use crate::object::ObjectStore;
pub use crate::presigned::PresignedStore;
pub use crate::types::{ChildrenPage, Etag, ObjectMeta, Page};
pub use crate::upload_session::{
    ByteStream, Part, SessionState, UploadSession, UploadSessionStore, channel_stream,
};

/// Boxed `AsyncRead` returned by [`ObjectStore::get_stream`]. Matches the
/// shape `blob_store` already uses so future migrations don't need a
/// conversion layer.
pub type BoxedReader = Box<dyn AsyncRead + Unpin + Send + Sync>;
