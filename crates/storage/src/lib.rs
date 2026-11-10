//! The object-storage abstraction shared by the blob, metadata and job stores:
//! [`ObjectStore`] on [`fs::Backend`] and [`s3::Backend`], plus
//! [`PresignedStore`] on S3 alone.

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
pub use crate::upload_session::{ByteStream, MultipartUploadPage, PendingMultipartUpload};

/// Boxed `AsyncRead` returned by [`ObjectStore::get_stream`].
pub type BoxedReader = Box<dyn AsyncRead + Unpin + Send + Sync>;
