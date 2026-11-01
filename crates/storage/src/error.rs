use std::io;

use angos_s3_client::Error as S3Error;
use thiserror::Error as ThisError;

/// Errors produced by the storage layer.
///
/// `storage::Error` carries only the storage-level cases. Domain semantics
/// (`BlobNotFound`, `ReferenceNotFound`, etc.) live in each consuming module's
/// own `Error` type and are produced by `From<storage::Error>` conversions
/// once the consumer applies its domain knowledge.
#[derive(Clone, Debug, PartialEq, ThisError)]
pub enum Error {
    /// The requested object key does not exist.
    #[error("object not found")]
    NotFound,

    /// The backend reported an error that does not map onto a typed variant.
    /// Carries the backend's own error message.
    #[error("storage backend error: {0}")]
    Backend(String),
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        if error.kind() == io::ErrorKind::NotFound {
            Error::NotFound
        } else {
            Error::Backend(error.to_string())
        }
    }
}

impl From<S3Error> for Error {
    fn from(error: S3Error) -> Self {
        match error {
            S3Error::NotFound(_) => Error::NotFound,
            other => Error::Backend(other.to_string()),
        }
    }
}
