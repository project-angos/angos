use std::io;

#[cfg(feature = "s3")]
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

    /// A conditional operation (`put_if_absent`, `put_if_match`,
    /// `delete_if_match`) was rejected because the precondition was not met.
    #[error("precondition failed")]
    PreconditionFailed,

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

#[cfg(feature = "s3")]
impl From<S3Error> for Error {
    fn from(error: S3Error) -> Self {
        match error {
            S3Error::NotFound(_) => Error::NotFound,
            S3Error::PreconditionFailed => Error::PreconditionFailed,
            other => Error::Backend(other.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::error::Error;

    #[test]
    fn not_found_display() {
        assert_eq!(Error::NotFound.to_string(), "object not found");
    }

    #[test]
    fn precondition_failed_display() {
        assert_eq!(Error::PreconditionFailed.to_string(), "precondition failed");
    }

    #[test]
    fn backend_display_includes_message() {
        let e = Error::Backend("connection reset".to_string());
        assert_eq!(e.to_string(), "storage backend error: connection reset");
    }
}
