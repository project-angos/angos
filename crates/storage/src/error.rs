use std::io;

use angos_s3_client::Error as S3Error;
use thiserror::Error as ThisError;

/// Storage-level failures only; consumers map them to their domain errors.
#[derive(Clone, Debug, PartialEq, ThisError)]
pub enum Error {
    #[error("object not found")]
    NotFound,

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
