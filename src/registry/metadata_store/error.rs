use std::fmt;

use angos_tx_engine::lock;

use crate::oci;
use angos_s3_client as s3_client;
use angos_storage::Error as StorageError;

#[derive(Debug, PartialEq)]
pub enum Error {
    DataStore(s3_client::Error),
    Coordination(String),
    InvalidData(String),
    StorageBackend(String),
    ReferenceNotFound,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::DataStore(err) => write!(f, "Data store error: {err}"),
            Error::Coordination(msg) => write!(f, "Coordination error: {msg}"),
            Error::InvalidData(msg) => write!(f, "Invalid data: {msg}"),
            Error::StorageBackend(msg) => write!(f, "Storage backend error: {msg}"),
            Error::ReferenceNotFound => write!(f, "Reference not found"),
        }
    }
}

impl std::error::Error for Error {}

impl From<s3_client::Error> for Error {
    fn from(err: s3_client::Error) -> Self {
        match err {
            s3_client::Error::NotFound(_) => Error::ReferenceNotFound,
            s3_client::Error::PreconditionFailed => {
                Error::Coordination("Precondition failed".to_string())
            }
            _ => Error::DataStore(err),
        }
    }
}

impl From<StorageError> for Error {
    fn from(err: StorageError) -> Self {
        match err {
            StorageError::NotFound => Error::ReferenceNotFound,
            StorageError::PreconditionFailed => {
                Error::Coordination("Precondition failed".to_string())
            }
            other @ StorageError::Backend(_) => Error::StorageBackend(other.to_string()),
        }
    }
}

impl From<redis::RedisError> for Error {
    fn from(err: redis::RedisError) -> Self {
        Error::Coordination(format!("Redis error: {err}"))
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::InvalidData(err.to_string())
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        if err.kind() == std::io::ErrorKind::NotFound {
            Error::ReferenceNotFound
        } else {
            Error::StorageBackend(err.to_string())
        }
    }
}

impl From<oci::Error> for Error {
    fn from(err: oci::Error) -> Self {
        Error::InvalidData(err.to_string())
    }
}

impl From<lock::Error> for Error {
    fn from(err: lock::Error) -> Self {
        match err {
            lock::Error::Lock(msg) => Error::Coordination(msg),
            lock::Error::InvalidData(msg) => Error::InvalidData(msg),
            lock::Error::StorageBackend(msg) => Error::StorageBackend(msg),
            lock::Error::NotFound => Error::ReferenceNotFound,
            invalidated @ lock::Error::Invalidated => Error::Coordination(invalidated.to_string()),
        }
    }
}
