use std::fmt;

use crate::{oci, s3_client};

#[derive(Debug, PartialEq)]
pub enum Error {
    DataStore(s3_client::Error),
    Lock(String),
    InvalidData(String),
    StorageBackend(String),
    ReferenceNotFound,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::DataStore(err) => write!(f, "Data store error: {err}"),
            Error::Lock(msg) => write!(f, "Lock error: {msg}"),
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
            s3_client::Error::PreconditionFailed => Error::Lock("Precondition failed".to_string()),
            _ => Error::DataStore(err),
        }
    }
}

impl From<redis::RedisError> for Error {
    fn from(err: redis::RedisError) -> Self {
        Error::Lock(format!("Redis error: {err}"))
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
