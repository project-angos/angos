use std::fmt;

use crate::{oci, registry::data_store};

#[derive(Debug, PartialEq)]
pub enum Error {
    DataStore(data_store::Error),
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

impl From<data_store::Error> for Error {
    fn from(err: data_store::Error) -> Self {
        match err {
            data_store::Error::NotFound(_) => Error::ReferenceNotFound,
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
