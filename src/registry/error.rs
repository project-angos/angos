use std::{
    cmp::PartialEq,
    fmt::{Debug, Display},
};

use cel_interpreter::SerializationError;
use hyper::{header::InvalidHeaderValue, http::uri::InvalidUri};
use tracing::{debug, warn};

use crate::{
    configuration, oci,
    registry::{blob_store, cache, metadata_store, task_queue},
};

#[derive(Debug, Clone, PartialEq)]
pub enum Error {
    Initialization(String),
    BlobUnknown,
    BlobUploadUnknown,
    DigestInvalid,
    ManifestBlobUnknown,
    ManifestInvalid(String),
    ManifestUnknown,
    NameInvalid,
    NameUnknown,
    Unauthorized(String),
    Denied(String),
    Unsupported,
    RangeNotSatisfiable,
    Internal(String),
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::BlobUnknown => write!(f, "blob unknown to registry"),
            Error::BlobUploadUnknown => write!(f, "blob upload unknown to registry"),
            Error::DigestInvalid => write!(f, "provided digest did not match uploaded content"),
            Error::ManifestBlobUnknown => {
                write!(f, "manifest references a blob unknown to registry")
            }
            Error::ManifestInvalid(s) => write!(f, "manifest invalid: {s}"),
            Error::ManifestUnknown => write!(f, "manifest unknown to registry"),
            Error::NameInvalid => write!(f, "invalid repository name"),
            Error::NameUnknown => write!(f, "repository name not known to registry"),
            Error::Initialization(s) | Error::Unauthorized(s) | Error::Denied(s) => {
                write!(f, "{s}")
            }
            Error::Unsupported => write!(f, "the operation is unsupported"),
            Error::RangeNotSatisfiable => write!(f, "range not satisfiable"),
            Error::Internal(s) => write!(f, "internal server error: {s}"),
        }
    }
}

impl From<configuration::Error> for Error {
    fn from(error: configuration::Error) -> Self {
        warn!("Configuration error: {error}");
        Error::Internal("Configuration error during operations".to_string())
    }
}

impl From<cache::Error> for Error {
    fn from(error: cache::Error) -> Self {
        warn!("Cache error: {error}");
        Error::Internal("Cache error during operations".to_string())
    }
}

impl From<oci::Error> for Error {
    fn from(error: oci::Error) -> Self {
        warn!("OCI error: {error}");
        Error::NameInvalid
    }
}

impl From<blob_store::Error> for Error {
    fn from(error: blob_store::Error) -> Self {
        match error {
            blob_store::Error::UploadNotFound => Error::BlobUploadUnknown,
            blob_store::Error::BlobNotFound => Error::BlobUnknown,
            blob_store::Error::ReferenceNotFound => Error::ManifestBlobUnknown,
            _ => {
                warn!("Data store error: {error}");
                Error::Internal("Data store error during operations".to_string())
            }
        }
    }
}

impl From<metadata_store::Error> for Error {
    fn from(error: metadata_store::Error) -> Self {
        warn!("Metadata store error: {error}");
        Error::Internal("Metadata store error during operations".to_string())
    }
}

impl From<task_queue::Error> for Error {
    fn from(error: task_queue::Error) -> Self {
        warn!("Task pool error: {error}");
        Error::Internal("Task pool error during operations".to_string())
    }
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        debug!("Error: {error}");
        Error::Internal("I/O error during operations".to_string())
    }
}

impl From<hyper::http::Error> for Error {
    fn from(error: hyper::http::Error) -> Self {
        debug!("Hyper HTTP error: {error}");
        Error::Internal("HTTP error during operations".to_string())
    }
}

// XXX: at least repository_upstream is using this error type
// usage should be reviewed and replaced by more specific errors
impl From<serde_json::Error> for Error {
    fn from(error: serde_json::Error) -> Self {
        debug!("Serde JSON error: {error}");
        Error::Internal("(De)Serialization error during operations".to_string())
    }
}

impl From<cel_interpreter::ExecutionError> for Error {
    fn from(error: cel_interpreter::ExecutionError) -> Self {
        debug!("CEL error: {error}");
        Error::Internal("Policy evaluation error".to_string())
    }
}

impl From<Box<dyn std::error::Error>> for Error {
    fn from(error: Box<dyn std::error::Error>) -> Self {
        debug!("STD Error: {error}");
        Error::Internal("Error during operations".to_string())
    }
}

impl From<InvalidHeaderValue> for Error {
    fn from(error: InvalidHeaderValue) -> Self {
        debug!("Invalid header value: {error}");
        Error::Internal("Invalid header value".to_string())
    }
}

impl From<InvalidUri> for Error {
    fn from(error: InvalidUri) -> Self {
        debug!("Invalid URI: {error}");
        Error::Internal("Invalid URI".to_string())
    }
}

impl From<SerializationError> for Error {
    fn from(error: SerializationError) -> Self {
        debug!("Serialization error: {error}");
        Error::Internal("Serialization error during operations".to_string())
    }
}

impl From<x509_parser::error::X509Error> for Error {
    fn from(error: x509_parser::error::X509Error) -> Self {
        debug!("X509 parsing error: {error}");
        Error::Unauthorized("Invalid client certificate".to_string())
    }
}
