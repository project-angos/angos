use cel_interpreter::SerializationError;
use hyper::{header::InvalidHeaderValue, http::uri::InvalidUri};

use angos_tx_engine::lock;

use crate::{
    configuration, oci, policy,
    registry::{blob_store, cache, job_store, metadata_store},
};

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("{0}")]
    Initialization(String),
    #[error("blob unknown to registry")]
    BlobUnknown,
    #[error("blob is still referenced")]
    BlobReferenced,
    #[error("blob upload unknown to registry")]
    BlobUploadUnknown,
    #[error("provided digest did not match uploaded content")]
    DigestInvalid,
    #[error("manifest references a blob unknown to registry")]
    ManifestBlobUnknown,
    #[error("manifest body exceeds supported size limit of {limit} bytes")]
    ManifestBodyTooLarge { limit: usize },
    #[error("manifest invalid: {0}")]
    ManifestInvalid(String),
    #[error("manifest unknown to registry")]
    ManifestUnknown,
    #[error("invalid repository name")]
    NameInvalid,
    #[error("repository name not known to registry")]
    NameUnknown,
    #[error("{0}")]
    Unauthorized(String),
    #[error("{0}")]
    Denied(String),
    #[error("the operation is unsupported")]
    Unsupported,
    #[error("range not satisfiable")]
    RangeNotSatisfiable,
    #[error("resource not found")]
    NotFound,
    #[error("{0}")]
    Conflict(String),
    /// A replication write lost last-writer-wins: the local tag is strictly
    /// newer than the incoming `source_ts`. Distinct from [`Error::Conflict`]
    /// so the sender can treat it as convergence rather than retry.
    #[error("{0}")]
    ReplicationSuperseded(String),
    #[error("internal server error: {0}")]
    Internal(String),

    // Typed variants that preserve the source error chain.
    #[error("configuration error during operations: {0}")]
    Configuration(#[from] configuration::Error),
    #[error("cache error during operations: {0}")]
    Cache(#[from] cache::Error),
    #[error("metadata store error during operations: {0}")]
    MetadataStore(#[from] metadata_store::Error),
    // `lock::Error` is routed through `MetadataStore`: every registry-level
    // caller of the lock primitive is doing metadata-store work, so the
    // metadata-store variant is the right home.
    #[error("I/O error during operations: {0}")]
    Io(#[from] std::io::Error),
    #[error("HTTP error during operations: {0}")]
    Http(#[from] hyper::http::Error),
    #[error("(de)serialization error during operations: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("policy evaluation error: {0}")]
    PolicyExecution(#[from] cel_interpreter::ExecutionError),
    #[error("invalid header value: {0}")]
    InvalidHeader(#[from] InvalidHeaderValue),
    #[error("invalid URI: {0}")]
    InvalidUri(#[from] InvalidUri),
    #[error("serialization error during operations: {0}")]
    Serialization(#[from] SerializationError),
}

// `policy::Error` routes to `Initialization` to preserve the prior behaviour.
// A `#[from]` variant is not used here because the mapping is semantic, not
// structural: all policy errors collapse into the string-carrying `Initialization`.
impl From<lock::Error> for Error {
    fn from(error: lock::Error) -> Self {
        Error::MetadataStore(error.into())
    }
}

impl From<policy::Error> for Error {
    fn from(error: policy::Error) -> Self {
        Error::Initialization(error.to_string())
    }
}

// `job_store::Error` routes by variant: a missing record is a `404`, every
// other failure (storage, initialisation) is an opaque `500`.
impl From<job_store::Error> for Error {
    fn from(error: job_store::Error) -> Self {
        match error {
            job_store::Error::NotFound => Error::NotFound,
            other => Error::Internal(other.to_string()),
        }
    }
}

// `blob_store::Error` requires variant-level routing, so the manual `From` is
// retained.  The catch-all arm produces `Internal` because `blob_store::Error`
// does not implement `std::error::Error`, preventing source chain attachment.
impl From<blob_store::Error> for Error {
    fn from(error: blob_store::Error) -> Self {
        match error {
            blob_store::Error::UploadNotFound => Error::BlobUploadUnknown,
            blob_store::Error::BlobNotFound => Error::BlobUnknown,
            blob_store::Error::ReferenceNotFound => Error::ManifestBlobUnknown,
            blob_store::Error::UploadBodyRead(_) | blob_store::Error::UploadBodySize { .. } => {
                Error::RangeNotSatisfiable
            }
            _ => Error::Internal(format!("Data store error during operations: {error}")),
        }
    }
}

// `oci::Error` routes to `NameInvalid` — preserve that semantic.
impl From<oci::Error> for Error {
    fn from(_: oci::Error) -> Self {
        Error::NameInvalid
    }
}

// `x509_parser::error::X509Error` routes to `Unauthorized` — preserve that
// semantic.
impl From<x509_parser::error::X509Error> for Error {
    fn from(_: x509_parser::error::X509Error) -> Self {
        Error::Unauthorized("Invalid client certificate".to_string())
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as StdError;

    use super::*;

    #[test]
    fn from_io_error_preserves_source() {
        let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "permission denied");
        let err: Error = io_err.into();
        assert!(matches!(err, Error::Io(_)));
        assert!(StdError::source(&err).is_some());
        assert!(err.to_string().contains("I/O error during operations"));
    }

    #[test]
    fn from_serde_json_preserves_source() {
        let serde_err = serde_json::from_str::<serde_json::Value>("{invalid}").unwrap_err();
        let err: Error = serde_err.into();
        assert!(matches!(err, Error::Serde(_)));
        assert!(StdError::source(&err).is_some());
        assert!(
            err.to_string()
                .contains("(de)serialization error during operations")
        );
    }

    #[test]
    fn from_metadata_store_preserves_source() {
        let meta_err = metadata_store::Error::ReferenceNotFound;
        let err: Error = meta_err.into();
        assert!(matches!(err, Error::MetadataStore(_)));
        assert!(StdError::source(&err).is_some());
        assert!(
            err.to_string()
                .contains("metadata store error during operations")
        );
    }

    #[test]
    fn from_configuration_preserves_source() {
        let config_err = configuration::Error::Initialization("cfg failed".to_string());
        let err: Error = config_err.into();
        assert!(matches!(err, Error::Configuration(_)));
        assert!(StdError::source(&err).is_some());
        assert!(
            err.to_string()
                .contains("configuration error during operations")
        );
    }

    #[test]
    fn from_cache_preserves_source() {
        let cache_err = cache::Error::Execution("cache miss".to_string());
        let err: Error = cache_err.into();
        assert!(matches!(err, Error::Cache(_)));
        assert!(StdError::source(&err).is_some());
        assert!(err.to_string().contains("cache error during operations"));
    }

    #[test]
    fn from_blob_store_routes_upload_not_found() {
        let err: Error = blob_store::Error::UploadNotFound.into();
        assert!(matches!(err, Error::BlobUploadUnknown));
    }

    #[test]
    fn from_blob_store_routes_blob_not_found() {
        let err: Error = blob_store::Error::BlobNotFound.into();
        assert!(matches!(err, Error::BlobUnknown));
    }

    #[test]
    fn from_blob_store_routes_reference_not_found() {
        let err: Error = blob_store::Error::ReferenceNotFound.into();
        assert!(matches!(err, Error::ManifestBlobUnknown));
    }

    #[test]
    fn from_blob_store_routes_upload_body_errors() {
        let read_err: Error = blob_store::Error::UploadBodyRead("reset".to_string()).into();
        assert!(matches!(read_err, Error::RangeNotSatisfiable));

        let size_err: Error = blob_store::Error::UploadBodySize {
            expected: 10,
            actual: 8,
        }
        .into();
        assert!(matches!(size_err, Error::RangeNotSatisfiable));
    }

    #[test]
    fn from_blob_store_catch_all_produces_internal() {
        let err: Error = blob_store::Error::StorageBackend("backend down".to_string()).into();
        assert!(matches!(err, Error::Internal(_)));
        assert!(
            err.to_string()
                .contains("Data store error during operations")
        );
    }

    #[test]
    fn from_oci_error_routes_to_name_invalid() {
        let err: Error = oci::Error::InvalidDigest("bad digest".to_string()).into();
        assert!(matches!(err, Error::NameInvalid));
    }

    #[test]
    fn from_policy_error_routes_to_initialization() {
        let err: Error = policy::Error::Evaluation("eval failed".to_string()).into();
        assert!(matches!(err, Error::Initialization(_)));
        assert!(err.to_string().contains("eval failed"));
    }

    #[test]
    fn from_x509_error_routes_to_unauthorized() {
        use x509_parser::error::X509Error;
        let err: Error = X509Error::InvalidCertificate.into();
        assert!(matches!(err, Error::Unauthorized(_)));
        assert!(err.to_string().contains("Invalid client certificate"));
    }
}
