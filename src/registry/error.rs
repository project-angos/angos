use std::{num::TryFromIntError, string::FromUtf8Error};

use http::header::InvalidHeaderValue;
use sha2::digest::common::hazmat::DeserializeStateError;
use tracing::warn;

use angos_oci::{Error as OciError, http_range};
use angos_storage::Error as StorageError;

use crate::{configuration, jobs::store as job_store, policy};

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
    #[error("blob body exceeds supported size limit of {limit} bytes")]
    BlobBodyTooLarge { limit: usize },
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
    /// A concurrent writer won the compare-and-set and the retry budget ran
    /// out. Distinct from [`Self::ReplicationSuperseded`], which is a
    /// last-writer-wins convergence rather than a retry-conflict.
    #[error("{0}")]
    Conflict(String),
    /// A collector run covers a blob the write references; transient, the
    /// client retries after the collector's batch moves on.
    #[error("reclamation in progress: {0}")]
    ReclamationInProgress(String),
    /// A replication write lost last-writer-wins: the local tag is strictly
    /// newer than the incoming `source_ts`. Carries a distinct OCI code so the
    /// sender treats it as convergence rather than a conflict to retry.
    #[error("{0}")]
    ReplicationSuperseded(String),
    /// A `required`-policy webhook rejected or failed the event delivery; the
    /// operation itself has already committed when this surfaces.
    #[error("event delivery failed: {0}")]
    EventDelivery(String),
    #[error("internal server error: {0}")]
    Internal(String),
    /// Stored content that does not decode. Distinct from [`Self::Internal`]
    /// because re-reading returns the same bytes, so a caller reclaiming broken
    /// state can act on it instead of retrying.
    #[error("corrupt stored data: {0}")]
    Corrupt(String),

    // Typed variants preserving the source error chain.
    #[error("configuration error during operations: {0}")]
    Configuration(#[from] configuration::Error),
    #[error("cache error during operations: {0}")]
    Cache(#[from] angos_cache::Error),
    #[error("I/O error during operations: {0}")]
    Io(#[from] std::io::Error),
    #[error("HTTP error during operations: {0}")]
    Http(#[from] http::Error),
    #[error("(de)serialization error during operations: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("invalid header value: {0}")]
    InvalidHeader(#[from] InvalidHeaderValue),
}

// A raw storage outcome carries no domain context: a call site that knows a
// miss means a specific blob/upload/manifest 404 must intercept
// `StorageError::NotFound` before `?` reaches this impl.
impl From<StorageError> for Error {
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::NotFound => Error::NotFound,
            StorageError::Backend(msg) => Error::Internal(msg),
        }
    }
}

// A decode failure is the content, not the read, so it is permanent.
impl From<DeserializeStateError> for Error {
    fn from(error: DeserializeStateError) -> Self {
        Error::Corrupt(format!("hash state deserialization error: {error}"))
    }
}

impl From<FromUtf8Error> for Error {
    fn from(error: FromUtf8Error) -> Self {
        Error::Corrupt(error.to_string())
    }
}

impl From<TryFromIntError> for Error {
    fn from(error: TryFromIntError) -> Self {
        Error::Internal(error.to_string())
    }
}

impl From<chrono::format::ParseError> for Error {
    fn from(error: chrono::format::ParseError) -> Self {
        Error::Corrupt(error.to_string())
    }
}

// Every policy error collapses into the string-carrying `Initialization`, a
// semantic mapping `#[from]` could not express.
impl From<policy::Error> for Error {
    fn from(error: policy::Error) -> Self {
        Error::Initialization(error.to_string())
    }
}

// A missing job record is a `404`; every other failure is an opaque `500`.
impl From<job_store::Error> for Error {
    fn from(error: job_store::Error) -> Self {
        match error {
            job_store::Error::NotFound => Error::NotFound,
            other => Error::Internal(other.to_string()),
        }
    }
}

// Variant for variant, so the pull-through path surfaces a remote miss as the
// matching local OCI code.
impl From<angos_oci_client::Error> for Error {
    fn from(error: angos_oci_client::Error) -> Self {
        match error {
            angos_oci_client::Error::Initialization(msg) => Error::Initialization(msg),
            angos_oci_client::Error::Unauthorized(msg) => Error::Unauthorized(msg),
            angos_oci_client::Error::Denied(msg) => Error::Denied(msg),
            angos_oci_client::Error::BlobUnknown => Error::BlobUnknown,
            angos_oci_client::Error::ManifestUnknown => Error::ManifestUnknown,
            angos_oci_client::Error::ManifestBodyTooLarge { limit } => {
                Error::ManifestBodyTooLarge { limit }
            }
            angos_oci_client::Error::Unsupported => Error::Unsupported,
            angos_oci_client::Error::RangeNotSatisfiable => Error::RangeNotSatisfiable,
            angos_oci_client::Error::Internal(msg) => Error::Internal(msg),
        }
    }
}

// A malformed range and an unservable one are both a 416 to the client.
impl From<http_range::Error> for Error {
    fn from(_: http_range::Error) -> Self {
        Error::RangeNotSatisfiable
    }
}

impl From<OciError> for Error {
    fn from(_: OciError) -> Self {
        Error::NameInvalid
    }
}

impl Error {
    /// The code the OCI spec gives a malformed manifest. The blanket
    /// `From<OciError>` answers `NameInvalid` instead, so every manifest body
    /// must go through here.
    pub fn manifest_invalid(error: &OciError) -> Self {
        warn!("Rejecting manifest: {error}");
        Error::ManifestInvalid(error.to_string())
    }
}

impl From<x509_parser::error::X509Error> for Error {
    fn from(_: x509_parser::error::X509Error) -> Self {
        Error::Unauthorized("Invalid client certificate".to_string())
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as StdError;

    use chrono::DateTime;
    use x509_parser::error::X509Error;

    use crate::registry::error::*;

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
    fn from_storage_not_found_routes_to_not_found() {
        let err: Error = StorageError::NotFound.into();
        assert!(matches!(err, Error::NotFound));
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
        let cache_err = angos_cache::Error::Execution("cache miss".to_string());
        let err: Error = cache_err.into();
        assert!(matches!(err, Error::Cache(_)));
        assert!(StdError::source(&err).is_some());
        assert!(err.to_string().contains("cache error during operations"));
    }

    #[test]
    fn from_oci_error_routes_to_name_invalid() {
        let err: Error = OciError::InvalidDigest("bad digest".to_string()).into();
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
        let err: Error = X509Error::InvalidCertificate.into();
        assert!(matches!(err, Error::Unauthorized(_)));
        assert!(err.to_string().contains("Invalid client certificate"));
    }

    /// The reclaim paths key off `Corrupt` to tell undecodable stored bytes
    /// from a transient read.
    #[test]
    fn decoding_stored_bytes_fails_as_corrupt() {
        let not_utf8 = String::from_utf8(vec![0xff, 0xfe]).unwrap_err();
        assert!(matches!(Error::from(not_utf8), Error::Corrupt(_)));

        let not_rfc3339 = DateTime::parse_from_rfc3339("not-a-timestamp").unwrap_err();
        assert!(matches!(Error::from(not_rfc3339), Error::Corrupt(_)));
    }

    /// A backend read that failed is transient, and must stay distinct from
    /// corrupt content or a reclaim would delete a live upload.
    #[test]
    fn a_backend_read_failure_stays_internal() {
        let error = Error::from(StorageError::Backend("connection reset".to_string()));
        assert!(matches!(error, Error::Internal(_)));
    }
}
