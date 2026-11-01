use std::io::Error as IoError;
use std::num::TryFromIntError;
use std::string::FromUtf8Error;

use chrono::format::ParseError as ChronoParseError;
use http::StatusCode;
use serde_json::Error as SerdeJsonError;
use sha2::digest::common::hazmat::DeserializeStateError;
use tracing::warn;
use x509_parser::error::X509Error;

use angos_cache::Error as CacheError;
use angos_oci::Error as OciError;
use angos_oci::http_range::Error as HttpRangeError;
use angos_oci::response::{
    ErrorCode, INTERNAL_ERROR_CODE, RECLAMATION_IN_PROGRESS_CODE, REPLICATION_SUPERSEDED_CODE,
    UPSTREAM_UNAVAILABLE_CODE,
};
use angos_oci_client::Error as ClientError;
use angos_storage::Error as StorageError;

use crate::configuration::Error as ConfigurationError;
use crate::jobs::store::Error as JobStoreError;
use crate::policy::Error as PolicyError;

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
    /// No pull-through upstream gave an answer: each was unreachable, failed,
    /// or answered unusably, and none said the content is absent. Distinct
    /// from a miss so an outage does not read as deleted content, and so a
    /// cached copy can be served in its place.
    #[error("upstream unavailable: {0}")]
    UpstreamUnavailable(String),
    /// A replication write lost last-writer-wins: the local tag is strictly
    /// newer than the incoming `source_ts`. Carries a distinct OCI code so the
    /// sender treats it as convergence rather than a conflict to retry.
    #[error("{0}")]
    ReplicationSuperseded(String),
    /// A `required`-policy webhook rejected or failed the event delivery; the
    /// operation itself has already been written when this surfaces.
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
    Configuration(#[from] ConfigurationError),
    #[error("cache error during operations: {0}")]
    Cache(#[from] CacheError),
    #[error("I/O error during operations: {0}")]
    Io(#[from] IoError),
    #[error("(de)serialization error during operations: {0}")]
    Serde(#[from] SerdeJsonError),
}

impl Error {
    /// How this outcome answers on the distribution API: its status, its
    /// error code and the message worth sending. A 4xx code must come from the
    /// spec's set, so each case picks the closest of those.
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn oci_answer(&self) -> (StatusCode, &'static str, Option<String>) {
        match self {
            Error::BlobUnknown => (StatusCode::NOT_FOUND, ErrorCode::BlobUnknown.as_str(), None),
            // `405` is what end-10 lists for a refused blob delete, so the
            // reason travels in the message rather than in a status outside
            // that set.
            Error::BlobReferenced => (
                StatusCode::METHOD_NOT_ALLOWED,
                ErrorCode::Denied.as_str(),
                Some(self.to_string()),
            ),
            Error::BlobUploadUnknown => (
                StatusCode::NOT_FOUND,
                ErrorCode::BlobUploadUnknown.as_str(),
                None,
            ),
            Error::DigestInvalid => (
                StatusCode::BAD_REQUEST,
                ErrorCode::DigestInvalid.as_str(),
                None,
            ),
            Error::ManifestBlobUnknown => (
                StatusCode::NOT_FOUND,
                ErrorCode::ManifestBlobUnknown.as_str(),
                None,
            ),
            Error::ManifestBodyTooLarge { .. } => (
                StatusCode::PAYLOAD_TOO_LARGE,
                ErrorCode::ManifestInvalid.as_str(),
                Some(self.to_string()),
            ),
            Error::BlobBodyTooLarge { .. } => (
                StatusCode::PAYLOAD_TOO_LARGE,
                ErrorCode::BlobUploadInvalid.as_str(),
                Some(self.to_string()),
            ),
            Error::ManifestInvalid(msg) => (
                StatusCode::BAD_REQUEST,
                ErrorCode::ManifestInvalid.as_str(),
                Some(msg.clone()),
            ),
            Error::ManifestUnknown => (
                StatusCode::NOT_FOUND,
                ErrorCode::ManifestUnknown.as_str(),
                None,
            ),
            Error::NameInvalid => (
                StatusCode::BAD_REQUEST,
                ErrorCode::NameInvalid.as_str(),
                None,
            ),
            Error::NameUnknown | Error::NotFound => {
                (StatusCode::NOT_FOUND, ErrorCode::NameUnknown.as_str(), None)
            }
            Error::Unauthorized(msg) => (
                StatusCode::UNAUTHORIZED,
                ErrorCode::Unauthorized.as_str(),
                Some(msg.clone()),
            ),
            Error::Denied(msg) => (
                StatusCode::FORBIDDEN,
                ErrorCode::Denied.as_str(),
                Some(msg.clone()),
            ),
            Error::Unsupported => (
                StatusCode::BAD_REQUEST,
                ErrorCode::Unsupported.as_str(),
                None,
            ),
            Error::RangeNotSatisfiable => (
                StatusCode::RANGE_NOT_SATISFIABLE,
                ErrorCode::SizeInvalid.as_str(),
                None,
            ),
            // A refused write (an immutable tag, a concurrent-writer CAS
            // conflict) answers 409 under the spec code closest to it.
            Error::Conflict(msg) => (
                StatusCode::CONFLICT,
                ErrorCode::Denied.as_str(),
                Some(msg.clone()),
            ),
            // Transient by construction, since the collector's batch moves on:
            // the client backs off and retries instead of reading a refusal.
            Error::ReclamationInProgress(msg) => (
                StatusCode::SERVICE_UNAVAILABLE,
                RECLAMATION_IN_PROGRESS_CODE,
                Some(msg.clone()),
            ),
            // Transient too: the client retries once the upstream is back,
            // instead of reading the outage as a missing image.
            Error::UpstreamUnavailable(msg) => (
                StatusCode::SERVICE_UNAVAILABLE,
                UPSTREAM_UNAVAILABLE_CODE,
                Some(msg.clone()),
            ),
            Error::ReplicationSuperseded(msg) => (
                StatusCode::CONFLICT,
                REPLICATION_SUPERSEDED_CODE,
                Some(msg.clone()),
            ),
            // Corrupt content is a 500 like any other internal failure; only
            // the reclaim paths inside angos act on the distinction.
            Error::Initialization(msg)
            | Error::EventDelivery(msg)
            | Error::Internal(msg)
            | Error::Corrupt(msg) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                INTERNAL_ERROR_CODE,
                Some(msg.clone()),
            ),
            Error::Configuration(_) | Error::Cache(_) | Error::Io(_) | Error::Serde(_) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                INTERNAL_ERROR_CODE,
                Some(self.to_string()),
            ),
        }
    }
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

impl From<ChronoParseError> for Error {
    fn from(error: ChronoParseError) -> Self {
        Error::Corrupt(error.to_string())
    }
}

// Every policy error collapses into the string-carrying `Initialization`, a
// semantic mapping `#[from]` could not express.
impl From<PolicyError> for Error {
    fn from(error: PolicyError) -> Self {
        Error::Initialization(error.to_string())
    }
}

// A missing job record is a `404`; every other failure is an opaque `500`.
impl From<JobStoreError> for Error {
    fn from(error: JobStoreError) -> Self {
        match error {
            JobStoreError::NotFound => Error::NotFound,
            other => Error::Internal(other.to_string()),
        }
    }
}

// Variant for variant, so the pull-through path surfaces a remote miss as the
// matching local OCI code.
impl From<ClientError> for Error {
    fn from(error: ClientError) -> Self {
        match error {
            ClientError::Initialization(msg) => Error::Initialization(msg),
            ClientError::Unauthorized(msg) => Error::Unauthorized(msg),
            ClientError::Denied(msg) => Error::Denied(msg),
            ClientError::BlobUnknown => Error::BlobUnknown,
            ClientError::ManifestUnknown => Error::ManifestUnknown,
            ClientError::ManifestBodyTooLarge { limit } => Error::ManifestBodyTooLarge { limit },
            ClientError::Unsupported => Error::Unsupported,
            ClientError::RangeNotSatisfiable => Error::RangeNotSatisfiable,
            ClientError::Internal(msg) => Error::Internal(msg),
        }
    }
}

// A malformed range and an unservable one are both a 416 to the client.
impl From<HttpRangeError> for Error {
    fn from(_: HttpRangeError) -> Self {
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

impl From<X509Error> for Error {
    fn from(_: X509Error) -> Self {
        Error::Unauthorized("Invalid client certificate".to_string())
    }
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;

    use super::*;

    #[test]
    fn from_storage_not_found_routes_to_not_found() {
        let err: Error = StorageError::NotFound.into();
        assert!(matches!(err, Error::NotFound));
    }

    #[test]
    fn from_oci_error_routes_to_name_invalid() {
        let err: Error = OciError::Digest("bad digest".to_string()).into();
        assert!(matches!(err, Error::NameInvalid));
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
