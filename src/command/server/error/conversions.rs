use hyper::StatusCode;

use angos_oci::response::ErrorCode;
use angos_oci_client::REPLICATION_SUPERSEDED_CODE;

use crate::{
    auth,
    command::{
        bootstrap,
        server::error::{Error, INTERNAL_ERROR_CODE, RECLAMATION_IN_PROGRESS_CODE},
    },
    configuration, event_webhook,
    jobs::store as job_store,
    metrics_provider, registry,
};

fn oci_error(status_code: StatusCode, code: ErrorCode, msg: Option<String>) -> Error {
    angos_error(status_code, code.as_str(), msg)
}

/// The same body under a code the spec does not define, angos's own extensions.
fn angos_error(status_code: StatusCode, code: &str, msg: Option<String>) -> Error {
    Error::Custom {
        status_code,
        code: code.to_string(),
        msg,
    }
}

impl From<registry::Error> for Error {
    fn from(error: registry::Error) -> Self {
        match error {
            registry::Error::Initialization(msg) => Error::Initialization(msg),
            registry::Error::BlobUnknown => {
                oci_error(StatusCode::NOT_FOUND, ErrorCode::BlobUnknown, None)
            }
            // `405` is what end-10 lists for a refused blob delete, so the
            // reason travels in the message rather than in a status outside
            // that set.
            registry::Error::BlobReferenced => oci_error(
                StatusCode::METHOD_NOT_ALLOWED,
                ErrorCode::Denied,
                Some(error.to_string()),
            ),
            registry::Error::BlobUploadUnknown => {
                oci_error(StatusCode::NOT_FOUND, ErrorCode::BlobUploadUnknown, None)
            }
            registry::Error::DigestInvalid => {
                oci_error(StatusCode::BAD_REQUEST, ErrorCode::DigestInvalid, None)
            }
            registry::Error::ManifestBlobUnknown => {
                oci_error(StatusCode::NOT_FOUND, ErrorCode::ManifestBlobUnknown, None)
            }
            registry::Error::ManifestBodyTooLarge { .. } => oci_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                ErrorCode::ManifestInvalid,
                Some(error.to_string()),
            ),
            registry::Error::BlobBodyTooLarge { .. } => oci_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                ErrorCode::BlobUploadInvalid,
                Some(error.to_string()),
            ),
            registry::Error::ManifestInvalid(msg) => oci_error(
                StatusCode::BAD_REQUEST,
                ErrorCode::ManifestInvalid,
                Some(msg),
            ),
            registry::Error::ManifestUnknown => {
                oci_error(StatusCode::NOT_FOUND, ErrorCode::ManifestUnknown, None)
            }
            registry::Error::NameInvalid => {
                oci_error(StatusCode::BAD_REQUEST, ErrorCode::NameInvalid, None)
            }
            registry::Error::NameUnknown | registry::Error::NotFound => {
                oci_error(StatusCode::NOT_FOUND, ErrorCode::NameUnknown, None)
            }
            registry::Error::Unauthorized(msg) => {
                oci_error(StatusCode::UNAUTHORIZED, ErrorCode::Unauthorized, Some(msg))
            }
            registry::Error::Denied(msg) => {
                oci_error(StatusCode::FORBIDDEN, ErrorCode::Denied, Some(msg))
            }
            registry::Error::Unsupported => {
                oci_error(StatusCode::BAD_REQUEST, ErrorCode::Unsupported, None)
            }
            registry::Error::RangeNotSatisfiable => oci_error(
                StatusCode::RANGE_NOT_SATISFIABLE,
                ErrorCode::SizeInvalid,
                None,
            ),
            // A refused write (an immutable tag, a concurrent-writer CAS
            // conflict) answers 409 under the spec code closest to it.
            registry::Error::Conflict(msg) => {
                oci_error(StatusCode::CONFLICT, ErrorCode::Denied, Some(msg))
            }
            // Transient by construction, since the collector's batch moves on:
            // the client backs off and retries instead of reading a refusal.
            registry::Error::ReclamationInProgress(msg) => angos_error(
                StatusCode::SERVICE_UNAVAILABLE,
                RECLAMATION_IN_PROGRESS_CODE,
                Some(msg),
            ),
            // The one code outside the spec's set: it answers a replication
            // write only, and its sender reads it to settle convergence.
            registry::Error::ReplicationSuperseded(msg) => {
                angos_error(StatusCode::CONFLICT, REPLICATION_SUPERSEDED_CODE, Some(msg))
            }
            registry::Error::EventDelivery(msg) => Error::Execution(msg),
            // Corrupt content is a 500 like any other internal failure; only
            // the reclaim paths inside angos act on the distinction.
            registry::Error::Internal(msg) | registry::Error::Corrupt(msg) => angos_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                INTERNAL_ERROR_CODE,
                Some(msg),
            ),
            // Opaque server-side failures with no client-actionable OCI code,
            // matched exhaustively so a new variant must be mapped here.
            registry::Error::Configuration(_)
            | registry::Error::Cache(_)
            | registry::Error::Io(_)
            | registry::Error::Http(_)
            | registry::Error::Serde(_)
            | registry::Error::InvalidHeader(_) => angos_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                INTERNAL_ERROR_CODE,
                Some(error.to_string()),
            ),
        }
    }
}

impl From<auth::Error> for Error {
    fn from(e: auth::Error) -> Self {
        match e {
            auth::Error::Initialization(msg) => Error::Initialization(msg),
            auth::Error::Execution(msg) => Error::Execution(msg),
            auth::Error::Unauthorized(msg) => Error::Unauthorized(msg),
            auth::Error::ProviderUnavailable(msg) => Error::ProviderUnavailable(msg),
            // A registry error the authorizer surfaced keeps its OCI mapping.
            auth::Error::Registry(inner) => Error::from(*inner),
        }
    }
}

// Every bootstrap failure names what it was initializing in its own message.
impl From<bootstrap::Error> for Error {
    fn from(e: bootstrap::Error) -> Self {
        Error::Initialization(e.to_string())
    }
}

impl From<job_store::Error> for Error {
    fn from(e: job_store::Error) -> Self {
        Error::Initialization(e.to_string())
    }
}

impl From<metrics_provider::Error> for Error {
    fn from(error: metrics_provider::Error) -> Self {
        match error {
            metrics_provider::Error::Initialization(msg) => Error::Initialization(msg),
            metrics_provider::Error::Encode(msg) => Error::Internal(msg),
        }
    }
}

impl From<configuration::Error> for Error {
    fn from(error: configuration::Error) -> Self {
        match error {
            configuration::Error::Initialization(msg)
            | configuration::Error::InvalidFormat(msg)
            | configuration::Error::NotReadable(msg) => Error::Internal(msg),
        }
    }
}

impl From<event_webhook::Error> for Error {
    fn from(error: event_webhook::Error) -> Self {
        match error {
            event_webhook::Error::Initialization(msg) => Error::Initialization(msg),
            event_webhook::Error::Dispatch(msg) => Error::Execution(msg),
        }
    }
}
