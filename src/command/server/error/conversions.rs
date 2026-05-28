use hyper::StatusCode;

use super::Error;
use crate::{
    command::bootstrap,
    configuration::{self, registry_storage},
    event_webhook, registry,
    registry::blob_store,
};

fn oci_error(status_code: StatusCode, code: &'static str, msg: Option<String>) -> Error {
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
            registry::Error::BlobUnknown => oci_error(StatusCode::NOT_FOUND, "BLOB_UNKNOWN", None),
            registry::Error::BlobReferenced => oci_error(
                StatusCode::METHOD_NOT_ALLOWED,
                "DENIED",
                Some(error.to_string()),
            ),
            registry::Error::BlobUploadUnknown => {
                oci_error(StatusCode::NOT_FOUND, "BLOB_UPLOAD_UNKNOWN", None)
            }
            registry::Error::DigestInvalid => {
                oci_error(StatusCode::BAD_REQUEST, "DIGEST_INVALID", None)
            }
            registry::Error::ManifestBlobUnknown => {
                oci_error(StatusCode::NOT_FOUND, "MANIFEST_BLOB_UNKNOWN", None)
            }
            registry::Error::ManifestBodyTooLarge { .. } => oci_error(
                StatusCode::BAD_REQUEST,
                "MANIFEST_INVALID",
                Some(error.to_string()),
            ),
            registry::Error::ManifestInvalid(msg) => {
                oci_error(StatusCode::BAD_REQUEST, "MANIFEST_INVALID", Some(msg))
            }
            registry::Error::ManifestUnknown => {
                oci_error(StatusCode::NOT_FOUND, "MANIFEST_UNKNOWN", None)
            }
            registry::Error::NameInvalid => {
                oci_error(StatusCode::BAD_REQUEST, "NAME_INVALID", None)
            }
            registry::Error::NameUnknown => oci_error(StatusCode::NOT_FOUND, "NAME_UNKNOWN", None),
            registry::Error::Unauthorized(msg) => {
                oci_error(StatusCode::UNAUTHORIZED, "UNAUTHORIZED", Some(msg))
            }
            registry::Error::Denied(msg) => oci_error(StatusCode::FORBIDDEN, "DENIED", Some(msg)),
            registry::Error::Unsupported => oci_error(StatusCode::BAD_REQUEST, "UNSUPPORTED", None),
            registry::Error::RangeNotSatisfiable => {
                oci_error(StatusCode::RANGE_NOT_SATISFIABLE, "SIZE_INVALID", None)
            }
            registry::Error::Internal(msg) => oci_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "INTERNAL_ERROR",
                Some(msg),
            ),
            _ => oci_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "INTERNAL_ERROR",
                Some(error.to_string()),
            ),
        }
    }
}

impl From<bootstrap::Error> for Error {
    fn from(e: bootstrap::Error) -> Self {
        match e {
            bootstrap::Error::BlobStore(_) => {
                Error::Initialization("Failed to initialize blob store".to_string())
            }
            bootstrap::Error::MetadataStore(inner) => {
                Error::Initialization(format!("Failed to initialize metadata store: {inner}"))
            }
            bootstrap::Error::RegistryStorage(inner) => {
                Error::Initialization(format!("Failed to initialize storage handles: {inner}"))
            }
            bootstrap::Error::Cache(inner) => {
                Error::Initialization(format!("Failed to initialize auth token cache: {inner}"))
            }
            bootstrap::Error::Repository { name, source } => Error::Initialization(format!(
                "Failed to initialize repository '{name}': {source}"
            )),
            bootstrap::Error::Overlap(inner) => Error::Initialization(inner.to_string()),
            bootstrap::Error::JobQueue(inner) => {
                Error::Initialization(format!("Failed to initialize job queue: {inner}"))
            }
        }
    }
}

impl From<registry_storage::Error> for Error {
    fn from(e: registry_storage::Error) -> Self {
        Error::Initialization(e.to_string())
    }
}

impl From<blob_store::Error> for Error {
    fn from(_: blob_store::Error) -> Self {
        Error::Initialization("Failed to initialize blob store".to_string())
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
