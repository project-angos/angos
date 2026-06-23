use angos_tx_engine::lock;

use crate::{
    cache,
    command::bootstrap::Error as BootstrapError,
    policy,
    registry::{self, blob_store, metadata_store},
};

/// Errors that can occur during a scrub run.
///
/// The `Initialization` string variant is retained for call sites where the
/// source error type is not one of the typed variants below, or where the call
/// site adds contextual information (e.g. a repository name) that is not
/// present in the source error.
///
/// ## Display prefix choice
///
/// The original implementation emitted bare strings with no context prefix,
/// making it impossible to distinguish a scrub error's origin in log output.
/// The new `#[error("...")]` attributes add a minimal prefix so that log lines
/// are unambiguously attributable.
///
/// ## `blob_store::Error` and `#[from]`
///
/// `blob_store::Error` does not implement `std::error::Error`, which is a
/// prerequisite for `#[from]` / `#[source]` in thiserror.  A manual `From`
/// impl is therefore provided instead; `source()` cannot chain into
/// `blob_store::Error` until that type is migrated.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("scrub initialization failed: {0}")]
    Initialization(String),

    /// A replication-reconcile failure (envelope build or durable-queue enqueue)
    /// raised mid-run, so it must not borrow the `Initialization` prefix.
    #[error("scrub replication error: {0}")]
    Replication(String),

    /// A durable-queue failure (list, read, or delete) raised while scrubbing
    /// orphan jobs on the replication or cache queue.
    #[error("scrub job queue error: {0}")]
    JobQueue(String),

    /// Wraps a `metadata_store::Error` with source preserved.
    #[error("scrub metadata store error: {0}")]
    MetadataStore(#[from] metadata_store::Error),

    /// Wraps a `blob_store::Error`.
    ///
    /// `blob_store::Error` does not implement `std::error::Error`, so `source()`
    /// cannot chain into it; a manual `From` impl is used instead of `#[from]`.
    #[error("scrub blob store error: {0}")]
    BlobStore(blob_store::Error),

    /// Wraps a `cache::Error` with source preserved.
    #[error("scrub cache error: {0}")]
    Cache(#[from] cache::Error),
}

impl From<blob_store::Error> for Error {
    fn from(e: blob_store::Error) -> Self {
        Error::BlobStore(e)
    }
}

impl From<policy::Error> for Error {
    fn from(e: policy::Error) -> Self {
        Error::Initialization(e.to_string())
    }
}

impl From<lock::Error> for Error {
    fn from(e: lock::Error) -> Self {
        Error::MetadataStore(e.into())
    }
}

impl From<registry::Error> for Error {
    fn from(e: registry::Error) -> Self {
        match e {
            registry::Error::MetadataStore(inner) => Error::MetadataStore(inner),
            other => Error::Initialization(other.to_string()),
        }
    }
}

impl From<BootstrapError> for Error {
    fn from(e: BootstrapError) -> Self {
        match e {
            BootstrapError::BlobStore(inner) => Error::BlobStore(inner),
            BootstrapError::MetadataStore(inner) => Error::MetadataStore(inner),
            BootstrapError::Cache(inner) => Error::Cache(inner),
            BootstrapError::Repository { name, source } => Error::Initialization(format!(
                "Failed to initialize repository '{name}': {source}"
            )),
            BootstrapError::Overlap(inner) => Error::Initialization(inner.to_string()),
            BootstrapError::JobQueue(inner) => Error::Initialization(inner.to_string()),
            BootstrapError::RegistryStorage(inner) => Error::Initialization(inner.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as StdError;

    use super::*;

    #[test]
    fn initialization_display_includes_prefix() {
        let error = Error::Initialization("Some init error".to_string());
        assert_eq!(
            format!("{error}"),
            "scrub initialization failed: Some init error"
        );
    }

    #[test]
    fn replication_display_includes_prefix() {
        let error = Error::Replication("failed to enqueue replication job".to_string());
        assert_eq!(
            format!("{error}"),
            "scrub replication error: failed to enqueue replication job"
        );
    }

    #[test]
    fn job_queue_display_includes_prefix() {
        let error = Error::JobQueue("failed to list cache jobs".to_string());
        assert_eq!(
            format!("{error}"),
            "scrub job queue error: failed to list cache jobs"
        );
    }

    #[test]
    fn blob_store_from_conversion() {
        let inner = blob_store::Error::BlobNotFound;
        let error: Error = inner.into();
        assert!(matches!(error, Error::BlobStore(_)));
    }

    #[test]
    fn cache_from_conversion() {
        let inner = cache::Error::Execution("connection refused".to_string());
        let error: Error = inner.into();
        assert!(matches!(error, Error::Cache(_)));
        assert!(error.source().is_some());
    }
}
