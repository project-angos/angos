use angos_tx_engine::lock;

use crate::{
    cache,
    command::bootstrap::Error as BootstrapError,
    policy,
    registry::{self, blob_store, metadata_store},
};

/// Errors shared by the `scrub`, `policy`, and `replication` subcommands.
///
/// The `Initialization` string variant is retained for call sites where the
/// source error type is not one of the typed variants below, or where the call
/// site adds contextual information (e.g. a repository name) that is not
/// present in the source error.
///
/// ## Display prefix choice
///
/// Each variant carries a neutral cause prefix and no command-specific word,
/// because the same type surfaces under `scrub`, `policy`, and `replication`.
/// The command label is supplied by `main.rs`, which wraps every failure with
/// its own per-command prefix (`Scrub error:`, `Policy error:`,
/// `Replication error:`), yielding lines such as
/// `Policy error: metadata store error: ...`.
///
/// ## `blob_store::Error` and `#[from]`
///
/// `blob_store::Error` does not implement `std::error::Error`, which is a
/// prerequisite for `#[from]` / `#[source]` in thiserror.  A manual `From`
/// impl is therefore provided instead; `source()` cannot chain into
/// `blob_store::Error` until that type is migrated.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("initialization failed: {0}")]
    Initialization(String),

    /// A replication-reconcile failure (envelope build or durable-queue enqueue)
    /// raised mid-run, so it must not borrow the `Initialization` prefix.
    #[error("replication error: {0}")]
    Replication(String),

    /// A durable-queue failure (list, read, or delete) raised while scrubbing
    /// orphan jobs on the replication or cache queue.
    #[error("job queue error: {0}")]
    JobQueue(String),

    /// Wraps a `metadata_store::Error` with source preserved.
    #[error("metadata store error: {0}")]
    MetadataStore(#[from] metadata_store::Error),

    /// Wraps a `blob_store::Error`.
    ///
    /// `blob_store::Error` does not implement `std::error::Error`, so `source()`
    /// cannot chain into it; a manual `From` impl is used instead of `#[from]`.
    #[error("blob store error: {0}")]
    BlobStore(blob_store::Error),

    /// Wraps a `cache::Error` with source preserved.
    #[error("cache error: {0}")]
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
        assert_eq!(format!("{error}"), "initialization failed: Some init error");
    }

    #[test]
    fn replication_display_includes_prefix() {
        let error = Error::Replication("failed to enqueue replication job".to_string());
        assert_eq!(
            format!("{error}"),
            "replication error: failed to enqueue replication job"
        );
    }

    #[test]
    fn job_queue_display_includes_prefix() {
        let error = Error::JobQueue("failed to list cache jobs".to_string());
        assert_eq!(
            format!("{error}"),
            "job queue error: failed to list cache jobs"
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
