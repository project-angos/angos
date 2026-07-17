use angos_tx_engine::lock;

use crate::{cache, command::bootstrap::Error as BootstrapError, policy, registry};

/// Errors raised by the maintenance machinery shared between `scrub` and
/// `prune`. `Display` stays command-neutral: the logging call sites and the
/// top-level command handlers add the attribution.
///
/// The `Initialization` string variant covers call sites where the source
/// error type is not one of the typed variants below, or where the call site
/// adds contextual information (e.g. a repository name) that is not present
/// in the source error.
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

    /// Wraps a `registry::Error` (the single blob-store / metadata-store domain
    /// error) with source preserved.
    #[error("registry error: {0}")]
    Registry(#[from] registry::Error),

    /// Wraps a `cache::Error` with source preserved.
    #[error("cache error: {0}")]
    Cache(#[from] cache::Error),
}

impl From<policy::Error> for Error {
    fn from(e: policy::Error) -> Self {
        Error::Initialization(e.to_string())
    }
}

impl From<lock::Error> for Error {
    fn from(e: lock::Error) -> Self {
        Error::Registry(e.into())
    }
}

impl From<BootstrapError> for Error {
    fn from(e: BootstrapError) -> Self {
        match e {
            BootstrapError::Cache(inner) => Error::Cache(inner),
            BootstrapError::Repository { name, source } => Error::Initialization(format!(
                "Failed to initialize repository '{name}': {source}"
            )),
            BootstrapError::Overlap(inner) => Error::Initialization(inner.to_string()),
            BootstrapError::JobQueue(inner) => Error::Initialization(inner.to_string()),
            BootstrapError::StorageBackend(inner) | BootstrapError::Coordination(inner) => {
                Error::Initialization(inner)
            }
            BootstrapError::EventWebhook(inner) => Error::Initialization(inner.to_string()),
            BootstrapError::Registry(inner) => Error::from(inner),
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
    fn registry_from_conversion() {
        let inner = registry::Error::BlobUnknown;
        let error: Error = inner.into();
        assert!(matches!(error, Error::Registry(_)));
        assert!(error.source().is_some());
    }

    #[test]
    fn cache_from_conversion() {
        let inner = cache::Error::Execution("connection refused".to_string());
        let error: Error = inner.into();
        assert!(matches!(error, Error::Cache(_)));
        assert!(error.source().is_some());
    }
}
