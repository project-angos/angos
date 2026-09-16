use crate::{command::bootstrap::Error as BootstrapError, policy, registry};

/// Errors raised by the maintenance machinery shared by `scrub`, `prune`, and
/// `replicate`. `Display` stays command-neutral: the logging call sites and the
/// top-level command handlers add the attribution.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A setup failure, or one whose source type is none of the typed variants
    /// below.
    #[error("initialization failed: {0}")]
    Initialization(String),

    /// A replication-reconcile failure raised mid-run, so it must not borrow
    /// the `Initialization` prefix.
    #[error("replication error: {0}")]
    Replication(String),

    /// A durable-queue failure raised while scrubbing orphan jobs.
    #[error("job queue error: {0}")]
    JobQueue(String),

    #[error("registry error: {0}")]
    Registry(#[from] registry::Error),

    #[error("cache error: {0}")]
    Cache(#[from] angos_cache::Error),
}

impl From<policy::Error> for Error {
    fn from(e: policy::Error) -> Self {
        Error::Initialization(e.to_string())
    }
}

impl From<BootstrapError> for Error {
    fn from(e: BootstrapError) -> Self {
        match e {
            BootstrapError::Cache(inner) => Error::Cache(inner),
            BootstrapError::Registry(inner) => Error::from(inner),
            other => Error::Initialization(other.to_string()),
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
        let inner = angos_cache::Error::Execution("connection refused".to_string());
        let error: Error = inner.into();
        assert!(matches!(error, Error::Cache(_)));
        assert!(error.source().is_some());
    }
}
