use angos_cache::Error as CacheError;

use crate::command::bootstrap::Error as BootstrapError;
use crate::policy::Error as PolicyError;
use crate::registry::Error as RegistryError;

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
    Registry(#[from] RegistryError),

    #[error("cache error: {0}")]
    Cache(#[from] CacheError),
}

impl From<PolicyError> for Error {
    fn from(e: PolicyError) -> Self {
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
