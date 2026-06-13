use crate::registry::Error as RegistryError;

/// Errors raised while building or running replication machinery.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// A required builder field was not set, or a downstream could not be
    /// resolved from its configuration.
    #[error("replication initialization failed: {0}")]
    Initialization(String),

    /// A [`crate::registry::Error`] raised while building the registry client
    /// or touching registry storage.
    #[error("replication registry error: {0}")]
    Registry(#[from] RegistryError),
}
