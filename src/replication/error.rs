use crate::registry::Error as RegistryError;

/// Errors raised while building or running replication machinery.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// A [`crate::registry::Error`] raised while building the registry client
    /// or touching registry storage.
    #[error("replication registry error: {0}")]
    Registry(#[from] RegistryError),
}
