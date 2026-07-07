use crate::registry_client;

/// Errors raised while building or running replication machinery.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The outbound registry client failed while talking to a downstream.
    #[error("replication client error: {0}")]
    Client(#[from] registry_client::Error),
    /// A replication-internal failure: namespace mapping, manifest parsing,
    /// serialization, or downstream-configuration resolution.
    #[error("replication error: {0}")]
    Internal(String),
}
