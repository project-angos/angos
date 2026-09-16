/// Errors raised while building or running replication machinery.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The outbound registry client failed while talking to a downstream.
    #[error("replication client error: {0}")]
    Client(#[from] angos_oci_client::Error),
    /// A manifest body that will never become valid: it does not parse, or it
    /// contradicts the media type recorded for it. Terminal, so the job
    /// dead-letters instead of spending its retry budget on the same bytes.
    #[error("invalid manifest content: {0}")]
    InvalidManifest(String),
    /// A replication-internal failure: namespace mapping, serialization, or
    /// downstream-configuration resolution.
    #[error("replication error: {0}")]
    Internal(String),
}
