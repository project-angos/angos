/// Errors from the outbound registry client, one variant per remote outcome
/// the callers distinguish. Each converts to its `registry::Error`
/// counterpart, which is how the pull-through path surfaces a remote miss as
/// its own OCI code.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Initialization(String),
    #[error("{0}")]
    Unauthorized(String),
    #[error("{0}")]
    Denied(String),
    #[error("blob unknown to the remote registry")]
    BlobUnknown,
    #[error("manifest unknown to the remote registry")]
    ManifestUnknown,
    #[error("manifest body exceeds supported size limit of {limit} bytes")]
    ManifestBodyTooLarge { limit: usize },
    #[error("the operation is unsupported by the remote registry")]
    Unsupported,
    #[error("the remote registry cannot satisfy the requested range")]
    RangeNotSatisfiable,
    #[error("{0}")]
    Internal(String),
}
