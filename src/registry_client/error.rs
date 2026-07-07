/// Errors from the outbound registry client, one variant per remote outcome
/// the callers distinguish. Converted into `registry::Error` variant for
/// variant where the pull-through path surfaces a remote miss as its local
/// OCI code.
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
    #[error("{0}")]
    Internal(String),
}
