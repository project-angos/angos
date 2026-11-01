#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid digest: {0}")]
    Digest(String),
    #[error("invalid namespace: {0}")]
    Namespace(String),
    #[error("invalid reference: {0}")]
    Reference(String),
    #[error("invalid manifest JSON: {0}")]
    ManifestJson(#[from] serde_json::Error),
    #[error("invalid manifest: {0}")]
    Manifest(String),
    #[error("invalid media range: {0}")]
    MediaRange(String),
    #[error("invalid media type: {0}")]
    MediaType(String),
    #[error("invalid upload session id: {0}")]
    UploadSessionId(String),
}
