// All variants are named `Invalid*` by design: each names the exact resource
// class that failed validation (digest, namespace, reference, manifest JSON,
// media type, upload session id). The shared prefix is intentional domain
// vocabulary, not an accidental smell.
#[allow(clippy::enum_variant_names)]
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid digest: {0}")]
    InvalidDigest(String),
    #[error("invalid namespace: {0}")]
    InvalidNamespace(String),
    #[error("invalid reference: {0}")]
    InvalidReference(String),
    #[error("invalid manifest JSON: {0}")]
    InvalidManifestJson(#[from] serde_json::Error),
    #[error("invalid media type: {0}")]
    InvalidMediaType(String),
    #[error("invalid upload session id: {0}")]
    InvalidUploadSessionId(String),
}
