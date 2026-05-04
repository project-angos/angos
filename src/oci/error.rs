// All variants are named `Invalid*` by design: each names the exact resource
// class that failed validation (digest, namespace, reference, manifest JSON).
// The shared prefix is intentional domain vocabulary, not an accidental smell.
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
}

#[cfg(test)]
mod tests {
    use std::error::Error as StdError;

    use super::*;

    #[test]
    fn invalid_manifest_json_preserves_source() {
        let json_err = serde_json::from_str::<serde_json::Value>("{not valid").unwrap_err();
        let err: Error = json_err.into();
        assert!(matches!(err, Error::InvalidManifestJson(_)));
        assert!(StdError::source(&err).is_some());
    }
}
