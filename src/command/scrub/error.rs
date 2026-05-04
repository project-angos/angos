use crate::{
    cache,
    registry::{blob_store, metadata_store},
};

/// Errors that can occur during a scrub run.
///
/// The `Initialization` string variant is retained for call sites where the
/// source error type is not one of the typed variants below, or where the call
/// site adds contextual information (e.g. a repository name) that is not
/// present in the source error.
///
/// ## Display prefix choice
///
/// The original implementation emitted bare strings with no context prefix,
/// making it impossible to distinguish a scrub error's origin in log output.
/// The new `#[error("...")]` attributes add a minimal prefix so that log lines
/// are unambiguously attributable.
///
/// ## `blob_store::Error` and `#[from]`
///
/// `blob_store::Error` does not implement `std::error::Error`, which is a
/// prerequisite for `#[from]` / `#[source]` in thiserror.  A manual `From`
/// impl is therefore provided instead; `source()` cannot chain into
/// `blob_store::Error` until that type is migrated.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("scrub initialization failed: {0}")]
    Initialization(String),

    /// Wraps a `metadata_store::Error` with source preserved.
    #[error("scrub metadata store error: {0}")]
    MetadataStore(#[from] metadata_store::Error),

    /// Wraps a `blob_store::Error`.
    ///
    /// `blob_store::Error` does not implement `std::error::Error`, so `source()`
    /// cannot chain into it; a manual `From` impl is used instead of `#[from]`.
    #[error("scrub blob store error: {0}")]
    BlobStore(blob_store::Error),

    /// Wraps a `cache::Error` with source preserved.
    #[error("scrub cache error: {0}")]
    Cache(#[from] cache::Error),
}

impl From<blob_store::Error> for Error {
    fn from(e: blob_store::Error) -> Self {
        Error::BlobStore(e)
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as StdError;

    use super::*;

    #[test]
    fn initialization_display_includes_prefix() {
        let error = Error::Initialization("Some init error".to_string());
        assert_eq!(
            format!("{error}"),
            "scrub initialization failed: Some init error"
        );
    }

    #[test]
    fn metadata_store_variant_preserves_source() {
        let inner = metadata_store::Error::ReferenceNotFound;
        let error = Error::MetadataStore(inner);
        assert!(
            error.source().is_some(),
            "MetadataStore variant must expose its source via Error::source()"
        );
        assert!(matches!(error, Error::MetadataStore(_)));
    }

    #[test]
    fn blob_store_from_conversion() {
        let inner = blob_store::Error::BlobNotFound;
        let error: Error = inner.into();
        assert!(matches!(error, Error::BlobStore(_)));
    }

    #[test]
    fn cache_from_conversion() {
        let inner = cache::Error::Execution("connection refused".to_string());
        let error: Error = inner.into();
        assert!(matches!(error, Error::Cache(_)));
        assert!(error.source().is_some());
    }
}
