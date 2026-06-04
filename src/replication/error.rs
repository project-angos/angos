use crate::registry::Error as RegistryError;

/// Errors raised while building or running replication-specific machinery
/// (downstream runtime types, the push pipeline, the job handler).
///
/// The `Initialization` string variant is used by the builder for missing
/// required fields, mirroring the sibling builders' use of
/// [`crate::registry::Error::Initialization`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// A required builder field was not set, or a downstream could not be
    /// resolved from its configuration.
    #[error("replication initialization failed: {0}")]
    Initialization(String),

    /// Wraps a [`crate::registry::Error`] raised while building the underlying
    /// [`crate::registry_client::RegistryClient`] or touching registry storage.
    #[error("replication registry error: {0}")]
    Registry(#[from] RegistryError),
}

#[cfg(test)]
mod tests {
    use std::error::Error as StdError;

    use crate::registry::Error as RegistryError;
    use crate::replication::Error;

    #[test]
    fn initialization_display_includes_prefix() {
        let error = Error::Initialization("missing name".to_string());
        assert_eq!(
            format!("{error}"),
            "replication initialization failed: missing name"
        );
    }

    #[test]
    fn registry_variant_preserves_source() {
        let inner = RegistryError::Initialization("boom".to_string());
        let error: Error = inner.into();
        assert!(matches!(error, Error::Registry(_)));
        assert!(StdError::source(&error).is_some());
    }
}
