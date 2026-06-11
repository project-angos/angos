use crate::registry::Error as RegistryError;

/// Errors raised while building or running replication machinery.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// A required builder field was not set, or a downstream could not be
    /// resolved from its configuration.
    #[error("replication initialization failed: {0}")]
    Initialization(String),

    /// A [`crate::registry::Error`] raised while building the registry client
    /// or touching registry storage.
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
