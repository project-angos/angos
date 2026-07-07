use crate::registry;

/// The auth subsystem's error vocabulary. Converted once into the HTTP
/// server's error at the boundary (`command::server::error::conversions`),
/// like every other subsystem; auth itself never names HTTP types.
///
/// The `Display` strings match the server error's variants one-to-one so the
/// boundary conversion changes the type, never the message.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Initialization(String),
    #[error("{0}")]
    Execution(String),
    #[error("Unauthorized: {0}")]
    Unauthorized(String),
    #[error("Conflict: {0}")]
    Conflict(String),
    #[error("Provider unavailable: {0}")]
    ProviderUnavailable(String),
    /// A registry lookup the authorizer performs (e.g. mount-source
    /// enumeration) failed; carried whole so the server boundary maps it to
    /// its OCI code exactly as if the registry had been called directly.
    /// Boxed because `registry::Error` dwarfs the string variants.
    #[error(transparent)]
    Registry(Box<registry::Error>),
}

impl From<registry::Error> for Error {
    fn from(e: registry::Error) -> Self {
        Error::Registry(Box::new(e))
    }
}
