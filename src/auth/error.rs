use crate::registry;

/// Converted into the server error at `command::server::error::conversions`;
/// the `Display` strings match its variants so the conversion keeps the message.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Initialization(String),
    #[error("{0}")]
    Execution(String),
    #[error("Unauthorized: {0}")]
    Unauthorized(String),
    #[error("Provider unavailable: {0}")]
    ProviderUnavailable(String),
    /// A registry lookup by the authorizer failed, carried whole so it maps to
    /// the same OCI code as a direct registry call.
    #[error(transparent)]
    Registry(Box<registry::Error>),
}

impl From<registry::Error> for Error {
    fn from(e: registry::Error) -> Self {
        Error::Registry(Box::new(e))
    }
}
