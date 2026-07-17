use crate::{command::bootstrap::Error as BootstrapError, registry};

/// Errors raised by `angos migrate`. Each `Display` carries a `migrate ...`
/// prefix so log lines are unambiguously attributable.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("migrate initialization failed: {0}")]
    Initialization(String),

    /// Wraps a `registry::Error` (the blob-store / metadata-store domain error)
    /// raised while listing, reading, or rewriting link objects.
    #[error("migrate registry error: {0}")]
    Registry(#[from] registry::Error),
}

impl From<BootstrapError> for Error {
    fn from(e: BootstrapError) -> Self {
        Error::Initialization(e.to_string())
    }
}
