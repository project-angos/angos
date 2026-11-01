use redis::RedisError;

/// Errors that can occur during cache operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A higher-level execution error (JSON serialization, deserialization, or
    /// other programmer-visible logic failures).
    #[error("cache execution error: {0}")]
    Execution(String),
    /// A Redis protocol or network error, preserving the original source.
    #[error("redis error: {0}")]
    Redis(#[from] RedisError),
}

// `redis::RedisError` does not implement `PartialEq`, so the derive cannot be
// used and the `Redis` variant is excluded from equality: two `RedisError`
// values from separate I/O operations are not meaningfully comparable.
impl PartialEq for Error {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Execution(a), Self::Execution(b)) => a == b,
            _ => false,
        }
    }
}
