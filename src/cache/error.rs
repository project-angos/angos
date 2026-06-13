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

// Manual `PartialEq` is required because `redis::RedisError` does not implement
// `PartialEq`, so `#[derive(PartialEq)]` cannot be used on an enum that contains
// it. Tests in `memory.rs` and `redis.rs` compare `Result<Option<String>, Error>`
// against `Ok(...)` values, which requires `Error: PartialEq` to compile. The
// `Redis` variant is intentionally excluded from equality because two distinct
// `RedisError` values produced by separate I/O operations are not meaningfully
// comparable.
impl PartialEq for Error {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Execution(a), Self::Execution(b)) => a == b,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use redis::RedisError;

    use super::*;

    #[test]
    fn test_error_display_execution() {
        let error = Error::Execution("JSON parse failed".to_string());
        assert_eq!(
            format!("{error}"),
            "cache execution error: JSON parse failed"
        );
    }

    #[test]
    fn test_from_redis_error_preserves_source() {
        let redis_err = RedisError::from((redis::ErrorKind::Io, "IO error occurred"));
        let error: Error = redis_err.into();
        assert!(matches!(error, Error::Redis(_)));
        assert!(format!("{error}").contains("IO error occurred"));
        // Verify source is accessible via std::error::Error::source().
        assert!(std::error::Error::source(&error).is_some());
    }
}
