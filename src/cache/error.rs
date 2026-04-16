use std::fmt;

use redis::RedisError;

#[derive(Debug, PartialEq)]
pub enum Error {
    Backend(String),
    Execution(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Backend(err) | Error::Execution(err) => write!(f, "{err}"),
        }
    }
}

impl From<RedisError> for Error {
    fn from(error: RedisError) -> Self {
        Error::Backend(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use redis::RedisError;

    use super::*;

    #[test]
    fn test_error_display() {
        let error = Error::Backend("Some error".to_string());
        assert_eq!(format!("{error}"), "Some error");

        let error = Error::Execution("Some error".to_string());
        assert_eq!(format!("{error}"), "Some error");
    }

    #[test]
    fn test_from_redis_error() {
        let error = RedisError::from((redis::ErrorKind::Io, "IO error occurred"));
        let error: Error = error.into();
        assert!(matches!(error, Error::Backend(_)));
        assert!(format!("{error}").contains("IO error occurred"));
    }
}
