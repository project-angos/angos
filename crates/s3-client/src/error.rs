use std::error::Error as StdError;
use std::{fmt, io};

use crate::circuit_breaker::CircuitBreakerError;

#[derive(Debug, Clone, PartialEq)]
pub enum Error {
    Configuration(String),
    Io(String),
    NotFound(String),
    PreconditionFailed,
    /// The backend answered with a refusal no retry can change: a denied
    /// action, a malformed request, a rejected checksum. Separate from
    /// [`Error::Io`] because the backend is healthy, so a run of these must
    /// not open the circuit breaker.
    Rejected(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Configuration(msg) => write!(f, "Configuration error: {msg}"),
            Error::Io(e) => write!(f, "IO error: {e}"),
            Error::NotFound(e) => write!(f, "Not found: {e}"),
            Error::PreconditionFailed => write!(f, "Precondition failed"),
            Error::Rejected(e) => write!(f, "Rejected: {e}"),
        }
    }
}

impl StdError for Error {}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        if err.kind() == io::ErrorKind::NotFound {
            Error::NotFound(err.to_string())
        } else {
            Error::Io(err.to_string())
        }
    }
}

impl From<CircuitBreakerError> for Error {
    fn from(err: CircuitBreakerError) -> Self {
        Error::Io(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_io_error_not_found() {
        let io_err = io::Error::new(io::ErrorKind::NotFound, "missing file");
        let err: Error = io_err.into();
        assert!(matches!(err, Error::NotFound(_)));
    }

    #[test]
    fn test_from_io_error_other() {
        let io_err = io::Error::new(io::ErrorKind::PermissionDenied, "access denied");
        let err: Error = io_err.into();
        assert!(matches!(err, Error::Io(_)));
    }
}
