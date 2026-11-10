use std::error::Error as StdError;
use std::{fmt, io};

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_io_error_keeps_not_found_apart() {
        let missing: Error = io::Error::new(io::ErrorKind::NotFound, "missing").into();
        assert!(matches!(missing, Error::NotFound(_)));
        let denied: Error = io::Error::new(io::ErrorKind::PermissionDenied, "denied").into();
        assert!(matches!(denied, Error::Io(_)));
    }
}
