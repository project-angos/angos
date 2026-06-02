use std::{fmt, io};

#[derive(Debug, Clone, PartialEq)]
pub enum Error {
    Configuration(String),
    Io(String),
    NotFound(String),
    PreconditionFailed,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Configuration(msg) => write!(f, "Configuration error: {msg}"),
            Error::Io(e) => write!(f, "IO error: {e}"),
            Error::NotFound(e) => write!(f, "Not found: {e}"),
            Error::PreconditionFailed => write!(f, "Precondition failed"),
        }
    }
}

impl std::error::Error for Error {}

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
    fn test_display() {
        assert_eq!(
            format!("{}", Error::Configuration("invalid config".to_string())),
            "Configuration error: invalid config"
        );
        assert_eq!(
            format!("{}", Error::Io("disk full".to_string())),
            "IO error: disk full"
        );
        assert_eq!(
            format!("{}", Error::NotFound("file.txt".to_string())),
            "Not found: file.txt"
        );
        assert_eq!(
            format!("{}", Error::PreconditionFailed),
            "Precondition failed"
        );
    }

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
