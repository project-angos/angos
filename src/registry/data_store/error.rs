use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub enum Error {
    Configuration(String),
    Io(String),
    Serialization(String),
    NotFound(String),
    PreconditionFailed,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Configuration(msg) => write!(f, "Configuration error: {msg}"),
            Error::Io(e) => write!(f, "IO error: {e}"),
            Error::Serialization(e) => write!(f, "Serialization error: {e}"),
            Error::NotFound(e) => write!(f, "Not found: {e}"),
            Error::PreconditionFailed => write!(f, "Precondition failed"),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        if err.kind() == std::io::ErrorKind::NotFound {
            Error::NotFound(err.to_string())
        } else {
            Error::Io(err.to_string())
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::Serialization(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use std::io;

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
            format!("{}", Error::Serialization("invalid JSON".to_string())),
            "Serialization error: invalid JSON"
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

    #[test]
    fn test_from_serde_json_error() {
        let json_err = serde_json::from_str::<serde_json::Value>("{invalid}").unwrap_err();
        let err: Error = json_err.into();
        assert!(matches!(err, Error::Serialization(_)));
    }
}
