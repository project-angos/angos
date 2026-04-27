use std::fmt;

#[derive(Debug, PartialEq)]
pub enum Error {
    Initialization(String),
    InvalidFormat(String),
    NotReadable(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Initialization(err) => write!(f, "initialization error: {err}"),
            Error::InvalidFormat(err) => write!(f, "invalid configuration format: {err}"),
            Error::NotReadable(err) => write!(f, "unable to read configuration: {err}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<notify::Error> for Error {
    fn from(err: notify::Error) -> Self {
        let msg = format!("Watcher error: {err}");
        Error::NotReadable(msg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let error = Error::Initialization("Some error".to_string());
        assert_eq!(format!("{error}"), "initialization error: Some error");

        let error = Error::InvalidFormat("Some error".to_string());
        assert_eq!(
            format!("{error}"),
            "invalid configuration format: Some error"
        );

        let error = Error::NotReadable("Some error".to_string());
        assert_eq!(
            format!("{error}"),
            "unable to read configuration: Some error"
        );
    }

    #[test]
    fn test_from_notify_error() {
        let notify_error = notify::Error::generic("Generic error");
        let error: Error = notify_error.into();

        assert_eq!(
            format!("{error}"),
            "unable to read configuration: Watcher error: Generic error"
        );
        assert_eq!(
            error,
            Error::NotReadable("Watcher error: Generic error".to_string())
        );
    }
}
