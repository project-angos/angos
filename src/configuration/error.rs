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
            Error::Initialization(err) | Error::InvalidFormat(err) | Error::NotReadable(err) => {
                write!(f, "{err}")
            }
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
        assert_eq!(format!("{error}"), "Some error");

        let error = Error::InvalidFormat("Some error".to_string());
        assert_eq!(format!("{error}"), "Some error");

        let error = Error::NotReadable("Some error".to_string());
        assert_eq!(format!("{error}"), "Some error");
    }

    #[test]
    fn test_from_notify_error() {
        let notify_error = notify::Error::generic("Generic error");
        let error: Error = notify_error.into();

        assert_eq!(format!("{error}"), "Watcher error: Generic error");
        assert_eq!(
            error,
            Error::NotReadable("Watcher error: Generic error".to_string())
        );
    }
}
