#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("initialization error: {0}")]
    Initialization(String),
    #[error("invalid configuration format: {0}")]
    InvalidFormat(String),
    #[error("unable to read configuration: {0}")]
    NotReadable(String),
}

// `notify::Error` is wrapped with a "Watcher error: " prefix to preserve context,
// so a bare `#[from]` variant is not used here, the message-building `From` impl
// is kept instead.
impl From<notify::Error> for Error {
    fn from(err: notify::Error) -> Self {
        Error::NotReadable(format!("Watcher error: {err}"))
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
        assert!(matches!(error, Error::NotReadable(_)));
    }
}
