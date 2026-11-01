#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("initialization error: {0}")]
    Initialization(String),
    #[error("invalid configuration format: {0}")]
    InvalidFormat(String),
    #[error("unable to read configuration: {0}")]
    NotReadable(String),
}

// Hand-written rather than `#[from]` to keep the "Watcher error: " prefix.
impl From<notify::Error> for Error {
    fn from(err: notify::Error) -> Self {
        Error::NotReadable(format!("Watcher error: {err}"))
    }
}
