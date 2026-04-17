use std::fmt;

#[derive(Debug, PartialEq)]
pub enum Error {
    InvalidFormat(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::InvalidFormat(s) => write!(f, "Invalid format: {s}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::InvalidFormat(format!("{e}"))
    }
}
