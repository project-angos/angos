use std::{fmt, io};

use argon2::password_hash;

#[derive(Debug, PartialEq)]
pub enum Error {
    Input(String),
    Hashing(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Input(err) | Error::Hashing(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::Input(err.to_string())
    }
}

impl From<password_hash::Error> for Error {
    fn from(err: password_hash::Error) -> Self {
        Error::Hashing(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let error = Error::Input("Some error".to_string());
        assert_eq!(format!("{error}"), "Some error");

        let error = Error::Hashing("Some error".to_string());
        assert_eq!(format!("{error}"), "Some error");
    }

    #[test]
    fn test_from_io_error() {
        let io_error = io::Error::other("IO error occurred");
        let error: Error = io_error.into();
        assert_eq!(error, Error::Input("IO error occurred".to_string()));
    }

    #[test]
    fn test_from_argon2_error() {
        let error: Error = password_hash::Error::Algorithm.into();
        assert_eq!(error, Error::Hashing("unsupported algorithm".to_string()));
    }
}
