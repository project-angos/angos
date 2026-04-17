use std::fmt;

#[derive(Debug, PartialEq)]
pub enum Error {
    Initialization(String),
    Execution(String),
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Initialization(err) | Error::Execution(err) => write!(f, "{err}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let error = Error::Initialization("Some init error".to_string());
        assert_eq!(format!("{error}"), "Some init error");

        let error = Error::Execution("Some init error".to_string());
        assert_eq!(format!("{error}"), "Some init error");
    }
}
