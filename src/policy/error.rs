use std::fmt;

#[derive(Debug, PartialEq)]
pub enum Error {
    Initialization(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Initialization(s) => write!(f, "Policy initialization error: {s}"),
        }
    }
}
