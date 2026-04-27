use std::fmt;

#[derive(Debug, PartialEq)]
pub enum Error {
    Evaluation(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Evaluation(s) => write!(f, "Policy evaluation error: {s}"),
        }
    }
}
