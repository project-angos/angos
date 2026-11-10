/// The scheme a request was served on, set by the listener as a request
/// extension: HTTP/1.1 origin-form targets carry no scheme.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestScheme {
    Http,
    Https,
}

impl RequestScheme {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Https => "https",
        }
    }
}
