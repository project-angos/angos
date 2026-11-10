/// The strongest method that authenticated a request, picked once so the trace
/// span and the denial audit log agree.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum AuthMethod {
    Mtls,
    Token,
    Oidc,
    Basic,
    /// No credential authenticated the request.
    #[default]
    Anonymous,
}

impl AuthMethod {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Mtls => "mtls",
            Self::Token => "token",
            Self::Oidc => "oidc",
            Self::Basic => "basic",
            Self::Anonymous => "anonymous",
        }
    }
}
