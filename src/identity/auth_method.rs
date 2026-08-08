/// How a client authenticated, as the authenticator classified it.
///
/// A request can satisfy several methods at once, so the authenticator picks
/// the strongest and states it once; the trace span and the denial audit log
/// both report that one answer rather than each deciding for itself.
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

#[cfg(test)]
mod tests {
    use super::AuthMethod;

    /// The labels reach operators through the request span and the audit log,
    /// so they are an output contract, not an internal name.
    #[test]
    fn every_method_keeps_its_logged_label() {
        assert_eq!(AuthMethod::Mtls.as_str(), "mtls");
        assert_eq!(AuthMethod::Token.as_str(), "token");
        assert_eq!(AuthMethod::Oidc.as_str(), "oidc");
        assert_eq!(AuthMethod::Basic.as_str(), "basic");
        assert_eq!(AuthMethod::Anonymous.as_str(), "anonymous");
        assert_eq!(AuthMethod::default(), AuthMethod::Anonymous);
    }
}
