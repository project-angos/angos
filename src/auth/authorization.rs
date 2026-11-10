//! `Authorization` header parsing shared by the authentication middlewares.

use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use http::HeaderMap;
use http::header::AUTHORIZATION;

/// The token of a `Bearer` authorization header, if one is present.
pub fn bearer_token(headers: &HeaderMap) -> Option<String> {
    authorization_parameter(headers, "Bearer").map(str::to_string)
}

/// The decoded `(username, password)` of a well-formed `Basic` authorization
/// header; `None` when the header is absent, malformed, or another scheme.
pub fn basic_credentials(headers: &HeaderMap) -> Option<(String, String)> {
    let value = authorization_parameter(headers, "Basic")?;
    let value = BASE64_STANDARD.decode(value).ok()?;
    let value = String::from_utf8(value).ok()?;

    let (username, password) = value.split_once(':')?;
    Some((username.to_string(), password.to_string()))
}

fn authorization_parameter<'a>(headers: &'a HeaderMap, scheme: &str) -> Option<&'a str> {
    let value = headers.get(AUTHORIZATION)?.to_str().ok()?.trim_start();
    let (actual_scheme, parameter) = value.split_once(char::is_whitespace)?;

    actual_scheme
        .eq_ignore_ascii_case(scheme)
        .then_some(parameter.trim_start())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers_with_authorization(value: Option<&str>) -> HeaderMap {
        let mut headers = HeaderMap::new();
        if let Some(value) = value {
            headers.insert(AUTHORIZATION, value.parse().unwrap());
        }
        headers
    }

    #[test]
    fn bearer_token_parses_only_a_bearer_header() {
        for (header, expected) in [
            (Some("Bearer test-token-123"), Some("test-token-123")),
            (Some("bearer test-token"), Some("test-token")),
            (Some("Bearer "), Some("")),
            (Some("Basic dXNlcjpwYXNz"), None),
            (None, None),
        ] {
            assert_eq!(
                bearer_token(&headers_with_authorization(header)).as_deref(),
                expected,
                "{header:?}"
            );
        }
    }

    #[test]
    fn basic_credentials_parse_only_a_well_formed_basic_header() {
        let basic = |credentials: &[u8]| format!("Basic {}", BASE64_STANDARD.encode(credentials));
        for (header, expected) in [
            (
                Some(basic(b"username:password")),
                Some(("username", "password")),
            ),
            (Some(basic(b"user:pass:word")), Some(("user", "pass:word"))),
            (
                Some(format!(
                    "basic {}",
                    BASE64_STANDARD.encode("username:password")
                )),
                Some(("username", "password")),
            ),
            (Some(basic(b"usernameonly")), None),
            (Some(basic(&[0xFF, 0xFE, 0xFD])), None),
            (Some("Basic not-valid-base64!!!".to_string()), None),
            (Some("Bearer test-token".to_string()), None),
            (None, None),
        ] {
            let credentials = basic_credentials(&headers_with_authorization(header.as_deref()));
            assert_eq!(
                credentials
                    .as_ref()
                    .map(|(user, password)| (user.as_str(), password.as_str())),
                expected,
                "{header:?}"
            );
        }
    }
}
