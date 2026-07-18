//! `Authorization` header parsing shared by the authentication middlewares.

use base64::{Engine, prelude::BASE64_STANDARD};
use hyper::{HeaderMap, header::AUTHORIZATION};

static BEARER_SCHEME: &str = "Bearer";
static BASIC_SCHEME: &str = "Basic";

/// The token of a `Bearer` authorization header, if one is present.
pub fn bearer_token(headers: &HeaderMap) -> Option<String> {
    authorization_parameter(headers, BEARER_SCHEME).map(str::to_string)
}

/// The decoded `(username, password)` of a well-formed `Basic` authorization
/// header; `None` when the header is absent, malformed, or another scheme.
pub fn basic_credentials(headers: &HeaderMap) -> Option<(String, String)> {
    let value = authorization_parameter(headers, BASIC_SCHEME)?;
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

    fn headers_with_authorization(value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, value.parse().unwrap());
        headers
    }

    #[test]
    fn test_bearer_token_valid() {
        let headers = headers_with_authorization("Bearer test-token-123");
        assert_eq!(bearer_token(&headers), Some("test-token-123".to_string()));
    }

    #[test]
    fn test_bearer_token_with_special_characters() {
        let headers =
            headers_with_authorization("Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.test");
        assert_eq!(
            bearer_token(&headers),
            Some("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.test".to_string())
        );
    }

    #[test]
    fn test_bearer_token_missing() {
        assert_eq!(bearer_token(&HeaderMap::new()), None);
    }

    #[test]
    fn test_bearer_token_wrong_scheme() {
        let headers = headers_with_authorization("Basic dXNlcjpwYXNz");
        assert_eq!(bearer_token(&headers), None);
    }

    #[test]
    fn test_bearer_token_scheme_is_case_insensitive() {
        let headers = headers_with_authorization("bearer test-token");
        assert_eq!(bearer_token(&headers), Some("test-token".to_string()));
    }

    #[test]
    fn test_bearer_token_empty() {
        let headers = headers_with_authorization("Bearer ");
        assert_eq!(bearer_token(&headers), Some(String::new()));
    }

    #[test]
    fn test_basic_credentials_valid() {
        let credentials = BASE64_STANDARD.encode("username:password");
        let headers = headers_with_authorization(&format!("Basic {credentials}"));
        assert_eq!(
            basic_credentials(&headers),
            Some(("username".to_string(), "password".to_string()))
        );
    }

    #[test]
    fn test_basic_credentials_with_special_characters() {
        let credentials = BASE64_STANDARD.encode("user@example.com:p@ssw0rd!");
        let headers = headers_with_authorization(&format!("Basic {credentials}"));
        assert_eq!(
            basic_credentials(&headers),
            Some(("user@example.com".to_string(), "p@ssw0rd!".to_string()))
        );
    }

    #[test]
    fn test_basic_credentials_with_colon_in_password() {
        let credentials = BASE64_STANDARD.encode("user:pass:word");
        let headers = headers_with_authorization(&format!("Basic {credentials}"));
        assert_eq!(
            basic_credentials(&headers),
            Some(("user".to_string(), "pass:word".to_string()))
        );
    }

    #[test]
    fn test_basic_credentials_empty_password() {
        let credentials = BASE64_STANDARD.encode("username:");
        let headers = headers_with_authorization(&format!("Basic {credentials}"));
        assert_eq!(
            basic_credentials(&headers),
            Some(("username".to_string(), String::new()))
        );
    }

    #[test]
    fn test_basic_credentials_missing() {
        assert_eq!(basic_credentials(&HeaderMap::new()), None);
    }

    #[test]
    fn test_basic_credentials_wrong_scheme() {
        let headers = headers_with_authorization("Bearer test-token");
        assert_eq!(basic_credentials(&headers), None);
    }

    #[test]
    fn test_basic_credentials_invalid_base64() {
        let headers = headers_with_authorization("Basic not-valid-base64!!!");
        assert_eq!(basic_credentials(&headers), None);
    }

    #[test]
    fn test_basic_credentials_invalid_utf8() {
        let encoded = BASE64_STANDARD.encode([0xFF, 0xFE, 0xFD]);
        let headers = headers_with_authorization(&format!("Basic {encoded}"));
        assert_eq!(basic_credentials(&headers), None);
    }

    #[test]
    fn test_basic_credentials_no_colon() {
        let credentials = BASE64_STANDARD.encode("usernameonly");
        let headers = headers_with_authorization(&format!("Basic {credentials}"));
        assert_eq!(basic_credentials(&headers), None);
    }

    #[test]
    fn test_basic_credentials_scheme_is_case_insensitive() {
        let credentials = BASE64_STANDARD.encode("username:password");
        let headers = headers_with_authorization(&format!("basic {credentials}"));
        assert_eq!(
            basic_credentials(&headers),
            Some(("username".to_string(), "password".to_string()))
        );
    }
}
