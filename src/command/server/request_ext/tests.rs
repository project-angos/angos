use hyper::{
    Request,
    header::{HeaderValue, RANGE, USER_AGENT},
};

use super::*;
use crate::command::server::response_body::ResponseBody;

#[test]
fn test_get_header_exists() {
    let request = Request::builder()
        .header(USER_AGENT, "test-agent/1.0")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let result = parts.get_header(USER_AGENT);
    assert_eq!(result, Some("test-agent/1.0".to_string()));
}

#[test]
fn test_get_header_missing() {
    let request = Request::builder().body(()).unwrap();
    let (parts, ()) = request.into_parts();

    let result = parts.get_header(USER_AGENT);
    assert_eq!(result, None);
}

#[test]
fn test_get_header_custom_header() {
    let request = Request::builder()
        .header("X-Custom-Header", "custom-value")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let result = parts.get_header("x-custom-header");
    assert_eq!(result, Some("custom-value".to_string()));
}

#[test]
fn test_get_header_invalid_utf8() {
    let request = Request::builder()
        .header("X-Test", HeaderValue::from_bytes(&[0xFF, 0xFE]).unwrap())
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let result = parts.get_header("x-test");
    assert_eq!(result, None);
}

#[test]
fn test_range_with_bytes_prefix() {
    let request = Request::builder()
        .header(RANGE, "bytes=0-499")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let range = parts.range(RANGE).unwrap().unwrap();
    assert_eq!(range, (0, Some(499)));
}

#[test]
fn test_range_without_bytes_prefix() {
    let request = Request::builder()
        .header(RANGE, "100-200")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let range = parts.range(RANGE).unwrap().unwrap();
    assert_eq!(range, (100, Some(200)));
}

#[test]
fn test_range_no_end() {
    let request = Request::builder()
        .header(RANGE, "bytes=0-")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let range = parts.range(RANGE).unwrap().unwrap();
    assert_eq!(range, (0, None));
}

#[test]
fn test_range_large_numbers() {
    let request = Request::builder()
        .header(RANGE, "bytes=1000000000-2000000000")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let range = parts.range(RANGE).unwrap().unwrap();
    assert_eq!(range, (1_000_000_000, Some(2_000_000_000)));
}

#[test]
fn test_range_missing_header() {
    let request = Request::builder().body(()).unwrap();
    let (parts, ()) = request.into_parts();

    let range = parts.range(RANGE).unwrap();
    assert_eq!(range, None);
}

#[test]
fn test_range_custom_header_name() {
    let custom_header = HeaderName::from_static("x-custom-range");
    let request = Request::builder()
        .header(&custom_header, "bytes=50-100")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let range = parts.range(custom_header).unwrap().unwrap();
    assert_eq!(range, (50, Some(100)));
}

#[test]
fn test_range_start_greater_than_end() {
    let request = Request::builder()
        .header(RANGE, "bytes=500-499")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let result = parts.range(RANGE);
    assert!(result.is_err());
    match result.unwrap_err() {
        Error::RangeNotSatisfiable(msg) => {
            assert!(msg.contains("start (500) > end (499)"));
        }
        _ => panic!("Expected RangeNotSatisfiable error"),
    }
}

#[test]
fn test_range_missing_start() {
    let request = Request::builder()
        .header(RANGE, "bytes=-499")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let result = parts.range(RANGE);
    assert!(result.is_err());
    match result.unwrap_err() {
        Error::RangeNotSatisfiable(_) => {}
        _ => panic!("Expected RangeNotSatisfiable error"),
    }
}

#[test]
fn test_range_invalid_format() {
    let request = Request::builder()
        .header(RANGE, "bytes=plouf")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let result = parts.range(RANGE);
    assert!(result.is_err());
    match result.unwrap_err() {
        Error::RangeNotSatisfiable(msg) => {
            assert!(msg.contains("Invalid Range header format"));
        }
        _ => panic!("Expected RangeNotSatisfiable error"),
    }
}

#[test]
fn test_range_non_numeric_start() {
    let request = Request::builder()
        .header(RANGE, "bytes=abc-100")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let result = parts.range(RANGE);
    assert!(result.is_err());
}

#[test]
fn test_range_non_numeric_end() {
    let request = Request::builder()
        .header(RANGE, "bytes=0-xyz")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let result = parts.range(RANGE);
    assert!(result.is_err());
    match result.unwrap_err() {
        Error::RangeNotSatisfiable(msg) => {
            assert!(
                msg.contains("Error parsing 'end'") || msg.contains("Invalid Range header format")
            );
        }
        _ => panic!("Expected RangeNotSatisfiable error"),
    }
}

#[test]
fn test_range_overflow() {
    let request = Request::builder()
        .header(RANGE, "bytes=99999999999999999999-100")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let result = parts.range(RANGE);
    assert!(result.is_err());
}

#[test]
fn test_accepted_content_types_multiple() {
    let request = Request::builder()
        .header(ACCEPT, HeaderValue::from_static("application/json"))
        .header(ACCEPT, HeaderValue::from_static("application/xml"))
        .header(ACCEPT, HeaderValue::from_static("text/plain"));
    let request = request.body(ResponseBody::empty()).unwrap();
    let (parts, _) = request.into_parts();

    let result = parts.accepted_content_types();
    assert_eq!(
        result,
        vec!["application/json", "application/xml", "text/plain"]
    );
}

#[test]
fn test_accepted_content_types_single() {
    let request = Request::builder()
        .header(ACCEPT, "application/json")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let result = parts.accepted_content_types();
    assert_eq!(result, vec!["application/json"]);
}

#[test]
fn test_accepted_content_types_empty() {
    let request = Request::builder().body(()).unwrap();
    let (parts, ()) = request.into_parts();

    let result = parts.accepted_content_types();
    assert!(result.is_empty());
}

#[test]
fn test_accepted_content_types_with_quality() {
    let request = Request::builder()
        .header(ACCEPT, "application/json;q=0.9")
        .header(ACCEPT, "text/html;q=0.8")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let result = parts.accepted_content_types();
    assert_eq!(result, vec!["application/json;q=0.9", "text/html;q=0.8"]);
}

#[test]
fn test_bearer_token_valid() {
    let request = Request::builder()
        .header(AUTHORIZATION, "Bearer test-token-123")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let token = parts.bearer_token();
    assert_eq!(token, Some("test-token-123".to_string()));
}

#[test]
fn test_bearer_token_with_special_characters() {
    let request = Request::builder()
        .header(
            AUTHORIZATION,
            "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.test",
        )
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let token = parts.bearer_token();
    assert_eq!(
        token,
        Some("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.test".to_string())
    );
}

#[test]
fn test_bearer_token_missing() {
    let request = Request::builder().body(()).unwrap();
    let (parts, ()) = request.into_parts();

    let token = parts.bearer_token();
    assert_eq!(token, None);
}

#[test]
fn test_bearer_token_wrong_scheme() {
    let request = Request::builder()
        .header(AUTHORIZATION, "Basic dXNlcjpwYXNz")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let token = parts.bearer_token();
    assert_eq!(token, None);
}

#[test]
fn test_bearer_token_case_sensitive() {
    let request = Request::builder()
        .header(AUTHORIZATION, "bearer test-token")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let token = parts.bearer_token();
    assert_eq!(token, None);
}

#[test]
fn test_bearer_token_empty() {
    let request = Request::builder()
        .header(AUTHORIZATION, "Bearer ")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let token = parts.bearer_token();
    assert_eq!(token, Some(String::new()));
}

#[test]
fn test_basic_auth_valid() {
    let credentials = BASE64_STANDARD.encode("username:password");
    let request = Request::builder()
        .header(AUTHORIZATION, format!("Basic {credentials}"))
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let auth = parts.basic_auth();
    assert_eq!(auth, Some(("username".to_string(), "password".to_string())));
}

#[test]
fn test_basic_auth_with_special_characters() {
    let credentials = BASE64_STANDARD.encode("user@example.com:p@ssw0rd!");
    let request = Request::builder()
        .header(AUTHORIZATION, format!("Basic {credentials}"))
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let auth = parts.basic_auth();
    assert_eq!(
        auth,
        Some(("user@example.com".to_string(), "p@ssw0rd!".to_string()))
    );
}

#[test]
fn test_basic_auth_with_colon_in_password() {
    let credentials = BASE64_STANDARD.encode("user:pass:word");
    let request = Request::builder()
        .header(AUTHORIZATION, format!("Basic {credentials}"))
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let auth = parts.basic_auth();
    assert_eq!(auth, Some(("user".to_string(), "pass:word".to_string())));
}

#[test]
fn test_basic_auth_empty_password() {
    let credentials = BASE64_STANDARD.encode("username:");
    let request = Request::builder()
        .header(AUTHORIZATION, format!("Basic {credentials}"))
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let auth = parts.basic_auth();
    assert_eq!(auth, Some(("username".to_string(), String::new())));
}

#[test]
fn test_basic_auth_missing() {
    let request = Request::builder().body(()).unwrap();
    let (parts, ()) = request.into_parts();

    let auth = parts.basic_auth();
    assert_eq!(auth, None);
}

#[test]
fn test_basic_auth_wrong_scheme() {
    let request = Request::builder()
        .header(AUTHORIZATION, "Bearer test-token")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let auth = parts.basic_auth();
    assert_eq!(auth, None);
}

#[test]
fn test_basic_auth_invalid_base64() {
    let request = Request::builder()
        .header(AUTHORIZATION, "Basic not-valid-base64!!!")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let auth = parts.basic_auth();
    assert_eq!(auth, None);
}

#[test]
fn test_basic_auth_invalid_utf8() {
    let invalid_bytes = vec![0xFF, 0xFE, 0xFD];
    let encoded = BASE64_STANDARD.encode(&invalid_bytes);
    let request = Request::builder()
        .header(AUTHORIZATION, format!("Basic {encoded}"))
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let auth = parts.basic_auth();
    assert_eq!(auth, None);
}

#[test]
fn test_basic_auth_no_colon() {
    let credentials = BASE64_STANDARD.encode("usernameonly");
    let request = Request::builder()
        .header(AUTHORIZATION, format!("Basic {credentials}"))
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let auth = parts.basic_auth();
    assert_eq!(auth, None);
}

#[test]
fn test_basic_auth_case_sensitive() {
    let credentials = BASE64_STANDARD.encode("username:password");
    let request = Request::builder()
        .header(AUTHORIZATION, format!("basic {credentials}"))
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let auth = parts.basic_auth();
    assert_eq!(auth, None);
}
