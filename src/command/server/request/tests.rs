use base64::{Engine, prelude::BASE64_STANDARD};
use chrono::{TimeZone, Utc};
use hyper::{
    Request,
    header::{
        ACCEPT, AUTHORIZATION, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, HeaderName,
        HeaderValue, RANGE,
    },
};

use crate::{
    command::server::{
        error::Error,
        request::{RequestHeaders, X_ANGOS_NO_REDIRECT},
        response_body::ResponseBody,
    },
    oci::MediaType,
    registry::BlobRange,
    registry_client::X_ANGOS_SOURCE_TIMESTAMP,
};

#[test]
fn test_range_with_bytes_prefix() {
    let request = Request::builder()
        .header(RANGE, "bytes=0-499")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let range = RequestHeaders::new(&parts.headers)
        .range(RANGE)
        .unwrap()
        .unwrap();
    assert_eq!(range, (0, Some(499)));
}

#[test]
fn test_blob_range_with_bytes_prefix() {
    let request = Request::builder()
        .header(RANGE, "bytes=0-499")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let range = RequestHeaders::new(&parts.headers)
        .blob_range()
        .unwrap()
        .unwrap();
    assert_eq!(
        range,
        BlobRange::FromTo {
            start: 0,
            end: Some(499)
        }
    );
}

#[test]
fn test_blob_range_unit_is_case_insensitive() {
    let request = Request::builder()
        .header(RANGE, "Bytes=0-499")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let range = RequestHeaders::new(&parts.headers)
        .blob_range()
        .unwrap()
        .unwrap();
    assert_eq!(
        range,
        BlobRange::FromTo {
            start: 0,
            end: Some(499)
        }
    );
}

#[test]
fn test_range_without_bytes_prefix() {
    let request = Request::builder()
        .header(RANGE, "100-200")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let range = RequestHeaders::new(&parts.headers)
        .range(RANGE)
        .unwrap()
        .unwrap();
    assert_eq!(range, (100, Some(200)));
}

#[test]
fn test_content_range_without_bytes_prefix() {
    let request = Request::builder()
        .header(CONTENT_RANGE, "100-200")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let range = RequestHeaders::new(&parts.headers)
        .range(CONTENT_RANGE)
        .unwrap()
        .unwrap();
    assert_eq!(range, (100, Some(200)));
}

#[test]
fn test_blob_range_requires_bytes_prefix() {
    let request = Request::builder()
        .header(RANGE, "100-200")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let result = RequestHeaders::new(&parts.headers).blob_range();
    assert!(result.is_err());
}

#[test]
fn test_range_no_end() {
    let request = Request::builder()
        .header(RANGE, "bytes=0-")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let range = RequestHeaders::new(&parts.headers)
        .range(RANGE)
        .unwrap()
        .unwrap();
    assert_eq!(range, (0, None));
}

#[test]
fn test_blob_range_suffix_range() {
    let request = Request::builder()
        .header(RANGE, "bytes=-499")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let range = RequestHeaders::new(&parts.headers)
        .blob_range()
        .unwrap()
        .unwrap();
    assert_eq!(range, BlobRange::Suffix(499));
}

#[test]
fn test_blob_range_zero_suffix_range() {
    let request = Request::builder()
        .header(RANGE, "bytes=-0")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let range = RequestHeaders::new(&parts.headers)
        .blob_range()
        .unwrap()
        .unwrap();
    assert_eq!(range, BlobRange::Suffix(0));
}

#[test]
fn test_range_large_numbers() {
    let request = Request::builder()
        .header(RANGE, "bytes=1000000000-2000000000")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let range = RequestHeaders::new(&parts.headers)
        .range(RANGE)
        .unwrap()
        .unwrap();
    assert_eq!(range, (1_000_000_000, Some(2_000_000_000)));
}

#[test]
fn test_range_missing_header() {
    let request = Request::builder().body(()).unwrap();
    let (parts, ()) = request.into_parts();

    let range = RequestHeaders::new(&parts.headers).range(RANGE).unwrap();
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

    let range = RequestHeaders::new(&parts.headers)
        .range(custom_header)
        .unwrap()
        .unwrap();
    assert_eq!(range, (50, Some(100)));
}

#[test]
fn test_range_start_greater_than_end() {
    let request = Request::builder()
        .header(RANGE, "bytes=500-499")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let result = RequestHeaders::new(&parts.headers).range(RANGE);
    assert!(result.is_err());
    match result.unwrap_err() {
        Error::RangeNotSatisfiable(msg) => {
            assert!(msg.contains("start (500) > end (499)"));
        }
        _ => panic!("Expected RangeNotSatisfiable error"),
    }
}

#[test]
fn test_range_missing_start_is_invalid_for_start_end_parser() {
    let request = Request::builder()
        .header(RANGE, "bytes=-499")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let result = RequestHeaders::new(&parts.headers).range(RANGE);
    assert!(result.is_err());
    match result.unwrap_err() {
        Error::RangeNotSatisfiable(msg) => {
            assert!(msg.contains("Invalid Range header format"));
        }
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

    let result = RequestHeaders::new(&parts.headers).range(RANGE);
    assert!(result.is_err());
    match result.unwrap_err() {
        Error::RangeNotSatisfiable(msg) => {
            assert!(msg.contains("Invalid Range header format"));
        }
        _ => panic!("Expected RangeNotSatisfiable error"),
    }
}

#[test]
fn test_blob_range_multiple_ranges_not_supported() {
    let request = Request::builder()
        .header(RANGE, "bytes=0-10,20-30")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let range = RequestHeaders::new(&parts.headers).blob_range().unwrap();
    assert_eq!(range, None);
}

#[test]
fn test_range_non_numeric_start() {
    let request = Request::builder()
        .header(RANGE, "bytes=abc-100")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let result = RequestHeaders::new(&parts.headers).range(RANGE);
    assert!(result.is_err());
}

#[test]
fn test_range_non_numeric_end() {
    let request = Request::builder()
        .header(RANGE, "bytes=0-xyz")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let result = RequestHeaders::new(&parts.headers).range(RANGE);
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

    let result = RequestHeaders::new(&parts.headers).range(RANGE);
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

    let result = RequestHeaders::new(&parts.headers).accepted_content_types();
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

    let result = RequestHeaders::new(&parts.headers).accepted_content_types();
    assert_eq!(result, vec!["application/json"]);
}

#[test]
fn test_accepted_content_types_empty() {
    let request = Request::builder().body(()).unwrap();
    let (parts, ()) = request.into_parts();

    let result = RequestHeaders::new(&parts.headers).accepted_content_types();
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

    let result = RequestHeaders::new(&parts.headers).accepted_content_types();
    assert_eq!(result, vec!["application/json;q=0.9", "text/html;q=0.8"]);
}

#[test]
fn test_accepted_content_types_splits_commas_and_orders_by_quality() {
    let request = Request::builder()
        .header(ACCEPT, "text/plain;q=0.2, application/json;q=0.9")
        .header(ACCEPT, "application/xml")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let result = RequestHeaders::new(&parts.headers).accepted_content_types();
    assert_eq!(
        result,
        vec![
            "application/xml",
            "application/json;q=0.9",
            "text/plain;q=0.2"
        ]
    );
}

#[test]
fn test_accepted_content_types_keeps_original_order_for_equal_quality() {
    let request = Request::builder()
        .header(ACCEPT, "application/json, application/xml;q=1.0")
        .header(ACCEPT, "text/plain")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let result = RequestHeaders::new(&parts.headers).accepted_content_types();
    assert_eq!(
        result,
        vec!["application/json", "application/xml;q=1.0", "text/plain"]
    );
}

#[test]
fn test_bearer_token_valid() {
    let request = Request::builder()
        .header(AUTHORIZATION, "Bearer test-token-123")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let token = RequestHeaders::new(&parts.headers).bearer_token();
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

    let token = RequestHeaders::new(&parts.headers).bearer_token();
    assert_eq!(
        token,
        Some("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.test".to_string())
    );
}

#[test]
fn test_bearer_token_missing() {
    let request = Request::builder().body(()).unwrap();
    let (parts, ()) = request.into_parts();

    let token = RequestHeaders::new(&parts.headers).bearer_token();
    assert_eq!(token, None);
}

#[test]
fn test_bearer_token_wrong_scheme() {
    let request = Request::builder()
        .header(AUTHORIZATION, "Basic dXNlcjpwYXNz")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let token = RequestHeaders::new(&parts.headers).bearer_token();
    assert_eq!(token, None);
}

#[test]
fn test_bearer_token_scheme_is_case_insensitive() {
    let request = Request::builder()
        .header(AUTHORIZATION, "bearer test-token")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let token = RequestHeaders::new(&parts.headers).bearer_token();
    assert_eq!(token, Some("test-token".to_string()));
}

#[test]
fn test_bearer_token_empty() {
    let request = Request::builder()
        .header(AUTHORIZATION, "Bearer ")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let token = RequestHeaders::new(&parts.headers).bearer_token();
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

    let auth = RequestHeaders::new(&parts.headers).basic_auth();
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

    let auth = RequestHeaders::new(&parts.headers).basic_auth();
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

    let auth = RequestHeaders::new(&parts.headers).basic_auth();
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

    let auth = RequestHeaders::new(&parts.headers).basic_auth();
    assert_eq!(auth, Some(("username".to_string(), String::new())));
}

#[test]
fn test_basic_auth_missing() {
    let request = Request::builder().body(()).unwrap();
    let (parts, ()) = request.into_parts();

    let auth = RequestHeaders::new(&parts.headers).basic_auth();
    assert_eq!(auth, None);
}

#[test]
fn test_basic_auth_wrong_scheme() {
    let request = Request::builder()
        .header(AUTHORIZATION, "Bearer test-token")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let auth = RequestHeaders::new(&parts.headers).basic_auth();
    assert_eq!(auth, None);
}

#[test]
fn test_basic_auth_invalid_base64() {
    let request = Request::builder()
        .header(AUTHORIZATION, "Basic not-valid-base64!!!")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let auth = RequestHeaders::new(&parts.headers).basic_auth();
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

    let auth = RequestHeaders::new(&parts.headers).basic_auth();
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

    let auth = RequestHeaders::new(&parts.headers).basic_auth();
    assert_eq!(auth, None);
}

#[test]
fn test_basic_auth_scheme_is_case_insensitive() {
    let credentials = BASE64_STANDARD.encode("username:password");
    let request = Request::builder()
        .header(AUTHORIZATION, format!("basic {credentials}"))
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let auth = RequestHeaders::new(&parts.headers).basic_auth();
    assert_eq!(auth, Some(("username".to_string(), "password".to_string())));
}

#[test]
fn test_content_length_missing() {
    let request = Request::builder().body(()).unwrap();
    let (parts, ()) = request.into_parts();

    let content_length = RequestHeaders::new(&parts.headers)
        .content_length()
        .unwrap();
    assert_eq!(content_length, None);
}

#[test]
fn test_content_length_valid() {
    let request = Request::builder()
        .header(CONTENT_LENGTH, "42")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let content_length = RequestHeaders::new(&parts.headers)
        .content_length()
        .unwrap();
    assert_eq!(content_length, Some(42));
}

#[test]
fn test_content_length_invalid() {
    let request = Request::builder()
        .header(CONTENT_LENGTH, "not-a-number")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let result = RequestHeaders::new(&parts.headers).content_length();
    assert!(matches!(result, Err(Error::BadRequest(_))));
}

#[test]
fn test_content_length_conflicting_duplicates() {
    let request = Request::builder()
        .header(CONTENT_LENGTH, "42")
        .header(CONTENT_LENGTH, "43")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let result = RequestHeaders::new(&parts.headers).content_length();
    assert!(matches!(result, Err(Error::BadRequest(_))));
}

#[test]
fn test_content_type_valid() {
    let request = Request::builder()
        .header(CONTENT_TYPE, "application/vnd.oci.image.manifest.v1+json")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let content_type = RequestHeaders::new(&parts.headers).content_type().unwrap();
    assert_eq!(
        content_type,
        Some(MediaType::new("application/vnd.oci.image.manifest.v1+json").unwrap())
    );
}

#[test]
fn test_source_timestamp_present() {
    let request = Request::builder()
        .header(X_ANGOS_SOURCE_TIMESTAMP, "2026-06-03T12:00:00Z")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let source_ts = RequestHeaders::new(&parts.headers).source_timestamp();
    assert_eq!(
        source_ts,
        Some(Utc.with_ymd_and_hms(2026, 6, 3, 12, 0, 0).unwrap())
    );
}

#[test]
fn test_source_timestamp_with_offset_normalises_to_utc() {
    let request = Request::builder()
        .header(X_ANGOS_SOURCE_TIMESTAMP, "  2026-06-03T14:00:00+02:00  ")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let source_ts = RequestHeaders::new(&parts.headers).source_timestamp();
    assert_eq!(
        source_ts,
        Some(Utc.with_ymd_and_hms(2026, 6, 3, 12, 0, 0).unwrap())
    );
}

#[test]
fn test_source_timestamp_missing() {
    let request = Request::builder().body(()).unwrap();
    let (parts, ()) = request.into_parts();

    let source_ts = RequestHeaders::new(&parts.headers).source_timestamp();
    assert_eq!(source_ts, None);
}

#[test]
fn test_source_timestamp_empty_is_none() {
    let request = Request::builder()
        .header(X_ANGOS_SOURCE_TIMESTAMP, "   ")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let source_ts = RequestHeaders::new(&parts.headers).source_timestamp();
    assert_eq!(source_ts, None);
}

#[test]
fn test_source_timestamp_garbage_is_none() {
    let request = Request::builder()
        .header(X_ANGOS_SOURCE_TIMESTAMP, "not-a-timestamp")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let source_ts = RequestHeaders::new(&parts.headers).source_timestamp();
    assert_eq!(source_ts, None);
}

#[test]
fn test_source_timestamp_future_is_clamped_to_now() {
    let request = Request::builder()
        .header(X_ANGOS_SOURCE_TIMESTAMP, "3000-01-01T00:00:00Z")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let before = Utc::now();
    let source_ts = RequestHeaders::new(&parts.headers)
        .source_timestamp()
        .expect("a parseable timestamp returns Some");
    let after = Utc::now();

    assert!(
        source_ts >= before && source_ts <= after,
        "a future source timestamp is clamped to ~now (got {source_ts}), not the future value"
    );
}

#[test]
fn test_redirect_not_suppressed_without_header() {
    let request = Request::builder().body(()).unwrap();
    let (parts, ()) = request.into_parts();

    assert!(
        !RequestHeaders::new(&parts.headers).redirect_suppressed(),
        "a request without the header keeps the redirect fast path (OCI clients)"
    );
}

#[test]
fn test_redirect_suppressed_for_truthy_values() {
    for value in ["1", "true", "TRUE", "yes"] {
        let request = Request::builder()
            .header(X_ANGOS_NO_REDIRECT, value)
            .body(())
            .unwrap();
        let (parts, ()) = request.into_parts();

        assert!(
            RequestHeaders::new(&parts.headers).redirect_suppressed(),
            "'{value}' must suppress the redirect so the browser UI gets an inline body"
        );
    }
}

#[test]
fn test_redirect_not_suppressed_for_falsey_values() {
    for value in ["0", "false", "FALSE", ""] {
        let request = Request::builder()
            .header(X_ANGOS_NO_REDIRECT, value)
            .body(())
            .unwrap();
        let (parts, ()) = request.into_parts();

        assert!(
            !RequestHeaders::new(&parts.headers).redirect_suppressed(),
            "'{value}' must not suppress the redirect"
        );
    }
}
