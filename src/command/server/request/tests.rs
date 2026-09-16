use chrono::{TimeZone, Utc};
use hyper::{
    Request,
    header::{ACCEPT, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, HeaderName, RANGE},
};

use angos_oci::http_range::{ByteWindow, RequestRange};
use angos_oci::{MediaRange, MediaType};
use angos_oci_client::X_ANGOS_SOURCE_TIMESTAMP;

use crate::command::server::{
    error::Error,
    request::{RequestHeaders, X_ANGOS_NO_REDIRECT},
};

/// end-5 spells a chunk's window `<start>-<end>`. The `bytes=` unit is the
/// `Range` header's, so a chunk carrying one is malformed rather than tolerated.
#[test]
fn test_chunk_range_refuses_the_range_unit() {
    let request = Request::builder()
        .header(RANGE, "bytes=0-499")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let result = RequestHeaders::new(&parts.headers).chunk_range(RANGE);
    assert!(
        matches!(result, Err(Error::RangeNotSatisfiable(_))),
        "a bytes=-prefixed chunk window must be refused, got: {result:?}"
    );
}

/// RFC 9110 has a `Range` the server cannot parse, or whose unit it does not
/// know, ignored: the whole blob is served, as for a multi-range request. Only
/// a range that parses and cannot be met is a `416`.
#[test]
fn test_unreadable_blob_range_is_ignored() {
    for value in [
        // A doubled unit, an absent one, an inverted window, and a
        // non-numeric bound.
        "bytes=bytes=0-5",
        "100-200",
        "bytes=500-499",
        "bytes=abc-100",
        "items=0-5",
    ] {
        let request = Request::builder().header(RANGE, value).body(()).unwrap();
        let (parts, ()) = request.into_parts();

        let range = RequestHeaders::new(&parts.headers)
            .blob_range()
            .expect("an unreadable range must not fail the request");
        assert!(range.is_none(), "'{value}' must be ignored, got: {range:?}");
    }
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
        RequestRange::FromTo(ByteWindow {
            start: 0,
            end: Some(499),
        })
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
        RequestRange::FromTo(ByteWindow {
            start: 0,
            end: Some(499),
        })
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
        .chunk_range(RANGE)
        .unwrap()
        .unwrap();
    assert_eq!(
        range,
        ByteWindow {
            start: 100,
            end: Some(200),
        }
    );
}

#[test]
fn test_content_range_without_bytes_prefix() {
    let request = Request::builder()
        .header(CONTENT_RANGE, "100-200")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let range = RequestHeaders::new(&parts.headers)
        .chunk_range(CONTENT_RANGE)
        .unwrap()
        .unwrap();
    assert_eq!(
        range,
        ByteWindow {
            start: 100,
            end: Some(200),
        }
    );
}

/// end-5 pins a chunk's window to `^[0-9]+-[0-9]+$`, so a value naming no last
/// byte is refused rather than read as an open-ended one.
#[test]
fn test_chunk_range_refuses_an_open_ended_window() {
    let request = Request::builder().header(RANGE, "0-").body(()).unwrap();
    let (parts, ()) = request.into_parts();

    let result = RequestHeaders::new(&parts.headers).chunk_range(RANGE);
    assert!(
        matches!(result, Err(Error::RangeNotSatisfiable(_))),
        "an open-ended chunk window must be refused, got: {result:?}"
    );
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
    assert_eq!(range, RequestRange::Suffix(499));
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
    assert_eq!(range, RequestRange::Suffix(0));
}

#[test]
fn test_range_large_numbers() {
    let request = Request::builder()
        .header(RANGE, "1000000000-2000000000")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let range = RequestHeaders::new(&parts.headers)
        .chunk_range(RANGE)
        .unwrap()
        .unwrap();
    assert_eq!(
        range,
        ByteWindow {
            start: 1_000_000_000,
            end: Some(2_000_000_000),
        }
    );
}

#[test]
fn test_range_missing_header() {
    let request = Request::builder().body(()).unwrap();
    let (parts, ()) = request.into_parts();

    let range = RequestHeaders::new(&parts.headers)
        .chunk_range(RANGE)
        .unwrap();
    assert_eq!(range, None);
}

#[test]
fn test_range_custom_header_name() {
    let custom_header = HeaderName::from_static("x-custom-range");
    let request = Request::builder()
        .header(&custom_header, "50-100")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let range = RequestHeaders::new(&parts.headers)
        .chunk_range(custom_header)
        .unwrap()
        .unwrap();
    assert_eq!(
        range,
        ByteWindow {
            start: 50,
            end: Some(100),
        }
    );
}

#[test]
fn test_range_start_greater_than_end() {
    let request = Request::builder()
        .header(RANGE, "500-499")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let result = RequestHeaders::new(&parts.headers).chunk_range(RANGE);
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

    let result = RequestHeaders::new(&parts.headers).chunk_range(RANGE);
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

    let result = RequestHeaders::new(&parts.headers).chunk_range(RANGE);
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

    let result = RequestHeaders::new(&parts.headers).chunk_range(RANGE);
    assert!(result.is_err());
}

#[test]
fn test_range_non_numeric_end() {
    let request = Request::builder()
        .header(RANGE, "bytes=0-xyz")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let result = RequestHeaders::new(&parts.headers).chunk_range(RANGE);
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

    let result = RequestHeaders::new(&parts.headers).chunk_range(RANGE);
    assert!(result.is_err());
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

/// `*/*` is what curl and Docker send by default and angos re-sends it to the
/// upstream, so it must survive parsing; a member that is not a media range is
/// dropped rather than relayed malformed.
#[test]
fn wildcard_accept_members_survive_and_malformed_ones_are_dropped() {
    let request = Request::builder()
        .header(ACCEPT, "*/*, application/*, not-a-range, application/json")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let accepted = RequestHeaders::new(&parts.headers).accepted_content_types();
    let result: Vec<&str> = accepted.iter().map(MediaRange::as_str).collect();
    assert_eq!(result, vec!["*/*", "application/*", "application/json"]);
}
