use chrono::{TimeZone, Utc};
use hyper::header::{ACCEPT, ACCEPT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE, HeaderValue, RANGE};
use hyper::{HeaderMap, Request};

use angos_oci::header::X_ANGOS_SOURCE_TIMESTAMP;
use angos_oci::http_range::{ByteWindow, RequestRange};
use angos_oci::{MediaRange, MediaType};

use crate::command::server::error::Error;
use crate::command::server::request::{RequestHeaders, X_ANGOS_NO_REDIRECT};

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
fn test_range_missing_header() {
    let request = Request::builder().body(()).unwrap();
    let (parts, ()) = request.into_parts();

    let range = RequestHeaders::new(&parts.headers)
        .chunk_range(RANGE)
        .unwrap();
    assert_eq!(range, None);
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

#[test]
fn accepts_gzip_honours_a_zero_quality() {
    let accepts = |value: &str| {
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT_ENCODING, HeaderValue::from_str(value).unwrap());
        RequestHeaders::new(&headers).accepts_gzip()
    };
    assert!(accepts("gzip, deflate, br, zstd"));
    assert!(accepts("br;q=1.0, GZIP;q=0.5"));
    assert!(!accepts("gzip;q=0"));
    assert!(!accepts("gzip; Q = 0"));
    assert!(!accepts("br, x-gzip2"));
    assert!(!RequestHeaders::new(&HeaderMap::new()).accepts_gzip());
}
