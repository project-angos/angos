use chrono::{TimeZone, Utc};
use hyper::HeaderMap;
use hyper::header::{HeaderName, HeaderValue};

use angos_oci::header::X_ANGOS_SOURCE_TIMESTAMP;
use angos_oci::http_range::{ByteWindow, RequestRange};
use angos_oci::{MediaRange, MediaType};

use crate::command::server::error::Error;
use crate::command::server::request::{RequestHeaders, X_ANGOS_NO_REDIRECT};

fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (name, value) in pairs {
        headers.append(
            name.parse::<HeaderName>().unwrap(),
            HeaderValue::from_str(value).unwrap(),
        );
    }
    headers
}

/// end-5 pins a chunk's window to `^[0-9]+-[0-9]+$`: the `Range` header's
/// `bytes=` unit and an open-ended window are malformed, not tolerated.
#[test]
fn chunk_range_reads_only_a_closed_unitless_window() {
    for value in ["bytes=0-499", "0-"] {
        let result = RequestHeaders::new(&headers(&[("content-range", value)])).chunk_range();
        assert!(
            matches!(result, Err(Error::RangeNotSatisfiable(_))),
            "'{value}' must be refused, got: {result:?}"
        );
    }

    let window = RequestHeaders::new(&headers(&[("content-range", "100-200")]))
        .chunk_range()
        .unwrap();
    assert_eq!(
        window,
        Some(ByteWindow {
            start: 100,
            end: Some(200),
        })
    );
    assert_eq!(
        RequestHeaders::new(&HeaderMap::new())
            .chunk_range()
            .unwrap(),
        None
    );
}

/// RFC 9110 has a `Range` the server cannot parse, or whose unit it does not
/// know, ignored: the whole blob is served, as for a multi-range request. Only
/// a range that parses and cannot be met is a `416`.
#[test]
fn blob_range_ignores_what_it_cannot_read() {
    for value in [
        "bytes=bytes=0-5",
        "100-200",
        "bytes=500-499",
        "bytes=abc-100",
        "items=0-5",
        "bytes=0-10,20-30",
    ] {
        let range = RequestHeaders::new(&headers(&[("range", value)]))
            .blob_range()
            .expect("an unreadable range must not fail the request");
        assert!(range.is_none(), "'{value}' must be ignored, got: {range:?}");
    }

    for value in ["bytes=0-499", "Bytes=0-499"] {
        let range = RequestHeaders::new(&headers(&[("range", value)]))
            .blob_range()
            .unwrap();
        assert_eq!(
            range,
            Some(RequestRange::FromTo(ByteWindow {
                start: 0,
                end: Some(499),
            })),
            "{value}"
        );
    }
}

#[test]
fn content_length_must_parse_and_agree() {
    let content_length =
        |pairs: &[(&str, &str)]| RequestHeaders::new(&headers(pairs)).content_length();

    assert_eq!(content_length(&[]).unwrap(), None);
    assert_eq!(
        content_length(&[("content-length", "42")]).unwrap(),
        Some(42)
    );
    for pairs in [
        &[("content-length", "not-a-number")][..],
        &[("content-length", "42"), ("content-length", "43")][..],
    ] {
        assert!(matches!(content_length(pairs), Err(Error::BadRequest(_))));
    }
}

#[test]
fn test_content_type_valid() {
    let content_type = RequestHeaders::new(&headers(&[(
        "content-type",
        "application/vnd.oci.image.manifest.v1+json",
    )]))
    .content_type()
    .unwrap();
    assert_eq!(
        content_type,
        Some(MediaType::new("application/vnd.oci.image.manifest.v1+json").unwrap())
    );
}

#[test]
fn source_timestamp_is_utc_and_none_when_unreadable() {
    let noon = Utc.with_ymd_and_hms(2026, 6, 3, 12, 0, 0).unwrap();
    for (value, expected) in [
        ("2026-06-03T12:00:00Z", Some(noon)),
        ("  2026-06-03T14:00:00+02:00  ", Some(noon)),
        ("   ", None),
        ("not-a-timestamp", None),
    ] {
        let source_ts =
            RequestHeaders::new(&headers(&[(X_ANGOS_SOURCE_TIMESTAMP.as_str(), value)]))
                .source_timestamp();
        assert_eq!(source_ts, expected, "'{value}'");
    }
    assert_eq!(
        RequestHeaders::new(&HeaderMap::new()).source_timestamp(),
        None
    );
}

#[test]
fn test_source_timestamp_future_is_clamped_to_now() {
    let before = Utc::now();
    let source_ts = RequestHeaders::new(&headers(&[(
        X_ANGOS_SOURCE_TIMESTAMP.as_str(),
        "3000-01-01T00:00:00Z",
    )]))
    .source_timestamp()
    .expect("a parseable timestamp returns Some");
    let after = Utc::now();

    assert!(
        source_ts >= before && source_ts <= after,
        "a future source timestamp is clamped to ~now (got {source_ts}), not the future value"
    );
}

/// OCI clients never send the header, so they keep the redirect fast path.
#[test]
fn redirect_is_suppressed_only_by_a_truthy_value() {
    assert!(!RequestHeaders::new(&HeaderMap::new()).redirect_suppressed());
    for (value, suppressed) in [
        ("1", true),
        ("true", true),
        ("TRUE", true),
        ("yes", true),
        ("0", false),
        ("false", false),
        ("FALSE", false),
        ("", false),
    ] {
        assert_eq!(
            RequestHeaders::new(&headers(&[(X_ANGOS_NO_REDIRECT, value)])).redirect_suppressed(),
            suppressed,
            "'{value}'"
        );
    }
}

/// `*/*` is what curl and Docker send by default and angos re-sends it to the
/// upstream, so it must survive parsing; a member that is not a media range is
/// dropped rather than relayed malformed.
#[test]
fn wildcard_accept_members_survive_and_malformed_ones_are_dropped() {
    let accepted = RequestHeaders::new(&headers(&[(
        "accept",
        "*/*, application/*, not-a-range, application/json",
    )]))
    .accepted_content_types();
    let result: Vec<&str> = accepted.iter().map(MediaRange::as_str).collect();
    assert_eq!(result, vec!["*/*", "application/*", "application/json"]);
}

#[test]
fn accepts_gzip_honours_a_zero_quality() {
    let accepts =
        |value: &str| RequestHeaders::new(&headers(&[("accept-encoding", value)])).accepts_gzip();
    assert!(accepts("gzip, deflate, br, zstd"));
    assert!(accepts("br;q=1.0, GZIP;q=0.5"));
    assert!(!accepts("gzip;q=0"));
    assert!(!accepts("gzip; Q = 0"));
    assert!(!accepts("br, x-gzip2"));
    assert!(!RequestHeaders::new(&HeaderMap::new()).accepts_gzip());
}
