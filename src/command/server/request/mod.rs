use std::{cmp::Ordering, io, sync::LazyLock};

use base64::{Engine, prelude::BASE64_STANDARD};
use chrono::{DateTime, Utc};
use futures_util::TryStreamExt;
use http_body_util::BodyExt;
use hyper::{
    HeaderMap,
    body::Incoming,
    header::{ACCEPT, AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE, HeaderName, HeaderValue, RANGE},
};
use regex::Regex;
use tokio::io::AsyncRead;
use tokio_util::io::StreamReader;

use crate::{
    command::server::error::Error, oci::MediaType, registry::BlobRange,
    registry_client::X_ANGOS_SOURCE_TIMESTAMP,
};

static START_END_RANGE_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^(?:bytes=)?(?P<start>\d+)-(?P<end>\d+)?$").unwrap());
static SUFFIX_RANGE_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^(?:bytes=)?-(?P<suffix>\d+)$").unwrap());

static BYTES_RANGE_PREFIX: &str = "bytes=";
static BEARER_SCHEME: &str = "Bearer";
static BASIC_SCHEME: &str = "Basic";
static QUALITY_PARAM: &str = "q";

#[derive(Clone, Debug)]
pub struct RequestHeaders<'a> {
    headers: &'a HeaderMap,
}

impl<'a> RequestHeaders<'a> {
    pub fn new(headers: &'a HeaderMap) -> Self {
        Self { headers }
    }

    pub fn accepted_content_types(&self) -> Vec<String> {
        let mut media_ranges = Vec::new();

        for header in self.headers.get_all(ACCEPT) {
            let Ok(header) = header.to_str() else {
                continue;
            };

            for media_range in header.split(',') {
                let media_range = media_range.trim();
                if media_range.is_empty() {
                    continue;
                }

                media_ranges.push(AcceptMediaRange {
                    value: media_range.to_string(),
                    quality: quality_for_media_range(media_range),
                    order: media_ranges.len(),
                });
            }
        }

        media_ranges.sort_by(|left, right| match right.quality.cmp(&left.quality) {
            Ordering::Equal => left.order.cmp(&right.order),
            ordering => ordering,
        });

        media_ranges
            .into_iter()
            .map(|media_range| media_range.value)
            .collect()
    }

    pub fn bearer_token(&self) -> Option<String> {
        self.authorization_parameter(BEARER_SCHEME)
            .map(str::to_string)
    }

    pub fn basic_auth(&self) -> Option<(String, String)> {
        let value = self.authorization_parameter(BASIC_SCHEME)?;
        let value = BASE64_STANDARD.decode(value).ok()?;
        let value = String::from_utf8(value).ok()?;

        let (username, password) = value.split_once(':')?;
        Some((username.to_string(), password.to_string()))
    }

    pub fn content_length(&self) -> Result<Option<u64>, Error> {
        let mut values = self.headers.get_all(CONTENT_LENGTH).iter();
        let Some(first) = values.next() else {
            return Ok(None);
        };

        let content_length = parse_content_length(first)?;
        for value in values {
            if parse_content_length(value)? != content_length {
                return Err(Error::BadRequest(
                    "Conflicting Content-Length headers".to_string(),
                ));
            }
        }

        Ok(Some(content_length))
    }

    pub fn content_type(&self) -> Result<Option<MediaType>, Error> {
        let Some(content_type) = self.headers.get(CONTENT_TYPE) else {
            return Ok(None);
        };

        let content_type = content_type
            .to_str()
            .map_err(|error| Error::BadRequest(format!("Invalid Content-Type header: {error}")))?;

        MediaType::new(content_type)
            .map(Some)
            .map_err(|error| Error::BadRequest(error.to_string()))
    }

    /// Reads `X-Angos-Source-Timestamp` as an RFC 3339 instant for
    /// receiver-side last-writer-wins; a missing or malformed value yields
    /// `None`, disabling LWW rather than failing the request. A future-dated
    /// value is clamped to now so this client-settable header cannot pin a LWW
    /// win or postdate the stored `created_at`.
    pub fn source_timestamp(&self) -> Option<DateTime<Utc>> {
        let value = self
            .headers
            .get(X_ANGOS_SOURCE_TIMESTAMP)?
            .to_str()
            .ok()?
            .trim();
        if value.is_empty() {
            return None;
        }

        let parsed = DateTime::parse_from_rfc3339(value)
            .ok()?
            .with_timezone(&Utc);
        Some(parsed.min(Utc::now()))
    }

    pub fn range(&self, header: HeaderName) -> Result<Option<(u64, Option<u64>)>, Error> {
        let Some(range_header) = self.header_string(header)? else {
            return Ok(None);
        };

        parse_start_end_range(&range_header).map(Some)
    }

    pub fn blob_range(&self) -> Result<Option<BlobRange>, Error> {
        let Some(range_header) = self.header_string(RANGE)? else {
            return Ok(None);
        };

        let Some(range_value) = strip_bytes_prefix(&range_header) else {
            return Err(invalid_range_header(&range_header));
        };

        if range_value.contains(',') {
            return Ok(None);
        }

        if START_END_RANGE_RE.is_match(range_value) {
            let (start, end) = parse_start_end_range(range_value)?;
            return Ok(Some(BlobRange::FromTo { start, end }));
        }

        if SUFFIX_RANGE_RE.is_match(range_value) {
            let suffix = parse_suffix_range(range_value)?;
            return Ok(Some(BlobRange::Suffix(suffix)));
        }

        Err(invalid_range_header(&range_header))
    }

    fn header_string(&self, header: HeaderName) -> Result<Option<String>, Error> {
        let Some(value) = self.headers.get(header) else {
            return Ok(None);
        };

        let value = value
            .to_str()
            .map_err(|error| Error::BadRequest(format!("Invalid header value: {error}")))?;

        Ok(Some(value.to_string()))
    }

    fn authorization_parameter(&self, scheme: &str) -> Option<&str> {
        let value = self.headers.get(AUTHORIZATION)?.to_str().ok()?.trim_start();
        let (actual_scheme, parameter) = value.split_once(char::is_whitespace)?;

        actual_scheme
            .eq_ignore_ascii_case(scheme)
            .then_some(parameter.trim_start())
    }
}

#[derive(Debug, PartialEq, Eq)]
struct AcceptMediaRange {
    value: String,
    quality: u16,
    order: usize,
}

fn strip_bytes_prefix(range_header: &str) -> Option<&str> {
    if range_header.len() < BYTES_RANGE_PREFIX.len() {
        return None;
    }

    let (prefix, range_value) = range_header.split_at(BYTES_RANGE_PREFIX.len());
    prefix
        .eq_ignore_ascii_case(BYTES_RANGE_PREFIX)
        .then_some(range_value)
}

fn invalid_range_header(range_header: &str) -> Error {
    let msg = format!("Invalid Range header format: '{range_header}'");
    Error::RangeNotSatisfiable(msg)
}

fn parse_start_end_range(range_header: &str) -> Result<(u64, Option<u64>), Error> {
    let captures = START_END_RANGE_RE
        .captures(range_header)
        .ok_or_else(|| invalid_range_header(range_header))?;

    let (Some(start), end) = (captures.name("start"), captures.name("end")) else {
        return Err(invalid_range_header(range_header));
    };

    let start = start.as_str().parse::<u64>().map_err(|error| {
        let msg = format!("Error parsing 'start' in Range header: {error}");
        Error::RangeNotSatisfiable(msg)
    })?;

    if let Some(end) = end {
        let end = end.as_str().parse::<u64>().map_err(|error| {
            let msg = format!("Error parsing 'end' in Range header: {error}");
            Error::RangeNotSatisfiable(msg)
        })?;

        if start > end {
            let msg = format!("Invalid Range header: start ({start}) > end ({end})");
            return Err(Error::RangeNotSatisfiable(msg));
        }

        Ok((start, Some(end)))
    } else {
        Ok((start, None))
    }
}

fn parse_suffix_range(range_header: &str) -> Result<u64, Error> {
    let captures = SUFFIX_RANGE_RE
        .captures(range_header)
        .ok_or_else(|| invalid_range_header(range_header))?;

    let Some(suffix) = captures.name("suffix") else {
        return Err(invalid_range_header(range_header));
    };

    suffix.as_str().parse::<u64>().map_err(|error| {
        let msg = format!("Error parsing 'suffix' in Range header: {error}");
        Error::RangeNotSatisfiable(msg)
    })
}

fn parse_content_length(value: &HeaderValue) -> Result<u64, Error> {
    let value = value
        .to_str()
        .map_err(|error| Error::BadRequest(format!("Invalid Content-Length header: {error}")))?;

    value
        .trim()
        .parse::<u64>()
        .map_err(|error| Error::BadRequest(format!("Invalid Content-Length header value: {error}")))
}

fn quality_for_media_range(media_range: &str) -> u16 {
    for parameter in media_range.split(';').skip(1) {
        let Some((name, value)) = parameter.trim().split_once('=') else {
            continue;
        };

        if name.trim().eq_ignore_ascii_case(QUALITY_PARAM) {
            return parse_quality(value.trim()).unwrap_or(0);
        }
    }

    1000
}

fn parse_quality(value: &str) -> Option<u16> {
    let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));

    match whole {
        "1" if fraction.chars().all(|digit| digit == '0') && fraction.len() <= 3 => Some(1000),
        "0" if fraction.chars().all(|digit| digit.is_ascii_digit()) && fraction.len() <= 3 => {
            let mut quality = 0;
            for index in 0..3 {
                quality *= 10;
                if let Some(digit) = fraction.as_bytes().get(index) {
                    quality += u16::from(*digit - b'0');
                }
            }

            Some(quality)
        }
        _ => None,
    }
}

pub fn incoming_into_async_read(incoming: Incoming) -> impl AsyncRead {
    let stream = incoming.into_data_stream().map_err(io::Error::other);
    StreamReader::new(stream)
}

#[cfg(test)]
mod tests;
