use std::{io, str::FromStr, sync::LazyLock};

use base64::{Engine, prelude::BASE64_STANDARD};
use futures_util::TryStreamExt;
use http_body_util::BodyExt;
use hyper::{
    HeaderMap,
    body::Incoming,
    header::{ACCEPT, AUTHORIZATION, AsHeaderName, HeaderName},
};
use regex::Regex;
use tokio::io::AsyncRead;
use tokio_util::io::StreamReader;

use crate::{command::server::error::Error, registry::BlobRange};

static START_END_RANGE_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^(?:bytes=)?(?P<start>\d+)-(?P<end>\d+)?$").unwrap());
static SUFFIX_RANGE_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^(?:bytes=)?-(?P<suffix>\d+)$").unwrap());

static BYTES_RANGE_PREFIX: &str = "bytes=";
static BEARER_PREFIX: &str = "Bearer ";
static BASIC_PREFIX: &str = "Basic ";

pub fn parse_header<T: FromStr, K: AsHeaderName>(headers: &HeaderMap, header: K) -> Option<T> {
    headers
        .get(header)
        .and_then(|header| header.to_str().ok())
        .and_then(|s| s.parse().ok())
}

pub fn range(headers: &HeaderMap, header: HeaderName) -> Result<Option<(u64, Option<u64>)>, Error> {
    let Some(range_header) = parse_header::<String, _>(headers, header) else {
        return Ok(None);
    };

    parse_start_end_range(&range_header).map(Some)
}

pub fn blob_range(headers: &HeaderMap, header: HeaderName) -> Result<Option<BlobRange>, Error> {
    let Some(range_header) = parse_header::<String, _>(headers, header) else {
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

pub fn accepted_content_types(headers: &HeaderMap) -> Vec<String> {
    headers
        .get_all(ACCEPT)
        .iter()
        .filter_map(|h| h.to_str().ok())
        .map(ToString::to_string)
        .collect()
}

pub fn bearer_token(headers: &HeaderMap) -> Option<String> {
    let authorization = parse_header::<String, _>(headers, AUTHORIZATION)?;
    authorization
        .strip_prefix(BEARER_PREFIX)
        .map(str::to_string)
}

pub fn basic_auth(headers: &HeaderMap) -> Option<(String, String)> {
    let authorization = parse_header::<String, _>(headers, AUTHORIZATION)?;

    let value = authorization.strip_prefix(BASIC_PREFIX)?;
    let value = BASE64_STANDARD.decode(value).ok()?;
    let value = String::from_utf8(value).ok()?;

    let (username, password) = value.split_once(':')?;
    Some((username.to_string(), password.to_string()))
}

pub fn incoming_into_async_read(incoming: Incoming) -> impl AsyncRead {
    let stream = incoming.into_data_stream().map_err(io::Error::other);
    StreamReader::new(stream)
}

#[cfg(test)]
mod tests;
