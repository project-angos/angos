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

use crate::command::server::error::Error;

static RANGE_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^(?:bytes=)?(?P<start>\d+)-(?P<end>\d+)?$").unwrap());

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

    let invalid_range_header = || {
        let msg = format!("Invalid Range header format: '{range_header}'");
        Error::RangeNotSatisfiable(msg)
    };

    let captures = RANGE_RE
        .captures(&range_header)
        .ok_or_else(invalid_range_header)?;

    let (Some(start), end) = (captures.name("start"), captures.name("end")) else {
        return Err(invalid_range_header());
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

        Ok(Some((start, Some(end))))
    } else {
        Ok(Some((start, None)))
    }
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
