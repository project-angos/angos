use std::{io, str::FromStr, sync::LazyLock};

use base64::{Engine, prelude::BASE64_STANDARD};
use futures_util::TryStreamExt;
use http_body_util::BodyExt;
use hyper::{
    body::Incoming,
    header::{ACCEPT, AUTHORIZATION, AsHeaderName, HeaderName},
    http::{request, response},
};
use regex::Regex;
use tokio::io::AsyncRead;
use tokio_util::io::StreamReader;

use crate::command::server::error::Error;

static RANGE_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^(?:bytes=)?(?P<start>\d+)-(?P<end>\d+)?$").unwrap());

static BEARER_PREFIX: &str = "Bearer ";
static BASIC_PREFIX: &str = "Basic ";

pub trait HeaderExt {
    fn headers(&self) -> &hyper::HeaderMap;

    fn get_header<K: AsHeaderName>(&self, header: K) -> Option<String> {
        self.headers()
            .get(header)
            .and_then(|header| header.to_str().ok())
            .map(ToString::to_string)
    }

    fn parse_header<T: FromStr, K: AsHeaderName>(&self, header: K) -> Option<T> {
        self.headers()
            .get(header)
            .and_then(|header| header.to_str().ok())
            .and_then(|s| s.parse().ok())
    }

    fn range(&self, header: HeaderName) -> Result<Option<(u64, Option<u64>)>, Error> {
        let Some(range_header) = self.get_header(header) else {
            return Ok(None);
        };

        let captures = RANGE_RE
            .captures(&range_header)
            .ok_or_else(|| invalid_range_header(&range_header))?;

        let (Some(start), end) = (captures.name("start"), captures.name("end")) else {
            return Err(invalid_range_header(&range_header));
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

    fn accepted_content_types(&self) -> Vec<String> {
        self.headers()
            .get_all(ACCEPT)
            .iter()
            .filter_map(|h| h.to_str().ok())
            .map(ToString::to_string)
            .collect()
    }

    fn bearer_token(&self) -> Option<String> {
        let authorization = self.get_header(AUTHORIZATION)?;
        authorization
            .strip_prefix(BEARER_PREFIX)
            .map(str::to_string)
    }

    fn basic_auth(&self) -> Option<(String, String)> {
        let authorization = self.get_header(AUTHORIZATION)?;

        let value = authorization.strip_prefix(BASIC_PREFIX)?;
        let value = BASE64_STANDARD.decode(value).ok()?;
        let value = String::from_utf8(value).ok()?;

        let (username, password) = value.split_once(':')?;
        Some((username.to_string(), password.to_string()))
    }
}

fn invalid_range_header(header: &str) -> Error {
    let msg = format!("Invalid Range header format: '{header}'");
    Error::RangeNotSatisfiable(msg)
}

impl HeaderExt for request::Parts {
    fn headers(&self) -> &hyper::HeaderMap {
        &self.headers
    }
}

impl HeaderExt for response::Parts {
    fn headers(&self) -> &hyper::HeaderMap {
        &self.headers
    }
}

pub fn incoming_into_async_read(incoming: Incoming) -> impl AsyncRead {
    let stream = incoming.into_data_stream().map_err(io::Error::other);
    StreamReader::new(stream)
}

#[cfg(test)]
mod tests;
