use std::io;

use chrono::{DateTime, Utc};
use futures_util::TryStreamExt;
use http_body_util::BodyExt;
use hyper::{
    HeaderMap,
    body::Incoming,
    header::{CONTENT_LENGTH, CONTENT_TYPE, HeaderName, HeaderValue, RANGE},
};
use tokio::io::AsyncRead;
use tokio_util::io::StreamReader;

use angos_oci::http_range::{ByteWindow, RequestRange};
use angos_oci::server;
use angos_oci::{MediaRange, MediaType};
use angos_oci_client::X_ANGOS_SOURCE_TIMESTAMP;

use crate::command::server::error::Error;

/// Set by the web UI to force an inline body instead of a presigned S3
/// redirect: a browser `fetch` cannot follow the cross-origin redirect (the
/// presigned URL carries no CORS headers) and loses `Docker-Content-Digest`.
/// OCI clients never send it, so their redirect fast path is unaffected.
pub const X_ANGOS_NO_REDIRECT: &str = "X-Angos-No-Redirect";

#[derive(Clone, Debug)]
pub struct RequestHeaders<'a> {
    headers: &'a HeaderMap,
}

impl<'a> RequestHeaders<'a> {
    pub fn new(headers: &'a HeaderMap) -> Self {
        Self { headers }
    }

    pub fn accepted_content_types(&self) -> Vec<MediaRange> {
        server::accepted_content_types(self.headers)
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

    /// Whether the client asked to be served the body inline rather than a
    /// presigned S3 redirect (see [`X_ANGOS_NO_REDIRECT`]). Any non-empty value
    /// other than `0`/`false` disables the redirect.
    pub fn redirect_suppressed(&self) -> bool {
        let Some(value) = self.headers.get(X_ANGOS_NO_REDIRECT) else {
            return false;
        };
        let value = value.to_str().unwrap_or("").trim();
        !value.is_empty()
            && !value.eq_ignore_ascii_case("0")
            && !value.eq_ignore_ascii_case("false")
    }

    pub fn content_type(&self) -> Result<Option<MediaType>, Error> {
        let Some(content_type) = self.headers.get(CONTENT_TYPE) else {
            return Ok(None);
        };

        let content_type = content_type
            .to_str()
            .map_err(|error| Error::BadRequest(format!("Invalid Content-Type header: {error}")))?;

        MediaType::from_content_type(content_type)
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

    /// The window carried by `header`, spelled `<start>-<end>` as end-5 has a
    /// chunk declare it.
    pub fn chunk_range(&self, header: HeaderName) -> Result<Option<ByteWindow>, Error> {
        let Some(range_header) = self.header_string(header)? else {
            return Ok(None);
        };

        ByteWindow::chunk_bounds(&range_header)
            .map(|(start, end)| {
                Some(ByteWindow {
                    start,
                    end: Some(end),
                })
            })
            .map_err(|error| Error::RangeNotSatisfiable(error.to_string()))
    }

    /// The window a blob `GET` asks for, `None` when it names none, names
    /// several, or names one this server cannot read: RFC 9110 has a `Range`
    /// whose unit is unknown or whose syntax does not parse ignored and the
    /// whole representation served. A `416` is for a range that parses and
    /// cannot be met.
    pub fn blob_range(&self) -> Result<Option<RequestRange>, Error> {
        let Some(range_header) = self.header_string(RANGE)? else {
            return Ok(None);
        };

        Ok(RequestRange::parse(&range_header).ok().flatten())
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

pub fn incoming_into_async_read(incoming: Incoming) -> impl AsyncRead {
    let stream = incoming.into_data_stream().map_err(io::Error::other);
    StreamReader::new(stream)
}

#[cfg(test)]
mod tests;
