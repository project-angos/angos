//! The HTTP transport layer: the hyper response body and its builders, the one
//! place in the workspace allowed to own hyper. Every HTTP surface renders
//! through it, the OCI service and extension crates that answer the protocol's
//! own operations as much as the server surface that answers the ones which
//! never reach the registry (errors, tokens, UI assets, probes).

use std::{
    fmt, io,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use http::{HeaderMap, HeaderValue, Response, StatusCode, header::CONTENT_TYPE};
use http_body_util::{Full, StreamBody};
use hyper::body::{Body, Frame};
use serde::Serialize;
use tokio::io::AsyncRead;
use tokio_util::io::ReaderStream;

type BytesFrameStream = Pin<Box<dyn Stream<Item = Result<Frame<Bytes>, io::Error>> + Send>>;

/// A rendering failure: a header value the spec would carry cannot be built, or
/// a JSON body cannot be serialized. Both are internal faults the transport
/// answers `500` for. Shared by every response builder, in the registry and the
/// service and extension crates alike.
#[derive(Debug)]
pub enum RenderError {
    Header(http::Error),
    Serialize(serde_json::Error),
}

impl From<http::header::InvalidHeaderValue> for RenderError {
    fn from(e: http::header::InvalidHeaderValue) -> Self {
        RenderError::Header(e.into())
    }
}

impl From<http::Error> for RenderError {
    fn from(e: http::Error) -> Self {
        RenderError::Header(e)
    }
}

impl From<serde_json::Error> for RenderError {
    fn from(e: serde_json::Error) -> Self {
        RenderError::Serialize(e)
    }
}

impl fmt::Display for RenderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RenderError::Header(e) => write!(f, "response headers: {e}"),
            RenderError::Serialize(e) => write!(f, "response body: {e}"),
        }
    }
}

impl std::error::Error for RenderError {}

/// The headers of a plain JSON response, the shape most non-OCI endpoints
/// serve.
pub fn json_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

    headers
}

/// A JSON response under [`json_headers`]. Generic over the caller's error so
/// the registry and the server surface share one builder.
/// # Errors
/// Propagates serialization and response-build failures via `E`.
pub fn json_response<T, E>(status: StatusCode, body: &T) -> Result<Response<ResponseBody>, E>
where
    T: Serialize,
    E: From<http::Error> + From<serde_json::Error>,
{
    Ok(build_response(
        status,
        json_headers(),
        ResponseBody::fixed(serde_json::to_vec(body)?),
    )?)
}

pub enum ResponseBody {
    Empty,
    Fixed(Full<Bytes>),
    Streaming(StreamBody<BytesFrameStream>),
}

impl ResponseBody {
    #[must_use]
    pub fn empty() -> Self {
        ResponseBody::Empty
    }

    #[must_use]
    pub fn fixed(data: Vec<u8>) -> Self {
        let data = Bytes::from(data);
        ResponseBody::Fixed(Full::new(data))
    }

    /// `frame_size` is the read buffer each frame is filled from, and must be
    /// non-zero: a zero-capacity buffer reads nothing, which ends the body
    /// immediately instead of failing.
    pub fn streaming<R>(reader: R, frame_size: usize) -> Self
    where
        R: AsyncRead + Send + 'static,
    {
        let stream =
            ReaderStream::with_capacity(reader, frame_size).map(|result| result.map(Frame::data));
        ResponseBody::Streaming(StreamBody::new(Box::pin(stream)))
    }
}

impl fmt::Debug for ResponseBody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            ResponseBody::Empty => "Empty",
            ResponseBody::Fixed(_) => "Fixed",
            ResponseBody::Streaming(_) => "Streaming",
        })
    }
}

impl Body for ResponseBody {
    type Data = Bytes;
    type Error = io::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        match self.get_mut() {
            ResponseBody::Empty => Poll::Ready(None),
            ResponseBody::Fixed(body) => Pin::new(body).poll_frame(cx).map_err(io::Error::other),
            ResponseBody::Streaming(body) => Pin::new(body).poll_frame(cx),
        }
    }
}

/// # Errors
/// Fails when the status/headers cannot form a valid response.
pub fn build_response(
    status: StatusCode,
    headers: HeaderMap,
    body: ResponseBody,
) -> Result<Response<ResponseBody>, http::Error> {
    let mut response = Response::builder().status(status).body(body)?;
    *response.headers_mut() = headers;

    Ok(response)
}

#[cfg(test)]
mod tests {
    use http_body_util::BodyExt;

    use super::ResponseBody;

    /// Frames come off a buffer of the configured size, so serving a blob costs
    /// one frame per that many bytes rather than per tokio-util's 4 KiB default.
    #[tokio::test]
    async fn a_streamed_body_frames_at_the_configured_size() {
        let mut body = ResponseBody::streaming(&b"0123456789abcdef"[..], 4);

        let mut frames = Vec::new();
        while let Some(frame) = body.frame().await {
            if let Ok(data) = frame.expect("frame").into_data() {
                frames.push(data);
            }
        }

        assert_eq!(frames, [&b"0123"[..], b"4567", b"89ab", b"cdef"]);
    }
}
