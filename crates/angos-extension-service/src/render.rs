//! Spec-correct hyper rendering for the `_angos/` responses, behind `hyper`.

use http::header::{
    CONTENT_DISPOSITION, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, VARY,
};
use http::{HeaderMap, HeaderValue, Response, StatusCode};
use serde::Serialize;
use tokio::io::AsyncRead;

use angos_oci::server;
use angos_transport::{ResponseBody, build_response, json_headers, json_response};

use crate::{
    FailedJobsBody, JobsBody, LayerEntries, LayerFile, LayerFileDetails, NamespacesBody, NoContent,
    PullsBody, RepositoriesBody, RevisionsBody, UploadsBody,
};

use angos_transport::RenderError;

type Rendered = Result<Response<ResponseBody>, RenderError>;

/// `200 OK` JSON of a plain info body.
macro_rules! json_body {
    ($($ty:ty),+ $(,)?) => {$(
        impl $ty {
            /// `200 OK` with this body as JSON.
            ///
            /// # Errors
            /// Fails when the body cannot be serialized.
            pub fn into_response(self) -> Rendered {
                json_response(StatusCode::OK, &self)
            }
        }
    )+};
}

json_body!(
    RepositoriesBody,
    NamespacesBody,
    RevisionsBody,
    UploadsBody,
    PullsBody,
    LayerFileDetails,
);

/// `200 OK` JSON, with a `Link` to the next page when the listing has more.
///
/// # Errors
/// Fails when the body cannot be serialized or a header value built.
fn paginated_json(body: &impl Serialize, next: Option<&str>) -> Rendered {
    Ok(build_response(
        StatusCode::OK,
        server::paginated_json_headers(next)?,
        ResponseBody::fixed(serde_json::to_vec(body)?),
    )?)
}

impl JobsBody {
    /// `200 OK` JSON, with a `Link` to the next page when the listing has more.
    ///
    /// # Errors
    /// Fails when the body cannot be serialized or a header value built.
    pub fn into_response(self) -> Rendered {
        paginated_json(&self, self.next.as_deref())
    }
}

impl FailedJobsBody {
    /// `200 OK` JSON, with a `Link` to the next page when the listing has more.
    ///
    /// # Errors
    /// Fails when the body cannot be serialized or a header value built.
    pub fn into_response(self) -> Rendered {
        paginated_json(&self, self.next.as_deref())
    }
}

impl NoContent {
    /// `204 No Content`, empty.
    ///
    /// # Errors
    /// Fails only if the response cannot be assembled.
    pub fn into_response(self) -> Rendered {
        Ok(build_response(
            StatusCode::NO_CONTENT,
            HeaderMap::new(),
            ResponseBody::empty(),
        )?)
    }
}

impl LayerEntries {
    /// `200 OK` with the listing, marked `Content-Encoding: gzip` when it is
    /// gzipped, or `202 Accepted` while the layer is still being indexed.
    ///
    /// # Errors
    /// Fails when the response cannot be built.
    pub fn into_response(self) -> Rendered {
        match self {
            LayerEntries::Indexing => Ok(build_response(
                StatusCode::ACCEPTED,
                HeaderMap::new(),
                ResponseBody::empty(),
            )?),
            LayerEntries::Ready { body, gzip } => {
                let mut headers = json_headers();
                headers.insert(VARY, HeaderValue::from_static("accept-encoding"));
                if gzip {
                    headers.insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
                }
                Ok(build_response(
                    StatusCode::OK,
                    headers,
                    ResponseBody::fixed(body),
                )?)
            }
        }
    }
}

impl<R: AsyncRead + Send + 'static> LayerFile<R> {
    /// `200 OK` streaming the file, or `206 Partial Content` the range asked
    /// for, with its `Content-Type` and byte `Content-Length`, sandboxed, plus a
    /// `Content-Disposition: attachment` when a download was asked for.
    /// `frame_size` is the streamed read-buffer size.
    ///
    /// # Errors
    /// Fails when a header value cannot be built.
    pub fn into_response(self, frame_size: usize) -> Rendered {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::try_from(self.content_type)?);
        headers.insert(CONTENT_LENGTH, self.size.into());
        server::sandbox(&mut headers);
        if self.download {
            let name = self
                .path
                .rsplit('/')
                .next()
                .unwrap_or(&self.path)
                .replace('"', "");
            headers.insert(
                CONTENT_DISPOSITION,
                HeaderValue::try_from(format!("attachment; filename=\"{name}\""))?,
            );
        }
        let status = match self.range {
            Some(range) => {
                headers.insert(CONTENT_RANGE, HeaderValue::try_from(range)?);
                StatusCode::PARTIAL_CONTENT
            }
            None => StatusCode::OK,
        };
        Ok(build_response(
            status,
            headers,
            ResponseBody::streaming(self.reader, frame_size),
        )?)
    }
}
