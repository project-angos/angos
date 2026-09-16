//! `into_response` for every OCI response type: the spec's status, headers and
//! body, built from [`angos_oci::server`] and [`angos_transport`]. Enabled
//! by the `hyper` feature; the transport calls these and never hand-builds a
//! header.

use http::header::InvalidHeaderValue;
use http::{HeaderMap, HeaderName, HeaderValue, Response, StatusCode};
use tokio::io::AsyncRead;

use angos_oci::server;
use angos_transport::{ResponseBody, build_response};

use crate::{
    Accepted, ApiVersion, BlobDescriptor, BlobGet, BlobStream, BlobWritten, ManifestDescriptor,
    ManifestGet, ManifestWritten, NoContent, Referrers, StartUpload, Tags, UploadSession,
};

use angos_transport::RenderError;

type Rendered = Result<Response<ResponseBody>, RenderError>;

/// The non-standard branding header `GET /v2/` may carry.
const X_POWERED_BY: HeaderName = HeaderName::from_static("x-powered-by");

impl ApiVersion {
    /// `200 OK` with the `Docker-Distribution-API-Version` header, and the
    /// caller's `X-Powered-By` when one was set.
    ///
    /// # Errors
    /// Fails when the `X-Powered-By` value cannot be carried by a header.
    pub fn into_response(self) -> Rendered {
        let mut headers = server::api_version_headers();
        if let Some(powered_by) = self.powered_by {
            headers.insert(X_POWERED_BY, HeaderValue::try_from(powered_by)?);
        }
        Ok(build_response(
            StatusCode::OK,
            headers,
            ResponseBody::empty(),
        )?)
    }
}

impl Accepted {
    /// `202 Accepted`, empty.
    ///
    /// # Errors
    /// Fails only if the response cannot be assembled.
    pub fn into_response(self) -> Rendered {
        Ok(build_response(
            StatusCode::ACCEPTED,
            HeaderMap::new(),
            ResponseBody::empty(),
        )?)
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

impl ManifestGet {
    /// `200 OK` with the bytes, or `307` to the redirect, `Docker-Content-Digest`
    /// on both.
    ///
    /// # Errors
    /// Fails when a header value cannot be built.
    pub fn into_response(self) -> Rendered {
        match self {
            ManifestGet::Content {
                digest,
                media_type,
                bytes,
            } => Ok(build_response(
                StatusCode::OK,
                server::manifest_headers(media_type.as_ref(), &digest, bytes.len() as u64)?,
                ResponseBody::fixed(bytes),
            )?),
            ManifestGet::Redirect {
                digest,
                media_type,
                location,
            } => Ok(build_response(
                StatusCode::TEMPORARY_REDIRECT,
                server::manifest_redirect_headers(&location, &digest, media_type.as_ref())?,
                ResponseBody::empty(),
            )?),
        }
    }
}

impl ManifestDescriptor {
    /// `200 OK`, manifest headers, no body.
    ///
    /// # Errors
    /// Fails when a header value cannot be built.
    pub fn into_response(self) -> Rendered {
        Ok(build_response(
            StatusCode::OK,
            server::manifest_headers(self.media_type.as_ref(), &self.digest, self.length)?,
            ResponseBody::empty(),
        )?)
    }
}

impl ManifestWritten {
    /// `201 Created` with `Location`, `Docker-Content-Digest`, `OCI-Subject` and
    /// `OCI-Tag`.
    ///
    /// # Errors
    /// Fails when a header value cannot be built.
    pub fn into_response(self) -> Rendered {
        Ok(build_response(
            StatusCode::CREATED,
            server::put_manifest_headers(
                &self.namespace,
                &self.reference,
                &self.digest,
                self.subject.as_ref(),
                &self.created_tags,
            )?,
            ResponseBody::empty(),
        )?)
    }
}

impl<R: AsyncRead + Send + 'static> BlobStream<R> {
    /// `200 OK`, or `206 Partial Content` when a range is set, streaming the
    /// blob. `frame_size` is the streamed body's read-buffer size.
    ///
    /// # Errors
    /// Fails when a header value cannot be built.
    pub fn into_response(self, frame_size: usize) -> Rendered {
        let (status, headers) = match self.range {
            None => (
                StatusCode::OK,
                server::blob_headers(&self.digest, self.total_length)?,
            ),
            Some(range) => (
                StatusCode::PARTIAL_CONTENT,
                server::partial_blob_headers(&self.digest, self.total_length, range)?,
            ),
        };
        Ok(build_response(
            status,
            headers,
            ResponseBody::streaming(self.reader, frame_size),
        )?)
    }
}

impl<R: AsyncRead + Send + 'static> BlobGet<R> {
    /// `200`/`206` streaming the blob, or `307` to the redirect. `frame_size`
    /// is the streamed body's read-buffer size.
    ///
    /// # Errors
    /// Fails when a header value cannot be built.
    pub fn into_response(self, frame_size: usize) -> Rendered {
        match self {
            BlobGet::Content(stream) => stream.into_response(frame_size),
            BlobGet::Redirect { digest, location } => Ok(build_response(
                StatusCode::TEMPORARY_REDIRECT,
                server::blob_redirect_headers(&location, &digest)?,
                ResponseBody::empty(),
            )?),
        }
    }
}

impl BlobDescriptor {
    /// `200 OK`, blob headers, no body.
    ///
    /// # Errors
    /// Fails when a header value cannot be built.
    pub fn into_response(self) -> Rendered {
        Ok(build_response(
            StatusCode::OK,
            server::blob_headers(&self.digest, self.size)?,
            ResponseBody::empty(),
        )?)
    }
}

impl BlobWritten {
    /// `201 Created` with the blob's canonical `Location`.
    ///
    /// # Errors
    /// Fails when a header value cannot be built.
    pub fn into_response(self) -> Rendered {
        Ok(build_response(
            StatusCode::CREATED,
            server::blob_location_headers(&self.namespace, &self.digest)?,
            ResponseBody::empty(),
        )?)
    }
}

impl UploadSession {
    fn headers(&self) -> Result<HeaderMap, InvalidHeaderValue> {
        if self.received == 0 {
            server::upload_session_headers(&self.namespace, &self.session_id)
        } else {
            server::upload_progress_headers(&self.namespace, &self.session_id, self.received, None)
        }
    }

    /// `202 Accepted` with `Location` and `Range`, for an opened or extended
    /// session.
    ///
    /// # Errors
    /// Fails when a header value cannot be built.
    pub fn into_open_response(self) -> Rendered {
        Ok(build_response(
            StatusCode::ACCEPTED,
            self.headers()?,
            ResponseBody::empty(),
        )?)
    }

    /// `204 No Content` with `Location` and `Range`, the answer to a status
    /// query.
    ///
    /// # Errors
    /// Fails when a header value cannot be built.
    pub fn into_status_response(self) -> Rendered {
        Ok(build_response(
            StatusCode::NO_CONTENT,
            self.headers()?,
            ResponseBody::empty(),
        )?)
    }
}

impl StartUpload {
    /// `202` for an open session, or `201` for a one-shot completed push.
    ///
    /// # Errors
    /// Fails when a header value cannot be built.
    pub fn into_response(self) -> Rendered {
        match self {
            StartUpload::Session(session) => session.into_open_response(),
            StartUpload::Completed(blob) => (*blob).into_response(),
        }
    }
}

impl Tags {
    /// `200 OK` JSON, with a `Link` to the next page when the listing has more.
    ///
    /// # Errors
    /// Fails when the body cannot be serialized or a header value built.
    pub fn into_response(self) -> Rendered {
        Ok(build_response(
            StatusCode::OK,
            server::paginated_json_headers(self.next.as_deref())?,
            ResponseBody::fixed(serde_json::to_vec(&self.list)?),
        )?)
    }
}

impl Referrers {
    /// `200 OK` serving the referrers image index, with the filter flag and the
    /// `Link` to the next page.
    ///
    /// # Errors
    /// Fails when the index cannot be serialized or a header value built.
    pub fn into_response(self) -> Rendered {
        Ok(build_response(
            StatusCode::OK,
            server::referrers_headers(self.filtered, self.next.as_deref())?,
            ResponseBody::fixed(serde_json::to_vec(&self.index)?),
        )?)
    }
}
