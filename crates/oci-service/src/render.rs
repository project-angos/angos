//! The spec's status, headers and body for every OCI response type.

use http::{HeaderMap, HeaderName, HeaderValue, Response, StatusCode};
use serde::Serialize;
use tokio::io::AsyncRead;

use angos_oci::client::blob_path;
use angos_oci::server;
use angos_transport::{RenderError, ResponseBody, build_response};

use crate::{
    Accepted, ApiVersion, BlobDescriptor, BlobGet, BlobStream, BlobWritten, ManifestDescriptor,
    ManifestGet, ManifestWritten, NoContent, Referrers, StartUpload, Tags, UploadSession,
};

type Rendered = Result<Response<ResponseBody>, RenderError>;

/// `200 OK` JSON, with a `Link` to the next page when the listing has more.
pub fn paginated_json(body: &impl Serialize, next: Option<&str>) -> Rendered {
    Ok(build_response(
        StatusCode::OK,
        server::paginated_json_headers(next)?,
        ResponseBody::fixed(serde_json::to_vec(body)?),
    )?)
}

const X_POWERED_BY: HeaderName = HeaderName::from_static("x-powered-by");

impl ApiVersion {
    pub fn into_response(self) -> Rendered {
        let mut headers = server::api_version_headers();
        if let Some(powered_by) = self.powered_by {
            headers.insert(X_POWERED_BY, HeaderValue::try_from(powered_by)?);
        }
        Ok(build_response(
            StatusCode::OK,
            headers,
            ResponseBody::Empty,
        )?)
    }
}

impl Accepted {
    pub fn into_response(self) -> Rendered {
        Ok(build_response(
            StatusCode::ACCEPTED,
            HeaderMap::new(),
            ResponseBody::Empty,
        )?)
    }
}

impl NoContent {
    pub fn into_response(self) -> Rendered {
        Ok(build_response(
            StatusCode::NO_CONTENT,
            HeaderMap::new(),
            ResponseBody::Empty,
        )?)
    }
}

impl ManifestGet {
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
                ResponseBody::Empty,
            )?),
        }
    }
}

impl ManifestDescriptor {
    pub fn into_response(self) -> Rendered {
        Ok(build_response(
            StatusCode::OK,
            server::manifest_headers(self.media_type.as_ref(), &self.digest, self.length)?,
            ResponseBody::Empty,
        )?)
    }
}

impl ManifestWritten {
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
            ResponseBody::Empty,
        )?)
    }
}

impl<R: AsyncRead + Send + 'static> BlobStream<R> {
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
    pub fn into_response(self, frame_size: usize) -> Rendered {
        match self {
            BlobGet::Content(stream) => stream.into_response(frame_size),
            BlobGet::Redirect { digest, location } => Ok(build_response(
                StatusCode::TEMPORARY_REDIRECT,
                server::blob_redirect_headers(&location, &digest)?,
                ResponseBody::Empty,
            )?),
        }
    }
}

impl BlobDescriptor {
    pub fn into_response(self) -> Rendered {
        Ok(build_response(
            StatusCode::OK,
            server::blob_headers(&self.digest, self.size)?,
            ResponseBody::Empty,
        )?)
    }
}

impl BlobWritten {
    pub fn into_response(self) -> Rendered {
        Ok(build_response(
            StatusCode::CREATED,
            server::blob_redirect_headers(
                &blob_path("", &self.namespace, &self.digest),
                &self.digest,
            )?,
            ResponseBody::Empty,
        )?)
    }
}

impl UploadSession {
    pub fn into_open_response(self) -> Rendered {
        Ok(build_response(
            StatusCode::ACCEPTED,
            server::upload_progress_headers(
                &self.namespace,
                &self.session_id,
                self.received,
                None,
            )?,
            ResponseBody::Empty,
        )?)
    }

    pub fn into_status_response(self) -> Rendered {
        Ok(build_response(
            StatusCode::NO_CONTENT,
            server::upload_progress_headers(
                &self.namespace,
                &self.session_id,
                self.received,
                None,
            )?,
            ResponseBody::Empty,
        )?)
    }
}

impl StartUpload {
    pub fn into_response(self) -> Rendered {
        match self {
            StartUpload::Session(session) => session.into_open_response(),
            StartUpload::Completed(blob) => (*blob).into_response(),
        }
    }
}

impl Tags {
    pub fn into_response(self) -> Rendered {
        paginated_json(&self.list, self.next.as_deref())
    }
}

impl Referrers {
    pub fn into_response(self) -> Rendered {
        Ok(build_response(
            StatusCode::OK,
            server::referrers_headers(self.filtered, self.next.as_deref())?,
            ResponseBody::fixed(serde_json::to_vec(&self.index)?),
        )?)
    }
}
