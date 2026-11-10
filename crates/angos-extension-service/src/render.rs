use http::header::{
    CONTENT_DISPOSITION, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, VARY,
};
use http::{HeaderMap, HeaderValue, Response, StatusCode};
use tokio::io::AsyncRead;

use angos_oci::server;
use angos_oci_service::render::paginated_json;
use angos_transport::{RenderError, ResponseBody, build_response, json_headers, json_response};

use crate::{
    FailedJobsBody, JobsBody, LayerEntries, LayerFile, LayerFileDetails, NamespacesBody, PullsBody,
    RepositoriesBody, RevisionsBody, UploadsBody,
};

type Rendered = Result<Response<ResponseBody>, RenderError>;

macro_rules! json_body {
    ($($ty:ty),+ $(,)?) => {$(
        impl $ty {
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

impl JobsBody {
    pub fn into_response(self) -> Rendered {
        paginated_json(&self, self.next.as_deref())
    }
}

impl FailedJobsBody {
    pub fn into_response(self) -> Rendered {
        paginated_json(&self, self.next.as_deref())
    }
}

impl LayerEntries {
    pub fn into_response(self) -> Rendered {
        match self {
            LayerEntries::Indexing => Ok(build_response(
                StatusCode::ACCEPTED,
                HeaderMap::new(),
                ResponseBody::Empty,
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
