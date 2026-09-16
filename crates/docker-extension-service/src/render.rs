//! Spec-correct hyper rendering for the catalog response, behind `hyper`.

use http::{Response, StatusCode};

use angos_oci::server;
use angos_transport::{ResponseBody, build_response};

use crate::Catalog;

use angos_transport::RenderError;

impl Catalog {
    /// `200 OK` JSON `{ "repositories": [...] }`, with a `Link` to the next page
    /// when the listing has more.
    ///
    /// # Errors
    /// Fails when the body cannot be serialized or a header value built.
    pub fn into_response(self) -> Result<Response<ResponseBody>, RenderError> {
        Ok(build_response(
            StatusCode::OK,
            server::paginated_json_headers(self.next.as_deref())?,
            ResponseBody::fixed(serde_json::to_vec(&self)?),
        )?)
    }
}
