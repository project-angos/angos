//! Spec-correct hyper rendering for the catalog response, behind `hyper`.

use http::Response;

use angos_oci_service::render::paginated_json;
use angos_transport::{RenderError, ResponseBody};

use crate::Catalog;

impl Catalog {
    /// `200 OK` JSON `{ "repositories": [...] }`, with a `Link` to the next page
    /// when the listing has more.
    ///
    /// # Errors
    /// Fails when the body cannot be serialized or a header value built.
    pub fn into_response(self) -> Result<Response<ResponseBody>, RenderError> {
        paginated_json(&self, self.next.as_deref())
    }
}
