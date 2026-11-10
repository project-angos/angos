use http::Response;

use angos_oci_service::render::paginated_json;
use angos_transport::{RenderError, ResponseBody};

use crate::Catalog;

impl Catalog {
    pub fn into_response(self) -> Result<Response<ResponseBody>, RenderError> {
        paginated_json(&self, self.next.as_deref())
    }
}
