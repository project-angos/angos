use hyper::{Response, StatusCode};
use tracing::instrument;

use crate::{
    command::server::response_body::ResponseBody,
    registry::{Error, Registry},
};

pub const DOCKER_DISTRIBUTION_API_VERSION: &str = "Docker-Distribution-API-Version";
pub const X_POWERED_BY: &str = "X-Powered-By";

impl Registry {
    #[instrument(skip(self))]
    pub async fn handle_get_api_version(&self) -> Result<Response<ResponseBody>, Error> {
        let res = Response::builder()
            .status(StatusCode::OK)
            .header(DOCKER_DISTRIBUTION_API_VERSION, "registry/2.0")
            .header(X_POWERED_BY, "Angos")
            .body(ResponseBody::empty())?;

        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{command::server::request_ext::HeaderExt, registry::tests::backends};

    #[tokio::test]
    async fn test_handle_get_api_version() {
        for test_case in backends() {
            let response = test_case.registry().handle_get_api_version().await.unwrap();

            assert_eq!(response.status(), StatusCode::OK);
            let (parts, _body) = response.into_parts();

            assert_eq!(
                parts.get_header(DOCKER_DISTRIBUTION_API_VERSION),
                Some("registry/2.0".to_string())
            );
            assert_eq!(parts.get_header(X_POWERED_BY), Some("Angos".to_string()));
        }
    }
}
