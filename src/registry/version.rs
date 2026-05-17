use crate::registry::{HeaderMap, ResponseHeaders};

pub const DOCKER_DISTRIBUTION_API_VERSION: &str = "Docker-Distribution-API-Version";
pub const X_POWERED_BY: &str = "X-Powered-By";

pub fn api_version_response() -> HeaderMap {
    ResponseHeaders::new()
        .with(DOCKER_DISTRIBUTION_API_VERSION, "registry/2.0")
        .with(X_POWERED_BY, "Angos")
        .into_inner()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn response_contains_expected_header_values() {
        let headers = api_version_response();
        assert_eq!(
            headers
                .get(DOCKER_DISTRIBUTION_API_VERSION)
                .map(String::as_str),
            Some("registry/2.0"),
        );
        assert_eq!(headers.get(X_POWERED_BY).map(String::as_str), Some("Angos"),);
    }

    #[test]
    fn response_contains_exactly_two_headers() {
        let headers = api_version_response();
        assert_eq!(headers.len(), 2);
    }
}
