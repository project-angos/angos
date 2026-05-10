use std::collections::HashMap;

pub const DOCKER_DISTRIBUTION_API_VERSION: &str = "Docker-Distribution-API-Version";
pub const X_POWERED_BY: &str = "X-Powered-By";

#[derive(Debug)]
pub struct ApiVersionResponse {
    pub headers: HashMap<&'static str, String>,
}

impl Default for ApiVersionResponse {
    fn default() -> Self {
        Self {
            headers: HashMap::from([
                (DOCKER_DISTRIBUTION_API_VERSION, "registry/2.0".to_string()),
                (X_POWERED_BY, "Angos".to_string()),
            ]),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_contains_expected_header_values() {
        let response = ApiVersionResponse::default();
        assert_eq!(
            response
                .headers
                .get(DOCKER_DISTRIBUTION_API_VERSION)
                .map(String::as_str),
            Some("registry/2.0"),
        );
        assert_eq!(
            response.headers.get(X_POWERED_BY).map(String::as_str),
            Some("Angos"),
        );
    }

    #[test]
    fn default_contains_exactly_two_headers() {
        let response = ApiVersionResponse::default();
        assert_eq!(response.headers.len(), 2);
    }
}
