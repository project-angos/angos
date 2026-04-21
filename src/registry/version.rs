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
