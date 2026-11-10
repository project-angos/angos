//! The one S3 test connection every workspace integration test uses, shared
//! with downstream crates through the `test-util` feature.

use std::env;

use crate::BackendConfig;

pub const TEST_ACCESS_KEY: &str = "root";
pub const TEST_SECRET_KEY: &str = "roottoor";
/// Bucket every S3 integration test writes into, namespaced by `key_prefix`.
pub const TEST_BUCKET: &str = "registry";
pub const TEST_REGION: &str = "region";

/// `ANGOS_TEST_S3_ENDPOINT` overrides the CI default for hosts where that port
/// is taken.
#[must_use]
pub fn test_endpoint() -> String {
    env::var("ANGOS_TEST_S3_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:9000".to_string())
}

/// Config for the live test backend, isolated under `key_prefix`.
#[must_use]
pub fn integration_config(key_prefix: String) -> BackendConfig {
    BackendConfig {
        access_key_id: TEST_ACCESS_KEY.to_string(),
        secret_key: TEST_SECRET_KEY.to_string(),
        endpoint: test_endpoint(),
        bucket: TEST_BUCKET.to_string(),
        region: TEST_REGION.to_string(),
        key_prefix,
        ..BackendConfig::default()
    }
}

/// Config for unit tests that never reach a live backend.
#[must_use]
pub fn mock_config(endpoint: String) -> BackendConfig {
    BackendConfig {
        access_key_id: "key".to_string(),
        secret_key: "secret".to_string(),
        endpoint,
        bucket: "test-bucket".to_string(),
        region: "us-east-1".to_string(),
        ..BackendConfig::default()
    }
}
