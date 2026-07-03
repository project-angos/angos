//! Shared test fixtures for S3-backed tests.
//!
//! Home of the one canonical connection every workspace S3 integration test
//! uses (rustfs, in CI and locally) so credentials, bucket, and endpoint are
//! declared once. Enabled for this crate's own tests and, through the
//! `test-util` feature, for downstream crates' dev-dependencies.

use std::env;

use crate::BackendConfig;

/// Access key of the workspace S3 test backend.
pub const TEST_ACCESS_KEY: &str = "root";
/// Secret key of the workspace S3 test backend.
pub const TEST_SECRET_KEY: &str = "roottoor";
/// Bucket every S3 integration test writes into, namespaced by `key_prefix`.
pub const TEST_BUCKET: &str = "registry";
/// Region the test backend is addressed with.
pub const TEST_REGION: &str = "region";

/// The live S3 endpoint integration tests talk to, `ANGOS_TEST_S3_ENDPOINT`
/// overriding the CI default of `http://127.0.0.1:9000` for hosts where that
/// port is taken.
#[must_use]
pub fn test_endpoint() -> String {
    env::var("ANGOS_TEST_S3_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:9000".to_string())
}

/// Canonical [`BackendConfig`] for integration tests against the live test
/// backend, isolated under `key_prefix`. Callers override the remaining
/// fields with struct-update syntax.
pub fn integration_config(key_prefix: impl Into<String>) -> BackendConfig {
    BackendConfig {
        access_key_id: TEST_ACCESS_KEY.to_string(),
        secret_key: TEST_SECRET_KEY.to_string(),
        endpoint: test_endpoint(),
        bucket: TEST_BUCKET.to_string(),
        region: TEST_REGION.to_string(),
        key_prefix: key_prefix.into(),
        ..BackendConfig::default()
    }
}

/// Dummy [`BackendConfig`] for unit tests that never reach a live backend
/// (config validation, wiremock servers).
pub fn mock_config(endpoint: impl Into<String>) -> BackendConfig {
    BackendConfig {
        access_key_id: "key".to_string(),
        secret_key: "secret".to_string(),
        endpoint: endpoint.into(),
        bucket: "test-bucket".to_string(),
        region: "us-east-1".to_string(),
        ..BackendConfig::default()
    }
}
