//! Shared test fixtures. Key material is generated once per process; fixtures
//! needing registry-private items live in `crate::registry::test_utils`.

pub mod client;
pub mod configuration;
pub mod logging;
pub mod mocks;
pub mod mtls;
pub mod oidc;
pub mod requests;
pub mod tls;
pub mod webhook;
