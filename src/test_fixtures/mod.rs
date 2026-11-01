//! Shared test fixtures: dynamically generated cryptographic material,
//! configuration builders, client configs, and wiremock responders.
//!
//! Every cert, private key, and JWK component used by unit tests is generated
//! at runtime (ECDSA P-256) and cached via `std::sync::LazyLock`. There are no
//! static base64 blobs or shell-out recipes to maintain. Registry-domain
//! fixtures (stores, registries, repositories) live in
//! `crate::registry::test_utils`, which needs access to registry-private
//! items.

pub mod client;
pub mod configuration;
pub mod logging;
pub mod mocks;
pub mod mtls;
pub mod oidc;
pub mod requests;
pub mod tls;
pub mod webhook;
