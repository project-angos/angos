//! Shared test fixtures: dynamically generated cryptographic material,
//! configuration builders, client configs, and wiremock responders.
//!
//! Every cert, private key, and JWK component used by unit tests is generated
//! at runtime (ECDSA P-256) and cached via `std::sync::LazyLock`. There are no
//! static base64 blobs or shell-out recipes to maintain. Registry-domain
//! fixtures (stores, registries, repositories) live in
//! `crate::registry::test_utils`, which needs access to registry-private
//! items.

#[allow(clippy::must_use_candidate)]
pub mod client;
#[allow(clippy::must_use_candidate)]
pub mod configuration;
#[allow(clippy::must_use_candidate)]
pub mod events;
#[allow(clippy::must_use_candidate)]
pub mod logging;
#[allow(clippy::must_use_candidate)]
pub mod mocks;
#[allow(clippy::must_use_candidate)]
pub mod mtls;
#[allow(clippy::must_use_candidate)]
pub mod oidc;
#[allow(clippy::must_use_candidate)]
pub mod requests;
#[allow(clippy::must_use_candidate)]
pub mod tls;
#[allow(clippy::must_use_candidate)]
pub mod webhook;
