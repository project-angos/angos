//! Dynamically generated cryptographic test fixtures.
//!
//! Every cert, private key, and JWK component used by unit tests is generated
//! at runtime (ECDSA P-256) and cached via `std::sync::LazyLock`. There are no
//! static base64 blobs or shell-out recipes to maintain.

#[allow(clippy::must_use_candidate)]
pub mod configuration;
#[allow(clippy::must_use_candidate)]
pub mod mtls;
#[allow(clippy::must_use_candidate)]
pub mod oidc;
#[allow(clippy::must_use_candidate)]
pub mod tls;
#[allow(clippy::must_use_candidate)]
pub mod webhook;
