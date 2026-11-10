pub mod authenticator;
pub mod authorization;
pub mod authorizer;
pub mod basic_auth;
mod error;
pub mod mtls;
pub mod oidc;
pub mod token_service;
pub mod webhook;

pub use authenticator::Authenticator;
pub use authorizer::Authorizer;
pub use basic_auth::BasicAuthValidator;
pub use error::Error;
pub use mtls::PeerCertificate;
pub use oidc::OidcValidator;
pub use token_service::{TokenIssuer, TokenValidator};

/// What one authentication method found; credentials present but invalid are
/// an `Err` instead.
#[derive(Debug)]
pub enum AuthResult {
    Authenticated,
    NoCredentials,
}
