use async_trait::async_trait;
use hyper::http::request::Parts;

use crate::{command::server::Error, identity::ClientIdentity};

/// Result of authentication attempt
#[derive(Debug)]
pub enum AuthResult {
    /// Authentication succeeded, identity was updated
    Authenticated,
    /// No credentials found for this auth method
    NoCredentials,
}

/// Authentication middleware trait that processes requests and builds client identity
#[async_trait]
pub trait AuthMiddleware: Send + Sync {
    /// Process the request and update the client identity if credentials are found
    ///
    /// Returns:
    /// - `Ok(AuthResult::Authenticated)` if valid credentials were found and identity updated
    /// - `Ok(AuthResult::NoCredentials)` if no credentials for this auth method were found
    /// - `Err(Error)` if credentials were found but invalid (should fail the request)
    async fn authenticate(
        &self,
        parts: &Parts,
        identity: &mut ClientIdentity,
    ) -> Result<AuthResult, Error>;
}
