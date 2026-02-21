#[cfg(test)]
mod tests;

use std::collections::HashMap;

use argon2::{Argon2, PasswordVerifier, password_hash::PasswordHashString};
use async_trait::async_trait;
use hyper::http::request::Parts;
use serde::Deserialize;
use tracing::{debug, instrument, warn};

use super::{AuthMiddleware, AuthResult};
use crate::{
    command::server::{error::Error, request_ext::HeaderExt},
    identity::ClientIdentity,
};

#[derive(Clone, Debug, Default, Deserialize)]
pub struct Config {
    pub username: String,
    pub password: String,
}

pub struct BasicAuthValidator {
    users: HashMap<String, (String, PasswordHashString)>,
}

fn build_users(
    identities: &HashMap<String, Config>,
) -> HashMap<String, (String, PasswordHashString)> {
    let mut credentials = HashMap::new();
    for (id, config) in identities {
        let password_hash = match PasswordHashString::new(&config.password) {
            Ok(hash) => hash,
            Err(err) => {
                warn!("Invalid password hash for user {}: {err}", config.username);
                continue;
            }
        };

        credentials.insert(config.username.clone(), (id.clone(), password_hash));
    }

    credentials
}

impl BasicAuthValidator {
    pub fn new(identities: &HashMap<String, Config>) -> Self {
        Self {
            users: build_users(identities),
        }
    }

    #[instrument(skip(self, password))]
    pub fn validate_credentials(&self, username: &str, password: &str) -> Option<String> {
        let Some((identity_id, identity_password)) = self.users.get(username) else {
            debug!("Username not found in credentials");
            return None;
        };

        let identity_password = identity_password.password_hash();

        match Argon2::default().verify_password(password.as_bytes(), &identity_password) {
            Ok(()) => Some(identity_id.clone()),
            Err(error) => {
                debug!("Password verification failed: {error}");
                None
            }
        }
    }
}

#[async_trait]
impl AuthMiddleware for BasicAuthValidator {
    async fn authenticate(
        &self,
        parts: &Parts,
        identity: &mut ClientIdentity,
    ) -> Result<AuthResult, Error> {
        let Some((username, password)) = parts.basic_auth() else {
            return Ok(AuthResult::NoCredentials);
        };

        match self.validate_credentials(&username, &password) {
            Some(identity_id) => {
                identity.id = Some(identity_id);
                identity.username = Some(username);
                Ok(AuthResult::Authenticated)
            }
            None => Ok(AuthResult::NoCredentials),
        }
    }
}
