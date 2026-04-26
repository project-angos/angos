#[cfg(test)]
mod tests;

use std::collections::HashMap;

use argon2::{Argon2, PasswordVerifier, password_hash::PasswordHashString};
use async_trait::async_trait;
use hyper::http::request::Parts;
use serde::{Deserialize, de};
use tracing::{debug, instrument};

use super::{AuthMiddleware, AuthResult};
use crate::{
    command::server::{Error, request_ext::HeaderExt},
    identity::ClientIdentity,
};

/// An Argon2 password hash string, validated at deserialize time.
#[derive(Clone, Debug)]
pub struct PasswordHash(PasswordHashString);

impl PasswordHash {
    pub fn as_password_hash(&self) -> argon2::password_hash::PasswordHash<'_> {
        self.0.password_hash()
    }
}

impl<'de> Deserialize<'de> for PasswordHash {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let source = String::deserialize(deserializer)?;
        PasswordHashString::new(&source)
            .map(Self)
            .map_err(|e| de::Error::custom(format!("invalid Argon2 password hash: {e}")))
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    pub username: String,
    pub password: PasswordHash,
}

pub struct BasicAuthValidator {
    users: HashMap<String, (String, PasswordHash)>,
}

fn build_users(identities: &HashMap<String, Config>) -> HashMap<String, (String, PasswordHash)> {
    identities
        .iter()
        .map(|(id, config)| {
            (
                config.username.clone(),
                (id.clone(), config.password.clone()),
            )
        })
        .collect()
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

        match Argon2::default()
            .verify_password(password.as_bytes(), &identity_password.as_password_hash())
        {
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
