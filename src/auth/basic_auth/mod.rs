#[cfg(test)]
mod tests;

use std::{collections::HashMap, fmt};

use argon2::{Argon2, PasswordVerifier, password_hash::PasswordHashString};
use async_trait::async_trait;
use hyper::http::request::Parts;
use serde::{Deserialize, de};
use tracing::{debug, instrument};

use super::{AuthMiddleware, AuthResult};
use crate::{auth::Error, command::server::RequestHeaders, identity::ClientIdentity};

/// An Argon2 password hash string, validated at deserialize time.
#[derive(Clone)]
pub struct PasswordHash(PasswordHashString);

impl fmt::Debug for PasswordHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[REDACTED]")
    }
}

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
    /// Hash verified on the unknown-username path so a miss costs the same
    /// Argon2 work as a hit and response timing cannot enumerate usernames.
    timing_guard: PasswordHash,
}

/// A fixed hash carrying `Argon2::default()`'s cost parameters; the password
/// it encodes never matches, only its verification cost matters.
const TIMING_GUARD_HASH: &str = "$argon2id$v=19$m=19456,t=2,p=1$YW5nb3MtdGltaW5nLWd1YXJk$FCScuxFhlmRU1Dn23hjKGjIaNsd/kD4tKsHrHAQU5VQ";

/// Maps identities to a username-keyed lookup table, rejecting duplicate
/// usernames: two `[auth.identity.*]` entries sharing a username would
/// otherwise collapse into whichever the map iterates last, making the
/// winning identity id (used by CEL rules and webhooks) change across
/// restarts.
fn build_users(
    identities: &HashMap<String, Config>,
) -> Result<HashMap<String, (String, PasswordHash)>, Error> {
    let mut users = HashMap::with_capacity(identities.len());
    for (id, config) in identities {
        if users
            .insert(
                config.username.clone(),
                (id.clone(), config.password.clone()),
            )
            .is_some()
        {
            return Err(Error::Initialization(format!(
                "duplicate basic-auth username '{}' across [auth.identity] entries",
                config.username
            )));
        }
    }
    Ok(users)
}

impl BasicAuthValidator {
    /// # Errors
    ///
    /// Returns [`Error::Initialization`] when two identities share a username.
    pub fn new(identities: &HashMap<String, Config>) -> Result<Self, Error> {
        let timing_guard = PasswordHashString::new(TIMING_GUARD_HASH)
            .map(PasswordHash)
            .map_err(|e| Error::Initialization(format!("invalid timing-guard hash: {e}")))?;
        Ok(Self {
            users: build_users(identities)?,
            timing_guard,
        })
    }

    #[instrument(skip(self, password))]
    pub fn validate_credentials(&self, username: &str, password: &str) -> Option<String> {
        let Some((identity_id, identity_password)) = self.users.get(username) else {
            // Burn the same Argon2 work as a real verification so response
            // timing cannot reveal whether the username exists.
            let _ = Argon2::default()
                .verify_password(password.as_bytes(), &self.timing_guard.as_password_hash());
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
        let headers = RequestHeaders::new(&parts.headers);
        let Some((username, password)) = headers.basic_auth() else {
            return Ok(AuthResult::NoCredentials);
        };

        match self.validate_credentials(&username, &password) {
            Some(identity_id) => {
                identity.id = Some(identity_id);
                identity.username = Some(username);
                Ok(AuthResult::Authenticated)
            }
            None => Err(Error::Unauthorized(
                "Invalid basic auth credentials".to_string(),
            )),
        }
    }
}
