#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::fmt;
use std::time::{Duration, Instant};

use argon2::{Argon2, PasswordHash as Argon2PasswordHash, PasswordVerifier};
use http::request::Parts;
use serde::{Deserialize, Deserializer, de};
use tokio::time::sleep;
use tracing::{debug, instrument};

use crate::auth::authorization::basic_credentials;
use crate::auth::{AuthResult, Error};
use crate::identity::ClientIdentity;

/// An Argon2 password hash string, validated at deserialize time.
#[derive(Clone)]
pub struct PasswordHash(Argon2PasswordHash);

impl fmt::Debug for PasswordHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[REDACTED]")
    }
}

impl PasswordHash {
    pub fn as_password_hash(&self) -> &Argon2PasswordHash {
        &self.0
    }
}

impl<'de> Deserialize<'de> for PasswordHash {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let source = String::deserialize(deserializer)?;
        Argon2PasswordHash::new(&source)
            .map(Self)
            .map_err(|e| de::Error::custom(format!("invalid Argon2 password hash: {e}")))
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    pub username: String,
    pub password: PasswordHash,
}

/// A configured identity, keyed by its username.
struct User {
    id: String,
    password: PasswordHash,
}

pub struct BasicAuthValidator {
    users: HashMap<String, User>,
    /// Hash verified on the unknown-username path so a miss costs the same
    /// Argon2 work as a hit and response timing cannot enumerate usernames.
    timing_guard: PasswordHash,
}

/// A hash with `Argon2::default()`'s cost, for a registry with no identity to
/// borrow one from; only its verification cost matters.
const TIMING_GUARD_HASH: &str = "$argon2id$v=19$m=19456,t=2,p=1$YW5nb3MtdGltaW5nLWd1YXJk$FCScuxFhlmRU1Dn23hjKGjIaNsd/kD4tKsHrHAQU5VQ";

/// Wall time every rejection takes, so an unknown username and a wrong password
/// are indistinguishable whatever the hashes cost, and guessing is throttled.
const REJECTION_FLOOR: Duration = Duration::from_secs(1);

/// Keys identities by username, rejecting duplicates: they would otherwise
/// collapse into whichever the map iterates last, changing the identity id
/// across restarts.
fn build_users(identities: &HashMap<String, Config>) -> Result<HashMap<String, User>, Error> {
    let mut users = HashMap::with_capacity(identities.len());
    for (id, config) in identities {
        if users
            .insert(
                config.username.clone(),
                User {
                    id: id.clone(),
                    password: config.password.clone(),
                },
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
        // A configured hash costs what the real verifications cost; the
        // constant only guesses, and an operator whose hashes carry other
        // parameters would leave a gap the guard exists to close.
        let timing_guard = match identities.values().next() {
            Some(config) => config.password.clone(),
            None => Argon2PasswordHash::new(TIMING_GUARD_HASH)
                .map(PasswordHash)
                .map_err(|e| Error::Initialization(format!("invalid timing-guard hash: {e}")))?,
        };
        Ok(Self {
            users: build_users(identities)?,
            timing_guard,
        })
    }

    #[instrument(skip(self, password))]
    fn validate_credentials(&self, username: &str, password: &str) -> Option<String> {
        let Some(user) = self.users.get(username) else {
            // Burn the same Argon2 work as a real verification so response
            // timing cannot reveal whether the username exists.
            let _ = Argon2::default()
                .verify_password(password.as_bytes(), self.timing_guard.as_password_hash());
            debug!("Username not found in credentials");
            return None;
        };

        match Argon2::default()
            .verify_password(password.as_bytes(), user.password.as_password_hash())
        {
            Ok(()) => Some(user.id.clone()),
            Err(error) => {
                debug!("Password verification failed: {error}");
                None
            }
        }
    }

    pub async fn authenticate(
        &self,
        parts: &Parts,
        identity: &mut ClientIdentity,
    ) -> Result<AuthResult, Error> {
        let Some((username, password)) = basic_credentials(&parts.headers) else {
            return Ok(AuthResult::NoCredentials);
        };

        let started = Instant::now();
        if let Some(identity_id) = self.validate_credentials(&username, &password) {
            identity.id = Some(identity_id);
            identity.username = Some(username);
            return Ok(AuthResult::Authenticated);
        }

        // Held to the floor rather than delayed by it, so every rejection
        // answers at the same moment.
        sleep(REJECTION_FLOOR.saturating_sub(started.elapsed())).await;
        Err(Error::Unauthorized(
            "Invalid basic auth credentials".to_string(),
        ))
    }
}
