use std::collections::HashMap;
use std::time::Instant;

use argon2::{
    Algorithm, Argon2, Params, PasswordHash as Argon2PasswordHash, PasswordHasher, Version,
};
use serde::Deserialize;

use crate::auth::basic_auth::{Config, REJECTION_FLOOR, TIMING_GUARD_HASH};
use crate::auth::{AuthResult, BasicAuthValidator, Error};
use crate::identity::ClientIdentity;
use crate::test_fixtures::requests::{empty_parts, parts_with_basic_auth};

#[derive(Deserialize)]
struct TestConfig {
    identity: HashMap<String, Config>,
}

fn hash_password_for_test(password: &str) -> String {
    // Minimal Argon2 cost parameters chosen for speed, not security. Production
    // code uses OWASP-recommended defaults (m=19456, t=2, p=1).
    let params = Params::new(8, 1, 1, None).expect("minimal Argon2 params must be valid");
    let argon = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
    argon
        .hash_password(password.as_bytes())
        .expect("test password hash must succeed")
        .to_string()
}

fn build_test_config() -> TestConfig {
    let hash1 = hash_password_for_test("password1");
    let hash2 = hash_password_for_test("password2");
    toml::from_str(&format!(
        r#"
[identity.id_1]
username = "user1"
password = "{hash1}"

[identity.id_2]
username = "user2"
password = "{hash2}"
"#
    ))
    .unwrap()
}

#[test]
fn duplicate_usernames_across_identities_are_rejected() {
    let hash = hash_password_for_test("pw");
    let config: TestConfig = toml::from_str(&format!(
        r#"
[identity.id_1]
username = "shared"
password = "{hash}"

[identity.id_2]
username = "shared"
password = "{hash}"
"#
    ))
    .expect("Failed to parse test config");

    let Err(Error::Initialization(msg)) = BasicAuthValidator::new(&config.identity) else {
        panic!("duplicate usernames must be rejected at construction");
    };
    assert!(
        msg.contains("shared"),
        "error must name the duplicate username, got: {msg}"
    );
}

/// The guard has to cost what a real verification costs, and only a configured
/// hash does: an operator hashing with other parameters would otherwise leave
/// the gap the guard exists to close.
#[test]
fn the_timing_guard_borrows_a_configured_hash() {
    let config = build_test_config();
    let auth = BasicAuthValidator::new(&config.identity).unwrap();

    let configured: Vec<&Argon2PasswordHash> = config
        .identity
        .values()
        .map(|identity| &identity.password.0)
        .collect();
    assert!(
        configured.contains(&&auth.timing_guard.0),
        "the guard must be one of the configured hashes, got: {}",
        auth.timing_guard.0
    );
}

/// With no identity to borrow from, nothing can be enumerated either, so the
/// constant is enough.
#[test]
fn the_timing_guard_falls_back_to_the_constant_without_identities() {
    let auth = BasicAuthValidator::new(&HashMap::new()).unwrap();

    assert_eq!(auth.timing_guard.0.to_string(), TIMING_GUARD_HASH);
}

/// Every rejection answers at the same moment whatever made it fail, so
/// response timing separates neither an unknown username from a wrong password
/// nor one configured hash's cost from another's.
#[tokio::test]
async fn a_rejection_is_held_to_the_floor() {
    let configured = BasicAuthValidator::new(&build_test_config().identity).unwrap();
    let empty = BasicAuthValidator::new(&HashMap::new()).unwrap();

    for (auth, username, password) in [
        (&configured, "no-such-user", "password1"),
        (&configured, "user1", "wrong-password"),
        (&empty, "user1", "password1"),
    ] {
        let parts = parts_with_basic_auth(username, password);
        let started = Instant::now();

        let result = auth
            .authenticate(&parts, &mut ClientIdentity::default())
            .await;

        assert!(
            matches!(result, Err(Error::Unauthorized(_))),
            "'{username}' must be refused, got: {result:?}"
        );
        assert!(
            started.elapsed() >= REJECTION_FLOOR,
            "'{username}' answered before the floor"
        );
    }
}

#[tokio::test]
async fn valid_credentials_set_the_identity() {
    let auth = BasicAuthValidator::new(&build_test_config().identity).unwrap();

    let mut identity = ClientIdentity::default();
    let result = auth
        .authenticate(&parts_with_basic_auth("user2", "password2"), &mut identity)
        .await;
    assert!(matches!(result, Ok(AuthResult::Authenticated)));
    assert_eq!(identity.username.as_deref(), Some("user2"));
    assert_eq!(identity.id.as_deref(), Some("id_2"));

    let result = auth.authenticate(&empty_parts(), &mut identity).await;
    assert!(matches!(result, Ok(AuthResult::NoCredentials)));
}

#[test]
fn test_invalid_password_hash_fails_at_deserialize() {
    let toml = r#"
[identity.id_1]
username = "user1"
password = "not-a-valid-argon2-hash"
"#;

    let result: Result<TestConfig, _> = toml::from_str(toml);
    assert!(
        result.is_err(),
        "invalid Argon2 hash must fail at deserialization"
    );
}
