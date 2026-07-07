use std::collections::HashMap;

use argon2::{
    Algorithm, Argon2, Params, PasswordHasher, PasswordVerifier, Version,
    password_hash::{SaltString, rand_core::OsRng},
};
use serde::Deserialize;

use crate::{
    auth::{
        AuthMiddleware, AuthResult, BasicAuthValidator,
        basic_auth::{Config, build_users},
    },
    command::server::Error,
    identity::ClientIdentity,
    test_fixtures::requests::{empty_parts, parts_with_basic_auth},
};

#[derive(Deserialize)]
struct TestConfig {
    identity: HashMap<String, Config>,
}

// Minimal Argon2 cost parameters for test-only use, chosen for speed, not security.
// Production code uses OWASP-recommended defaults (m=19456, t=2, p=1).
fn tiny_argon_params() -> Params {
    Params::new(8, 1, 1, None).expect("minimal Argon2 params must be valid")
}

fn hash_password_for_test(password: &str) -> String {
    let salt = SaltString::generate(OsRng);
    let argon = Argon2::new(Algorithm::Argon2id, Version::V0x13, tiny_argon_params());
    argon
        .hash_password(password.as_bytes(), &salt)
        .expect("test password hash must succeed")
        .to_string()
}

fn build_test_toml() -> String {
    let hash1 = hash_password_for_test("password1");
    let hash2 = hash_password_for_test("password2");
    format!(
        r#"
[identity.id_1]
username = "user1"
password = "{hash1}"

[identity.id_2]
username = "user2"
password = "{hash2}"
"#
    )
}

fn build_test_config() -> TestConfig {
    toml::from_str(&build_test_toml()).expect("Failed to parse test config")
}

#[test]
fn test_build_users() {
    let config = build_test_config();
    let users = build_users(&config.identity).unwrap();

    assert_eq!(users.len(), 2);
    assert!(users.contains_key("user1"));
    assert!(users.contains_key("user2"));

    let (id1, pass1) = users.get("user1").unwrap();
    assert_eq!(id1, "id_1");
    // Use the minimal-cost verifier: the hash was also produced with tiny params.
    let argon = Argon2::new(Algorithm::Argon2id, Version::V0x13, tiny_argon_params());
    assert!(
        argon
            .verify_password("password1".as_bytes(), &pass1.as_password_hash())
            .is_ok()
    );

    let (id2, pass2) = users.get("user2").unwrap();
    assert_eq!(id2, "id_2");
    assert!(
        argon
            .verify_password("password2".as_bytes(), &pass2.as_password_hash())
            .is_ok()
    );
}

#[test]
fn test_new_auth() {
    let config = build_test_config();
    let auth = BasicAuthValidator::new(&config.identity).unwrap();

    assert_eq!(auth.users.len(), 2);
    assert!(auth.users.contains_key("user1"));
    assert!(auth.users.contains_key("user2"));
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

/// The unknown-username path verifies the timing-guard hash and discards the
/// outcome. Even the exact password the guard hash encodes must not turn an
/// unknown username into a hit.
#[test]
fn unknown_username_is_rejected_even_with_the_guard_password() {
    let config = build_test_config();
    let auth = BasicAuthValidator::new(&config.identity).unwrap();

    assert_eq!(
        auth.validate_credentials("no-such-user", "angos-timing-guard"),
        None
    );
}

#[test]
fn test_validate_credentials() {
    let config = build_test_config();
    let auth = BasicAuthValidator::new(&config.identity).unwrap();

    let user1_id = auth.validate_credentials("user1", "password1");
    assert_eq!(user1_id, Some("id_1".to_string()));

    let user2_id = auth.validate_credentials("user2", "password2");
    assert_eq!(user2_id, Some("id_2".to_string()));

    let invalid_user = auth.validate_credentials("invalid_user", "password1");
    assert_eq!(invalid_user, None);

    let invalid_pass = auth.validate_credentials("user1", "wrong_password");
    assert_eq!(invalid_pass, None);
}

#[tokio::test]
async fn test_authenticate() {
    let config = build_test_config();
    let auth = BasicAuthValidator::new(&config.identity).unwrap();

    let parts = parts_with_basic_auth("user1", "password1");
    let mut identity = ClientIdentity::default();
    let result = auth.authenticate(&parts, &mut identity).await.unwrap();
    assert!(matches!(result, AuthResult::Authenticated));
    assert_eq!(identity.username, Some("user1".to_string()));
    assert_eq!(identity.id, Some("id_1".to_string()));

    let parts = parts_with_basic_auth("user1", "wrong_password");
    let mut identity = ClientIdentity::default();
    let result = auth.authenticate(&parts, &mut identity).await;
    assert!(matches!(result, Err(Error::Unauthorized(_))));

    let parts = parts_with_basic_auth("invalid_user", "password1");
    let mut identity = ClientIdentity::default();
    let result = auth.authenticate(&parts, &mut identity).await;
    assert!(matches!(result, Err(Error::Unauthorized(_))));
}

#[tokio::test]
async fn test_authenticate_without_authorization_header_returns_no_credentials() {
    let config = build_test_config();
    let auth = BasicAuthValidator::new(&config.identity).unwrap();

    let parts = empty_parts();
    let mut identity = ClientIdentity::default();
    let result = auth.authenticate(&parts, &mut identity).await.unwrap();

    assert!(matches!(result, AuthResult::NoCredentials));
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

#[tokio::test]
async fn test_empty_identity_map_rejects_presented_credentials() {
    let auth = BasicAuthValidator::new(&HashMap::new()).unwrap();

    let parts = parts_with_basic_auth("user1", "password1");
    let mut identity = ClientIdentity::default();
    let result = auth.authenticate(&parts, &mut identity).await;
    assert!(matches!(result, Err(Error::Unauthorized(_))));
}

#[test]
fn test_validate_credentials_empty_username_returns_none() {
    let config = build_test_config();
    let auth = BasicAuthValidator::new(&config.identity).unwrap();

    let result = auth.validate_credentials("", "password1");
    assert_eq!(result, None);
}

#[test]
fn test_validate_credentials_whitespace_only_username_returns_none() {
    let config = build_test_config();
    let auth = BasicAuthValidator::new(&config.identity).unwrap();

    let result = auth.validate_credentials("   ", "password1");
    assert_eq!(result, None);
}

#[test]
fn test_empty_password_hash_string_fails_at_deserialize() {
    let toml = r#"
[identity.id_1]
username = "user1"
password = ""
"#;

    let result: Result<TestConfig, _> = toml::from_str(toml);
    assert!(
        result.is_err(),
        "empty password hash string must fail at deserialization"
    );
}
