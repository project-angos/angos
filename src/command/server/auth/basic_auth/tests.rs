use std::collections::HashMap;

use argon2::{Argon2, PasswordVerifier};
use base64::{Engine, prelude::BASE64_STANDARD};
use hyper::{Request, http::request::Parts};
use serde::Deserialize;

use crate::{
    command::server::auth::{
        AuthMiddleware, AuthResult, BasicAuthValidator,
        basic_auth::{Config, build_users},
    },
    identity::ClientIdentity,
};

#[derive(Deserialize)]
struct TestConfig {
    identity: HashMap<String, Config>,
}

static TEST_CONFIG: &str = r#"
[identity.id_1]
username = "user1"
password = "$argon2id$v=19$m=19456,t=2,p=1$9pxWwg0VtZzDXno/25417Q$e+cuKy9VisJVxec/EEuKvvfIIIOy5yDGRzYKiuDLjx0"  # password is "password1"

[identity.id_2]
username = "user2"
password = "$argon2id$v=19$m=19456,t=2,p=1$Uy1qF140d+2nOKIz1ZFltw$xAii0VrKbNn2d/rb5hUWUmEcwq6kjVFE5mW5ymzFudw"  # password is "password2"

[identity.id_3]
username = "user3"
password = "invalid-password-hash"
"#;

fn build_test_config() -> TestConfig {
    let config: TestConfig = toml::from_str(TEST_CONFIG).expect("Failed to parse test config");
    config
}

fn build_basic_auth_header(username: &str, password: &str) -> String {
    let credentials = format!("{username}:{password}");
    let encoded = BASE64_STANDARD.encode(credentials);
    format!("Basic {encoded}")
}

fn build_test_parts(username: &str, password: &str) -> Parts {
    let basic_auth = build_basic_auth_header(username, password);

    let request = Request::builder()
        .header("Authorization", basic_auth)
        .body(())
        .unwrap();

    let (parts, ()) = request.into_parts();
    parts
}

#[test]
fn test_build_users() {
    let config = build_test_config();
    let users = build_users(&config.identity);

    assert_eq!(users.len(), 2);
    assert!(users.contains_key("user1"));
    assert!(users.contains_key("user2"));

    let (id1, pass1) = users.get("user1").unwrap();
    assert_eq!(id1, "id_1");
    assert!(
        Argon2::default()
            .verify_password("password1".as_bytes(), &pass1.password_hash())
            .is_ok()
    );

    let (id2, pass2) = users.get("user2").unwrap();
    assert_eq!(id2, "id_2");
    assert!(
        Argon2::default()
            .verify_password("password2".as_bytes(), &pass2.password_hash())
            .is_ok()
    );
    assert_eq!(users.get("user3"), None);
}

#[test]
fn test_new_auth() {
    let config = build_test_config();

    let auth = BasicAuthValidator::new(&config.identity);

    assert_eq!(auth.users.len(), 2);
    assert!(auth.users.contains_key("user1"));
    assert!(auth.users.contains_key("user2"));
    assert!(!auth.users.contains_key("user3")); // invalid
}

#[test]
fn test_validate_credentials() {
    let config = build_test_config();
    let auth = BasicAuthValidator::new(&config.identity);

    // Valid credentials
    let user1_id = auth.validate_credentials("user1", "password1");
    assert_eq!(user1_id, Some("id_1".to_string()));

    let user2_id = auth.validate_credentials("user2", "password2");
    assert_eq!(user2_id, Some("id_2".to_string()));

    // Invalid username
    let invalid_user = auth.validate_credentials("invalid_user", "password1");
    assert_eq!(invalid_user, None);

    // Invalid password
    let invalid_pass = auth.validate_credentials("user1", "wrong_password");
    assert_eq!(invalid_pass, None);
}

#[tokio::test]
async fn test_authenticate() {
    let config = build_test_config();
    let auth = BasicAuthValidator::new(&config.identity);

    let parts = build_test_parts("user1", "password1");
    let mut identity = ClientIdentity::default();
    let result = auth.authenticate(&parts, &mut identity).await.unwrap();
    assert!(matches!(result, AuthResult::Authenticated));
    assert_eq!(identity.username, Some("user1".to_string()));
    assert_eq!(identity.id, Some("id_1".to_string()));

    let parts = build_test_parts("user1", "wrong_password");
    let mut identity = ClientIdentity::default();
    let result = auth.authenticate(&parts, &mut identity).await.unwrap();
    assert!(matches!(result, AuthResult::NoCredentials));

    let parts = build_test_parts("invalid_user", "password1");
    let mut identity = ClientIdentity::default();
    let result = auth.authenticate(&parts, &mut identity).await.unwrap();
    assert!(matches!(result, AuthResult::NoCredentials));
}
