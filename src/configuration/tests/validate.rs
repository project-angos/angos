use crate::{
    configuration::{Configuration, Error},
    test_fixtures::configuration::config_toml,
};

#[test]
fn global_max_manifest_size_must_be_greater_than_zero() {
    let config = config_toml(
        r"
    max_manifest_size = 0
    ",
    );

    let result = Configuration::load_from_str(&config);
    match result {
        Err(Error::InvalidFormat(msg)) => {
            assert!(msg.contains("global.max_manifest_size must be greater than zero"));
        }
        other => panic!("Expected InvalidFormat error, got {other:?}"),
    }
}

#[test]
fn test_validate_webhook_referenced_globally() {
    let config = config_toml(
        r#"
    authorization_webhook = "my-webhook"

    [auth.webhook.my-webhook]
    url = "https://example.com/webhook"
    timeout_ms = 5000
    "#,
    );

    let result = Configuration::load_from_str(&config);
    assert!(result.is_ok());
}

#[test]
fn test_validate_webhook_missing_global_reference() {
    let config = config_toml(
        r#"
    authorization_webhook = "nonexistent-webhook"
    "#,
    );

    let result = Configuration::load_from_str(&config);
    assert!(result.is_err());
    match result {
        Err(Error::InvalidFormat(msg)) => {
            assert!(msg.contains("Webhook 'nonexistent-webhook' not found"));
            assert!(msg.contains("referenced globally"));
        }
        _ => panic!("Expected InvalidFormat error"),
    }
}

#[test]
fn test_validate_webhook_referenced_in_repository() {
    let config = config_toml(
        r#"
    [repository.myapp]
    authorization_webhook = "repo-webhook"

    [auth.webhook.repo-webhook]
    url = "https://example.com/webhook"
    timeout_ms = 5000
    "#,
    );

    let result = Configuration::load_from_str(&config);
    assert!(result.is_ok());
}

#[test]
fn test_validate_webhook_missing_repository_reference() {
    let config = config_toml(
        r#"
    [repository.myapp]
    authorization_webhook = "missing-webhook"
    "#,
    );

    let result = Configuration::load_from_str(&config);
    assert!(result.is_err());
    match result {
        Err(Error::InvalidFormat(msg)) => {
            assert!(msg.contains("Webhook 'missing-webhook' not found"));
            assert!(msg.contains("referenced in 'myapp' repository"));
        }
        _ => panic!("Expected InvalidFormat error"),
    }
}

#[test]
fn test_validate_webhook_empty_string_in_repository() {
    let config = config_toml(
        r#"
    [repository.myapp]
    authorization_webhook = ""
    "#,
    );

    let result = Configuration::load_from_str(&config);
    assert!(result.is_ok());
}

#[test]
fn test_validate_invalid_webhook_config() {
    let config = config_toml(
        r#"
    [auth.webhook.bad-webhook]
    url = "ht!tp://::invalid"
    timeout_ms = 5000
    "#,
    );

    let result = Configuration::load_from_str(&config);
    let err = result.expect_err("malformed webhook URL must fail to load");
    assert!(
        err.to_string().contains("url"),
        "error should mention the offending url field: {err}"
    );
}

#[test]
fn test_validate_multiple_repositories_with_webhooks() {
    let config = config_toml(
        r#"
    [repository.app1]
    authorization_webhook = "webhook1"

    [repository.app2]
    authorization_webhook = "webhook2"

    [auth.webhook.webhook1]
    url = "https://webhook1.example.com"
    timeout_ms = 5000

    [auth.webhook.webhook2]
    url = "https://webhook2.example.com"
    timeout_ms = 5000
    "#,
    );

    let result = Configuration::load_from_str(&config);
    assert!(result.is_ok());
}

#[test]
fn event_webhook_bad_global_reference_fails_load() {
    let config = config_toml(
        r#"
    event_webhooks = ["nonexistent-hook"]
    "#,
    );

    let result = Configuration::load_from_str(&config);
    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("nonexistent-hook"),
        "Error must name the unresolved webhook: {msg}"
    );
    assert!(
        msg.contains("globally") || msg.contains("global"),
        "Error must identify global as the source: {msg}"
    );
}

#[test]
fn event_webhook_bad_repo_reference_fails_load() {
    let config = config_toml(
        r#"
    [repository.prod]
    event_webhooks = ["ghost-hook"]
    "#,
    );

    let result = Configuration::load_from_str(&config);
    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("ghost-hook"),
        "Error must name the unresolved webhook: {msg}"
    );
    assert!(
        msg.contains("prod"),
        "Error must identify the repository as the source: {msg}"
    );
}
