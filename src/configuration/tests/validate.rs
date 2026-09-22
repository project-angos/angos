use crate::{
    configuration::{Configuration, Error},
    test_fixtures::configuration::{config_toml, load_config},
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

/// A zero-capacity read buffer reads nothing, so a blob body would end at once
/// with a `200` rather than fail.
#[test]
fn global_blob_stream_frame_size_must_be_greater_than_zero() {
    let config = config_toml(
        r"
    blob_stream_frame_size = 0
    ",
    );

    let result = Configuration::load_from_str(&config);
    match result {
        Err(Error::InvalidFormat(msg)) => {
            assert!(msg.contains("global.blob_stream_frame_size must be greater than zero"));
        }
        other => panic!("Expected InvalidFormat error, got {other:?}"),
    }
}

#[test]
fn global_max_blob_size_must_be_greater_than_zero() {
    let config = config_toml(
        r"
    max_blob_size = 0
    ",
    );

    let result = Configuration::load_from_str(&config);
    match result {
        Err(Error::InvalidFormat(msg)) => {
            assert!(msg.contains("global.max_blob_size must be greater than zero"));
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

#[test]
fn ui_sign_in_provider_must_be_a_configured_oidc_provider() {
    let config = config_toml(
        r#"
    [ui]
    enabled = true

    [ui.oidc]
    provider = "dex"
    client_id = "angos-ui"

    [auth.oidc.okta]
    issuer = "https://org.okta.com"
    "#,
    );

    let result = Configuration::load_from_str(&config);
    match result {
        Err(Error::InvalidFormat(msg)) => {
            assert!(msg.contains("ui.oidc.provider 'dex' has no matching auth.oidc provider"));
        }
        other => panic!("Expected InvalidFormat error, got {other:?}"),
    }
}

#[test]
fn ui_sign_in_accepts_a_configured_oidc_provider() {
    let config = config_toml(
        r#"
    [ui]
    enabled = true

    [ui.oidc]
    provider = "dex"
    client_id = "angos-ui"

    [auth.oidc.dex]
    issuer = "https://dex.example.com"
    "#,
    );

    let configuration = Configuration::load_from_str(&config).unwrap();

    assert_eq!(configuration.ui.oidc.unwrap().client_id, "angos-ui");
}

#[test]
fn listing_read_concurrency_must_be_greater_than_zero() {
    let config = config_toml("listing_read_concurrency = 0");

    match Configuration::load_from_str(&config) {
        Err(Error::InvalidFormat(msg)) => {
            assert!(
                msg.contains("listing_read_concurrency"),
                "the refusal must name the key; got: {msg}"
            );
        }
        other => panic!("Expected InvalidFormat error, got {other:?}"),
    }
}

#[test]
fn listing_read_concurrency_defaults_and_loads() {
    let default = Configuration::load_from_str(&config_toml("")).unwrap();
    assert_eq!(default.global.listing_read_concurrency.get(), 16);

    let raised =
        Configuration::load_from_str(&config_toml("listing_read_concurrency = 64")).unwrap();
    assert_eq!(raised.global.listing_read_concurrency.get(), 64);
}

/// Loads the minimal configuration with a scanner and `extra` appended.
fn scan_error(extra: &str) -> String {
    let config = config_toml(&format!(
        r#"
    [global.scan]
    url = "http://scanner:8766"
    {extra}
    "#
    ));
    match Configuration::load_from_str(&config) {
        Err(Error::InvalidFormat(msg)) => msg,
        other => panic!("Expected InvalidFormat error, got {other:?}"),
    }
}

#[test]
fn scan_tables_parse_at_both_levels() {
    let config = load_config(
        r#"
    [global.scan]
    url = "http://scanner:8766"

    [global.scan.refresh]
    rules = ["image.scanned_at < now() - days(30)"]

    [repository."apps".scan.refresh]
    rules = ["image.scanned_at < now() - days(7)"]

    [repository."web".scan]
    "#,
    );
    let refresh = config
        .global
        .scan
        .as_ref()
        .unwrap()
        .refresh
        .as_ref()
        .unwrap();
    assert_eq!(refresh.rules.len(), 1);
    assert_eq!(
        config.repository["apps"].refresh().map(|r| r.rules.len()),
        Some(1)
    );
    let web = config.repository["web"].scan.as_ref().unwrap();
    assert!(
        web.refresh.is_none(),
        "an empty scan table opts in without refresh rules"
    );
}

#[test]
fn a_repository_scan_table_requires_the_scanner() {
    let config = config_toml(
        r#"
    [repository."apps".scan]
    "#,
    );
    match Configuration::load_from_str(&config) {
        Err(Error::InvalidFormat(msg)) => {
            assert!(
                msg.contains(
                    "repository 'apps' has a scan table but [global.scan] is not configured"
                ),
                "{msg}"
            );
        }
        other => panic!("Expected InvalidFormat error, got {other:?}"),
    }
}

#[test]
fn a_repository_refresh_table_lists_at_least_one_rule() {
    let msg = scan_error(
        r#"
    [repository."apps".scan.refresh]
    rules = []
    "#,
    );
    assert!(
        msg.contains(r#"repository."apps".scan.refresh.rules must list at least one rule"#),
        "{msg}"
    );
}

#[test]
fn refresh_rules_reading_pull_times_require_update_pull_time() {
    let msg = scan_error(
        r#"
    [global.scan.refresh]
    rules = ["image.last_pulled_at > now() - days(7)"]
    "#,
    );
    assert!(
        msg.contains("global.scan.refresh.rules use last_pulled_at or top_pulled"),
        "{msg}"
    );
}

#[test]
fn a_global_index_table_parses() {
    let config = load_config(
        r#"
    [global.index]

    [repository."apps".index]
    "#,
    );
    assert!(config.global.index.is_some());
    assert!(config.repository["apps"].index.is_some());
}
