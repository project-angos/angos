use crate::{
    configuration::{Configuration, GlobalConfig},
    test_fixtures::configuration::{MINIMAL_CONFIG_TOML, config_toml},
};

#[test]
fn test_redirect_resolver_only_enable_redirect_true() {
    let config = GlobalConfig {
        enable_redirect: Some(true),
        ..GlobalConfig::default()
    };
    assert!(config.resolved_enable_blob_redirect());
    assert!(config.resolved_enable_manifest_redirect());
}

#[test]
fn test_redirect_resolver_only_enable_redirect_false() {
    let config = GlobalConfig {
        enable_redirect: Some(false),
        ..GlobalConfig::default()
    };
    assert!(!config.resolved_enable_blob_redirect());
    assert!(!config.resolved_enable_manifest_redirect());
}

#[test]
fn test_redirect_resolver_both_new_fields_set() {
    let config = GlobalConfig {
        enable_blob_redirect: Some(true),
        enable_manifest_redirect: Some(false),
        ..GlobalConfig::default()
    };
    assert!(config.resolved_enable_blob_redirect());
    assert!(!config.resolved_enable_manifest_redirect());
}

#[test]
fn test_redirect_resolver_new_wins_over_old() {
    let config = GlobalConfig {
        enable_redirect: Some(false),
        enable_blob_redirect: Some(true),
        ..GlobalConfig::default()
    };
    assert!(config.resolved_enable_blob_redirect());
    assert!(!config.resolved_enable_manifest_redirect());
}

#[test]
fn test_redirect_resolver_nothing_set_defaults_true() {
    let config = GlobalConfig::default();
    assert!(config.resolved_enable_blob_redirect());
    assert!(config.resolved_enable_manifest_redirect());
}

#[test]
fn deprecated_fields_empty_when_no_deprecated_config() {
    let config = Configuration::load_from_str(MINIMAL_CONFIG_TOML).unwrap();
    assert!(
        config.deprecated_fields().is_empty(),
        "No deprecated fields should be reported for a clean config"
    );
}

#[test]
fn deprecated_fields_contains_enable_redirect_when_set() {
    let config = config_toml(
        r"
        enable_redirect = true
        ",
    );

    let config = Configuration::load_from_str(&config).unwrap();
    let fields = config.deprecated_fields();
    assert_eq!(fields, vec!["global.enable_redirect"]);
}

#[test]
fn deprecated_fields_not_present_when_new_redirect_fields_used() {
    let config = config_toml(
        r"
        enable_blob_redirect = true
        enable_manifest_redirect = false
        ",
    );

    let config = Configuration::load_from_str(&config).unwrap();
    assert!(
        config.deprecated_fields().is_empty(),
        "New redirect fields must not trigger the deprecation warning"
    );
}
