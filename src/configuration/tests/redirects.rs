use crate::configuration::{Configuration, GlobalConfig};
use crate::test_fixtures::configuration::config_toml;

#[test]
fn redirect_flags_default_to_true() {
    let config = GlobalConfig::default();
    assert!(config.redirect.blob);
    assert!(config.redirect.manifest);
}

#[test]
fn redirect_flags_deserialize_with_true_default() {
    let toml = config_toml("enable_blob_redirect = false");
    let config = Configuration::load_from_str(&toml).unwrap();
    assert!(!config.global.redirect.blob);
    assert!(
        config.global.redirect.manifest,
        "an omitted redirect flag defaults to true"
    );
}
