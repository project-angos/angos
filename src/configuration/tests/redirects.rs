use crate::{
    configuration::{Configuration, GlobalConfig},
    test_fixtures::configuration::config_toml,
};

#[test]
fn redirect_flags_default_to_true() {
    let config = GlobalConfig::default();
    assert!(config.enable_blob_redirect);
    assert!(config.enable_manifest_redirect);
}

#[test]
fn redirect_flags_deserialize_with_true_default() {
    let toml = config_toml("enable_blob_redirect = false");
    let config = Configuration::load_from_str(&toml).unwrap();
    assert!(!config.global.enable_blob_redirect);
    assert!(
        config.global.enable_manifest_redirect,
        "an omitted redirect flag defaults to true"
    );
}
