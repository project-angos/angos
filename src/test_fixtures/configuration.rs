use crate::configuration::{Configuration, Error};

pub const MINIMAL_CONFIG_TOML: &str = r#"
    [blob_store.fs]
    root_dir = "/tmp/test"

    [metadata_store.fs]
    root_dir = "/tmp/test"

    [cache.memory]

    [server]
    bind_address = "0.0.0.0"
    port = 8000

    [global]
    update_pull_time = false
    max_concurrent_cache_jobs = 10
"#;

/// Returns the shared minimal test configuration.
///
/// # Panics
///
/// Panics if the built-in TOML fixture becomes invalid.
pub fn minimal_config() -> Configuration {
    Configuration::load_from_str(MINIMAL_CONFIG_TOML).unwrap()
}

pub fn config_toml(extra: &str) -> String {
    format!("{MINIMAL_CONFIG_TOML}\n{extra}")
}

/// Loads the shared minimal test configuration with extra TOML appended.
///
/// # Panics
///
/// Panics when the appended TOML makes the configuration invalid.
pub fn load_config(extra: &str) -> Configuration {
    try_load_config(extra).unwrap()
}

/// Tries to load the shared minimal test configuration with extra TOML appended.
///
/// # Errors
///
/// Returns an error when the appended TOML makes the configuration invalid.
pub fn try_load_config(extra: &str) -> Result<Configuration, Error> {
    Configuration::load_from_str(&config_toml(extra))
}
