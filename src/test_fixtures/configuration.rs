use crate::configuration::Configuration;

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

pub fn minimal_config() -> Configuration {
    Configuration::load_from_str(MINIMAL_CONFIG_TOML).unwrap()
}

pub fn config_toml(extra: &str) -> String {
    format!("{MINIMAL_CONFIG_TOML}\n{extra}")
}
