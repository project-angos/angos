use serde::Deserialize;

use crate::registry::blob_store::{FsBackendConfig, S3BackendConfig};

/// The `[metadata_store]` table, backing both the metadata and job stores; the
/// default `Inherit` resolves to the blob-store configuration. Unknown keys are
/// ignored so a config carrying retired knobs still loads.
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub enum RegistryStorageConfig {
    #[default]
    Inherit,
    #[serde(rename = "fs")]
    FS(FsBackendConfig),
    #[serde(rename = "s3")]
    S3(Box<S3BackendConfig>),
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn s3_toml_with(extra: &str) -> String {
        format!(
            r#"
            access_key_id = "k"
            secret_key    = "s"
            endpoint      = "http://localhost:9000"
            bucket        = "b"
            region        = "r"
            {extra}
        "#
        )
    }

    /// The retired coordination keys still parse, in every shape they are
    /// written in, and are ignored.
    #[test]
    fn deprecated_lock_keys_parse_and_are_ignored() {
        for extra in [
            "conditional_operations = false",
            r#"lock_strategy = "memory""#,
            "[lock_strategy.s3]\nttl_secs = 30",
            "[lock_strategy.redis]\nurl = \"redis://localhost\"",
            "[redis]\nurl = \"redis://localhost\"",
        ] {
            let cfg: S3BackendConfig = toml::from_str(&s3_toml_with(extra))
                .unwrap_or_else(|e| panic!("deprecated key {extra:?} must still parse: {e}"));
            assert_eq!(cfg.connection.bucket, "b");
        }
    }

    /// Same tolerance for the FS sub-table.
    #[test]
    fn deprecated_lock_keys_parse_and_are_ignored_on_fs() {
        for extra in [
            r#"lock_strategy = "memory""#,
            "[redis]\nurl = \"redis://localhost\"",
        ] {
            let toml = format!("root_dir = \"/data\"\n{extra}");
            let cfg: FsBackendConfig = toml::from_str(&toml)
                .unwrap_or_else(|e| panic!("deprecated key {extra:?} must still parse: {e}"));
            assert_eq!(cfg.root_dir, PathBuf::from("/data"));
        }
    }
}
