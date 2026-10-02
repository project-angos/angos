use serde::Deserialize;

use crate::registry::blob_store::{BlobStoreConfig, FsBackendConfig, S3BackendConfig};

// Unknown keys in any sub-table are ignored (serde's default), so a config
// carrying knobs angos does not read still loads.

/// Storage configuration shared by the metadata store and the job store, both
/// of which run over the one `ObjectStore` the CLI bootstrap builds from it.
///
/// The operator-facing TOML key is `[metadata_store]`, with `.fs` or `.s3`
/// sub-tables taking the same keys as the blob store's; the default `Inherit`
/// resolves to the blob-store configuration.
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum RegistryStorageConfig {
    /// Inherit the whole blob-store configuration, transport knobs included,
    /// resolved via
    /// [`crate::configuration::Configuration::resolve_registry_storage`] before
    /// any backend is built.
    #[default]
    Inherit,
    #[serde(rename = "fs")]
    FS(FsBackendConfig),
    #[serde(rename = "s3")]
    S3(S3BackendConfig),
}

/// A [`RegistryStorageConfig`] whose `Inherit` default has been resolved to a
/// concrete backend, so consumers match without a dead `Inherit` arm.
#[derive(Clone, Debug, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum ResolvedStorageConfig {
    FS(FsBackendConfig),
    S3(S3BackendConfig),
}

impl ResolvedStorageConfig {
    /// Mirror the given blob-store config, which is how `Inherit` resolves.
    pub fn from_blob_store(blob: &BlobStoreConfig) -> Self {
        match blob {
            BlobStoreConfig::FS(config) => ResolvedStorageConfig::FS(config.clone()),
            BlobStoreConfig::S3(config) => ResolvedStorageConfig::S3(config.clone()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use angos_secret::Secret;

    use super::*;
    use crate::registry::{blob_store, s3_connection::S3ConnectionConfig};

    #[test]
    fn test_from_blob_store_fs_copies_paths_and_sync() {
        let blob = blob_store::BlobStoreConfig::FS(blob_store::FsBackendConfig {
            root_dir: PathBuf::from("/var/lib/registry"),
            sync_to_disk: true,
        });
        match ResolvedStorageConfig::from_blob_store(&blob) {
            ResolvedStorageConfig::FS(c) => {
                assert_eq!(c.root_dir, PathBuf::from("/var/lib/registry"));
                assert!(c.sync_to_disk);
            }
            ResolvedStorageConfig::S3(_) => {
                panic!("expected FS storage config")
            }
        }
    }

    /// The metadata store inherits the blob store's transport knobs too, not
    /// only its connection.
    #[test]
    fn test_from_blob_store_s3_copies_connection_and_transport() {
        let blob = blob_store::BlobStoreConfig::S3(blob_store::S3BackendConfig {
            connection: S3ConnectionConfig {
                access_key_id: Secret::new("key".to_string()),
                secret_key: Secret::new("secret".to_string()),
                endpoint: "http://localhost:9000".to_string(),
                bucket: "test-bucket".to_string(),
                region: "us-east-1".to_string(),
                key_prefix: "foo".to_string(),
            },
            transport: blob_store::TransportFields {
                max_attempts: 7,
                operation_timeout_secs: 42,
                ..blob_store::TransportFields::default()
            },
        });
        match ResolvedStorageConfig::from_blob_store(&blob) {
            ResolvedStorageConfig::S3(c) => {
                assert_eq!(c.transport.max_attempts, 7);
                assert_eq!(c.transport.operation_timeout_secs, 42);
                assert_eq!(c.connection.bucket, "test-bucket");
                assert_eq!(c.connection.region, "us-east-1");
                assert_eq!(c.connection.endpoint, "http://localhost:9000");
                assert_eq!(c.connection.access_key_id.expose(), "key");
                assert_eq!(c.connection.secret_key.expose(), "secret");
                assert_eq!(c.connection.key_prefix, "foo");
            }
            ResolvedStorageConfig::FS(_) => {
                panic!("expected S3 storage config")
            }
        }
    }

    /// Flat TOML deserialises into the metadata store's S3 config, transport
    /// knobs included. The ignored keys of removed subsystems are kept in the
    /// fixture: a config carrying them must still load.
    #[test]
    fn s3_backend_config_toml_round_trip() {
        let toml = r#"
            access_key_id            = "meta-key"
            secret_key               = "meta-secret"
            endpoint                 = "https://meta.s3.example.com"
            bucket                   = "meta-bucket"
            region                   = "eu-central-1"
            key_prefix               = "_meta"
            max_attempts             = 7
            link_cache_ttl           = 60
            access_time_debounce_secs = 120
        "#;

        let cfg: S3BackendConfig = toml::from_str(toml).expect("deserialize");
        assert_eq!(cfg.connection.access_key_id.expose(), "meta-key");
        assert_eq!(cfg.connection.secret_key.expose(), "meta-secret");
        assert_eq!(cfg.connection.endpoint, "https://meta.s3.example.com");
        assert_eq!(cfg.connection.bucket, "meta-bucket");
        assert_eq!(cfg.connection.region, "eu-central-1");
        assert_eq!(cfg.connection.key_prefix, "_meta");
        assert_eq!(cfg.transport.max_attempts, 7);
    }

    /// Regression: `region` must be required, matching the documented schema.
    #[test]
    fn s3_backend_config_requires_region() {
        let toml = r#"
            access_key_id = "k"
            secret_key    = "s"
            endpoint      = "http://localhost:9000"
            bucket        = "b"
        "#;
        let err = toml::from_str::<S3BackendConfig>(toml).expect_err("region must be required");
        assert!(
            err.to_string().contains("region"),
            "error should mention the missing `region` field, got: {err}"
        );
    }

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
