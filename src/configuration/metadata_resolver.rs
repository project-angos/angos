use super::Configuration;
use crate::configuration::RegistryStorageConfig;

impl Configuration {
    pub fn resolve_registry_storage(&self) -> RegistryStorageConfig {
        match &self.registry_storage {
            RegistryStorageConfig::Inherit => {
                RegistryStorageConfig::from_blob_store(&self.blob_store)
            }
            RegistryStorageConfig::FS(_) | RegistryStorageConfig::S3(_) => {
                self.registry_storage.clone()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use angos_tx_engine::lock::LockStrategy;

    use super::*;
    use crate::configuration::Configuration;

    #[test]
    fn test_inherit_resolves_to_fs_from_fs_blob_store() {
        let config_str = r#"
        [server]
        bind_address = "0.0.0.0"

        [blob_store.fs]
        root_dir = "/data/blobs"
        sync_to_disk = true
        "#;

        let config = Configuration::load_from_str(config_str).unwrap();
        assert!(
            matches!(config.registry_storage, RegistryStorageConfig::Inherit),
            "absent [metadata_store] section must deserialise as Inherit"
        );

        let resolved = config.resolve_registry_storage();
        match resolved {
            RegistryStorageConfig::FS(fs_config) => {
                assert_eq!(fs_config.root_dir, "/data/blobs");
                assert!(fs_config.sync_to_disk);
                assert_eq!(fs_config.lock_strategy, LockStrategy::Memory);
            }
            other => panic!("expected FS storage config from Inherit, got {other:?}"),
        }
    }

    #[test]
    fn test_explicit_fs_config_is_not_overridden_by_s3_blob_store() {
        let config_str = r#"
        [server]
        bind_address = "0.0.0.0"

        [blob_store.s3]
        bucket = "blob-bucket"
        region = "us-east-1"
        endpoint = "https://s3.example.com"
        access_key_id = "blob-key"
        secret_key = "blob-secret"

        [metadata_store.fs]
        root_dir = "/custom/metadata"
        "#;

        let config = Configuration::load_from_str(config_str).unwrap();
        assert!(
            matches!(config.registry_storage, RegistryStorageConfig::FS(_)),
            "explicit [metadata_store.fs] must not be Inherit"
        );

        let resolved = config.resolve_registry_storage();
        match resolved {
            RegistryStorageConfig::FS(fs_config) => {
                assert_eq!(fs_config.root_dir, "/custom/metadata");
            }
            other => panic!("expected explicit FS storage config, got {other:?}"),
        }
    }

    #[test]
    fn test_explicit_s3_config_is_not_overridden_by_s3_blob_store() {
        let config_str = r#"
        [server]
        bind_address = "0.0.0.0"

        [blob_store.s3]
        bucket = "blob-bucket"
        region = "us-east-1"
        endpoint = "https://s3.amazonaws.com"
        access_key_id = "blob-key"
        secret_key = "blob-secret"

        [metadata_store.s3]
        bucket = "metadata-bucket"
        region = "eu-west-1"
        endpoint = "https://metadata.example.com"
        access_key_id = "meta-key"
        secret_key = "meta-secret"
        "#;

        let config = Configuration::load_from_str(config_str).unwrap();
        assert!(
            matches!(config.registry_storage, RegistryStorageConfig::S3(_)),
            "explicit [metadata_store.s3] must not be Inherit"
        );

        let resolved = config.resolve_registry_storage();
        match resolved {
            RegistryStorageConfig::S3(s3_config) => {
                assert_eq!(s3_config.connection.bucket, "metadata-bucket");
                assert_eq!(s3_config.connection.region, "eu-west-1");
                assert_eq!(
                    s3_config.connection.endpoint,
                    "https://metadata.example.com"
                );
            }
            other => panic!("expected explicit S3 storage config, got {other:?}"),
        }
    }

    #[test]
    fn test_inherit_resolves_to_s3_from_s3_blob_store() {
        let config_str = r#"
        [server]
        bind_address = "0.0.0.0"

        [blob_store.s3]
        bucket = "my-bucket"
        region = "us-east-1"
        endpoint = "https://s3.example.com"
        access_key_id = "key123"
        secret_key = "secret456"
        key_prefix = "prefix/"
        "#;

        let config = Configuration::load_from_str(config_str).unwrap();
        assert!(
            matches!(config.registry_storage, RegistryStorageConfig::Inherit),
            "absent [metadata_store] section must deserialise as Inherit"
        );

        let resolved = config.resolve_registry_storage();
        match resolved {
            RegistryStorageConfig::S3(s3_config) => {
                assert_eq!(s3_config.connection.bucket, "my-bucket");
                assert_eq!(s3_config.connection.region, "us-east-1");
                assert_eq!(s3_config.connection.endpoint, "https://s3.example.com");
                assert_eq!(s3_config.connection.access_key_id.expose(), "key123");
                assert_eq!(s3_config.connection.secret_key.expose(), "secret456");
                assert_eq!(s3_config.connection.key_prefix, "prefix/");
            }
            other => panic!("expected S3 storage config from Inherit, got {other:?}"),
        }
    }

    #[test]
    fn test_inherit_is_default_for_registry_storage_field() {
        let config_str = r#"
        [server]
        bind_address = "0.0.0.0"
        "#;

        let config = Configuration::load_from_str(config_str).unwrap();
        assert_eq!(
            config.registry_storage,
            RegistryStorageConfig::Inherit,
            "Configuration.registry_storage must default to Inherit when [metadata_store] is absent"
        );
    }
}
