use super::Configuration;
use crate::registry::metadata_store::MetadataStoreConfig;

impl Configuration {
    pub fn resolve_metadata_config(&self) -> MetadataStoreConfig {
        match &self.metadata_store {
            MetadataStoreConfig::Inherit => MetadataStoreConfig::from_blob_store(&self.blob_store),
            MetadataStoreConfig::FS(_) | MetadataStoreConfig::S3(_) => self.metadata_store.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{configuration::Configuration, registry::metadata_store};

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
            matches!(config.metadata_store, MetadataStoreConfig::Inherit),
            "absent [metadata_store] section must deserialise as Inherit"
        );

        let resolved = config.resolve_metadata_config();
        match resolved {
            MetadataStoreConfig::FS(fs_config) => {
                assert_eq!(fs_config.root_dir, "/data/blobs");
                assert!(fs_config.sync_to_disk);
                assert_eq!(
                    fs_config.lock_strategy,
                    metadata_store::LockStrategy::Memory
                );
            }
            other => panic!("expected FS metadata config from Inherit, got {other:?}"),
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
            matches!(config.metadata_store, MetadataStoreConfig::FS(_)),
            "explicit [metadata_store.fs] must not be Inherit"
        );

        let resolved = config.resolve_metadata_config();
        match resolved {
            MetadataStoreConfig::FS(fs_config) => {
                assert_eq!(fs_config.root_dir, "/custom/metadata");
            }
            other => panic!("expected explicit FS metadata config, got {other:?}"),
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
            matches!(config.metadata_store, MetadataStoreConfig::S3(_)),
            "explicit [metadata_store.s3] must not be Inherit"
        );

        let resolved = config.resolve_metadata_config();
        match resolved {
            MetadataStoreConfig::S3(s3_config) => {
                assert_eq!(s3_config.bucket, "metadata-bucket");
                assert_eq!(s3_config.region, "eu-west-1");
                assert_eq!(s3_config.endpoint, "https://metadata.example.com");
            }
            other => panic!("expected explicit S3 metadata config, got {other:?}"),
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
            matches!(config.metadata_store, MetadataStoreConfig::Inherit),
            "absent [metadata_store] section must deserialise as Inherit"
        );

        let resolved = config.resolve_metadata_config();
        match resolved {
            MetadataStoreConfig::S3(s3_config) => {
                assert_eq!(s3_config.bucket, "my-bucket");
                assert_eq!(s3_config.region, "us-east-1");
                assert_eq!(s3_config.endpoint, "https://s3.example.com");
                assert_eq!(s3_config.access_key_id.expose(), "key123");
                assert_eq!(s3_config.secret_key.expose(), "secret456");
                assert_eq!(s3_config.key_prefix, "prefix/");
            }
            other => panic!("expected S3 metadata config from Inherit, got {other:?}"),
        }
    }

    #[test]
    fn test_inherit_is_default_for_metadata_store_field() {
        let config_str = r#"
        [server]
        bind_address = "0.0.0.0"
        "#;

        let config = Configuration::load_from_str(config_str).unwrap();
        assert_eq!(
            config.metadata_store,
            MetadataStoreConfig::Inherit,
            "Configuration.metadata_store must default to Inherit when [metadata_store] is absent"
        );
    }
}
