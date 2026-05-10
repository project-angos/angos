use super::super::*;
use crate::registry::metadata_store;

#[test]
fn test_resolve_metadata_config_from_fs_blob_store() {
    let config = r#"
    [server]
    bind_address = "0.0.0.0"

    [blob_store.fs]
    root_dir = "/data/blobs"
    sync_to_disk = true
    "#;

    let config = Configuration::load_from_str(config).unwrap();
    let metadata_config = config.resolve_metadata_config();

    match metadata_config {
        metadata_store::MetadataStoreConfig::FS(fs_config) => {
            assert_eq!(fs_config.root_dir, "/data/blobs");
            assert!(fs_config.sync_to_disk);
            assert_eq!(
                fs_config.lock_strategy,
                metadata_store::LockStrategy::Memory
            );
        }
        metadata_store::MetadataStoreConfig::Inherit
        | metadata_store::MetadataStoreConfig::S3(_) => {
            panic!("Expected FS metadata store config")
        }
    }
}

#[test]
fn test_resolve_metadata_config_from_s3_blob_store() {
    let config = r#"
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

    let config = Configuration::load_from_str(config).unwrap();
    let metadata_config = config.resolve_metadata_config();

    match metadata_config {
        metadata_store::MetadataStoreConfig::S3(s3_config) => {
            assert_eq!(s3_config.bucket, "my-bucket");
            assert_eq!(s3_config.region, "us-east-1");
            assert_eq!(s3_config.endpoint, "https://s3.example.com");
            assert_eq!(s3_config.access_key_id.expose(), "key123");
            assert_eq!(s3_config.secret_key.expose(), "secret456");
            assert_eq!(s3_config.key_prefix, "prefix/");
            assert_eq!(
                s3_config.lock_strategy,
                metadata_store::LockStrategy::Memory
            );
        }
        metadata_store::MetadataStoreConfig::Inherit
        | metadata_store::MetadataStoreConfig::FS(_) => {
            panic!("Expected S3 metadata store config")
        }
    }
}

#[test]
fn test_resolve_metadata_config_preserves_explicit_s3_config() {
    let config = r#"
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

    let config = Configuration::load_from_str(config).unwrap();
    let metadata_config = config.resolve_metadata_config();

    match metadata_config {
        metadata_store::MetadataStoreConfig::S3(s3_config) => {
            assert_eq!(s3_config.bucket, "metadata-bucket");
            assert_eq!(s3_config.region, "eu-west-1");
            assert_eq!(s3_config.endpoint, "https://metadata.example.com");
        }
        metadata_store::MetadataStoreConfig::Inherit
        | metadata_store::MetadataStoreConfig::FS(_) => {
            panic!("Expected S3 metadata store config")
        }
    }
}
