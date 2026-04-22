use std::path::PathBuf;

use super::*;
use crate::{auth::oidc, registry::data_store};

#[test]
fn test_load_minimal_config() {
    let config = r#"
    [server]
    bind_address = "0.0.0.0"
    "#;

    let config = Configuration::load_from_str(config).unwrap();

    assert_eq!(config.global.max_concurrent_requests, 64);
    assert_eq!(config.global.max_concurrent_cache_jobs, 4);
    assert!(!config.global.update_pull_time);

    let ServerConfig::Insecure(server_config) = config.server else {
        panic!("Expected Insecure server config");
    };

    let bind_address = server_config.bind_address.to_string();
    assert_eq!(bind_address, "0.0.0.0".to_string());
    assert_eq!(server_config.port, 8000);
    assert_eq!(server_config.query_timeout, 3600);
    assert_eq!(server_config.query_timeout_grace_period, 60);

    assert_eq!(config.cache, cache::Config::Memory);
    assert_eq!(config.blob_store, blob_store::BlobStorageConfig::default());

    assert!(config.auth.identity.is_empty());
    assert!(config.repository.is_empty());
    assert!(config.observability.is_none());
}

#[test]
fn test_storage_field_backward_compatibility() {
    // Test that old 'storage' field is supported for backward compatibility
    let config = r#"
    [server]
    bind_address = "0.0.0.0"

    [storage.fs]
    root_dir = "/data/registry"
    "#;

    let config = Configuration::load_from_str(config).unwrap();

    // Should parse 'storage' as 'blob_store'
    let expected = blob_store::BlobStorageConfig::FS(data_store::fs::BackendConfig {
        root_dir: "/data/registry".to_string(),
        sync_to_disk: false,
    });
    assert_eq!(config.blob_store, expected);

    // Should autoconfigure metadata store based on blob store
    assert_eq!(config.metadata_store, None);
}

#[test]
fn test_tls_config_detection() {
    let config = r#"
    [server]
    bind_address = "0.0.0.0"
    port = 8000

    [server.tls]
    server_certificate_bundle = "server.pem"
    server_private_key = "server.key"
    "#;

    let config = Configuration::load_from_str(config).unwrap();

    match config.server {
        ServerConfig::Tls(tls_config) => {
            assert_eq!(
                tls_config.tls.server_certificate_bundle.to_str(),
                Some("server.pem")
            );
            assert_eq!(
                tls_config.tls.server_private_key.to_str(),
                Some("server.key")
            );
        }
        ServerConfig::Insecure(_) => {
            panic!("Expected TLS server config but got Insecure");
        }
    }
}

#[test]
fn test_metadata_store_explicit_config_not_overridden() {
    // When metadata store is explicitly configured, it should not be overridden
    let config = r#"
    [server]
    bind_address = "0.0.0.0"

    [blob_store.s3]
    bucket = "blob-bucket"
    region = "us-west-2"
    endpoint = "https://blob.example.com"
    access_key_id = "blob-key"
    secret_key = "blob-secret"

    [metadata_store.fs]
    root_dir = "/custom/metadata/path"
    "#;

    let config = Configuration::load_from_str(config).unwrap();

    // Should keep the explicitly configured FS metadata store
    match config.metadata_store {
        Some(metadata_store::MetadataStoreConfig::FS(config)) => {
            assert_eq!(config.root_dir, "/custom/metadata/path");
        }
        _ => panic!("Expected explicitly configured FS metadata store to be preserved"),
    }
}

#[test]
fn test_auth_section() {
    let config = r#"
    [server]
    bind_address = "0.0.0.0"

    [auth.identity.user1]
    username = "bob"
    password = "password456"

    [auth.oidc.generic]
    provider = "generic"
    issuer = "https://example.com"
    discovery_url = "https://example.com/.well-known/openid-configuration"
    "#;

    let config = Configuration::load_from_str(config).unwrap();
    assert_eq!(config.auth.identity.len(), 1);
    assert_eq!(config.auth.identity["user1"].username, "bob");
    assert_eq!(config.auth.oidc.len(), 1);
    assert!(matches!(
        config.auth.oidc.get("generic"),
        Some(oidc::Config::Generic(_))
    ));
}

#[test]
fn test_global_config_default() {
    let config = GlobalConfig::default();
    assert_eq!(config.max_concurrent_requests, 64);
    assert_eq!(config.max_concurrent_cache_jobs, 4);
    assert!(!config.update_pull_time);
    assert!(!config.immutable_tags);
    assert!(config.immutable_tags_exclusions.is_empty());
    assert!(config.authorization_webhook.is_none());
}

#[test]
fn test_global_config_custom_values() {
    let config = r#"
    [server]
    bind_address = "0.0.0.0"

    [global]
    max_concurrent_requests = 10
    max_concurrent_cache_jobs = 8
    update_pull_time = true
    immutable_tags = true
    immutable_tags_exclusions = ["latest", "dev"]
    authorization_webhook = "my-webhook"

    [auth.webhook.my-webhook]
    url = "https://example.com/webhook"
    timeout_ms = 5000
    "#;

    let config = Configuration::load_from_str(config).unwrap();
    assert_eq!(config.global.max_concurrent_requests, 10);
    assert_eq!(config.global.max_concurrent_cache_jobs, 8);
    assert!(config.global.update_pull_time);
    assert!(config.global.immutable_tags);
    assert_eq!(config.global.immutable_tags_exclusions.len(), 2);
    assert_eq!(config.global.immutable_tags_exclusions[0], "latest");
    assert_eq!(config.global.immutable_tags_exclusions[1], "dev");
    assert_eq!(
        config.global.authorization_webhook,
        Some("my-webhook".to_string())
    );
}

#[test]
fn test_ui_enabled_config() {
    let config = r#"
    [server]
    bind_address = "0.0.0.0"

    [ui]
    enabled = true
    "#;

    let config = Configuration::load_from_str(config).unwrap();
    assert!(config.ui.enabled);
}

#[test]
fn test_ui_enabled_default_false() {
    let config = r#"
    [server]
    bind_address = "0.0.0.0"
    "#;

    let config = Configuration::load_from_str(config).unwrap();
    assert!(!config.ui.enabled);
    assert_eq!(config.ui.name, "Angos");
}

#[test]
fn test_ui_custom_name() {
    let config = r#"
    [server]
    bind_address = "0.0.0.0"

    [ui]
    enabled = true
    name = "my-registry"
    "#;

    let config = Configuration::load_from_str(config).unwrap();
    assert!(config.ui.enabled);
    assert_eq!(config.ui.name, "my-registry");
}

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
        metadata_store::MetadataStoreConfig::S3(_) => {
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
            assert_eq!(s3_config.access_key_id, "key123");
            assert_eq!(s3_config.secret_key, "secret456");
            assert_eq!(s3_config.key_prefix, "prefix/");
            assert_eq!(
                s3_config.lock_strategy,
                metadata_store::LockStrategy::Memory
            );
        }
        metadata_store::MetadataStoreConfig::FS(_) => {
            panic!("Expected S3 metadata store config")
        }
    }
}

#[test]
fn test_repository_config() {
    let config = r#"
    [server]
    bind_address = "0.0.0.0"

    [repository.myapp]
    immutable_tags = true
    immutable_tags_exclusions = ["dev"]
    "#;

    let config = Configuration::load_from_str(config).unwrap();
    assert_eq!(config.repository.len(), 1);
    assert!(config.repository.contains_key("myapp"));
    assert!(config.repository["myapp"].immutable_tags);
    assert_eq!(
        config.repository["myapp"].immutable_tags_exclusions.len(),
        1
    );
}

#[test]
fn test_observability_config() {
    let config = r#"
    [server]
    bind_address = "0.0.0.0"

    [observability.tracing]
    endpoint = "http://jaeger:4317"
    sampling_rate = 0.1
    "#;

    let config = Configuration::load_from_str(config).unwrap();
    assert!(config.observability.is_some());
    let observability = config.observability.unwrap();
    assert!(observability.tracing.is_some());
    let tracing = observability.tracing.unwrap();
    assert_eq!(tracing.endpoint, "http://jaeger:4317");
    assert!((tracing.sampling_rate - 0.1).abs() < f64::EPSILON);
}

#[test]
fn test_cache_config_memory() {
    let config = r#"
    [server]
    bind_address = "0.0.0.0"

    [cache]
    memory = {}
    "#;

    let config = Configuration::load_from_str(config).unwrap();
    assert!(matches!(config.cache, cache::Config::Memory));
}

#[test]
fn test_cache_config_redis() {
    let config = r#"
    [server]
    bind_address = "0.0.0.0"

    [cache.redis]
    url = "redis://localhost:6379"
    key_prefix = "angos:"
    "#;

    let config = Configuration::load_from_str(config).unwrap();
    match config.cache {
        cache::Config::Redis(redis_config) => {
            assert_eq!(redis_config.url, "redis://localhost:6379");
        }
        cache::Config::Memory => panic!("Expected Redis cache config"),
    }
}

#[test]
fn test_cache_store_backward_compatibility() {
    let config = r#"
    [server]
    bind_address = "0.0.0.0"

    [cache_store.redis]
    url = "redis://localhost:6379"
    key_prefix = "angos:"
    "#;

    let config = Configuration::load_from_str(config).unwrap();
    match config.cache {
        cache::Config::Redis(redis_config) => {
            assert_eq!(redis_config.url, "redis://localhost:6379");
        }
        cache::Config::Memory => panic!("Expected Redis cache config"),
    }
}

#[test]
fn test_invalid_toml_format() {
    let config = r#"
    [server
    bind_address = "0.0.0.0"
    "#;

    let result = Configuration::load_from_str(config);
    assert!(result.is_err());
    match result {
        Err(Error::NotReadable(_)) => {}
        _ => panic!("Expected NotReadable error"),
    }
}

#[test]
fn test_validate_webhook_referenced_globally() {
    let config = r#"
    [server]
    bind_address = "0.0.0.0"

    [global]
    authorization_webhook = "my-webhook"

    [auth.webhook.my-webhook]
    url = "https://example.com/webhook"
    timeout_ms = 5000
    "#;

    let result = Configuration::load_from_str(config);
    assert!(result.is_ok());
}

#[test]
fn test_validate_webhook_missing_global_reference() {
    let config = r#"
    [server]
    bind_address = "0.0.0.0"

    [global]
    authorization_webhook = "nonexistent-webhook"
    "#;

    let result = Configuration::load_from_str(config);
    assert!(result.is_err());
    match result {
        Err(Error::InvalidFormat(msg)) => {
            assert!(msg.contains("Webhook 'nonexistent-webhook' not found"));
            assert!(msg.contains("referenced globally"));
        }
        _ => panic!("Expected InvalidFormat error"),
    }
}

#[test]
fn test_validate_webhook_referenced_in_repository() {
    let config = r#"
    [server]
    bind_address = "0.0.0.0"

    [repository.myapp]
    authorization_webhook = "repo-webhook"

    [auth.webhook.repo-webhook]
    url = "https://example.com/webhook"
    timeout_ms = 5000
    "#;

    let result = Configuration::load_from_str(config);
    assert!(result.is_ok());
}

#[test]
fn test_validate_webhook_missing_repository_reference() {
    let config = r#"
    [server]
    bind_address = "0.0.0.0"

    [repository.myapp]
    authorization_webhook = "missing-webhook"
    "#;

    let result = Configuration::load_from_str(config);
    assert!(result.is_err());
    match result {
        Err(Error::InvalidFormat(msg)) => {
            assert!(msg.contains("Webhook 'missing-webhook' not found"));
            assert!(msg.contains("referenced in 'myapp' repository"));
        }
        _ => panic!("Expected InvalidFormat error"),
    }
}

#[test]
fn test_validate_webhook_empty_string_in_repository() {
    let config = r#"
    [server]
    bind_address = "0.0.0.0"

    [repository.myapp]
    authorization_webhook = ""
    "#;

    let result = Configuration::load_from_str(config);
    assert!(result.is_ok());
}

#[test]
fn test_validate_invalid_webhook_config() {
    let config = r#"
    [server]
    bind_address = "0.0.0.0"

    [auth.webhook.bad-webhook]
    url = "ht!tp://::invalid"
    timeout_ms = 5000
    "#;

    let result = Configuration::load_from_str(config);
    assert!(result.is_err());
    match result {
        Err(Error::InvalidFormat(msg)) => {
            assert!(msg.contains("Invalid webhook 'bad-webhook'"));
        }
        _ => panic!("Expected InvalidFormat error"),
    }
}

#[test]
fn test_tls_config_with_client_ca() {
    let config = r#"
    [server]
    bind_address = "0.0.0.0"
    port = 8443

    [server.tls]
    server_certificate_bundle = "server.pem"
    server_private_key = "server.key"
    client_ca_bundle = "ca.pem"
    "#;

    let config = Configuration::load_from_str(config).unwrap();
    match config.server {
        ServerConfig::Tls(tls_config) => {
            assert_eq!(
                tls_config.tls.server_certificate_bundle,
                PathBuf::from("server.pem")
            );
            assert_eq!(
                tls_config.tls.server_private_key,
                PathBuf::from("server.key")
            );
            assert_eq!(
                tls_config.tls.client_ca_bundle,
                Some(PathBuf::from("ca.pem"))
            );
        }
        ServerConfig::Insecure(_) => panic!("Expected TLS server config"),
    }
}

#[test]
fn test_insecure_config_with_custom_port() {
    let config = r#"
    [server]
    bind_address = "127.0.0.1"
    port = 9000
    query_timeout = 7200
    query_timeout_grace_period = 120
    "#;

    let config = Configuration::load_from_str(config).unwrap();
    match config.server {
        ServerConfig::Insecure(insecure_config) => {
            assert_eq!(insecure_config.bind_address.to_string(), "127.0.0.1");
            assert_eq!(insecure_config.port, 9000);
            assert_eq!(insecure_config.query_timeout, 7200);
            assert_eq!(insecure_config.query_timeout_grace_period, 120);
        }
        ServerConfig::Tls(_) => panic!("Expected Insecure server config"),
    }
}

#[test]
fn test_multiple_repositories() {
    let config = r#"
    [server]
    bind_address = "0.0.0.0"

    [repository.app1]
    immutable_tags = true

    [repository.app2]
    immutable_tags = false

    [repository.app3]
    immutable_tags = true
    immutable_tags_exclusions = ["dev", "test"]
    "#;

    let config = Configuration::load_from_str(config).unwrap();
    assert_eq!(config.repository.len(), 3);
    assert!(config.repository["app1"].immutable_tags);
    assert!(!config.repository["app2"].immutable_tags);
    assert!(config.repository["app3"].immutable_tags);
    assert_eq!(config.repository["app3"].immutable_tags_exclusions.len(), 2);
}

#[test]
fn test_metadata_store_s3_with_redis() {
    let config = r#"
    [server]
    bind_address = "0.0.0.0"

    [blob_store.s3]
    bucket = "my-bucket"
    region = "us-east-1"
    endpoint = "https://s3.amazonaws.com"
    access_key_id = "blob-key"
    secret_key = "blob-secret"

    [metadata_store.s3]
    bucket = "metadata-bucket"
    region = "us-east-1"
    endpoint = "https://s3.amazonaws.com"
    access_key_id = "key"
    secret_key = "secret"

    [metadata_store.s3.redis]
    url = "redis://localhost:6379"
    ttl = 30
    "#;

    let config = Configuration::load_from_str(config).unwrap();
    let metadata_config = config.resolve_metadata_config();

    match metadata_config {
        metadata_store::MetadataStoreConfig::S3(s3_config) => {
            assert_eq!(s3_config.bucket, "metadata-bucket");
            match &s3_config.lock_strategy {
                metadata_store::LockStrategy::Redis(lock_config) => {
                    assert_eq!(lock_config.url, "redis://localhost:6379");
                    assert_eq!(lock_config.ttl, 30);
                }
                other => panic!("Expected Redis lock strategy, got {other:?}"),
            }
        }
        metadata_store::MetadataStoreConfig::FS(_) => {
            panic!("Expected S3 metadata store config")
        }
    }
}

#[test]
fn test_metadata_store_fs_with_redis() {
    let config = r#"
    [server]
    bind_address = "0.0.0.0"

    [metadata_store.fs]
    root_dir = "/data/metadata"

    [metadata_store.fs.redis]
    url = "redis://localhost:6379"
    ttl = 30
    "#;

    let config = Configuration::load_from_str(config).unwrap();
    let metadata_config = config.resolve_metadata_config();

    match metadata_config {
        metadata_store::MetadataStoreConfig::FS(fs_config) => {
            assert_eq!(fs_config.root_dir, "/data/metadata");
            match &fs_config.lock_strategy {
                metadata_store::LockStrategy::Redis(lock_config) => {
                    assert_eq!(lock_config.url, "redis://localhost:6379");
                    assert_eq!(lock_config.ttl, 30);
                }
                other => panic!("Expected Redis lock strategy, got {other:?}"),
            }
        }
        metadata_store::MetadataStoreConfig::S3(_) => {
            panic!("Expected FS metadata store config")
        }
    }
}

#[test]
fn test_ipv6_bind_address() {
    let config = r#"
    [server]
    bind_address = "::1"
    "#;

    let config = Configuration::load_from_str(config).unwrap();
    match config.server {
        ServerConfig::Insecure(insecure_config) => {
            assert_eq!(insecure_config.bind_address.to_string(), "::1");
        }
        ServerConfig::Tls(_) => panic!("Expected Insecure server config"),
    }
}

#[test]
fn test_access_policy_in_global_config() {
    let config = r#"
    [server]
    bind_address = "0.0.0.0"

    [global.access_policy]
    mode = "deny"
    rules = ["allow(true)"]
    "#;

    let config = Configuration::load_from_str(config).unwrap();
    assert_eq!(config.global.access_policy.rules.len(), 1);
}

#[test]
fn test_retention_policy_in_global_config() {
    let config = r#"
    [server]
    bind_address = "0.0.0.0"

    [global.retention_policy]
    rules = ["age(image) > days(30)"]
    "#;

    let config = Configuration::load_from_str(config).unwrap();
    assert_eq!(config.global.retention_policy.rules.len(), 1);
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
        metadata_store::MetadataStoreConfig::FS(_) => {
            panic!("Expected S3 metadata store config")
        }
    }
}

#[test]
fn test_multiple_webhooks() {
    let config = r#"
    [server]
    bind_address = "0.0.0.0"

    [auth.webhook.webhook1]
    url = "https://webhook1.example.com"
    timeout_ms = 5000

    [auth.webhook.webhook2]
    url = "https://webhook2.example.com"
    timeout_ms = 5000

    [auth.webhook.webhook3]
    url = "https://webhook3.example.com"
    timeout_ms = 5000
    "#;

    let config = Configuration::load_from_str(config).unwrap();
    assert_eq!(config.auth.webhook.len(), 3);
    assert!(config.auth.webhook.contains_key("webhook1"));
    assert!(config.auth.webhook.contains_key("webhook2"));
    assert!(config.auth.webhook.contains_key("webhook3"));
}

#[test]
fn test_validate_multiple_repositories_with_webhooks() {
    let config = r#"
    [server]
    bind_address = "0.0.0.0"

    [repository.app1]
    authorization_webhook = "webhook1"

    [repository.app2]
    authorization_webhook = "webhook2"

    [auth.webhook.webhook1]
    url = "https://webhook1.example.com"
    timeout_ms = 5000

    [auth.webhook.webhook2]
    url = "https://webhook2.example.com"
    timeout_ms = 5000
    "#;

    let result = Configuration::load_from_str(config);
    assert!(result.is_ok());
}

#[test]
fn test_load_from_file() {
    use std::io::Write;

    use tempfile::NamedTempFile;

    let config_content = r#"
    [server]
    bind_address = "127.0.0.1"
    port = 8080

    [global]
    max_concurrent_requests = 8

    [blob_store.fs]
    root_dir = "/data/registry"
    "#;

    let mut temp_file = NamedTempFile::new().unwrap();
    temp_file.write_all(config_content.as_bytes()).unwrap();
    temp_file.flush().unwrap();

    let result = Configuration::load(temp_file.path());
    assert!(result.is_ok());

    let config = result.unwrap();
    assert_eq!(config.global.max_concurrent_requests, 8);

    match config.server {
        ServerConfig::Insecure(server_config) => {
            assert_eq!(server_config.bind_address.to_string(), "127.0.0.1");
            assert_eq!(server_config.port, 8080);
        }
        ServerConfig::Tls(_) => panic!("Expected Insecure server config"),
    }
}

#[test]
fn test_load_from_nonexistent_file() {
    let result = Configuration::load("/nonexistent/path/to/config.toml");
    assert!(result.is_err());

    match result {
        Err(Error::NotReadable(msg)) => {
            assert!(msg.contains("Unable to read configuration file"));
        }
        _ => panic!("Expected NotReadable error"),
    }
}

#[test]
fn test_load_from_file_with_invalid_content() {
    use std::io::Write;

    use tempfile::NamedTempFile;

    let invalid_config = r#"
    [server
    bind_address = "0.0.0.0"
    "#;

    let mut temp_file = NamedTempFile::new().unwrap();
    temp_file.write_all(invalid_config.as_bytes()).unwrap();
    temp_file.flush().unwrap();

    let result = Configuration::load(temp_file.path());
    assert!(result.is_err());

    match result {
        Err(Error::NotReadable(_)) => {}
        _ => panic!("Expected NotReadable error"),
    }
}

#[test]
fn test_load_from_file_with_tls_config() {
    use std::io::Write;

    use tempfile::NamedTempFile;

    let config_content = r#"
    [server]
    bind_address = "0.0.0.0"
    port = 8443

    [server.tls]
    server_certificate_bundle = "/path/to/cert.pem"
    server_private_key = "/path/to/key.pem"
    "#;

    let mut temp_file = NamedTempFile::new().unwrap();
    temp_file.write_all(config_content.as_bytes()).unwrap();
    temp_file.flush().unwrap();

    let result = Configuration::load(temp_file.path());
    assert!(result.is_ok());

    let config = result.unwrap();
    match config.server {
        ServerConfig::Tls(tls_config) => {
            assert_eq!(
                tls_config.tls.server_certificate_bundle,
                PathBuf::from("/path/to/cert.pem")
            );
            assert_eq!(
                tls_config.tls.server_private_key,
                PathBuf::from("/path/to/key.pem")
            );
        }
        ServerConfig::Insecure(_) => panic!("Expected TLS server config"),
    }
}

#[test]
fn test_load_from_file_with_validation_error() {
    use std::io::Write;

    use tempfile::NamedTempFile;

    let config_content = r#"
    [server]
    bind_address = "0.0.0.0"

    [global]
    authorization_webhook = "missing-webhook"
    "#;

    let mut temp_file = NamedTempFile::new().unwrap();
    temp_file.write_all(config_content.as_bytes()).unwrap();
    temp_file.flush().unwrap();

    let result = Configuration::load(temp_file.path());
    assert!(result.is_err());

    match result {
        Err(Error::InvalidFormat(msg)) => {
            assert!(msg.contains("Webhook 'missing-webhook' not found"));
        }
        _ => panic!("Expected InvalidFormat error"),
    }
}

#[test]
fn test_event_webhook_config_parses() {
    let config = r#"
    [server]
    bind_address = "0.0.0.0"

    [event_webhook.notify]
    url = "https://example.com/events"
    policy = "required"
    events = ["manifest.push", "tag.create"]

    [event_webhook.audit]
    url = "https://audit.example.com/hook"
    policy = "async"
    token = "my-secret"
    timeout_ms = 10000
    max_retries = 3
    events = ["manifest.push", "manifest.delete", "blob.push", "tag.create", "tag.delete"]
    repository_filter = ["^production/.*"]
    "#;

    let config = Configuration::load_from_str(config).unwrap();
    assert_eq!(config.event_webhook.len(), 2);
    assert!(config.event_webhook.contains_key("notify"));
    assert!(config.event_webhook.contains_key("audit"));
}

#[test]
fn test_event_webhook_backward_compatible() {
    let config = r#"
    [server]
    bind_address = "0.0.0.0"
    "#;

    let config = Configuration::load_from_str(config).unwrap();
    assert!(config.event_webhook.is_empty());
}

#[test]
fn test_global_event_webhooks_references() {
    let config = r#"
    [server]
    bind_address = "0.0.0.0"

    [global]
    event_webhooks = ["notify"]

    [event_webhook.notify]
    url = "https://example.com/events"
    policy = "optional"
    events = ["manifest.push"]
    "#;

    let config = Configuration::load_from_str(config).unwrap();
    assert_eq!(config.global.event_webhooks.len(), 1);
    assert_eq!(config.global.event_webhooks[0], "notify");
}

#[test]
fn test_repository_event_webhooks_references() {
    let config = r#"
    [server]
    bind_address = "0.0.0.0"

    [event_webhook.notify]
    url = "https://example.com/events"
    policy = "required"
    events = ["manifest.push"]

    [repository.myapp]
    event_webhooks = ["notify"]
    "#;

    let config = Configuration::load_from_str(config).unwrap();
    assert_eq!(config.repository["myapp"].event_webhooks.len(), 1);
    assert_eq!(config.repository["myapp"].event_webhooks[0], "notify");
}

#[test]
fn test_event_webhook_nonexistent_global_reference_fails() {
    let config = r#"
    [server]
    bind_address = "0.0.0.0"

    [global]
    event_webhooks = ["nonexistent"]
    "#;

    let result = Configuration::load_from_str(config);
    assert!(result.is_err());
    match result {
        Err(Error::InvalidFormat(msg)) => {
            assert!(msg.contains("nonexistent"));
        }
        _ => panic!("Expected InvalidFormat error"),
    }
}

#[test]
fn test_event_webhook_nonexistent_repository_reference_fails() {
    let config = r#"
    [server]
    bind_address = "0.0.0.0"

    [event_webhook.notify]
    url = "https://example.com/events"
    policy = "optional"
    events = ["manifest.push"]

    [repository.myapp]
    event_webhooks = ["missing-hook"]
    "#;

    let result = Configuration::load_from_str(config);
    assert!(result.is_err());
    match result {
        Err(Error::InvalidFormat(msg)) => {
            assert!(msg.contains("missing-hook"));
            assert!(msg.contains("myapp"));
        }
        _ => panic!("Expected InvalidFormat error"),
    }
}

#[test]
fn test_event_webhook_invalid_config_fails_validation() {
    let config = r#"
    [server]
    bind_address = "0.0.0.0"

    [event_webhook.bad]
    url = "ht!tp://::invalid"
    policy = "required"
    events = ["manifest.push"]
    "#;

    let result = Configuration::load_from_str(config);
    assert!(result.is_err());
    match result {
        Err(Error::InvalidFormat(msg)) => {
            assert!(msg.contains("bad"));
        }
        _ => panic!("Expected InvalidFormat error"),
    }
}

#[test]
fn test_metadata_store_s3_lock_strategy_s3_defaults() {
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
    region = "us-east-1"
    endpoint = "https://s3.amazonaws.com"
    access_key_id = "key"
    secret_key = "secret"

    [metadata_store.s3.lock_strategy.s3]
    "#;

    let config = Configuration::load_from_str(config).unwrap();
    let metadata_config = config.resolve_metadata_config();

    match metadata_config {
        metadata_store::MetadataStoreConfig::S3(s3_config) => {
            assert_eq!(s3_config.bucket, "metadata-bucket");
            match &s3_config.lock_strategy {
                metadata_store::LockStrategy::S3(lock_config) => {
                    assert_eq!(lock_config.ttl_secs, 30);
                    assert_eq!(lock_config.max_retries, 100);
                    assert_eq!(lock_config.retry_delay_ms, 50);
                }
                other => panic!("Expected S3 lock strategy, got {other:?}"),
            }
        }
        metadata_store::MetadataStoreConfig::FS(_) => {
            panic!("Expected S3 metadata store config")
        }
    }
}

#[test]
fn test_metadata_store_s3_lock_strategy_s3_custom_values() {
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
    region = "us-east-1"
    endpoint = "https://s3.amazonaws.com"
    access_key_id = "key"
    secret_key = "secret"

    [metadata_store.s3.lock_strategy.s3]
    ttl_secs = 60
    max_retries = 50
    retry_delay_ms = 100
    "#;

    let config = Configuration::load_from_str(config).unwrap();
    let metadata_config = config.resolve_metadata_config();

    match metadata_config {
        metadata_store::MetadataStoreConfig::S3(s3_config) => match &s3_config.lock_strategy {
            metadata_store::LockStrategy::S3(lock_config) => {
                assert_eq!(lock_config.ttl_secs, 60);
                assert_eq!(lock_config.max_retries, 50);
                assert_eq!(lock_config.retry_delay_ms, 100);
            }
            other => panic!("Expected S3 lock strategy, got {other:?}"),
        },
        metadata_store::MetadataStoreConfig::FS(_) => {
            panic!("Expected S3 metadata store config")
        }
    }
}

#[test]
fn test_metadata_store_s3_lock_strategy_memory() {
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
    region = "us-east-1"
    endpoint = "https://s3.amazonaws.com"
    access_key_id = "key"
    secret_key = "secret"
    lock_strategy = "memory"
    "#;

    let config = Configuration::load_from_str(config).unwrap();
    let metadata_config = config.resolve_metadata_config();

    match metadata_config {
        metadata_store::MetadataStoreConfig::S3(s3_config) => {
            assert!(
                matches!(
                    s3_config.lock_strategy,
                    metadata_store::LockStrategy::Memory
                ),
                "Expected Memory lock strategy, got {:?}",
                s3_config.lock_strategy
            );
        }
        metadata_store::MetadataStoreConfig::FS(_) => {
            panic!("Expected S3 metadata store config")
        }
    }
}

#[test]
fn test_metadata_store_s3_both_redis_and_lock_strategy_fails() {
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
    region = "us-east-1"
    endpoint = "https://s3.amazonaws.com"
    access_key_id = "key"
    secret_key = "secret"
    lock_strategy = "memory"

    [metadata_store.s3.redis]
    url = "redis://localhost:6379"
    ttl = 30
    "#;

    let result = Configuration::load_from_str(config);
    assert!(
        result.is_err(),
        "Expected error when both redis and lock_strategy are set"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("lock_strategy") && err_msg.contains("redis"),
        "Error should mention both 'lock_strategy' and 'redis', got: {err_msg}"
    );
}

#[test]
fn test_metadata_store_fs_lock_strategy_s3_fails() {
    let config = r#"
    [server]
    bind_address = "0.0.0.0"

    [metadata_store.fs]
    root_dir = "/data/metadata"

    [metadata_store.fs.lock_strategy.s3]
    "#;

    let result = Configuration::load_from_str(config);
    assert!(
        result.is_err(),
        "Expected error when S3 lock strategy is used with FS metadata store"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("S3 lock strategy") || err_msg.contains("filesystem"),
        "Error should explain S3 lock strategy is unsupported for FS store, got: {err_msg}"
    );
}

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
