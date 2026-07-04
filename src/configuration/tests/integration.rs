use std::{num::NonZeroUsize, path::PathBuf};

use angos_tx_engine::lock::LockStrategy;

use crate::{
    auth::oidc,
    cache,
    configuration::listeners::ClientAuth,
    configuration::{Configuration, Error, RegistryStorageConfig, ServerConfig},
    policy::AccessMode,
    registry::blob_store,
    replication::ReplicationMode,
};

#[test]
fn test_load_minimal_config() {
    let config = r#"
    [server]
    bind_address = "0.0.0.0"
    "#;

    let config = Configuration::load_from_str(config).unwrap();

    assert_eq!(config.global.max_concurrent_requests, 64);
    assert!(!config.global.update_pull_time);

    let ServerConfig::Insecure(server_config) = config.server else {
        panic!("Expected Insecure server config");
    };

    let bind_address = server_config.base.bind_address.to_string();
    assert_eq!(bind_address, "0.0.0.0".to_string());
    assert_eq!(server_config.base.port, 8000);
    assert_eq!(server_config.base.query_timeout.get(), 3600);
    assert_eq!(server_config.base.query_timeout_grace_period.get(), 60);

    assert_eq!(config.cache, cache::Config::Memory);
    assert_eq!(config.blob_store, blob_store::BlobStoreConfig::default());

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
    let expected = blob_store::BlobStoreConfig::FS(blob_store::FsBackendConfig {
        root_dir: "/data/registry".to_string(),
        sync_to_disk: false,
    });
    assert_eq!(config.blob_store, expected);

    // No explicit metadata_store section: field must be Inherit
    assert_eq!(config.registry_storage, RegistryStorageConfig::Inherit);
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
    match config.registry_storage {
        RegistryStorageConfig::FS(config) => {
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
    password = "$argon2id$v=19$m=19456,t=2,p=1$9pxWwg0VtZzDXno/25417Q$e+cuKy9VisJVxec/EEuKvvfIIIOy5yDGRzYKiuDLjx0"

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
fn test_repository_config() {
    let config = r#"
    [server]
    bind_address = "0.0.0.0"

    [auth.webhook.repo-auth]
    url = "https://auth.example.com/authorize"
    timeout_ms = 1000

    [event_webhook.repo-events]
    url = "https://events.example.com/hook"
    policy = "optional"
    events = ["manifest.push"]

    [repository.myapp]
    immutable_tags = true
    immutable_tags_exclusions = ["dev"]
    authorization_webhook = "repo-auth"
    event_webhooks = ["repo-events"]

    [[repository.myapp.upstream]]
    url = "https://registry.example.com"
    max_redirect = 3
    username = "mirror"
    password = "secret"

    [repository.myapp.access_policy]
    default = "allow"
    rules = ["identity.username == 'admin'"]

    [repository.myapp.retention_policy]
    rules = ["image.tag == 'latest'"]
    "#;

    let config = Configuration::load_from_str(config).unwrap();
    assert_eq!(config.repository.len(), 1);
    let repo = &config.repository["myapp"];
    assert_eq!(repo.upstream.len(), 1);
    assert_eq!(repo.upstream[0].url, "https://registry.example.com");
    assert_eq!(repo.upstream[0].max_redirect, 3);
    assert_eq!(repo.upstream[0].username.as_deref(), Some("mirror"));
    assert_eq!(
        repo.upstream[0]
            .password
            .as_ref()
            .map(|p| p.expose().as_str()),
        Some("secret")
    );
    assert_eq!(repo.access_policy.default, AccessMode::Allow);
    assert_eq!(repo.access_policy.rules.len(), 1);
    assert_eq!(repo.retention_policy.rules.len(), 1);
    assert!(repo.immutable_tags);
    assert_eq!(repo.immutable_tags_exclusions.len(), 1);
    assert_eq!(repo.immutable_tags_exclusions[0].as_source(), "dev");
    assert_eq!(repo.authorization_webhook.as_deref(), Some("repo-auth"));
    assert_eq!(repo.event_webhooks, ["repo-events"]);
}

#[test]
fn test_repository_downstream_config() {
    // Downstream tables must parse through the full Configuration, including
    // the repository map visitor and the flattened RegistryClientConfig.
    let config = r#"
    [server]
    bind_address = "0.0.0.0"

    [[repository.myapp.downstream]]
    name = "instance-b"
    url = "https://angos-eu.example.com"
    username = "replicator"
    password = "s3cret"
    mode = "event-only"
    namespace_filter = ["^nginx/.*"]
    max_concurrent_pushes = 8
    prune = true
    "#;

    let config = Configuration::load_from_str(config).unwrap();
    let repo = &config.repository["myapp"];
    assert_eq!(repo.downstream.len(), 1);
    let downstream = &repo.downstream[0];
    assert_eq!(downstream.name, "instance-b");
    // Flattened RegistryClientConfig fields.
    assert_eq!(downstream.client.url, "https://angos-eu.example.com");
    assert_eq!(downstream.client.username.as_deref(), Some("replicator"));
    assert_eq!(
        downstream
            .client
            .password
            .as_ref()
            .map(|p| p.expose().as_str()),
        Some("s3cret")
    );
    // Replication-only fields.
    assert_eq!(downstream.mode, ReplicationMode::EventOnly);
    assert_eq!(downstream.namespace_filter.len(), 1);
    assert_eq!(downstream.namespace_filter[0].as_source(), "^nginx/.*");
    assert_eq!(downstream.max_concurrent_pushes, NonZeroUsize::new(8));
    assert!(downstream.prune);
}

#[test]
fn test_repository_downstream_rejects_partial_mtls() {
    // The mTLS-pairing validation must fire through the full Configuration
    // parse too.
    let config = r#"
    [server]
    bind_address = "0.0.0.0"

    [[repository.myapp.downstream]]
    name = "instance-b"
    url = "https://angos-eu.example.com"
    client_certificate = "cert.pem"
    "#;

    let result = Configuration::load_from_str(config);
    assert!(
        result.is_err(),
        "partial mTLS in a downstream must be rejected at full-config parse"
    );
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
        Err(Error::InvalidFormat(_)) => {}
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
            // When client_auth is omitted, it defaults to Optional for backwards compatibility.
            assert_eq!(tls_config.tls.client_auth, ClientAuth::Optional);
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
            assert_eq!(insecure_config.base.bind_address.to_string(), "127.0.0.1");
            assert_eq!(insecure_config.base.port, 9000);
            assert_eq!(insecure_config.base.query_timeout.get(), 7200);
            assert_eq!(insecure_config.base.query_timeout_grace_period.get(), 120);
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
    let metadata_config = config.resolve_registry_storage();

    match metadata_config {
        RegistryStorageConfig::S3(s3_config) => {
            assert_eq!(s3_config.connection.bucket, "metadata-bucket");
            match &s3_config.lock_strategy {
                Some(LockStrategy::Redis(lock_config)) => {
                    assert_eq!(lock_config.url, "redis://localhost:6379");
                    assert_eq!(lock_config.ttl, 30);
                }
                other => panic!("Expected Redis lock strategy, got {other:?}"),
            }
        }
        RegistryStorageConfig::Inherit | RegistryStorageConfig::FS(_) => {
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
    let metadata_config = config.resolve_registry_storage();

    match metadata_config {
        RegistryStorageConfig::FS(fs_config) => {
            assert_eq!(fs_config.root_dir, "/data/metadata");
            match &fs_config.lock_strategy {
                LockStrategy::Redis(lock_config) => {
                    assert_eq!(lock_config.url, "redis://localhost:6379");
                    assert_eq!(lock_config.ttl, 30);
                }
                other => panic!("Expected Redis lock strategy, got {other:?}"),
            }
        }
        RegistryStorageConfig::Inherit | RegistryStorageConfig::S3(_) => {
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
            assert_eq!(insecure_config.base.bind_address.to_string(), "::1");
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
    default = "deny"
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
            assert_eq!(server_config.base.bind_address.to_string(), "127.0.0.1");
            assert_eq!(server_config.base.port, 8080);
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
fn test_event_webhook_invalid_config_fails_deserialization() {
    let config = r#"
    [server]
    bind_address = "0.0.0.0"

    [event_webhook.bad]
    url = "ht!tp://::invalid"
    policy = "required"
    events = ["manifest.push"]
    "#;

    let result = Configuration::load_from_str(config);
    let err = result.expect_err("malformed event webhook URL must fail to load");
    assert!(
        err.to_string().contains("url"),
        "error should mention the offending url field: {err}"
    );
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
    let metadata_config = config.resolve_registry_storage();

    match metadata_config {
        RegistryStorageConfig::S3(s3_config) => {
            assert_eq!(s3_config.connection.bucket, "metadata-bucket");
            match &s3_config.lock_strategy {
                Some(LockStrategy::S3(lock_config)) => {
                    assert_eq!(lock_config.ttl_secs, 30);
                    assert_eq!(lock_config.max_retries, 100);
                    assert_eq!(lock_config.retry_delay_ms, 50);
                }
                other => panic!("Expected S3 lock strategy, got {other:?}"),
            }
        }
        RegistryStorageConfig::Inherit | RegistryStorageConfig::FS(_) => {
            panic!("Expected S3 metadata store config")
        }
    }
}

// An S3 blob store with no explicit metadata-store lock strategy defaults to
// the shared S3 lock when the provider supports conditional writes, so the
// durable queue is accepted at config time; a provider that turns out to lack
// them is rejected at startup instead.
#[test]
fn job_queue_allows_unset_lock_strategy_on_s3() {
    let config = r#"
    [server]
    bind_address = "0.0.0.0"

    [global.job_queue]

    [blob_store.s3]
    bucket = "blob-bucket"
    region = "us-east-1"
    endpoint = "https://s3.amazonaws.com"
    access_key_id = "key"
    secret_key = "secret"
    "#;

    assert!(
        Configuration::load_from_str(config).is_ok(),
        "durable queue with an unset lock strategy on S3 must be accepted"
    );
}

// Declaring `conditional_operations = false` pins the unset lock strategy's
// fallback to the in-process `memory` lock, which cannot serialize across
// processes.
#[test]
fn job_queue_rejects_declared_no_cas_memory_fallback_on_s3() {
    let config = r#"
    [server]
    bind_address = "0.0.0.0"

    [global.job_queue]

    [blob_store.s3]
    bucket = "blob-bucket"
    region = "us-east-1"
    endpoint = "https://s3.amazonaws.com"
    access_key_id = "key"
    secret_key = "secret"

    [metadata_store.s3]
    bucket = "metadata-bucket"
    region = "us-east-1"
    endpoint = "https://s3.amazonaws.com"
    access_key_id = "key"
    secret_key = "secret"
    conditional_operations = false
    "#;

    let err = Configuration::load_from_str(config).unwrap_err();
    let message = err.to_string();
    assert!(
        message.contains("memory") && message.contains("lock"),
        "durable queue + declared no-CAS memory fallback must be rejected, got: {message}"
    );
}

// An explicit `memory` lock wins over the CAS-based default and stays
// incompatible with the durable queue.
#[test]
fn job_queue_rejects_explicit_memory_lock_on_s3() {
    let config = r#"
    [server]
    bind_address = "0.0.0.0"

    [global.job_queue]

    [blob_store.s3]
    bucket = "blob-bucket"
    region = "us-east-1"
    endpoint = "https://s3.amazonaws.com"
    access_key_id = "key"
    secret_key = "secret"

    [metadata_store.s3]
    bucket = "metadata-bucket"
    region = "us-east-1"
    endpoint = "https://s3.amazonaws.com"
    access_key_id = "key"
    secret_key = "secret"
    lock_strategy = "memory"
    "#;

    let err = Configuration::load_from_str(config).unwrap_err();
    let message = err.to_string();
    assert!(
        message.contains("memory") && message.contains("lock"),
        "durable queue + explicit memory lock must be rejected, got: {message}"
    );
}

#[test]
fn job_queue_rejects_fs_memory_lock() {
    let config = r#"
    [server]
    bind_address = "0.0.0.0"

    [global.job_queue]

    [blob_store.fs]
    root_dir = "/data/blobs"
    "#;

    let err = Configuration::load_from_str(config).unwrap_err();
    assert!(
        err.to_string().contains("lock"),
        "durable queue + FS memory lock must be rejected, got: {err}"
    );
}

#[test]
fn job_queue_allows_shared_s3_lock() {
    let config = r#"
    [server]
    bind_address = "0.0.0.0"

    [global.job_queue]

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

    assert!(
        Configuration::load_from_str(config).is_ok(),
        "durable queue with a shared S3 lock must be accepted"
    );
}

// Without [global.job_queue] the queue drains in a single process, so the
// memory lock suffices.
#[test]
fn in_process_queue_allows_memory_lock() {
    let config = r#"
    [server]
    bind_address = "0.0.0.0"

    [blob_store.fs]
    root_dir = "/data/blobs"
    "#;

    assert!(
        Configuration::load_from_str(config).is_ok(),
        "in-process queue with a memory lock must be accepted"
    );
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
    let metadata_config = config.resolve_registry_storage();

    match metadata_config {
        RegistryStorageConfig::S3(s3_config) => match &s3_config.lock_strategy {
            Some(LockStrategy::S3(lock_config)) => {
                assert_eq!(lock_config.ttl_secs, 60);
                assert_eq!(lock_config.max_retries, 50);
                assert_eq!(lock_config.retry_delay_ms, 100);
            }
            other => panic!("Expected S3 lock strategy, got {other:?}"),
        },
        RegistryStorageConfig::Inherit | RegistryStorageConfig::FS(_) => {
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
    let metadata_config = config.resolve_registry_storage();

    match metadata_config {
        RegistryStorageConfig::S3(s3_config) => {
            assert!(
                matches!(s3_config.lock_strategy, Some(LockStrategy::Memory)),
                "Expected Memory lock strategy, got {:?}",
                s3_config.lock_strategy
            );
        }
        RegistryStorageConfig::Inherit | RegistryStorageConfig::FS(_) => {
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
fn event_webhook_valid_global_reference_loads() {
    let config = r#"
    [server]
    bind_address = "0.0.0.0"

    [global]
    event_webhooks = ["my-hook"]

    [event_webhook.my-hook]
    url = "https://example.com/hook"
    policy = "optional"
    events = ["manifest.push"]
    "#;

    let config = Configuration::load_from_str(config).unwrap();
    assert!(config.event_webhook.contains_key("my-hook"));
}

#[test]
fn event_webhook_valid_repo_reference_loads() {
    let config = r#"
    [server]
    bind_address = "0.0.0.0"

    [event_webhook.repo-hook]
    url = "https://example.com/hook"
    policy = "optional"
    events = ["manifest.push"]

    [repository.myrepo]
    event_webhooks = ["repo-hook"]
    "#;

    let config = Configuration::load_from_str(config).unwrap();
    assert!(config.repository.contains_key("myrepo"));
    assert!(config.event_webhook.contains_key("repo-hook"));
}

#[test]
fn event_webhook_empty_events_list_fails_load() {
    let config = r#"
    [server]
    bind_address = "0.0.0.0"

    [event_webhook.silent]
    url = "https://example.com/hook"
    policy = "required"
    events = []
    "#;

    let result = Configuration::load_from_str(config);
    let err = result.expect_err("event webhook with empty events list must fail to load");
    assert!(
        err.to_string().contains("silent"),
        "Error must identify the offending webhook by name: {err}"
    );
}
