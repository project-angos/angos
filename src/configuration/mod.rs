use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;

use serde::Deserialize;
use tracing::info;

mod error;

pub use error::Error;

use crate::cache;
use crate::command::server::auth::authenticator;
use crate::command::server::listeners::{insecure, tls};
use crate::event_webhook::config::EventWebhookConfig;
use crate::policy::{AccessPolicyConfig, RetentionPolicyConfig};
use crate::registry::{blob_store, metadata_store, repository};

#[derive(Clone, Debug, Deserialize)]
pub struct Configuration {
    pub server: ServerConfig,
    #[serde(default)]
    pub global: GlobalConfig,
    #[serde(default)]
    pub ui: UiConfig,
    #[serde(default, alias = "cache_store")]
    pub cache: cache::Config,
    #[serde(default, alias = "storage")]
    pub blob_store: blob_store::BlobStorageConfig,
    #[serde(default)]
    pub metadata_store: Option<metadata_store::MetadataStoreConfig>,
    #[serde(default)]
    pub auth: authenticator::AuthConfig,
    #[serde(default)]
    pub repository: HashMap<String, repository::Config>, // hashmap of namespace <-> repository_config
    #[serde(default)]
    pub event_webhook: HashMap<String, EventWebhookConfig>,
    #[serde(default)]
    pub observability: Option<ObservabilityConfig>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
pub enum ServerConfig {
    Tls(tls::Config),
    Insecure(insecure::Config),
}

#[derive(Clone, Debug, Deserialize)]
pub struct GlobalConfig {
    #[serde(default = "GlobalConfig::default_max_concurrent_requests")]
    pub max_concurrent_requests: usize,
    #[serde(default = "GlobalConfig::default_max_concurrent_cache_jobs")]
    pub max_concurrent_cache_jobs: usize,
    #[serde(default = "GlobalConfig::default_update_pull_time")]
    pub update_pull_time: bool,
    #[serde(default = "GlobalConfig::default_enable_redirect")]
    pub enable_redirect: bool,
    #[serde(default)]
    pub access_policy: AccessPolicyConfig,
    #[serde(default)]
    pub retention_policy: RetentionPolicyConfig,
    #[serde(default)]
    pub immutable_tags: bool,
    #[serde(default)]
    pub immutable_tags_exclusions: Vec<String>,
    pub authorization_webhook: Option<String>,
    #[serde(default)]
    pub event_webhooks: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct UiConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "UiConfig::default_name")]
    pub name: String,
}

impl Default for UiConfig {
    fn default() -> Self {
        UiConfig {
            enabled: false,
            name: UiConfig::default_name(),
        }
    }
}

impl UiConfig {
    fn default_name() -> String {
        "Angos".to_string()
    }
}

impl Default for GlobalConfig {
    fn default() -> Self {
        GlobalConfig {
            max_concurrent_requests: GlobalConfig::default_max_concurrent_requests(),
            max_concurrent_cache_jobs: GlobalConfig::default_max_concurrent_cache_jobs(),
            update_pull_time: GlobalConfig::default_update_pull_time(),
            enable_redirect: GlobalConfig::default_enable_redirect(),
            access_policy: AccessPolicyConfig::default(),
            retention_policy: RetentionPolicyConfig::default(),
            immutable_tags: false,
            immutable_tags_exclusions: Vec::new(),
            authorization_webhook: None,
            event_webhooks: Vec::new(),
        }
    }
}

impl GlobalConfig {
    fn default_max_concurrent_requests() -> usize {
        64
    }

    fn default_max_concurrent_cache_jobs() -> usize {
        4
    }

    fn default_update_pull_time() -> bool {
        false
    }

    fn default_enable_redirect() -> bool {
        true
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct ObservabilityConfig {
    #[serde(default)]
    pub tracing: Option<TracingConfig>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct TracingConfig {
    pub endpoint: String,
    pub sampling_rate: f64,
}

impl Configuration {
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let config = match fs::read_to_string(path) {
            Ok(config) => Ok(config),
            Err(err) => Err(Error::NotReadable(format!(
                "Unable to read configuration file: {err}"
            ))),
        }?;

        Self::load_from_str(&config)
    }

    pub fn load_from_str(slice: &str) -> Result<Self, Error> {
        let config: Configuration = toml::from_str(slice).map_err(|e| {
            println!("Configuration file format error:");
            println!("{e}");
            Error::NotReadable(e.to_string())
        })?;

        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), Error> {
        for (name, webhook) in &self.auth.webhook {
            webhook.validate().map_err(|e| {
                let msg = format!("Invalid webhook '{name}': {e}");
                Error::InvalidFormat(msg)
            })?;
        }

        let webhook_names = self.auth.webhook.keys().collect::<HashSet<_>>();

        if let Some(webhook_name) = &self.global.authorization_webhook
            && !webhook_names.contains(&webhook_name)
        {
            let msg = format!("Webhook '{webhook_name}' not found (referenced globally)");
            return Err(Error::InvalidFormat(msg));
        }

        for (repository, config) in &self.repository {
            if let Some(webhook_name) = &config.authorization_webhook
                && !webhook_name.is_empty()
                && !webhook_names.contains(&webhook_name)
            {
                let msg = format!(
                    "Webhook '{webhook_name}' not found (referenced in '{repository}' repository)"
                );
                return Err(Error::InvalidFormat(msg));
            }
        }

        let event_webhook_names = self.event_webhook.keys().collect::<HashSet<_>>();

        for (name, config) in &self.event_webhook {
            config.validate().map_err(|e| {
                let msg = format!("Invalid event webhook '{name}': {e}");
                Error::InvalidFormat(msg)
            })?;
        }

        for name in &self.global.event_webhooks {
            if !event_webhook_names.contains(name) {
                let msg = format!("Event webhook '{name}' not found (referenced globally)");
                return Err(Error::InvalidFormat(msg));
            }
        }

        for (repository, config) in &self.repository {
            for name in &config.event_webhooks {
                if !event_webhook_names.contains(name) {
                    let msg = format!(
                        "Event webhook '{name}' not found (referenced in '{repository}' repository)"
                    );
                    return Err(Error::InvalidFormat(msg));
                }
            }
        }

        Ok(())
    }

    pub fn resolve_metadata_config(&self) -> metadata_store::MetadataStoreConfig {
        match &self.metadata_store {
            Some(config) => config.clone(),
            None => match &self.blob_store {
                blob_store::BlobStorageConfig::FS(config) => {
                    metadata_store::MetadataStoreConfig::FS(metadata_store::fs::BackendConfig {
                        root_dir: config.root_dir.clone(),
                        redis: None,
                        sync_to_disk: config.sync_to_disk,
                    })
                }
                blob_store::BlobStorageConfig::S3(config) => {
                    info!("Auto-configuring S3 metadata-store from blob-store");
                    metadata_store::MetadataStoreConfig::S3(metadata_store::s3::BackendConfig {
                        bucket: config.bucket.clone(),
                        region: config.region.clone(),
                        endpoint: config.endpoint.clone(),
                        access_key_id: config.access_key_id.clone(),
                        secret_key: config.secret_key.clone(),
                        key_prefix: config.key_prefix.clone(),
                        redis: None,
                    })
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::command::server::auth::oidc;
    use crate::registry::data_store;

    #[tokio::test]
    async fn test_load_minimal_config() {
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

    #[tokio::test]
    async fn test_storage_field_backward_compatibility() {
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

    #[tokio::test]
    async fn test_tls_config_detection() {
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

    #[tokio::test]
    async fn test_metadata_store_explicit_config_not_overridden() {
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

    #[tokio::test]
    async fn test_auth_section() {
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

    #[tokio::test]
    async fn test_global_config_custom_values() {
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

    #[tokio::test]
    async fn test_ui_enabled_config() {
        let config = r#"
        [server]
        bind_address = "0.0.0.0"

        [ui]
        enabled = true
        "#;

        let config = Configuration::load_from_str(config).unwrap();
        assert!(config.ui.enabled);
    }

    #[tokio::test]
    async fn test_ui_enabled_default_false() {
        let config = r#"
        [server]
        bind_address = "0.0.0.0"
        "#;

        let config = Configuration::load_from_str(config).unwrap();
        assert!(!config.ui.enabled);
        assert_eq!(config.ui.name, "Angos");
    }

    #[tokio::test]
    async fn test_ui_custom_name() {
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

    #[tokio::test]
    async fn test_resolve_metadata_config_from_fs_blob_store() {
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
                assert!(fs_config.redis.is_none());
            }
            metadata_store::MetadataStoreConfig::S3(_) => {
                panic!("Expected FS metadata store config")
            }
        }
    }

    #[tokio::test]
    async fn test_resolve_metadata_config_from_s3_blob_store() {
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
                assert!(s3_config.redis.is_none());
            }
            metadata_store::MetadataStoreConfig::FS(_) => {
                panic!("Expected S3 metadata store config")
            }
        }
    }

    #[tokio::test]
    async fn test_repository_config() {
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

    #[tokio::test]
    async fn test_observability_config() {
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

    #[tokio::test]
    async fn test_cache_config_memory() {
        let config = r#"
        [server]
        bind_address = "0.0.0.0"

        [cache]
        memory = {}
        "#;

        let config = Configuration::load_from_str(config).unwrap();
        assert!(matches!(config.cache, cache::Config::Memory));
    }

    #[tokio::test]
    async fn test_cache_config_redis() {
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

    #[tokio::test]
    async fn test_cache_store_backward_compatibility() {
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

    #[tokio::test]
    async fn test_invalid_toml_format() {
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

    #[tokio::test]
    async fn test_validate_webhook_referenced_globally() {
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

    #[tokio::test]
    async fn test_validate_webhook_missing_global_reference() {
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

    #[tokio::test]
    async fn test_validate_webhook_referenced_in_repository() {
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

    #[tokio::test]
    async fn test_validate_webhook_missing_repository_reference() {
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

    #[tokio::test]
    async fn test_validate_webhook_empty_string_in_repository() {
        let config = r#"
        [server]
        bind_address = "0.0.0.0"

        [repository.myapp]
        authorization_webhook = ""
        "#;

        let result = Configuration::load_from_str(config);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_validate_invalid_webhook_config() {
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

    #[tokio::test]
    async fn test_tls_config_with_client_ca() {
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

    #[tokio::test]
    async fn test_insecure_config_with_custom_port() {
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

    #[tokio::test]
    async fn test_multiple_repositories() {
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

    #[tokio::test]
    async fn test_metadata_store_s3_with_redis() {
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
                assert!(s3_config.redis.is_some());
                assert_eq!(s3_config.redis.unwrap().url, "redis://localhost:6379");
            }
            metadata_store::MetadataStoreConfig::FS(_) => {
                panic!("Expected S3 metadata store config")
            }
        }
    }

    #[tokio::test]
    async fn test_metadata_store_fs_with_redis() {
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
                assert!(fs_config.redis.is_some());
                assert_eq!(fs_config.redis.unwrap().url, "redis://localhost:6379");
            }
            metadata_store::MetadataStoreConfig::S3(_) => {
                panic!("Expected FS metadata store config")
            }
        }
    }

    #[tokio::test]
    async fn test_ipv6_bind_address() {
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

    #[tokio::test]
    async fn test_access_policy_in_global_config() {
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

    #[tokio::test]
    async fn test_retention_policy_in_global_config() {
        let config = r#"
        [server]
        bind_address = "0.0.0.0"

        [global.retention_policy]
        rules = ["age(image) > days(30)"]
        "#;

        let config = Configuration::load_from_str(config).unwrap();
        assert_eq!(config.global.retention_policy.rules.len(), 1);
    }

    #[tokio::test]
    async fn test_resolve_metadata_config_preserves_explicit_s3_config() {
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

    #[tokio::test]
    async fn test_multiple_webhooks() {
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

    #[tokio::test]
    async fn test_validate_multiple_repositories_with_webhooks() {
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

    #[tokio::test]
    async fn test_load_from_file() {
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

    #[tokio::test]
    async fn test_load_from_nonexistent_file() {
        let result = Configuration::load("/nonexistent/path/to/config.toml");
        assert!(result.is_err());

        match result {
            Err(Error::NotReadable(msg)) => {
                assert!(msg.contains("Unable to read configuration file"));
            }
            _ => panic!("Expected NotReadable error"),
        }
    }

    #[tokio::test]
    async fn test_load_from_file_with_invalid_content() {
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

    #[tokio::test]
    async fn test_load_from_file_with_tls_config() {
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

    #[tokio::test]
    async fn test_load_from_file_with_validation_error() {
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

    #[tokio::test]
    async fn test_event_webhook_config_parses() {
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

    #[tokio::test]
    async fn test_event_webhook_backward_compatible() {
        let config = r#"
        [server]
        bind_address = "0.0.0.0"
        "#;

        let config = Configuration::load_from_str(config).unwrap();
        assert!(config.event_webhook.is_empty());
    }

    #[tokio::test]
    async fn test_global_event_webhooks_references() {
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

    #[tokio::test]
    async fn test_repository_event_webhooks_references() {
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

    #[tokio::test]
    async fn test_event_webhook_nonexistent_global_reference_fails() {
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

    #[tokio::test]
    async fn test_event_webhook_nonexistent_repository_reference_fails() {
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

    #[tokio::test]
    async fn test_event_webhook_invalid_config_fails_validation() {
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
}
