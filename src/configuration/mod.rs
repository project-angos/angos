use std::{
    collections::{HashMap, HashSet},
    fs,
    path::Path,
};

use serde::Deserialize;
use tracing::{info, warn};

mod error;
pub mod watcher;

pub use error::Error;

#[cfg(test)]
mod tests;

use crate::{
    cache,
    command::server::{
        auth::authenticator,
        listeners::{insecure, tls},
    },
    event_webhook::config::EventWebhookConfig,
    policy::{AccessPolicyConfig, RetentionPolicyConfig},
    registry::{blob_store, metadata_store, repository},
};

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
    #[serde(default)]
    pub enable_redirect: Option<bool>,
    #[serde(default)]
    pub enable_blob_redirect: Option<bool>,
    #[serde(default)]
    pub enable_manifest_redirect: Option<bool>,
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
            enable_redirect: None,
            enable_blob_redirect: None,
            enable_manifest_redirect: None,
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

    pub fn resolved_enable_blob_redirect(&self) -> bool {
        self.enable_blob_redirect
            .or(self.enable_redirect)
            .unwrap_or(true)
    }

    pub fn resolved_enable_manifest_redirect(&self) -> bool {
        self.enable_manifest_redirect
            .or(self.enable_redirect)
            .unwrap_or(true)
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
        let config = fs::read_to_string(path)
            .map_err(|e| Error::NotReadable(format!("Unable to read configuration file: {e}")))?;
        Self::load_from_str(&config)
    }

    pub fn load_from_str(slice: &str) -> Result<Self, Error> {
        let config: Configuration =
            toml::from_str(slice).map_err(|e| Error::NotReadable(e.to_string()))?;

        config.validate()?;
        Ok(config)
    }

    pub fn log_deprecations(&self) {
        if self.global.enable_redirect.is_some() {
            warn!(
                "'global.enable_redirect' is deprecated; use \
                 'global.enable_blob_redirect' and 'global.enable_manifest_redirect' instead"
            );
        }
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
                        sync_to_disk: config.sync_to_disk,
                        ..Default::default()
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
                        ..Default::default()
                    })
                }
            },
        }
    }
}
