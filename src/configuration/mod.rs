use std::{collections::HashMap, fs, path::Path, sync::Arc};

use serde::{Deserialize, Deserializer};
use tracing::warn;

mod error;
mod global;
pub mod listeners;
mod metadata_resolver;
mod observability;
pub mod regex_pattern;
mod server;
mod ui;
pub mod watcher;

pub use error::Error;
pub use global::GlobalConfig;
pub use observability::ObservabilityConfig;
pub use regex_pattern::RegexPattern;
pub use server::ServerConfig;
pub use ui::UiConfig;

#[cfg(test)]
mod tests;

use global::RawGlobalConfig;
use repository::RawConfig as RawRepositoryConfig;

use crate::{
    auth::{authenticator, webhook},
    cache,
    event_webhook::config::EventWebhookConfig,
    registry::{blob_store, metadata_store, repository},
};

/// Parsed shape of the top-level configuration before webhook name references are resolved.
#[derive(Deserialize)]
struct RawConfiguration {
    server: ServerConfig,
    #[serde(default)]
    global: RawGlobalConfig,
    #[serde(default)]
    ui: UiConfig,
    #[serde(default, alias = "cache_store")]
    cache: cache::Config,
    #[serde(default, alias = "storage")]
    blob_store: blob_store::BlobStorageConfig,
    #[serde(default)]
    metadata_store: Option<metadata_store::MetadataStoreConfig>,
    #[serde(default)]
    auth: RawAuthConfig,
    #[serde(default)]
    repository: HashMap<String, RawRepositoryConfig>,
    #[serde(default)]
    event_webhook: HashMap<String, EventWebhookConfig>,
    #[serde(default)]
    observability: Option<ObservabilityConfig>,
}

/// Raw auth config that still holds `webhook::Config` by value (pre-resolution).
#[derive(Clone, Debug, Default, Deserialize)]
struct RawAuthConfig {
    #[serde(default)]
    identity: HashMap<String, crate::auth::basic_auth::Config>,
    #[serde(default)]
    oidc: HashMap<String, crate::auth::oidc::Config>,
    #[serde(default)]
    webhook: HashMap<String, webhook::Config>,
}

#[derive(Clone, Debug)]
pub struct Configuration {
    pub server: ServerConfig,
    pub global: GlobalConfig,
    pub ui: UiConfig,
    pub cache: cache::Config,
    pub blob_store: blob_store::BlobStorageConfig,
    pub metadata_store: Option<metadata_store::MetadataStoreConfig>,
    pub auth: authenticator::AuthConfig,
    pub repository: HashMap<String, repository::Config>,
    pub event_webhook: HashMap<String, EventWebhookConfig>,
    pub observability: Option<ObservabilityConfig>,
}

/// Serde `Deserialize` impl routes through `RawConfiguration` and runs resolution.
/// This allows `toml::from_str::<Configuration>` to work transparently.
impl<'de> Deserialize<'de> for Configuration {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = RawConfiguration::deserialize(deserializer)?;
        Self::resolve(raw).map_err(serde::de::Error::custom)
    }
}

impl Configuration {
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let config = fs::read_to_string(path)
            .map_err(|e| Error::NotReadable(format!("Unable to read configuration file: {e}")))?;
        Self::load_from_str(&config)
    }

    /// Parse and resolve a TOML configuration string, returning typed errors.
    pub fn load_from_str(slice: &str) -> Result<Self, Error> {
        let raw: RawConfiguration =
            toml::from_str(slice).map_err(|e| Error::InvalidFormat(e.to_string()))?;
        Self::resolve(raw)
    }

    fn resolve(raw: RawConfiguration) -> Result<Self, Error> {
        let auth_webhooks = resolve_auth_webhooks(raw.auth.webhook)?;
        let event_webhooks = resolve_event_webhooks(raw.event_webhook)?;
        let global = resolve_global(raw.global, &auth_webhooks)?;
        let repositories = resolve_repositories(raw.repository, &auth_webhooks)?;
        let auth = authenticator::AuthConfig {
            identity: raw.auth.identity,
            oidc: raw.auth.oidc,
            webhook: auth_webhooks,
        };
        Ok(Configuration {
            server: raw.server,
            global,
            ui: raw.ui,
            cache: raw.cache,
            blob_store: raw.blob_store,
            metadata_store: raw.metadata_store,
            auth,
            repository: repositories,
            event_webhook: event_webhooks,
            observability: raw.observability,
        })
    }

    pub fn log_deprecations(&self) {
        if self.global.enable_redirect.is_some() {
            warn!(
                "'global.enable_redirect' is deprecated; use \
                 'global.enable_blob_redirect' and 'global.enable_manifest_redirect' instead"
            );
        }
    }
}

fn resolve_auth_webhooks(
    raw: HashMap<String, webhook::Config>,
) -> Result<HashMap<String, Arc<webhook::Config>>, Error> {
    raw.into_iter()
        .map(|(name, mut config)| {
            config
                .validate()
                .map_err(|e| Error::InvalidFormat(format!("Invalid webhook '{name}': {e}")))?;
            config.name.clone_from(&name);
            Ok((name, Arc::new(config)))
        })
        .collect()
}

fn resolve_event_webhooks(
    raw: HashMap<String, EventWebhookConfig>,
) -> Result<HashMap<String, EventWebhookConfig>, Error> {
    raw.into_iter()
        .map(|(name, mut config)| {
            config.validate().map_err(|e| {
                Error::InvalidFormat(format!("Invalid event webhook '{name}': {e}"))
            })?;
            config.name.clone_from(&name);
            Ok((name, config))
        })
        .collect()
}

fn resolve_global(
    raw: RawGlobalConfig,
    auth_webhooks: &HashMap<String, Arc<webhook::Config>>,
) -> Result<GlobalConfig, Error> {
    let authorization_webhook = raw
        .authorization_webhook
        .map(|name| {
            auth_webhooks.get(&name).cloned().ok_or_else(|| {
                Error::InvalidFormat(format!("Webhook '{name}' not found (referenced globally)"))
            })
        })
        .transpose()?;

    Ok(GlobalConfig {
        max_concurrent_requests: raw.max_concurrent_requests,
        max_concurrent_cache_jobs: raw.max_concurrent_cache_jobs,
        update_pull_time: raw.update_pull_time,
        enable_redirect: raw.enable_redirect,
        enable_blob_redirect: raw.enable_blob_redirect,
        enable_manifest_redirect: raw.enable_manifest_redirect,
        access_policy: raw.access_policy,
        retention_policy: raw.retention_policy,
        immutable_tags: raw.immutable_tags,
        immutable_tags_exclusions: raw.immutable_tags_exclusions,
        authorization_webhook,
    })
}

fn resolve_repositories(
    raw: HashMap<String, RawRepositoryConfig>,
    auth_webhooks: &HashMap<String, Arc<webhook::Config>>,
) -> Result<HashMap<String, repository::Config>, Error> {
    raw.into_iter()
        .map(|(repo_name, raw_repo)| {
            let authorization_webhook = raw_repo
                .authorization_webhook
                .filter(|name| !name.is_empty())
                .map(|name| {
                    auth_webhooks.get(&name).cloned().ok_or_else(|| {
                        Error::InvalidFormat(format!(
                            "Webhook '{name}' not found (referenced in '{repo_name}' repository)"
                        ))
                    })
                })
                .transpose()?;
            let config = repository::Config {
                upstream: raw_repo.upstream,
                access_policy: raw_repo.access_policy,
                retention_policy: raw_repo.retention_policy,
                immutable_tags: raw_repo.immutable_tags,
                immutable_tags_exclusions: raw_repo.immutable_tags_exclusions,
                authorization_webhook,
            };
            Ok((repo_name, config))
        })
        .collect()
}
