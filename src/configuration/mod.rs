use std::{collections::HashMap, fs, path::Path};

use serde::Deserialize;
use tracing::warn;

mod error;
mod global;
pub mod listeners;
mod metadata_resolver;
mod observability;
mod server;
mod ui;
mod validate;
pub mod watcher;

pub use error::Error;
pub use global::GlobalConfig;
pub use observability::ObservabilityConfig;
pub use server::ServerConfig;
pub use ui::UiConfig;

#[cfg(test)]
mod tests;

use crate::{
    auth::authenticator,
    cache,
    event_webhook::config::EventWebhookConfig,
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
    pub repository: HashMap<String, repository::Config>,
    #[serde(default)]
    pub event_webhook: HashMap<String, EventWebhookConfig>,
    #[serde(default)]
    pub observability: Option<ObservabilityConfig>,
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
}
