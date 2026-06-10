use std::{collections::HashMap, fmt, fs, path::Path};

use serde::{
    Deserialize, Deserializer,
    de::{self, MapAccess, Visitor},
};
use tracing::warn;

use angos_tx_engine::lock::LockStrategy;

mod error;
pub mod global;
pub mod listeners;
mod metadata_resolver;
mod observability;
pub mod regex_pattern;
pub mod registry_storage;
mod server;
mod ui;
pub mod watcher;

pub use error::Error;
pub use global::GlobalConfig;
pub use observability::ObservabilityConfig;
pub use regex_pattern::RegexPattern;
pub use registry_storage::RegistryStorageConfig;
pub use server::ServerConfig;
pub use ui::UiConfig;

#[cfg(test)]
mod tests;

use crate::{
    auth::{authenticator, webhook},
    cache,
    event_webhook::config::EventWebhookConfig,
    registry::{blob_store, repository},
};

#[derive(Clone, Debug)]
pub struct Configuration {
    pub server: ServerConfig,
    pub global: GlobalConfig,
    pub ui: UiConfig,
    pub cache: cache::Config,
    pub blob_store: blob_store::BlobStoreConfig,
    pub registry_storage: RegistryStorageConfig,
    pub auth: authenticator::AuthConfig,
    pub repository: HashMap<String, repository::Config>,
    pub event_webhook: HashMap<String, EventWebhookConfig>,
    pub observability: Option<ObservabilityConfig>,
}

impl<'de> Deserialize<'de> for Configuration {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ConfigurationVisitor;

        impl<'de> Visitor<'de> for ConfigurationVisitor {
            type Value = Configuration;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("Angos configuration")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut server = None;
                let mut global = None;
                let mut ui = None;
                let mut cache = None;
                let mut blob_store = None;
                let mut registry_storage = None;
                let mut auth = None;
                let mut repository = None;
                let mut event_webhook = None;
                let mut observability = None;

                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "server" => assign_once(&mut server, "server", map.next_value()?)?,
                        "global" => assign_once(&mut global, "global", map.next_value()?)?,
                        "ui" => assign_once(&mut ui, "ui", map.next_value()?)?,
                        "cache" | "cache_store" => {
                            assign_once(&mut cache, "cache", map.next_value()?)?;
                        }
                        "blob_store" | "storage" => {
                            assign_once(&mut blob_store, "blob_store", map.next_value()?)?;
                        }
                        "metadata_store" => {
                            assign_once(
                                &mut registry_storage,
                                "metadata_store",
                                map.next_value()?,
                            )?;
                        }
                        "auth" => assign_once(&mut auth, "auth", map.next_value()?)?,
                        "repository" => {
                            assign_once(&mut repository, "repository", map.next_value()?)?;
                        }
                        "event_webhook" => {
                            assign_once(&mut event_webhook, "event_webhook", map.next_value()?)?;
                        }
                        "observability" => {
                            assign_once(&mut observability, "observability", map.next_value()?)?;
                        }
                        _ => {
                            let _ = map.next_value::<de::IgnoredAny>()?;
                        }
                    }
                }

                Configuration {
                    server: server.ok_or_else(|| de::Error::missing_field("server"))?,
                    global: global.unwrap_or_default(),
                    ui: ui.unwrap_or_default(),
                    cache: cache.unwrap_or_default(),
                    blob_store: blob_store.unwrap_or_default(),
                    registry_storage: registry_storage.unwrap_or_default(),
                    auth: auth.unwrap_or_default(),
                    repository: repository.unwrap_or_default(),
                    event_webhook: event_webhook.unwrap_or_default(),
                    observability: observability.unwrap_or_default(),
                }
                .validate()
                .map_err(de::Error::custom)
            }
        }

        deserializer.deserialize_map(ConfigurationVisitor)
    }
}

fn assign_once<T, E>(slot: &mut Option<T>, field: &'static str, value: T) -> Result<(), E>
where
    E: de::Error,
{
    if slot.replace(value).is_some() {
        return Err(de::Error::duplicate_field(field));
    }
    Ok(())
}

impl Configuration {
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let config = fs::read_to_string(path)
            .map_err(|e| Error::NotReadable(format!("Unable to read configuration file: {e}")))?;
        Self::load_from_str(&config)
    }

    /// Parse and resolve a TOML configuration string, returning typed errors.
    pub fn load_from_str(slice: &str) -> Result<Self, Error> {
        toml::from_str(slice).map_err(|e| Error::InvalidFormat(e.to_string()))
    }

    fn validate(self) -> Result<Self, Error> {
        validate_global(&self.global, &self.auth.webhook, &self.event_webhook)?;
        validate_repositories(&self.repository, &self.auth.webhook, &self.event_webhook)?;
        validate_durable_queue_lock(&self)?;
        Ok(self)
    }

    pub fn log_deprecations(&self) {
        for field in self.deprecated_fields() {
            warn!(
                "'{field}' is deprecated; use \
                 'global.enable_blob_redirect' and 'global.enable_manifest_redirect' instead"
            );
        }
    }

    /// Returns the names of deprecated configuration fields that are set.
    ///
    /// Exposed for testing without requiring log capture infrastructure.
    pub fn deprecated_fields(&self) -> Vec<&'static str> {
        let mut fields = Vec::new();
        if self.global.enable_redirect.is_some() {
            fields.push("global.enable_redirect");
        }
        fields
    }
}

fn validate_global(
    global: &GlobalConfig,
    auth_webhooks: &HashMap<String, webhook::Config>,
    event_webhooks: &HashMap<String, EventWebhookConfig>,
) -> Result<(), Error> {
    if global.max_manifest_size.as_u64() == 0 {
        return Err(Error::InvalidFormat(
            "global.max_manifest_size must be greater than zero".to_string(),
        ));
    }

    global
        .authorization_webhook
        .as_ref()
        .map(|name| {
            auth_webhooks.get(name).ok_or_else(|| {
                Error::InvalidFormat(format!("Webhook '{name}' not found (referenced globally)"))
            })
        })
        .transpose()?;

    validate_event_webhook_refs(
        &global.event_webhooks,
        event_webhooks,
        "referenced globally",
    )
}

/// A durable job queue (`[global.job_queue]`) exists to coordinate work across
/// multiple processes: the server enqueues and one or more `angos worker`
/// processes drain. That coordination relies on the per-job execution lock being
/// shared across processes, but the in-process `memory` lock strategy cannot be,
/// so multiple workers (or `scrub --replicate` running alongside a worker) would
/// each claim and run the same job. Require a shared lock backend whenever the
/// durable queue is configured (the in-process queue, used when `[global.job_queue]`
/// is absent, runs in a single process and is unaffected).
fn validate_durable_queue_lock(config: &Configuration) -> Result<(), Error> {
    if config.global.job_queue.is_none() {
        return Ok(());
    }
    let lock_strategy = match config.resolve_registry_storage() {
        RegistryStorageConfig::FS(fs) => fs.lock_strategy,
        RegistryStorageConfig::S3(s3) => s3.lock_strategy,
        // Reaching this arm means the `resolve_registry_storage` invariant
        // regressed (it must map `Inherit` to a concrete FS/S3 backend). Fail
        // closed rather than silently skip the durable-queue lock check, which
        // would let a multi-worker deployment run with a non-shared lock.
        RegistryStorageConfig::Inherit => {
            return Err(Error::InvalidFormat(
                "[metadata_store] did not resolve to a concrete backend before \
                 durable-queue lock validation"
                    .to_string(),
            ));
        }
    };
    if matches!(lock_strategy, LockStrategy::Memory) {
        return Err(Error::InvalidFormat(
            "[global.job_queue] needs a shared lock strategy so workers serialize on the \
             same jobs across processes; the in-process 'memory' lock cannot coordinate \
             across processes. Set the metadata store's lock_strategy to \"s3\" or \"redis\", \
             or remove [global.job_queue] to use the in-process queue."
                .to_string(),
        ));
    }
    Ok(())
}

fn validate_repositories(
    repositories: &HashMap<String, repository::Config>,
    auth_webhooks: &HashMap<String, webhook::Config>,
    event_webhooks: &HashMap<String, EventWebhookConfig>,
) -> Result<(), Error> {
    for (repo_name, repo) in repositories {
        if let Some(name) = repo
            .authorization_webhook
            .as_deref()
            .filter(|n| !n.is_empty())
        {
            auth_webhooks.get(name).map(|_| ()).ok_or_else(|| {
                Error::InvalidFormat(format!(
                    "Webhook '{name}' not found (referenced in '{repo_name}' repository)"
                ))
            })?;
        }
        let context = format!("referenced in '{repo_name}' repository");
        validate_event_webhook_refs(&repo.event_webhooks, event_webhooks, &context)?;
    }
    Ok(())
}

/// Validates that every name in `refs` exists in `known`. The `context` string
/// is appended to the error message in parentheses to identify the caller
/// (e.g. `"referenced globally"`, `"referenced in 'foo' repository"`).
fn validate_event_webhook_refs(
    refs: &[String],
    known: &HashMap<String, EventWebhookConfig>,
    context: &str,
) -> Result<(), Error> {
    for name in refs {
        if !known.contains_key(name) {
            return Err(Error::InvalidFormat(format!(
                "Event webhook '{name}' not found ({context})"
            )));
        }
    }
    Ok(())
}
