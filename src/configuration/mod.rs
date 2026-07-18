use std::{collections::HashMap, fs, path::Path};

use serde::{Deserialize, Deserializer, de::Error as DeError};

use angos_tx_engine::lock::LockStrategy;

mod error;
pub mod global;
pub mod listeners;
mod observability;
pub mod regex_pattern;
pub mod registry_storage;
mod server;
pub mod trusted_proxy;
mod ui;
pub mod watcher;

pub use error::Error;

/// Deserialize a positive integer into a `NonZero` type, naming `field` in the
/// rejection. The single home of the "must be > 0" config validation.
pub fn deserialize_positive_nonzero<'de, D, P, N>(
    deserializer: D,
    field: &str,
) -> Result<N, D::Error>
where
    D: Deserializer<'de>,
    P: Deserialize<'de>,
    N: TryFrom<P>,
{
    let value = P::deserialize(deserializer)?;
    N::try_from(value).map_err(|_| D::Error::custom(format!("{field} must be > 0")))
}
pub use global::GlobalConfig;
pub use observability::ObservabilityConfig;
pub use regex_pattern::RegexPattern;
pub use registry_storage::RegistryStorageConfig;
pub use server::ServerConfig;
pub use trusted_proxy::TrustedProxy;
pub use ui::UiConfig;

#[cfg(test)]
mod tests;

use crate::{
    auth::{authenticator, webhook},
    cache,
    event_webhook::config::EventWebhookConfig,
    registry::{blob_store, repository},
};

/// Cross-section validation runs in the `TryFrom` conversion, so a parsed
/// `Configuration` is always a validated one.
#[derive(Clone, Debug, Deserialize)]
#[serde(try_from = "ConfigurationFields")]
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

#[derive(Deserialize)]
struct ConfigurationFields {
    server: ServerConfig,
    #[serde(default)]
    global: GlobalConfig,
    #[serde(default)]
    ui: UiConfig,
    #[serde(default)]
    cache: cache::Config,
    #[serde(default)]
    blob_store: blob_store::BlobStoreConfig,
    #[serde(default, rename = "metadata_store")]
    registry_storage: RegistryStorageConfig,
    #[serde(default)]
    auth: authenticator::AuthConfig,
    #[serde(default)]
    repository: HashMap<String, repository::Config>,
    #[serde(default)]
    event_webhook: HashMap<String, EventWebhookConfig>,
    #[serde(default)]
    observability: Option<ObservabilityConfig>,
}

impl TryFrom<ConfigurationFields> for Configuration {
    type Error = Error;

    fn try_from(fields: ConfigurationFields) -> Result<Self, Error> {
        Configuration {
            server: fields.server,
            global: fields.global,
            ui: fields.ui,
            cache: fields.cache,
            blob_store: fields.blob_store,
            registry_storage: fields.registry_storage,
            auth: fields.auth,
            repository: fields.repository,
            event_webhook: fields.event_webhook,
            observability: fields.observability,
        }
        .validate()
    }
}

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

    if global.max_blob_size.as_u64() == 0 {
        return Err(Error::InvalidFormat(
            "global.max_blob_size must be greater than zero".to_string(),
        ));
    }

    validate_auth_webhook_ref(
        global.authorization_webhook.as_deref(),
        auth_webhooks,
        "referenced globally",
    )?;

    validate_event_webhook_refs(
        &global.event_webhooks,
        event_webhooks,
        "referenced globally",
    )
}

/// Rejects `[global.job_queue]` combined with the in-process `memory` lock
/// strategy: the durable queue coordinates multiple processes through the
/// per-job execution lock, and a non-shared lock would let each worker claim
/// and run the same job. The in-process queue, used when `[global.job_queue]`
/// is absent, is unaffected.
fn validate_durable_queue_lock(config: &Configuration) -> Result<(), Error> {
    if config.global.job_queue.is_none() {
        return Ok(());
    }
    let memory_locked = match config.resolve_registry_storage() {
        RegistryStorageConfig::FS(fs) => matches!(fs.lock_strategy, LockStrategy::Memory),
        // An unset S3 lock strategy resolves against the provider's
        // conditional-write support at startup, so only a declared
        // `conditional_operations = false` makes the memory fallback certain
        // here; the probed no-CAS case is rejected at startup instead.
        RegistryStorageConfig::S3(s3) => matches!(
            s3.resolved_lock_strategy(s3.conditional_operations.unwrap_or(true)),
            LockStrategy::Memory
        ),
        // `resolve_registry_storage` must map `Inherit` to a concrete backend;
        // fail closed rather than silently skip the lock check.
        RegistryStorageConfig::Inherit => {
            return Err(Error::InvalidFormat(
                "[metadata_store] did not resolve to a concrete backend before \
                 durable-queue lock validation"
                    .to_string(),
            ));
        }
    };
    if memory_locked {
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
        let context = format!("referenced in '{repo_name}' repository");
        validate_auth_webhook_ref(
            repo.authorization_webhook
                .as_deref()
                .filter(|n| !n.is_empty()),
            auth_webhooks,
            &context,
        )?;
        validate_event_webhook_refs(&repo.event_webhooks, event_webhooks, &context)?;
    }
    Ok(())
}

/// Validates that an optional authorization-webhook reference names a
/// configured webhook; `context` identifies the referencing site in the error.
fn validate_auth_webhook_ref(
    name: Option<&str>,
    known: &HashMap<String, webhook::Config>,
    context: &str,
) -> Result<(), Error> {
    if let Some(name) = name
        && !known.contains_key(name)
    {
        return Err(Error::InvalidFormat(format!(
            "Webhook '{name}' not found ({context})"
        )));
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

#[cfg(test)]
mod metadata_resolver_tests {
    use angos_tx_engine::lock::LockStrategy;

    use crate::configuration::{Configuration, RegistryStorageConfig};

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
