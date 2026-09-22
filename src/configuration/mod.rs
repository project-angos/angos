use std::{collections::HashMap, fs, path::Path};

use serde::{Deserialize, de::DeserializeOwned};
use toml::{
    Spanned,
    de::{DeTable, Deserializer as TomlDeserializer},
};

pub mod base64_string;
mod error;
pub mod global;
pub mod listeners;
mod merge;
mod observability;
pub mod regex_pattern;
pub mod registry_storage;
mod server;
pub mod trusted_proxy;
mod ui;
pub mod watcher;

pub use base64_string::Base64String;
pub use error::Error;

pub use global::GlobalConfig;
pub use observability::ObservabilityConfig;
pub use regex_pattern::RegexPattern;
pub use registry_storage::{RegistryStorageConfig, ResolvedStorageConfig};
pub use server::ServerConfig;
pub use trusted_proxy::TrustedProxy;
pub use ui::UiConfig;

#[cfg(test)]
mod tests;

use crate::{
    auth::{authenticator, oidc, webhook},
    event_webhook::config::EventWebhookConfig,
    policy::{CelRule, rules_use_pull_time},
    registry::{blob_store, repository},
};

/// Cross-section validation runs in [`Configuration::from_table`], the one
/// place a file is deserialized, so a loaded `Configuration` is a validated one.
#[derive(Clone, Debug, Deserialize)]
pub struct Configuration {
    pub server: ServerConfig,
    #[serde(default)]
    pub global: GlobalConfig,
    #[serde(default)]
    pub ui: UiConfig,
    #[serde(default)]
    pub cache: angos_cache::Config,
    /// Required: a registry with no configured storage would otherwise default
    /// to the filesystem backend rooted at the process working directory.
    pub blob_store: blob_store::BlobStoreConfig,
    #[serde(default, rename = "metadata_store")]
    pub registry_storage: RegistryStorageConfig,
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
    pub fn resolve_registry_storage(&self) -> ResolvedStorageConfig {
        match &self.registry_storage {
            RegistryStorageConfig::Inherit => {
                ResolvedStorageConfig::from_blob_store(&self.blob_store)
            }
            RegistryStorageConfig::FS(fs) => ResolvedStorageConfig::FS(fs.clone()),
            RegistryStorageConfig::S3(s3) => ResolvedStorageConfig::S3(s3.clone()),
        }
    }

    /// Load and merge configuration files in order, later files winning.
    /// Merging happens before deserialization because a file that overrides
    /// only a few keys is not a `Configuration` on its own.
    pub fn load_all<P: AsRef<Path>>(paths: &[P]) -> Result<Self, Error> {
        let documents = read_documents(paths)?;
        if let [single] = documents.as_slice() {
            return Self::load_from_str(single);
        }
        let table = merged_table(paths, &documents)?;
        // A merged tree spans several documents, so quoting one of them would
        // point at the wrong file. The error then names the key path instead.
        Self::from_table(table, None).map_err(|e| annotate_sources(e, paths))
    }

    /// Deserialize the merged files as `T` alone, with no cross-section
    /// validation: for a subcommand that reads one section on a host where
    /// the rest of a registry's configuration does not apply.
    pub fn load_section<T: DeserializeOwned, P: AsRef<Path>>(paths: &[P]) -> Result<T, Error> {
        let documents = read_documents(paths)?;
        let raw = match documents.as_slice() {
            [single] => Some(single.as_str()),
            _ => None,
        };
        let table = merged_table(paths, &documents)?;
        deserialize_table(table, raw).map_err(|e| match raw {
            Some(_) => e,
            None => annotate_sources(e, paths),
        })
    }

    /// Parse and resolve a TOML configuration string, returning typed errors.
    pub fn load_from_str(slice: &str) -> Result<Self, Error> {
        let table = DeTable::parse(slice).map_err(|e| Error::InvalidFormat(e.to_string()))?;
        Self::from_table(table, Some(slice))
    }

    /// Deserialize an already parsed TOML tree. `raw` is the document the tree
    /// came from and restores the source excerpt in error messages; a tree
    /// merged from several documents has no single source and passes `None`.
    fn from_table(table: Spanned<DeTable<'_>>, raw: Option<&str>) -> Result<Self, Error> {
        deserialize_table::<Self>(table, raw)?.validate()
    }

    fn validate(self) -> Result<Self, Error> {
        if let Some(name) = self
            .event_webhook
            .iter()
            .find_map(|(name, hook)| hook.events.is_empty().then_some(name))
        {
            return Err(Error::InvalidFormat(format!(
                "event_webhook.{name} must have at least one event"
            )));
        }
        validate_ui(&self.ui, &self.auth.oidc)?;
        validate_global(&self.global, &self.auth.webhook, &self.event_webhook)?;
        validate_blob_store(&self.blob_store)?;
        validate_repositories(
            &self.repository,
            &self.auth.webhook,
            &self.event_webhook,
            self.global.scan.is_some(),
        )?;
        validate_image_policies(&self.global, &self.repository)?;
        Ok(self)
    }
}

/// Refuses a scan or index table that decides nothing, and rules reading
/// pull times that are never recorded.
fn validate_image_policies(
    global: &GlobalConfig,
    repositories: &HashMap<String, repository::Config>,
) -> Result<(), Error> {
    let mut tables: Vec<(String, &[CelRule])> = Vec::new();
    if let Some(scan) = &global.scan {
        tables.push(("global.scan".to_string(), &scan.policy.rules));
    }
    if let Some(index) = &global.index {
        if !index.is_set() {
            return Err(Error::InvalidFormat(
                "global.index sets neither default nor rules".to_string(),
            ));
        }
        tables.push(("global.index".to_string(), &index.rules));
    }
    for (name, repo) in repositories {
        if let Some(scan) = &repo.scan {
            if !scan.is_set() {
                return Err(Error::InvalidFormat(format!(
                    "repository.\"{name}\".scan sets neither default nor rules"
                )));
            }
            tables.push((format!("repository.\"{name}\".scan"), &scan.rules));
        }
        if let Some(index) = &repo.index {
            if !index.is_set() {
                return Err(Error::InvalidFormat(format!(
                    "repository.\"{name}\".index sets neither default nor rules"
                )));
            }
            tables.push((format!("repository.\"{name}\".index"), &index.rules));
        }
    }
    for (name, rules) in tables {
        if rules_use_pull_time(rules) && !global.update_pull_time {
            return Err(Error::InvalidFormat(format!(
                "{name}.rules use last_pulled_at or top_pulled but update_pull_time is disabled, \
                 so pull times are never recorded"
            )));
        }
    }
    Ok(())
}

/// Refuses a UI sign-in pointed at an OIDC provider that is not configured,
/// which would otherwise send the browser to an issuer no token is accepted from.
fn validate_ui(ui: &UiConfig, oidc: &HashMap<String, oidc::Config>) -> Result<(), Error> {
    let Some(sign_in) = &ui.oidc else {
        return Ok(());
    };

    if !oidc.contains_key(&sign_in.provider) {
        return Err(Error::InvalidFormat(format!(
            "ui.oidc.provider '{}' has no matching auth.oidc provider",
            sign_in.provider
        )));
    }
    Ok(())
}

/// Reads every configuration file, refusing an empty list.
fn read_documents<P: AsRef<Path>>(paths: &[P]) -> Result<Vec<String>, Error> {
    if paths.is_empty() {
        return Err(Error::NotReadable(
            "No configuration file was provided".to_string(),
        ));
    }
    paths
        .iter()
        .map(|path| {
            let path = path.as_ref();
            fs::read_to_string(path).map_err(|e| {
                let path = path.display();
                Error::NotReadable(format!("Unable to read configuration file {path}: {e}"))
            })
        })
        .collect()
}

/// Parses each document and merges them in order, later ones winning.
fn merged_table<'a, P: AsRef<Path>>(
    paths: &[P],
    documents: &'a [String],
) -> Result<Spanned<DeTable<'a>>, Error> {
    let mut merged: Option<Spanned<DeTable<'a>>> = None;
    for (path, document) in paths.iter().zip(documents) {
        let table = DeTable::parse(document)
            .map_err(|e| Error::InvalidFormat(format!("{}: {e}", path.as_ref().display())))?;
        match &mut merged {
            Some(base) => merge::merge(base.get_mut(), table.into_inner()),
            None => merged = Some(table),
        }
    }
    merged.ok_or_else(|| Error::NotReadable("No configuration file was provided".to_string()))
}

fn deserialize_table<T: DeserializeOwned>(
    table: Spanned<DeTable<'_>>,
    raw: Option<&str>,
) -> Result<T, Error> {
    T::deserialize(TomlDeserializer::from(table)).map_err(|mut e| {
        e.set_input(raw);
        Error::InvalidFormat(e.to_string())
    })
}

/// Name the files a merged configuration was built from, since an error on a
/// merged tree can only report the offending key path.
fn annotate_sources<P: AsRef<Path>>(error: Error, paths: &[P]) -> Error {
    let Error::InvalidFormat(message) = &error else {
        return error;
    };

    let sources = paths
        .iter()
        .map(|path| path.as_ref().display().to_string())
        .collect::<Vec<_>>()
        .join(", ");
    Error::InvalidFormat(format!("{}\nmerged from {sources}", message.trim_end()))
}

/// An FS blob store rooted at the empty path resolves every object relative to
/// the working directory, which in a container is the ephemeral layer rather
/// than the mounted volume. The metadata store inherits this root by default,
/// so one check covers both.
fn validate_blob_store(blob_store: &blob_store::BlobStoreConfig) -> Result<(), Error> {
    let blob_store::BlobStoreConfig::FS(fs) = blob_store else {
        return Ok(());
    };
    if fs.root_dir.as_os_str().is_empty() {
        return Err(Error::InvalidFormat(
            "blob_store.fs.root_dir must not be empty".to_string(),
        ));
    }
    Ok(())
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

    // A zero-capacity read buffer reads nothing, which would end every blob
    // body at once rather than fail.
    if global.blob_stream_frame_size.as_u64() == 0 {
        return Err(Error::InvalidFormat(
            "global.blob_stream_frame_size must be greater than zero".to_string(),
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

fn validate_repositories(
    repositories: &HashMap<String, repository::Config>,
    auth_webhooks: &HashMap<String, webhook::Config>,
    event_webhooks: &HashMap<String, EventWebhookConfig>,
    scan_configured: bool,
) -> Result<(), Error> {
    for (repo_name, repo) in repositories {
        if repo.scan.is_some() && !scan_configured {
            return Err(Error::InvalidFormat(format!(
                "repository '{repo_name}' has a scan table but [global.scan] is not configured"
            )));
        }
        let context = format!("referenced in '{repo_name}' repository");
        validate_auth_webhook_ref(repo.authorization_webhook_ref(), auth_webhooks, &context)?;
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

/// Validates that every name in `refs` exists in `known`; `context` identifies
/// the referencing site in the error.
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
    use std::path::PathBuf;

    use crate::configuration::{Configuration, RegistryStorageConfig, ResolvedStorageConfig};

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
            ResolvedStorageConfig::FS(fs_config) => {
                assert_eq!(fs_config.root_dir, PathBuf::from("/data/blobs"));
                assert!(fs_config.sync_to_disk);
            }
            other @ ResolvedStorageConfig::S3(_) => {
                panic!("expected FS storage config from Inherit, got {other:?}")
            }
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
            ResolvedStorageConfig::FS(fs_config) => {
                assert_eq!(fs_config.root_dir, PathBuf::from("/custom/metadata"));
            }
            other @ ResolvedStorageConfig::S3(_) => {
                panic!("expected explicit FS storage config, got {other:?}")
            }
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
            ResolvedStorageConfig::S3(s3_config) => {
                assert_eq!(s3_config.connection.bucket, "metadata-bucket");
                assert_eq!(s3_config.connection.region, "eu-west-1");
                assert_eq!(
                    s3_config.connection.endpoint,
                    "https://metadata.example.com"
                );
            }
            other @ ResolvedStorageConfig::FS(_) => {
                panic!("expected explicit S3 storage config, got {other:?}")
            }
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
            ResolvedStorageConfig::S3(s3_config) => {
                assert_eq!(s3_config.connection.bucket, "my-bucket");
                assert_eq!(s3_config.connection.region, "us-east-1");
                assert_eq!(s3_config.connection.endpoint, "https://s3.example.com");
                assert_eq!(s3_config.connection.access_key_id.expose(), "key123");
                assert_eq!(s3_config.connection.secret_key.expose(), "secret456");
                assert_eq!(s3_config.connection.key_prefix, "prefix/");
            }
            other @ ResolvedStorageConfig::FS(_) => {
                panic!("expected S3 storage config from Inherit, got {other:?}")
            }
        }
    }
}
