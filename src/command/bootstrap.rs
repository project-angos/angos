use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;

use tracing::info;

use angos_cache::{Cache, Error as CacheError};
use angos_s3_client::Error as S3Error;
use angos_storage::fs::Backend as StorageFsBackend;
use angos_storage::{Error as StorageError, ObjectStore};

use crate::configuration::{Configuration, GlobalConfig, ResolvedStorageConfig};
use crate::event_webhook::Error as EventWebhookError;
use crate::event_webhook::dispatcher::EventDispatcher;
use crate::jobs::store::{Error as JobStoreError, JobStore};
use crate::policy::ImagePolicy;
use crate::registry::blob_store::BlobStore;
use crate::registry::metadata_store::{MetadataStore, PullHistoryConfig, Settings};
use crate::registry::repository_resolver::{OverlapError, RepositoryResolver};
use crate::registry::{
    Error as RegistryError, Registry, RegistryConfig, Repository, prime_pull_through, repository,
};

/// Errors produced by the shared CLI bootstrap helpers.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("storage backend failed: {0}")]
    StorageBackend(String),
    #[error("failed to initialize cache: {0}")]
    Cache(#[from] CacheError),
    #[error("failed to initialize repository '{name}': {source}")]
    Repository {
        name: String,
        source: Box<RegistryError>,
    },
    #[error("repository configuration is invalid: {0}")]
    Overlap(#[from] OverlapError),
    #[error("failed to initialize job queue: {0}")]
    JobQueue(#[from] JobStoreError),
    #[error("failed to initialize event webhooks: {0}")]
    EventWebhook(#[from] EventWebhookError),
    #[error("failed to initialize registry: {0}")]
    Registry(#[from] RegistryError),
}

impl From<StorageError> for Error {
    fn from(e: StorageError) -> Self {
        Error::StorageBackend(e.to_string())
    }
}

impl From<S3Error> for Error {
    fn from(e: S3Error) -> Self {
        Error::StorageBackend(e.to_string())
    }
}

/// Build the object store shared by the metadata store and the job store.
pub fn build_object_store(config: &ResolvedStorageConfig) -> Result<Arc<dyn ObjectStore>, Error> {
    let object: Arc<dyn ObjectStore> = match config {
        ResolvedStorageConfig::FS(config) => {
            info!("Using filesystem storage backend");
            Arc::new(
                StorageFsBackend::builder(&config.root_dir)
                    .sync_to_disk(config.sync_to_disk)
                    .build(),
            )
        }
        ResolvedStorageConfig::S3(config) => {
            info!("Using S3 storage backend");
            Arc::new(config.build()?)
        }
    };

    Ok(object)
}

pub fn metadata_store(
    config: &ResolvedStorageConfig,
    namespace_walk_concurrency: NonZeroUsize,
    gc_grace_secs: u64,
    pull_history: PullHistoryConfig,
) -> Result<Arc<MetadataStore>, Error> {
    let store = build_object_store(config)?;

    Ok(Arc::new(MetadataStore::new(
        store,
        Settings {
            namespace_walk_concurrency,
            gc_grace_secs,
            pull_history,
            // No configuration knob: the linger is bounded by the writer
            // backoff the store itself defines.
            ..Settings::default()
        },
    )))
}

/// The storage and repository handles every maintenance command boots with.
pub struct MaintenanceContext {
    pub blob_store: Arc<BlobStore>,
    pub metadata_store: Arc<MetadataStore>,
    pub repositories: Arc<RepositoryResolver>,
}

/// Build the auth cache, blob backend, metadata store and repositories a
/// maintenance command shares.
pub async fn maintenance_context(config: &Configuration) -> Result<MaintenanceContext, Error> {
    let auth_cache = config.cache.to_backend()?;
    let blob_store = Arc::new(
        config
            .blob_store
            .build_backend()?
            .with_namespace_walk_concurrency(config.global.namespace_walk_concurrency),
    );
    let metadata_store = metadata_store(
        &config.resolve_registry_storage(),
        config.global.namespace_walk_concurrency,
        config.global.gc_grace_secs,
        config.global.pull_history,
    )?;
    let repositories = repositories(&config.repository, &auth_cache, &config.global).await?;
    Ok(MaintenanceContext {
        blob_store,
        metadata_store,
        repositories,
    })
}

/// Registry over the shared stores, with webhooks from configuration and a
/// caller-held job queue so no in-process drain loop is spawned.
pub fn registry(
    config: &Configuration,
    blob_store: Arc<BlobStore>,
    metadata_store: Arc<MetadataStore>,
    resolver: Arc<RepositoryResolver>,
    job_store: Arc<JobStore>,
) -> Result<Arc<Registry>, Error> {
    let dispatcher = EventDispatcher::from_config(config)?;
    let registry = Registry::new(
        blob_store,
        metadata_store,
        resolver,
        RegistryConfig {
            event_dispatcher: dispatcher,
            ..RegistryConfig::new(job_store)
        },
    );
    Ok(registry)
}

/// `global` supplies the manifest size bound and the scan and index policies
/// a repository without tables of its own follows.
pub async fn repositories(
    configs: &HashMap<String, repository::Config>,
    auth_cache: &Arc<Cache>,
    global: &GlobalConfig,
) -> Result<Arc<RepositoryResolver>, Error> {
    let max_manifest_size_bytes = global.max_manifest_size_bytes();
    let global_scan = global
        .scan
        .as_ref()
        .filter(|scan| scan.policy.is_set())
        .map(|scan| &scan.policy);
    let mut map = HashMap::with_capacity(configs.len());
    for (name, config) in configs {
        let mut repository = Repository::new(name, config, auth_cache, max_manifest_size_bytes)
            .await
            .map_err(|source| Error::Repository {
                name: name.clone(),
                source: Box::new(source),
            })?;
        if repository.scan.is_none() {
            repository.scan = global_scan.map(ImagePolicy::new);
        }
        if repository.index.is_none() {
            repository.index = global.index.as_ref().map(ImagePolicy::new);
        }
        map.insert(name.clone(), repository);
    }
    for repository in map.values() {
        if repository.is_pull_through() {
            prime_pull_through(repository.name.as_ref());
        }
    }
    let resolver = RepositoryResolver::new(Arc::new(map))?;
    Ok(Arc::new(resolver))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use wiremock::MockServer;

    use angos_cache::Config as CacheConfig;
    use angos_oci::Namespace;

    use crate::command::bootstrap::{Error, repositories};
    use crate::configuration::GlobalConfig;
    use crate::layer::IndexAction;
    use crate::metrics_provider::metrics_provider;
    use crate::policy::{AccessMode, PolicyConfig};
    use crate::registry::repository;
    use crate::registry::repository_resolver::RepositoryResolver;
    use crate::test_fixtures::client::test_client_config;

    #[tokio::test]
    async fn repositories_prime_pull_through_outcomes_at_zero() {
        let server = MockServer::start().await;
        let configs = HashMap::from([(
            "primed-repo".to_string(),
            repository::Config {
                upstream: vec![test_client_config(server.uri())],
                ..repository::Config::default()
            },
        )]);
        let cache = CacheConfig::Memory.to_backend().unwrap();
        repositories(&configs, &cache, &GlobalConfig::default())
            .await
            .unwrap();

        let payload = metrics_provider().gather().unwrap();
        let text = String::from_utf8(payload).unwrap();
        for (kind, outcome) in [
            ("manifest", "hit"),
            ("manifest", "miss"),
            ("manifest", "refresh"),
            ("manifest", "stale"),
            ("blob", "hit"),
            ("blob", "miss"),
        ] {
            let line = format!(
                "angos_pull_through_total{{kind=\"{kind}\",outcome=\"{outcome}\",repository=\"primed-repo\"}} 0"
            );
            assert!(
                text.contains(&line),
                "a repository with an upstream must publish {kind} {outcome} at zero, output:\n{text}"
            );
        }
    }

    /// `[global.index]` is the policy of every repository without an `index`
    /// table of its own, which keeps its own.
    #[tokio::test]
    async fn the_global_index_policy_covers_repositories_without_their_own() {
        let policy = |default| PolicyConfig {
            default: Some(default),
            rules: Vec::new(),
        };
        let configs = HashMap::from([
            ("plain".to_string(), repository::Config::default()),
            (
                "own".to_string(),
                repository::Config {
                    index: Some(policy(IndexAction::Skip)),
                    ..repository::Config::default()
                },
            ),
        ]);
        let cache = CacheConfig::Memory.to_backend().unwrap();
        let global = GlobalConfig {
            index: Some(policy(IndexAction::Index)),
            ..GlobalConfig::default()
        };
        let indexes = |resolver: &RepositoryResolver, name: &str| {
            resolver
                .get(name)
                .and_then(|r| r.index.as_ref())
                .is_some_and(|policy| policy.applies_at_push(&Namespace::new(name).unwrap(), &[]))
        };
        let resolver = repositories(&configs, &cache, &global).await.unwrap();
        assert!(indexes(&resolver, "plain"), "the global policy applies");
        assert!(!indexes(&resolver, "own"), "a repository's own policy wins");

        let resolver = repositories(&configs, &cache, &GlobalConfig::default())
            .await
            .unwrap();
        assert!(resolver.get("plain").is_some_and(|r| r.index.is_none()));
    }

    #[tokio::test]
    async fn repositories_overlapping_prefixes_fails() {
        let mut configs = HashMap::new();
        configs.insert(
            "team".to_string(),
            repository::Config {
                access_policy: Some(PolicyConfig {
                    default: Some(AccessMode::Allow),
                    ..PolicyConfig::default()
                }),
                ..repository::Config::default()
            },
        );
        configs.insert(
            "team/app".to_string(),
            repository::Config {
                access_policy: Some(PolicyConfig {
                    default: Some(AccessMode::Allow),
                    ..PolicyConfig::default()
                }),
                ..repository::Config::default()
            },
        );
        let cache = CacheConfig::Memory.to_backend().unwrap();
        let result = repositories(&configs, &cache, &GlobalConfig::default()).await;
        assert!(matches!(result, Err(Error::Overlap(_))));
    }
}
