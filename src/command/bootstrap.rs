use std::{collections::HashMap, sync::Arc};

use tracing::info;

use angos_s3_client::Backend as S3HttpBackend;
use angos_storage::{
    ConditionalStore, ObjectStore, fs::Backend as StorageFsBackend, s3::Backend as StorageS3Backend,
};
use angos_tx_engine::{
    error::Error as EngineError, lock::LockStrategy, probe::probe_cas_support, store::Store,
};

use crate::{
    cache::{self, Cache},
    configuration::{Configuration, RegistryStorageConfig, registry_storage::MetadataS3Config},
    event_webhook::{self, dispatcher::EventDispatcher},
    jobs::store::{self as job_store, JobStore},
    registry::{
        self, Registry, RegistryConfig, Repository,
        blob_store::BlobStore,
        metadata_store::MetadataStore,
        repository,
        repository_resolver::{OverlapError, RepositoryResolver},
    },
};

/// Errors produced by the shared CLI bootstrap helpers.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("storage backend failed: {0}")]
    StorageBackend(String),
    #[error("storage coordination/configuration error: {0}")]
    Coordination(String),
    #[error("failed to initialize cache: {0}")]
    Cache(#[from] cache::Error),
    #[error("failed to initialize repository '{name}': {source}")]
    Repository {
        name: String,
        source: Box<registry::Error>,
    },
    #[error("repository configuration is invalid: {0}")]
    Overlap(#[from] OverlapError),
    #[error("failed to initialize job queue: {0}")]
    JobQueue(#[from] job_store::Error),
    #[error("failed to initialize event webhooks: {0}")]
    EventWebhook(#[from] event_webhook::Error),
    #[error("failed to initialize registry: {0}")]
    Registry(#[from] registry::Error),
}

impl From<angos_storage::Error> for Error {
    fn from(e: angos_storage::Error) -> Self {
        Error::StorageBackend(e.to_string())
    }
}

impl From<angos_s3_client::Error> for Error {
    fn from(e: angos_s3_client::Error) -> Self {
        Error::StorageBackend(e.to_string())
    }
}

impl From<EngineError> for Error {
    fn from(e: EngineError) -> Self {
        match &e {
            EngineError::Storage(_) => Error::StorageBackend(e.to_string()),
            _ => Error::Coordination(e.to_string()),
        }
    }
}

/// S3 lock strategy requires the full conditional set (If-None-Match and
/// If-Match on PUT, If-Match on DELETE). Returns a Coordination error when
/// the strategy is S3 but the provider lacks it.
fn ensure_s3_cas_supported(lock_strategy: &LockStrategy, cas: bool) -> Result<(), Error> {
    if matches!(lock_strategy, LockStrategy::S3(_)) && !cas {
        return Err(Error::Coordination(
            "S3 lock strategy requires conditional writes (If-None-Match and If-Match on \
             PUT, If-Match on DELETE), but the provider does not support them all. Use \
             lock_strategy = redis or lock_strategy = memory instead."
                .to_string(),
        ));
    }
    Ok(())
}

/// Probe an S3 backend for conditional-write support, rejecting an
/// operator-configured S3 lock when the provider lacks the conditional set.
async fn probe_s3(config: &MetadataS3Config) -> Result<bool, Error> {
    let http = S3HttpBackend::new(&config.connection.to_client_config())?;
    let storage = Arc::new(StorageS3Backend::builder(Arc::new(http)).build());
    let cas = probe_cas_support(storage.as_ref()).await?;
    if let Some(strategy) = &config.lock_strategy {
        ensure_s3_cas_supported(strategy, cas)?;
    }
    Ok(cas)
}

/// Probe the underlying S3 store for conditional-write support.
///
/// Returns `None` for FS configs (nothing to probe). Returns
/// [`Error::Coordination`] when called on the `Inherit` variant: callers
/// must resolve first via `Configuration::resolve_registry_storage`.
pub async fn probe_storage(config: &RegistryStorageConfig) -> Result<Option<bool>, Error> {
    match config {
        RegistryStorageConfig::Inherit => Err(Error::Coordination(
            "RegistryStorageConfig::Inherit reached probe_storage(); callers must \
             resolve via Configuration::resolve_registry_storage first"
                .to_string(),
        )),
        RegistryStorageConfig::S3(config) => Ok(Some(probe_s3(config).await?)),
        RegistryStorageConfig::FS(_) => Ok(None),
    }
}

/// Build the [`Store`] façade shared by the metadata store, the job store,
/// and the engine-maintenance loops.
///
/// For S3 without an operator-declared `conditional_operations` this probes
/// the endpoint to configure the executor. Server callers that want to
/// memoize the probe across hot-reloads should resolve it up front (see
/// `setup::build_metadata_store`) and inject it into the config so this
/// path skips the probe.
pub async fn build_store(config: &RegistryStorageConfig) -> Result<Arc<Store>, Error> {
    let store = match config {
        RegistryStorageConfig::Inherit => {
            return Err(Error::Coordination(
                "RegistryStorageConfig::Inherit reached build_store(); callers must \
                 resolve via Configuration::resolve_registry_storage first"
                    .to_string(),
            ));
        }
        RegistryStorageConfig::FS(config) => {
            if matches!(config.lock_strategy, LockStrategy::S3(_)) {
                return Err(Error::Coordination(
                    "S3 lock strategy is not supported for filesystem storage".to_string(),
                ));
            }
            info!(
                "Using filesystem storage backend with lock_strategy={:?}",
                config.lock_strategy
            );
            let object: Arc<dyn ObjectStore> = Arc::new(
                StorageFsBackend::builder(&config.root_dir)
                    .sync_to_disk(config.sync_to_disk)
                    .build(),
            );
            Store::new(object, None, config.lock_strategy.clone(), None)?
        }
        RegistryStorageConfig::S3(config) => {
            let cas = match config.conditional_operations {
                Some(declared) => declared,
                None => probe_s3(config).await?,
            };
            let lock_strategy = config.resolved_lock_strategy(cas);
            ensure_s3_cas_supported(&lock_strategy, cas)?;
            info!("Using S3 storage backend with lock_strategy={lock_strategy:?}");

            let http = S3HttpBackend::new(&config.connection.to_client_config())?;
            let backend = Arc::new(StorageS3Backend::builder(Arc::new(http)).build());
            let object: Arc<dyn ObjectStore> = backend.clone();
            let conditional_store: Option<Arc<dyn ConditionalStore>> =
                cas.then_some(backend as Arc<dyn ConditionalStore>);

            let s3_lock_store: Option<Arc<dyn ConditionalStore>> = match &lock_strategy {
                LockStrategy::S3(s3_lock_config) => {
                    let lock_http = S3HttpBackend::new(
                        &config.connection.to_lock_client_config(s3_lock_config),
                    )?;
                    let lock_backend = StorageS3Backend::builder(Arc::new(lock_http)).build();
                    Some(Arc::new(lock_backend))
                }
                LockStrategy::Redis(_) | LockStrategy::Memory => None,
            };

            Store::new(object, conditional_store, lock_strategy, s3_lock_store)?
        }
    };

    Ok(Arc::new(store))
}

pub async fn metadata_store(
    config: &RegistryStorageConfig,
    auth_cache: &Arc<Cache>,
) -> Result<Arc<MetadataStore>, Error> {
    let store = build_store(config).await?;

    let s3_ttl = if let RegistryStorageConfig::S3(s3_cfg) = config {
        (s3_cfg.link_cache_ttl, s3_cfg.access_time_debounce_secs)
    } else {
        (0, 0)
    };

    let mut builder = MetadataStore::builder(store)
        .link_cache_ttl(s3_ttl.0)
        .access_time_debounce_secs(s3_ttl.1);

    // Wire in the auth cache for link-metadata caching (only meaningful on S3,
    // where link_cache_ttl > 0 by default).
    builder = builder.cache(auth_cache.clone());

    Ok(Arc::new(builder.build()))
}

/// The storage and repository handles every maintenance command (`prune`,
/// `replicate`, `scrub`, `worker`) boots with.
pub struct MaintenanceContext {
    pub blob_store: Arc<BlobStore>,
    pub metadata_store: Arc<MetadataStore>,
    pub repositories: Arc<RepositoryResolver>,
}

/// Build the shared maintenance-command context: auth cache, blob backend,
/// metadata store, and the resolved repositories, in the one canonical order.
pub async fn maintenance_context(config: &Configuration) -> Result<MaintenanceContext, Error> {
    let auth_cache = auth_cache(&config.cache)?;
    let blob_store = Arc::new(config.blob_store.build_backend()?);
    let metadata_store = metadata_store(&config.resolve_registry_storage(), &auth_cache).await?;
    let repositories = repositories(
        &config.repository,
        &auth_cache,
        config.global.max_manifest_size_bytes(),
    )
    .await?;
    Ok(MaintenanceContext {
        blob_store,
        metadata_store,
        repositories,
    })
}

pub fn auth_cache(config: &cache::Config) -> Result<Arc<Cache>, Error> {
    config.to_backend().map_err(Error::from)
}

/// Registry over the shared stores, with webhooks wired from configuration
/// and a caller-held job queue so no in-process drain loops are spawned.
/// Used by the maintenance and worker commands; the server wires its own
/// queue choice in `server setup`.
pub fn registry(
    config: &Configuration,
    blob_store: Arc<BlobStore>,
    metadata_store: Arc<MetadataStore>,
    resolver: Arc<RepositoryResolver>,
    job_store: Arc<JobStore>,
) -> Result<Arc<Registry>, Error> {
    let dispatcher = EventDispatcher::from_config(&config.event_webhook)?;
    let registry = Registry::new(
        blob_store,
        metadata_store,
        resolver,
        RegistryConfig {
            job_queue: Some(job_store),
            event_dispatcher: dispatcher,
            ..RegistryConfig::default()
        },
    );
    Ok(registry)
}

pub async fn repository(
    name: &str,
    config: &repository::Config,
    auth_cache: &Arc<Cache>,
    max_manifest_size_bytes: usize,
) -> Result<Repository, Error> {
    Repository::new(name, config, auth_cache, max_manifest_size_bytes)
        .await
        .map_err(|source| Error::Repository {
            name: name.to_string(),
            source: Box::new(source),
        })
}

pub async fn repositories(
    configs: &HashMap<String, repository::Config>,
    auth_cache: &Arc<Cache>,
    max_manifest_size_bytes: usize,
) -> Result<Arc<RepositoryResolver>, Error> {
    let mut map = HashMap::with_capacity(configs.len());
    for (name, config) in configs {
        map.insert(
            name.clone(),
            repository(name, config, auth_cache, max_manifest_size_bytes).await?,
        );
    }
    let resolver = RepositoryResolver::new(Arc::new(map))?;
    Ok(Arc::new(resolver))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use angos_tx_engine::lock::{LockStrategy, S3LockConfig};

    use crate::{
        cache,
        command::bootstrap::{self, Error, auth_cache, build_store, probe_storage, repositories},
        command::maintenance::Error as MaintenanceError,
        command::server::Error as ServerError,
        configuration::{
            RegistryStorageConfig,
            registry_storage::{MetadataFsConfig, MetadataS3Config},
        },
        policy::{AccessMode, AccessPolicyConfig},
        registry::{
            self, manifest::DEFAULT_MAX_MANIFEST_SIZE_BYTES, repository,
            test_utils::s3_test_connection,
        },
    };

    #[test]
    fn auth_cache_memory_succeeds() {
        let config = cache::Config::Memory;
        let result = auth_cache(&config);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn repository_with_default_config_succeeds() {
        let repo_config = repository::Config {
            access_policy: Some(AccessPolicyConfig {
                default: AccessMode::Allow,
                ..AccessPolicyConfig::default()
            }),
            ..repository::Config::default()
        };
        let cache = auth_cache(&cache::Config::Memory).unwrap();
        let result = bootstrap::repository(
            "test-repo",
            &repo_config,
            &cache,
            DEFAULT_MAX_MANIFEST_SIZE_BYTES,
        )
        .await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().name, "test-repo");
    }

    #[tokio::test]
    async fn repositories_empty_map_succeeds() {
        let configs = HashMap::new();
        let cache = auth_cache(&cache::Config::Memory).unwrap();
        let result = repositories(&configs, &cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn repositories_overlapping_prefixes_fails() {
        let mut configs = HashMap::new();
        configs.insert(
            "team".to_string(),
            repository::Config {
                access_policy: Some(AccessPolicyConfig {
                    default: AccessMode::Allow,
                    ..AccessPolicyConfig::default()
                }),
                ..repository::Config::default()
            },
        );
        configs.insert(
            "team/app".to_string(),
            repository::Config {
                access_policy: Some(AccessPolicyConfig {
                    default: AccessMode::Allow,
                    ..AccessPolicyConfig::default()
                }),
                ..repository::Config::default()
            },
        );
        let cache = auth_cache(&cache::Config::Memory).unwrap();
        let result = repositories(&configs, &cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::Overlap(_)));
    }

    #[test]
    fn error_registry_converts_from_registry_error() {
        let err: Error = registry::Error::BlobUnknown.into();
        assert!(matches!(err, Error::Registry(_)));
    }

    #[test]
    fn error_cache_converts_from_cache_error() {
        let inner = cache::Error::Execution("backend down".to_string());
        let err: Error = inner.into();
        assert!(matches!(err, Error::Cache(_)));
    }

    #[test]
    fn error_into_scrub_error_registry_variant() {
        let bootstrap_err: Error = registry::Error::BlobUnknown.into();
        let scrub_err: MaintenanceError = bootstrap_err.into();
        assert!(matches!(scrub_err, MaintenanceError::Registry(_)));
    }

    #[test]
    fn error_into_scrub_error_cache_variant() {
        let bootstrap_err: Error = cache::Error::Execution("x".to_string()).into();
        let scrub_err: MaintenanceError = bootstrap_err.into();
        assert!(matches!(scrub_err, MaintenanceError::Cache(_)));
    }

    #[test]
    fn error_into_server_error_registry_variant() {
        let bootstrap_err: Error = registry::Error::BlobUnknown.into();
        let server_err: ServerError = bootstrap_err.into();
        assert!(matches!(server_err, ServerError::Initialization(_)));
    }

    fn s3_config_with_lock_strategy(lock_strategy: Option<LockStrategy>) -> RegistryStorageConfig {
        RegistryStorageConfig::S3(MetadataS3Config {
            connection: s3_test_connection(format!("probe-test-{}", uuid::Uuid::new_v4())),
            lock_strategy,
            link_cache_ttl: 30,
            access_time_debounce_secs: 0,
            conditional_operations: None,
        })
    }

    #[tokio::test]
    async fn test_probe_s3_lock_strategy_detects_s3_capabilities() {
        let config = s3_config_with_lock_strategy(Some(LockStrategy::S3(S3LockConfig::default())));
        let result = probe_storage(&config).await;
        assert!(
            result.is_ok(),
            "Probe should succeed against the S3 backend with S3 lock strategy: {result:?}"
        );
        let cas = result.unwrap().expect("S3 lock strategy should probe");
        assert!(
            cas,
            "the S3 test backend should support the full conditional set"
        );
    }

    #[tokio::test]
    async fn test_probe_memory_lock_strategy_detects_capabilities() {
        let config = s3_config_with_lock_strategy(Some(LockStrategy::Memory));
        let result = probe_storage(&config).await;
        assert!(
            result.is_ok(),
            "Probe should succeed for Memory lock strategy: {result:?}"
        );
        let cas = result
            .unwrap()
            .expect("S3 metadata store should return a probe verdict");
        assert!(
            cas,
            "the S3 test backend should support the full conditional set"
        );
    }

    #[tokio::test]
    async fn test_probe_fs_config_is_noop() {
        let config = RegistryStorageConfig::FS(MetadataFsConfig {
            root_dir: "/tmp/probe-test".to_string(),
            lock_strategy: LockStrategy::Memory,
            sync_to_disk: false,
        });
        let result = probe_storage(&config).await;
        assert!(result.is_ok(), "Probe should be no-op for FS config");
        assert!(
            result.unwrap().is_none(),
            "FS config should return no capabilities"
        );
    }

    #[tokio::test]
    async fn test_build_store_defaults_to_s3_lock_when_cas_is_supported() {
        let config = s3_config_with_lock_strategy(None);
        let store = build_store(&config).await.expect("build store");
        assert_eq!(
            store.lock_backend(),
            "s3",
            "a CAS-capable provider with no configured lock strategy must default to the S3 lock"
        );
    }
}
