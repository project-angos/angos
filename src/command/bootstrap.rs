use std::{collections::HashMap, sync::Arc};

use crate::{
    cache::{self, Cache},
    configuration::{RegistryStorageConfig, registry_storage},
    registry::{
        self, Repository, blob_store, job_store,
        metadata_store::{self, MetadataStore},
        repository,
        repository_resolver::{OverlapError, RepositoryResolver},
    },
};

/// Errors produced by the shared CLI bootstrap helpers.
///
/// `blob_store::Error` does not implement `std::error::Error`, which is a
/// prerequisite for `#[from]` in thiserror. A manual `From` impl is provided
/// instead; `source()` cannot chain into `blob_store::Error` until that type
/// is migrated.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to initialize blob store: {0}")]
    BlobStore(blob_store::Error),
    #[error("failed to initialize metadata store: {0}")]
    MetadataStore(#[from] metadata_store::Error),
    #[error("failed to initialize storage handles: {0}")]
    RegistryStorage(#[from] registry_storage::Error),
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
}

impl From<blob_store::Error> for Error {
    fn from(e: blob_store::Error) -> Self {
        Self::BlobStore(e)
    }
}

pub async fn metadata_store(
    config: &RegistryStorageConfig,
    auth_cache: &Arc<Cache>,
) -> Result<Arc<MetadataStore>, Error> {
    let store = config.build_store().await?;

    let (link_cache_ttl, access_time_debounce_secs) = config.link_cache_tuning();
    let mut builder = MetadataStore::builder(store)
        .link_cache_ttl(link_cache_ttl)
        .access_time_debounce_secs(access_time_debounce_secs);

    // Wire in the auth cache for link-metadata caching (only meaningful on S3,
    // where link_cache_ttl > 0 by default).
    builder = builder.cache(auth_cache.clone());

    Ok(Arc::new(builder.build()))
}

pub fn auth_cache(config: &cache::Config) -> Result<Arc<Cache>, Error> {
    config.to_backend().map_err(Error::from)
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

    use crate::{
        cache,
        command::bootstrap::{self, Error, auth_cache, repositories},
        command::scrub::Error as ScrubError,
        command::server::Error as ServerError,
        policy::{AccessMode, AccessPolicyConfig},
        registry::{blob_store, manifest::DEFAULT_MAX_MANIFEST_SIZE_BYTES, repository},
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
            access_policy: AccessPolicyConfig {
                default: AccessMode::Allow,
                ..AccessPolicyConfig::default()
            },
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
                access_policy: AccessPolicyConfig {
                    default: AccessMode::Allow,
                    ..AccessPolicyConfig::default()
                },
                ..repository::Config::default()
            },
        );
        configs.insert(
            "team/app".to_string(),
            repository::Config {
                access_policy: AccessPolicyConfig {
                    default: AccessMode::Allow,
                    ..AccessPolicyConfig::default()
                },
                ..repository::Config::default()
            },
        );
        let cache = auth_cache(&cache::Config::Memory).unwrap();
        let result = repositories(&configs, &cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::Overlap(_)));
    }

    #[test]
    fn error_blob_store_converts_from_blob_store_error() {
        let inner = blob_store::Error::BlobNotFound;
        let err: Error = inner.into();
        assert!(matches!(err, Error::BlobStore(_)));
    }

    #[test]
    fn error_cache_converts_from_cache_error() {
        let inner = cache::Error::Execution("backend down".to_string());
        let err: Error = inner.into();
        assert!(matches!(err, Error::Cache(_)));
    }

    #[test]
    fn error_into_scrub_error_blob_store_variant() {
        let bootstrap_err: Error = blob_store::Error::BlobNotFound.into();
        let scrub_err: ScrubError = bootstrap_err.into();
        assert!(matches!(scrub_err, ScrubError::BlobStore(_)));
    }

    #[test]
    fn error_into_scrub_error_cache_variant() {
        let bootstrap_err: Error = cache::Error::Execution("x".to_string()).into();
        let scrub_err: ScrubError = bootstrap_err.into();
        assert!(matches!(scrub_err, ScrubError::Cache(_)));
    }

    #[test]
    fn error_into_server_error_blob_store_variant() {
        let bootstrap_err: Error = blob_store::Error::BlobNotFound.into();
        let server_err: ServerError = bootstrap_err.into();
        assert!(matches!(server_err, ServerError::Initialization(_)));
        assert_eq!(server_err.to_string(), "Failed to initialize blob store");
    }
}
