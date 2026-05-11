use std::{collections::HashMap, sync::Arc};

use crate::{
    cache::{self, Cache},
    registry::{
        Repository,
        blob_store::{self, BlobStoreHandles},
        metadata_store::{ConditionalCapabilities, MetadataStore, MetadataStoreConfig},
        repository,
    },
};

/// Errors produced by the shared CLI bootstrap helpers.
///
/// `blob_store::Error` does not implement `std::error::Error`, which is a
/// prerequisite for `#[from]` in thiserror. A manual `From` impl is provided
/// instead; `source()` cannot chain into `blob_store::Error` until that type
/// is migrated (tracked in Epic 09).
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to initialize blob store: {0}")]
    BlobStore(blob_store::Error),
    #[error("failed to initialize metadata store: {0}")]
    MetadataStore(#[from] crate::registry::metadata_store::Error),
    #[error("failed to initialize cache: {0}")]
    Cache(#[from] cache::Error),
    #[error("failed to initialize repository '{name}': {source}")]
    Repository {
        name: String,
        source: Box<crate::registry::Error>,
    },
}

impl From<blob_store::Error> for Error {
    fn from(e: blob_store::Error) -> Self {
        Self::BlobStore(e)
    }
}

pub fn blob_stores(
    config: &blob_store::BlobStorageConfig,
    auth_cache: &Arc<Cache>,
) -> Result<BlobStoreHandles, Error> {
    config
        .to_backend(Some(auth_cache.clone()))
        .map_err(Error::from)
}

pub async fn metadata_store(
    config: &MetadataStoreConfig,
    auth_cache: &Arc<Cache>,
) -> Result<
    (
        Arc<dyn MetadataStore + Send + Sync>,
        Option<ConditionalCapabilities>,
    ),
    Error,
> {
    config
        .to_backend(Some(auth_cache.clone()))
        .await
        .map_err(Error::from)
}

pub fn auth_cache(config: &cache::Config) -> Result<Arc<Cache>, Error> {
    config.to_backend().map_err(Error::from)
}

pub fn repository(
    name: &str,
    config: &repository::Config,
    auth_cache: &Arc<Cache>,
) -> Result<Repository, Error> {
    Repository::new(name, config, auth_cache).map_err(|source| Error::Repository {
        name: name.to_string(),
        source: Box::new(source),
    })
}

pub fn repositories(
    configs: &HashMap<String, repository::Config>,
    auth_cache: &Arc<Cache>,
) -> Result<Arc<HashMap<String, Repository>>, Error> {
    let mut map = HashMap::with_capacity(configs.len());
    for (name, config) in configs {
        map.insert(name.clone(), repository(name, config, auth_cache)?);
    }
    Ok(Arc::new(map))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        cache,
        policy::{AccessMode, AccessPolicyConfig},
    };

    #[test]
    fn auth_cache_memory_succeeds() {
        let config = cache::Config::Memory;
        let result = auth_cache(&config);
        assert!(result.is_ok());
    }

    #[test]
    fn repository_with_default_config_succeeds() {
        let repo_config = repository::Config {
            access_policy: AccessPolicyConfig {
                default: AccessMode::Allow,
                ..AccessPolicyConfig::default()
            },
            ..repository::Config::default()
        };
        let cache = auth_cache(&cache::Config::Memory).unwrap();
        let result = repository("test-repo", &repo_config, &cache);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().name, "test-repo");
    }

    #[test]
    fn repositories_empty_map_succeeds() {
        let configs = HashMap::new();
        let cache = auth_cache(&cache::Config::Memory).unwrap();
        let result = repositories(&configs, &cache);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0);
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
        use crate::command::scrub::Error as ScrubError;
        let bootstrap_err: Error = blob_store::Error::BlobNotFound.into();
        let scrub_err: ScrubError = bootstrap_err.into();
        assert!(matches!(scrub_err, ScrubError::BlobStore(_)));
    }

    #[test]
    fn error_into_scrub_error_cache_variant() {
        use crate::command::scrub::Error as ScrubError;
        let bootstrap_err: Error = cache::Error::Execution("x".to_string()).into();
        let scrub_err: ScrubError = bootstrap_err.into();
        assert!(matches!(scrub_err, ScrubError::Cache(_)));
    }

    #[test]
    fn error_into_server_error_blob_store_variant() {
        use crate::command::server::Error as ServerError;
        let bootstrap_err: Error = blob_store::Error::BlobNotFound.into();
        let server_err: ServerError = bootstrap_err.into();
        assert!(matches!(server_err, ServerError::Initialization(_)));
        assert_eq!(server_err.to_string(), "Failed to initialize blob store");
    }
}
