use std::sync::{Arc, Mutex};

use crate::{
    cache::Cache,
    command::{bootstrap, server::error::Error},
    configuration::Configuration,
    registry::{
        Registry, RegistryConfig,
        metadata_store::{ConditionalCapabilities, MetadataStore, MetadataStoreConfig},
    },
};

pub async fn build_metadata_store(
    config: &Configuration,
    cache: &Arc<Cache>,
    cached_capabilities: &Arc<Mutex<Option<ConditionalCapabilities>>>,
) -> Result<Arc<dyn MetadataStore>, Error> {
    let mut metadata_config = config.resolve_metadata_config();

    if let MetadataStoreConfig::S3(ref mut backend_config) = metadata_config
        && backend_config.capabilities.is_none()
    {
        let guard = cached_capabilities
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if guard.is_some() {
            backend_config.capabilities.clone_from(&guard);
        }
    }

    let (store, caps) = bootstrap::metadata_store(&metadata_config, cache)
        .await
        .map_err(Error::from)?;

    let mut guard = cached_capabilities
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    *guard = caps;

    Ok(store)
}

pub async fn build_registry(
    config: &Configuration,
    cached_capabilities: &Arc<Mutex<Option<ConditionalCapabilities>>>,
) -> Result<Registry, Error> {
    let auth_cache = bootstrap::auth_cache(&config.cache)?;
    let blob_handles = bootstrap::blob_stores(&config.blob_store, &auth_cache)?;
    let metadata_store = build_metadata_store(config, &auth_cache, cached_capabilities).await?;
    let repositories = bootstrap::repositories(&config.repository, &auth_cache).await?;

    let registry_config = RegistryConfig::new()
        .update_pull_time(config.global.update_pull_time)
        .enable_blob_redirect(config.global.resolved_enable_blob_redirect())
        .enable_manifest_redirect(config.global.resolved_enable_manifest_redirect())
        .concurrent_cache_jobs(config.global.max_concurrent_cache_jobs)
        .global_immutable_tags(config.global.immutable_tags)
        .global_immutable_tags_exclusions(config.global.immutable_tags_exclusions.clone());

    Registry::new(
        blob_handles.blob_store,
        blob_handles.upload_store,
        blob_handles.presigned_store,
        metadata_store,
        repositories,
        registry_config,
    )
    .map_err(|e| Error::Initialization(e.to_string()))
}
