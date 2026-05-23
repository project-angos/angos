use std::{
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use crate::{
    cache::Cache,
    command::{bootstrap, server::error::Error},
    configuration::Configuration,
    registry::{
        Registry, RegistryConfig,
        job_store::{JobStore, durable::DurableJobQueue},
        metadata_store::{ConditionalCapabilities, MetadataStore, MetadataStoreConfig},
    },
};

/// Handle on the durable job-store and the interval the server should refresh
/// the `angos_job_queue_pending` gauge at. `None` when `[global.job_queue]`
/// is absent (in-process `TaskQueue` path).
pub struct PendingGaugeRefresh {
    pub store: Arc<dyn JobStore>,
    pub interval: Duration,
    pub ready_horizon_secs: u64,
}

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
            .unwrap_or_else(PoisonError::into_inner);
        if guard.is_some() {
            backend_config.capabilities.clone_from(&guard);
        }
    }

    let (store, caps) = bootstrap::metadata_store(&metadata_config, cache)
        .await
        .map_err(Error::from)?;

    *cached_capabilities
        .lock()
        .unwrap_or_else(PoisonError::into_inner) = caps;

    Ok(store)
}

/// Build the runtime `Registry`. When `[global.job_queue]` selects a durable
/// backend the second element carries the `JobStore` and the configured
/// pending-gauge refresh interval; the server spawns its own ticker from it.
/// The server never drains the queue itself — that is `angos worker`'s job.
pub async fn build_registry(
    config: &Configuration,
    cached_capabilities: &Arc<Mutex<Option<ConditionalCapabilities>>>,
) -> Result<(Registry, Option<PendingGaugeRefresh>), Error> {
    let auth_cache = bootstrap::auth_cache(&config.cache)?;
    let blob_handles = bootstrap::blob_stores(&config.blob_store, &auth_cache)?;
    let metadata_store = build_metadata_store(config, &auth_cache, cached_capabilities).await?;
    let max_manifest_size_bytes = config.global.max_manifest_size_bytes();
    let repositories =
        bootstrap::repositories(&config.repository, &auth_cache, max_manifest_size_bytes).await?;

    let mut registry_config = RegistryConfig::default()
        .update_pull_time(config.global.update_pull_time)
        .enable_blob_redirect(config.global.resolved_enable_blob_redirect())
        .enable_manifest_redirect(config.global.resolved_enable_manifest_redirect())
        .max_manifest_size_bytes(max_manifest_size_bytes)
        .concurrent_cache_jobs(config.global.max_concurrent_cache_jobs)
        .global_immutable_tags(config.global.immutable_tags)
        .global_immutable_tags_exclusions(config.global.immutable_tags_exclusions.clone());

    // When [global.job_queue] is present, route cache-fill jobs through the
    // durable backend (so they survive restarts and let `angos worker` drain
    // them) and surface the pending count on this server's /metrics for
    // autoscaling.
    let pending = if let Some(jq_config) = &config.global.job_queue {
        let backends = jq_config.to_backends().map_err(bootstrap::Error::from)?;
        registry_config =
            registry_config.job_queue(Arc::new(DurableJobQueue::new(backends.store.clone())));
        Some(PendingGaugeRefresh {
            store: backends.store,
            interval: Duration::from_secs(jq_config.pending_refresh_interval_secs),
            ready_horizon_secs: jq_config.pending_ready_horizon_secs,
        })
    } else {
        None
    };

    let registry = Registry::new(
        blob_handles.blob_store,
        blob_handles.upload_store,
        blob_handles.presigned_store,
        metadata_store,
        repositories,
        registry_config,
    )
    .map_err(|e| Error::Initialization(e.to_string()))?;

    Ok((registry, pending))
}
