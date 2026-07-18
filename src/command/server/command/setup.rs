use std::{
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};
use tracing::info;

use tokio_util::sync::CancellationToken;

use crate::{
    cache::Cache,
    command::{bootstrap, server::error::Error},
    configuration::{Configuration, RegistryStorageConfig},
    event_webhook::dispatcher::EventDispatcher,
    jobs::store::{self as job_store, JobStore},
    registry::{Registry, RegistryConfig},
};

/// Handle on the durable job-store and the interval the server should refresh
/// the `angos_job_queue_pending` gauge at. `None` when `[global.job_queue]`
/// is absent (in-process engine-backed queue; no pending gauge is needed).
pub struct PendingGaugeRefresh {
    pub store: Arc<JobStore>,
    pub interval: Duration,
    pub ready_horizon_secs: u64,
}

/// Resolve the registry-storage config once per (re)build, injecting the
/// memoized S3 conditional-write probe so every consumer of the resolved
/// config shares one verdict and hot reloads never re-probe the endpoint.
async fn resolve_storage_config(
    config: &Configuration,
    cached_conditional_operations: &Arc<Mutex<Option<bool>>>,
) -> Result<RegistryStorageConfig, Error> {
    let mut storage_config = config.resolve_registry_storage();
    if matches!(config.registry_storage, RegistryStorageConfig::Inherit)
        && matches!(&storage_config, RegistryStorageConfig::S3(_))
    {
        info!("Auto-configuring S3 metadata-store from blob-store");
    }

    // An operator-declared value skips the probe entirely. Injecting the
    // resolved value into the config means `build_store` won't re-probe.
    if matches!(&storage_config, RegistryStorageConfig::S3(b) if b.conditional_operations.is_none())
    {
        let cached = *cached_conditional_operations
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let cas = if let Some(cas) = cached {
            cas
        } else {
            let probed = bootstrap::probe_storage(&storage_config)
                .await
                .map_err(Error::from)?;
            let resolved = probed.unwrap_or_default();
            *cached_conditional_operations
                .lock()
                .unwrap_or_else(PoisonError::into_inner) = Some(resolved);
            resolved
        };
        if let RegistryStorageConfig::S3(ref mut backend_config) = storage_config {
            backend_config.conditional_operations = Some(cas);
        }
    }

    Ok(storage_config)
}

/// Build the runtime `Registry`. When `[global.job_queue]` selects a durable
/// backend the second element carries the `JobStore` and the configured
/// pending-gauge refresh interval; the server spawns its own ticker from it.
/// The server never drains the queue itself: that is `angos worker`'s job.
///
/// When `engine_maintenance` is `Some`, the transactional-engine recovery
/// loop is spawned tied to that token. Pass `None` on hot-reload paths where
/// it was already started by the initial bootstrap.
pub async fn build_registry(
    config: &Configuration,
    auth_cache: &Arc<Cache>,
    cached_conditional_operations: &Arc<Mutex<Option<bool>>>,
    engine_maintenance: Option<CancellationToken>,
) -> Result<(Arc<Registry>, Option<PendingGaugeRefresh>), Error> {
    let blob_backend = Arc::new(config.blob_store.build_backend()?);
    let storage_config = resolve_storage_config(config, cached_conditional_operations).await?;
    let metadata_store = bootstrap::metadata_store(&storage_config, auth_cache)
        .await
        .map_err(Error::from)?;
    let max_manifest_size_bytes = config.global.max_manifest_size_bytes();
    let repositories =
        bootstrap::repositories(&config.repository, auth_cache, max_manifest_size_bytes).await?;

    let mut registry_config = RegistryConfig {
        update_pull_time: config.global.update_pull_time,
        enable_blob_redirect: config.global.enable_blob_redirect,
        enable_manifest_redirect: config.global.enable_manifest_redirect,
        max_manifest_size_bytes,
        max_blob_size_bytes: config.global.max_blob_size_bytes(),
        validate_manifest_references: !config.global.allow_missing_manifest_references,
        global_immutable_tags: config.global.immutable_tags,
        global_immutable_tags_exclusions: config.global.immutable_tags_exclusions.clone(),
        max_concurrent_cache_jobs: config.global.max_concurrent_cache_jobs,
        max_concurrent_replication_jobs: config.global.max_concurrent_replication_jobs,
        event_dispatcher: EventDispatcher::from_config(&config.event_webhook)?,
        ..RegistryConfig::default()
    };

    // When [global.job_queue] is present, route cache-fill jobs through the
    // durable backend (so they survive restarts and let `angos worker` drain
    // them) and surface the pending count on this server's /metrics for
    // autoscaling. The job store and the engine maintenance loop share the
    // metadata store's engine façade, so no second store is wired and the
    // conditional-write probe runs at most once per process.
    let pending = if let Some(jq_config) = &config.global.job_queue {
        let engine = metadata_store.store_arc();
        job_store::ensure_shared_lock(&engine)?;
        let job_store: Arc<JobStore> = Arc::new(JobStore::new(engine, "server"));
        registry_config.job_queue = Some(job_store.clone());
        Some(PendingGaugeRefresh {
            store: job_store,
            interval: Duration::from_secs(jq_config.pending_refresh_interval_secs),
            ready_horizon_secs: jq_config.pending_ready_horizon_secs,
        })
    } else {
        None
    };

    if let Some(token) = engine_maintenance {
        tokio::spawn(metadata_store.store_arc().recovery(token));
    }

    let registry = Registry::new(blob_backend, metadata_store, repositories, registry_config);

    Ok((registry, pending))
}
