use std::{
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use tokio_util::sync::CancellationToken;

use crate::{
    cache::Cache,
    command::{bootstrap, server::error::Error},
    configuration::{Configuration, RegistryStorageConfig},
    event_webhook::dispatcher::EventDispatcher,
    jobs::store::{self as job_store, JobStore},
    registry::{Registry, RegistryConfig, metadata_store::MetadataStore},
};

/// Handle on the durable job-store and the interval the server should refresh
/// the `angos_job_queue_pending` gauge at. `None` when `[global.job_queue]`
/// is absent (in-process engine-backed queue; no pending gauge is needed).
pub struct PendingGaugeRefresh {
    pub store: Arc<JobStore>,
    pub interval: Duration,
    pub ready_horizon_secs: u64,
}

pub async fn build_metadata_store(
    config: &Configuration,
    cache: &Arc<Cache>,
    cached_conditional_operations: &Arc<Mutex<Option<bool>>>,
) -> Result<Arc<MetadataStore>, Error> {
    let mut storage_config = config.resolve_registry_storage();

    // Resolve S3 conditional-write support once and memoize it so a config
    // hot-reload rebuilds the metadata store without re-probing the endpoint.
    // An operator-declared value skips this entirely. Injecting the resolved
    // value into the config means `build_store` won't re-probe.
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

    bootstrap::metadata_store(&storage_config, cache)
        .await
        .map_err(Error::from)
}

/// Build the runtime `Registry`. When `[global.job_queue]` selects a durable
/// backend the second element carries the `JobStore` and the configured
/// pending-gauge refresh interval; the server spawns its own ticker from it.
/// The server never drains the queue itself: that is `angos worker`'s job.
///
/// When `engine_maintenance` is `Some`, the transactional-engine recovery loop
/// and body janitor are spawned tied to that token. Pass `None` on
/// hot-reload paths where the maintenance tasks were already started by the
/// initial bootstrap.
pub async fn build_registry(
    config: &Configuration,
    cached_conditional_operations: &Arc<Mutex<Option<bool>>>,
    engine_maintenance: Option<CancellationToken>,
) -> Result<(Arc<Registry>, Option<PendingGaugeRefresh>), Error> {
    let auth_cache = bootstrap::auth_cache(&config.cache)?;
    let blob_backend = Arc::new(config.blob_store.build_backend()?);
    let metadata_store =
        build_metadata_store(config, &auth_cache, cached_conditional_operations).await?;
    let max_manifest_size_bytes = config.global.max_manifest_size_bytes();
    let repositories =
        bootstrap::repositories(&config.repository, &auth_cache, max_manifest_size_bytes).await?;

    let mut registry_config = RegistryConfig {
        update_pull_time: config.global.update_pull_time,
        enable_blob_redirect: config.global.resolved_enable_blob_redirect(),
        enable_manifest_redirect: config.global.resolved_enable_manifest_redirect(),
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
    // autoscaling. The storage handles are shared with the metadata store.
    //
    // Always fetch the shared storage handles when this is the initial
    // bootstrap so we can spawn the engine maintenance loops against the same
    // store the metadata/job paths use.
    let storage_config = config.resolve_registry_storage();
    let maintenance_handles = if engine_maintenance.is_some() || config.global.job_queue.is_some() {
        Some(bootstrap::build_store(&storage_config).await?)
    } else {
        None
    };

    let pending = if let Some(jq_config) = &config.global.job_queue {
        if let Some(handles) = maintenance_handles.as_ref() {
            job_store::ensure_shared_lock(handles)?;
            let job_store: Arc<JobStore> = Arc::new(JobStore::new(handles.clone(), "server"));
            registry_config.job_queue = Some(job_store.clone());
            Some(PendingGaugeRefresh {
                store: job_store,
                interval: Duration::from_secs(jq_config.pending_refresh_interval_secs),
                ready_horizon_secs: jq_config.pending_ready_horizon_secs,
            })
        } else {
            None
        }
    } else {
        None
    };

    if let (Some(token), Some(handles)) = (engine_maintenance, maintenance_handles) {
        tokio::spawn(handles.maintenance(token));
    }

    let registry = Registry::new(blob_backend, metadata_store, repositories, registry_config)
        .map_err(|e| Error::Initialization(e.to_string()))?;

    Ok((registry, pending))
}
