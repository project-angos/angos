use std::{num::NonZeroUsize, sync::Arc, time::Duration};

use tokio::time::timeout;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{info, warn};

use crate::{
    cache::Cache,
    cache_fill::CacheFillJobHandler,
    command::{bootstrap, server::error::Error},
    configuration::{Configuration, RegistryStorageConfig, ResolvedStorageConfig},
    event_webhook::dispatcher::EventDispatcher,
    jobs::{
        Queue,
        runner::claim_loop,
        store::{self as job_store, ClaimMode, JobHandler, JobStore, QueueDepthRefresh},
    },
    layer::IndexLayerJobHandler,
    registry::{
        Registry, RegistryConfig, blob_store::BlobStore, metadata_store::MetadataStore,
        repository_resolver::RepositoryResolver,
    },
    replication::ReplicationJobHandler,
    scan::ScanJobHandler,
};

/// A built registry and what the server owns around it: the handle its
/// queue-depth gauges refresh from when `[global.job_queue]` is configured,
/// and otherwise the claim loops draining the in-process queue.
pub struct BuiltRegistry {
    pub registry: Arc<Registry>,
    pub depth_refresh: Option<QueueDepthRefresh>,
    pub in_process_loops: InProcessLoops,
}

/// The claim loops draining the server's own job queue, and the handles that
/// stop them. Holds no loop when `[global.job_queue]` leaves draining to
/// `angos worker`.
pub struct InProcessLoops {
    shutdown: CancellationToken,
    tracker: TaskTracker,
}

impl InProcessLoops {
    /// Adds `count` claim loops for `queue`, stopped with the others.
    fn spawn(
        &self,
        job_store: &Arc<JobStore>,
        handler: &Arc<dyn JobHandler>,
        queue: Queue,
        count: NonZeroUsize,
    ) {
        for _ in 0..count.get() {
            self.tracker.spawn(claim_loop(
                job_store.clone(),
                handler.clone(),
                queue,
                self.shutdown.clone(),
            ));
        }
    }

    /// No loops, so nothing to stop.
    fn none() -> Self {
        Self {
            shutdown: CancellationToken::new(),
            tracker: TaskTracker::new(),
        }
    }

    /// Stop claiming, without waiting on the jobs in flight. Used for the
    /// generation a reload displaces, which finishes what it holds while the
    /// generation replacing it claims the rest.
    pub fn cancel(&self) {
        self.shutdown.cancel();
    }

    /// Stop claiming and wait up to `grace` for the jobs in flight, which
    /// cancellation lets run to completion rather than interrupting.
    pub async fn shutdown_with_timeout(&self, grace: Duration) {
        self.shutdown.cancel();
        self.tracker.close();
        if timeout(grace, self.tracker.wait()).await.is_err() {
            warn!("In-process job loops did not drain within shutdown grace period");
        }
    }
}

/// Spawn `cache_loops` and `replication_loops` claim loops draining
/// `job_store`. Replication drains only when a downstream is configured: an
/// always-empty queue would just storm the object store with `LIST`s.
/// How many in-process loops each queue gets.
struct LoopCounts {
    cache: NonZeroUsize,
    replication: NonZeroUsize,
    index: NonZeroUsize,
}

fn spawn_in_process_loops(
    job_store: &Arc<JobStore>,
    resolver: &Arc<RepositoryResolver>,
    blob_store: &Arc<BlobStore>,
    metadata_store: &Arc<MetadataStore>,
    event_dispatcher: Option<Arc<EventDispatcher>>,
    counts: &LoopCounts,
) -> InProcessLoops {
    let loops = InProcessLoops::none();

    // Any image may be indexed on demand, so the queue always drains.
    let index_handler: Arc<dyn JobHandler> = Arc::new(IndexLayerJobHandler::new(
        blob_store.clone(),
        metadata_store.clone(),
    ));
    loops.spawn(job_store, &index_handler, Queue::Index, counts.index);

    let cache_handler: Arc<dyn JobHandler> = Arc::new(CacheFillJobHandler::new(
        resolver.clone(),
        blob_store.clone(),
        metadata_store.clone(),
        event_dispatcher,
    ));
    loops.spawn(job_store, &cache_handler, Queue::Cache, counts.cache);

    let any_downstream = resolver
        .keys()
        .filter_map(|name| resolver.get(name))
        .any(|repository| !repository.replication.is_empty());
    if any_downstream {
        // Mesh cycles terminate: only state-changing writes dispatch, and
        // receiver-side no-op suppression stops any remaining replays.
        let replication_handler: Arc<dyn JobHandler> = Arc::new(ReplicationJobHandler::new(
            resolver.clone(),
            blob_store.clone(),
            metadata_store.clone(),
        ));
        loops.spawn(
            job_store,
            &replication_handler,
            Queue::Replication,
            counts.replication,
        );
    }

    loops
}

/// Resolve the registry-storage config once per (re)build.
fn resolve_storage_config(config: &Configuration) -> ResolvedStorageConfig {
    let storage_config = config.resolve_registry_storage();
    if matches!(config.registry_storage, RegistryStorageConfig::Inherit)
        && matches!(&storage_config, ResolvedStorageConfig::S3(_))
    {
        info!("Auto-configuring S3 metadata-store from blob-store");
    }
    storage_config
}

/// Build the runtime `Registry` and the job queue it enqueues into. With
/// `[global.job_queue]` that queue is durable and draining it stays
/// `angos worker`'s job; without it the server drains an in-process queue
/// through loops it owns.
pub async fn build_registry(
    config: &Configuration,
    auth_cache: &Arc<Cache>,
) -> Result<BuiltRegistry, Error> {
    let blob_backend = Arc::new(
        config
            .blob_store
            .build_backend()?
            .with_namespace_walk_concurrency(config.global.namespace_walk_concurrency),
    );
    let storage_config = resolve_storage_config(config);
    let metadata_store = bootstrap::metadata_store(
        &storage_config,
        auth_cache,
        config.global.namespace_walk_concurrency,
        config.global.gc_grace_secs,
        config.global.atime_audit_window_secs,
    )
    .map_err(Error::from)?;
    let max_manifest_size_bytes = config.global.max_manifest_size_bytes();
    let repositories =
        bootstrap::repositories(&config.repository, auth_cache, max_manifest_size_bytes).await?;

    let event_dispatcher = EventDispatcher::from_config(&config.event_webhook)?;

    // With [global.job_queue] the jobs survive a restart and `angos worker`
    // drains them; without it the queue lives beside the metadata store and
    // the loops spawned here drain it until the server stops them.
    let (job_store, depth_refresh, in_process_loops) =
        if let Some(jq_config) = &config.global.job_queue {
            let claim_mode = job_store::ensure_claim_support(metadata_store.object_store()).await?;
            let job_store: Arc<JobStore> = Arc::new(JobStore::with_retry_policy(
                metadata_store.object_store().clone(),
                "server",
                claim_mode,
                jq_config.retry_policy(),
            ));
            let refresh = QueueDepthRefresh {
                store: job_store.clone(),
                period: Duration::from_secs(jq_config.pending_refresh_interval_secs),
                ready_horizon_secs: jq_config.pending_ready_horizon_secs,
            };
            (job_store, Some(refresh), InProcessLoops::none())
        } else {
            // Atomic mode: in-process draining runs no startup probe to pick one.
            let job_store: Arc<JobStore> = Arc::new(JobStore::new(
                metadata_store.object_store().clone(),
                "in-process",
                ClaimMode::Atomic,
            ));
            let loops = spawn_in_process_loops(
                &job_store,
                &repositories,
                &blob_backend,
                &metadata_store,
                event_dispatcher.clone(),
                &LoopCounts {
                    cache: config.global.max_concurrent_cache_jobs,
                    replication: config.global.max_concurrent_replication_jobs,
                    index: config.global.max_concurrent_index_jobs,
                },
            );
            (job_store, None, loops)
        };

    let registry_config = RegistryConfig {
        update_pull_time: config.global.update_pull_time,
        enable_blob_redirect: config.global.enable_blob_redirect,
        enable_manifest_redirect: config.global.enable_manifest_redirect,
        max_manifest_size_bytes,
        max_blob_size_bytes: config.global.max_blob_size_bytes(),
        blob_stream_frame_size: config.global.blob_stream_frame_size_bytes(),
        validate_manifest_references: !config.global.allow_missing_manifest_references,
        global_immutable_tags: config.global.immutable_tags,
        global_immutable_tags_exclusions: config.global.immutable_tags_exclusions.clone(),
        event_dispatcher,
        ..RegistryConfig::new(job_store)
    };
    let job_store = registry_config.job_queue.clone();
    let (blob_store_for_scan, metadata_store_for_scan) =
        (blob_backend.clone(), metadata_store.clone());
    let registry = Registry::new(blob_backend, metadata_store, repositories, registry_config);
    // The scan handler pushes reports through the registry, so its loops can
    // only start once the registry exists.
    if let Some(scan) = &config.global.scan
        && config.global.job_queue.is_none()
    {
        let handler: Arc<dyn JobHandler> = Arc::new(
            ScanJobHandler::new(
                registry.clone(),
                blob_store_for_scan,
                metadata_store_for_scan,
                scan,
            )
            .map_err(Error::from)?,
        );
        in_process_loops.spawn(
            &job_store,
            &handler,
            Queue::Scan,
            config.global.max_concurrent_scan_jobs,
        );
    }

    Ok(BuiltRegistry {
        registry,
        depth_refresh,
        in_process_loops,
    })
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use tempfile::TempDir;
    use tokio::time::{sleep, timeout};
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use angos_oci::header::DOCKER_CONTENT_DIGEST;
    use angos_oci::{Namespace, Tag};

    use super::{InProcessLoops, LoopCounts, spawn_in_process_loops};
    use crate::{
        configuration::global::{
            DEFAULT_MAX_CONCURRENT_CACHE_JOBS, DEFAULT_MAX_CONCURRENT_INDEX_JOBS,
            DEFAULT_MAX_CONCURRENT_REPLICATION_JOBS,
        },
        jobs::{
            Queue,
            store::{ClaimMode, JobEnvelope, JobStore},
        },
        metrics_provider::init_for_tests,
        registry::{
            Registry, RegistryConfig, Repository,
            manifest::DispatchTarget,
            metadata_store::MetadataStore,
            test_utils::{
                FsTestStack, downstream_client, fs_test_stack, repository_with_downstream,
                repository_with_replication, seed_manifest, single_repo_resolver,
            },
        },
        registry_client::RegistryClient,
        replication::REPLICATION_PUSH_MANIFEST_KIND,
    };

    const NAMESPACE: &str = "nginx";
    const REPO: &str = "nginx";

    /// A registry whose queue the server's own loops drain, as it is wired
    /// without `[global.job_queue]`.
    struct InProcessStack {
        registry: Arc<Registry>,
        job_store: Arc<JobStore>,
        metadata_store: Arc<MetadataStore>,
        _loops: InProcessLoops,
        _dir: TempDir,
    }

    fn build_stack(repository: Repository) -> InProcessStack {
        init_for_tests();
        let FsTestStack {
            dir,
            store: _,
            metadata_store,
            blob_store,
        } = fs_test_stack();
        let resolver = single_repo_resolver(REPO, repository);
        let job_store: Arc<JobStore> = Arc::new(JobStore::new(
            metadata_store.object_store().clone(),
            "in-process",
            ClaimMode::Atomic,
        ));
        let loops = spawn_in_process_loops(
            &job_store,
            &resolver,
            &blob_store,
            &metadata_store,
            None,
            &LoopCounts {
                cache: DEFAULT_MAX_CONCURRENT_CACHE_JOBS,
                replication: DEFAULT_MAX_CONCURRENT_REPLICATION_JOBS,
                index: DEFAULT_MAX_CONCURRENT_INDEX_JOBS,
            },
        );
        let registry = Registry::new(
            blob_store,
            metadata_store.clone(),
            resolver,
            RegistryConfig::new(job_store.clone()),
        );
        InProcessStack {
            registry,
            job_store,
            metadata_store,
            _loops: loops,
            _dir: dir,
        }
    }

    /// The drained job runs the whole push pipeline (HEAD before PUT for each
    /// blob, then the manifest) against a wiremock downstream.
    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn the_in_process_loops_drain_a_replication_push() {
        let mock_server = MockServer::start().await;
        let client: Arc<RegistryClient> = downstream_client(&mock_server.uri());
        let stack = build_stack(repository_with_downstream(REPO, client));
        let namespace = Namespace::new(NAMESPACE).unwrap();

        let (manifest_digest, config_digest, layer_digest) = seed_manifest(
            stack.metadata_store.object_store(),
            &stack.metadata_store,
            &namespace,
        )
        .await;

        for blob in [&config_digest, &layer_digest] {
            Mock::given(method("HEAD"))
                .and(path(format!("/v2/{NAMESPACE}/blobs/{blob}")))
                .respond_with(ResponseTemplate::new(404))
                .mount(&mock_server)
                .await;
        }
        Mock::given(method("POST"))
            .and(path(format!("/v2/{NAMESPACE}/blobs/uploads/")))
            .respond_with(
                ResponseTemplate::new(202)
                    .insert_header("Location", format!("/v2/{NAMESPACE}/blobs/uploads/s1")),
            )
            .mount(&mock_server)
            .await;
        Mock::given(method("PATCH"))
            .and(path(format!("/v2/{NAMESPACE}/blobs/uploads/s1")))
            .respond_with(
                ResponseTemplate::new(202)
                    .insert_header("Location", format!("/v2/{NAMESPACE}/blobs/uploads/s1")),
            )
            .mount(&mock_server)
            .await;
        Mock::given(method("PUT"))
            .and(path(format!("/v2/{NAMESPACE}/blobs/uploads/s1")))
            .respond_with(ResponseTemplate::new(201))
            .mount(&mock_server)
            .await;
        Mock::given(method("PUT"))
            .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
            .respond_with(
                ResponseTemplate::new(201)
                    .insert_header(DOCKER_CONTENT_DIGEST, manifest_digest.to_string().as_str()),
            )
            .expect(1..)
            .mount(&mock_server)
            .await;

        let repository = stack.registry.get_repository_for_namespace(&namespace).ok();
        let tag = Tag::new("v1").unwrap();
        stack
            .registry
            .dispatch_replication(
                repository,
                &namespace,
                DispatchTarget::Push {
                    tag: Some(&tag),
                    digest: &manifest_digest,
                },
                None,
            )
            .await;

        let manifest_path = format!("/v2/{NAMESPACE}/manifests/v1");
        let saw_put = timeout(Duration::from_secs(10), async {
            loop {
                let received = mock_server.received_requests().await.unwrap_or_default();
                if received
                    .iter()
                    .any(|r| r.method.as_str() == "PUT" && r.url.path() == manifest_path)
                {
                    return true;
                }
                sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .unwrap_or(false);
        assert!(
            saw_put,
            "the in-process loops must drain the job and push the manifest downstream"
        );

        // Zero pending proves the loop completed the job rather than retrying.
        let drained = timeout(Duration::from_secs(10), async {
            loop {
                if stack
                    .job_store
                    .count_pending(Queue::Replication, 0)
                    .await
                    .unwrap_or(u64::MAX)
                    == 0
                {
                    return true;
                }
                sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .unwrap_or(false);
        assert!(drained, "the replication queue must drain to zero pending");
        assert_eq!(
            stack
                .job_store
                .list_failed_page(Queue::Replication, 16, None)
                .await
                .map_or(usize::MAX, |page| page.items.len()),
            0,
            "a successful push must not dead-letter the job"
        );
    }

    /// With no downstream configured no replication loop is spawned, so the job
    /// stays pending rather than being drained.
    #[tokio::test]
    async fn no_replication_loop_without_a_downstream() {
        let stack = build_stack(repository_with_replication(REPO, Vec::new()));

        stack
            .job_store
            .enqueue(
                JobEnvelope::new(
                    Queue::Replication,
                    REPLICATION_PUSH_MANIFEST_KIND,
                    format!("{}.eu:nginx:v1", Queue::Replication),
                    &(),
                )
                .unwrap(),
            )
            .await
            .unwrap();

        // A spawned loop idles at 10 ms under cfg(test), so one would have
        // claimed the job many times over within 500 ms.
        sleep(Duration::from_millis(500)).await;

        assert_eq!(
            stack
                .job_store
                .count_pending(Queue::Replication, 0)
                .await
                .unwrap(),
            1,
            "no downstream means no replication loop, so the job must stay pending"
        );
    }
}
