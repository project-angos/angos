use std::iter::repeat_n;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{info, warn};

use angos_cache::Cache;

use crate::cache_fill::CacheFillJobHandler;
use crate::command::bootstrap::{self, StoreContext};
use crate::command::server::error::Error;
use crate::configuration::{Configuration, GlobalConfig, RegistryStorageConfig};
use crate::event_webhook::dispatcher::EventDispatcher;
use crate::jobs::Queue;
use crate::jobs::runner::claim_loop;
use crate::jobs::store::{
    ClaimMode, JobHandler, JobStore, QueueDepthRefresh, ensure_claim_support,
};
use crate::layer::handler::IndexLayerJobHandler;
use crate::registry::blob_store::{BlobStore, BlobStoreConfig};
use crate::registry::metadata_store::MetadataStore;
use crate::registry::repository_resolver::RepositoryResolver;
use crate::registry::{Registry, RegistryConfig};
use crate::replication::ReplicationJobHandler;
use crate::scan::handler::ScanJobHandler;

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
#[derive(Default)]
pub struct InProcessLoops {
    shutdown: CancellationToken,
    tracker: TaskTracker,
}

impl InProcessLoops {
    fn spawn(
        &self,
        job_store: &Arc<JobStore>,
        handler: Arc<dyn JobHandler>,
        queue: Queue,
        count: NonZeroUsize,
    ) {
        for handler in repeat_n(handler, count.get()) {
            self.tracker.spawn(claim_loop(
                job_store.clone(),
                handler,
                queue,
                self.shutdown.clone(),
            ));
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

/// Spawn the claim loops draining `job_store`. Replication drains only when a
/// downstream is configured: an always-empty queue would just storm the object
/// store with `LIST`s.
fn spawn_in_process_loops(
    job_store: &Arc<JobStore>,
    resolver: &Arc<RepositoryResolver>,
    blob_store: &Arc<BlobStore>,
    metadata_store: &Arc<MetadataStore>,
    event_dispatcher: Option<Arc<EventDispatcher>>,
    global: &GlobalConfig,
) -> InProcessLoops {
    let loops = InProcessLoops::default();

    // Any image may be indexed on demand, so the queue always drains.
    loops.spawn(
        job_store,
        Arc::new(IndexLayerJobHandler::new(
            blob_store.clone(),
            metadata_store.clone(),
            global.index_limits(),
        )),
        Queue::Index,
        global.max_concurrent_index_jobs,
    );
    loops.spawn(
        job_store,
        Arc::new(CacheFillJobHandler::new(
            resolver.clone(),
            blob_store.clone(),
            metadata_store.clone(),
            event_dispatcher,
        )),
        Queue::Cache,
        global.max_concurrent_cache_jobs,
    );

    let any_downstream = resolver
        .values()
        .any(|repository| !repository.replication.is_empty());
    if any_downstream {
        // Mesh cycles terminate: only state-changing writes dispatch, and
        // receiver-side no-op suppression stops any remaining replays.
        loops.spawn(
            job_store,
            Arc::new(ReplicationJobHandler::new(
                resolver.clone(),
                blob_store.clone(),
                metadata_store.clone(),
            )),
            Queue::Replication,
            global.max_concurrent_replication_jobs,
        );
    }

    loops
}

/// Build the runtime `Registry` and the job queue it enqueues into. With
/// `[global.job_queue]` that queue is durable and draining it stays
/// `angos worker`'s job; without it the server drains an in-process queue
/// through loops it owns.
pub async fn build_registry(
    config: &Configuration,
    auth_cache: &Arc<Cache>,
) -> Result<BuiltRegistry, Error> {
    if matches!(config.registry_storage, RegistryStorageConfig::Inherit)
        && matches!(config.blob_store, BlobStoreConfig::S3(_))
    {
        info!("Auto-configuring S3 metadata-store from blob-store");
    }
    let StoreContext {
        blob_store: blob_backend,
        metadata_store,
        repositories,
    } = bootstrap::build_context(config, auth_cache).await?;

    let event_dispatcher = EventDispatcher::from_config(config)?;

    // With [global.job_queue] the jobs survive a restart and `angos worker`
    // drains them; without it the queue lives beside the metadata store and
    // the loops spawned here drain it until the server stops them.
    let (job_store, depth_refresh, in_process_loops) =
        if let Some(jq_config) = &config.global.job_queue {
            let claim_mode = ensure_claim_support(&metadata_store).await?;
            let job_store = Arc::new(JobStore::with_retry_policy(
                &metadata_store,
                "server",
                claim_mode,
                jq_config,
            ));
            let refresh = QueueDepthRefresh {
                store: job_store.clone(),
                period: Duration::from_secs(jq_config.pending_refresh_interval_secs),
                ready_horizon_secs: jq_config.pending_ready_horizon_secs,
            };
            (job_store, Some(refresh), InProcessLoops::default())
        } else {
            // Atomic mode: in-process draining runs no startup probe to pick one.
            let job_store = Arc::new(JobStore::new(
                &metadata_store,
                "in-process",
                ClaimMode::Atomic,
            ));
            let loops = spawn_in_process_loops(
                &job_store,
                &repositories,
                &blob_backend,
                &metadata_store,
                event_dispatcher.clone(),
                &config.global,
            );
            (job_store, None, loops)
        };

    let registry_config = RegistryConfig {
        update_pull_time: config.global.update_pull_time,
        redirect: config.global.redirect,
        max_manifest_size_bytes: config.global.max_manifest_size_bytes(),
        max_blob_size_bytes: config.global.max_blob_size.as_u64(),
        blob_stream_frame_size: config.global.blob_stream_frame_size_bytes(),
        validate_manifest_references: !config.global.allow_missing_manifest_references,
        global_immutable_tags: config.global.immutable_tags,
        global_immutable_tags_exclusions: config.global.immutable_tags_exclusions.clone(),
        listing_read_concurrency: config.global.listing_read_concurrency,
        revision_cache_entries: config.global.revision_cache_entries,
        namespace_counts: config.global.namespace_counts,
        event_dispatcher,
        ..RegistryConfig::new(job_store.clone())
    };
    let registry = Registry::new(
        blob_backend.clone(),
        metadata_store.clone(),
        repositories,
        registry_config,
    );
    // The scan handler pushes reports through the registry, so its loops can
    // only start once the registry exists.
    if let Some(scan) = &config.global.scan
        && config.global.job_queue.is_none()
    {
        in_process_loops.spawn(
            &job_store,
            Arc::new(ScanJobHandler::new(
                registry.clone(),
                blob_backend,
                metadata_store,
                scan,
            )?),
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
    use std::sync::Arc;
    use std::time::Duration;

    use tempfile::TempDir;
    use tokio::time::{sleep, timeout};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use angos_oci::header::DOCKER_CONTENT_DIGEST;
    use angos_oci::{Namespace, Tag};
    use angos_oci_client::RegistryClient;

    use super::{InProcessLoops, spawn_in_process_loops};
    use crate::configuration::GlobalConfig;
    use crate::jobs::Queue;
    use crate::jobs::store::{ClaimMode, JobEnvelope, JobStore};
    use crate::metrics_provider::initialize_metrics;
    use crate::registry::blob_store::BlobStore;
    use crate::registry::manifest::DispatchTarget;
    use crate::registry::metadata_store::MetadataStore;
    use crate::registry::test_utils::{
        FsTestStack, downstream_client, fs_test_stack, repository_with_downstream,
        repository_with_replication, seed_manifest, single_repo_resolver,
    };
    use crate::registry::{Registry, RegistryConfig, Repository};
    use crate::replication::REPLICATION_PUSH_MANIFEST_KIND;
    use crate::test_fixtures::mocks::mount_blob_upload_accepted;

    const NAMESPACE: &str = "nginx";
    const REPO: &str = "nginx";

    /// A registry whose queue the server's own loops drain, as it is wired
    /// without `[global.job_queue]`.
    struct InProcessStack {
        registry: Arc<Registry>,
        blob_store: Arc<BlobStore>,
        job_store: Arc<JobStore>,
        metadata_store: Arc<MetadataStore>,
        _loops: InProcessLoops,
        _dir: TempDir,
    }

    fn build_stack(repository: Repository) -> InProcessStack {
        initialize_metrics();
        let FsTestStack {
            dir,
            metadata_store,
            blob_store,
        } = fs_test_stack();
        let resolver = single_repo_resolver(REPO, repository);
        let job_store: Arc<JobStore> = Arc::new(JobStore::new(
            &metadata_store,
            "in-process",
            ClaimMode::Atomic,
        ));
        let loops = spawn_in_process_loops(
            &job_store,
            &resolver,
            &blob_store,
            &metadata_store,
            None,
            &GlobalConfig::default(),
        );
        let registry = Registry::new(
            blob_store.clone(),
            metadata_store.clone(),
            resolver,
            RegistryConfig::new(job_store.clone()),
        );
        InProcessStack {
            registry,
            blob_store,
            job_store,
            metadata_store,
            _loops: loops,
            _dir: dir,
        }
    }

    /// The drained job runs the whole push pipeline (HEAD before PUT for each
    /// blob, then the manifest) against a wiremock downstream.
    #[tokio::test]
    async fn the_in_process_loops_drain_a_replication_push() {
        let mock_server = MockServer::start().await;
        let client: Arc<RegistryClient> = downstream_client(&mock_server.uri());
        let stack = build_stack(repository_with_downstream(REPO, client));
        let namespace = Namespace::new(NAMESPACE).unwrap();

        let (manifest_digest, config_digest, layer_digest) =
            seed_manifest(&stack.blob_store, &stack.metadata_store, &namespace).await;

        mount_blob_upload_accepted(&mock_server, NAMESPACE, &[&config_digest, &layer_digest]).await;
        Mock::given(method("PUT"))
            .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
            .respond_with(
                ResponseTemplate::new(201)
                    .insert_header(DOCKER_CONTENT_DIGEST, manifest_digest.to_string().as_str()),
            )
            .expect(1..)
            .mount(&mock_server)
            .await;

        let repository = stack.registry.repository_for_namespace(&namespace);
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
                    &format!("{}.eu:nginx:v1", Queue::Replication),
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
