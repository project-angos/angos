use std::{
    collections::HashMap,
    fmt,
    num::NonZeroUsize,
    sync::{Arc, Weak},
    time::Duration,
};

use tokio::{select, time::sleep};
use tokio_util::sync::CancellationToken;
use tracing::instrument;

pub mod blob;
pub mod blob_ownership;
pub mod blob_store;
pub mod cache_job_handler;
pub mod content_discovery;
mod error;
#[cfg(test)]
mod event_emission_tests;
mod ext;
mod headers;
pub mod job_runner;
pub mod job_store;
pub mod manifest;
pub mod metadata_store;
pub mod pagination;
mod path_builder;
pub mod repository;
pub mod repository_resolver;
pub mod s3_connection;
#[cfg(test)]
pub mod test_utils;
pub mod upload;
pub mod version;

pub use blob::{BlobRange, GetBlobResponse};
pub use error::Error;
pub use headers::{HeaderMap, ResponseHeaders};
pub use manifest::{GetManifestResponse, ParsedManifestDigests, parse_manifest_digests};
pub use repository::Repository;
pub use upload::{BlobMount, StartUploadResponse};

pub const DOCKER_CONTENT_DIGEST: &str = "Docker-Content-Digest";
pub const DOCKER_UPLOAD_UUID: &str = "Docker-Upload-UUID";
pub const OCI_SUBJECT: &str = "OCI-Subject";
pub const OCI_TAG: &str = "OCI-Tag";
pub const APPLICATION_JSON: &str = "application/json";

/// Response for endpoints whose body is a JSON (or JSON-flavoured) payload.
///
/// The registry is the sole authority on both the headers (Content-Type,
/// Link, OCI-Filters-Applied, ...) and the serialized body bytes. Handlers
/// attach the headers verbatim and pass the body through.
pub struct JsonResponse {
    pub headers: HashMap<&'static str, String>,
    pub body: Vec<u8>,
}

pub use crate::policy::AccessPolicy;
use crate::{
    cache,
    configuration::{
        RegexPattern,
        global::{DEFAULT_MAX_CONCURRENT_CACHE_JOBS, DEFAULT_MAX_CONCURRENT_REPLICATION_JOBS},
    },
    event_webhook::{dispatcher::EventDispatcher, event::Event},
    oci::Namespace,
    registry::{
        blob_store::BlobStore,
        cache_job_handler::CacheJobHandler,
        job_runner::execute_one,
        job_store::{JobHandler, JobStore, Queue},
        metadata_store::MetadataStore,
        repository_resolver::RepositoryResolver,
    },
    replication::ReplicationJobHandler,
};

#[allow(clippy::struct_excessive_bools)]
pub struct RegistryConfig {
    pub update_pull_time: bool,
    pub enable_blob_redirect: bool,
    pub enable_manifest_redirect: bool,
    pub global_immutable_tags: bool,
    pub global_immutable_tags_exclusions: Vec<RegexPattern>,
    pub max_manifest_size_bytes: usize,
    pub max_blob_size_bytes: u64,
    /// When `true`, a client manifest push is rejected with
    /// `MANIFEST_BLOB_UNKNOWN` if any referenced blob or child manifest is not
    /// present and owned by the target namespace. When `false`, the push is
    /// accepted but the unowned references are left dangling rather than granted,
    /// so they resolve as unknown on a later pull and a namespace never gains
    /// read access to content it did not push. `subject` references are exempt
    /// either way. Pull-through cache-fill writes are always trusted, independent
    /// of this flag.
    pub validate_manifest_references: bool,
    /// When set, the registry routes all cache-fill and replication jobs through
    /// this pre-built queue (typically the durable backend wired in `server setup`).
    /// When absent, an engine-backed in-process queue is constructed
    /// automatically. The choice is made once at startup; no runtime switching.
    pub job_queue: Option<Arc<JobStore>>,
    /// Number of in-process cache-fill jobs that may run in parallel. Only
    /// consulted when `job_queue` is `None`; durable deployments use the
    /// equivalent worker-side setting instead.
    pub max_concurrent_cache_jobs: NonZeroUsize,
    /// Parallel in-process replication-push jobs. Only consulted when
    /// `job_queue` is `None`; durable deployments use `angos worker` instead.
    pub max_concurrent_replication_jobs: NonZeroUsize,
    /// Webhook dispatcher through which operations deliver their events.
    /// `None` (the default) disables event delivery entirely.
    pub event_dispatcher: Option<Arc<EventDispatcher>>,
}

impl Default for RegistryConfig {
    fn default() -> Self {
        Self {
            update_pull_time: false,
            enable_blob_redirect: true,
            enable_manifest_redirect: true,
            global_immutable_tags: false,
            global_immutable_tags_exclusions: Vec::new(),
            max_manifest_size_bytes: manifest::DEFAULT_MAX_MANIFEST_SIZE_BYTES,
            max_blob_size_bytes: upload::DEFAULT_MAX_BLOB_SIZE_BYTES,
            // Strict by default at the struct level so test/internal registries
            // built from `RegistryConfig::default()` keep validating; the server
            // opts into the permissive production default via `[global]`.
            validate_manifest_references: true,
            job_queue: None,
            max_concurrent_cache_jobs: DEFAULT_MAX_CONCURRENT_CACHE_JOBS,
            max_concurrent_replication_jobs: DEFAULT_MAX_CONCURRENT_REPLICATION_JOBS,
            event_dispatcher: None,
        }
    }
}

impl RegistryConfig {
    pub fn update_pull_time(mut self, enabled: bool) -> Self {
        self.update_pull_time = enabled;
        self
    }

    pub fn enable_blob_redirect(mut self, enabled: bool) -> Self {
        self.enable_blob_redirect = enabled;
        self
    }

    pub fn enable_manifest_redirect(mut self, enabled: bool) -> Self {
        self.enable_manifest_redirect = enabled;
        self
    }

    pub fn global_immutable_tags(mut self, enabled: bool) -> Self {
        self.global_immutable_tags = enabled;
        self
    }

    pub fn global_immutable_tags_exclusions(mut self, exclusions: Vec<RegexPattern>) -> Self {
        self.global_immutable_tags_exclusions = exclusions;
        self
    }

    pub fn max_manifest_size_bytes(mut self, limit: usize) -> Self {
        self.max_manifest_size_bytes = limit;
        self
    }

    pub fn max_blob_size_bytes(mut self, limit: u64) -> Self {
        self.max_blob_size_bytes = limit;
        self
    }

    pub fn validate_manifest_references(mut self, enabled: bool) -> Self {
        self.validate_manifest_references = enabled;
        self
    }

    pub fn job_queue(mut self, queue: Arc<JobStore>) -> Self {
        self.job_queue = Some(queue);
        self
    }

    pub fn max_concurrent_cache_jobs(mut self, value: NonZeroUsize) -> Self {
        self.max_concurrent_cache_jobs = value;
        self
    }

    pub fn max_concurrent_replication_jobs(mut self, value: NonZeroUsize) -> Self {
        self.max_concurrent_replication_jobs = value;
        self
    }

    pub fn event_dispatcher(mut self, dispatcher: Option<Arc<EventDispatcher>>) -> Self {
        self.event_dispatcher = dispatcher;
        self
    }
}

#[allow(clippy::struct_excessive_bools)]
pub struct Registry {
    blob_store: Arc<BlobStore>,
    metadata_store: Arc<MetadataStore>,
    resolver: Arc<RepositoryResolver>,
    enable_blob_redirect: bool,
    enable_manifest_redirect: bool,
    update_pull_time: bool,
    job_queue: Arc<JobStore>,
    /// Cancels the in-process claim loops when this `Registry` is dropped.
    /// `None` when a durable `[global.job_queue]` is configured (no in-process
    /// loops to cancel).
    in_process_shutdown: Option<CancellationToken>,
    global_immutable_tags: bool,
    global_immutable_tags_exclusions: Vec<RegexPattern>,
    max_manifest_size_bytes: usize,
    max_blob_size_bytes: u64,
    validate_manifest_references: bool,
    event_dispatcher: Option<Arc<EventDispatcher>>,
}

impl fmt::Debug for Registry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Registry").finish()
    }
}

impl Registry {
    /// Returns an `Arc` because the in-process job queue's cache handler
    /// holds a `Weak` self-reference, so cache fills run (and emit events)
    /// through the registry itself.
    #[instrument(skip(blob_store, metadata_store, resolver, config))]
    pub fn new(
        blob_store: Arc<BlobStore>,
        metadata_store: Arc<MetadataStore>,
        resolver: Arc<RepositoryResolver>,
        config: RegistryConfig,
    ) -> Result<Arc<Self>, Error> {
        Ok(Arc::new_cyclic(|registry| {
            let (job_queue, in_process_shutdown): (Arc<JobStore>, Option<CancellationToken>) =
                if let Some(q) = config.job_queue {
                    (q, None)
                } else {
                    let (q, shutdown) = build_in_process_queue(
                        &resolver,
                        &blob_store,
                        &metadata_store,
                        config.max_concurrent_cache_jobs,
                        config.max_concurrent_replication_jobs,
                        registry.clone(),
                    );
                    (q, Some(shutdown))
                };

            Self {
                update_pull_time: config.update_pull_time,
                enable_blob_redirect: config.enable_blob_redirect,
                enable_manifest_redirect: config.enable_manifest_redirect,
                blob_store,
                metadata_store,
                resolver,
                job_queue,
                in_process_shutdown,
                global_immutable_tags: config.global_immutable_tags,
                global_immutable_tags_exclusions: config.global_immutable_tags_exclusions,
                max_manifest_size_bytes: config.max_manifest_size_bytes,
                max_blob_size_bytes: config.max_blob_size_bytes,
                validate_manifest_references: config.validate_manifest_references,
                event_dispatcher: config.event_dispatcher,
            }
        }))
    }

    /// A cache-fill job handler backed by this registry, for external drains
    /// (`angos worker`).
    pub fn cache_job_handler(self: &Arc<Self>) -> Arc<dyn JobHandler> {
        Arc::new(CacheJobHandler::new(Arc::downgrade(self)))
    }

    pub async fn flush_pending_writes(&self) {
        self.metadata_store.flush_access_times().await;
    }

    #[cfg(test)]
    pub fn has_event_dispatcher(&self) -> bool {
        self.event_dispatcher.is_some()
    }

    /// Delivers `events` to the configured webhooks, attempting every event
    /// even if an earlier delivery fails; the first error is returned once
    /// all have been attempted. Operations call this before they perform the
    /// action, so a performed action can never go unnotified (at-least-once):
    /// an action failing after emission leaves a false-positive notification
    /// of its intent. With no dispatcher configured this is a no-op.
    pub async fn dispatch_events(&self, events: &[Event]) -> Result<(), Error> {
        let Some(dispatcher) = &self.event_dispatcher else {
            return Ok(());
        };
        let mut first_error: Option<Error> = None;
        for event in events {
            if let Err(error) = dispatcher.dispatch(event).await
                && first_error.is_none()
            {
                first_error = Some(Error::EventDelivery(error.to_string()));
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// Flushes pending writes and drains in-flight async webhook deliveries
    /// to completion.
    pub async fn shutdown(&self) {
        self.flush_pending_writes().await;
        if let Some(dispatcher) = &self.event_dispatcher {
            dispatcher.shutdown().await;
        }
    }

    pub async fn check_ready(&self) -> Result<(), Error> {
        self.metadata_store
            .list_namespaces(1, None)
            .await
            .map_err(|e| Error::Internal(format!("storage backend not ready: {e}")))?;
        Ok(())
    }

    #[instrument]
    pub fn get_repository_for_namespace(
        &self,
        namespace: &Namespace,
    ) -> Result<&Repository, Error> {
        self.resolver.resolve(namespace).ok_or(Error::NameUnknown)
    }

    /// Resolves the configured repository name for a namespace, or empty string
    /// if none matches. Used when constructing events where the event's
    /// `repository` field should reflect the configured repository scope.
    pub fn repository_name_for(&self, namespace: &Namespace) -> String {
        self.get_repository_for_namespace(namespace)
            .map(|r| r.name.to_string())
            .unwrap_or_default()
    }
}

/// Construct the in-process job queue used when `[global.job_queue]` is absent.
///
/// The queue runs on the metadata store's coordinated `Store`: job records
/// under its `_jobs/` prefix are committed through its transaction executor and
/// survive restarts. The cache-fill handler resolves blob bytes and metadata
/// grants directly through their own stores as idempotent work, so the queue
/// needs no co-location with the blob backend.
fn build_in_process_queue(
    resolver: &Arc<RepositoryResolver>,
    blob_store: &Arc<BlobStore>,
    metadata_store: &Arc<MetadataStore>,
    cache_concurrency: NonZeroUsize,
    replication_concurrency: NonZeroUsize,
    registry: Weak<Registry>,
) -> (Arc<JobStore>, CancellationToken) {
    let job_store: Arc<JobStore> =
        Arc::new(JobStore::new(metadata_store.store_arc(), "in-process"));

    let cache_handler: Arc<dyn JobHandler> = Arc::new(CacheJobHandler::new(registry));

    // Drain replication only when a downstream is configured: with none, the
    // queue stays empty forever, so its loops would just storm the object store
    // with `LIST`s. (Replication jobs left from a removed downstream are reaped
    // by `scrub --replication-orphans`, not drained here.) Build the fallible
    // handler before spawning any loop so an error cannot leak a cache loop.
    let any_downstream = resolver
        .keys()
        .filter_map(|name| resolver.get(name))
        .any(|repository| !repository.replication.is_empty());
    let replication_handler: Option<Arc<dyn JobHandler>> = if any_downstream {
        // Mesh cycles terminate: only state-changing writes dispatch, and
        // receiver-side no-op suppression stops any remaining replays.
        Some(Arc::new(ReplicationJobHandler::new(
            resolver.clone(),
            blob_store.clone(),
            metadata_store.clone(),
        )))
    } else {
        None
    };

    // One shared token cancels every loop when the owning `Registry` is dropped.
    let shutdown = CancellationToken::new();

    for _ in 0..cache_concurrency.get() {
        tokio::spawn(in_process_claim_loop(
            job_store.clone(),
            cache_handler.clone(),
            Queue::Cache,
            shutdown.clone(),
        ));
    }

    if let Some(replication_handler) = replication_handler {
        for _ in 0..replication_concurrency.get() {
            tokio::spawn(in_process_claim_loop(
                job_store.clone(),
                replication_handler.clone(),
                Queue::Replication,
                shutdown.clone(),
            ));
        }
    }

    (job_store, shutdown)
}

/// Idle poll interval for the in-process claim loops. Production polls once a
/// second (matching the durable `angos worker`) so an empty queue does not
/// storm the object store with `LIST`s; tests poll fast so the suite stays
/// snappy.
#[cfg(not(test))]
const IN_PROCESS_IDLE_POLL: Duration = Duration::from_secs(1);
#[cfg(test)]
const IN_PROCESS_IDLE_POLL: Duration = Duration::from_millis(10);

/// Single claim-loop task for the in-process pool. Mirrors the per-worker
/// loop in `command::worker::command::Command::run`, idling at
/// [`IN_PROCESS_IDLE_POLL`]. `handler` must be the handler bound to `queue`.
///
/// Cancellation races only the claim, so an already-claimed job runs to
/// completion rather than being interrupted mid-execute.
async fn in_process_claim_loop(
    consumer: Arc<JobStore>,
    handler: Arc<dyn JobHandler>,
    queue: Queue,
    shutdown: CancellationToken,
) {
    loop {
        select! {
            () = shutdown.cancelled() => return,
            outcome = consumer.claim_one(queue) => match outcome {
                // `claim_one` self-throttles on a backend error; nothing to do.
                Err(_) => {}
                Ok(claim_outcome) => match claim_outcome.claimed {
                    None => sleep(claim_outcome.idle_sleep(IN_PROCESS_IDLE_POLL)).await,
                    Some(claimed) => {
                        execute_one(consumer.as_ref(), handler.as_ref(), claimed).await;
                    }
                },
            },
        }
    }
}

impl Drop for Registry {
    fn drop(&mut self) {
        // Claim loops hold their own `Arc<JobStore>` clones, so only cancelling
        // the token stops them; leased durable jobs are re-claimed after restart.
        if let Some(shutdown) = &self.in_process_shutdown {
            shutdown.cancel();
        }
    }
}

#[cfg(test)]
mod in_process_replication_tests {
    use std::{sync::Arc, time::Duration};

    use tempfile::TempDir;
    use tokio::time::{sleep, timeout};
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use crate::{
        oci::{Namespace, Tag},
        registry::{
            DOCKER_CONTENT_DIGEST, Registry, RegistryConfig, Repository,
            blob_store::BlobStore,
            job_store::{JobEnvelope, JobStore, Queue},
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

    fn repository_without_downstream() -> Repository {
        repository_with_replication(REPO, Vec::new())
    }

    /// Build a `Registry` with an automatic in-process queue (no
    /// `[global.job_queue]`) over `repository`, plus the shared stores for
    /// seeding local state.
    fn build_registry_with(
        repository: Repository,
    ) -> (Arc<Registry>, Arc<BlobStore>, Arc<MetadataStore>, TempDir) {
        let FsTestStack {
            dir,
            store: _,
            metadata_store,
            blob_store,
        } = fs_test_stack();
        let resolver = single_repo_resolver(REPO, repository);

        let config = RegistryConfig::default();
        let registry =
            Registry::new(blob_store.clone(), metadata_store.clone(), resolver, config).unwrap();

        (registry, blob_store, metadata_store, dir)
    }

    fn build_registry(
        client: Arc<RegistryClient>,
    ) -> (Arc<Registry>, Arc<BlobStore>, Arc<MetadataStore>, TempDir) {
        build_registry_with(repository_with_downstream(REPO, client))
    }

    /// The drained job runs the full push pipeline (HEAD-before-PUT blobs, PUT
    /// manifest) against a wiremock downstream.
    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn in_process_loop_drains_replication_push_job() {
        crate::metrics_provider::init_for_tests();
        let mock_server = MockServer::start().await;

        let client = downstream_client(&mock_server.uri());
        let (registry, _blob_store, metadata_store, _dir) = build_registry(client);
        let namespace = Namespace::new(NAMESPACE).unwrap();

        let (manifest_digest, config_digest, layer_digest) =
            seed_manifest(metadata_store.store(), &metadata_store, &namespace).await;

        // Downstream is missing both blobs (404 on HEAD) -> upload sequence runs.
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
        // The manifest PUT-by-tag is what we assert the loop reaches.
        Mock::given(method("PUT"))
            .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
            .respond_with(
                ResponseTemplate::new(201)
                    .insert_header(DOCKER_CONTENT_DIGEST, manifest_digest.to_string().as_str()),
            )
            .expect(1..)
            .mount(&mock_server)
            .await;

        // Enqueue via the production event path.
        let repository = registry.resolver.resolve(&namespace);
        let tag = Tag::new("v1").unwrap();
        registry
            .dispatch_replication(
                repository,
                &namespace,
                REPLICATION_PUSH_MANIFEST_KIND,
                Some(&tag),
                Some(&manifest_digest),
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

        let inspector = JobStore::new(metadata_store.store_arc(), "inspector");

        if !saw_put {
            let received = mock_server.received_requests().await.unwrap_or_default();
            let summary: Vec<String> = received
                .iter()
                .map(|r| format!("{} {}", r.method.as_str(), r.url.path()))
                .collect();
            let pending = inspector
                .count_pending(Queue::Replication, 0)
                .await
                .unwrap_or(u64::MAX);
            let failed = inspector
                .list_failed_page(Queue::Replication, 16, None)
                .await
                .map_or(usize::MAX, |(keys, _)| keys.len());
            panic!(
                "in-process replication loop must drain the job and PUT the manifest \
                 downstream; received requests: {summary:?}; pending={pending}; failed={failed}"
            );
        }

        // Zero pending proves the loop completed the job rather than retrying.
        let drained = timeout(Duration::from_secs(10), async {
            loop {
                let pending = inspector
                    .count_pending(Queue::Replication, 0)
                    .await
                    .unwrap_or(u64::MAX);
                if pending == 0 {
                    return true;
                }
                sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .unwrap_or(false);

        let failed = inspector
            .list_failed_page(Queue::Replication, 16, None)
            .await
            .map_or(usize::MAX, |(keys, _)| keys.len());
        assert!(
            drained,
            "the replication queue must drain to zero pending after a successful push"
        );
        assert_eq!(failed, 0, "a successful push must not dead-letter the job");
    }

    /// With no downstream configured, no replication loop is spawned, so a
    /// replication job is never claimed: it must stay pending rather than be
    /// drained (the loops would otherwise poll an always-empty queue forever).
    #[tokio::test]
    async fn no_replication_loop_when_no_downstream_configured() {
        crate::metrics_provider::init_for_tests();
        let (registry, _blob_store, _metadata_store, _dir) =
            build_registry_with(repository_without_downstream());

        registry
            .job_queue
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

        // A spawned loop idles at 10ms under cfg(test), so 500ms is many ticks:
        // if one existed it would have claimed the job by now.
        sleep(Duration::from_millis(500)).await;

        assert_eq!(
            registry
                .job_queue
                .count_pending(Queue::Replication, 0)
                .await
                .unwrap(),
            1,
            "no downstream means no replication loop, so the job must stay pending"
        );
    }
}
