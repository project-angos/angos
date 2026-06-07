use std::{collections::HashMap, fmt, num::NonZeroUsize, sync::Arc, time::Duration};

use tokio::time::sleep;
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
pub use manifest::ParsedManifestDigests;
pub use manifest::{GetManifestResponse, parse_manifest_digests};
pub use repository::Repository;
pub use upload::{BlobMount, StartUploadResponse};

pub const DOCKER_CONTENT_DIGEST: &str = "Docker-Content-Digest";
pub const DOCKER_UPLOAD_UUID: &str = "Docker-Upload-UUID";
pub const OCI_SUBJECT: &str = "OCI-Subject";
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
    command::worker::runner::execute_one,
    configuration::{
        RegexPattern,
        global::{DEFAULT_MAX_CONCURRENT_CACHE_JOBS, DEFAULT_MAX_CONCURRENT_REPLICATION_JOBS},
    },
    oci::{Digest, Namespace},
    registry::{
        blob_store::BlobStore,
        cache_job_handler::{CACHE_QUEUE, CacheJobHandler},
        job_store::{JobHandler, JobStore},
        metadata_store::MetadataStore,
        repository_resolver::RepositoryResolver,
    },
    replication::{REPLICATION_QUEUE, ReplicationJobHandler},
};
use angos_tx_engine::lock::LockSession;

#[allow(clippy::struct_excessive_bools)]
pub struct RegistryConfig {
    pub update_pull_time: bool,
    pub enable_blob_redirect: bool,
    pub enable_manifest_redirect: bool,
    pub global_immutable_tags: bool,
    pub global_immutable_tags_exclusions: Vec<RegexPattern>,
    pub max_manifest_size_bytes: usize,
    /// When set, the registry routes all cache-fill and replication jobs through
    /// this pre-built queue (typically the durable backend wired in `server setup`).
    /// When absent, an engine-backed in-process queue is constructed
    /// automatically. The choice is made once at startup; no runtime switching.
    pub job_queue: Option<Arc<JobStore>>,
    /// Number of in-process cache-fill jobs that may run in parallel. Only
    /// consulted when `job_queue` is `None`; durable deployments use the
    /// equivalent worker-side setting instead.
    pub max_concurrent_cache_jobs: NonZeroUsize,
    /// Number of in-process replication-push jobs that may run in parallel.
    /// Only consulted when `job_queue` is `None`; durable deployments drain the
    /// replication queue with `angos worker --queue replication` instead.
    pub max_concurrent_replication_jobs: NonZeroUsize,
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
            job_queue: None,
            max_concurrent_cache_jobs: DEFAULT_MAX_CONCURRENT_CACHE_JOBS,
            max_concurrent_replication_jobs: DEFAULT_MAX_CONCURRENT_REPLICATION_JOBS,
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
    global_immutable_tags: bool,
    global_immutable_tags_exclusions: Vec<RegexPattern>,
    max_manifest_size_bytes: usize,
}

impl fmt::Debug for Registry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Registry").finish()
    }
}

impl Registry {
    #[instrument(skip(blob_store, metadata_store, resolver, config))]
    pub fn new(
        blob_store: Arc<BlobStore>,
        metadata_store: Arc<MetadataStore>,
        resolver: Arc<RepositoryResolver>,
        config: RegistryConfig,
    ) -> Result<Self, Error> {
        let job_queue: Arc<JobStore> = if let Some(q) = config.job_queue {
            q
        } else {
            build_in_process_queue(
                &resolver,
                &blob_store,
                &metadata_store,
                config.max_concurrent_cache_jobs,
                config.max_concurrent_replication_jobs,
            )?
        };

        Ok(Self {
            update_pull_time: config.update_pull_time,
            enable_blob_redirect: config.enable_blob_redirect,
            enable_manifest_redirect: config.enable_manifest_redirect,
            blob_store,
            metadata_store,
            resolver,
            job_queue,
            global_immutable_tags: config.global_immutable_tags,
            global_immutable_tags_exclusions: config.global_immutable_tags_exclusions,
            max_manifest_size_bytes: config.max_manifest_size_bytes,
        })
    }

    pub async fn flush_pending_writes(&self) {
        self.metadata_store.flush_access_times().await;
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
            .map(|r| r.name.clone())
            .unwrap_or_default()
    }

    /// Acquire the coarse `blob-data:{digest}` lock that serialises blob-data
    /// creation (upload completion) against reclamation (unreferenced delete)
    /// and against concurrent manifest pushes — which declare the same coarse
    /// lock on their link transactions. Without it, a delete can reclaim a
    /// content-addressed blob's bytes in the window between another repository
    /// granting a reference and validating its manifest, surfacing as
    /// `ManifestBlobUnknown`.
    ///
    /// Delegates to [`MetadataStore::acquire_blob_data_lock`], the canonical
    /// home for the key string and the lock domain (the metadata executor).
    pub async fn acquire_blob_data_lock(&self, digest: &Digest) -> Result<LockSession, Error> {
        self.metadata_store
            .acquire_blob_data_lock(digest)
            .await
            .map_err(|e| Error::Internal(format!("blob-data lock acquire failed: {e}")))
    }
}

/// Construct the in-process job queue used when `[global.job_queue]` is absent.
///
/// Builds a [`JobStore`] over the registry's **shared** object store and
/// executor (via `blob_store.store`) and spawns two pools of claim-loop tasks
/// that drain it in-process on the SAME store: `cache_concurrency` tasks for the
/// cache-fill queue and `replication_concurrency` tasks for the replication
/// queue. Sharing the backend is required for correctness: cache-fill jobs
/// commit a transaction that moves staged upload bytes into blob-data and grants
/// metadata references, which only resolves against the store where those bytes
/// live; replication jobs read the local manifest/blob bytes off the same store
/// before pushing them downstream. Jobs are therefore persisted under the shared
/// store's `_jobs/` prefix (and are picked back up by the claim loops after a
/// restart) rather than discarded. The in-process replication drain is what lets
/// a single-process server self-replicate without a separate `angos worker`.
fn build_in_process_queue(
    resolver: &Arc<RepositoryResolver>,
    blob_store: &Arc<BlobStore>,
    metadata_store: &Arc<MetadataStore>,
    cache_concurrency: NonZeroUsize,
    replication_concurrency: NonZeroUsize,
) -> Result<Arc<JobStore>, Error> {
    // Share the registry's object store and transaction executor (the same
    // handle the blob/metadata stores use). The cache-fill handler stages bytes
    // into the blob store and returns a transaction that moves them into
    // blob-data and grants metadata references; that transaction must commit
    // against the backend where the bytes were staged. A private object store
    // here would make those mutations fail with `NotFound`
    // (see doc/reviews/20260603-in-process-cache-fill-broken.md).
    let store = Arc::clone(&blob_store.store);
    let job_store: Arc<JobStore> = Arc::new(JobStore::new(store, "in-process"));

    let cache_handler: Arc<dyn JobHandler> = Arc::new(CacheJobHandler::new(
        resolver.clone(),
        blob_store.clone(),
        metadata_store.clone(),
    ));

    for _ in 0..cache_concurrency.get() {
        tokio::spawn(in_process_claim_loop(
            job_store.clone(),
            cache_handler.clone(),
            CACHE_QUEUE,
        ));
    }

    // Replication handler over the same shared store: it reads local manifest /
    // blob bytes and pushes them to each downstream `RegistryClient`. Converged
    // replays are suppressed sender-side — only state-changing writes dispatch,
    // and receiver-side no-op suppression terminates any remaining mesh cycles.
    let replication_handler: Arc<dyn JobHandler> = Arc::new(
        ReplicationJobHandler::builder()
            .resolver(resolver.clone())
            .blob_store(blob_store.clone())
            .metadata_store(metadata_store.clone())
            .build()
            .map_err(|e| Error::Internal(format!("failed to build replication handler: {e}")))?,
    );

    for _ in 0..replication_concurrency.get() {
        tokio::spawn(in_process_claim_loop(
            job_store.clone(),
            replication_handler.clone(),
            REPLICATION_QUEUE,
        ));
    }

    Ok(job_store)
}

/// Single claim-loop task for the in-process pool. Mirrors the per-worker
/// loop in `command::worker::command::Command::run` but with a fixed 10 ms
/// idle tick so small test suites stay snappy. `queue` selects which queue this
/// loop drains (and `handler` must be the handler bound to that queue).
async fn in_process_claim_loop(
    consumer: Arc<JobStore>,
    handler: Arc<dyn JobHandler>,
    queue: &'static str,
) {
    loop {
        match consumer.claim_one(queue).await {
            Err(_) => sleep(Duration::from_millis(100)).await,
            Ok(claim_outcome) => match claim_outcome.claimed {
                None => sleep(claim_outcome.idle_sleep(Duration::from_millis(10))).await,
                Some(claimed) => execute_one(consumer.as_ref(), handler.as_ref(), claimed).await,
            },
        }
    }
}

#[cfg(test)]
mod in_process_replication_tests {
    use std::{collections::HashMap, sync::Arc, time::Duration};

    use serde_json::json;
    use tempfile::TempDir;
    use tokio::time::{sleep, timeout};
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use angos_storage::{ObjectStore, fs::Backend as StorageFsBackend};

    use crate::{
        cache,
        oci::{Digest, Namespace},
        policy::{RetentionPolicy, RetentionPolicyConfig, SystemClock},
        registry::{
            DOCKER_CONTENT_DIGEST, Registry, RegistryConfig, Repository,
            blob_store::BlobStore,
            manifest::DEFAULT_MAX_MANIFEST_SIZE_BYTES,
            metadata_store::{LinkOperation, MetadataStore, link_kind::LinkKind},
            repository_resolver::RepositoryResolver,
            test_utils::{build_store, build_test_fs_executor, put_blob_direct},
        },
        registry_client::RegistryClient,
        replication::{REPLICATION_PUSH_MANIFEST_KIND, ReplicationDownstream, ReplicationMode},
    };

    const NAMESPACE: &str = "nginx";
    const REPO: &str = "nginx";
    const DOWNSTREAM: &str = "eu-region";

    fn downstream_client(uri: &str) -> Arc<RegistryClient> {
        let backend = cache::Config::Memory.to_backend().unwrap();
        Arc::new(
            RegistryClient::builder()
                .url(uri.to_string())
                .client(reqwest::Client::new())
                .cache(backend)
                .max_manifest_size_bytes(DEFAULT_MAX_MANIFEST_SIZE_BYTES)
                .build()
                .unwrap(),
        )
    }

    fn repository_with_downstream(client: Arc<RegistryClient>) -> Repository {
        Repository {
            name: REPO.to_string(),
            upstreams: Vec::new(),
            replication: vec![
                ReplicationDownstream::builder()
                    .name(DOWNSTREAM.to_string())
                    .registry_client(client)
                    .mode(ReplicationMode::EventReconcile)
                    .namespace_filter(Vec::new())
                    .max_concurrent_pushes(4)
                    .build()
                    .unwrap(),
            ],
            retention_policy: RetentionPolicy::new(
                &RetentionPolicyConfig::default(),
                Arc::new(SystemClock),
            ),
            immutable_tags: false,
            immutable_tags_exclusions: Vec::new(),
        }
    }

    /// Build a `Registry` whose in-process queue is constructed automatically
    /// (no `[global.job_queue]`), so both the cache and replication claim loops
    /// are spawned on the shared store. Returns the registry plus the shared
    /// blob/metadata stores so the test can seed local state.
    fn build_registry(
        root: &str,
        client: Arc<RegistryClient>,
    ) -> (Registry, Arc<BlobStore>, Arc<MetadataStore>) {
        let object: Arc<dyn ObjectStore> =
            Arc::new(StorageFsBackend::builder().root_dir(root).build().unwrap());
        let executor = build_test_fs_executor(root, false);
        let store = build_store(object, executor);
        let metadata_store = Arc::new(
            MetadataStore::builder()
                .store(store.clone())
                .link_cache_ttl(0)
                .access_time_debounce_secs(0)
                .build()
                .unwrap(),
        );
        let blob_store = Arc::new(BlobStore::builder().store(store.clone()).build().unwrap());

        let mut repositories = HashMap::new();
        repositories.insert(REPO.to_string(), repository_with_downstream(client));
        let resolver = Arc::new(RepositoryResolver::new(Arc::new(repositories)).unwrap());

        // No `job_queue` => `build_in_process_queue` runs, spawning the
        // replication claim loops over the shared store.
        let config = RegistryConfig::default();
        let registry =
            Registry::new(blob_store.clone(), metadata_store.clone(), resolver, config).unwrap();

        (registry, blob_store, metadata_store)
    }

    /// Seed a config blob, a layer blob, a manifest referencing both, and a `v1`
    /// tag link. Returns the manifest + referenced blob digests.
    async fn seed_manifest(
        store: &Arc<angos_tx_engine::store::Store>,
        metadata_store: &Arc<MetadataStore>,
    ) -> (Digest, Digest, Digest) {
        let config_bytes = br#"{"config":true}"#.to_vec();
        let layer_bytes = b"layer-bytes".to_vec();

        let config_digest = put_blob_direct(store, &config_bytes).await;
        let layer_digest = put_blob_direct(store, &layer_bytes).await;

        let manifest = json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "digest": config_digest.to_string(),
                "size": config_bytes.len(),
            },
            "layers": [{
                "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                "digest": layer_digest.to_string(),
                "size": layer_bytes.len(),
            }],
        });
        let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
        let manifest_digest = put_blob_direct(store, &manifest_bytes).await;

        metadata_store
            .update_links(
                NAMESPACE,
                &[
                    LinkOperation::create(LinkKind::Tag("v1".to_string()), manifest_digest.clone()),
                    LinkOperation::create(
                        LinkKind::Config(config_digest.clone()),
                        config_digest.clone(),
                    ),
                    LinkOperation::create(
                        LinkKind::Layer(layer_digest.clone()),
                        layer_digest.clone(),
                    ),
                ],
            )
            .await
            .unwrap();

        (manifest_digest, config_digest, layer_digest)
    }

    /// End-to-end: a replication push job enqueued onto the registry's shared
    /// in-process queue is drained by the spawned replication claim loop, which
    /// runs the push pipeline (HEAD-before-PUT blobs + PUT manifest) against the
    /// wiremock downstream.
    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn in_process_loop_drains_replication_push_job() {
        crate::metrics_provider::init_for_tests();
        let mock_server = MockServer::start().await;

        let dir = TempDir::new().unwrap();
        let root = dir.path().to_str().unwrap();
        let client = downstream_client(&mock_server.uri());
        let (registry, blob_store, metadata_store) = build_registry(root, client);

        let (manifest_digest, config_digest, layer_digest) =
            seed_manifest(&blob_store.store, &metadata_store).await;

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

        let namespace = Namespace::new(NAMESPACE).unwrap();

        // Enqueue via the production event path; the in-process replication loop
        // claims and executes it asynchronously.
        let repository = registry.resolver.resolve(&namespace);
        registry
            .dispatch_replication(
                repository,
                &namespace,
                REPLICATION_PUSH_MANIFEST_KIND,
                Some("v1"),
                Some(&manifest_digest),
            )
            .await;

        // Poll the wiremock server until the manifest PUT lands (or time out).
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

        let inspector =
            crate::registry::job_store::JobStore::new(Arc::clone(&blob_store.store), "inspector");

        if !saw_put {
            let received = mock_server.received_requests().await.unwrap_or_default();
            let summary: Vec<String> = received
                .iter()
                .map(|r| format!("{} {}", r.method.as_str(), r.url.path()))
                .collect();
            let pending = inspector
                .count_pending(crate::replication::REPLICATION_QUEUE, 0)
                .await
                .unwrap_or(u64::MAX);
            let failed = inspector
                .list_failed_page(crate::replication::REPLICATION_QUEUE, 16, None)
                .await
                .map_or(usize::MAX, |(keys, _)| keys.len());
            panic!(
                "in-process replication loop must drain the job and PUT the manifest \
                 downstream; received requests: {summary:?}; pending={pending}; failed={failed}"
            );
        }

        // The PUT landed; now assert the job actually COMPLETES — the queue must
        // drain to zero pending (and zero dead-lettered), proving the loop calls
        // `complete` rather than leaving the job stuck/retrying.
        let drained = timeout(Duration::from_secs(10), async {
            loop {
                let pending = inspector
                    .count_pending(crate::replication::REPLICATION_QUEUE, 0)
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
            .list_failed_page(crate::replication::REPLICATION_QUEUE, 16, None)
            .await
            .map_or(usize::MAX, |(keys, _)| keys.len());
        assert!(
            drained,
            "the replication queue must drain to zero pending after a successful push"
        );
        assert_eq!(failed, 0, "a successful push must not dead-letter the job");
    }
}
