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
pub use manifest::{GetManifestResponse, parse_manifest_digests};
pub use repository::Repository;
pub use upload::StartUploadResponse;

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
    configuration::{RegexPattern, global::DEFAULT_MAX_CONCURRENT_CACHE_JOBS},
    oci::{Digest, Namespace},
    registry::{
        blob_ownership::BlobOwnership,
        blob_store::{BlobStore, Error as BlobStoreError},
        cache_job_handler::{CACHE_QUEUE, CacheJobHandler},
        job_store::{JobHandler, JobStore},
        metadata_store::MetadataStore,
        repository_resolver::RepositoryResolver,
    },
};
use angos_storage::{MemoryObjectStore, ObjectStore};
use angos_tx_engine::{executor::build_executor, lock::LockStrategy};

#[allow(clippy::struct_excessive_bools)]
pub struct RegistryConfig {
    pub update_pull_time: bool,
    pub enable_blob_redirect: bool,
    pub enable_manifest_redirect: bool,
    pub global_immutable_tags: bool,
    pub global_immutable_tags_exclusions: Vec<RegexPattern>,
    pub max_manifest_size_bytes: usize,
    /// When set, the registry routes all cache-fill jobs through this
    /// pre-built queue (typically the durable backend wired in `server setup`).
    /// When absent, an engine-backed in-process queue is constructed
    /// automatically. The choice is made once at startup; no runtime switching.
    pub job_queue: Option<Arc<JobStore>>,
    /// Number of in-process cache-fill jobs that may run in parallel. Only
    /// consulted when `job_queue` is `None`; durable deployments use the
    /// equivalent worker-side setting instead.
    pub max_concurrent_cache_jobs: NonZeroUsize,
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
}

#[allow(clippy::struct_excessive_bools)]
pub struct Registry {
    blob_store: Arc<BlobStore>,
    metadata_store: Arc<MetadataStore>,
    resolver: Arc<RepositoryResolver>,
    enable_blob_redirect: bool,
    enable_manifest_redirect: bool,
    update_pull_time: bool,
    cache_queue: Arc<JobStore>,
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
        let cache_queue: Arc<JobStore> = if let Some(q) = config.job_queue {
            q
        } else {
            build_in_process_queue(
                &resolver,
                &blob_store,
                &metadata_store,
                config.max_concurrent_cache_jobs,
            )?
        };

        Ok(Self {
            update_pull_time: config.update_pull_time,
            enable_blob_redirect: config.enable_blob_redirect,
            enable_manifest_redirect: config.enable_manifest_redirect,
            blob_store,
            metadata_store,
            resolver,
            cache_queue,
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

    async fn delete_blob_data_if_unreferenced(&self, digest: &Digest) -> Result<(), Error> {
        let ownership = BlobOwnership::new(self.metadata_store.as_ref());
        if ownership.has_any_reference(digest).await? {
            return Ok(());
        }

        match self.blob_store.delete_blob(digest).await {
            Ok(()) | Err(BlobStoreError::BlobNotFound | BlobStoreError::ReferenceNotFound) => {
                Ok(())
            }
            Err(error) => Err(error.into()),
        }
    }
}

/// Construct the in-process job queue used when `[global.job_queue]` is absent.
///
/// Builds an engine-backed [`JobStore`] over a [`MemoryObjectStore`] with a
/// memory-backed lock and spawns a pool of `concurrency` claim-loop tasks.
/// Jobs survive as long as the process is alive; they are not durable across
/// restarts.
///
/// # Errors
///
/// Returns [`Error::Internal`] when the executor cannot be built (should never
/// happen with `LockStrategy::Memory`).
fn build_in_process_queue(
    resolver: &Arc<RepositoryResolver>,
    blob_store: &Arc<BlobStore>,
    metadata_store: &Arc<MetadataStore>,
    concurrency: NonZeroUsize,
) -> Result<Arc<JobStore>, Error> {
    let object_store: Arc<dyn ObjectStore> = Arc::new(MemoryObjectStore::new());

    let executor = build_executor(
        object_store.clone(),
        None,
        LockStrategy::Memory,
        None,
        false,
        false,
    )
    .map_err(|e| Error::Internal(format!("failed to build in-process job executor: {e}")))?;

    let job_store: Arc<JobStore> = Arc::new(JobStore::new(object_store, executor, "in-process"));

    let handler: Arc<dyn JobHandler> = Arc::new(CacheJobHandler::new(
        resolver.clone(),
        blob_store.clone(),
        metadata_store.clone(),
    ));

    for _ in 0..concurrency.get() {
        tokio::spawn(in_process_claim_loop(job_store.clone(), handler.clone()));
    }

    Ok(job_store)
}

/// Single claim-loop task for the in-process pool. Mirrors the per-worker
/// loop in `command::worker::command::Command::run` but with a fixed 10 ms
/// idle tick so small test suites stay snappy.
async fn in_process_claim_loop(consumer: Arc<JobStore>, handler: Arc<dyn JobHandler>) {
    loop {
        match consumer.claim_one(CACHE_QUEUE).await {
            Err(_) => sleep(Duration::from_millis(100)).await,
            Ok(claim_outcome) => match claim_outcome.claimed {
                None => sleep(claim_outcome.idle_sleep(Duration::from_millis(10))).await,
                Some(claimed) => execute_one(consumer.as_ref(), handler.as_ref(), claimed).await,
            },
        }
    }
}
