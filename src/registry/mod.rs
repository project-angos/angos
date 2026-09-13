use std::{fmt, sync::Arc};

use http::{
    Response, StatusCode,
    header::{HeaderName, HeaderValue},
};
use tracing::instrument;

pub mod admin;
pub mod blob;
pub mod blob_ownership;
pub mod blob_store;
pub mod content_discovery;
mod error;
#[cfg(test)]
mod event_emission_tests;
pub mod keys;
pub mod layers;
pub mod manifest;
pub mod metadata_store;
pub mod pagination;
pub mod repository;
pub mod repository_resolver;
pub mod s3_connection;
#[cfg(test)]
pub mod test_utils;
pub mod upload;

use angos_oci::server;
use angos_oci::{Namespace, Reference, Tag};

use crate::{
    cache,
    configuration::RegexPattern,
    event_webhook::{dispatcher::EventDispatcher, event::Event},
    http_response::{ResponseBody, build_response},
    jobs::store::JobStore,
    metrics_provider::metrics_provider,
    registry::{
        blob_store::BlobStore, metadata_store::MetadataStore,
        repository_resolver::RepositoryResolver,
    },
};
pub use admin::{DeleteJobRequest, ListJobsRequest, ListPullsRequest, RetryJobRequest};
pub use error::Error;
pub use repository::Repository;

/// Angos's own response header, alongside the distribution API's own names in
/// [`angos_oci::header`].
pub const X_POWERED_BY: HeaderName = HeaderName::from_static("x-powered-by");

#[allow(clippy::struct_excessive_bools)]
pub struct RegistryConfig {
    pub update_pull_time: bool,
    pub enable_blob_redirect: bool,
    pub enable_manifest_redirect: bool,
    pub global_immutable_tags: bool,
    pub global_immutable_tags_exclusions: Vec<RegexPattern>,
    pub max_manifest_size_bytes: usize,
    pub max_blob_size_bytes: u64,
    /// Read buffer each frame of a streamed blob response is filled from.
    pub blob_stream_frame_size: usize,
    /// When `true`, a push is rejected with `MANIFEST_BLOB_UNKNOWN` if any
    /// referenced blob or child manifest is not owned by the target namespace;
    /// when `false` the unowned references are left dangling rather than
    /// granted, so a namespace never gains read access to content it did not
    /// push. `subject` references and pull-through cache-fill writes are exempt
    /// either way.
    pub validate_manifest_references: bool,
    /// The queue every cache-fill and replication job is enqueued to. Who
    /// drains it is the caller's business: the registry only enqueues, and
    /// starts nothing of its own.
    pub job_queue: Arc<JobStore>,
    /// Webhook dispatcher operations deliver their events through; `None`
    /// disables delivery entirely.
    pub event_dispatcher: Option<Arc<EventDispatcher>>,
}

impl RegistryConfig {
    /// The defaults, around the one field that has none: a registry always
    /// needs a queue to enqueue into.
    pub fn new(job_queue: Arc<JobStore>) -> Self {
        Self {
            update_pull_time: false,
            enable_blob_redirect: true,
            enable_manifest_redirect: true,
            global_immutable_tags: false,
            global_immutable_tags_exclusions: Vec::new(),
            max_manifest_size_bytes: manifest::DEFAULT_MAX_MANIFEST_SIZE_BYTES,
            max_blob_size_bytes: upload::DEFAULT_MAX_BLOB_SIZE_BYTES,
            blob_stream_frame_size: blob::DEFAULT_BLOB_STREAM_FRAME_SIZE_BYTES,
            // Strict here so registries built from these defaults keep
            // validating; the server opts into the permissive production
            // default via `[global]`.
            validate_manifest_references: true,
            job_queue,
            event_dispatcher: None,
        }
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
    max_blob_size_bytes: u64,
    blob_stream_frame_size: usize,
    validate_manifest_references: bool,
    event_dispatcher: Option<Arc<EventDispatcher>>,
}

impl fmt::Debug for Registry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Registry").finish()
    }
}

/// The OCI API version this registry speaks, served on `/v2/`.
pub fn api_version() -> Result<Response<ResponseBody>, Error> {
    let mut headers = server::api_version_headers();
    headers.insert(X_POWERED_BY, HeaderValue::from_static("Angos"));

    Ok(build_response(
        StatusCode::OK,
        headers,
        ResponseBody::empty(),
    )?)
}

impl Registry {
    /// Whether `reference` names something that may not be overwritten; a
    /// digest is content-addressed, so it is never immutable in this sense.
    pub fn is_reference_immutable(
        &self,
        repository: Option<&Repository>,
        reference: &Reference,
    ) -> bool {
        match reference {
            Reference::Tag(tag) => self.is_tag_immutable(repository, tag),
            Reference::Digest(_) => false,
        }
    }

    /// A repository's own `immutable_tags` opts in on top of the global flag,
    /// and its own exclusions replace the global ones when it declares any.
    pub fn is_tag_immutable(&self, repository: Option<&Repository>, tag: &Tag) -> bool {
        let immutable =
            self.global_immutable_tags || repository.is_some_and(|repo| repo.immutable_tags);
        if !immutable {
            return false;
        }

        let exclusions = match repository {
            Some(repo) if !repo.immutable_tags_exclusions.is_empty() => {
                &repo.immutable_tags_exclusions
            }
            _ => &self.global_immutable_tags_exclusions,
        };

        !exclusions
            .iter()
            .any(|pattern| pattern.is_match(tag.as_ref()))
    }

    /// The metadata store, for the ownership and link reads outside the
    /// registry's own operations.
    pub fn metadata_store(&self) -> &MetadataStore {
        &self.metadata_store
    }

    #[instrument(skip(blob_store, metadata_store, resolver, config))]
    pub fn new(
        blob_store: Arc<BlobStore>,
        metadata_store: Arc<MetadataStore>,
        resolver: Arc<RepositoryResolver>,
        config: RegistryConfig,
    ) -> Arc<Self> {
        Arc::new(Self {
            update_pull_time: config.update_pull_time,
            enable_blob_redirect: config.enable_blob_redirect,
            enable_manifest_redirect: config.enable_manifest_redirect,
            blob_store,
            metadata_store,
            resolver,
            job_queue: config.job_queue,
            global_immutable_tags: config.global_immutable_tags,
            global_immutable_tags_exclusions: config.global_immutable_tags_exclusions,
            max_manifest_size_bytes: config.max_manifest_size_bytes,
            max_blob_size_bytes: config.max_blob_size_bytes,
            blob_stream_frame_size: config.blob_stream_frame_size,
            validate_manifest_references: config.validate_manifest_references,
            event_dispatcher: config.event_dispatcher,
        })
    }

    /// The configured webhook dispatcher, shared so externally built handlers
    /// emit through the same instance [`Registry::shutdown`] drains.
    pub fn event_dispatcher(&self) -> Option<Arc<EventDispatcher>> {
        self.event_dispatcher.clone()
    }

    #[cfg(test)]
    pub fn has_event_dispatcher(&self) -> bool {
        self.event_dispatcher.is_some()
    }

    /// Delivers every event even when an earlier delivery fails, returning the
    /// first error. Operations call this before performing the action, so a
    /// performed action can never go unnotified (at-least-once) at the cost of
    /// a false-positive notification when the action then fails.
    pub async fn dispatch_events(&self, events: &[Event]) -> Result<(), Error> {
        let Some(dispatcher) = &self.event_dispatcher else {
            return Ok(());
        };
        let mut outcome = Ok(());
        for event in events {
            let delivered = dispatcher
                .dispatch(event)
                .await
                .map_err(|error| Error::EventDelivery(error.to_string()));
            outcome = outcome.and(delivered);
        }
        outcome
    }

    /// Drains in-flight async webhook deliveries to completion.
    pub async fn shutdown(&self) {
        if let Some(dispatcher) = &self.event_dispatcher {
            dispatcher.shutdown().await;
        }
    }

    /// Probes the metadata backend.
    pub async fn check_ready(&self) -> Result<(), Error> {
        self.metadata_store.check_ready().await
    }

    /// The repository mirroring the registry namespace a proxying client names
    /// in `?ns=`, or `None` when no repository claims it.
    pub fn repository_for_ns(&self, ns: &str) -> Option<&Repository> {
        self.resolver.resolve_ns(ns)
    }

    #[instrument]
    pub fn get_repository_for_namespace(
        &self,
        namespace: &Namespace,
    ) -> Result<&Repository, Error> {
        self.resolver.resolve(namespace).ok_or(Error::NameUnknown)
    }

    /// The event `repository` field for `namespace`, empty when none matches.
    /// Callers already holding the resolved repository use [`repository_name`]
    /// instead, so a request never resolves the same namespace twice.
    pub fn repository_name_for(&self, namespace: &Namespace) -> String {
        repository_name(self.get_repository_for_namespace(namespace).ok())
    }
}

/// The pull-through repository serving a pull, `None` when the repository
/// mirrors no upstream and so caches nothing.
pub fn pull_through_name(repository: Option<&Repository>) -> Option<&str> {
    repository
        .filter(|repository| repository.is_pull_through())
        .map(|repository| repository.name.as_ref())
}

/// Records one pull-through cache outcome for `repository`, which names the
/// pull-through repository serving the pull; a repository with no upstream
/// caches nothing and so records nothing.
pub fn record_pull_through(repository: Option<&str>, kind: &str, outcome: &str) {
    if let Some(repository) = repository {
        metrics_provider()
            .pull_through_total
            .with_label_values(&[repository, kind, outcome])
            .inc();
    }
}

/// The event `repository` field for an already-resolved repository, empty when
/// no `[repository]` entry matched.
pub fn repository_name(repository: Option<&Repository>) -> String {
    repository.map_or_else(String::new, |repository| repository.name.to_string())
}

#[cfg(test)]
mod immutable_tag_tests {
    use std::{collections::HashMap, sync::Arc};

    use tempfile::TempDir;

    use angos_oci::{Namespace, Reference, Tag};
    use angos_storage::{ObjectStore, fs::Backend as StorageFsBackend};

    use crate::registry::{Registry, RegistryConfig};
    use crate::{
        configuration::RegexPattern,
        registry::{
            blob_store::BlobStore,
            repository::Repository,
            repository_resolver::RepositoryResolver,
            test_utils::{metadata_store_over, repository_with_replication, test_job_store},
        },
    };

    /// A registry whose sole `myrepo/*` repository carries the immutability
    /// settings under test, over the global ones.
    fn registry_with(
        global_immutable_tags: bool,
        global_exclusions: &[&str],
        repository_immutable_tags: bool,
        repository_exclusions: &[&str],
    ) -> (Arc<Registry>, TempDir) {
        let mut repository = repository_with_replication("myrepo", Vec::new());
        repository.immutable_tags = repository_immutable_tags;
        repository.immutable_tags_exclusions = patterns(repository_exclusions);

        let mut repositories = HashMap::new();
        repositories.insert("myrepo".to_string(), repository);

        let dir = TempDir::new().expect("temp dir");
        let object: Arc<dyn ObjectStore> = Arc::new(StorageFsBackend::builder(dir.path()).build());
        let metadata_store = metadata_store_over(object.clone());
        let registry = Registry::new(
            Arc::new(BlobStore::new(object, None)),
            metadata_store.clone(),
            Arc::new(RepositoryResolver::new(Arc::new(repositories)).expect("test resolver")),
            RegistryConfig {
                global_immutable_tags,
                global_immutable_tags_exclusions: patterns(global_exclusions),
                ..RegistryConfig::new(test_job_store(&metadata_store))
            },
        );
        (registry, dir)
    }

    fn patterns(sources: &[&str]) -> Vec<RegexPattern> {
        sources
            .iter()
            .map(|source| RegexPattern::compile(*source).expect("test pattern must compile"))
            .collect()
    }

    fn namespace() -> Namespace {
        Namespace::new("myrepo/app").unwrap()
    }

    fn repository_of(registry: &Registry) -> Option<&Repository> {
        registry.get_repository_for_namespace(&namespace()).ok()
    }

    fn tag(name: &str) -> Tag {
        Tag::new(name).unwrap()
    }

    #[tokio::test]
    async fn the_global_flag_freezes_every_tag() {
        let (registry, _dir) = registry_with(true, &[], false, &[]);

        assert!(registry.is_tag_immutable(repository_of(&registry), &tag("v1.0.0")));
    }

    #[tokio::test]
    async fn global_exclusions_stay_mutable() {
        let (registry, _dir) = registry_with(true, &["^latest$", "^dev-.*"], false, &[]);

        assert!(!registry.is_tag_immutable(repository_of(&registry), &tag("latest")));
        assert!(!registry.is_tag_immutable(repository_of(&registry), &tag("dev-branch")));
        assert!(registry.is_tag_immutable(repository_of(&registry), &tag("v1.0.0")));
    }

    #[tokio::test]
    async fn a_repository_can_freeze_its_own_tags() {
        let (registry, _dir) = registry_with(false, &[], true, &[]);

        assert!(registry.is_tag_immutable(repository_of(&registry), &tag("v1.0.0")));
    }

    /// A repository declaring exclusions replaces the global list rather than
    /// adding to it, so `latest` is frozen here despite the global exclusion.
    #[tokio::test]
    async fn repository_exclusions_replace_the_global_ones() {
        let (registry, _dir) = registry_with(true, &["^latest$"], true, &["^test-.*"]);

        assert!(!registry.is_tag_immutable(repository_of(&registry), &tag("test-123")));
        assert!(registry.is_tag_immutable(repository_of(&registry), &tag("latest")));
    }

    #[tokio::test]
    async fn a_digest_reference_is_never_immutable() {
        let (registry, _dir) = registry_with(true, &[], true, &[]);
        let digest = "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
            .parse()
            .unwrap();

        assert!(
            !registry.is_reference_immutable(repository_of(&registry), &Reference::Digest(digest))
        );
        assert!(
            registry
                .is_reference_immutable(repository_of(&registry), &Reference::Tag(tag("v1.0.0")))
        );
    }
}

#[cfg(test)]
mod api_version_tests {
    use angos_oci::header::DOCKER_DISTRIBUTION_API_VERSION;

    use crate::registry::test_utils::response_header;
    use crate::registry::{X_POWERED_BY, api_version};

    #[test]
    fn api_version_announces_the_v2_protocol() {
        let response = api_version().unwrap();

        assert_eq!(
            *response_header(&response, &DOCKER_DISTRIBUTION_API_VERSION),
            "registry/2.0"
        );
        assert_eq!(*response_header(&response, &X_POWERED_BY), "Angos");
    }
}
