use std::{collections::HashMap, io::Cursor, sync::Arc};

use bytes::Bytes;
use bytesize::ByteSize;
use serde_json::json;
use tempfile::TempDir;
use uuid::Uuid;

use crate::{
    cache,
    configuration::GlobalConfig,
    metrics_provider,
    oci::{Digest, MediaType, Namespace, Tag, UploadSessionId},
    policy::{RetentionPolicy, RetentionPolicyConfig, SystemClock},
    registry::{
        Error, Registry, RegistryConfig, Repository,
        blob::{BlobRange, GetBlobResponse},
        blob_ownership::BlobOwnership,
        blob_store,
        blob_store::{BlobStore, BlobStoreConfig},
        job_store::{JobStore, Queue},
        manifest::DEFAULT_MAX_MANIFEST_SIZE_BYTES,
        metadata_store::{LinkKind, LinkOperation, MetadataStore},
        path_builder,
        repository_resolver::RepositoryResolver,
        s3_connection::S3ConnectionConfig,
    },
    registry_client::RegistryClient,
    replication::{ReplicationDownstream, ReplicationMode, ReplicationPushPayload},
    secret::Secret,
};
use angos_s3_client::Backend as S3HttpBackend;
use angos_s3_client::test_util::{
    TEST_ACCESS_KEY, TEST_BUCKET, TEST_REGION, TEST_SECRET_KEY, test_endpoint,
};
use angos_storage::{
    ObjectStore, fs::Backend as StorageFsBackend, s3::Backend as StorageS3Backend,
};
use angos_tx_engine::{lock::LockStrategy, store::Store};

/// Canonical connection to the live S3 test backend (rustfs, in CI and
/// locally), single-sourced from the s3-client test fixtures so credentials,
/// bucket, and the `ANGOS_TEST_S3_ENDPOINT` override are declared once.
pub fn s3_test_connection(key_prefix: String) -> S3ConnectionConfig {
    S3ConnectionConfig {
        access_key_id: Secret::new(TEST_ACCESS_KEY.to_string()),
        secret_key: Secret::new(TEST_SECRET_KEY.to_string()),
        endpoint: test_endpoint(),
        region: TEST_REGION.to_string(),
        bucket: TEST_BUCKET.to_string(),
        key_prefix,
    }
}

/// Wrap an object store into a [`Store`] façade for tests, using a locked
/// executor serialising on a fresh in-memory lock.
pub fn build_store(object: Arc<dyn ObjectStore>) -> Arc<Store> {
    Arc::new(Store::new(object, None, LockStrategy::Memory, None).expect("test store"))
}

/// FS-backed test stack over a fresh temp directory: one [`Store`] façade
/// shared by a cache-less [`MetadataStore`] and a presign-less [`BlobStore`].
/// Keep the stack alive for the test's duration; dropping it deletes the
/// directory.
pub struct FsTestStack {
    pub dir: TempDir,
    pub store: Arc<Store>,
    pub metadata_store: Arc<MetadataStore>,
    pub blob_store: Arc<BlobStore>,
}

pub fn fs_test_stack() -> FsTestStack {
    metrics_provider::init_for_tests();
    let dir = TempDir::new().expect("temp dir for fs test stack");
    let object: Arc<dyn ObjectStore> = Arc::new(StorageFsBackend::builder(dir.path()).build());
    let store = build_store(object);
    let metadata_store = Arc::new(
        MetadataStore::builder(store.clone())
            .link_cache_ttl(0)
            .access_time_debounce_secs(0)
            .build(),
    );
    let blob_store = Arc::new(BlobStore::new(store.object_store().clone(), None));
    FsTestStack {
        dir,
        store,
        metadata_store,
        blob_store,
    }
}

/// Resolver over a single named repository.
pub fn single_repo_resolver(name: &str, repository: Repository) -> Arc<RepositoryResolver> {
    let mut repositories = HashMap::new();
    repositories.insert(name.to_string(), repository);
    Arc::new(RepositoryResolver::new(Arc::new(repositories)).expect("test resolver"))
}

/// Run `test` once per registry backend, printing the active backend first
/// (captured test output names it on failure) and running the case's
/// best-effort cleanup after the body. The single home of the backend matrix;
/// tests never iterate [`backends`] themselves.
pub async fn for_each_backend<F>(test: F)
where
    F: AsyncFn(&dyn RegistryTestCase),
{
    for case in backends() {
        eprintln!("running against the {} backend", case.name());
        test(case.as_ref()).await;
        case.cleanup().await;
    }
}

/// Wrap an object store into a cache-less [`MetadataStore`] for tests (link
/// cache and access-time debounce both disabled).
pub fn metadata_store_over(object: Arc<dyn ObjectStore>) -> Arc<MetadataStore> {
    metadata_store_over_cached(object, 0)
}

/// Like [`metadata_store_over`] but with a memory-backed link cache at
/// `link_cache_ttl_secs` (`0` keeps it disabled).
pub fn metadata_store_over_cached(
    object: Arc<dyn ObjectStore>,
    link_cache_ttl_secs: u64,
) -> Arc<MetadataStore> {
    Arc::new(
        MetadataStore::builder(build_store(object))
            .cache(cache::Config::Memory.to_backend().expect("memory cache"))
            .link_cache_ttl(link_cache_ttl_secs)
            .access_time_debounce_secs(0)
            .build(),
    )
}

pub fn create_test_repositories() -> Arc<HashMap<String, Repository>> {
    metrics_provider::init_for_tests();

    let mut repositories = HashMap::new();
    repositories.insert(
        "test-repo".to_string(),
        repository_with_replication("test-repo", Vec::new()),
    );

    Arc::new(repositories)
}

pub fn create_test_registry(
    blob_store: Arc<BlobStore>,
    metadata_store: Arc<MetadataStore>,
) -> Arc<Registry> {
    create_test_registry_with(blob_store, metadata_store, true)
}

/// Like [`create_test_registry`] but lets a test pin whether the live
/// `accept_put_manifest` path enforces manifest-reference validation, so both
/// the strict and the permissive (`allow_missing_manifest_references`) modes
/// can be exercised end-to-end.
pub fn create_test_registry_with(
    blob_store: Arc<BlobStore>,
    metadata_store: Arc<MetadataStore>,
    validate_manifest_references: bool,
) -> Arc<Registry> {
    let resolver = Arc::new(
        RepositoryResolver::new(create_test_repositories())
            .expect("test repositories must not have overlapping prefixes"),
    );
    let global = GlobalConfig::default();

    let config = RegistryConfig::default()
        .update_pull_time(global.update_pull_time)
        .enable_blob_redirect(global.resolved_enable_blob_redirect())
        .enable_manifest_redirect(global.resolved_enable_manifest_redirect())
        .max_manifest_size_bytes(global.max_manifest_size_bytes())
        .max_blob_size_bytes(global.max_blob_size_bytes())
        .validate_manifest_references(validate_manifest_references)
        .global_immutable_tags(global.immutable_tags)
        .global_immutable_tags_exclusions(global.immutable_tags_exclusions.clone());

    Registry::new(blob_store, metadata_store, resolver, config).unwrap()
}

/// Write raw bytes at the canonical link path for `link` in `namespace`,
/// bypassing the transactional `update_links` path so tests can seed
/// hand-crafted or deliberately corrupt link files.
pub async fn put_link_raw(store: &Store, namespace: &Namespace, link: &LinkKind, body: &[u8]) {
    store
        .object_store()
        .put(
            &path_builder::link_path(link, namespace),
            Bytes::copy_from_slice(body),
        )
        .await
        .expect("raw link write");
}

/// Parse a media type that tests know to be valid.
pub fn media_type(value: &str) -> MediaType {
    MediaType::new(value).unwrap()
}

/// Upload `content` through the full registry upload state machine (session
/// create plus monolithic complete), returning its SHA-256 digest.
pub async fn upload_blob(registry: &Registry, namespace: &Namespace, content: &[u8]) -> Digest {
    let session_id = UploadSessionId::generate();
    registry
        .blob_store
        .create_upload(namespace, session_id.as_ref())
        .await
        .unwrap();

    let body = content.to_vec();
    let digest = Digest::sha256_of_bytes(&body);
    registry
        .complete_upload(
            None,
            namespace,
            &session_id,
            &digest,
            None,
            Some(body.len() as u64),
            Cursor::new(body),
        )
        .await
        .unwrap();
    digest
}

/// Test-only helper that writes `content` directly at the canonical blob path
/// via the underlying `ObjectStore` (no upload state machine, no namespace),
/// returning its SHA-256 digest.
pub async fn put_blob_direct(store: &Store, content: &[u8]) -> Digest {
    let digest = Digest::sha256_of_bytes(content);
    store
        .object_store()
        .put(
            &path_builder::blob_path(&digest),
            Bytes::copy_from_slice(content),
        )
        .await
        .unwrap();
    digest
}

/// Fetch a blob the way `resolve_get_blob` does minus the redirect and event
/// paths: resolve the namespace's ownership verdict, then serve locally or
/// through the pull-through upstream.
pub async fn get_blob(
    registry: &Registry,
    repository: &Repository,
    accepted_types: &[String],
    namespace: &Namespace,
    digest: &Digest,
    range: Option<BlobRange>,
) -> Result<GetBlobResponse, Error> {
    let has_access = BlobOwnership::new(registry.metadata_store.as_ref())
        .can_read(namespace, digest)
        .await?;
    registry
        .get_blob_with_access(
            repository,
            accepted_types,
            namespace,
            digest,
            range,
            has_access,
        )
        .await
}

pub async fn create_test_blob(
    registry: &Registry,
    namespace: &Namespace,
    content: &[u8],
) -> (Digest, Repository) {
    let digest = put_blob_direct(registry.metadata_store.store(), content).await;

    let tag_link = LinkKind::Tag(Tag::new("latest").unwrap());
    let layer_link = LinkKind::Layer(digest.clone());
    registry
        .metadata_store
        .update_links(
            namespace,
            &[
                LinkOperation::create(tag_link.clone(), digest.clone()),
                LinkOperation::create(layer_link.clone(), digest.clone()),
            ],
        )
        .await
        .unwrap();

    let blob_index = registry
        .metadata_store
        .read_blob_index(&digest)
        .await
        .unwrap();
    assert!(blob_index.namespace.contains_key(namespace));
    let namespace_links = blob_index.namespace.get(namespace).unwrap();
    assert!(namespace_links.contains(&layer_link));

    let repository = repository_with_replication("test-repo", Vec::new());

    (digest, repository)
}

#[async_trait::async_trait(?Send)]
pub trait RegistryTestCase {
    fn name(&self) -> &'static str;
    fn registry(&self) -> &Registry;
    fn blob_store(&self) -> Arc<BlobStore>;
    fn metadata_store(&self) -> Arc<MetadataStore>;
    async fn cleanup(&self) {}
}

pub fn backends() -> Vec<Box<dyn RegistryTestCase>> {
    vec![
        Box::new(FSRegistryTestCase::new()),
        Box::new(S3RegistryTestCase::new()),
    ]
}

pub struct FSRegistryTestCase {
    blob_store: Arc<BlobStore>,
    metadata_store: Arc<MetadataStore>,
    registry: Arc<Registry>,
    temp_dir: TempDir,
}

impl FSRegistryTestCase {
    pub fn new() -> Self {
        Self::with_link_cache_ttl(0)
    }

    /// Like [`Self::new`] but with the per-process link cache enabled at
    /// `link_cache_ttl_secs`, for tests pinning which reads must bypass it.
    pub fn with_link_cache_ttl(link_cache_ttl_secs: u64) -> Self {
        let temp_dir = TempDir::new().expect("Failed to create temp dir for FSBackendConfig");
        let path = temp_dir.path().to_string_lossy().to_string();

        let config = BlobStoreConfig::FS(blob_store::FsBackendConfig {
            root_dir: path.clone(),
            sync_to_disk: false,
        });
        let blob_store = Arc::new(config.build_backend().expect("fs blob backend"));

        let meta_storage: Arc<dyn ObjectStore> =
            Arc::new(StorageFsBackend::builder(&path).sync_to_disk(false).build());
        let metadata_store = metadata_store_over_cached(meta_storage, link_cache_ttl_secs);
        let registry = create_test_registry(blob_store.clone(), metadata_store.clone());

        Self {
            blob_store,
            metadata_store,
            registry,
            temp_dir,
        }
    }

    /// Build the blob and metadata stores over **separate** roots, the
    /// split-backend topology a deployment can configure (distinct S3
    /// bucket/`key_prefix`). `blob_path(digest)` then resolves to different
    /// physical objects per store, so a manifest written through the metadata
    /// transaction would be invisible to the blob-store read path, the
    /// cross-store-isolation regression this fixture exercises.
    pub fn with_split_backends() -> Self {
        let temp_dir = TempDir::new().expect("Failed to create temp dir for split backends");
        let blob_path = temp_dir.path().join("blob").to_string_lossy().into_owned();
        let meta_path = temp_dir.path().join("meta").to_string_lossy().into_owned();

        let config = BlobStoreConfig::FS(blob_store::FsBackendConfig {
            root_dir: blob_path,
            sync_to_disk: false,
        });
        let blob_store = Arc::new(config.build_backend().expect("fs blob backend"));

        let meta_storage: Arc<dyn ObjectStore> = Arc::new(
            StorageFsBackend::builder(&meta_path)
                .sync_to_disk(false)
                .build(),
        );
        let metadata_store = metadata_store_over_cached(meta_storage, 0);
        let registry = create_test_registry(blob_store.clone(), metadata_store.clone());

        Self {
            blob_store,
            metadata_store,
            registry,
            temp_dir,
        }
    }

    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    pub fn temp_dir(&self) -> &TempDir {
        &self.temp_dir
    }
}

#[async_trait::async_trait(?Send)]
impl RegistryTestCase for FSRegistryTestCase {
    fn name(&self) -> &'static str {
        "fs"
    }

    fn registry(&self) -> &Registry {
        &self.registry
    }

    fn blob_store(&self) -> Arc<BlobStore> {
        self.blob_store.clone()
    }

    fn metadata_store(&self) -> Arc<MetadataStore> {
        self.metadata_store.clone()
    }
}

pub struct S3RegistryTestCase {
    key_prefix: String,
    s3_blob_store: Arc<BlobStore>,
    s3_metadata_store: Arc<MetadataStore>,
    s3_registry: Arc<Registry>,
}

impl S3RegistryTestCase {
    pub fn new() -> Self {
        let key_prefix = format!("test-{}", Uuid::new_v4());

        let connection = s3_test_connection(key_prefix.clone());

        let s3_config = blob_store::S3BackendConfig {
            connection: connection.clone(),
            transport: blob_store::TransportFields {
                multipart_copy_threshold: ByteSize::mib(5),
                multipart_copy_chunk_size: ByteSize::mib(5),
                multipart_part_size: ByteSize::mib(5),
                ..blob_store::TransportFields::default()
            },
        };
        let blob_store = Arc::new(
            BlobStoreConfig::S3(s3_config)
                .build_backend()
                .expect("s3 blob backend"),
        );

        let meta_http =
            Arc::new(S3HttpBackend::new(&connection.to_client_config()).expect("s3 http client"));
        let meta_object_store: Arc<dyn ObjectStore> =
            Arc::new(StorageS3Backend::builder(meta_http).build());
        let metadata_store = metadata_store_over(meta_object_store);

        let registry = create_test_registry(blob_store.clone(), metadata_store.clone());

        Self {
            key_prefix,
            s3_blob_store: blob_store,
            s3_metadata_store: metadata_store,
            s3_registry: registry,
        }
    }
}

#[async_trait::async_trait(?Send)]
impl RegistryTestCase for S3RegistryTestCase {
    fn name(&self) -> &'static str {
        "s3"
    }

    fn registry(&self) -> &Registry {
        &self.s3_registry
    }

    fn blob_store(&self) -> Arc<BlobStore> {
        self.s3_blob_store.clone()
    }

    fn metadata_store(&self) -> Arc<MetadataStore> {
        self.s3_metadata_store.clone()
    }

    async fn cleanup(&self) {
        if let Err(e) = self
            .s3_blob_store
            .object_store()
            .delete_prefix(&self.key_prefix)
            .await
        {
            println!("Warning: Failed to clean up S3RegistryTestCase data: {e:?}");
        }
    }
}

/// Build an `Arc<RegistryClient>` pointed at `uri` with a fresh in-memory
/// cache. Callers pass a real mock-server URI, or a placeholder like
/// `"https://unused.test"` when the client is never dialed.
pub fn downstream_client(uri: &str) -> Arc<RegistryClient> {
    let backend = cache::Config::Memory.to_backend().unwrap();
    Arc::new(
        RegistryClient::builder(uri.to_string(), reqwest::Client::new(), backend)
            .max_manifest_size_bytes(DEFAULT_MAX_MANIFEST_SIZE_BYTES)
            .build(),
    )
}

/// Build a test `Repository` named `name` carrying `replication` downstreams.
/// The sole `Repository` test literal lives here, so a new struct field is
/// edited in one place; callers vary only the downstream set.
pub fn repository_with_replication(
    name: &str,
    replication: Vec<ReplicationDownstream>,
) -> Repository {
    Repository {
        name: Namespace::new(name).unwrap(),
        upstreams: Vec::new(),
        replication,
        retention_policy: RetentionPolicy::new(
            &RetentionPolicyConfig::default(),
            Arc::new(SystemClock),
        ),
        immutable_tags: false,
        immutable_tags_exclusions: Vec::new(),
    }
}

/// Build a `Repository` named `name` carrying exactly one event+reconcile
/// downstream "eu-region" (match-all filter, `max_concurrent_pushes` 4) backed
/// by `client`.
pub fn repository_with_downstream(name: &str, client: Arc<RegistryClient>) -> Repository {
    repository_with_replication(
        name,
        vec![
            ReplicationDownstream::builder("eu-region".to_string(), client, 4)
                .mode(ReplicationMode::EventReconcile)
                .namespace_filter(Vec::new())
                .build(),
        ],
    )
}

/// Decode the payload of the sole pending replication job, panicking unless
/// exactly one is pending.
pub async fn sole_pending_payload(job_store: &JobStore) -> ReplicationPushPayload {
    let keys = job_store
        .list_pending(Queue::Replication, 16)
        .await
        .unwrap();
    assert_eq!(
        keys.len(),
        1,
        "expected exactly one pending replication job"
    );
    let envelope = job_store
        .read_pending(Queue::Replication, &keys[0])
        .await
        .unwrap();
    assert_eq!(envelope.queue, Queue::Replication);
    serde_json::from_value(envelope.payload).expect("decode ReplicationPushPayload")
}

/// Seed a config blob, a layer blob, a manifest referencing both, and a `v1`
/// tag link under `namespace`, returning the (manifest, config, layer) digests.
pub async fn seed_manifest(
    store: &Store,
    metadata_store: &MetadataStore,
    namespace: &Namespace,
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
            namespace,
            &[
                LinkOperation::create(
                    LinkKind::Tag(Tag::new("v1").unwrap()),
                    manifest_digest.clone(),
                ),
                LinkOperation::create(
                    LinkKind::Config(config_digest.clone()),
                    config_digest.clone(),
                ),
                LinkOperation::create(LinkKind::Layer(layer_digest.clone()), layer_digest.clone()),
            ],
        )
        .await
        .unwrap();

    (manifest_digest, config_digest, layer_digest)
}
