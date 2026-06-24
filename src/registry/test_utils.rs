use std::{collections::HashMap, sync::Arc};

use bytes::Bytes;
use bytesize::ByteSize;
use serde_json::json;
use tempfile::TempDir;
use uuid::Uuid;

use crate::{
    cache,
    configuration::GlobalConfig,
    metrics_provider,
    oci::{Digest, Namespace, Tag},
    policy::{AccessMode, AccessPolicyConfig, RetentionPolicy, RetentionPolicyConfig, SystemClock},
    registry::{
        Registry, RegistryConfig, Repository, blob_store,
        blob_store::{BlobStore, BlobStoreConfig},
        job_store::{JobStore, Queue},
        manifest::DEFAULT_MAX_MANIFEST_SIZE_BYTES,
        metadata_store::{LinkKind, LinkOperation, MetadataStore},
        path_builder, repository,
        repository_resolver::RepositoryResolver,
        s3_connection::S3ConnectionConfig,
    },
    registry_client::RegistryClient,
    replication::{ReplicationDownstream, ReplicationMode, ReplicationPushPayload},
    secret::Secret,
};
use angos_s3_client::Backend as S3HttpBackend;
use angos_storage::{
    ConditionalStore, ObjectStore, fs::Backend as StorageFsBackend, s3::Backend as StorageS3Backend,
};
use angos_tx_engine::{
    executor::{TransactionExecutor, build_executor, locked::LockedExecutor},
    lock::{LockStrategy, primitive::Lock, storage::memory::MemoryLockStorage},
    store::Store,
};

/// Build a fresh in-memory lock primitive for tests.
pub fn memory_lock() -> Arc<Lock> {
    Arc::new(
        Lock::builder(Arc::new(MemoryLockStorage::new()))
            .build()
            .expect("test lock"),
    )
}

/// Build an in-process `LockedExecutor` over `store`, serialising on a fresh
/// in-memory lock.
pub fn locked_executor_over(store: Arc<dyn ObjectStore>) -> Arc<dyn TransactionExecutor> {
    Arc::new(LockedExecutor::builder(store, memory_lock()).build())
}

/// Build an in-process FS-backed `LockedExecutor` suitable for unit tests.
pub fn build_test_fs_executor(root_dir: &str, sync_to_disk: bool) -> Arc<dyn TransactionExecutor> {
    let store = Arc::new(
        StorageFsBackend::builder(root_dir)
            .sync_to_disk(sync_to_disk)
            .build(),
    );
    locked_executor_over(store)
}

/// Wrap an object store + executor into a [`Store`] façade for tests. The
/// store handle is an [`ObjectStore`] (the CRUD floor plus the upload-session
/// lifecycle), matching the façade's composed surface.
pub fn build_store(
    object: Arc<dyn ObjectStore>,
    executor: Arc<dyn TransactionExecutor>,
) -> Arc<Store> {
    Arc::new(Store::builder(object, executor).build())
}

/// Wrap an object store + executor into a cache-less [`MetadataStore`] for
/// tests (link cache and access-time debounce both disabled).
pub fn metadata_store_over(
    object: Arc<dyn ObjectStore>,
    executor: Arc<dyn TransactionExecutor>,
) -> Arc<MetadataStore> {
    metadata_store_over_cached(object, executor, 0)
}

/// Like [`metadata_store_over`] but with a memory-backed link cache at
/// `link_cache_ttl_secs` (`0` keeps it disabled).
pub fn metadata_store_over_cached(
    object: Arc<dyn ObjectStore>,
    executor: Arc<dyn TransactionExecutor>,
    link_cache_ttl_secs: u64,
) -> Arc<MetadataStore> {
    Arc::new(
        MetadataStore::builder(build_store(object, executor))
            .cache(cache::Config::Memory.to_backend().expect("memory cache"))
            .link_cache_ttl(link_cache_ttl_secs)
            .access_time_debounce_secs(0)
            .build(),
    )
}

pub fn create_test_repositories() -> Arc<HashMap<String, Repository>> {
    metrics_provider::init_for_tests();

    let config = repository::Config {
        access_policy: AccessPolicyConfig {
            default: AccessMode::Allow,
            ..AccessPolicyConfig::default()
        },
        retention_policy: RetentionPolicyConfig::default(),
        ..repository::Config::default()
    };

    let mut repositories = HashMap::new();
    repositories.insert(
        "test-repo".to_string(),
        Repository {
            name: Namespace::new("test-repo").unwrap(),
            upstreams: Vec::new(),
            replication: Vec::new(),
            retention_policy: RetentionPolicy::new(&config.retention_policy, Arc::new(SystemClock)),
            immutable_tags: config.immutable_tags,
            immutable_tags_exclusions: config.immutable_tags_exclusions,
        },
    );

    Arc::new(repositories)
}

pub fn create_test_registry(
    blob_store: Arc<BlobStore>,
    metadata_store: Arc<MetadataStore>,
) -> Registry {
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
) -> Registry {
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
        .put(
            &path_builder::link_path(link, namespace),
            Bytes::copy_from_slice(body),
        )
        .await
        .expect("raw link write");
}

/// Test-only helper that writes `content` directly at the canonical blob path
/// via the underlying `ObjectStore` (no upload state machine, no namespace),
/// returning its SHA-256 digest.
pub async fn put_blob_direct(store: &Store, content: &[u8]) -> Digest {
    let digest = Digest::sha256_of_bytes(content);
    store
        .put(
            &path_builder::blob_path(&digest),
            Bytes::copy_from_slice(content),
        )
        .await
        .unwrap();
    digest
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

    let repository = Repository {
        name: Namespace::new("test-repo").unwrap(),
        upstreams: Vec::new(),
        replication: Vec::new(),
        retention_policy: RetentionPolicy::new(
            &RetentionPolicyConfig { rules: Vec::new() },
            Arc::new(SystemClock),
        ),
        immutable_tags: false,
        immutable_tags_exclusions: Vec::new(),
    };

    (digest, repository)
}

#[async_trait::async_trait(?Send)]
pub trait RegistryTestCase {
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
    registry: Registry,
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

        let meta_executor = build_test_fs_executor(&path, false);
        let meta_storage: Arc<dyn ObjectStore> =
            Arc::new(StorageFsBackend::builder(&path).sync_to_disk(false).build());
        let metadata_store =
            metadata_store_over_cached(meta_storage, meta_executor, link_cache_ttl_secs);
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

        let meta_executor = build_test_fs_executor(&meta_path, false);
        let meta_storage: Arc<dyn ObjectStore> = Arc::new(
            StorageFsBackend::builder(&meta_path)
                .sync_to_disk(false)
                .build(),
        );
        let metadata_store = metadata_store_over_cached(meta_storage, meta_executor, 0);
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
    s3_registry: Registry,
}

impl S3RegistryTestCase {
    pub fn new() -> Self {
        let key_prefix = format!("test-{}", Uuid::new_v4());

        let connection = S3ConnectionConfig {
            access_key_id: Secret::new("root".to_string()),
            secret_key: Secret::new("roottoor".to_string()),
            endpoint: "http://127.0.0.1:9000".to_string(),
            region: "region".to_string(),
            bucket: "registry".to_string(),
            key_prefix: key_prefix.clone(),
        };

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
        let meta_raw_storage = Arc::new(StorageS3Backend::builder(meta_http).build());
        let meta_object_store: Arc<dyn ObjectStore> = meta_raw_storage.clone();
        let meta_conditional: Arc<dyn ConditionalStore> = meta_raw_storage;
        let meta_executor = build_executor(
            meta_object_store.clone(),
            Some(meta_conditional),
            LockStrategy::Memory,
            None,
            false,
            false,
        )
        .expect("s3 metadata executor");
        let metadata_store = metadata_store_over(meta_object_store, meta_executor);

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
            .store
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
