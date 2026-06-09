use std::{collections::HashMap, sync::Arc};

use bytes::Bytes;
use bytesize::ByteSize;
use sha2::{Digest as Sha256Digest, Sha256};
use tempfile::TempDir;
use uuid::Uuid;

use crate::{
    cache,
    configuration::GlobalConfig,
    metrics_provider,
    oci::{Digest, Namespace},
    policy::{AccessMode, AccessPolicyConfig, RetentionPolicy, RetentionPolicyConfig, SystemClock},
    registry::{
        Registry, RegistryConfig, Repository,
        blob_store::{self, sha256_ext::Sha256Ext},
        metadata_store::{LinkOperation, MetadataStore, link_kind::LinkKind},
        path_builder, repository,
        repository_resolver::RepositoryResolver,
        s3_connection::S3ConnectionConfig,
    },
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
        Lock::builder()
            .storage(Arc::new(MemoryLockStorage::new()))
            .build()
            .expect("test lock"),
    )
}

/// Build an in-process `LockedExecutor` over `store`, serialising on a fresh
/// in-memory lock.
pub fn locked_executor_over(store: Arc<dyn ObjectStore>) -> Arc<dyn TransactionExecutor> {
    Arc::new(
        LockedExecutor::builder()
            .store(store)
            .lock(memory_lock())
            .build()
            .expect("test executor"),
    )
}

/// Build an in-process FS-backed `LockedExecutor` suitable for unit tests.
pub fn build_test_fs_executor(root_dir: &str, sync_to_disk: bool) -> Arc<dyn TransactionExecutor> {
    let store = Arc::new(
        StorageFsBackend::builder()
            .root_dir(root_dir)
            .sync_to_disk(sync_to_disk)
            .build()
            .expect("test fs store"),
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
    Arc::new(
        Store::builder()
            .object(object)
            .executor(executor)
            .build()
            .expect("test store façade"),
    )
}

/// Wrap an object store + executor into a cache-less [`MetadataStore`] for
/// tests (link cache and access-time debounce both disabled).
pub fn metadata_store_over(
    object: Arc<dyn ObjectStore>,
    executor: Arc<dyn TransactionExecutor>,
) -> Arc<MetadataStore> {
    metadata_store_over_cached(object, executor, 0)
}

/// Like [`metadata_store_over`] but with a memory-backed link cache enabled at
/// `link_cache_ttl_secs` (`0` keeps it disabled), for tests pinning which reads
/// must bypass the per-process cache.
pub fn metadata_store_over_cached(
    object: Arc<dyn ObjectStore>,
    executor: Arc<dyn TransactionExecutor>,
    link_cache_ttl_secs: u64,
) -> Arc<MetadataStore> {
    Arc::new(
        MetadataStore::builder()
            .store(build_store(object, executor))
            .cache(cache::Config::Memory.to_backend().expect("memory cache"))
            .link_cache_ttl(link_cache_ttl_secs)
            .access_time_debounce_secs(0)
            .build()
            .expect("test metadata backend"),
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
            name: "test-repo".to_string(),
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
    blob_store: Arc<blob_store::BlobStore>,
    metadata_store: Arc<MetadataStore>,
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
        .global_immutable_tags(global.immutable_tags)
        .global_immutable_tags_exclusions(global.immutable_tags_exclusions.clone());

    Registry::new(blob_store, metadata_store, resolver, config).unwrap()
}

/// Write `content` directly at the canonical blob path via the underlying
/// `ObjectStore`. Returns the SHA-256 digest.
///
/// Test-only setup helper that replaces the deleted `BlobStore::create`
/// shortcut. Seeds blob bytes without invoking the upload state machine
/// (no upload-session record, no namespace required) — which matches the
/// legacy `BlobStore::create` semantics most closely.
pub async fn put_blob_direct(store: &Store, content: &[u8]) -> Digest {
    let mut hasher = Sha256::new();
    hasher.update(content);
    let digest = hasher.digest();
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

    let tag_link = LinkKind::Tag("latest".to_string());
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
    assert!(blob_index.namespace.contains_key(namespace.as_ref()));
    let namespace_links = blob_index.namespace.get(namespace.as_ref()).unwrap();
    assert!(namespace_links.contains(&layer_link));

    let repository = Repository {
        name: "test-repo".to_string(),
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
    fn blob_store(&self) -> Arc<blob_store::BlobStore>;
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
    blob_store: Arc<blob_store::BlobStore>,
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

        let config = blob_store::BlobStoreConfig::FS(blob_store::FsBackendConfig {
            root_dir: path.clone(),
            sync_to_disk: false,
        });
        let blob_store = Arc::new(config.build_backend().expect("fs blob backend"));

        let meta_executor = build_test_fs_executor(&path, false);
        let meta_storage: Arc<dyn ObjectStore> = Arc::new(
            StorageFsBackend::builder()
                .root_dir(&path)
                .sync_to_disk(false)
                .build()
                .expect("fs metadata storage"),
        );
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

    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    pub fn set_repositories(&mut self, repositories: Arc<HashMap<String, Repository>>) {
        self.registry.resolver = Arc::new(
            RepositoryResolver::new(repositories)
                .expect("test repositories must not have overlapping prefixes"),
        );
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

    fn blob_store(&self) -> Arc<blob_store::BlobStore> {
        self.blob_store.clone()
    }

    fn metadata_store(&self) -> Arc<MetadataStore> {
        self.metadata_store.clone()
    }
}

pub struct S3RegistryTestCase {
    key_prefix: String,
    s3_blob_store: Arc<blob_store::BlobStore>,
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
            blob_store::BlobStoreConfig::S3(s3_config)
                .build_backend()
                .expect("s3 blob backend"),
        );

        let meta_http =
            Arc::new(S3HttpBackend::new(&connection.to_client_config()).expect("s3 http client"));
        let meta_raw_storage = Arc::new(
            StorageS3Backend::builder()
                .client(meta_http)
                .build()
                .expect("s3 metadata storage"),
        );
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

    fn blob_store(&self) -> Arc<blob_store::BlobStore> {
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
