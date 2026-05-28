use std::{collections::HashMap, sync::Arc};

use bytes::Bytes;
use bytesize::ByteSize;
use sha2::{Digest as Sha256Digest, Sha256};
use tempfile::TempDir;
use uuid::Uuid;

use super::*;
use crate::{
    configuration::GlobalConfig,
    metrics_provider,
    oci::{Digest, Namespace},
    policy::{AccessMode, AccessPolicyConfig, RetentionPolicy, RetentionPolicyConfig, SystemClock},
    registry::{
        blob_store::{self, BlobStore, PresignedBlobStore, UploadStore, sha256_ext::Sha256Ext},
        metadata_store::{LinkOperation, MetadataStore, link_kind::LinkKind},
        path_builder,
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
};

/// Build an in-process `LockedExecutor` suitable for unit tests.
pub fn build_test_fs_executor(root_dir: &str, sync_to_disk: bool) -> Arc<dyn TransactionExecutor> {
    let store = Arc::new(
        StorageFsBackend::builder()
            .root_dir(root_dir)
            .sync_to_disk(sync_to_disk)
            .build()
            .expect("test fs store"),
    );
    let lock = Arc::new(
        Lock::builder()
            .storage(Arc::new(MemoryLockStorage::new()))
            .build()
            .expect("test lock"),
    );
    Arc::new(
        LockedExecutor::builder()
            .store(store)
            .lock(lock)
            .build()
            .expect("test executor"),
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
            retention_policy: RetentionPolicy::new(&config.retention_policy, Arc::new(SystemClock)),
            immutable_tags: config.immutable_tags,
            immutable_tags_exclusions: config.immutable_tags_exclusions,
        },
    );

    Arc::new(repositories)
}

pub fn create_test_registry(
    blob_store: Arc<dyn BlobStore + Send + Sync>,
    upload_store: Arc<dyn UploadStore + Send + Sync>,
    presigned_blob_store: Option<Arc<dyn PresignedBlobStore>>,
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

    Registry::new(
        blob_store,
        upload_store,
        presigned_blob_store,
        metadata_store,
        resolver,
        config,
    )
    .unwrap()
}

/// Write `content` directly at the canonical blob path via the underlying
/// `ObjectStore`. Returns the SHA-256 digest.
///
/// Test-only setup helper that replaces the deleted `BlobStore::create`
/// shortcut. Seeds blob bytes without invoking the upload state machine
/// (no upload-session record, no namespace required) — which matches the
/// legacy `BlobStore::create` semantics most closely.
pub async fn put_blob_direct(store: &dyn ObjectStore, content: &[u8]) -> Digest {
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
    fn blob_store(&self) -> Arc<dyn BlobStore>;
    fn upload_store(&self) -> Arc<dyn UploadStore>;
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
    blob_store: Arc<blob_store::fs::Backend>,
    metadata_store: Arc<MetadataStore>,
    registry: Registry,
    temp_dir: TempDir,
}

impl FSRegistryTestCase {
    pub fn new() -> Self {
        let temp_dir = TempDir::new().expect("Failed to create temp dir for FSBackendConfig");
        let path = temp_dir.path().to_string_lossy().to_string();

        let config = blob_store::fs::BackendConfig {
            root_dir: path.clone(),
            sync_to_disk: false,
        };
        let executor = build_test_fs_executor(&config.root_dir, config.sync_to_disk);
        let blob_store = Arc::new(
            blob_store::fs::Backend::builder()
                .root_dir(&config.root_dir)
                .sync_to_disk(config.sync_to_disk)
                .executor(executor)
                .build()
                .unwrap(),
        );

        let meta_executor = build_test_fs_executor(&path, false);
        let meta_storage: Arc<dyn ObjectStore> = Arc::new(
            StorageFsBackend::builder()
                .root_dir(&path)
                .sync_to_disk(false)
                .build()
                .expect("fs metadata storage"),
        );
        let metadata_store = Arc::new(
            MetadataStore::builder()
                .store(meta_storage)
                .executor(meta_executor)
                .build()
                .expect("fs metadata backend"),
        );
        let registry = create_test_registry(
            blob_store.clone(),
            blob_store.clone(),
            None,
            metadata_store.clone(),
        );

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

    pub fn blob_store(&self) -> &blob_store::fs::Backend {
        &self.blob_store
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

    fn blob_store(&self) -> Arc<dyn BlobStore> {
        self.blob_store.clone()
    }

    fn upload_store(&self) -> Arc<dyn UploadStore> {
        self.blob_store.clone()
    }

    fn metadata_store(&self) -> Arc<MetadataStore> {
        self.metadata_store.clone()
    }
}

/// Build an S3 `blob_store::Backend` with a memory-lock executor for tests.
pub fn build_s3_blob_backend(config: &blob_store::s3::BackendConfig) -> blob_store::s3::Backend {
    let http = Arc::new(
        S3HttpBackend::new(&config.connection.to_client_config()).expect("s3 blob http client"),
    );
    let raw = Arc::new(
        StorageS3Backend::builder()
            .client(http)
            .build()
            .expect("s3 blob raw storage"),
    );
    let object_store: Arc<dyn ObjectStore> = raw.clone();
    let conditional: Arc<dyn ConditionalStore> = raw;
    let executor = build_executor(
        object_store,
        Some(conditional),
        LockStrategy::Memory,
        None,
        false,
        false,
    )
    .expect("s3 blob executor");
    blob_store::s3::Backend::builder()
        .access_key_id(config.connection.access_key_id.clone())
        .secret_key(config.connection.secret_key.clone())
        .endpoint(&config.connection.endpoint)
        .bucket(&config.connection.bucket)
        .region(&config.connection.region)
        .key_prefix(&config.connection.key_prefix)
        .multipart_copy_threshold(config.transport.multipart_copy_threshold)
        .multipart_copy_chunk_size(config.transport.multipart_copy_chunk_size)
        .multipart_copy_jobs(config.transport.multipart_copy_jobs)
        .multipart_part_size(config.transport.multipart_part_size)
        .multipart_uniform_parts(config.transport.multipart_uniform_parts)
        .operation_timeout_secs(config.transport.operation_timeout_secs)
        .operation_attempt_timeout_secs(config.transport.operation_attempt_timeout_secs)
        .max_attempts(config.transport.max_attempts)
        .executor(executor)
        .build()
        .expect("s3 blob backend")
}

pub struct S3RegistryTestCase {
    key_prefix: String,
    s3_blob_store: Arc<blob_store::s3::Backend>,
    s3_metadata_store: Arc<MetadataStore>,
    s3_registry: Registry,
}

impl S3RegistryTestCase {
    pub fn new() -> Self {
        let key_prefix = format!("test-{}", Uuid::new_v4());

        let s3_config = blob_store::s3::BackendConfig {
            connection: S3ConnectionConfig {
                access_key_id: Secret::new("root".to_string()),
                secret_key: Secret::new("roottoor".to_string()),
                endpoint: "http://127.0.0.1:9000".to_string(),
                region: "region".to_string(),
                bucket: "registry".to_string(),
                key_prefix: key_prefix.clone(),
            },
            transport: blob_store::s3::TransportFields {
                multipart_copy_threshold: ByteSize::mib(5),
                multipart_copy_chunk_size: ByteSize::mib(5),
                multipart_part_size: ByteSize::mib(5),
                ..blob_store::s3::TransportFields::default()
            },
        };
        let blob_store = Arc::new(build_s3_blob_backend(&s3_config));

        let meta_connection = S3ConnectionConfig {
            access_key_id: Secret::new("root".to_string()),
            secret_key: Secret::new("roottoor".to_string()),
            endpoint: "http://127.0.0.1:9000".to_string(),
            region: "region".to_string(),
            bucket: "registry".to_string(),
            key_prefix: key_prefix.clone(),
        };
        let meta_http = Arc::new(
            S3HttpBackend::new(&meta_connection.to_client_config()).expect("s3 http client"),
        );
        let meta_raw_storage = Arc::new(
            StorageS3Backend::builder()
                .client(meta_http.clone())
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
        let metadata_store = Arc::new(
            MetadataStore::builder()
                .store(meta_object_store)
                .executor(meta_executor)
                .link_cache_ttl(0)
                .access_time_debounce_secs(0)
                .build()
                .expect("s3 metadata backend"),
        );

        let registry = create_test_registry(
            blob_store.clone(),
            blob_store.clone(),
            Some(blob_store.clone() as Arc<dyn PresignedBlobStore>),
            metadata_store.clone(),
        );

        Self {
            key_prefix,
            s3_blob_store: blob_store,
            s3_metadata_store: metadata_store,
            s3_registry: registry,
        }
    }

    pub fn blob_store(&self) -> &blob_store::s3::Backend {
        &self.s3_blob_store
    }
}

impl S3RegistryTestCase {
    pub async fn cleanup(&self) {
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

#[async_trait::async_trait(?Send)]
impl RegistryTestCase for S3RegistryTestCase {
    fn registry(&self) -> &Registry {
        &self.s3_registry
    }

    fn blob_store(&self) -> Arc<dyn BlobStore> {
        self.s3_blob_store.clone()
    }

    fn upload_store(&self) -> Arc<dyn UploadStore> {
        self.s3_blob_store.clone()
    }

    fn metadata_store(&self) -> Arc<MetadataStore> {
        self.s3_metadata_store.clone()
    }

    async fn cleanup(&self) {
        S3RegistryTestCase::cleanup(self).await;
    }
}
