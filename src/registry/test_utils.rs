use std::{collections::HashMap, sync::Arc};

use bytesize::ByteSize;
use tempfile::TempDir;
use uuid::Uuid;

use super::*;
use crate::{
    configuration::GlobalConfig,
    metrics_provider,
    oci::Digest,
    policy::{AccessMode, AccessPolicyConfig, RetentionPolicyConfig},
    registry::{
        blob_store::{self, BlobStore, PresignedBlobStore, UploadStore},
        data_store, metadata_store,
        metadata_store::{MetadataStore, MetadataStoreExt, link_kind::LinkKind},
    },
};

pub fn create_test_repositories() -> Arc<HashMap<String, Repository>> {
    metrics_provider::init_for_tests();
    let token_cache = cache::Config::default().to_backend().unwrap();

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
        Repository::new("test-repo", &config, &token_cache).unwrap(),
    );

    Arc::new(repositories)
}

pub fn create_test_registry(
    blob_store: Arc<dyn BlobStore + Send + Sync>,
    upload_store: Arc<dyn UploadStore + Send + Sync>,
    presigned_blob_store: Option<Arc<dyn PresignedBlobStore>>,
    metadata_store: Arc<dyn MetadataStore + Send + Sync>,
) -> Registry {
    let repositories_config = create_test_repositories();
    let global = GlobalConfig::default();

    let config = RegistryConfig::new()
        .update_pull_time(global.update_pull_time)
        .enable_blob_redirect(global.resolved_enable_blob_redirect())
        .enable_manifest_redirect(global.resolved_enable_manifest_redirect())
        .concurrent_cache_jobs(global.max_concurrent_cache_jobs)
        .global_immutable_tags(global.immutable_tags)
        .global_immutable_tags_exclusions(global.immutable_tags_exclusions.clone());

    Registry::new(
        blob_store,
        upload_store,
        presigned_blob_store,
        metadata_store,
        repositories_config,
        config,
    )
    .unwrap()
}

pub async fn create_test_blob(
    registry: &Registry,
    namespace: &str,
    content: &[u8],
) -> (Digest, Repository) {
    let digest = registry.blob_store.create(content).await.unwrap();

    let tag_link = LinkKind::Tag("latest".to_string());
    let layer_link = LinkKind::Layer(digest.clone());
    let mut tx = registry.metadata_store.begin_transaction(namespace);
    tx.create_link(&tag_link, &digest).add();
    tx.create_link(&layer_link, &digest).add();
    tx.commit().await.unwrap();

    let blob_index = registry
        .metadata_store
        .read_blob_index(&digest)
        .await
        .unwrap();
    assert!(blob_index.namespace.contains_key(namespace));
    let namespace_links = blob_index.namespace.get(namespace).unwrap();
    assert!(namespace_links.contains(&layer_link));

    let cache = cache::Config::Memory.to_backend().unwrap();
    let repository = Repository::new(
        "test-repo",
        &repository::Config {
            upstream: Vec::new(),
            access_policy: AccessPolicyConfig::default(),
            retention_policy: RetentionPolicyConfig { rules: Vec::new() },
            immutable_tags: false,
            immutable_tags_exclusions: Vec::new(),
            ..repository::Config::default()
        },
        &cache,
    )
    .unwrap();

    (digest, repository)
}

pub trait RegistryTestCase {
    fn registry(&self) -> &Registry;
    fn blob_store(&self) -> Arc<dyn BlobStore>;
    fn upload_store(&self) -> Arc<dyn UploadStore>;
    fn metadata_store(&self) -> Arc<dyn MetadataStore + Send + Sync>;
}

pub fn backends() -> Vec<Box<dyn RegistryTestCase>> {
    vec![
        Box::new(FSRegistryTestCase::new()),
        Box::new(S3RegistryTestCase::new()),
    ]
}

pub struct FSRegistryTestCase {
    blob_store: Arc<blob_store::fs::Backend>,
    metadata_store: Arc<metadata_store::fs::Backend>,
    registry: Registry,
    temp_dir: TempDir,
}

impl FSRegistryTestCase {
    pub fn new() -> Self {
        let temp_dir = TempDir::new().expect("Failed to create temp dir for FSBackendConfig");
        let path = temp_dir.path().to_string_lossy().to_string();

        let blob_store = Arc::new(blob_store::fs::Backend::new(
            &data_store::fs::BackendConfig {
                root_dir: path.clone(),
                sync_to_disk: false,
            },
        ));

        let metadata_store = Arc::new(
            metadata_store::fs::Backend::new(&metadata_store::fs::BackendConfig {
                root_dir: path,
                sync_to_disk: false,
                lock_strategy: metadata_store::LockStrategy::Memory,
            })
            .unwrap(),
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
        self.registry.repositories = repositories;
    }

    pub fn blob_store(&self) -> &blob_store::fs::Backend {
        &self.blob_store
    }

    pub fn temp_dir(&self) -> &TempDir {
        &self.temp_dir
    }
}

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

    fn metadata_store(&self) -> Arc<dyn MetadataStore + Send + Sync> {
        self.metadata_store.clone()
    }
}

pub struct S3RegistryTestCase {
    key_prefix: String,
    s3_blob_store: Arc<blob_store::s3::Backend>,
    s3_metadata_store: Arc<metadata_store::s3::Backend>,
    s3_registry: Registry,
}

impl S3RegistryTestCase {
    pub fn new() -> Self {
        let key_prefix = format!("test-{}", Uuid::new_v4());

        let blob_store = Arc::new(
            blob_store::s3::Backend::new(&data_store::s3::BackendConfig {
                access_key_id: "root".to_string(),
                secret_key: "roottoor".to_string(),
                endpoint: "http://127.0.0.1:9000".to_string(),
                region: "region".to_string(),
                bucket: "registry".to_string(),
                key_prefix: key_prefix.clone(),
                multipart_copy_threshold: ByteSize::mib(5),
                multipart_copy_chunk_size: ByteSize::mib(5),
                multipart_part_size: ByteSize::mib(5),
                ..Default::default()
            })
            .unwrap(),
        );

        let metadata_store = Arc::new(
            metadata_store::s3::Backend::new(
                &metadata_store::s3::BackendConfig {
                    access_key_id: "root".to_string(),
                    secret_key: "roottoor".to_string(),
                    endpoint: "http://127.0.0.1:9000".to_string(),
                    region: "region".to_string(),
                    bucket: "registry".to_string(),
                    key_prefix: key_prefix.clone(),
                    lock_strategy: metadata_store::LockStrategy::Memory,
                    link_cache_ttl: 0,
                    access_time_debounce_secs: 0,
                    capabilities: None,
                },
                None,
            )
            .unwrap(),
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

    fn metadata_store(&self) -> Arc<dyn MetadataStore + Send + Sync> {
        self.s3_metadata_store.clone()
    }
}

impl Drop for S3RegistryTestCase {
    fn drop(&mut self) {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let key_prefix = self.key_prefix.clone();
            let blob_store = self.s3_blob_store.clone();
            handle.spawn(async move {
                if let Err(e) = blob_store.store.delete_prefix(&key_prefix).await {
                    println!("Warning: Failed to clean up S3RegistryTestCase data: {e:?}");
                }
            });
        }
    }
}
