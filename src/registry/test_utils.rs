use std::{collections::HashMap, sync::Arc};

use bytesize::ByteSize;
use tempfile::TempDir;
use uuid::Uuid;

use super::*;
use crate::{
    configuration::GlobalConfig,
    metrics_provider,
    oci::{Digest, Namespace},
    policy::{AccessMode, AccessPolicyConfig, RetentionPolicy, RetentionPolicyConfig, SystemClock},
    registry::{
        blob_store::{self, BlobStore, PresignedBlobStore, UploadStore},
        metadata_store,
        metadata_store::{LinkOperation, MetadataStore, link_kind::LinkKind},
        repository_resolver::RepositoryResolver,
    },
    secret::Secret,
};
use angos_s3_client as s3_client;

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
    metadata_store: Arc<dyn MetadataStore + Send + Sync>,
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
        .concurrent_cache_jobs(global.max_concurrent_cache_jobs)
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

pub async fn create_test_blob(
    registry: &Registry,
    namespace: &Namespace,
    content: &[u8],
) -> (Digest, Repository) {
    let digest = registry.blob_store.create(content).await.unwrap();

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
    fn metadata_store(&self) -> Arc<dyn MetadataStore + Send + Sync>;
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
    metadata_store: Arc<metadata_store::Backend>,
    registry: Registry,
    temp_dir: TempDir,
}

impl FSRegistryTestCase {
    pub fn new() -> Self {
        let temp_dir = TempDir::new().expect("Failed to create temp dir for FSBackendConfig");
        let path = temp_dir.path().to_string_lossy().to_string();

        let blob_store = Arc::new(blob_store::fs::Backend::new(
            &blob_store::fs::BackendConfig {
                root_dir: path.clone(),
                sync_to_disk: false,
            },
        ));

        let metadata_store = Arc::new(
            metadata_store::fs::BackendConfig {
                root_dir: path,
                sync_to_disk: false,
                lock_strategy: metadata_store::LockStrategy::Memory,
            }
            .to_backend()
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

    fn metadata_store(&self) -> Arc<dyn MetadataStore + Send + Sync> {
        self.metadata_store.clone()
    }
}

pub struct S3RegistryTestCase {
    key_prefix: String,
    s3_blob_store: Arc<blob_store::s3::Backend>,
    s3_metadata_store: Arc<metadata_store::Backend>,
    s3_registry: Registry,
}

impl S3RegistryTestCase {
    pub fn new() -> Self {
        let key_prefix = format!("test-{}", Uuid::new_v4());

        let blob_store = Arc::new(
            blob_store::s3::Backend::new(&s3_client::BackendConfig {
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
            metadata_store::s3::BackendConfig {
                access_key_id: Secret::new("root".to_string()),
                secret_key: Secret::new("roottoor".to_string()),
                endpoint: "http://127.0.0.1:9000".to_string(),
                region: "region".to_string(),
                bucket: "registry".to_string(),
                key_prefix: key_prefix.clone(),
                lock_strategy: metadata_store::LockStrategy::Memory,
                link_cache_ttl: 0,
                access_time_debounce_secs: 0,
                capabilities: None,
            }
            .to_backend(
                Some(metadata_store::ConditionalCapabilities {
                    put_if_none_match: true,
                    put_if_match: true,
                    delete_if_match: false,
                }),
                None,
            )
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

    fn metadata_store(&self) -> Arc<dyn MetadataStore + Send + Sync> {
        self.s3_metadata_store.clone()
    }

    async fn cleanup(&self) {
        S3RegistryTestCase::cleanup(self).await;
    }
}

/// No-op `MultipartCleanup` for tests that need to construct a scrub `Executor`
/// or `MultipartChecker` but don't exercise multipart cleanup behaviour.
///
/// Returns an empty list of orphans and silently succeeds on abort. Use when
/// the test's subject under test doesn't touch S3 multipart uploads.
pub struct NoopMultipart;

#[async_trait::async_trait]
impl blob_store::MultipartCleanup for NoopMultipart {
    async fn list_orphan_multipart_uploads(
        &self,
        _timeout: chrono::Duration,
    ) -> Result<Vec<blob_store::OrphanMultipartUpload>, blob_store::Error> {
        Ok(vec![])
    }

    async fn abort_orphan_multipart_upload(
        &self,
        _upload: &blob_store::OrphanMultipartUpload,
    ) -> Result<(), blob_store::Error> {
        Ok(())
    }
}
