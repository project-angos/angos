use std::collections::HashMap;
use std::sync::Arc;

use bytesize::ByteSize;
use tempfile::TempDir;
use uuid::Uuid;

use crate::registry::blob_store::BlobStore;
use crate::registry::data_store;
use crate::registry::metadata_store;
use crate::registry::metadata_store::MetadataStore;
use crate::registry::test_utils::create_test_registry;
use crate::registry::{Registry, Repository, blob_store};

pub trait RegistryTestCase {
    fn registry(&self) -> &Registry;
    fn blob_store(&self) -> Arc<dyn BlobStore>;
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

        let blob_store = blob_store::fs::Backend::new(&data_store::fs::BackendConfig {
            root_dir: path.clone(),
            sync_to_disk: false,
        });
        let blob_store = Arc::new(blob_store);

        let metadata_store = metadata_store::fs::Backend::new(&metadata_store::fs::BackendConfig {
            root_dir: path,
            sync_to_disk: false,
            redis: None,
        })
        .unwrap();
        let metadata_store = Arc::new(metadata_store);
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

        let blob_store = blob_store::s3::Backend::new(&data_store::s3::BackendConfig {
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
        .unwrap();
        let blob_store = Arc::new(blob_store);

        let metadata_store = metadata_store::s3::Backend::new(&metadata_store::s3::BackendConfig {
            access_key_id: "root".to_string(),
            secret_key: "roottoor".to_string(),
            endpoint: "http://127.0.0.1:9000".to_string(),
            region: "region".to_string(),
            bucket: "registry".to_string(),
            key_prefix: key_prefix.clone(),
            redis: None,
            link_cache_ttl: 0,
        })
        .unwrap();
        let metadata_store = Arc::new(metadata_store);

        let registry = create_test_registry(blob_store.clone(), metadata_store.clone());

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
