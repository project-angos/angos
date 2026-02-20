use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use tracing::instrument;

pub mod blob;
pub mod blob_store;
pub mod cel;
pub mod content_discovery;
pub mod data_store;
mod error;
mod ext;
pub mod manifest;
pub mod metadata_store;
pub mod pagination;
mod path_builder;
pub mod repository;
pub mod task_queue;
#[cfg(test)]
pub mod tests;
pub mod upload;
mod version;

pub use error::Error;
pub use manifest::parse_manifest_digests;
pub use repository::Repository;

use crate::cache;
use crate::oci::Namespace;
pub use crate::policy::AccessPolicy;
use crate::registry::blob_store::BlobStore;
use crate::registry::metadata_store::MetadataStore;
use crate::registry::task_queue::TaskQueue;

pub struct RegistryConfig {
    pub update_pull_time: bool,
    pub enable_redirect: bool,
    pub concurrent_cache_jobs: usize,
    pub global_immutable_tags: bool,
    pub global_immutable_tags_exclusions: Vec<String>,
}

impl Default for RegistryConfig {
    fn default() -> Self {
        Self {
            update_pull_time: false,
            enable_redirect: true,
            concurrent_cache_jobs: 4,
            global_immutable_tags: false,
            global_immutable_tags_exclusions: Vec::new(),
        }
    }
}

impl RegistryConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn update_pull_time(mut self, enabled: bool) -> Self {
        self.update_pull_time = enabled;
        self
    }

    pub fn enable_redirect(mut self, enabled: bool) -> Self {
        self.enable_redirect = enabled;
        self
    }

    pub fn concurrent_cache_jobs(mut self, jobs: usize) -> Self {
        self.concurrent_cache_jobs = jobs;
        self
    }

    pub fn global_immutable_tags(mut self, enabled: bool) -> Self {
        self.global_immutable_tags = enabled;
        self
    }

    pub fn global_immutable_tags_exclusions(mut self, exclusions: Vec<String>) -> Self {
        self.global_immutable_tags_exclusions = exclusions;
        self
    }
}

pub struct Registry {
    blob_store: Arc<dyn BlobStore + Send + Sync>,
    metadata_store: Arc<dyn MetadataStore + Send + Sync>,
    repositories: Arc<HashMap<String, Repository>>,
    enable_redirect: bool,
    update_pull_time: bool,
    task_queue: TaskQueue,
    global_immutable_tags: bool,
    global_immutable_tags_exclusions: Vec<String>,
}

impl Debug for Registry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Registry").finish()
    }
}

impl Registry {
    #[instrument(skip(blob_store, metadata_store, repositories, config))]
    pub fn new(
        blob_store: Arc<dyn BlobStore>,
        metadata_store: Arc<dyn MetadataStore>,
        repositories: Arc<HashMap<String, Repository>>,
        config: RegistryConfig,
    ) -> Result<Self, Error> {
        let res = Self {
            update_pull_time: config.update_pull_time,
            enable_redirect: config.enable_redirect,
            blob_store,
            metadata_store,
            repositories,
            task_queue: TaskQueue::new(config.concurrent_cache_jobs, "cache-worker")?,
            global_immutable_tags: config.global_immutable_tags,
            global_immutable_tags_exclusions: config.global_immutable_tags_exclusions,
        };

        Ok(res)
    }

    pub async fn flush_pending_writes(&self) {
        self.metadata_store.flush_access_times().await;
    }

    #[instrument]
    pub fn get_repository_for_namespace(
        &self,
        namespace: &Namespace,
    ) -> Result<&Repository, Error> {
        self.repositories
            .iter()
            .find(|(repository, _)| {
                namespace.as_ref() == repository.as_str()
                    || namespace.starts_with(&format!("{repository}/"))
            })
            .map(|(_, repository)| repository)
            .ok_or(Error::NameUnknown)
    }
}

#[cfg(test)]
pub mod test_utils {
    use super::*;
    use crate::configuration::GlobalConfig;
    use crate::oci::Digest;
    use crate::policy::{AccessPolicyConfig, RetentionPolicyConfig};
    use crate::registry::metadata_store::MetadataStoreExt;
    use crate::registry::metadata_store::link_kind::LinkKind;

    pub fn create_test_repositories() -> Arc<HashMap<String, Repository>> {
        let token_cache = cache::Config::default().to_backend().unwrap();

        let config = repository::Config {
            access_policy: AccessPolicyConfig {
                default_allow: true,
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
        metadata_store: Arc<dyn MetadataStore + Send + Sync>,
    ) -> Registry {
        let repositories_config = create_test_repositories();
        let global = GlobalConfig::default();

        let config = RegistryConfig::new()
            .update_pull_time(global.update_pull_time)
            .enable_redirect(global.enable_redirect)
            .concurrent_cache_jobs(global.max_concurrent_cache_jobs)
            .global_immutable_tags(global.immutable_tags)
            .global_immutable_tags_exclusions(global.immutable_tags_exclusions.clone());

        Registry::new(blob_store, metadata_store, repositories_config, config).unwrap()
    }

    pub async fn create_test_blob(
        registry: &Registry,
        namespace: &str,
        content: &[u8],
    ) -> (Digest, Repository) {
        // Create a test blob
        let digest = registry.blob_store.create_blob(content).await.unwrap();

        // Create a tag to ensure the namespace exists
        let tag_link = LinkKind::Tag("latest".to_string());
        let mut tx = registry.metadata_store.begin_transaction(namespace);
        tx.create_link(&tag_link, &digest);
        tx.commit().await.unwrap();

        // Verify the blob index is updated
        let blob_index = registry
            .metadata_store
            .read_blob_index(&digest)
            .await
            .unwrap();
        assert!(blob_index.namespace.contains_key(namespace));
        let namespace_links = blob_index.namespace.get(namespace).unwrap();
        assert!(namespace_links.contains(&tag_link));

        // Create a non-pull-through repository
        let cache = cache::Config::Memory.to_backend().unwrap();
        let repository = Repository::new(
            "test-repo",
            &repository::Config {
                upstream: Vec::new(),
                access_policy: AccessPolicyConfig::default(),
                retention_policy: RetentionPolicyConfig { rules: Vec::new() },
                immutable_tags: false,
                immutable_tags_exclusions: Vec::new(),
                authorization_webhook: None,
                event_webhooks: Vec::new(),
            },
            &cache,
        )
        .unwrap();

        (digest, repository)
    }
}
