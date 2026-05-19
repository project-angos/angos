use std::{collections::HashMap, fmt, sync::Arc};

use tracing::instrument;

pub mod blob;
pub mod blob_ownership;
pub mod blob_store;
pub mod content_discovery;
mod error;
#[cfg(test)]
mod event_emission_tests;
mod ext;
mod fs_ops;
mod headers;
pub mod manifest;
pub mod metadata_store;
pub mod pagination;
mod path_builder;
pub mod repository;
pub mod repository_resolver;
pub mod task_queue;
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
    configuration::RegexPattern,
    oci::{Digest, Namespace},
    registry::{
        blob_ownership::BlobOwnership,
        blob_store::{BlobStore, Error as BlobStoreError, PresignedBlobStore, UploadStore},
        metadata_store::{Error as MetadataError, MetadataStore},
        repository_resolver::RepositoryResolver,
        task_queue::TaskQueue,
    },
};

#[allow(clippy::struct_excessive_bools)]
pub struct RegistryConfig {
    pub update_pull_time: bool,
    pub enable_blob_redirect: bool,
    pub enable_manifest_redirect: bool,
    pub concurrent_cache_jobs: usize,
    pub global_immutable_tags: bool,
    pub global_immutable_tags_exclusions: Vec<RegexPattern>,
    pub max_manifest_size_bytes: usize,
}

impl Default for RegistryConfig {
    fn default() -> Self {
        Self {
            update_pull_time: false,
            enable_blob_redirect: true,
            enable_manifest_redirect: true,
            concurrent_cache_jobs: 4,
            global_immutable_tags: false,
            global_immutable_tags_exclusions: Vec::new(),
            max_manifest_size_bytes: manifest::DEFAULT_MAX_MANIFEST_SIZE_BYTES,
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

    pub fn concurrent_cache_jobs(mut self, jobs: usize) -> Self {
        self.concurrent_cache_jobs = jobs;
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
}

#[allow(clippy::struct_excessive_bools)]
pub struct Registry {
    blob_store: Arc<dyn BlobStore>,
    upload_store: Arc<dyn UploadStore>,
    presigned_blob_store: Option<Arc<dyn PresignedBlobStore>>,
    metadata_store: Arc<dyn MetadataStore + Send + Sync>,
    resolver: Arc<RepositoryResolver>,
    enable_blob_redirect: bool,
    enable_manifest_redirect: bool,
    update_pull_time: bool,
    task_queue: TaskQueue,
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
    #[instrument(skip(
        blob_store,
        upload_store,
        presigned_blob_store,
        metadata_store,
        resolver,
        config
    ))]
    pub fn new(
        blob_store: Arc<dyn BlobStore>,
        upload_store: Arc<dyn UploadStore>,
        presigned_blob_store: Option<Arc<dyn PresignedBlobStore>>,
        metadata_store: Arc<dyn MetadataStore>,
        resolver: Arc<RepositoryResolver>,
        config: RegistryConfig,
    ) -> Result<Self, Error> {
        let res = Self {
            update_pull_time: config.update_pull_time,
            enable_blob_redirect: config.enable_blob_redirect,
            enable_manifest_redirect: config.enable_manifest_redirect,
            blob_store,
            upload_store,
            presigned_blob_store,
            metadata_store,
            resolver,
            task_queue: TaskQueue::new(config.concurrent_cache_jobs)?,
            global_immutable_tags: config.global_immutable_tags,
            global_immutable_tags_exclusions: config.global_immutable_tags_exclusions,
            max_manifest_size_bytes: config.max_manifest_size_bytes,
        };

        Ok(res)
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
        let guard = self.metadata_store.acquire_blob_data_lock(digest).await?;
        let result = self.delete_blob_data_if_unreferenced_locked(digest).await;
        let lock_valid = guard.is_valid();
        guard.release().await;

        result?;
        if !lock_valid {
            return Err(
                MetadataError::Lock("lock invalidated during blob data deletion".into()).into(),
            );
        }

        Ok(())
    }

    async fn delete_blob_data_if_unreferenced_locked(&self, digest: &Digest) -> Result<(), Error> {
        let ownership = BlobOwnership::new(self.metadata_store.as_ref());
        if ownership.has_any_reference(digest).await? {
            return Ok(());
        }

        match self.blob_store.delete(digest).await {
            Ok(()) | Err(BlobStoreError::BlobNotFound | BlobStoreError::ReferenceNotFound) => {
                Ok(())
            }
            Err(error) => Err(error.into()),
        }
    }
}
