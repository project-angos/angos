use std::{collections::HashMap, path::PathBuf, sync::Arc};

use async_trait::async_trait;
use futures_util::stream::{self, StreamExt};
use serde::Deserialize;
use tracing::{debug, info, instrument};

use crate::{
    oci::{Descriptor, Digest},
    registry::{
        data_store,
        metadata_store::{
            BlobIndex, BlobIndexOperation, Error, LinkMetadata, LinkOperation, LockConfig,
            LockStrategy, MetadataStore,
            link_kind::LinkKind,
            lock::{self, LockBackend, MemoryBackend},
            lock_ops::LockOps,
            referrer_resolver::resolve_referrer_descriptor,
            transaction,
        },
        pagination, path_builder,
    },
};

#[derive(Clone, Debug, PartialEq)]
pub struct BackendConfig {
    pub root_dir: String,
    pub lock_strategy: LockStrategy,
    pub sync_to_disk: bool,
}

impl Default for BackendConfig {
    fn default() -> Self {
        Self {
            root_dir: String::new(),
            lock_strategy: LockStrategy::Memory,
            sync_to_disk: false,
        }
    }
}

impl<'de> Deserialize<'de> for BackendConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Raw {
            root_dir: String,
            #[serde(default)]
            redis: Option<LockConfig>,
            #[serde(default)]
            lock_strategy: Option<LockStrategy>,
            #[serde(default)]
            sync_to_disk: bool,
        }

        let raw = Raw::deserialize(deserializer)?;

        let lock_strategy = lock::resolve_lock_strategy(raw.lock_strategy, raw.redis, false)?;

        Ok(BackendConfig {
            root_dir: raw.root_dir,
            lock_strategy,
            sync_to_disk: raw.sync_to_disk,
        })
    }
}

impl From<BackendConfig> for data_store::fs::BackendConfig {
    fn from(config: BackendConfig) -> Self {
        Self {
            root_dir: config.root_dir,
            sync_to_disk: config.sync_to_disk,
        }
    }
}

#[derive(Clone)]
pub struct Backend {
    store: data_store::fs::Backend,
    lock: Arc<dyn LockBackend + Send + Sync>,
}

impl Backend {
    pub fn new(config: &BackendConfig) -> Result<Self, Error> {
        info!("Using filesystem metadata-store backend");
        let store = data_store::fs::Backend::new(&data_store::fs::BackendConfig {
            root_dir: config.root_dir.clone(),
            sync_to_disk: config.sync_to_disk,
        });

        let lock: Arc<dyn LockBackend + Send + Sync> = match &config.lock_strategy {
            LockStrategy::Redis(redis_config) => {
                info!("Using Redis lock store for filesystem metadata-store");
                let backend = lock::RedisBackend::new(redis_config).map_err(|e| {
                    Error::Lock(format!("Failed to initialize Redis lock store: {e}"))
                })?;
                Arc::new(backend)
            }
            LockStrategy::S3(_) => {
                return Err(Error::Lock(
                    "S3 lock strategy is not supported for filesystem metadata store".to_string(),
                ));
            }
            LockStrategy::Memory => {
                info!("Using in-memory lock store for filesystem metadata-store");
                Arc::new(MemoryBackend::new())
            }
        };

        Ok(Self { store, lock })
    }

    #[instrument(skip(self))]
    async fn collect_repositories(&self, base_path: &str) -> Vec<String> {
        let mut path_stack: Vec<String> = vec![base_path.to_string()];
        let mut repositories = Vec::new();

        while let Some(current_path) = path_stack.pop() {
            if let Ok(entries) = self.store.list_dir(&current_path).await {
                for entry in entries {
                    let path = if current_path.ends_with('/') {
                        format!("{current_path}{entry}")
                    } else if current_path.is_empty() {
                        entry.clone()
                    } else {
                        format!("{current_path}/{entry}")
                    };

                    // check entries starting with a "_": it means it's a repository
                    // add entries not starting with a "_" as paths to explore
                    if entry.starts_with('_') {
                        // Extract the repository name from the parent path
                        if let Some(repo_name) = PathBuf::from(&current_path)
                            .strip_prefix(base_path)
                            .ok()
                            .and_then(|p| p.to_str())
                            && !repo_name.is_empty()
                        {
                            debug!("Found repository: {repo_name}");
                            repositories.push(repo_name.to_string());
                        }
                    } else {
                        debug!("Exploring path: {}", path);
                        path_stack.push(path);
                    }
                }
            }
        }

        repositories.sort();
        repositories
    }
}

#[async_trait]
impl MetadataStore for Backend {
    #[instrument(skip(self))]
    async fn list_namespaces(
        &self,
        n: u16,
        last: Option<String>,
    ) -> Result<(Vec<String>, Option<String>), Error> {
        let base_path = path_builder::repository_dir();

        let mut repositories = self.collect_repositories(base_path).await;
        repositories.dedup();

        Ok(pagination::paginate(&repositories, n, last.as_deref()))
    }

    #[instrument(skip(self))]
    async fn list_tags(
        &self,
        namespace: &str,
        n: u16,
        last: Option<String>,
    ) -> Result<(Vec<String>, Option<String>), Error> {
        let path = path_builder::manifest_tags_dir(namespace);
        debug!("Listing tags in path: {path}");
        let mut tags = self.store.list_dir(&path).await?;
        tags.sort();

        Ok(pagination::paginate(&tags, n, last.as_deref()))
    }

    #[instrument(skip(self))]
    async fn list_referrers(
        &self,
        namespace: &str,
        digest: &Digest,
        artifact_type: Option<String>,
    ) -> Result<Vec<Descriptor>, Error> {
        let path = format!(
            "{}/sha256",
            path_builder::manifest_referrers_dir(namespace, digest)
        );
        let all_manifest = self.store.list_dir(&path).await?;

        let digest_entries: Vec<Digest> = all_manifest
            .into_iter()
            .map(|entry| Digest::Sha256(entry.into()))
            .collect();

        let results: Vec<Option<Descriptor>> = stream::iter(digest_entries)
            .map(|manifest_digest| {
                let artifact_type = artifact_type.as_ref();
                async move {
                    resolve_referrer_descriptor(
                        digest,
                        manifest_digest,
                        artifact_type,
                        |link| async move { self.read_link_reference(namespace, &link).await },
                        |path| async move { self.store.read(&path).await },
                    )
                    .await
                }
            })
            .buffer_unordered(10)
            .collect()
            .await;

        let mut referrers: Vec<Descriptor> = results.into_iter().flatten().collect();
        referrers.sort_by(|a, b| a.digest.cmp(&b.digest));
        Ok(referrers)
    }

    async fn has_referrers(&self, namespace: &str, subject: &Digest) -> Result<bool, Error> {
        let path = format!(
            "{}/sha256",
            path_builder::manifest_referrers_dir(namespace, subject)
        );
        match self.store.list_dir(&path).await {
            Ok(entries) => Ok(!entries.is_empty()),
            Err(_) => Ok(false),
        }
    }

    async fn list_revisions(
        &self,
        namespace: &str,
        n: u16,
        continuation_token: Option<String>,
    ) -> Result<(Vec<Digest>, Option<String>), Error> {
        let path = path_builder::manifest_revisions_link_root_dir(namespace, "sha256");

        let all_revisions = self.store.list_dir(&path).await?;
        let mut revisions = Vec::new();

        for revision in all_revisions {
            revisions.push(Digest::Sha256(revision.into()));
        }

        Ok(pagination::paginate(
            &revisions,
            n,
            continuation_token.as_deref(),
        ))
    }

    async fn count_manifests(&self, namespace: &str) -> Result<usize, Error> {
        let path = path_builder::manifest_revisions_link_root_dir(namespace, "sha256");
        let revisions = self.store.list_dir(&path).await?;
        Ok(revisions.len())
    }

    #[instrument(skip(self))]
    async fn read_blob_index(&self, digest: &Digest) -> Result<BlobIndex, Error> {
        let path = path_builder::blob_index_path(digest);
        let content = self.store.read_to_string(&path).await?;

        let index = serde_json::from_str(&content)?;
        Ok(index)
    }

    async fn update_blob_index(
        &self,
        namespace: &str,
        digest: &Digest,
        operation: BlobIndexOperation,
    ) -> Result<(), Error> {
        let lock_keys = [format!("blob:{digest}")];
        let guard = self.lock.acquire(&lock_keys).await?;

        let mut pending_blob_ops: HashMap<Digest, Vec<BlobIndexOperation>> = HashMap::new();
        pending_blob_ops
            .entry(digest.clone())
            .or_default()
            .push(operation);
        let result = self
            .apply_pending_blob_index_ops(namespace, pending_blob_ops)
            .await;

        let lock_valid = guard.is_valid();
        guard.release().await;

        result?;
        if !lock_valid {
            return Err(Error::Lock(
                "lock invalidated during blob index update".into(),
            ));
        }

        Ok(())
    }

    #[instrument(skip(self))]
    async fn read_link(
        &self,
        namespace: &str,
        link: &LinkKind,
        update_access_time: bool,
    ) -> Result<LinkMetadata, Error> {
        if update_access_time {
            let guard = self.lock.acquire(&[link.to_string()]).await?;
            let link_data = self.read_link_reference(namespace, link).await?.accessed();
            if !guard.is_valid() {
                return Err(Error::Lock(
                    "lock invalidated during access time update".into(),
                ));
            }
            self.write_link_reference(namespace, link, &link_data)
                .await?;
            guard.release().await;
            Ok(link_data)
        } else {
            self.read_link_reference(namespace, link).await
        }
    }

    #[instrument(skip(self))]
    async fn update_links(
        &self,
        namespace: &str,
        operations: &[LinkOperation],
    ) -> Result<(), Error> {
        transaction::run_link_transaction(self, &*self.lock, namespace, operations).await
    }
}

#[async_trait]
impl LockOps for Backend {
    async fn read_link_reference(
        &self,
        namespace: &str,
        link: &LinkKind,
    ) -> Result<LinkMetadata, Error> {
        let link_path = path_builder::link_path(link, namespace);
        let data = self.store.read(&link_path).await?;
        LinkMetadata::from_bytes(data)
    }

    async fn write_link_reference(
        &self,
        namespace: &str,
        link: &LinkKind,
        metadata: &LinkMetadata,
    ) -> Result<(), Error> {
        let link_path = path_builder::link_path(link, namespace);
        let serialized_link_data = serde_json::to_vec(metadata)?;
        self.store.write(&link_path, &serialized_link_data).await?;
        Ok(())
    }

    async fn delete_link_reference(&self, namespace: &str, link: &LinkKind) -> Result<(), Error> {
        let path = path_builder::link_container_path(link, namespace);
        debug!("Deleting link at path: {path}");
        self.store.delete_dir(&path).await?;
        let _ = self.store.delete_empty_parent_dirs(&path).await;
        Ok(())
    }

    fn lock_key_for_link(_namespace: &str, link: &LinkKind) -> String {
        link.to_string()
    }

    async fn apply_pending_blob_index_ops(
        &self,
        namespace: &str,
        pending_blob_ops: HashMap<Digest, Vec<BlobIndexOperation>>,
    ) -> Result<(), Error> {
        for (digest, ops) in &pending_blob_ops {
            let path = path_builder::blob_index_path(digest);

            let mut blob_index = match self.store.read_to_string(&path).await.map_err(Error::from) {
                Ok(content) => serde_json::from_str::<BlobIndex>(&content)?,
                Err(Error::ReferenceNotFound) => BlobIndex::default(),
                Err(e) => Err(e)?,
            };

            let mut ns_links = blob_index.namespace.remove(namespace).unwrap_or_default();
            for op in ops {
                match op {
                    BlobIndexOperation::Insert(link) => {
                        ns_links.insert(link.clone());
                    }
                    BlobIndexOperation::Remove(link) => {
                        ns_links.remove(link);
                    }
                }
            }
            if !ns_links.is_empty() {
                blob_index.namespace.insert(namespace.to_string(), ns_links);
            }

            if blob_index.namespace.is_empty() {
                debug!("Deleting no longer referenced blob index: {digest}");
                self.store.delete(&path).await?;
                let _ = self.store.delete_empty_parent_dirs(&path).await;
            } else {
                let content = serde_json::to_string(&blob_index)?;
                self.store.write(&path, content.as_bytes()).await?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures_util::future::join_all;
    use tempfile::TempDir;

    use super::*;
    use crate::{metrics_provider, util::sha256};

    #[tokio::test]
    async fn removing_last_blob_index_reference_keeps_blob_data() {
        metrics_provider::init_for_tests();

        let temp_dir = TempDir::new().unwrap();
        let backend = Backend::new(&BackendConfig {
            root_dir: temp_dir.path().to_string_lossy().into_owned(),
            ..BackendConfig::default()
        })
        .unwrap();

        let content = b"blob-content";
        let digest = sha256::digest(content);
        let blob_path = path_builder::blob_path(&digest);
        backend.store.write(&blob_path, content).await.unwrap();

        let link = LinkKind::Layer(digest.clone());
        backend
            .update_blob_index(
                "test/repo",
                &digest,
                BlobIndexOperation::Insert(link.clone()),
            )
            .await
            .unwrap();
        backend
            .update_blob_index("test/repo", &digest, BlobIndexOperation::Remove(link))
            .await
            .unwrap();

        let stored_content = backend.store.read(&blob_path).await.unwrap();
        assert_eq!(stored_content, content);
        assert!(matches!(
            backend.read_blob_index(&digest).await,
            Err(Error::ReferenceNotFound)
        ));
    }

    #[tokio::test]
    async fn concurrent_blob_index_updates_preserve_all_namespaces() {
        metrics_provider::init_for_tests();

        let temp_dir = TempDir::new().unwrap();
        let backend = Arc::new(
            Backend::new(&BackendConfig {
                root_dir: temp_dir.path().to_string_lossy().into_owned(),
                ..BackendConfig::default()
            })
            .unwrap(),
        );

        let digest = sha256::digest(b"shared blob");
        let updates = (0..32).map(|index| {
            let backend = Arc::clone(&backend);
            let digest = digest.clone();
            async move {
                let namespace = format!("test/repo-{index}");
                let link = LinkKind::Blob(digest.clone());
                backend
                    .update_blob_index(&namespace, &digest, BlobIndexOperation::Insert(link))
                    .await
                    .unwrap();
            }
        });

        join_all(updates).await;

        let blob_index = backend.read_blob_index(&digest).await.unwrap();
        assert_eq!(blob_index.namespace.len(), 32);
    }
}
