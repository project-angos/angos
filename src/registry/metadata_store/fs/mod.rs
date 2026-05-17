use std::{
    collections::{HashMap, HashSet},
    io::ErrorKind,
    path::PathBuf,
    sync::Arc,
};

use async_trait::async_trait;
use futures_util::stream::{self, StreamExt};
use serde::Deserialize;
use tokio::fs;
use tracing::{debug, info, instrument, warn};

use crate::{
    oci::{Descriptor, Digest},
    registry::{
        fs_ops::{
            atomic_write, list_dir_or_empty, prune_empty_ancestors, remove_dir_all_if_exists,
            remove_file_if_exists,
        },
        metadata_store::{
            BlobIndex, BlobIndexOperation, Error, LinkMetadata, LinkOperation, LockConfig,
            LockStrategy, MetadataStore,
            link_kind::LinkKind,
            lock::{self, LockBackend, LockGuard, MemoryBackend},
            lock_ops::{LockOps, blob_index_lock_key, link_lock_key, with_validated_lock},
            referrer_resolver::resolve_referrer_descriptor,
            sharded::{self, NamespaceRegistry},
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

#[derive(Clone)]
pub struct Backend {
    root: PathBuf,
    sync_to_disk: bool,
    lock: Arc<dyn LockBackend + Send + Sync>,
}

impl Backend {
    pub fn new(config: &BackendConfig) -> Result<Self, Error> {
        info!("Using filesystem metadata-store backend");

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

        Ok(Self {
            root: PathBuf::from(&config.root_dir),
            sync_to_disk: config.sync_to_disk,
            lock,
        })
    }

    fn full_path(&self, path: &str) -> PathBuf {
        self.root.join(path)
    }

    #[instrument(skip(self))]
    async fn collect_repositories(&self, base_path: &str) -> Vec<String> {
        let mut path_stack: Vec<String> = vec![base_path.to_string()];
        let mut repositories = Vec::new();

        while let Some(current_path) = path_stack.pop() {
            if let Ok(entries) = list_dir_or_empty(&self.full_path(&current_path)).await {
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

    async fn read_blob_index_shards(&self, digest: &Digest) -> Result<Option<BlobIndex>, Error> {
        let refs_dir = path_builder::blob_index_refs_dir(digest);
        let mut entries = list_dir_or_empty(&self.full_path(&refs_dir)).await?;
        entries.sort();

        if entries.is_empty() {
            return Ok(None);
        }

        let shard_results = stream::iter(entries.into_iter().map(|entry| {
            let shard_path = format!("{refs_dir}/{entry}");
            async move {
                match fs::read(self.full_path(&shard_path)).await {
                    Ok(data) => {
                        let links = serde_json::from_slice::<HashSet<LinkKind>>(&data)?;
                        if links.is_empty() {
                            Ok(None)
                        } else {
                            Ok(Some((
                                sharded::decode_blob_index_shard_namespace(&entry),
                                links,
                            )))
                        }
                    }
                    Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
                    Err(e) => Err(Error::from(e)),
                }
            }
        }))
        .buffer_unordered(sharded::SHARD_READ_CONCURRENCY)
        .collect::<Vec<Result<Option<(String, HashSet<LinkKind>)>, Error>>>()
        .await;

        let shards = shard_results
            .into_iter()
            .filter_map(Result::transpose)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Some(sharded::collect_blob_index_shards(shards)))
    }

    async fn read_legacy_blob_index(&self, digest: &Digest) -> Result<Option<BlobIndex>, Error> {
        let path = path_builder::blob_index_path(digest);
        match fs::read_to_string(self.full_path(&path)).await {
            Ok(content) => Ok(Some(serde_json::from_str::<BlobIndex>(&content)?)),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
            Err(e) => Err(Error::from(e)),
        }
    }

    async fn migrate_legacy_blob_index_data(
        &self,
        digest: &Digest,
        blob_index: &BlobIndex,
    ) -> Result<(), Error> {
        let lock_keys = [blob_index_lock_key(digest)];
        with_validated_lock(
            &*self.lock,
            &lock_keys,
            "lock invalidated during blob index layout migration",
            || self.migrate_legacy_blob_index_data_locked(digest, blob_index),
        )
        .await
    }

    async fn migrate_legacy_blob_index_data_locked(
        &self,
        digest: &Digest,
        blob_index: &BlobIndex,
    ) -> Result<(), Error> {
        for (namespace, links) in &blob_index.namespace {
            let operations: Vec<BlobIndexOperation> = links
                .iter()
                .map(|link| BlobIndexOperation::Insert(link.clone()))
                .collect();
            self.update_blob_index_shard(namespace, digest, &operations)
                .await?;
        }

        let legacy_path = self.full_path(&path_builder::blob_index_path(digest));
        remove_file_if_exists(&legacy_path).await?;
        let _ = prune_empty_ancestors(&legacy_path, &self.root, 3).await;
        Ok(())
    }

    async fn migrate_blob_index_layout(&self, digest: &Digest) -> Result<(), Error> {
        let Some(blob_index) = self.read_legacy_blob_index(digest).await? else {
            return Ok(());
        };

        self.migrate_legacy_blob_index_data(digest, &blob_index)
            .await?;
        info!(
            "Migrated legacy filesystem blob index for '{digest}' ({} namespaces)",
            blob_index.namespace.len()
        );
        Ok(())
    }

    async fn update_blob_index_shard(
        &self,
        namespace: &str,
        digest: &Digest,
        operations: &[BlobIndexOperation],
    ) -> Result<(), Error> {
        let shard_path = path_builder::blob_index_shard_path(digest, namespace);
        let mut links: HashSet<LinkKind> = match fs::read(self.full_path(&shard_path)).await {
            Ok(data) => serde_json::from_slice(&data).unwrap_or_default(),
            Err(e) if e.kind() == ErrorKind::NotFound => HashSet::new(),
            Err(e) => return Err(Error::from(e)),
        };

        sharded::apply_blob_index_operations(&mut links, operations);

        if links.is_empty() {
            let abs = self.full_path(&shard_path);
            remove_file_if_exists(&abs).await?;
            let _ = prune_empty_ancestors(&abs, &self.root, 3).await;
        } else {
            let content = serde_json::to_vec(&links)?;
            atomic_write(&self.full_path(&shard_path), &content, self.sync_to_disk).await?;
        }

        Ok(())
    }

    async fn read_namespace_registry(&self) -> Result<Option<NamespaceRegistry>, Error> {
        let shard_dir = path_builder::namespace_registry_shard_dir();
        let mut entries = list_dir_or_empty(&self.full_path(&shard_dir)).await?;
        entries.sort();

        if entries.is_empty() {
            let path = path_builder::namespace_registry_path();
            return match fs::read(self.full_path(&path)).await {
                Ok(data) => match serde_json::from_slice::<NamespaceRegistry>(&data) {
                    Ok(registry) => Ok(Some(registry)),
                    Err(error) => {
                        warn!("Corrupt filesystem namespace registry, will rebuild: {error}");
                        Ok(None)
                    }
                },
                Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
                Err(e) => Err(Error::from(e)),
            };
        }

        let shard_results = stream::iter(entries.into_iter().map(|entry| {
            let shard_path = format!("{shard_dir}/{entry}");
            async move {
                match fs::read(self.full_path(&shard_path)).await {
                    Ok(data) => match serde_json::from_slice::<NamespaceRegistry>(&data) {
                        Ok(registry) => Ok(registry.namespaces),
                        Err(error) => {
                            warn!(
                                "Corrupt filesystem namespace registry shard '{shard_path}', ignoring: {error}"
                            );
                            Ok(Vec::new())
                        }
                    },
                    Err(e) if e.kind() == ErrorKind::NotFound => Ok(Vec::new()),
                    Err(e) => Err(Error::from(e)),
                }
            }
        }))
        .buffer_unordered(sharded::SHARD_READ_CONCURRENCY)
        .collect::<Vec<Result<Vec<String>, Error>>>()
        .await;

        let mut namespaces = Vec::new();
        for result in shard_results {
            namespaces.extend(result?);
        }
        let namespaces = sharded::normalize_namespaces(namespaces);

        Ok(Some(NamespaceRegistry { namespaces }))
    }

    async fn rebuild_namespace_registry(&self) -> Result<(), Error> {
        let base_path = path_builder::repository_dir();
        let mut namespaces = self.collect_repositories(base_path).await;
        namespaces.dedup();

        let shards = sharded::group_namespaces_by_shard(namespaces);

        for (shard_key, shard_namespaces) in shards {
            self.process_namespace_shard(&shard_key, &shard_namespaces)
                .await?;
        }

        Ok(())
    }

    async fn process_namespace_shard(
        &self,
        shard_key: &str,
        shard_namespaces: &[String],
    ) -> Result<(), Error> {
        let lock_keys = [format!("namespace_registry_shard_{shard_key}")];
        with_validated_lock(
            &*self.lock,
            &lock_keys,
            "lock invalidated during namespace registry rebuild",
            || async {
                let registry = sharded::registry_for_namespaces(shard_namespaces);
                let content = serde_json::to_vec(&registry)?;
                let path = format!(
                    "{}/{shard_key}.json",
                    path_builder::namespace_registry_shard_dir()
                );
                atomic_write(&self.full_path(&path), &content, self.sync_to_disk)
                    .await
                    .map_err(Error::from)
            },
        )
        .await
    }

    async fn register_namespace(&self, namespace: &str) -> Result<(), Error> {
        let shard_key = path_builder::namespace_shard_key(namespace);
        let path = path_builder::namespace_registry_shard_path(namespace);
        let lock_keys = [format!("namespace_registry_shard_{shard_key}")];
        with_validated_lock(
            &*self.lock,
            &lock_keys,
            "lock invalidated during namespace registry update",
            || async {
                let mut registry = match fs::read(self.full_path(&path)).await {
                    Ok(data) => {
                        serde_json::from_slice::<NamespaceRegistry>(&data).unwrap_or_default()
                    }
                    Err(e) if e.kind() == ErrorKind::NotFound => NamespaceRegistry::default(),
                    Err(e) => return Err(Error::from(e)),
                };

                if sharded::insert_sorted_unique(&mut registry.namespaces, namespace) {
                    let content = serde_json::to_vec(&registry)?;
                    atomic_write(&self.full_path(&path), &content, self.sync_to_disk).await?;
                }
                Ok(())
            },
        )
        .await
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
        let repositories = if let Some(registry) = self.read_namespace_registry().await? {
            registry.namespaces
        } else {
            let base_path = path_builder::repository_dir();
            let mut repositories = self.collect_repositories(base_path).await;
            repositories.dedup();
            repositories
        };

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
        let mut tags = list_dir_or_empty(&self.full_path(&path)).await?;
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
        let all_manifest = list_dir_or_empty(&self.full_path(&path)).await?;

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
                        |path| async move { fs::read(self.full_path(&path)).await },
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
        match list_dir_or_empty(&self.full_path(&path)).await {
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

        let all_revisions = list_dir_or_empty(&self.full_path(&path)).await?;
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
        let revisions = list_dir_or_empty(&self.full_path(&path)).await?;
        Ok(revisions.len())
    }

    #[instrument(skip(self))]
    async fn read_blob_index(&self, digest: &Digest) -> Result<BlobIndex, Error> {
        if let Some(index) = self.read_blob_index_shards(digest).await? {
            if index.namespace.is_empty() {
                return Err(Error::ReferenceNotFound);
            }
            return Ok(index);
        }

        let Some(index) = self.read_legacy_blob_index(digest).await? else {
            return Err(Error::ReferenceNotFound);
        };

        self.migrate_legacy_blob_index_data(digest, &index).await?;
        info!(
            "Migrated legacy filesystem blob index for '{digest}' ({} namespaces)",
            index.namespace.len()
        );

        Ok(index)
    }

    #[instrument(skip(self))]
    async fn has_blob_references(&self, digest: &Digest) -> Result<bool, Error> {
        if let Some(index) = self.read_blob_index_shards(digest).await? {
            return Ok(index.namespace.values().any(|links| !links.is_empty()));
        }

        let Some(index) = self.read_legacy_blob_index(digest).await? else {
            return Ok(false);
        };
        Ok(index.namespace.values().any(|links| !links.is_empty()))
    }

    #[instrument(skip(self))]
    async fn read_blob_index_namespace(
        &self,
        namespace: &str,
        digest: &Digest,
    ) -> Result<HashSet<LinkKind>, Error> {
        let shard_path = path_builder::blob_index_shard_path(digest, namespace);
        match fs::read(self.full_path(&shard_path)).await {
            Ok(data) => {
                let links = serde_json::from_slice::<HashSet<LinkKind>>(&data)?;
                sharded::non_empty_links_or_not_found(links)
            }
            Err(e) if e.kind() == ErrorKind::NotFound => {
                let blob_index = self.read_blob_index(digest).await?;
                sharded::namespace_links_from_index(&blob_index, namespace)
            }
            Err(e) => Err(Error::from(e)),
        }
    }

    async fn update_blob_index(
        &self,
        namespace: &str,
        digest: &Digest,
        operation: BlobIndexOperation,
    ) -> Result<(), Error> {
        let lock_keys = [blob_index_lock_key(digest)];
        with_validated_lock(
            &*self.lock,
            &lock_keys,
            "lock invalidated during blob index update",
            || async {
                let mut pending_blob_ops: HashMap<Digest, Vec<BlobIndexOperation>> = HashMap::new();
                pending_blob_ops
                    .entry(digest.clone())
                    .or_default()
                    .push(operation);
                self.apply_pending_blob_index_ops(namespace, pending_blob_ops)
                    .await
            },
        )
        .await
    }

    #[instrument(skip(self))]
    async fn migrate_blob_index(&self, digest: &Digest) -> Result<(), Error> {
        self.migrate_blob_index_layout(digest).await
    }

    #[instrument(skip(self))]
    async fn migrate_namespace_registry(&self) -> Result<(), Error> {
        self.rebuild_namespace_registry().await?;
        let legacy_path = self.full_path(&path_builder::namespace_registry_path());
        remove_file_if_exists(&legacy_path).await?;
        let _ = prune_empty_ancestors(&legacy_path, &self.root, 3).await;
        Ok(())
    }

    async fn acquire_blob_data_lock(&self, digest: &Digest) -> Result<LockGuard, Error> {
        self.lock.acquire(&[format!("blob-data:{digest}")]).await
    }

    #[instrument(skip(self))]
    async fn read_link(
        &self,
        namespace: &str,
        link: &LinkKind,
        update_access_time: bool,
    ) -> Result<LinkMetadata, Error> {
        if update_access_time {
            let lock_keys = [link_lock_key(namespace, link)];
            with_validated_lock(
                &*self.lock,
                &lock_keys,
                "lock invalidated during access time update",
                || async {
                    let link_data = self.read_link_reference(namespace, link).await?.accessed();
                    self.write_link_reference(namespace, link, &link_data)
                        .await?;
                    Ok(link_data)
                },
            )
            .await
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
        let data = fs::read(self.full_path(&link_path)).await?;
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
        atomic_write(
            &self.full_path(&link_path),
            &serialized_link_data,
            self.sync_to_disk,
        )
        .await?;
        Ok(())
    }

    async fn delete_link_reference(&self, namespace: &str, link: &LinkKind) -> Result<(), Error> {
        let path = self.full_path(&path_builder::link_container_path(link, namespace));
        debug!("Deleting link at path: {}", path.display());
        remove_dir_all_if_exists(&path).await?;
        let _ = prune_empty_ancestors(&path, &self.root, 4).await;
        Ok(())
    }

    async fn apply_pending_blob_index_ops(
        &self,
        namespace: &str,
        pending_blob_ops: HashMap<Digest, Vec<BlobIndexOperation>>,
    ) -> Result<(), Error> {
        for (digest, ops) in &pending_blob_ops {
            if let Some(legacy_index) = self.read_legacy_blob_index(digest).await? {
                self.migrate_legacy_blob_index_data_locked(digest, &legacy_index)
                    .await?;
            }
            self.update_blob_index_shard(namespace, digest, ops).await?;
        }
        Ok(())
    }

    async fn after_update(&self, namespace: &str, had_creates: bool) -> Result<(), Error> {
        if had_creates && let Err(error) = self.register_namespace(namespace).await {
            warn!(namespace, error = %error, "Failed to register filesystem namespace");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, sync::Arc};

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
        atomic_write(
            &backend.root.join(&blob_path),
            content,
            backend.sync_to_disk,
        )
        .await
        .unwrap();

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

        let stored_content = fs::read(backend.root.join(&blob_path)).await.unwrap();
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

    #[tokio::test]
    async fn update_blob_index_writes_namespace_shard_not_legacy_index() {
        metrics_provider::init_for_tests();

        let temp_dir = TempDir::new().unwrap();
        let backend = Backend::new(&BackendConfig {
            root_dir: temp_dir.path().to_string_lossy().into_owned(),
            ..BackendConfig::default()
        })
        .unwrap();

        let digest = sha256::digest(b"sharded fs blob index");
        let namespace = "test/repo";
        let link = LinkKind::Layer(digest.clone());
        backend
            .update_blob_index(namespace, &digest, BlobIndexOperation::Insert(link.clone()))
            .await
            .unwrap();

        let shard_path = path_builder::blob_index_shard_path(&digest, namespace);
        let links: HashSet<LinkKind> =
            serde_json::from_slice(&fs::read(backend.root.join(&shard_path)).await.unwrap())
                .unwrap();
        assert!(links.contains(&link));

        let legacy_path = path_builder::blob_index_path(&digest);
        assert_eq!(
            fs::read(backend.root.join(&legacy_path))
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::NotFound
        );
    }

    #[tokio::test]
    async fn read_blob_index_migrates_legacy_index_to_namespace_shard() {
        metrics_provider::init_for_tests();

        let temp_dir = TempDir::new().unwrap();
        let backend = Backend::new(&BackendConfig {
            root_dir: temp_dir.path().to_string_lossy().into_owned(),
            ..BackendConfig::default()
        })
        .unwrap();

        let digest = sha256::digest(b"legacy fs blob index");
        let namespace = "legacy/repo";
        let link = LinkKind::Config(digest.clone());
        let mut blob_index = BlobIndex::default();
        blob_index
            .namespace
            .insert(namespace.to_string(), HashSet::from([link.clone()]));

        let legacy_path = path_builder::blob_index_path(&digest);
        atomic_write(
            &backend.root.join(&legacy_path),
            &serde_json::to_vec(&blob_index).unwrap(),
            backend.sync_to_disk,
        )
        .await
        .unwrap();

        let migrated = backend.read_blob_index(&digest).await.unwrap();
        assert_eq!(migrated.namespace, blob_index.namespace);
        assert_eq!(
            fs::read(backend.root.join(&legacy_path))
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::NotFound
        );

        let shard_path = path_builder::blob_index_shard_path(&digest, namespace);
        let links: HashSet<LinkKind> =
            serde_json::from_slice(&fs::read(backend.root.join(&shard_path)).await.unwrap())
                .unwrap();
        assert!(links.contains(&link));
    }

    #[tokio::test]
    async fn update_links_registers_namespace_in_sharded_registry() {
        metrics_provider::init_for_tests();

        let temp_dir = TempDir::new().unwrap();
        let backend = Backend::new(&BackendConfig {
            root_dir: temp_dir.path().to_string_lossy().into_owned(),
            ..BackendConfig::default()
        })
        .unwrap();

        let digest = sha256::digest(b"namespace registry fs");
        let namespace = "catalog/repo";
        let link = LinkKind::Digest(digest.clone());
        backend
            .update_links(
                namespace,
                &[LinkOperation::Create {
                    link,
                    target: digest,
                    referrer: None,
                    media_type: None,
                    descriptor: None,
                }],
            )
            .await
            .unwrap();

        let shard_path = path_builder::namespace_registry_shard_path(namespace);
        let registry: NamespaceRegistry =
            serde_json::from_slice(&fs::read(backend.root.join(&shard_path)).await.unwrap())
                .unwrap();
        assert_eq!(registry.namespaces, vec![namespace.to_string()]);
    }
}
