use std::{collections::HashMap, path::PathBuf, sync::Arc};

use async_trait::async_trait;
use futures_util::{
    future::join_all,
    stream::{self, StreamExt},
};
use serde::Deserialize;
use tracing::{debug, info, instrument, warn};

use crate::{
    oci::{Descriptor, Digest, Manifest},
    registry::{
        data_store,
        metadata_store::{
            BlobIndex, BlobIndexOperation, Error, LinkMetadata, LinkOperation, LockConfig,
            MetadataStore,
            link_kind::LinkKind,
            lock::{self, LockBackend, MemoryBackend},
        },
        pagination, path_builder,
    },
};

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct BackendConfig {
    pub root_dir: String,
    #[serde(default)]
    pub redis: Option<LockConfig>,
    #[serde(default)]
    pub sync_to_disk: bool,
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
    lock: Arc<dyn LockBackend<Guard = Box<dyn Send>> + Send + Sync>,
}

impl Backend {
    pub fn new(config: &BackendConfig) -> Result<Self, Error> {
        info!("Using filesystem metadata-store backend");
        let store = data_store::fs::Backend::new(&data_store::fs::BackendConfig {
            root_dir: config.root_dir.clone(),
            sync_to_disk: config.sync_to_disk,
        });

        let lock: Arc<dyn LockBackend<Guard = Box<dyn Send>> + Send + Sync> =
            if let Some(redis_config) = &config.redis {
                info!("Using Redis lock store for filesystem metadata-store");
                let backend = lock::RedisBackend::new(redis_config).map_err(|e| {
                    Error::Lock(format!("Failed to initialize Redis lock store: {e}"))
                })?;
                Arc::new(backend)
            } else {
                info!("Using in-memory lock store for filesystem metadata-store");
                Arc::new(MemoryBackend::new())
            };

        Ok(Self { store, lock })
    }

    //

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
                    let referrer_link = LinkKind::Referrer(digest.clone(), manifest_digest.clone());
                    if let Ok(metadata) = self.read_link_reference(namespace, &referrer_link).await
                        && let Some(desc) = metadata.descriptor
                    {
                        match artifact_type {
                            Some(at) if desc.artifact_type.as_ref() == Some(at) => {
                                return Some(desc);
                            }
                            None => return Some(desc),
                            Some(_) if desc.artifact_type.is_none() => {}
                            Some(_) => return None,
                        }
                    }

                    let blob_path = path_builder::blob_path(&manifest_digest);
                    match self.store.read(&blob_path).await {
                        Ok(data) => {
                            let manifest_len = data.len();
                            match Manifest::from_slice(&data) {
                                Ok(manifest) => manifest.to_descriptor(
                                    artifact_type,
                                    manifest_digest,
                                    manifest_len as u64,
                                ),
                                Err(e) => {
                                    warn!("Failed to parse manifest at {blob_path}: {e}");
                                    None
                                }
                            }
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                            warn!("Referrer blob not found at {blob_path}, skipping");
                            None
                        }
                        Err(e) => {
                            warn!("Failed to read referrer blob at {blob_path}: {e}");
                            None
                        }
                    }
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
        let path = path_builder::manifest_revisions_link_root_dir(namespace, "sha256"); // HACK: hardcoded sha256

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
        debug!("Ensuring container directory for digest: {digest}");

        debug!("Updating reference count for digest: {digest}");
        let path = path_builder::blob_index_path(digest);

        let mut reference_index = match self.store.read_to_string(&path).await.map_err(Error::from)
        {
            Ok(content) => serde_json::from_str::<BlobIndex>(&content)?,
            Err(Error::ReferenceNotFound) => BlobIndex::default(),
            Err(e) => Err(e)?,
        };

        debug!("Updating reference index");
        let mut index = reference_index
            .namespace
            .remove(namespace)
            .unwrap_or_default();
        match operation {
            BlobIndexOperation::Insert(link) => {
                index.insert(link);
            }
            BlobIndexOperation::Remove(link) => {
                index.remove(&link);
            }
        }
        if !index.is_empty() {
            reference_index
                .namespace
                .insert(namespace.to_string(), index);
        }

        if reference_index.namespace.is_empty() {
            debug!("Deleting no longer referenced Blob: {digest}");
            let path = path_builder::blob_container_dir(digest);
            self.store.delete_dir(&path).await?;
            let _ = self.store.delete_empty_parent_dirs(&path).await;
        } else {
            debug!("Writing reference count to path: {path}");
            let content = serde_json::to_string(&reference_index)?;
            self.store.write(&path, content.as_bytes()).await?;
            debug!("Reference index for {digest} updated");
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
            let _guard = self.lock.acquire(&[link.to_string()]).await?;
            let link_data = self.read_link_reference(namespace, link).await?.accessed();
            self.write_link_reference(namespace, link, &link_data)
                .await?;
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
        if operations.is_empty() {
            return Ok(());
        }

        loop {
            let mut link_cache: HashMap<LinkKind, LinkMetadata> = HashMap::new();

            let prelock_results = join_all(operations.iter().map(|op| async move {
                match op {
                    LinkOperation::Create {
                        link,
                        target,
                        referrer,
                        media_type,
                        descriptor,
                    } => {
                        let old_target = self
                            .read_link_reference(namespace, link)
                            .await
                            .ok()
                            .map(|m| m.target);
                        (
                            op,
                            Some((
                                link.clone(),
                                target.clone(),
                                old_target,
                                referrer.clone(),
                                media_type.clone(),
                                descriptor.as_ref().clone(),
                            )),
                            None,
                        )
                    }
                    LinkOperation::Delete { link, referrer } => {
                        let metadata = self.read_link_reference(namespace, link).await.ok();
                        (op, None, Some((link.clone(), metadata, referrer.clone())))
                    }
                }
            }))
            .await;

            let mut lock_keys: Vec<String> = Vec::new();
            let mut creates: Vec<(
                LinkKind,
                Digest,
                Option<Digest>,
                Option<Digest>,
                Option<String>,
                Option<Descriptor>,
            )> = Vec::new();
            let mut deletes: Vec<(LinkKind, Digest, Option<Digest>)> = Vec::new();

            for (_, create_data, delete_data) in prelock_results {
                if let Some((link, target, old_target, referrer, media_type, descriptor)) =
                    create_data
                {
                    lock_keys.push(link.to_string());
                    lock_keys.push(format!("blob:{target}"));
                    if let Some(ref old) = old_target {
                        lock_keys.push(format!("blob:{old}"));
                    }
                    creates.push((link, target, old_target, referrer, media_type, descriptor));
                } else if let Some((link, Some(meta), referrer)) = delete_data {
                    lock_keys.push(link.to_string());
                    lock_keys.push(format!("blob:{}", meta.target));
                    deletes.push((link, meta.target, referrer));
                }
            }

            if creates.is_empty() && deletes.is_empty() {
                return Ok(());
            }

            lock_keys.sort();
            lock_keys.dedup();
            let _guard = self.lock.acquire(&lock_keys).await?;

            let validation_results = join_all(creates.iter().map(
                |(link, _, expected_old, _, _, _)| async move {
                    let current = self.read_link_reference(namespace, link).await.ok();
                    let current_target = current.as_ref().map(|m| m.target.clone());
                    (link.clone(), current, current_target, expected_old.clone())
                },
            ))
            .await;

            if validation_results
                .iter()
                .any(|(_, _, current_target, expected)| *current_target != *expected)
            {
                continue;
            }
            for (link, metadata, _, _) in validation_results {
                if let Some(m) = metadata {
                    link_cache.insert(link, m);
                }
            }

            let delete_results =
                join_all(deletes.iter().map(|(link, target, referrer)| async move {
                    let result = self.read_link_reference(namespace, link).await;
                    (link.clone(), target.clone(), referrer.clone(), result)
                }))
                .await;

            let mut valid_deletes = Vec::new();
            let mut needs_retry = false;
            for (link, target, referrer, result) in delete_results {
                match result {
                    Ok(metadata) if metadata.target == target => {
                        link_cache.insert(link.clone(), metadata);
                        valid_deletes.push((link, target, referrer));
                    }
                    Ok(_) => {
                        needs_retry = true;
                        break;
                    }
                    Err(Error::ReferenceNotFound) => {}
                    Err(e) => return Err(e),
                }
            }
            if needs_retry {
                continue;
            }

            let mut pending_blob_ops: HashMap<Digest, Vec<BlobIndexOperation>> = HashMap::new();

            // Tracked creates: sequential (read-modify-write with blob index)
            // Non-tracked creates: accumulate blob index ops, then parallel link writes
            let mut non_tracked_create_writes: Vec<(LinkKind, LinkMetadata)> = Vec::new();
            for (link, target, old_target, referrer, media_type, descriptor) in &creates {
                let is_tracked = is_tracked_link(link);

                if is_tracked && referrer.is_some() {
                    let mut metadata = link_cache.remove(link).unwrap_or_else(|| {
                        LinkMetadata::from_digest(target.clone())
                            .with_media_type(media_type.clone())
                    });

                    if let Some(manifest_digest) = referrer {
                        metadata.add_referrer(manifest_digest.clone());
                    }

                    if old_target.is_none() {
                        pending_blob_ops
                            .entry(target.clone())
                            .or_default()
                            .push(BlobIndexOperation::Insert(link.clone()));
                    }

                    self.write_link_reference(namespace, link, &metadata)
                        .await?;
                } else {
                    pending_blob_ops
                        .entry(target.clone())
                        .or_default()
                        .push(BlobIndexOperation::Insert(link.clone()));
                    if let Some(old) = old_target
                        && *old != *target
                    {
                        pending_blob_ops
                            .entry(old.clone())
                            .or_default()
                            .push(BlobIndexOperation::Remove(link.clone()));
                    }

                    non_tracked_create_writes.push((
                        link.clone(),
                        LinkMetadata::from_digest(target.clone())
                            .with_media_type(media_type.clone())
                            .with_descriptor((*descriptor).clone()),
                    ));
                }
            }
            join_all(
                non_tracked_create_writes
                    .iter()
                    .map(|(link, metadata)| async move {
                        self.write_link_reference(namespace, link, metadata).await
                    }),
            )
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;

            // Tracked deletes: sequential (read-modify-write with blob index)
            // Non-tracked deletes: parallel link deletes, then accumulate blob index ops
            let mut non_tracked_delete_links: Vec<LinkKind> = Vec::new();
            for (link, target, referrer) in &valid_deletes {
                let is_tracked = is_tracked_link(link);

                if is_tracked && referrer.is_some() {
                    if let Some(mut metadata) = link_cache.remove(link) {
                        if let Some(manifest_digest) = referrer {
                            metadata.remove_referrer(manifest_digest);
                        }

                        if metadata.has_references() {
                            self.write_link_reference(namespace, link, &metadata)
                                .await?;
                        } else {
                            self.delete_link_reference(namespace, link).await?;
                            pending_blob_ops
                                .entry(target.clone())
                                .or_default()
                                .push(BlobIndexOperation::Remove(link.clone()));
                        }
                    }
                } else {
                    non_tracked_delete_links.push(link.clone());
                    pending_blob_ops
                        .entry(target.clone())
                        .or_default()
                        .push(BlobIndexOperation::Remove(link.clone()));
                }
            }
            join_all(
                non_tracked_delete_links
                    .iter()
                    .map(|link| async move { self.delete_link_reference(namespace, link).await }),
            )
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;

            for (digest, ops) in &pending_blob_ops {
                let path = path_builder::blob_index_path(digest);
                let mut blob_index =
                    match self.store.read_to_string(&path).await.map_err(Error::from) {
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
                    let container = path_builder::blob_container_dir(digest);
                    self.store.delete_dir(&container).await?;
                    let _ = self.store.delete_empty_parent_dirs(&container).await;
                } else {
                    let content = serde_json::to_string(&blob_index)?;
                    self.store.write(&path, content.as_bytes()).await?;
                }
            }

            return Ok(());
        }
    }
}

fn is_tracked_link(link: &LinkKind) -> bool {
    matches!(
        link,
        LinkKind::Layer(_) | LinkKind::Config(_) | LinkKind::Manifest(_, _)
    )
}

impl Backend {
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
}
