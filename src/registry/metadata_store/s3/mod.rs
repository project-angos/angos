use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::future::join_all;
use futures_util::stream::{self, StreamExt};
use serde::Deserialize;
use tracing::{debug, info, instrument, warn};

use crate::oci::{Descriptor, Digest, Manifest};
use crate::registry::metadata_store::link_kind::LinkKind;
use crate::registry::metadata_store::lock::{self, LockBackend, MemoryBackend};
use crate::registry::metadata_store::{BlobIndex, Error};
use crate::registry::metadata_store::{
    BlobIndexOperation, LinkMetadata, LinkOperation, LockConfig, MetadataStore,
};
use crate::registry::{data_store, pagination, path_builder};

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct BackendConfig {
    pub access_key_id: String,
    pub secret_key: String,
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    #[serde(default)]
    pub key_prefix: String,
    #[serde(default)]
    pub redis: Option<LockConfig>,
}

impl From<BackendConfig> for data_store::s3::BackendConfig {
    fn from(config: BackendConfig) -> Self {
        Self {
            access_key_id: config.access_key_id,
            secret_key: config.secret_key,
            endpoint: config.endpoint,
            bucket: config.bucket,
            region: config.region,
            key_prefix: config.key_prefix,
            ..Default::default()
        }
    }
}

#[derive(Clone)]
pub struct Backend {
    pub store: data_store::s3::Backend,
    lock: Arc<dyn LockBackend<Guard = Box<dyn Send>> + Send + Sync>,
}

impl Backend {
    pub fn new(config: &BackendConfig) -> Result<Self, Error> {
        info!("Using S3 metadata-store backend");
        let store = data_store::s3::Backend::new(&data_store::s3::BackendConfig {
            access_key_id: config.access_key_id.clone(),
            secret_key: config.secret_key.clone(),
            endpoint: config.endpoint.clone(),
            bucket: config.bucket.clone(),
            region: config.region.clone(),
            key_prefix: config.key_prefix.clone(),
            ..Default::default()
        })?;

        let lock: Arc<dyn LockBackend<Guard = Box<dyn Send>> + Send + Sync> =
            if let Some(redis_config) = &config.redis {
                info!("Using Redis lock store for S3 metadata-store");
                let backend = lock::RedisBackend::new(redis_config).map_err(|e| {
                    Error::Lock(format!("Failed to initialize Redis lock store: {e}"))
                })?;
                Arc::new(backend)
            } else {
                info!("Using in-memory lock store for S3 metadata-store");
                Arc::new(MemoryBackend::new())
            };

        Ok(Self { store, lock })
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
        debug!("Fetching {n} namespace(s) with continuation token: {last:?}");

        let repo_dir = path_builder::repository_dir();
        let mut namespaces = Vec::new();
        self.collect_namespaces(repo_dir, "", &mut namespaces)
            .await?;
        namespaces.sort();

        Ok(pagination::paginate_sorted(&namespaces, n, last.as_deref()))
    }

    #[instrument(skip(self))]
    async fn list_tags(
        &self,
        namespace: &str,
        n: u16,
        last: Option<String>,
    ) -> Result<(Vec<String>, Option<String>), Error> {
        debug!("Listing {n} tag(s) for namespace '{namespace}' starting with last '{last:?}'");
        let tags_dir = path_builder::manifest_tags_dir(namespace);

        let (tags, _, next_token) = self
            .store
            .list_prefixes(&tags_dir, "/", i32::from(n), None, last)
            .await?;

        let continuation = if next_token.is_some() {
            tags.last().cloned()
        } else {
            None
        };

        Ok((tags, continuation))
    }

    #[instrument(skip(self))]
    async fn list_referrers(
        &self,
        namespace: &str,
        digest: &Digest,
        artifact_type: Option<String>,
    ) -> Result<Vec<Descriptor>, Error> {
        let referrers_dir = path_builder::manifest_referrers_dir(namespace, digest);

        let mut referrers = Vec::new();
        let mut continuation_token = None;

        loop {
            let (objects, next_token) = self
                .store
                .list_objects(&referrers_dir, 100, continuation_token)
                .await?;

            let digest_entries: Vec<(Digest, String)> = objects
                .iter()
                .filter_map(|key| {
                    let parts: Vec<&str> = key.split('/').collect();
                    if parts.len() < 2 || parts[0] != "sha256" {
                        return None;
                    }
                    let manifest_digest = Digest::Sha256(parts[1].into());
                    let blob_path = path_builder::blob_path(&manifest_digest);
                    Some((manifest_digest, blob_path))
                })
                .collect();

            let results: Vec<Option<Descriptor>> = stream::iter(digest_entries)
                .map(|(manifest_digest, blob_path)| {
                    let store = &self.store;
                    let artifact_type = artifact_type.as_ref();
                    async move {
                        match store.read(&blob_path).await {
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

            referrers.extend(results.into_iter().flatten());

            continuation_token = next_token;
            if continuation_token.is_none() {
                break;
            }
        }

        referrers.sort_by(|a, b| a.digest.cmp(&b.digest));
        Ok(referrers)
    }

    async fn has_referrers(&self, namespace: &str, subject: &Digest) -> Result<bool, Error> {
        let referrers_dir = path_builder::manifest_referrers_dir(namespace, subject);

        let (objects, _) = self.store.list_objects(&referrers_dir, 1, None).await?;

        Ok(!objects.is_empty())
    }

    async fn list_revisions(
        &self,
        namespace: &str,
        n: u16,
        continuation_token: Option<String>,
    ) -> Result<(Vec<Digest>, Option<String>), Error> {
        debug!(
            "Fetching {n} revision(s) for namespace '{namespace}' with continuation token: {continuation_token:?}"
        );
        let revisions_dir = path_builder::manifest_revisions_link_root_dir(namespace, "sha256");

        let (prefixes, _, next_last) = self
            .store
            .list_prefixes(&revisions_dir, "/", i32::from(n), continuation_token, None)
            .await?;

        let mut revisions = Vec::new();
        for key in prefixes {
            revisions.push(Digest::Sha256(key.into()));
        }

        Ok((revisions, next_last))
    }

    async fn count_manifests(&self, namespace: &str) -> Result<usize, Error> {
        let revisions_dir = path_builder::manifest_revisions_link_root_dir(namespace, "sha256");
        let mut count = 0;
        let mut continuation_token = None;

        loop {
            let (prefixes, _, next_token) = self
                .store
                .list_prefixes(&revisions_dir, "/", 1000, continuation_token, None)
                .await?;

            count += prefixes.len();
            continuation_token = next_token;
            if continuation_token.is_none() {
                break;
            }
        }

        Ok(count)
    }

    #[instrument(skip(self))]
    async fn read_blob_index(&self, digest: &Digest) -> Result<BlobIndex, Error> {
        let path = path_builder::blob_index_path(digest);

        let data = match self.store.read(&path).await {
            Ok(data) => data,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(Error::ReferenceNotFound);
            }
            Err(e) => return Err(e.into()),
        };
        let index = serde_json::from_slice(&data)?;

        Ok(index)
    }

    #[instrument(skip(self))]
    async fn update_blob_index(
        &self,
        namespace: &str,
        digest: &Digest,
        operation: BlobIndexOperation,
    ) -> Result<(), Error> {
        let path = path_builder::blob_index_path(digest);

        let mut reference_index = match self.store.read(&path).await {
            Ok(data) => serde_json::from_slice::<BlobIndex>(&data)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => BlobIndex::default(),
            Err(e) => return Err(e.into()),
        };

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
            let path = path_builder::blob_container_dir(digest);
            self.store.delete_prefix(&path).await?;
        } else {
            let content = Bytes::from(serde_json::to_vec(&reference_index)?);
            self.store.put_object(&path, content).await?;
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
                    } => {
                        let old_target = self
                            .read_link_reference(namespace, link)
                            .await
                            .ok()
                            .map(|m| m.target);
                        (
                            op,
                            Some((link.clone(), target.clone(), old_target, referrer.clone())),
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
            let mut creates: Vec<(LinkKind, Digest, Option<Digest>, Option<Digest>)> = Vec::new();
            let mut deletes: Vec<(LinkKind, Digest, Option<Digest>)> = Vec::new();

            for (_, create_data, delete_data) in prelock_results {
                if let Some((link, target, old_target, referrer)) = create_data {
                    lock_keys.push(link.to_string());
                    lock_keys.push(format!("blob:{target}"));
                    if let Some(ref old) = old_target {
                        lock_keys.push(format!("blob:{old}"));
                    }
                    creates.push((link, target, old_target, referrer));
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

            let validation_results =
                join_all(creates.iter().map(|(link, _, expected_old, _)| async move {
                    let current = self.read_link_reference(namespace, link).await.ok();
                    let current_target = current.as_ref().map(|m| m.target.clone());
                    (link.clone(), current, current_target, expected_old.clone())
                }))
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
            for (link, target, old_target, referrer) in &creates {
                let is_tracked = is_tracked_link(link);

                if is_tracked && referrer.is_some() {
                    let mut metadata = link_cache
                        .remove(link)
                        .unwrap_or_else(|| LinkMetadata::from_digest(target.clone()));

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

                    non_tracked_create_writes
                        .push((link.clone(), LinkMetadata::from_digest(target.clone())));
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
                let mut blob_index = match self.store.read(&path).await {
                    Ok(data) => serde_json::from_slice::<BlobIndex>(&data)?,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => BlobIndex::default(),
                    Err(e) => return Err(e.into()),
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
                    self.store.delete_prefix(&container).await?;
                } else {
                    let content = Bytes::from(serde_json::to_vec(&blob_index)?);
                    self.store.put_object(&path, content).await?;
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
    async fn collect_namespaces(
        &self,
        path: &str,
        prefix: &str,
        namespaces: &mut Vec<String>,
    ) -> Result<(), Error> {
        let mut continuation_token = None;
        loop {
            let (prefixes, _, next_token) = self
                .store
                .list_prefixes(path, "/", 1000, continuation_token, None)
                .await?;

            for entry in &prefixes {
                if entry.starts_with('_') {
                    if entry == "_manifests" {
                        let namespace = prefix.strip_suffix('/').unwrap_or(prefix);
                        if !namespace.is_empty() {
                            namespaces.push(namespace.to_string());
                        }
                    }
                    continue;
                }

                let child_path = format!("{path}/{entry}");
                let child_prefix = format!("{prefix}{entry}/");
                Box::pin(self.collect_namespaces(&child_path, &child_prefix, namespaces)).await?;
            }

            continuation_token = next_token;
            if continuation_token.is_none() {
                break;
            }
        }
        Ok(())
    }

    async fn read_link_reference(
        &self,
        namespace: &str,
        link: &LinkKind,
    ) -> Result<LinkMetadata, Error> {
        let link_path = path_builder::link_path(link, namespace);
        match self.store.read(&link_path).await {
            Ok(data) => LinkMetadata::from_bytes(data),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Err(Error::ReferenceNotFound),
            Err(e) => Err(e.into()),
        }
    }

    async fn write_link_reference(
        &self,
        namespace: &str,
        link: &LinkKind,
        metadata: &LinkMetadata,
    ) -> Result<(), Error> {
        let link_path = path_builder::link_path(link, namespace);
        let serialized_link_data = Bytes::from(serde_json::to_vec(metadata)?);
        self.store
            .put_object(&link_path, serialized_link_data)
            .await?;
        Ok(())
    }

    async fn delete_link_reference(&self, namespace: &str, link: &LinkKind) -> Result<(), Error> {
        let link_path = path_builder::link_path(link, namespace);
        self.store.delete(&link_path).await?;
        Ok(())
    }
}
