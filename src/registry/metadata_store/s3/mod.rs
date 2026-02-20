use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::future::join_all;
use futures_util::stream::{self, StreamExt};
use serde::Deserialize;
use tracing::{debug, info, instrument, warn};

use crate::cache::{Cache, CacheExt};
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
    #[serde(default = "default_link_cache_ttl")]
    pub link_cache_ttl: u64,
}

fn default_link_cache_ttl() -> u64 {
    30
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
    cache: Option<Arc<dyn Cache>>,
    link_cache_ttl: u64,
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

        Ok(Self {
            store,
            lock,
            cache: None,
            link_cache_ttl: config.link_cache_ttl,
        })
    }

    pub fn with_cache(mut self, cache: Arc<dyn Cache>) -> Self {
        self.cache = Some(cache);
        self
    }

    fn cache_key(namespace: &str, link: &LinkKind) -> String {
        format!("link:{namespace}:{link}")
    }

    async fn cache_get(&self, namespace: &str, link: &LinkKind) -> Option<LinkMetadata> {
        if self.link_cache_ttl == 0 {
            return None;
        }
        let cache = self.cache.as_ref()?;
        cache
            .retrieve::<LinkMetadata>(&Self::cache_key(namespace, link))
            .await
            .ok()
            .flatten()
    }

    async fn cache_put(&self, namespace: &str, link: &LinkKind, metadata: &LinkMetadata) {
        if self.link_cache_ttl == 0 {
            return;
        }
        if let Some(cache) = &self.cache {
            let _ = cache
                .store(
                    &Self::cache_key(namespace, link),
                    metadata,
                    self.link_cache_ttl,
                )
                .await;
        }
    }

    async fn cache_invalidate(&self, namespace: &str, link: &LinkKind) {
        if let Some(cache) = &self.cache {
            let _ = cache
                .store_value(&Self::cache_key(namespace, link), "", 0)
                .await;
        }
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
            self.cache_put(namespace, link, &link_data).await;
            Ok(link_data)
        } else {
            if let Some(cached) = self.cache_get(namespace, link).await {
                return Ok(cached);
            }
            let link_data = self.read_link_reference(namespace, link).await?;
            self.cache_put(namespace, link, &link_data).await;
            Ok(link_data)
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

            for (link, _, _, _) in &creates {
                self.cache_invalidate(namespace, link).await;
            }
            for (link, _, _) in &valid_deletes {
                self.cache_invalidate(namespace, link).await;
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

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use crate::cache;
    use crate::cache::CacheExt;
    use crate::registry::metadata_store::{LinkOperation, MetadataStore};

    fn test_config() -> BackendConfig {
        BackendConfig {
            access_key_id: "root".to_string(),
            secret_key: "roottoor".to_string(),
            endpoint: "http://127.0.0.1:9000".to_string(),
            region: "region".to_string(),
            bucket: "registry".to_string(),
            key_prefix: format!("test-cache-{}", uuid::Uuid::new_v4()),
            redis: None,
            link_cache_ttl: 30,
        }
    }

    fn test_backend_with_cache(config: &BackendConfig) -> (Backend, Arc<dyn cache::Cache>) {
        let cache: Arc<dyn cache::Cache> = Arc::new(cache::memory::Backend::new());
        let backend = Backend::new(config).unwrap().with_cache(cache.clone());
        (backend, cache)
    }

    #[tokio::test]
    async fn test_read_link_cache_hit_skips_s3() {
        let config = test_config();
        let (backend, _cache) = test_backend_with_cache(&config);
        let namespace = "cache-hit-ns";
        let digest = Digest::from_str(
            "sha256:a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
        )
        .unwrap();
        let tag = LinkKind::Tag("latest".into());

        // Create link via update_links
        let ops = vec![LinkOperation::Create {
            link: tag.clone(),
            target: digest.clone(),
            referrer: None,
        }];
        backend.update_links(namespace, &ops).await.unwrap();

        // First read populates cache
        let meta = backend.read_link(namespace, &tag, false).await.unwrap();
        assert_eq!(meta.target, digest);

        // Delete the S3 object directly
        let link_path = path_builder::link_path(&tag, namespace);
        backend.store.delete(&link_path).await.unwrap();

        // Second read should succeed from cache
        let meta = backend.read_link(namespace, &tag, false).await.unwrap();
        assert_eq!(meta.target, digest);
    }

    #[tokio::test]
    async fn test_read_link_cache_miss_fetches_from_s3() {
        let config = test_config();
        let (backend, cache) = test_backend_with_cache(&config);
        let namespace = "cache-miss-ns";
        let digest = Digest::from_str(
            "sha256:b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3",
        )
        .unwrap();
        let tag = LinkKind::Tag("latest".into());

        // Create link via update_links
        let ops = vec![LinkOperation::Create {
            link: tag.clone(),
            target: digest.clone(),
            referrer: None,
        }];
        backend.update_links(namespace, &ops).await.unwrap();

        // First read should return correct data from S3
        let meta = backend.read_link(namespace, &tag, false).await.unwrap();
        assert_eq!(meta.target, digest);

        // Verify cache was populated
        let cache_key = format!("link:{namespace}:{tag}");
        let cached: Option<LinkMetadata> = cache.retrieve(&cache_key).await.unwrap();
        assert!(cached.is_some(), "Cache should be populated after read");
        assert_eq!(cached.unwrap().target, digest);
    }

    #[tokio::test]
    async fn test_read_link_cache_expired_refetches() {
        let mut config = test_config();
        config.link_cache_ttl = 1;
        let (backend, _cache) = test_backend_with_cache(&config);
        let namespace = "cache-expired-ns";
        let digest_a = Digest::from_str(
            "sha256:c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4",
        )
        .unwrap();
        let digest_b = Digest::from_str(
            "sha256:d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5",
        )
        .unwrap();
        let tag = LinkKind::Tag("latest".into());

        // Create link pointing to digest_a
        let ops = vec![LinkOperation::Create {
            link: tag.clone(),
            target: digest_a.clone(),
            referrer: None,
        }];
        backend.update_links(namespace, &ops).await.unwrap();

        // Read to populate cache
        let meta = backend.read_link(namespace, &tag, false).await.unwrap();
        assert_eq!(meta.target, digest_a);

        // Wait for cache to expire
        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;

        // Write new data directly to S3 (bypassing cache invalidation)
        let new_metadata = LinkMetadata::from_digest(digest_b.clone());
        backend
            .write_link_reference(namespace, &tag, &new_metadata)
            .await
            .unwrap();

        // Read again should get new digest (cache expired)
        let meta = backend.read_link(namespace, &tag, false).await.unwrap();
        assert_eq!(meta.target, digest_b);
    }

    #[tokio::test]
    async fn test_update_links_invalidates_cache() {
        let config = test_config();
        let (backend, _cache) = test_backend_with_cache(&config);
        let namespace = "cache-invalidate-ns";
        let digest_a = Digest::from_str(
            "sha256:e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6",
        )
        .unwrap();
        let digest_b = Digest::from_str(
            "sha256:f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1",
        )
        .unwrap();
        let tag = LinkKind::Tag("latest".into());

        // Create tag pointing to digest_a
        let ops = vec![LinkOperation::Create {
            link: tag.clone(),
            target: digest_a.clone(),
            referrer: None,
        }];
        backend.update_links(namespace, &ops).await.unwrap();

        // Read to populate cache
        let meta = backend.read_link(namespace, &tag, false).await.unwrap();
        assert_eq!(meta.target, digest_a);

        // Update tag to point to digest_b via update_links
        let ops = vec![LinkOperation::Create {
            link: tag.clone(),
            target: digest_b.clone(),
            referrer: None,
        }];
        backend.update_links(namespace, &ops).await.unwrap();

        // Read should return digest_b (cache was invalidated by update_links)
        let meta = backend.read_link(namespace, &tag, false).await.unwrap();
        assert_eq!(meta.target, digest_b);
    }

    #[tokio::test]
    async fn test_read_link_with_access_time_update_populates_cache() {
        let config = test_config();
        let (backend, _cache) = test_backend_with_cache(&config);
        let namespace = "cache-access-time-ns";
        let digest = Digest::from_str(
            "sha256:a1a2a3a4a5a6a7a8a1a2a3a4a5a6a7a8a1a2a3a4a5a6a7a8a1a2a3a4a5a6a7a8",
        )
        .unwrap();
        let tag = LinkKind::Tag("latest".into());

        // Create link
        let ops = vec![LinkOperation::Create {
            link: tag.clone(),
            target: digest.clone(),
            referrer: None,
        }];
        backend.update_links(namespace, &ops).await.unwrap();

        // Read with access time update
        let meta = backend.read_link(namespace, &tag, true).await.unwrap();
        assert_eq!(meta.target, digest);
        assert!(
            meta.accessed_at.is_some(),
            "accessed_at should be set after read with update_access_time=true"
        );

        // Subsequent read without access time update should return cached value with accessed_at
        let meta = backend.read_link(namespace, &tag, false).await.unwrap();
        assert_eq!(meta.target, digest);
        assert!(
            meta.accessed_at.is_some(),
            "accessed_at should be present in cached value"
        );
    }

    #[tokio::test]
    async fn test_cache_disabled_when_ttl_zero() {
        let mut config = test_config();
        config.link_cache_ttl = 0;
        let (backend, _cache) = test_backend_with_cache(&config);
        let namespace = "cache-disabled-ns";
        let digest = Digest::from_str(
            "sha256:b1b2b3b4b5b6b7b8b1b2b3b4b5b6b7b8b1b2b3b4b5b6b7b8b1b2b3b4b5b6b7b8",
        )
        .unwrap();
        let tag = LinkKind::Tag("latest".into());

        // Create link
        let ops = vec![LinkOperation::Create {
            link: tag.clone(),
            target: digest.clone(),
            referrer: None,
        }];
        backend.update_links(namespace, &ops).await.unwrap();

        // Read once
        let meta = backend.read_link(namespace, &tag, false).await.unwrap();
        assert_eq!(meta.target, digest);

        // Delete S3 object directly
        let link_path = path_builder::link_path(&tag, namespace);
        backend.store.delete(&link_path).await.unwrap();

        // Read again should fail (no caching when ttl is 0)
        let result = backend.read_link(namespace, &tag, false).await;
        assert!(
            matches!(result, Err(Error::ReferenceNotFound)),
            "Should get ReferenceNotFound when cache is disabled and S3 object is deleted"
        );
    }

    #[tokio::test]
    async fn test_cache_keys_are_namespace_scoped() {
        let config = test_config();
        let (backend, _cache) = test_backend_with_cache(&config);
        let namespace_a = "cache-scope-ns-a";
        let namespace_b = "cache-scope-ns-b";
        let digest_a = Digest::from_str(
            "sha256:c1c2c3c4c5c6c7c8c1c2c3c4c5c6c7c8c1c2c3c4c5c6c7c8c1c2c3c4c5c6c7c8",
        )
        .unwrap();
        let digest_b = Digest::from_str(
            "sha256:d1d2d3d4d5d6d7d8d1d2d3d4d5d6d7d8d1d2d3d4d5d6d7d8d1d2d3d4d5d6d7d8",
        )
        .unwrap();
        let tag = LinkKind::Tag("latest".into());

        // Create same tag in two namespaces pointing to different digests
        let ops_a = vec![LinkOperation::Create {
            link: tag.clone(),
            target: digest_a.clone(),
            referrer: None,
        }];
        backend.update_links(namespace_a, &ops_a).await.unwrap();

        let ops_b = vec![LinkOperation::Create {
            link: tag.clone(),
            target: digest_b.clone(),
            referrer: None,
        }];
        backend.update_links(namespace_b, &ops_b).await.unwrap();

        // Read both - each should return its own digest
        let meta_a = backend.read_link(namespace_a, &tag, false).await.unwrap();
        let meta_b = backend.read_link(namespace_b, &tag, false).await.unwrap();

        assert_eq!(meta_a.target, digest_a);
        assert_eq!(meta_b.target, digest_b);
    }
}
