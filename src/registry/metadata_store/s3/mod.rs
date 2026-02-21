use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::future::join_all;
use futures_util::stream::{self, StreamExt};
use serde::Deserialize;
use tokio::sync::Mutex;
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
    #[serde(default = "default_access_time_debounce")]
    pub access_time_debounce_secs: u64,
}

fn default_link_cache_ttl() -> u64 {
    30
}

fn default_access_time_debounce() -> u64 {
    60
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
struct AccessTimeWriter {
    pending: Arc<Mutex<HashMap<String, (String, LinkKind)>>>,
}

impl AccessTimeWriter {
    fn new() -> Self {
        Self {
            pending: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn record(&self, namespace: &str, link: &LinkKind) {
        let key = format!("{namespace}:{link}");
        self.pending
            .lock()
            .await
            .insert(key, (namespace.to_string(), link.clone()));
    }

    async fn flush(&self, backend: &Backend) {
        let entries: Vec<(String, LinkKind)> = {
            let mut pending = self.pending.lock().await;
            pending.drain().map(|(_, v)| v).collect()
        };

        stream::iter(entries)
            .for_each_concurrent(10, |(namespace, link)| async move {
                if let Err(e) = Self::flush_one(backend, &namespace, &link).await {
                    warn!("Failed to flush access time for {namespace}:{link}: {e}");
                }
            })
            .await;
    }

    async fn flush_one(backend: &Backend, namespace: &str, link: &LinkKind) -> Result<(), Error> {
        let _guard = backend.lock.acquire(&[link.to_string()]).await?;
        let link_data = backend
            .read_link_reference(namespace, link)
            .await?
            .accessed();
        backend
            .write_link_reference(namespace, link, &link_data)
            .await?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct Backend {
    pub store: data_store::s3::Backend,
    lock: Arc<dyn LockBackend<Guard = Box<dyn Send>> + Send + Sync>,
    cache: Option<Arc<dyn Cache>>,
    link_cache_ttl: u64,
    access_time_writer: Option<AccessTimeWriter>,
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

        let access_time_writer = if config.access_time_debounce_secs > 0 {
            Some(AccessTimeWriter::new())
        } else {
            None
        };

        let backend = Self {
            store,
            lock,
            cache: None,
            link_cache_ttl: config.link_cache_ttl,
            access_time_writer,
        };

        if config.access_time_debounce_secs > 0 {
            let flush_backend = backend.clone();
            let interval = Duration::from_secs(config.access_time_debounce_secs);
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(interval).await;
                    flush_backend.flush_access_times().await;
                }
            });
        }

        Ok(backend)
    }

    pub async fn flush_access_times(&self) {
        if let Some(writer) = &self.access_time_writer {
            writer.flush(self).await;
        }
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
            if let Some(writer) = &self.access_time_writer {
                let link_data = if let Some(cached) = self.cache_get(namespace, link).await {
                    cached
                } else {
                    let data = self.read_link_reference(namespace, link).await?;
                    self.cache_put(namespace, link, &data).await;
                    data
                };
                writer.record(namespace, link).await;
                Ok(link_data)
            } else {
                let _guard = self.lock.acquire(&[link.to_string()]).await?;
                let link_data = self.read_link_reference(namespace, link).await?.accessed();
                self.write_link_reference(namespace, link, &link_data)
                    .await?;
                self.cache_put(namespace, link, &link_data).await;
                Ok(link_data)
            }
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
                        media_type,
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
            )> = Vec::new();
            let mut deletes: Vec<(LinkKind, Digest, Option<Digest>)> = Vec::new();

            for (_, create_data, delete_data) in prelock_results {
                if let Some((link, target, old_target, referrer, media_type)) = create_data {
                    lock_keys.push(link.to_string());
                    lock_keys.push(format!("blob:{target}"));
                    if let Some(ref old) = old_target {
                        lock_keys.push(format!("blob:{old}"));
                    }
                    creates.push((link, target, old_target, referrer, media_type));
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
                |(link, _, expected_old, _, _)| async move {
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

            let mut tracked_create_writes: Vec<(LinkKind, LinkMetadata)> = Vec::new();
            let mut non_tracked_create_writes: Vec<(LinkKind, LinkMetadata)> = Vec::new();
            for (link, target, old_target, referrer, media_type) in &creates {
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

                    tracked_create_writes.push((link.clone(), metadata));
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
                            .with_media_type(media_type.clone()),
                    ));
                }
            }
            join_all(
                tracked_create_writes
                    .iter()
                    .chain(non_tracked_create_writes.iter())
                    .map(|(link, metadata)| async move {
                        self.write_link_reference(namespace, link, metadata).await
                    }),
            )
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;

            let mut tracked_delete_writes: Vec<(LinkKind, LinkMetadata)> = Vec::new();
            let mut tracked_delete_removes: Vec<LinkKind> = Vec::new();
            let mut non_tracked_delete_links: Vec<LinkKind> = Vec::new();
            for (link, target, referrer) in &valid_deletes {
                let is_tracked = is_tracked_link(link);

                if is_tracked && referrer.is_some() {
                    if let Some(mut metadata) = link_cache.remove(link) {
                        if let Some(manifest_digest) = referrer {
                            metadata.remove_referrer(manifest_digest);
                        }

                        if metadata.has_references() {
                            tracked_delete_writes.push((link.clone(), metadata));
                        } else {
                            tracked_delete_removes.push(link.clone());
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
                tracked_delete_writes
                    .iter()
                    .map(|(link, metadata)| {
                        let fut: std::pin::Pin<
                            Box<dyn std::future::Future<Output = Result<(), Error>> + Send + '_>,
                        > = Box::pin(self.write_link_reference(namespace, link, metadata));
                        fut
                    })
                    .chain(
                        tracked_delete_removes
                            .iter()
                            .chain(non_tracked_delete_links.iter())
                            .map(|link| {
                                let fut: std::pin::Pin<
                                    Box<
                                        dyn std::future::Future<Output = Result<(), Error>>
                                            + Send
                                            + '_,
                                    >,
                                > = Box::pin(self.delete_link_reference(namespace, link));
                                fut
                            }),
                    ),
            )
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;

            join_all(pending_blob_ops.iter().map(|(digest, ops)| async move {
                let path = path_builder::blob_index_path(digest);
                let mut blob_index = match self.store.read(&path).await {
                    Ok(data) => serde_json::from_slice::<BlobIndex>(&data)?,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => BlobIndex::default(),
                    Err(e) => return Err(Error::from(e)),
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
                Ok(())
            }))
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;

            for (link, metadata) in tracked_create_writes
                .iter()
                .chain(non_tracked_create_writes.iter())
            {
                self.cache_put(namespace, link, metadata).await;
            }
            for (link, _, _) in &valid_deletes {
                self.cache_invalidate(namespace, link).await;
            }

            return Ok(());
        }
    }

    async fn flush_access_times(&self) {
        Backend::flush_access_times(self).await;
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
            access_time_debounce_secs: 0,
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
            media_type: None,
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
            media_type: None,
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
            media_type: None,
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
    async fn test_update_links_populates_cache_on_overwrite() {
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
            media_type: None,
        }];
        backend.update_links(namespace, &ops).await.unwrap();

        // Read to populate cache
        let meta = backend.read_link(namespace, &tag, false).await.unwrap();
        assert_eq!(meta.target, digest_a);

        // Overwrite tag to point to digest_b via update_links
        let ops = vec![LinkOperation::Create {
            link: tag.clone(),
            target: digest_b.clone(),
            referrer: None,
            media_type: None,
        }];
        backend.update_links(namespace, &ops).await.unwrap();

        // Delete the S3 object to prove the read comes from cache
        let link_path = path_builder::link_path(&tag, namespace);
        backend.store.delete(&link_path).await.unwrap();

        // Read should return digest_b from cache (populated by update_links)
        let meta = backend.read_link(namespace, &tag, false).await.unwrap();
        assert_eq!(meta.target, digest_b);
    }

    #[tokio::test]
    async fn test_update_links_populates_cache_on_create() {
        let config = test_config();
        let (backend, _cache) = test_backend_with_cache(&config);
        let namespace = "cache-populate-create-ns";
        let digest = Digest::from_str(
            "sha256:a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1",
        )
        .unwrap();
        let tag = LinkKind::Tag("v1".into());

        // Create a new tag via update_links
        let ops = vec![LinkOperation::Create {
            link: tag.clone(),
            target: digest.clone(),
            referrer: None,
            media_type: None,
        }];
        backend.update_links(namespace, &ops).await.unwrap();

        // Delete the S3 object to prove the read comes from cache
        let link_path = path_builder::link_path(&tag, namespace);
        backend.store.delete(&link_path).await.unwrap();

        // Read should succeed from cache (populated by update_links, not just invalidated)
        let meta = backend.read_link(namespace, &tag, false).await.unwrap();
        assert_eq!(meta.target, digest);
    }

    #[tokio::test]
    async fn test_update_links_invalidates_cache_on_delete() {
        let config = test_config();
        let (backend, _cache) = test_backend_with_cache(&config);
        let namespace = "cache-invalidate-delete-ns";
        let digest = Digest::from_str(
            "sha256:b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2",
        )
        .unwrap();
        let tag = LinkKind::Tag("to-delete".into());

        // Create a tag
        let ops = vec![LinkOperation::Create {
            link: tag.clone(),
            target: digest.clone(),
            referrer: None,
            media_type: None,
        }];
        backend.update_links(namespace, &ops).await.unwrap();

        // Read to populate cache
        let meta = backend.read_link(namespace, &tag, false).await.unwrap();
        assert_eq!(meta.target, digest);

        // Delete the tag via update_links
        let ops = vec![LinkOperation::Delete {
            link: tag.clone(),
            referrer: None,
        }];
        backend.update_links(namespace, &ops).await.unwrap();

        // Read should return ReferenceNotFound (cache was invalidated, not stale)
        let result = backend.read_link(namespace, &tag, false).await;
        assert!(
            matches!(result, Err(Error::ReferenceNotFound)),
            "Should get ReferenceNotFound after deleting a tag via update_links"
        );
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
            media_type: None,
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
            media_type: None,
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
            media_type: None,
        }];
        backend.update_links(namespace_a, &ops_a).await.unwrap();

        let ops_b = vec![LinkOperation::Create {
            link: tag.clone(),
            target: digest_b.clone(),
            referrer: None,
            media_type: None,
        }];
        backend.update_links(namespace_b, &ops_b).await.unwrap();

        // Read both - each should return its own digest
        let meta_a = backend.read_link(namespace_a, &tag, false).await.unwrap();
        let meta_b = backend.read_link(namespace_b, &tag, false).await.unwrap();

        assert_eq!(meta_a.target, digest_a);
        assert_eq!(meta_b.target, digest_b);
    }

    fn test_backend_with_debounce(config: &BackendConfig, debounce_secs: u64) -> Backend {
        let mut cfg = config.clone();
        cfg.access_time_debounce_secs = debounce_secs;
        Backend::new(&cfg).unwrap()
    }

    #[tokio::test]
    async fn test_deferred_access_time_returns_data_immediately() {
        let config = test_config();
        let backend = test_backend_with_debounce(&config, 60);
        let namespace = "deferred-test-1";
        let digest = Digest::from_str(
            "sha256:da01a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6",
        )
        .unwrap();
        let tag = LinkKind::Tag("deferred-v1".into());

        let ops = vec![LinkOperation::Create {
            link: tag.clone(),
            target: digest.clone(),
            referrer: None,
            media_type: None,
        }];
        backend.update_links(namespace, &ops).await.unwrap();

        // Read with access time update; debounce=60s means write is deferred
        let meta = backend.read_link(namespace, &tag, true).await.unwrap();
        assert_eq!(meta.target, digest);

        // Read directly from S3 to verify the access time was NOT written synchronously
        let raw = backend.read_link_reference(namespace, &tag).await.unwrap();
        assert!(
            raw.accessed_at.is_none(),
            "accessed_at should still be None in S3 because the write was deferred"
        );
    }

    #[tokio::test]
    async fn test_deferred_access_time_writes_eventually() {
        let config = test_config();
        let backend = test_backend_with_debounce(&config, 1);
        let namespace = "deferred-test-2";
        let digest = Digest::from_str(
            "sha256:da02b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1",
        )
        .unwrap();
        let tag = LinkKind::Tag("deferred-v2".into());

        let ops = vec![LinkOperation::Create {
            link: tag.clone(),
            target: digest.clone(),
            referrer: None,
            media_type: None,
        }];
        backend.update_links(namespace, &ops).await.unwrap();

        // Trigger deferred access time update
        backend.read_link(namespace, &tag, true).await.unwrap();

        // Wait for the background flush (debounce = 1s, wait 1.5s)
        tokio::time::sleep(std::time::Duration::from_millis(1500)).await;

        // Read directly from S3: accessed_at should now be set
        let raw = backend.read_link_reference(namespace, &tag).await.unwrap();
        assert!(
            raw.accessed_at.is_some(),
            "accessed_at should be set in S3 after background flush"
        );
    }

    #[tokio::test]
    async fn test_deferred_access_time_coalesces_writes() {
        let config = test_config();
        let backend = test_backend_with_debounce(&config, 2);
        let namespace = "deferred-test-3";
        let digest = Digest::from_str(
            "sha256:da03c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
        )
        .unwrap();
        let tag = LinkKind::Tag("deferred-v3".into());

        let ops = vec![LinkOperation::Create {
            link: tag.clone(),
            target: digest.clone(),
            referrer: None,
            media_type: None,
        }];
        backend.update_links(namespace, &ops).await.unwrap();

        // Call read_link with update_access_time 10 times rapidly
        let start = std::time::Instant::now();
        for _ in 0..10 {
            let meta = backend.read_link(namespace, &tag, true).await.unwrap();
            assert_eq!(meta.target, digest);
        }
        let elapsed = start.elapsed();

        // All 10 reads should complete quickly since writes are deferred
        assert!(
            elapsed < std::time::Duration::from_secs(1),
            "10 deferred reads should complete in under 1 second, took {elapsed:?}"
        );

        // Explicitly flush pending access time writes
        backend.flush_access_times().await;

        // Verify access_at is set in S3
        let raw = backend.read_link_reference(namespace, &tag).await.unwrap();
        assert!(
            raw.accessed_at.is_some(),
            "accessed_at should be set after flush"
        );
    }

    #[tokio::test]
    async fn test_deferred_access_time_different_links_independent() {
        let config = test_config();
        let backend = test_backend_with_debounce(&config, 60);
        let namespace = "deferred-test-4";
        let digest1 = Digest::from_str(
            "sha256:da04d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3",
        )
        .unwrap();
        let digest2 = Digest::from_str(
            "sha256:da04e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4",
        )
        .unwrap();
        let tag1 = LinkKind::Tag("tag1".into());
        let tag2 = LinkKind::Tag("tag2".into());

        let ops = vec![
            LinkOperation::Create {
                link: tag1.clone(),
                target: digest1.clone(),
                referrer: None,
                media_type: None,
            },
            LinkOperation::Create {
                link: tag2.clone(),
                target: digest2.clone(),
                referrer: None,
                media_type: None,
            },
        ];
        backend.update_links(namespace, &ops).await.unwrap();

        // Read both with access time update
        backend.read_link(namespace, &tag1, true).await.unwrap();
        backend.read_link(namespace, &tag2, true).await.unwrap();

        // Flush all pending writes
        backend.flush_access_times().await;

        // Both should have independent accessed_at values
        let raw1 = backend.read_link_reference(namespace, &tag1).await.unwrap();
        let raw2 = backend.read_link_reference(namespace, &tag2).await.unwrap();
        assert!(
            raw1.accessed_at.is_some(),
            "tag1 accessed_at should be set after flush"
        );
        assert!(
            raw2.accessed_at.is_some(),
            "tag2 accessed_at should be set after flush"
        );
    }

    #[tokio::test]
    async fn test_deferred_access_time_flush_on_explicit_call() {
        let config = test_config();
        let backend = test_backend_with_debounce(&config, 60);
        let namespace = "deferred-test-5";
        let digest = Digest::from_str(
            "sha256:da05f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5",
        )
        .unwrap();
        let tag = LinkKind::Tag("deferred-v5".into());

        let ops = vec![LinkOperation::Create {
            link: tag.clone(),
            target: digest.clone(),
            referrer: None,
            media_type: None,
        }];
        backend.update_links(namespace, &ops).await.unwrap();

        // Read with access time update (deferred, won't auto-flush for 60s)
        backend.read_link(namespace, &tag, true).await.unwrap();

        // Explicitly flush
        backend.flush_access_times().await;

        // Verify S3 has the updated accessed_at
        let raw = backend.read_link_reference(namespace, &tag).await.unwrap();
        assert!(
            raw.accessed_at.is_some(),
            "accessed_at should be set in S3 after explicit flush"
        );
    }

    #[tokio::test]
    async fn test_deferred_access_time_zero_debounce_writes_synchronously() {
        let config = test_config();
        let backend = test_backend_with_debounce(&config, 0);
        let namespace = "deferred-test-6";
        let digest = Digest::from_str(
            "sha256:da06a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6",
        )
        .unwrap();
        let tag = LinkKind::Tag("deferred-v6".into());

        let ops = vec![LinkOperation::Create {
            link: tag.clone(),
            target: digest.clone(),
            referrer: None,
            media_type: None,
        }];
        backend.update_links(namespace, &ops).await.unwrap();

        // Read with access time update; debounce=0 means synchronous write
        backend.read_link(namespace, &tag, true).await.unwrap();

        // Immediately read directly from S3 without any flush
        let raw = backend.read_link_reference(namespace, &tag).await.unwrap();
        assert!(
            raw.accessed_at.is_some(),
            "accessed_at should be set immediately when debounce is 0 (synchronous mode)"
        );
    }

    #[tokio::test]
    async fn test_deferred_access_time_does_not_block_read_path() {
        let config = test_config();
        let backend = test_backend_with_debounce(&config, 60);
        let namespace = "deferred-test-7";
        let digest = Digest::from_str(
            "sha256:da07b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1",
        )
        .unwrap();
        let tag = LinkKind::Tag("deferred-v7".into());

        let ops = vec![LinkOperation::Create {
            link: tag.clone(),
            target: digest.clone(),
            referrer: None,
            media_type: None,
        }];
        backend.update_links(namespace, &ops).await.unwrap();

        // Spawn 50 concurrent read_link calls with update_access_time=true
        let start = std::time::Instant::now();
        let mut handles = Vec::new();
        for _ in 0..50 {
            let backend = backend.clone();
            let tag = tag.clone();
            let digest = digest.clone();
            let handle = tokio::spawn(async move {
                let meta = backend.read_link(namespace, &tag, true).await.unwrap();
                assert_eq!(meta.target, digest);
            });
            handles.push(handle);
        }
        for handle in handles {
            handle.await.unwrap();
        }
        let elapsed = start.elapsed();

        assert!(
            elapsed < std::time::Duration::from_secs(2),
            "50 concurrent deferred reads should complete within 2 seconds, took {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn test_flush_processes_entries_concurrently() {
        let config = test_config();
        let backend = test_backend_with_debounce(&config, 60);
        let namespace = "deferred-test-concurrent";
        let entry_count = 20;

        // Create 20 independent tags
        let mut tags = Vec::new();
        for i in 0..entry_count {
            let digest = Digest::from_str(&format!("sha256:{:0>64}", format!("cc{i:02}"))).unwrap();
            let tag = LinkKind::Tag(format!("concurrent-{i}"));

            let ops = vec![LinkOperation::Create {
                link: tag.clone(),
                target: digest,
                referrer: None,
                media_type: None,
            }];
            backend.update_links(namespace, &ops).await.unwrap();
            tags.push(tag);
        }

        // Record access times for all 20 tags (deferred)
        for tag in &tags {
            backend.read_link(namespace, tag, true).await.unwrap();
        }

        // Flush and measure time; concurrent flush should be significantly
        // faster than sequential because each flush_one involves lock + read + write
        let start = std::time::Instant::now();
        backend.flush_access_times().await;
        let elapsed = start.elapsed();

        // Verify all 20 tags have their access times flushed
        for tag in &tags {
            let raw = backend.read_link_reference(namespace, tag).await.unwrap();
            assert!(
                raw.accessed_at.is_some(),
                "accessed_at should be set for {tag} after concurrent flush"
            );
        }

        // With 20 entries and concurrent processing (limit ~10), the flush
        // should complete in roughly 2 batches worth of time. Sequential
        // processing of 20 entries would take much longer. This is a soft
        // assertion to validate concurrency is happening.
        assert!(
            elapsed < std::time::Duration::from_secs(5),
            "Flushing 20 entries concurrently should complete within 5 seconds, took {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn test_flush_errors_do_not_prevent_other_entries() {
        let config = test_config();
        let backend = test_backend_with_debounce(&config, 60);
        let namespace = "deferred-test-error-isolation";

        // Create two valid tags
        let digest1 = Digest::from_str(
            "sha256:ee01a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6",
        )
        .unwrap();
        let digest2 = Digest::from_str(
            "sha256:ee02b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1",
        )
        .unwrap();
        let tag1 = LinkKind::Tag("error-iso-1".into());
        let tag2 = LinkKind::Tag("error-iso-2".into());

        let ops = vec![
            LinkOperation::Create {
                link: tag1.clone(),
                target: digest1,
                referrer: None,
                media_type: None,
            },
            LinkOperation::Create {
                link: tag2.clone(),
                target: digest2,
                referrer: None,
                media_type: None,
            },
        ];
        backend.update_links(namespace, &ops).await.unwrap();

        // Record access times for both
        backend.read_link(namespace, &tag1, true).await.unwrap();
        backend.read_link(namespace, &tag2, true).await.unwrap();

        // Also record a bogus entry that will fail during flush
        // (non-existent namespace/tag combo)
        backend
            .access_time_writer
            .as_ref()
            .unwrap()
            .record("nonexistent-namespace", &LinkKind::Tag("bogus".into()))
            .await;

        // Flush should succeed for the valid entries despite the bogus one failing
        backend.flush_access_times().await;

        // Both valid entries should have their access times set
        let raw1 = backend.read_link_reference(namespace, &tag1).await.unwrap();
        let raw2 = backend.read_link_reference(namespace, &tag2).await.unwrap();
        assert!(
            raw1.accessed_at.is_some(),
            "tag1 accessed_at should be set despite another entry failing"
        );
        assert!(
            raw2.accessed_at.is_some(),
            "tag2 accessed_at should be set despite another entry failing"
        );
    }

    #[tokio::test]
    async fn test_blob_index_updates_multiple_digests() {
        let config = test_config();
        let backend = Backend::new(&config).unwrap();
        let namespace = "blob-index-multi-digest-test";

        let digests: Vec<Digest> = (0..5)
            .map(|i| {
                Digest::from_str(&format!(
                    "sha256:a{i}a0000000000000000000000000000000000000000000000000000000000000"
                ))
                .unwrap()
            })
            .collect();

        let ops: Vec<LinkOperation> = digests
            .iter()
            .enumerate()
            .map(|(i, digest)| LinkOperation::Create {
                link: LinkKind::Tag(format!("tag-bim-{i}")),
                target: digest.clone(),
                referrer: None,
                media_type: None,
            })
            .collect();

        backend.update_links(namespace, &ops).await.unwrap();

        for (i, digest) in digests.iter().enumerate() {
            let path = path_builder::blob_index_path(digest);
            let data = backend.store.read(&path).await.unwrap();
            let blob_index: BlobIndex = serde_json::from_slice(&data).unwrap();
            let ns_links = blob_index.namespace.get(namespace).unwrap();
            let expected_link = LinkKind::Tag(format!("tag-bim-{i}"));
            assert!(
                ns_links.contains(&expected_link),
                "Blob index for digest {digest} should contain {expected_link}"
            );
        }
    }

    #[tokio::test]
    async fn test_tracked_link_creates_with_referrers() {
        let config = test_config();
        let backend = Backend::new(&config).unwrap();
        let namespace = "tracked-creates-referrer-test";

        let referrer_digest = Digest::from_str(
            "sha256:aa00000000000000000000000000000000000000000000000000000000000001",
        )
        .unwrap();

        let layer_digests: Vec<Digest> = (0..3)
            .map(|i| {
                Digest::from_str(&format!(
                    "sha256:b{i}b0000000000000000000000000000000000000000000000000000000000000"
                ))
                .unwrap()
            })
            .collect();

        let config_digest = Digest::from_str(
            "sha256:bb00000000000000000000000000000000000000000000000000000000000001",
        )
        .unwrap();

        let mut ops: Vec<LinkOperation> = layer_digests
            .iter()
            .map(|d| LinkOperation::Create {
                link: LinkKind::Layer(d.clone()),
                target: d.clone(),
                referrer: Some(referrer_digest.clone()),
                media_type: None,
            })
            .collect();

        ops.push(LinkOperation::Create {
            link: LinkKind::Config(config_digest.clone()),
            target: config_digest.clone(),
            referrer: Some(referrer_digest.clone()),
            media_type: None,
        });

        backend.update_links(namespace, &ops).await.unwrap();

        for layer_digest in &layer_digests {
            let link = LinkKind::Layer(layer_digest.clone());
            let meta = backend.read_link_reference(namespace, &link).await.unwrap();
            assert_eq!(meta.target, *layer_digest);
            assert!(
                meta.referenced_by.contains(&referrer_digest),
                "Layer link {link} should have referrer {referrer_digest}"
            );
        }

        let config_link = LinkKind::Config(config_digest.clone());
        let meta = backend
            .read_link_reference(namespace, &config_link)
            .await
            .unwrap();
        assert_eq!(meta.target, config_digest);
        assert!(
            meta.referenced_by.contains(&referrer_digest),
            "Config link should have referrer {referrer_digest}"
        );
    }

    #[tokio::test]
    async fn test_tracked_link_deletes_with_referrers() {
        let config = test_config();
        let backend = Backend::new(&config).unwrap();
        let namespace = "tracked-deletes-referrer-test";

        let referrer_digest = Digest::from_str(
            "sha256:cc00000000000000000000000000000000000000000000000000000000000001",
        )
        .unwrap();

        let layer_digests: Vec<Digest> = (0..3)
            .map(|i| {
                Digest::from_str(&format!(
                    "sha256:c{i}c0000000000000000000000000000000000000000000000000000000000000"
                ))
                .unwrap()
            })
            .collect();

        // Create tracked links with referrers
        let create_ops: Vec<LinkOperation> = layer_digests
            .iter()
            .map(|d| LinkOperation::Create {
                link: LinkKind::Layer(d.clone()),
                target: d.clone(),
                referrer: Some(referrer_digest.clone()),
                media_type: None,
            })
            .collect();
        backend.update_links(namespace, &create_ops).await.unwrap();

        // Verify they exist
        for d in &layer_digests {
            let link = LinkKind::Layer(d.clone());
            let meta = backend.read_link_reference(namespace, &link).await.unwrap();
            assert_eq!(meta.target, *d);
        }

        // Delete all tracked links with referrer
        let delete_ops: Vec<LinkOperation> = layer_digests
            .iter()
            .map(|d| LinkOperation::Delete {
                link: LinkKind::Layer(d.clone()),
                referrer: Some(referrer_digest.clone()),
            })
            .collect();
        backend.update_links(namespace, &delete_ops).await.unwrap();

        // Verify links are deleted and blob indices cleaned up
        for d in &layer_digests {
            let link = LinkKind::Layer(d.clone());
            let result = backend.read_link_reference(namespace, &link).await;
            assert!(
                matches!(result, Err(Error::ReferenceNotFound)),
                "Tracked link {link} should be deleted"
            );

            let path = path_builder::blob_index_path(d);
            let result = backend.store.read(&path).await;
            assert!(
                result.is_err(),
                "Blob index for {d} should be removed after all links deleted"
            );
        }
    }

    #[tokio::test]
    async fn test_mixed_creates_and_deletes_across_digests() {
        let config = test_config();
        let backend = Backend::new(&config).unwrap();
        let namespace = "mixed-ops-across-digests-test";

        let digest_keep = Digest::from_str(
            "sha256:dd00000000000000000000000000000000000000000000000000000000000001",
        )
        .unwrap();
        let digest_remove = Digest::from_str(
            "sha256:dd00000000000000000000000000000000000000000000000000000000000002",
        )
        .unwrap();
        let digest_add = Digest::from_str(
            "sha256:dd00000000000000000000000000000000000000000000000000000000000003",
        )
        .unwrap();

        // Setup: create tags for digest_keep and digest_remove
        let setup_ops = vec![
            LinkOperation::Create {
                link: LinkKind::Tag("keep-tag".into()),
                target: digest_keep.clone(),
                referrer: None,
                media_type: None,
            },
            LinkOperation::Create {
                link: LinkKind::Tag("remove-tag".into()),
                target: digest_remove.clone(),
                referrer: None,
                media_type: None,
            },
        ];
        backend.update_links(namespace, &setup_ops).await.unwrap();

        // Mixed operation: delete remove-tag, add new-tag pointing to digest_add
        let mixed_ops = vec![
            LinkOperation::Delete {
                link: LinkKind::Tag("remove-tag".into()),
                referrer: None,
            },
            LinkOperation::Create {
                link: LinkKind::Tag("new-tag".into()),
                target: digest_add.clone(),
                referrer: None,
                media_type: None,
            },
        ];
        backend.update_links(namespace, &mixed_ops).await.unwrap();

        // Verify: keep-tag still exists
        let keep_path = path_builder::blob_index_path(&digest_keep);
        let keep_data = backend.store.read(&keep_path).await.unwrap();
        let keep_index: BlobIndex = serde_json::from_slice(&keep_data).unwrap();
        let keep_links = keep_index.namespace.get(namespace).unwrap();
        assert!(keep_links.contains(&LinkKind::Tag("keep-tag".into())));

        // Verify: remove-tag blob index no longer has it
        let remove_path = path_builder::blob_index_path(&digest_remove);
        let remove_result = backend.store.read(&remove_path).await;
        if let Ok(data) = remove_result {
            let idx: BlobIndex = serde_json::from_slice(&data).unwrap();
            let links = idx.namespace.get(namespace);
            assert!(
                links.is_none() || !links.unwrap().contains(&LinkKind::Tag("remove-tag".into())),
                "remove-tag should not be in blob index after delete"
            );
        }

        // Verify: new-tag exists in digest_add's blob index
        let add_path = path_builder::blob_index_path(&digest_add);
        let add_data = backend.store.read(&add_path).await.unwrap();
        let add_index: BlobIndex = serde_json::from_slice(&add_data).unwrap();
        let add_links = add_index.namespace.get(namespace).unwrap();
        assert!(add_links.contains(&LinkKind::Tag("new-tag".into())));

        // Verify: remove-tag link itself is gone
        let result = backend
            .read_link_reference(namespace, &LinkKind::Tag("remove-tag".into()))
            .await;
        assert!(matches!(result, Err(Error::ReferenceNotFound)));

        // Verify: new-tag link exists and points to digest_add
        let new_meta = backend
            .read_link_reference(namespace, &LinkKind::Tag("new-tag".into()))
            .await
            .unwrap();
        assert_eq!(new_meta.target, digest_add);
    }

    #[tokio::test]
    async fn test_read_link_with_access_time_debounce_uses_cache() {
        let config = test_config();
        let mut cfg = config.clone();
        cfg.access_time_debounce_secs = 60;
        let cache: Arc<dyn cache::Cache> = Arc::new(cache::memory::Backend::new());
        let backend = Backend::new(&cfg).unwrap().with_cache(cache.clone());
        let namespace = "cache-debounce-hit-ns";
        let digest = Digest::from_str(
            "sha256:db01a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6",
        )
        .unwrap();
        let tag = LinkKind::Tag("debounce-cached".into());

        // Create link via update_links (populates cache)
        let ops = vec![LinkOperation::Create {
            link: tag.clone(),
            target: digest.clone(),
            referrer: None,
            media_type: None,
        }];
        backend.update_links(namespace, &ops).await.unwrap();

        // First read with access time update (debounce path)
        let meta = backend.read_link(namespace, &tag, true).await.unwrap();
        assert_eq!(meta.target, digest);

        // Delete the S3 object to prove the next read must come from cache
        let link_path = path_builder::link_path(&tag, namespace);
        backend.store.delete(&link_path).await.unwrap();

        // Second read with access time update should succeed from cache
        let meta = backend.read_link(namespace, &tag, true).await.unwrap();
        assert_eq!(meta.target, digest);

        // Verify writer.record() was still called on cache hit
        let writer = backend.access_time_writer.as_ref().unwrap();
        let pending = writer.pending.lock().await;
        assert!(
            !pending.is_empty(),
            "writer.record() should have been called even on cache hit"
        );
    }
}
