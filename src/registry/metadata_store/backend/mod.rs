use std::{
    collections::HashSet,
    io,
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};

use async_trait::async_trait;
use futures_util::stream::{self, StreamExt};
use tokio::sync::Mutex;
use tracing::{debug, info, instrument, warn};

use crate::{
    cache::Cache,
    oci::{Descriptor, Digest},
    registry::{
        metadata_store::{
            BlobIndex, BlobIndexOperation, Error, LinkMetadata, LinkOperation, LockGuard,
            MetadataStore,
            link_kind::LinkKind,
            lock::LockBackend,
            lock_ops::LockOps,
            lock_ops::{blob_index_lock_key, link_lock_key, with_validated_lock},
            referrer_resolver::resolve_referrer_descriptor,
            sharded::{
                SHARD_READ_CONCURRENCY, collect_blob_index_shards,
                decode_blob_index_shard_namespace, namespace_links_from_index,
                non_empty_links_or_not_found,
            },
            update_links::run_update_links,
        },
        pagination, path_builder,
    },
};
use angos_storage::{Error as StorageError, ObjectStore};

mod access_time;
mod blob_index;
mod coordinator;
mod link_ops;
mod namespace_registry;

#[cfg(test)]
mod tests;

use access_time::{AccessTimeWriter, FlushHandle};
pub use coordinator::Coordinator;

// ───────────────────────────────────────────────────────────────────────────
// Backend
// ───────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct Backend {
    coordinator: Arc<Coordinator>,
    cache: Option<Arc<Cache>>,
    link_cache_ttl: u64,
    access_time_writer: Option<AccessTimeWriter>,
    known_namespaces: Arc<Mutex<HashSet<String>>>,
    // Held for Drop side-effect: signals the flush task to exit when the last clone is dropped.
    _flush_handle: Option<Arc<FlushHandle>>,
}

pub struct Builder {
    coordinator: Option<Arc<Coordinator>>,
    cache: Option<Arc<Cache>>,
    link_cache_ttl: u64,
    access_time_debounce_secs: u64,
}

impl Builder {
    fn new() -> Self {
        Self {
            coordinator: None,
            cache: None,
            link_cache_ttl: 30,
            access_time_debounce_secs: 0,
        }
    }

    pub fn coordinator(mut self, coordinator: Arc<Coordinator>) -> Self {
        self.coordinator = Some(coordinator);
        self
    }

    pub fn cache(mut self, cache: Arc<Cache>) -> Self {
        self.cache = Some(cache);
        self
    }

    pub fn link_cache_ttl(mut self, ttl: u64) -> Self {
        self.link_cache_ttl = ttl;
        self
    }

    pub fn access_time_debounce_secs(mut self, secs: u64) -> Self {
        self.access_time_debounce_secs = secs;
        self
    }

    pub fn build(self) -> Result<Backend, Error> {
        let coordinator = self
            .coordinator
            .ok_or_else(|| Error::Lock("Backend requires a coordinator".to_string()))?;

        let (access_time_writer, flush_handle) = if self.access_time_debounce_secs > 0 {
            let writer = AccessTimeWriter::new();
            let shutdown = Arc::new(AtomicBool::new(false));
            let interval = Duration::from_secs(self.access_time_debounce_secs);

            let flush_backend = Backend {
                coordinator: coordinator.clone(),
                cache: None,
                link_cache_ttl: self.link_cache_ttl,
                access_time_writer: Some(writer.clone()),
                known_namespaces: Arc::new(Mutex::new(HashSet::new())),
                _flush_handle: None,
            };

            Backend::spawn_flush_task(flush_backend, shutdown.clone(), interval);

            (Some(writer), Some(Arc::new(FlushHandle::new(shutdown))))
        } else {
            (None, None)
        };

        Ok(Backend {
            coordinator,
            cache: self.cache,
            link_cache_ttl: self.link_cache_ttl,
            access_time_writer,
            known_namespaces: Arc::new(Mutex::new(HashSet::new())),
            _flush_handle: flush_handle,
        })
    }
}

impl Backend {
    pub fn builder() -> Builder {
        Builder::new()
    }

    pub fn store(&self) -> &dyn ObjectStore {
        self.coordinator.store()
    }

    pub fn lock(&self) -> &(dyn LockBackend + Send + Sync) {
        self.coordinator.lock()
    }

    // ── Cache helpers ─────────────────────────────────────────────────────

    fn cache_key(namespace: &str, link: &LinkKind) -> String {
        format!("link:{namespace}:{link}")
    }

    pub async fn cache_get(&self, namespace: &str, link: &LinkKind) -> Option<LinkMetadata> {
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

    pub async fn cache_put(&self, namespace: &str, link: &LinkKind, metadata: &LinkMetadata) {
        if self.link_cache_ttl == 0 {
            return;
        }
        if let Some(cache) = &self.cache {
            let key = Self::cache_key(namespace, link);
            if let Err(err) = cache.store(&key, metadata, self.link_cache_ttl).await {
                warn!("Failed to store link metadata in cache for {namespace}/{link}: {err}");
            }
        }
    }

    pub async fn cache_invalidate(&self, namespace: &str, link: &LinkKind) {
        if let Some(cache) = &self.cache {
            let _ = cache.delete_value(&Self::cache_key(namespace, link)).await;
        }
    }
}

// ───────────────────────────────────────────────────────────────────────────
// MetadataStore impl
// ───────────────────────────────────────────────────────────────────────────

#[async_trait]
impl MetadataStore for Backend {
    #[instrument(skip(self))]
    async fn list_namespaces(
        &self,
        n: u16,
        last: Option<String>,
    ) -> Result<(Vec<String>, Option<String>), Error> {
        debug!("Fetching {n} namespace(s) with continuation token: {last:?}");

        let namespaces = if let Some(registry) = self.read_namespace_registry().await? {
            registry.namespaces
        } else {
            info!("Namespace registry not found, rebuilding");
            self.rebuild_namespace_registry().await?;
            self.read_namespace_registry()
                .await?
                .map(|r| r.namespaces)
                .unwrap_or_default()
        };

        {
            let mut cache = self.known_namespaces.lock().await;
            cache.extend(namespaces.iter().cloned());
        }

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

        let page = self.store().list_children(&tags_dir, n, None, last).await?;

        let tags = page.sub_prefixes;
        let continuation = if page.next_token.is_some() {
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
        let mut token = None;

        loop {
            let page = self.store().list(&referrers_dir, 100, token).await?;

            let digest_entries: Vec<Digest> = page
                .items
                .iter()
                .filter_map(|key| {
                    let parts: Vec<&str> = key.split('/').collect();
                    if parts.len() < 2 || parts[0] != "sha256" {
                        return None;
                    }
                    Some(Digest::Sha256(parts[1].into()))
                })
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
                            |path| async move {
                                self.store().get(&path).await.map_err(|e| match e {
                                    StorageError::NotFound => {
                                        io::Error::new(io::ErrorKind::NotFound, e.to_string())
                                    }
                                    other => io::Error::other(other.to_string()),
                                })
                            },
                        )
                        .await
                    }
                })
                .buffer_unordered(10)
                .collect()
                .await;

            referrers.extend(results.into_iter().flatten());

            token = page.next_token;
            if token.is_none() {
                break;
            }
        }

        referrers.sort_by(|a, b| a.digest.cmp(&b.digest));
        Ok(referrers)
    }

    async fn has_referrers(&self, namespace: &str, subject: &Digest) -> Result<bool, Error> {
        let referrers_dir = path_builder::manifest_referrers_dir(namespace, subject);

        let page = self.store().list(&referrers_dir, 1, None).await?;

        Ok(!page.items.is_empty())
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

        let page = self
            .store()
            .list_children(&revisions_dir, n, continuation_token, None)
            .await?;

        let revisions = page
            .sub_prefixes
            .into_iter()
            .map(|key| Digest::Sha256(key.into()))
            .collect();

        Ok((revisions, page.next_token))
    }

    async fn count_manifests(&self, namespace: &str) -> Result<usize, Error> {
        let revisions_dir = path_builder::manifest_revisions_link_root_dir(namespace, "sha256");
        let mut count = 0;
        let mut token = None;

        loop {
            let page = self
                .store()
                .list_children(&revisions_dir, 1000, token, None)
                .await?;

            count += page.sub_prefixes.len();
            token = page.next_token;
            if token.is_none() {
                break;
            }
        }

        Ok(count)
    }

    #[instrument(skip(self))]
    async fn read_blob_index(&self, digest: &Digest) -> Result<BlobIndex, Error> {
        let refs_dir = path_builder::blob_index_refs_dir(digest);
        let mut index = BlobIndex::default();
        let mut found_shards = false;
        let mut token = None;

        loop {
            let page = self
                .store()
                .list_children(&refs_dir, 1000, token, None)
                .await?;

            if !page.objects.is_empty() {
                found_shards = true;
            }

            let shard_results = stream::iter(page.objects.into_iter().map(|obj| {
                let shard_path = format!("{refs_dir}/{obj}");
                async move {
                    match self.store().get(&shard_path).await {
                        Ok(data) => {
                            if let Ok(links) = serde_json::from_slice::<HashSet<LinkKind>>(&data) {
                                let namespace = decode_blob_index_shard_namespace(&obj);
                                if !links.is_empty() {
                                    return Ok(Some((namespace, links)));
                                }
                            }
                            Ok(None)
                        }
                        Err(StorageError::NotFound) => Ok(None),
                        Err(e) => Err(Error::from(e)),
                    }
                }
            }))
            .buffer_unordered(SHARD_READ_CONCURRENCY)
            .collect::<Vec<Result<Option<(String, HashSet<LinkKind>)>, Error>>>()
            .await;

            let shards = shard_results
                .into_iter()
                .filter_map(Result::transpose)
                .collect::<Result<Vec<_>, _>>()?;
            index
                .namespace
                .extend(collect_blob_index_shards(shards).namespace);

            token = page.next_token;
            if token.is_none() {
                break;
            }
        }

        if !found_shards || index.namespace.is_empty() {
            let legacy_path = path_builder::blob_index_path(digest);
            match self.store().get(&legacy_path).await {
                Ok(data) => {
                    let legacy: BlobIndex = serde_json::from_slice(&data).unwrap_or_default();
                    if legacy.namespace.is_empty() {
                        return Err(Error::ReferenceNotFound);
                    }
                    return Ok(legacy);
                }
                Err(StorageError::NotFound) => return Err(Error::ReferenceNotFound),
                Err(e) => return Err(e.into()),
            }
        }
        Ok(index)
    }

    #[instrument(skip(self))]
    async fn has_blob_references(&self, digest: &Digest) -> Result<bool, Error> {
        let refs_dir = path_builder::blob_index_refs_dir(digest);
        let mut token = None;

        loop {
            let page = self
                .store()
                .list_children(&refs_dir, 1, token, None)
                .await?;

            for obj in page.objects {
                let shard_path = format!("{refs_dir}/{obj}");
                match self.store().get(&shard_path).await {
                    Ok(data) => {
                        let links = serde_json::from_slice::<HashSet<LinkKind>>(&data)?;
                        if !links.is_empty() {
                            return Ok(true);
                        }
                    }
                    Err(StorageError::NotFound) => {}
                    Err(e) => return Err(e.into()),
                }
            }

            token = page.next_token;
            if token.is_none() {
                break;
            }
        }

        // Fall back to the legacy `index.json` layout: if it exists and any
        // namespace still has a non-empty link set, the blob is referenced.
        let legacy_path = path_builder::blob_index_path(digest);
        match self.store().get(&legacy_path).await {
            Ok(data) => {
                let legacy: BlobIndex = serde_json::from_slice(&data).unwrap_or_default();
                Ok(legacy.namespace.values().any(|links| !links.is_empty()))
            }
            Err(StorageError::NotFound) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    #[instrument(skip(self))]
    async fn read_blob_index_namespace(
        &self,
        namespace: &str,
        digest: &Digest,
    ) -> Result<HashSet<LinkKind>, Error> {
        let shard_path = path_builder::blob_index_shard_path(digest, namespace);
        match self.store().get(&shard_path).await {
            Ok(data) => {
                let links = serde_json::from_slice::<HashSet<LinkKind>>(&data)?;
                non_empty_links_or_not_found(links)
            }
            Err(StorageError::NotFound) => {
                // Shard absent — try the legacy single-file layout with one GET.
                let legacy_path = path_builder::blob_index_path(digest);
                match self.store().get(&legacy_path).await {
                    Ok(data) => {
                        let legacy: BlobIndex = serde_json::from_slice(&data).unwrap_or_default();
                        namespace_links_from_index(&legacy, namespace)
                    }
                    Err(StorageError::NotFound) => Err(Error::ReferenceNotFound),
                    Err(e) => Err(e.into()),
                }
            }
            Err(e) => Err(e.into()),
        }
    }

    #[instrument(skip(self))]
    async fn update_blob_index(
        &self,
        namespace: &str,
        digest: &Digest,
        operation: BlobIndexOperation,
    ) -> Result<(), Error> {
        if self.coordinator.is_cas() {
            return self
                .update_blob_index_cas(namespace, digest, &[operation])
                .await;
        }

        let lock_keys = [blob_index_lock_key(digest)];
        with_validated_lock(
            self.lock(),
            &lock_keys,
            "lock invalidated during blob index update",
            || async {
                self.update_blob_index_locked(namespace, digest, &[operation])
                    .await
            },
        )
        .await
    }

    #[instrument(skip(self))]
    async fn migrate_blob_index(&self, digest: &Digest) -> Result<(), Error> {
        let lock_keys = [blob_index_lock_key(digest)];
        with_validated_lock(
            self.lock(),
            &lock_keys,
            "lock invalidated during blob index migration",
            || async move {
                let legacy_path = path_builder::blob_index_path(digest);
                let data = match self.store().get(&legacy_path).await {
                    Ok(data) => data,
                    Err(StorageError::NotFound) => return Ok(()),
                    Err(e) => return Err(e.into()),
                };
                let blob_index: BlobIndex = serde_json::from_slice(&data).map_err(Error::from)?;
                self.migrate_legacy_blob_index_data(digest, &blob_index)
                    .await?;
                info!(
                    "Migrated legacy blob index for '{digest}' ({} namespaces)",
                    blob_index.namespace.len()
                );
                Ok(())
            },
        )
        .await
    }

    #[instrument(skip(self))]
    async fn migrate_namespace_registry(&self) -> Result<(), Error> {
        self.rebuild_namespace_registry().await?;
        self.store()
            .delete(&path_builder::namespace_registry_path())
            .await?;
        Ok(())
    }

    async fn acquire_blob_data_lock(&self, digest: &Digest) -> Result<LockGuard, Error> {
        self.lock().acquire(&[format!("blob-data:{digest}")]).await
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
            } else if let Coordinator::Cas { store, .. } = &*self.coordinator {
                let link_data = self
                    .read_and_spawn_access_time_update(namespace, link, store.clone())
                    .await?;
                self.cache_put(namespace, link, &link_data).await;
                Ok(link_data)
            } else {
                let lock_keys = [link_lock_key(namespace, link)];
                let link_data = with_validated_lock(
                    self.lock(),
                    &lock_keys,
                    "lock invalidated during access time update",
                    || async {
                        let link_data = self.read_link_reference(namespace, link).await?.accessed();
                        self.write_link_reference(namespace, link, &link_data)
                            .await?;
                        Ok(link_data)
                    },
                )
                .await?;
                self.cache_put(namespace, link, &link_data).await;
                Ok(link_data)
            }
        } else if let Some(cached) = self.cache_get(namespace, link).await {
            Ok(cached)
        } else {
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
        if self.coordinator.is_cas() {
            self.update_links_cas(namespace, operations).await
        } else {
            run_update_links(self, self.lock(), namespace, operations).await
        }
    }

    async fn flush_access_times(&self) {
        if let Some(writer) = &self.access_time_writer {
            writer.flush(self).await;
        }
    }
}
