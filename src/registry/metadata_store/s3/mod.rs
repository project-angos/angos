use std::{
    collections::{HashMap, HashSet},
    future::Future,
    io::ErrorKind,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
pub use config::BackendConfig;
use futures_util::{
    future::join_all,
    stream::{self, StreamExt},
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::{debug, info, instrument, warn};

use crate::{
    cache::{Cache, CacheExt},
    oci::{Descriptor, Digest, Manifest},
    registry::{
        data_store,
        metadata_store::{
            BlobIndex, BlobIndexOperation, ConditionalCapabilities, Error, LinkMetadata,
            LinkOperation, LockStrategy, MetadataStore,
            link_kind::LinkKind,
            lock::{self, LockBackend, MemoryBackend},
            simple_jitter,
        },
        pagination, path_builder,
    },
};

mod access_time;
mod config;

#[cfg(test)]
mod tests;

/// A single create operation tuple.
type CreateOp = (
    LinkKind,
    Digest,
    Option<Digest>,
    Option<Digest>,
    Option<String>,
    Option<Descriptor>,
);

/// Return type of `build_create_ops`.
type CreateOpsResult = (
    HashMap<Digest, Vec<BlobIndexOperation>>,
    Vec<(LinkKind, LinkMetadata)>,
    Vec<(LinkKind, LinkMetadata)>,
);

/// Return type of `build_delete_ops`.
type DeleteOpsResult = (
    HashMap<Digest, Vec<BlobIndexOperation>>,
    Vec<(LinkKind, LinkMetadata)>,
    Vec<LinkKind>,
    Vec<LinkKind>,
);

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct NamespaceRegistry {
    namespaces: Vec<String>,
}

#[derive(Clone)]
pub struct Backend {
    pub store: data_store::s3::Backend,
    lock: Arc<dyn LockBackend + Send + Sync>,
    cache: Option<Arc<dyn Cache>>,
    link_cache_ttl: u64,
    access_time_writer: Option<access_time::AccessTimeWriter>,
    /// Per-operation conditional capabilities probed at startup.
    /// Controls whether CAS paths are used for blob index, link updates, namespace
    /// registry writes, and access-time updates.
    conditional: ConditionalCapabilities,
    known_namespaces: Arc<Mutex<HashSet<String>>>,
    // Held for Drop side-effect: signals the flush task to exit when the last Backend is dropped.
    #[allow(dead_code)]
    flush_handle: Option<Arc<access_time::FlushHandle>>,
}

const MAX_UPDATE_RETRIES: u32 = 10;
const MAX_BLOB_INDEX_CAS_RETRIES: u32 = 20;

impl Backend {
    /// Create a new S3 metadata-store backend.
    ///
    /// `conditional` overrides the capabilities used for lock backend construction and CAS
    /// write paths. Pass `None` to use defaults (all capabilities assumed present for S3 lock
    /// strategy, none for other strategies). Pass `Some(caps)` with the result of
    /// `probe_conditional_capabilities` to use the actual probed values.
    pub fn new(
        config: &BackendConfig,
        conditional: Option<ConditionalCapabilities>,
    ) -> Result<Self, Error> {
        info!("Using S3 metadata-store backend");
        let store = data_store::s3::Backend::new(&config.to_data_store_config())?;

        let conditional = conditional.unwrap_or_else(|| {
            if matches!(config.lock_strategy, LockStrategy::S3(_)) {
                ConditionalCapabilities {
                    put_if_none_match: true,
                    put_if_match: true,
                    delete_if_match: true,
                }
            } else {
                ConditionalCapabilities::default()
            }
        });

        let lock: Arc<dyn LockBackend + Send + Sync> = match &config.lock_strategy {
            LockStrategy::Redis(redis_config) => {
                info!("Using Redis lock store for S3 metadata-store");
                let backend = lock::RedisBackend::new(redis_config).map_err(|e| {
                    Error::Lock(format!("Failed to initialize Redis lock store: {e}"))
                })?;
                Arc::new(backend)
            }
            LockStrategy::S3(s3_lock_config) => {
                info!("Using S3 lock store for S3 metadata-store");
                let lock_store = Arc::new(
                    data_store::s3::Backend::new(&config.to_lock_store_config(s3_lock_config))
                        .map_err(|e| {
                            Error::Lock(format!("Failed to initialize S3 lock store: {e}"))
                        })?,
                );
                Arc::new(
                    lock::S3LockBackend::new(
                        lock_store,
                        s3_lock_config,
                        conditional.delete_if_match,
                    )
                    .map_err(|e| Error::Lock(format!("Failed to initialize S3 lock store: {e}")))?,
                )
            }
            LockStrategy::Memory => {
                info!("Using in-memory lock store for S3 metadata-store");
                Arc::new(MemoryBackend::new())
            }
        };

        if config.access_time_debounce_secs == 0
            && matches!(config.lock_strategy, LockStrategy::S3(_))
        {
            warn!(
                "access_time_debounce_secs is 0 with S3 lock strategy; \
                 every manifest pull will trigger a synchronous access time update \
                 (CAS loop, with lock fallback), adding S3 API latency. \
                 Consider setting access_time_debounce_secs to 60 or higher."
            );
        }

        let access_time_writer = if config.access_time_debounce_secs > 0 {
            Some(access_time::AccessTimeWriter::new())
        } else {
            None
        };

        let flush_handle = if config.access_time_debounce_secs > 0 {
            let shutdown = Arc::new(AtomicBool::new(false));
            let shutdown_flag = shutdown.clone();
            let interval = Duration::from_secs(config.access_time_debounce_secs);

            let flush_backend = Self {
                store: store.clone(),
                lock: lock.clone(),
                cache: None,
                link_cache_ttl: config.link_cache_ttl,
                access_time_writer: access_time_writer.clone(),
                conditional: conditional.clone(),
                known_namespaces: Arc::new(Mutex::new(HashSet::new())),
                flush_handle: None,
            };

            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(interval).await;
                    if shutdown_flag.load(Ordering::Acquire) {
                        flush_backend.flush_access_times().await;
                        return;
                    }
                    flush_backend.flush_access_times().await;
                }
            });

            Some(Arc::new(access_time::FlushHandle { shutdown }))
        } else {
            None
        };

        let backend = Self {
            store,
            lock,
            cache: None,
            link_cache_ttl: config.link_cache_ttl,
            access_time_writer,
            conditional,
            known_namespaces: Arc::new(Mutex::new(HashSet::new())),
            flush_handle,
        };

        Ok(backend)
    }

    /// Probe each conditional S3 operation independently.
    ///
    /// Tests `PutObject If-None-Match: *`, `PutObject If-Match: <etag>`, and
    /// `DeleteObject If-Match: <etag>` in sequence. Each probe is self-validating:
    /// bogus-ETag attempts verify that the provider actually enforces the condition.
    ///
    /// Returns the probed capabilities. The caller is responsible for failing startup
    /// if any required capability is absent.
    pub async fn probe_conditional_capabilities(
        store: &data_store::s3::Backend,
    ) -> Result<ConditionalCapabilities, Error> {
        let probe_key = format!("_angos_probe_{}", uuid::Uuid::new_v4());
        let content: &[u8] = b"probe";

        store.put_object(&probe_key, content).await.map_err(|e| {
            Error::Lock(format!(
                "conditional capability probe: failed to create probe object: {e}"
            ))
        })?;

        // Test If-None-Match: * — expect 412 because the object already exists.
        let put_if_none_match = match store.put_object_if_not_exists(&probe_key, content).await {
            Err(data_store::Error::PreconditionFailed) => true,
            Ok(_) => {
                warn!(
                    "conditional probe: If-None-Match: * was accepted on existing key; provider does not enforce it"
                );
                false
            }
            Err(e) => {
                warn!("conditional probe: If-None-Match error: {e}");
                false
            }
        };

        // Test If-Match: <etag> — correct ETag must succeed; bogus ETag must fail.
        let put_if_match = match store.read_with_etag(&probe_key).await {
            Ok((_, Some(etag))) => {
                let correct = store
                    .put_object_if_match(&probe_key, &etag, b"updated".to_vec())
                    .await
                    .is_ok();
                let bogus_rejected = matches!(
                    store
                        .put_object_if_match(&probe_key, "\"bogus\"", b"fail".to_vec())
                        .await,
                    Err(data_store::Error::PreconditionFailed)
                );
                correct && bogus_rejected
            }
            Ok((_, None)) => {
                warn!("conditional probe: ETag not returned; If-Match support cannot be verified");
                false
            }
            Err(e) => {
                warn!("conditional probe: failed to read probe object for If-Match test: {e}");
                false
            }
        };

        // Test DeleteObject If-Match: <etag> — bogus ETag must fail; correct ETag must succeed.
        // Re-read the current ETag after the put_if_match update may have changed it.
        let delete_if_match = match store.read_with_etag(&probe_key).await {
            Ok((_, Some(etag))) => {
                // Bogus-ETag attempt first: if the provider ignores the condition and deletes
                // the object, the correct-ETag attempt below will hit NotFound and correct=false.
                let bogus_rejected = matches!(
                    store.delete_if_match(&probe_key, "\"bogus\"").await,
                    Err(data_store::Error::PreconditionFailed)
                );
                let correct = store.delete_if_match(&probe_key, &etag).await.is_ok();
                bogus_rejected && correct
            }
            Ok((_, None)) => {
                warn!(
                    "conditional probe: ETag not returned; DeleteObject If-Match support cannot be verified"
                );
                false
            }
            Err(e) if e.kind() == ErrorKind::NotFound => false,
            Err(e) => {
                warn!(
                    "conditional probe: failed to read probe object for delete_if_match test: {e}"
                );
                false
            }
        };

        // Cleanup — may already have been deleted by the delete_if_match test.
        let _ = store.delete(&probe_key).await;

        let capabilities = ConditionalCapabilities {
            put_if_none_match,
            put_if_match,
            delete_if_match,
        };

        info!(
            if_none_match = capabilities.put_if_none_match,
            if_match = capabilities.put_if_match,
            delete_if_match = capabilities.delete_if_match,
            "S3 conditional capability probe complete"
        );

        Ok(capabilities)
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
            let _ = cache.delete_value(&Self::cache_key(namespace, link)).await;
        }
    }

    /// Applies a batch of blob index operations for a single digest using optimistic
    /// concurrency (CAS). Reads the current blob index with its `ETag`, applies all
    /// operations, and writes back with `If-Match`. Retries on `ETag` conflict.
    ///
    /// For new blob indexes (not-found), uses `If-None-Match: *` to create atomically,
    /// falling back to CAS if another writer created the index concurrently.
    async fn update_blob_index_cas(
        &self,
        namespace: &str,
        digest: &Digest,
        operations: &[BlobIndexOperation],
    ) -> Result<(), Error> {
        let shard_path = path_builder::blob_index_shard_path(digest, namespace);

        for attempt in 0..MAX_BLOB_INDEX_CAS_RETRIES {
            let (mut links, etag) = match self.store.read_with_etag(&shard_path).await {
                Ok((data, etag)) => (
                    serde_json::from_slice::<HashSet<LinkKind>>(&data).unwrap_or_default(),
                    etag,
                ),
                Err(e) if e.kind() == ErrorKind::NotFound => (HashSet::new(), None),
                Err(e) => return Err(Error::from(e)),
            };

            for op in operations {
                match op {
                    BlobIndexOperation::Insert(link) => {
                        links.insert(link.clone());
                    }
                    BlobIndexOperation::Remove(link) => {
                        links.remove(link);
                    }
                }
            }

            // Write the shard back with CAS. Empty shards are written rather than
            // deleted to avoid a race where a concurrent writer creates a new entry
            // between our read and a non-conditional delete. Scrub handles cleanup.
            let content = Bytes::from(serde_json::to_vec(&links)?);

            let write_result = if let Some(ref etag) = etag {
                self.store
                    .put_object_if_match(&shard_path, etag, content)
                    .await
                    .map(|_| ())
            } else {
                self.store
                    .put_object_if_not_exists(&shard_path, content)
                    .await
                    .map(|_| ())
            };

            match write_result {
                Ok(()) => return Ok(()),
                Err(data_store::Error::PreconditionFailed) => {
                    debug!(
                        digest = %digest,
                        namespace,
                        attempt,
                        "Blob index shard CAS conflict, retrying"
                    );
                    let max_ms = 50u64.saturating_mul(1u64 << attempt.min(4));
                    tokio::time::sleep(Duration::from_millis(simple_jitter(max_ms))).await;
                }
                Err(e) => return Err(Error::StorageBackend(e.to_string())),
            }
        }

        warn!(
            %digest,
            namespace,
            attempts = MAX_BLOB_INDEX_CAS_RETRIES,
            "Blob index shard CAS retries exhausted"
        );
        Err(Error::Lock(format!(
            "blob index CAS retries exhausted for digest {digest} after {MAX_BLOB_INDEX_CAS_RETRIES} attempts"
        )))
    }
}

#[async_trait]
impl MetadataStore for Backend {
    fn conditional_capabilities(&self) -> Option<ConditionalCapabilities> {
        Some(self.conditional.clone())
    }

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
            info!("Namespace registry not found, rebuilding from S3 tree walk");
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

            let digest_entries: Vec<(Digest, LinkKind)> = objects
                .iter()
                .filter_map(|key| {
                    let parts: Vec<&str> = key.split('/').collect();
                    if parts.len() < 2 || parts[0] != "sha256" {
                        return None;
                    }
                    let manifest_digest = Digest::Sha256(parts[1].into());
                    let referrer_link = LinkKind::Referrer(digest.clone(), manifest_digest.clone());
                    Some((manifest_digest, referrer_link))
                })
                .collect();

            let results: Vec<Option<Descriptor>> = stream::iter(digest_entries)
                .map(|(manifest_digest, referrer_link)| {
                    let artifact_type = artifact_type.as_ref();
                    async move {
                        if let Ok(metadata) =
                            self.read_link_reference(namespace, &referrer_link).await
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
                            Err(e) if e.kind() == ErrorKind::NotFound => {
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
        // Try sharded format first (refs/{namespace}.json per namespace)
        let refs_dir = path_builder::blob_index_refs_dir(digest);
        let mut index = BlobIndex::default();
        let mut found_shards = false;
        let mut continuation_token = None;

        loop {
            let (_, objects, next_token) = self
                .store
                .list_prefixes(&refs_dir, "/", 1000, continuation_token, None)
                .await?;

            if !objects.is_empty() {
                found_shards = true;
            }

            let shard_results: Vec<Result<Option<(String, HashSet<LinkKind>)>, Error>> =
                stream::iter(objects.into_iter().map(|obj| {
                    let shard_path = format!("{refs_dir}/{obj}");
                    async move {
                        match self.store.read(&shard_path).await {
                            Ok(data) => {
                                if let Ok(links) =
                                    serde_json::from_slice::<HashSet<LinkKind>>(&data)
                                {
                                    let namespace = obj
                                        .strip_suffix(".json")
                                        .unwrap_or(&obj)
                                        .replace("%2F", "/")
                                        .replace("%25", "%");
                                    if !links.is_empty() {
                                        return Ok(Some((namespace, links)));
                                    }
                                }
                                Ok(None)
                            }
                            Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
                            Err(e) => Err(Error::from(e)),
                        }
                    }
                }))
                .buffer_unordered(10)
                .collect()
                .await;

            for result in shard_results {
                if let Some((namespace, links)) = result? {
                    index.namespace.insert(namespace, links);
                }
            }

            continuation_token = next_token;
            if continuation_token.is_none() {
                break;
            }
        }

        if found_shards {
            if index.namespace.is_empty() {
                return Err(Error::ReferenceNotFound);
            }
            return Ok(index);
        }

        // XXX: remove legacy index.json fallback and migration below for v2.0.0
        let legacy_path = path_builder::blob_index_path(digest);
        let data = match self.store.read(&legacy_path).await {
            Ok(data) => data,
            Err(e) if e.kind() == ErrorKind::NotFound => {
                return Err(Error::ReferenceNotFound);
            }
            Err(e) => return Err(e.into()),
        };
        let blob_index: BlobIndex = serde_json::from_slice(&data).map_err(Error::from)?;

        // Migrate: write shards, then delete legacy file
        for (namespace, links) in &blob_index.namespace {
            let ops: Vec<BlobIndexOperation> = links
                .iter()
                .map(|link| BlobIndexOperation::Insert(link.clone()))
                .collect();
            self.update_blob_index_shard(namespace, digest, &ops)
                .await?;
        }
        self.store.delete(&legacy_path).await?;
        info!(
            "Migrated legacy blob index for '{digest}' ({} namespaces)",
            blob_index.namespace.len()
        );

        Ok(blob_index)
    }

    #[instrument(skip(self))]
    async fn update_blob_index(
        &self,
        namespace: &str,
        digest: &Digest,
        operation: BlobIndexOperation,
    ) -> Result<(), Error> {
        if self.conditional.supports_cas() {
            return self
                .update_blob_index_cas(namespace, digest, &[operation])
                .await;
        }

        self.update_blob_index_shard(namespace, digest, &[operation])
            .await
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
            } else if self.conditional.put_if_match {
                let link_data = self
                    .read_and_spawn_access_time_update(namespace, link)
                    .await?;
                self.cache_put(namespace, link, &link_data).await;
                Ok(link_data)
            } else {
                let guard = self.lock.acquire(&[format!("{namespace}:{link}")]).await?;
                let link_data = self.read_link_reference(namespace, link).await?.accessed();
                if !guard.is_valid() {
                    return Err(Error::Lock(
                        "lock invalidated during access time update".into(),
                    ));
                }
                self.write_link_reference(namespace, link, &link_data)
                    .await?;
                self.cache_put(namespace, link, &link_data).await;
                guard.release().await;
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

        if self.conditional.supports_cas() {
            self.update_links_cas(namespace, operations).await
        } else {
            self.update_links_locked(namespace, operations).await
        }
    }

    async fn flush_access_times(&self) {
        Backend::flush_access_times(self).await;
    }
}

impl Backend {
    /// CAS-optimized write path.
    ///
    /// Phase 1: All creates attempt lock-free `If-None-Match: *`.
    ///
    /// Phase 2: Locked read-modify-write for creates that lost the race and deletes.
    ///
    /// Phase 3: CAS blob index writes for new or re-targeted links.
    async fn update_links_cas(
        &self,
        namespace: &str,
        operations: &[LinkOperation],
    ) -> Result<(), Error> {
        let mut locked_ops: Vec<LinkOperation> = Vec::new();
        let mut any_creates = false;

        let mut optimistic: Vec<(LinkOperation, LinkKind, Digest, LinkMetadata)> = Vec::new();

        for op in operations {
            match op {
                LinkOperation::Create {
                    link,
                    target,
                    referrer,
                    media_type,
                    descriptor,
                } => {
                    any_creates = true;
                    let mut metadata = LinkMetadata::from_digest(target.clone())
                        .with_media_type(media_type.clone())
                        .with_descriptor(descriptor.as_ref().clone());
                    if let Some(r) = referrer {
                        metadata.add_referrer(r.clone());
                    }
                    optimistic.push((op.clone(), link.clone(), target.clone(), metadata));
                }
                LinkOperation::Delete { .. } => {
                    locked_ops.push(op.clone());
                }
            }
        }

        // Phase 1: All creates attempt lock-free If-None-Match: *
        let link_results = join_all(optimistic.iter().map(|(_, link, _, metadata)| {
            self.write_link_if_not_exists(namespace, link, metadata)
        }))
        .await;

        let mut lockfree_blob_ops: HashMap<Digest, Vec<BlobIndexOperation>> = HashMap::new();
        for ((op, link, target, metadata), result) in optimistic.into_iter().zip(link_results) {
            match result {
                Ok(true) => {
                    lockfree_blob_ops
                        .entry(target)
                        .or_default()
                        .push(BlobIndexOperation::Insert(link.clone()));
                    self.cache_put(namespace, &link, &metadata).await;
                }
                Ok(false) => {
                    locked_ops.push(op);
                }
                Err(e) => return Err(e),
            }
        }

        // Phase 2: Locked path for remaining operations.
        let locked_blob_ops = self
            .execute_locked_cas_updates(namespace, &locked_ops)
            .await?;

        // Merge blob index operations from both paths.
        let mut pending_blob_ops = lockfree_blob_ops;
        for (digest, ops) in locked_blob_ops {
            pending_blob_ops.entry(digest).or_default().extend(ops);
        }

        // Phase 3: CAS blob index writes.
        join_all(
            pending_blob_ops
                .iter()
                .map(|(digest, ops)| self.update_blob_index_cas(namespace, digest, ops)),
        )
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

        if any_creates && let Err(e) = self.register_namespace(namespace).await {
            warn!(namespace, error = %e, "Failed to register namespace");
        }

        Ok(())
    }

    /// Executes link operations under a distributed lock, returning pending
    /// blob index operations for CAS updates after the lock is released.
    async fn execute_locked_cas_updates(
        &self,
        namespace: &str,
        operations: &[LinkOperation],
    ) -> Result<HashMap<Digest, Vec<BlobIndexOperation>>, Error> {
        if operations.is_empty() {
            return Ok(HashMap::new());
        }

        let mut lock_keys: Vec<String> = operations
            .iter()
            .map(|op| {
                let link = match op {
                    LinkOperation::Create { link, .. } | LinkOperation::Delete { link, .. } => link,
                };
                format!("{namespace}:{link}")
            })
            .collect();
        lock_keys.sort();
        lock_keys.dedup();

        let guard = self.lock.acquire(&lock_keys).await?;

        let read_results = join_all(operations.iter().map(|op| async move {
            let link = match op {
                LinkOperation::Create { link, .. } | LinkOperation::Delete { link, .. } => link,
            };
            let metadata = self.read_link_reference(namespace, link).await.ok();
            (op, metadata)
        }))
        .await;

        let mut link_cache: HashMap<LinkKind, LinkMetadata> = HashMap::new();
        let mut creates: Vec<CreateOp> = Vec::new();
        let mut deletes: Vec<(LinkKind, Digest, Option<Digest>)> = Vec::new();

        for (op, metadata) in &read_results {
            match op {
                LinkOperation::Create {
                    link,
                    target,
                    referrer,
                    media_type,
                    descriptor,
                } => {
                    let old_target = metadata.as_ref().map(|m| m.target.clone());
                    if let Some(m) = metadata {
                        link_cache.insert(link.clone(), m.clone());
                    }
                    creates.push((
                        link.clone(),
                        target.clone(),
                        old_target,
                        referrer.clone(),
                        media_type.clone(),
                        descriptor.as_ref().clone(),
                    ));
                }
                LinkOperation::Delete { link, referrer } => {
                    if let Some(m) = metadata {
                        link_cache.insert(link.clone(), m.clone());
                        deletes.push((link.clone(), m.target.clone(), referrer.clone()));
                    }
                }
            }
        }

        if creates.is_empty() && deletes.is_empty() {
            guard.release().await;
            return Ok(HashMap::new());
        }

        if !guard.is_valid() {
            guard.release().await;
            return Err(Error::Lock("lock invalidated during operation".into()));
        }

        let pending_blob_ops = self
            .apply_link_operations(namespace, &creates, &deletes, &mut link_cache)
            .await?;

        guard.release().await;

        Ok(pending_blob_ops)
    }

    /// Non-CAS write path: pre-lock reads to compute blob digest lock keys,
    /// then validates under the lock with re-reads. Retries on concurrent
    /// modification to ensure lock key coverage.
    #[allow(clippy::too_many_lines)]
    async fn update_links_locked(
        &self,
        namespace: &str,
        operations: &[LinkOperation],
    ) -> Result<(), Error> {
        let mut update_retries = MAX_UPDATE_RETRIES;
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
            let mut creates: Vec<CreateOp> = Vec::new();
            let mut deletes: Vec<(LinkKind, Digest, Option<Digest>)> = Vec::new();

            for (_, create_data, delete_data) in prelock_results {
                if let Some((link, target, old_target, referrer, media_type, descriptor)) =
                    create_data
                {
                    lock_keys.push(format!("{namespace}:{link}"));
                    lock_keys.push(format!("blob:{target}"));
                    if let Some(ref old) = old_target {
                        lock_keys.push(format!("blob:{old}"));
                    }
                    creates.push((link, target, old_target, referrer, media_type, descriptor));
                } else if let Some((link, Some(meta), referrer)) = delete_data {
                    lock_keys.push(format!("{namespace}:{link}"));
                    lock_keys.push(format!("blob:{}", meta.target));
                    deletes.push((link, meta.target, referrer));
                }
            }

            if creates.is_empty() && deletes.is_empty() {
                return Ok(());
            }

            lock_keys.sort();
            lock_keys.dedup();
            let guard = self.lock.acquire(&lock_keys).await?;

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
                guard.release().await;
                if update_retries == 0 {
                    return Err(Error::Lock(
                        "update_links exceeded maximum retries due to concurrent modifications"
                            .into(),
                    ));
                }
                update_retries -= 1;
                continue;
            }
            for (link, metadata, _, _) in validation_results {
                if let Some(m) = metadata {
                    link_cache.insert(link, m);
                }
            }

            if !guard.is_valid() {
                guard.release().await;
                return Err(Error::Lock("lock invalidated during operation".into()));
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
                guard.release().await;
                if update_retries == 0 {
                    return Err(Error::Lock(
                        "update_links exceeded maximum retries due to concurrent modifications"
                            .into(),
                    ));
                }
                update_retries -= 1;
                continue;
            }

            if !guard.is_valid() {
                guard.release().await;
                return Err(Error::Lock("lock invalidated during operation".into()));
            }

            let pending_blob_ops = self
                .apply_link_operations(namespace, &creates, &valid_deletes, &mut link_cache)
                .await?;

            if !guard.is_valid() {
                guard.release().await;
                return Err(Error::Lock("lock invalidated during operation".into()));
            }

            // Blob index shard updates run under the lock (blob:{digest} keys
            // are included in lock_keys above).
            join_all(
                pending_blob_ops
                    .iter()
                    .map(|(digest, ops)| self.update_blob_index_shard(namespace, digest, ops)),
            )
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;

            if !guard.is_valid() {
                guard.release().await;
                return Err(Error::Lock("lock invalidated during operation".into()));
            }

            guard.release().await;

            if !creates.is_empty()
                && let Err(e) = self.register_namespace(namespace).await
            {
                warn!(namespace, error = %e, "Failed to register namespace");
            }

            return Ok(());
        }
    }

    /// Executes the write+delete+cache-update sequence for a set of create and
    /// delete link operations, returning the accumulated pending blob index
    /// operations for the caller to apply.
    async fn apply_link_operations(
        &self,
        namespace: &str,
        creates: &[CreateOp],
        deletes: &[(LinkKind, Digest, Option<Digest>)],
        link_cache: &mut HashMap<LinkKind, LinkMetadata>,
    ) -> Result<HashMap<Digest, Vec<BlobIndexOperation>>, Error> {
        let (pending_blob_ops, tracked_create_writes, non_tracked_create_writes) =
            Self::build_create_ops(creates, link_cache);

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

        let (
            pending_blob_ops,
            tracked_delete_writes,
            tracked_delete_removes,
            non_tracked_delete_links,
        ) = Self::build_delete_ops(deletes, link_cache, pending_blob_ops);

        join_all(
            tracked_delete_writes
                .iter()
                .map(|(link, metadata)| {
                    let fut: Pin<Box<dyn Future<Output = Result<(), Error>> + Send + '_>> =
                        Box::pin(self.write_link_reference(namespace, link, metadata));
                    fut
                })
                .chain(
                    tracked_delete_removes
                        .iter()
                        .chain(non_tracked_delete_links.iter())
                        .map(|link| {
                            let fut: Pin<Box<dyn Future<Output = Result<(), Error>> + Send + '_>> =
                                Box::pin(self.delete_link_reference(namespace, link));
                            fut
                        }),
                ),
        )
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

        for (link, metadata) in tracked_create_writes
            .iter()
            .chain(non_tracked_create_writes.iter())
            .chain(tracked_delete_writes.iter())
        {
            self.cache_put(namespace, link, metadata).await;
        }
        for link in tracked_delete_removes
            .iter()
            .chain(non_tracked_delete_links.iter())
        {
            self.cache_invalidate(namespace, link).await;
        }

        Ok(pending_blob_ops)
    }

    /// Builds blob index operations and link writes for create operations.
    fn build_create_ops(
        creates: &[CreateOp],
        link_cache: &mut HashMap<LinkKind, LinkMetadata>,
    ) -> CreateOpsResult {
        let mut pending_blob_ops: HashMap<Digest, Vec<BlobIndexOperation>> = HashMap::new();
        let mut tracked_create_writes: Vec<(LinkKind, LinkMetadata)> = Vec::new();
        let mut non_tracked_create_writes: Vec<(LinkKind, LinkMetadata)> = Vec::new();

        for (link, target, old_target, referrer, media_type, descriptor) in creates {
            let is_tracked = link.is_tracked();

            if is_tracked && referrer.is_some() {
                let mut metadata = link_cache.remove(link).unwrap_or_else(|| {
                    LinkMetadata::from_digest(target.clone())
                        .with_media_type(media_type.clone())
                        .with_descriptor(descriptor.clone())
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
                if old_target.as_ref() != Some(target) {
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
                }

                non_tracked_create_writes.push((
                    link.clone(),
                    LinkMetadata::from_digest(target.clone())
                        .with_media_type(media_type.clone())
                        .with_descriptor(descriptor.clone()),
                ));
            }
        }

        (
            pending_blob_ops,
            tracked_create_writes,
            non_tracked_create_writes,
        )
    }

    /// Builds blob index operations and link writes/deletes for delete operations.
    fn build_delete_ops(
        deletes: &[(LinkKind, Digest, Option<Digest>)],
        link_cache: &mut HashMap<LinkKind, LinkMetadata>,
        mut pending_blob_ops: HashMap<Digest, Vec<BlobIndexOperation>>,
    ) -> DeleteOpsResult {
        let mut tracked_delete_writes: Vec<(LinkKind, LinkMetadata)> = Vec::new();
        let mut tracked_delete_removes: Vec<LinkKind> = Vec::new();
        let mut non_tracked_delete_links: Vec<LinkKind> = Vec::new();

        for (link, target, referrer) in deletes {
            let is_tracked = link.is_tracked();

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

        (
            pending_blob_ops,
            tracked_delete_writes,
            tracked_delete_removes,
            non_tracked_delete_links,
        )
    }

    /// Unconditional read-modify-write on a namespace shard (caller must hold lock).
    async fn update_blob_index_shard(
        &self,
        namespace: &str,
        digest: &Digest,
        operations: &[BlobIndexOperation],
    ) -> Result<(), Error> {
        let shard_path = path_builder::blob_index_shard_path(digest, namespace);
        let mut links: HashSet<LinkKind> = match self.store.read(&shard_path).await {
            Ok(data) => serde_json::from_slice(&data).unwrap_or_default(),
            Err(e) if e.kind() == ErrorKind::NotFound => HashSet::new(),
            Err(e) => return Err(e.into()),
        };

        for operation in operations {
            match operation {
                BlobIndexOperation::Insert(link) => {
                    links.insert(link.clone());
                }
                BlobIndexOperation::Remove(link) => {
                    links.remove(link);
                }
            }
        }

        if links.is_empty() {
            self.store.delete(&shard_path).await?;
        } else {
            let content = Bytes::from(serde_json::to_vec(&links)?);
            self.store.put_object(&shard_path, content).await?;
        }

        Ok(())
    }

    async fn read_namespace_registry(&self) -> Result<Option<NamespaceRegistry>, Error> {
        // Read sharded format: list all shard files under _registry/ns/ and merge
        let shard_dir = path_builder::namespace_registry_shard_dir();
        let mut all_namespaces = Vec::new();
        let mut found_shards = false;
        let mut continuation_token = None;

        loop {
            let (_, objects, next_token) = self
                .store
                .list_prefixes(&shard_dir, "/", 1000, continuation_token, None)
                .await?;

            if !objects.is_empty() {
                found_shards = true;
            }

            let shard_results: Vec<Result<Vec<String>, Error>> =
                stream::iter(objects.into_iter().map(|obj| {
                    let shard_path = format!("{shard_dir}/{obj}");
                    async move {
                        match self.store.read(&shard_path).await {
                            Ok(data) => match serde_json::from_slice::<NamespaceRegistry>(&data) {
                                Ok(registry) => Ok(registry.namespaces),
                                Err(_) => Ok(Vec::new()),
                            },
                            Err(e) if e.kind() == ErrorKind::NotFound => Ok(Vec::new()),
                            Err(e) => Err(Error::from(e)),
                        }
                    }
                }))
                .buffer_unordered(10)
                .collect()
                .await;

            for result in shard_results {
                all_namespaces.extend(result?);
            }

            continuation_token = next_token;
            if continuation_token.is_none() {
                break;
            }
        }

        if !found_shards {
            // Fall back to legacy single-file format for backward compatibility
            let path = path_builder::namespace_registry_path();
            return match self.store.read(&path).await {
                Ok(data) => match serde_json::from_slice(&data) {
                    Ok(registry) => Ok(Some(registry)),
                    Err(e) => {
                        warn!("Corrupt namespace registry, will rebuild: {e}");
                        Ok(None)
                    }
                },
                Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
                Err(e) => Err(Error::from(e)),
            };
        }

        all_namespaces.sort();
        all_namespaces.dedup();
        Ok(Some(NamespaceRegistry {
            namespaces: all_namespaces,
        }))
    }

    pub async fn rebuild_namespace_registry(&self) -> Result<(), Error> {
        let repo_dir = path_builder::repository_dir();
        let mut namespaces = Vec::new();
        self.collect_namespaces(repo_dir, "", &mut namespaces)
            .await?;
        namespaces.sort();
        namespaces.dedup();

        // Group namespaces by shard key
        let mut shards: HashMap<String, Vec<String>> = HashMap::new();
        for ns in &namespaces {
            let key = path_builder::namespace_shard_key(ns);
            shards.entry(key).or_default().push(ns.clone());
        }

        // Write each shard
        for (shard_key, shard_namespaces) in &shards {
            let registry = NamespaceRegistry {
                namespaces: shard_namespaces.clone(),
            };
            let content = Bytes::from(serde_json::to_vec(&registry)?);
            let path = format!(
                "{}/{shard_key}.json",
                path_builder::namespace_registry_shard_dir()
            );

            if self.conditional.supports_cas() {
                let etag = match self.store.read_with_etag(&path).await {
                    Ok((_, etag)) => etag,
                    Err(e) if e.kind() == ErrorKind::NotFound => None,
                    Err(e) => return Err(Error::from(e)),
                };
                let write_result = if let Some(ref etag) = etag {
                    self.store
                        .put_object_if_match(&path, etag, content)
                        .await
                        .map(|_| ())
                } else {
                    self.store
                        .put_object_if_not_exists(&path, content)
                        .await
                        .map(|_| ())
                };
                match write_result {
                    Ok(()) => {}
                    Err(data_store::Error::PreconditionFailed) => {
                        debug!(
                            shard_key,
                            "Namespace registry shard rebuild lost CAS race; concurrent write wins"
                        );
                    }
                    Err(e) => return Err(Error::StorageBackend(e.to_string())),
                }
            } else {
                let lock_key = format!("namespace_registry_shard_{shard_key}");
                let guard = self.lock.acquire(&[lock_key]).await?;
                self.store.put_object(&path, content).await?;
                guard.release().await;
            }
        }

        Ok(())
    }

    async fn register_namespace(&self, namespace: &str) -> Result<(), Error> {
        if self.known_namespaces.lock().await.contains(namespace) {
            return Ok(());
        }

        if self.conditional.supports_cas() {
            self.register_namespace_cas(namespace).await
        } else {
            self.register_namespace_unconditional(namespace).await
        }
    }

    async fn register_namespace_unconditional(&self, namespace: &str) -> Result<(), Error> {
        let shard_key = path_builder::namespace_shard_key(namespace);
        let path = path_builder::namespace_registry_shard_path(namespace);
        let lock_key = format!("namespace_registry_shard_{shard_key}");
        let guard = self.lock.acquire(&[lock_key]).await?;

        let mut registry = match self.store.read(&path).await {
            Ok(data) => serde_json::from_slice::<NamespaceRegistry>(&data).unwrap_or_default(),
            Err(e) if e.kind() == ErrorKind::NotFound => NamespaceRegistry::default(),
            Err(e) => {
                guard.release().await;
                return Err(Error::from(e));
            }
        };

        if let Err(pos) = registry.namespaces.binary_search(&namespace.to_string()) {
            registry.namespaces.insert(pos, namespace.to_string());
            let content = Bytes::from(serde_json::to_vec(&registry)?);
            if let Err(e) = self.store.put_object(&path, content).await {
                guard.release().await;
                return Err(e.into());
            }
        }

        guard.release().await;
        self.known_namespaces
            .lock()
            .await
            .insert(namespace.to_string());
        Ok(())
    }

    async fn register_namespace_cas(&self, namespace: &str) -> Result<(), Error> {
        let path = path_builder::namespace_registry_shard_path(namespace);

        for attempt in 0..MAX_BLOB_INDEX_CAS_RETRIES {
            let (mut registry, etag) = match self.store.read_with_etag(&path).await {
                Ok((data, etag)) => match serde_json::from_slice::<NamespaceRegistry>(&data) {
                    Ok(r) => (r, etag),
                    Err(_) => (NamespaceRegistry::default(), etag),
                },
                Err(e) if e.kind() == ErrorKind::NotFound => (NamespaceRegistry::default(), None),
                Err(e) => return Err(Error::from(e)),
            };

            let Err(pos) = registry.namespaces.binary_search(&namespace.to_string()) else {
                self.known_namespaces
                    .lock()
                    .await
                    .insert(namespace.to_string());
                return Ok(());
            };
            registry.namespaces.insert(pos, namespace.to_string());

            let content = Bytes::from(serde_json::to_vec(&registry)?);
            let write_result = if let Some(ref etag) = etag {
                self.store
                    .put_object_if_match(&path, etag, content)
                    .await
                    .map(|_| ())
            } else {
                self.store
                    .put_object_if_not_exists(&path, content)
                    .await
                    .map(|_| ())
            };

            match write_result {
                Ok(()) => {
                    self.known_namespaces
                        .lock()
                        .await
                        .insert(namespace.to_string());
                    return Ok(());
                }
                Err(data_store::Error::PreconditionFailed) => {
                    debug!(
                        namespace,
                        attempt, "Namespace registry shard CAS conflict, retrying"
                    );
                    let max_ms = 50u64.saturating_mul(1u64 << attempt.min(4));
                    tokio::time::sleep(Duration::from_millis(simple_jitter(max_ms))).await;
                }
                Err(e) => return Err(Error::StorageBackend(e.to_string())),
            }
        }

        warn!(
            namespace,
            "Namespace registry shard CAS retries exhausted; namespace will appear after scrub reconciles"
        );
        Ok(())
    }

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

    /// Reads the link with its `ETag`, then spawns the access time write as a
    /// background task so the caller doesn't block on the CAS round-trip.
    ///
    /// The spawned write is best-effort: under concurrent pulls the `ETag` may
    /// become stale and the CAS will silently fail. This is acceptable because
    /// access times are advisory — a missed update simply means the timestamp
    /// lags by one pull interval. Requires a CAS-capable backend (`put_if_match`).
    async fn read_and_spawn_access_time_update(
        &self,
        namespace: &str,
        link: &LinkKind,
    ) -> Result<LinkMetadata, Error> {
        let link_path = path_builder::link_path(link, namespace);

        let (data, etag) = match self.store.read_with_etag(&link_path).await {
            Ok(result) => result,
            Err(e) if e.kind() == ErrorKind::NotFound => return Err(Error::ReferenceNotFound),
            Err(e) => return Err(e.into()),
        };
        let link_data = LinkMetadata::from_bytes(data)?;

        if let Some(etag) = etag {
            let store = self.store.clone();
            let updated = link_data.clone().accessed();
            let path = link_path;
            tokio::spawn(async move {
                let serialized = match serde_json::to_vec(&updated) {
                    Ok(v) => Bytes::from(v),
                    Err(_) => return,
                };
                let _ = store.put_object_if_match(&path, &etag, serialized).await;
            });
        }

        Ok(link_data)
    }

    async fn read_link_reference(
        &self,
        namespace: &str,
        link: &LinkKind,
    ) -> Result<LinkMetadata, Error> {
        let link_path = path_builder::link_path(link, namespace);
        match self.store.read(&link_path).await {
            Ok(data) => LinkMetadata::from_bytes(data),
            Err(e) if e.kind() == ErrorKind::NotFound => Err(Error::ReferenceNotFound),
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

    /// Atomically creates a link only if it does not already exist, using
    /// S3 conditional `If-None-Match: *`. Returns `true` if the link was
    /// created, `false` if it already existed (precondition failed).
    async fn write_link_if_not_exists(
        &self,
        namespace: &str,
        link: &LinkKind,
        metadata: &LinkMetadata,
    ) -> Result<bool, Error> {
        let link_path = path_builder::link_path(link, namespace);
        let serialized = Bytes::from(serde_json::to_vec(metadata)?);
        match self
            .store
            .put_object_if_not_exists(&link_path, serialized)
            .await
        {
            Ok(_) => Ok(true),
            Err(data_store::Error::PreconditionFailed) => Ok(false),
            Err(e) => Err(Error::StorageBackend(e.to_string())),
        }
    }

    async fn delete_link_reference(&self, namespace: &str, link: &LinkKind) -> Result<(), Error> {
        let link_path = path_builder::link_path(link, namespace);
        self.store.delete(&link_path).await?;
        Ok(())
    }
}
