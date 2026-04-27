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
pub use config::BackendConfig;
use futures_util::{
    future::join_all,
    stream::{self, StreamExt},
};
use tokio::sync::Mutex;
use tracing::{debug, info, instrument, warn};

use crate::{
    cache::Cache,
    oci::{Descriptor, Digest, Manifest},
    registry::{
        data_store,
        metadata_store::{
            BlobIndex, BlobIndexOperation, ConditionalCapabilities, Error, LinkMetadata,
            LinkOperation, LockStrategy, MetadataStore, ResolvedCreate, ResolvedDelete,
            link_kind::LinkKind,
            lock::{self, MemoryBackend},
            lock_ops::LockOps,
        },
        pagination, path_builder,
    },
};

mod access_time;
mod blob_index;
mod config;
mod coordinator;
mod link_ops;
mod namespace_registry;

#[cfg(test)]
mod tests;

use coordinator::WriteCoordinator;

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

#[derive(Clone)]
pub struct Backend {
    pub store: data_store::s3::Backend,
    cache: Option<Arc<dyn Cache>>,
    link_cache_ttl: u64,
    access_time_writer: Option<access_time::AccessTimeWriter>,
    coordinator: Arc<dyn WriteCoordinator>,
    pub known_namespaces: Arc<Mutex<HashSet<String>>>,
    // Held for Drop side-effect: signals the flush task to exit when the last Backend is dropped.
    #[allow(dead_code)]
    flush_handle: Option<Arc<access_time::FlushHandle>>,
}

const MAX_UPDATE_RETRIES: u32 = 10;
const MAX_BLOB_INDEX_CAS_RETRIES: u32 = 20;

impl Backend {
    fn build_coordinator(
        config: &BackendConfig,
        caps: &ConditionalCapabilities,
    ) -> Result<Arc<dyn WriteCoordinator>, Error> {
        match &config.lock_strategy {
            LockStrategy::S3(s3_lock_config) => {
                if !caps.supports_cas() {
                    return Err(Error::Lock(format!(
                        "S3 lock strategy requires If-None-Match and If-Match support, \
                         but provider has put_if_none_match={}, put_if_match={}. \
                         Use lock_strategy = redis or lock_strategy = memory instead.",
                        caps.put_if_none_match, caps.put_if_match
                    )));
                }
                info!("Using CAS coordinator with S3 lock for S3 metadata-store");
                let lock_store = Arc::new(
                    data_store::s3::Backend::new(&config.to_lock_store_config(s3_lock_config))
                        .map_err(|e| {
                            Error::Lock(format!("Failed to initialize S3 lock store: {e}"))
                        })?,
                );
                let lock = Arc::new(
                    lock::S3LockBackend::new(lock_store, s3_lock_config, caps.delete_if_match)
                        .map_err(|e| {
                            Error::Lock(format!("Failed to initialize S3 lock store: {e}"))
                        })?,
                );
                Ok(Arc::new(coordinator::CasCoordinator { lock }))
            }
            LockStrategy::Redis(redis_config) => {
                info!("Using Redis lock store for S3 metadata-store");
                let backend = lock::RedisBackend::new(redis_config).map_err(|e| {
                    Error::Lock(format!("Failed to initialize Redis lock store: {e}"))
                })?;
                Ok(Arc::new(coordinator::LockCoordinator {
                    lock: Arc::new(backend),
                }))
            }
            LockStrategy::Memory => {
                info!("Using in-memory lock store for S3 metadata-store");
                Ok(Arc::new(coordinator::LockCoordinator {
                    lock: Arc::new(MemoryBackend::new()),
                }))
            }
        }
    }

    /// Create a new S3 metadata-store backend.
    ///
    /// `conditional` carries the capabilities used by the CAS coordinator when the
    /// operator selects `lock_strategy = "s3"`. Pass `None` to assume all capabilities
    /// are present for the S3 lock strategy and none for other strategies, or
    /// `Some(caps)` with the result of `probe_conditional_capabilities`.
    ///
    /// `lock_strategy` drives coordinator selection: `"s3"` selects the CAS coordinator
    /// (which requires `put_if_none_match` and `put_if_match`; `delete_if_match` is
    /// propagated to the internal S3 lock for release safety), while `"redis"` and
    /// `"memory"` select the lock coordinator with the corresponding lock backend.
    /// Capabilities are not consulted outside the S3 lock strategy.
    pub fn new(
        config: &BackendConfig,
        conditional: Option<ConditionalCapabilities>,
    ) -> Result<Self, Error> {
        info!("Using S3 metadata-store backend");
        let store = data_store::s3::Backend::new(&config.to_data_store_config())?;

        let caps = conditional.unwrap_or_else(|| {
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

        let coordinator = Self::build_coordinator(config, &caps)?;

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
                cache: None,
                link_cache_ttl: config.link_cache_ttl,
                access_time_writer: access_time_writer.clone(),
                coordinator: coordinator.clone(),
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
            cache: None,
            link_cache_ttl: config.link_cache_ttl,
            access_time_writer,
            coordinator,
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
        if let Err(e) = store.delete(&probe_key).await
            && e.kind() != ErrorKind::NotFound
        {
            warn!("conditional probe: cleanup failed for probe object {probe_key}: {e}");
        }

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

    pub fn with_cache(mut self, cache: Arc<dyn Cache>) -> Self {
        self.cache = Some(cache);
        self
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

        // Legacy index.json fallback — remove after v2.0.0 migration
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
        self.coordinator
            .update_blob_index(self, namespace, digest, operation)
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
            } else {
                self.coordinator
                    .touch_link_access_time(self, namespace, link)
                    .await
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

        self.coordinator
            .update_links(self, namespace, operations)
            .await
    }

    async fn flush_access_times(&self) {
        if let Some(writer) = &self.access_time_writer {
            writer.flush(self).await;
        }
    }
}

impl Backend {
    /// Executes the write+delete+cache-update sequence for a set of create and
    /// delete link operations, returning the accumulated pending blob index
    /// operations for the caller to apply.
    pub async fn apply_link_operations(
        &self,
        namespace: &str,
        creates: &[ResolvedCreate],
        deletes: &[ResolvedDelete],
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
        creates: &[ResolvedCreate],
        link_cache: &mut HashMap<LinkKind, LinkMetadata>,
    ) -> CreateOpsResult {
        let mut pending_blob_ops: HashMap<Digest, Vec<BlobIndexOperation>> = HashMap::new();
        let mut tracked_create_writes: Vec<(LinkKind, LinkMetadata)> = Vec::new();
        let mut non_tracked_create_writes: Vec<(LinkKind, LinkMetadata)> = Vec::new();

        for op in creates {
            let is_tracked = op.link.is_tracked();

            if is_tracked && op.referrer.is_some() {
                let mut metadata = link_cache.remove(&op.link).unwrap_or_else(|| {
                    LinkMetadata::from_digest(op.target.clone())
                        .with_media_type(op.media_type.clone())
                        .with_descriptor(op.descriptor.clone())
                });

                if let Some(manifest_digest) = &op.referrer {
                    metadata.add_referrer(manifest_digest.clone());
                }

                if op.old_target.is_none() {
                    pending_blob_ops
                        .entry(op.target.clone())
                        .or_default()
                        .push(BlobIndexOperation::Insert(op.link.clone()));
                }

                tracked_create_writes.push((op.link.clone(), metadata));
            } else {
                if op.old_target.as_ref() != Some(&op.target) {
                    pending_blob_ops
                        .entry(op.target.clone())
                        .or_default()
                        .push(BlobIndexOperation::Insert(op.link.clone()));
                    if let Some(old) = &op.old_target
                        && *old != op.target
                    {
                        pending_blob_ops
                            .entry(old.clone())
                            .or_default()
                            .push(BlobIndexOperation::Remove(op.link.clone()));
                    }
                }

                non_tracked_create_writes.push((
                    op.link.clone(),
                    LinkMetadata::from_digest(op.target.clone())
                        .with_media_type(op.media_type.clone())
                        .with_descriptor(op.descriptor.clone()),
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
        deletes: &[ResolvedDelete],
        link_cache: &mut HashMap<LinkKind, LinkMetadata>,
        mut pending_blob_ops: HashMap<Digest, Vec<BlobIndexOperation>>,
    ) -> DeleteOpsResult {
        let mut tracked_delete_writes: Vec<(LinkKind, LinkMetadata)> = Vec::new();
        let mut tracked_delete_removes: Vec<LinkKind> = Vec::new();
        let mut non_tracked_delete_links: Vec<LinkKind> = Vec::new();

        for op in deletes {
            let is_tracked = op.link.is_tracked();

            if is_tracked && op.referrer.is_some() {
                if let Some(mut metadata) = link_cache.remove(&op.link) {
                    if let Some(manifest_digest) = &op.referrer {
                        metadata.remove_referrer(manifest_digest);
                    }

                    if metadata.has_references() {
                        tracked_delete_writes.push((op.link.clone(), metadata));
                    } else {
                        tracked_delete_removes.push(op.link.clone());
                        pending_blob_ops
                            .entry(op.target.clone())
                            .or_default()
                            .push(BlobIndexOperation::Remove(op.link.clone()));
                    }
                }
            } else {
                non_tracked_delete_links.push(op.link.clone());
                pending_blob_ops
                    .entry(op.target.clone())
                    .or_default()
                    .push(BlobIndexOperation::Remove(op.link.clone()));
            }
        }

        (
            pending_blob_ops,
            tracked_delete_writes,
            tracked_delete_removes,
            non_tracked_delete_links,
        )
    }
}
