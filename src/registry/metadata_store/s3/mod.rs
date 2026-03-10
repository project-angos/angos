use std::{
    collections::{HashMap, HashSet, hash_map::RandomState},
    future::Future,
    hash::{BuildHasher, Hasher},
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
            BlobIndex, BlobIndexOperation, Error, LinkMetadata, LinkOperation, LockConfig,
            LockStrategy, MetadataStore,
            link_kind::LinkKind,
            lock::{self, LockBackend, MemoryBackend},
        },
        pagination, path_builder,
    },
};

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

#[derive(Clone, Debug, PartialEq)]
pub struct BackendConfig {
    pub access_key_id: String,
    pub secret_key: String,
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub key_prefix: String,
    pub lock_strategy: LockStrategy,
    pub link_cache_ttl: u64,
    pub access_time_debounce_secs: u64,
}

impl Default for BackendConfig {
    fn default() -> Self {
        Self {
            access_key_id: String::new(),
            secret_key: String::new(),
            endpoint: String::new(),
            bucket: String::new(),
            region: String::new(),
            key_prefix: String::new(),
            lock_strategy: LockStrategy::Memory,
            link_cache_ttl: default_link_cache_ttl(),
            access_time_debounce_secs: default_access_time_debounce(),
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
            access_key_id: String,
            secret_key: String,
            endpoint: String,
            bucket: String,
            region: String,
            #[serde(default)]
            key_prefix: String,
            #[serde(default)]
            redis: Option<LockConfig>,
            #[serde(default)]
            lock_strategy: Option<LockStrategy>,
            #[serde(default = "default_link_cache_ttl")]
            link_cache_ttl: u64,
            #[serde(default = "default_access_time_debounce")]
            access_time_debounce_secs: u64,
        }

        let raw = Raw::deserialize(deserializer)?;

        let lock_strategy = lock::resolve_lock_strategy(raw.lock_strategy, raw.redis, true)?;

        Ok(BackendConfig {
            access_key_id: raw.access_key_id,
            secret_key: raw.secret_key,
            endpoint: raw.endpoint,
            bucket: raw.bucket,
            region: raw.region,
            key_prefix: raw.key_prefix,
            lock_strategy,
            link_cache_ttl: raw.link_cache_ttl,
            access_time_debounce_secs: raw.access_time_debounce_secs,
        })
    }
}

fn default_link_cache_ttl() -> u64 {
    30
}

fn default_access_time_debounce() -> u64 {
    60
}

impl BackendConfig {
    pub fn to_data_store_config(&self) -> data_store::s3::BackendConfig {
        data_store::s3::BackendConfig {
            access_key_id: self.access_key_id.clone(),
            secret_key: self.secret_key.clone(),
            endpoint: self.endpoint.clone(),
            bucket: self.bucket.clone(),
            region: self.region.clone(),
            key_prefix: self.key_prefix.clone(),
            ..Default::default()
        }
    }

    pub fn to_lock_store_config(
        &self,
        lock_config: &lock::s3::S3LockConfig,
    ) -> data_store::s3::BackendConfig {
        data_store::s3::BackendConfig {
            operation_timeout_secs: lock_config.operation_timeout_secs,
            operation_attempt_timeout_secs: lock_config.operation_attempt_timeout_secs,
            max_attempts: lock_config.max_attempts,
            ..self.to_data_store_config()
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
        let guard = backend
            .lock
            .acquire(&[format!("{namespace}:{link}")])
            .await?;
        let link_data = backend
            .read_link_reference(namespace, link)
            .await?
            .accessed();
        if !guard.is_valid() {
            return Err(Error::Lock(
                "lock invalidated during access time flush".into(),
            ));
        }
        backend
            .write_link_reference(namespace, link, &link_data)
            .await?;
        guard.release().await;
        Ok(())
    }
}

struct FlushHandle {
    shutdown: Arc<AtomicBool>,
}

impl Drop for FlushHandle {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
    }
}

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
    access_time_writer: Option<AccessTimeWriter>,
    /// Whether the S3 backend supports conditional writes (If-Match / If-None-Match).
    /// When true, blob index updates use lock-free CAS. When false, they are protected
    /// by the distributed lock alongside link updates.
    supports_conditional_writes: bool,
    known_namespaces: Arc<Mutex<HashSet<String>>>,
    // Held for Drop side-effect: signals the flush task to exit when the last Backend is dropped.
    #[allow(dead_code)]
    flush_handle: Option<Arc<FlushHandle>>,
}

const MAX_UPDATE_RETRIES: u32 = 10;
const MAX_BLOB_INDEX_CAS_RETRIES: u32 = 20;

impl Backend {
    pub fn new(config: &BackendConfig) -> Result<Self, Error> {
        info!("Using S3 metadata-store backend");
        let store = data_store::s3::Backend::new(&config.to_data_store_config())?;

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
                    lock::S3LockBackend::new(lock_store, s3_lock_config).map_err(|e| {
                        Error::Lock(format!("Failed to initialize S3 lock store: {e}"))
                    })?,
                )
            }
            LockStrategy::Memory => {
                info!("Using in-memory lock store for S3 metadata-store");
                Arc::new(MemoryBackend::new())
            }
        };

        // S3 lock strategy probes conditional write support at startup, so if
        // we're using S3 locks we know conditionals work. For other lock strategies
        // the S3 backend may not support them (e.g. older S3-compatible stores).
        let supports_conditional_writes = matches!(config.lock_strategy, LockStrategy::S3(_));

        if config.access_time_debounce_secs == 0
            && matches!(config.lock_strategy, LockStrategy::S3(_))
        {
            warn!(
                "access_time_debounce_secs is 0 with S3 lock strategy; \
                 every manifest pull will acquire an S3 lock for access time updates, \
                 which adds significant latency and S3 API cost at scale. \
                 Consider setting access_time_debounce_secs to 60 or higher."
            );
        }

        let access_time_writer = if config.access_time_debounce_secs > 0 {
            Some(AccessTimeWriter::new())
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
                supports_conditional_writes,
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

            Some(Arc::new(FlushHandle { shutdown }))
        } else {
            None
        };

        let backend = Self {
            store,
            lock,
            cache: None,
            link_cache_ttl: config.link_cache_ttl,
            access_time_writer,
            supports_conditional_writes,
            known_namespaces: Arc::new(Mutex::new(HashSet::new())),
            flush_handle,
        };

        Ok(backend)
    }

    pub async fn probe_conditional_write_support(
        store: &data_store::s3::Backend,
    ) -> Result<(), Error> {
        let probe_key = format!("_angos_probe_{}", uuid::Uuid::new_v4());
        let content: &[u8] = b"probe";

        // Step 1: PUT the probe object
        if let Err(e) = store.put_object(&probe_key, content).await {
            let _ = store.delete(&probe_key).await;
            return Err(Error::Lock(format!(
                "S3 conditional write probe failed to create probe object: {e}"
            )));
        }

        // Step 2: PUT again with If-None-Match: * — expect 412 PreconditionFailed
        let if_none_match_result = store.put_object_if_not_exists(&probe_key, content).await;

        // Step 3: Test If-Match — read current ETag, update should succeed
        let if_match_result = match store.read_with_etag(&probe_key).await {
            Ok((_, Some(etag))) => {
                let update_result = store
                    .put_object_if_match(&probe_key, &etag, b"updated".to_vec())
                    .await;
                let bogus_result = store
                    .put_object_if_match(&probe_key, "\"bogus\"", b"fail".to_vec())
                    .await;
                Some((update_result, bogus_result))
            }
            Ok((_, None)) => {
                let _ = store.delete(&probe_key).await;
                return Err(Error::Lock(
                    "S3 conditional write probe: ETag not returned, \
                     If-Match support cannot be verified"
                        .to_string(),
                ));
            }
            Err(e) => {
                let _ = store.delete(&probe_key).await;
                return Err(Error::Lock(format!(
                    "S3 conditional write probe failed to read probe object for If-Match test: {e}"
                )));
            }
        };

        // Step 4: Clean up probe object regardless of outcome
        let _ = store.delete(&probe_key).await;

        // Step 5: Evaluate If-None-Match result
        match if_none_match_result {
            Err(data_store::Error::PreconditionFailed) => {}
            Ok(_) => {
                return Err(Error::Lock(
                    "S3 backend does not support If-None-Match: * conditional writes, \
                     required for lock_strategy = s3. Use lock_strategy = redis or \
                     lock_strategy = memory instead."
                        .to_string(),
                ));
            }
            Err(e) => {
                return Err(Error::Lock(format!(
                    "S3 conditional write probe (If-None-Match) failed: {e}"
                )));
            }
        }

        // Step 6: Evaluate If-Match results
        if let Some((update_result, bogus_result)) = if_match_result {
            if let Err(e) = update_result {
                return Err(Error::Lock(format!(
                    "S3 backend does not support If-Match conditional writes (update failed): {e}"
                )));
            }
            match bogus_result {
                Err(data_store::Error::PreconditionFailed) => {}
                Ok(_) => {
                    return Err(Error::Lock(
                        "S3 backend does not enforce If-Match: bogus ETag was accepted, \
                         required for lock heartbeat. Use lock_strategy = redis or \
                         lock_strategy = memory instead."
                            .to_string(),
                    ));
                }
                Err(e) => {
                    return Err(Error::Lock(format!(
                        "S3 conditional write probe (If-Match bogus) failed unexpectedly: {e}"
                    )));
                }
            }
        }

        info!("S3 conditional write probe passed (If-None-Match and If-Match supported)");
        Ok(())
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
                    let jitter_ms = {
                        let max = 50u64.saturating_mul(1u64 << attempt.min(4));
                        if max == 0 {
                            0
                        } else {
                            RandomState::new().build_hasher().finish() % max
                        }
                    };
                    tokio::time::sleep(Duration::from_millis(jitter_ms)).await;
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
        if self.supports_conditional_writes {
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

        if self.supports_conditional_writes {
            self.update_links_cas(namespace, operations).await
        } else {
            self.update_links_locked(namespace, operations).await
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
    /// CAS-optimized write path with lock-free fast path for new links.
    ///
    /// Phase 1: For creates that produce fresh metadata (non-tracked, or tracked
    /// without referrer), tries `If-None-Match: *` to atomically create the link
    /// without a distributed lock. This avoids locking entirely for the common
    /// case of fresh pushes where most links are new.
    ///
    /// Phase 2: For operations that need a lock (deletes, tracked creates with
    /// referrer merging, creates where the link already existed), acquires the
    /// lock and uses the standard read-modify-write flow.
    ///
    /// Phase 3: CAS blob index updates (always lock-free with conditional writes).
    #[allow(clippy::too_many_lines)]
    async fn update_links_cas(
        &self,
        namespace: &str,
        operations: &[LinkOperation],
    ) -> Result<(), Error> {
        // Phase 1: Optimistic lock-free creation for eligible creates.
        // Creates that produce fresh metadata (no referrer merge) can use
        // If-None-Match:* to atomically create the link without locking.
        let mut lockfree_blob_ops: HashMap<Digest, Vec<BlobIndexOperation>> = HashMap::new();
        let mut lockfree_cache_puts: Vec<(LinkKind, LinkMetadata)> = Vec::new();
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
                } if !is_tracked_link(link) || referrer.is_none() => {
                    any_creates = true;
                    let metadata = LinkMetadata::from_digest(target.clone())
                        .with_media_type(media_type.clone())
                        .with_descriptor(descriptor.as_ref().clone());
                    optimistic.push((op.clone(), link.clone(), target.clone(), metadata));
                }
                LinkOperation::Create { .. } => {
                    any_creates = true;
                    locked_ops.push(op.clone());
                }
                LinkOperation::Delete { .. } => {
                    locked_ops.push(op.clone());
                }
            }
        }

        if !optimistic.is_empty() {
            let results = join_all(optimistic.iter().map(|(_, link, _, metadata)| {
                self.write_link_if_not_exists(namespace, link, metadata)
            }))
            .await;

            for ((op, link, target, metadata), result) in optimistic.into_iter().zip(results) {
                match result {
                    Ok(true) => {
                        lockfree_blob_ops
                            .entry(target)
                            .or_default()
                            .push(BlobIndexOperation::Insert(link.clone()));
                        lockfree_cache_puts.push((link, metadata));
                    }
                    Ok(false) => {
                        locked_ops.push(op);
                    }
                    Err(e) => return Err(e),
                }
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

        // Cache updates for lock-free writes.
        for (link, metadata) in &lockfree_cache_puts {
            self.cache_put(namespace, link, metadata).await;
        }

        // Phase 3: CAS blob index updates — ETags handle concurrency.
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
    #[allow(clippy::too_many_lines)]
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

        let (pending_blob_ops, tracked_create_writes, non_tracked_create_writes) =
            Self::build_create_ops(&creates, &mut link_cache);

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
        ) = Self::build_delete_ops(&deletes, &mut link_cache, pending_blob_ops);

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

            let (pending_blob_ops, tracked_create_writes, non_tracked_create_writes) =
                Self::build_create_ops(&creates, &mut link_cache);

            if !guard.is_valid() {
                guard.release().await;
                return Err(Error::Lock("lock invalidated during operation".into()));
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

            let (
                pending_blob_ops,
                tracked_delete_writes,
                tracked_delete_removes,
                non_tracked_delete_links,
            ) = Self::build_delete_ops(&valid_deletes, &mut link_cache, pending_blob_ops);

            if !guard.is_valid() {
                guard.release().await;
                return Err(Error::Lock("lock invalidated during operation".into()));
            }

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
                                let fut: Pin<
                                    Box<dyn Future<Output = Result<(), Error>> + Send + '_>,
                                > = Box::pin(self.delete_link_reference(namespace, link));
                                fut
                            }),
                    ),
            )
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;

            if !guard.is_valid() {
                guard.release().await;
                return Err(Error::Lock("lock invalidated during operation".into()));
            }

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

    /// Builds blob index operations and link writes for create operations.
    fn build_create_ops(
        creates: &[CreateOp],
        link_cache: &mut HashMap<LinkKind, LinkMetadata>,
    ) -> CreateOpsResult {
        let mut pending_blob_ops: HashMap<Digest, Vec<BlobIndexOperation>> = HashMap::new();
        let mut tracked_create_writes: Vec<(LinkKind, LinkMetadata)> = Vec::new();
        let mut non_tracked_create_writes: Vec<(LinkKind, LinkMetadata)> = Vec::new();

        for (link, target, old_target, referrer, media_type, descriptor) in creates {
            let is_tracked = is_tracked_link(link);

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

            if self.supports_conditional_writes {
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

        if self.supports_conditional_writes {
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
                    let jitter_ms = {
                        let max = 50u64.saturating_mul(1u64 << attempt.min(4));
                        if max == 0 {
                            0
                        } else {
                            RandomState::new().build_hasher().finish() % max
                        }
                    };
                    tokio::time::sleep(Duration::from_millis(jitter_ms)).await;
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

#[cfg(test)]
mod tests {
    use std::{str::FromStr, time::Instant};

    use super::*;
    use crate::{
        cache,
        cache::CacheExt,
        registry::metadata_store::{LinkOperation, MetadataStore},
    };

    fn test_config() -> BackendConfig {
        BackendConfig {
            access_key_id: "root".to_string(),
            secret_key: "roottoor".to_string(),
            endpoint: "http://127.0.0.1:9000".to_string(),
            region: "region".to_string(),
            bucket: "registry".to_string(),
            key_prefix: format!("test-cache-{}", uuid::Uuid::new_v4()),
            lock_strategy: LockStrategy::Memory,
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
            descriptor: Box::new(None),
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
            descriptor: Box::new(None),
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
            descriptor: Box::new(None),
        }];
        backend.update_links(namespace, &ops).await.unwrap();

        // Read to populate cache
        let meta = backend.read_link(namespace, &tag, false).await.unwrap();
        assert_eq!(meta.target, digest_a);

        // Wait for cache to expire
        tokio::time::sleep(Duration::from_millis(1100)).await;

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
            descriptor: Box::new(None),
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
            descriptor: Box::new(None),
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
            descriptor: Box::new(None),
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
            descriptor: Box::new(None),
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
            descriptor: Box::new(None),
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
            descriptor: Box::new(None),
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
            descriptor: Box::new(None),
        }];
        backend.update_links(namespace_a, &ops_a).await.unwrap();

        let ops_b = vec![LinkOperation::Create {
            link: tag.clone(),
            target: digest_b.clone(),
            referrer: None,
            media_type: None,
            descriptor: Box::new(None),
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
            descriptor: Box::new(None),
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
            descriptor: Box::new(None),
        }];
        backend.update_links(namespace, &ops).await.unwrap();

        // Trigger deferred access time update
        backend.read_link(namespace, &tag, true).await.unwrap();

        // Wait for the background flush (debounce = 1s, wait 1.5s)
        tokio::time::sleep(Duration::from_millis(1500)).await;

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
            descriptor: Box::new(None),
        }];
        backend.update_links(namespace, &ops).await.unwrap();

        // Call read_link with update_access_time 10 times rapidly
        let start = Instant::now();
        for _ in 0..10 {
            let meta = backend.read_link(namespace, &tag, true).await.unwrap();
            assert_eq!(meta.target, digest);
        }
        let elapsed = start.elapsed();

        // All 10 reads should complete quickly since writes are deferred
        assert!(
            elapsed < Duration::from_secs(1),
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
                descriptor: Box::new(None),
            },
            LinkOperation::Create {
                link: tag2.clone(),
                target: digest2.clone(),
                referrer: None,
                media_type: None,
                descriptor: Box::new(None),
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
            descriptor: Box::new(None),
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
            descriptor: Box::new(None),
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
            descriptor: Box::new(None),
        }];
        backend.update_links(namespace, &ops).await.unwrap();

        // Spawn 50 concurrent read_link calls with update_access_time=true
        let start = Instant::now();
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
            elapsed < Duration::from_secs(2),
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
                descriptor: Box::new(None),
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
        let start = Instant::now();
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
            elapsed < Duration::from_secs(5),
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
                descriptor: Box::new(None),
            },
            LinkOperation::Create {
                link: tag2.clone(),
                target: digest2,
                referrer: None,
                media_type: None,
                descriptor: Box::new(None),
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
                descriptor: Box::new(None),
            })
            .collect();

        backend.update_links(namespace, &ops).await.unwrap();

        for (i, digest) in digests.iter().enumerate() {
            let blob_index = backend.read_blob_index(digest).await.unwrap();
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
                descriptor: Box::new(None),
            })
            .collect();

        ops.push(LinkOperation::Create {
            link: LinkKind::Config(config_digest.clone()),
            target: config_digest.clone(),
            referrer: Some(referrer_digest.clone()),
            media_type: None,
            descriptor: Box::new(None),
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
                descriptor: Box::new(None),
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

            let result = backend.read_blob_index(d).await;
            assert!(
                matches!(result, Err(Error::ReferenceNotFound)),
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
                descriptor: Box::new(None),
            },
            LinkOperation::Create {
                link: LinkKind::Tag("remove-tag".into()),
                target: digest_remove.clone(),
                referrer: None,
                media_type: None,
                descriptor: Box::new(None),
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
                descriptor: Box::new(None),
            },
        ];
        backend.update_links(namespace, &mixed_ops).await.unwrap();

        // Verify: keep-tag still exists
        let keep_index = backend.read_blob_index(&digest_keep).await.unwrap();
        let keep_links = keep_index.namespace.get(namespace).unwrap();
        assert!(keep_links.contains(&LinkKind::Tag("keep-tag".into())));

        // Verify: remove-tag blob index no longer has it
        match backend.read_blob_index(&digest_remove).await {
            Ok(idx) => {
                let links = idx.namespace.get(namespace);
                assert!(
                    links.is_none()
                        || !links.unwrap().contains(&LinkKind::Tag("remove-tag".into())),
                    "remove-tag should not be in blob index after delete"
                );
            }
            Err(Error::ReferenceNotFound) => {}
            Err(e) => panic!("Unexpected error reading blob index: {e}"),
        }

        // Verify: new-tag exists in digest_add's blob index
        let add_index = backend.read_blob_index(&digest_add).await.unwrap();
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
            descriptor: Box::new(None),
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

    #[tokio::test]
    async fn test_probe_conditional_write_support() {
        let config = test_config();
        let store = data_store::s3::Backend::new(&data_store::s3::BackendConfig {
            access_key_id: config.access_key_id.clone(),
            secret_key: config.secret_key.clone(),
            endpoint: config.endpoint.clone(),
            bucket: config.bucket.clone(),
            region: config.region.clone(),
            key_prefix: config.key_prefix.clone(),
            ..Default::default()
        })
        .unwrap();

        let result = Backend::probe_conditional_write_support(&store).await;
        assert!(
            result.is_ok(),
            "Probe should pass on MinIO (supports If-None-Match): {result:?}"
        );
    }
}
