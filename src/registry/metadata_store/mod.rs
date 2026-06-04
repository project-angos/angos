use std::{
    collections::HashSet,
    io,
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};

use bytes::Bytes;
use futures_util::stream::{self, StreamExt};
use tracing::{debug, info, instrument, warn};
use uuid::Uuid;

use angos_tx_engine::{
    StorageError,
    error::Error as TxError,
    executor::{DEFAULT_RETRY_BUDGET, TransactionExecutor, execute_with_retry},
    lock::LockSession,
    store::Store,
    transaction::{Mutation, Transaction},
};

use crate::{
    cache::Cache,
    oci::{Descriptor, Digest},
    registry::{pagination, pagination::collect_all_pages, path_builder},
};

mod access_time;
mod blob_index;
mod error;
pub mod link_kind;
mod link_metadata;
mod link_operation;
mod link_ops;
mod manifest;
mod namespace_registry;
mod sharded;

pub mod referrer_resolver;

#[cfg(test)]
mod tests;

pub use blob_index::{BlobIndex, BlobIndexOperation};
pub use error::Error;
pub use link_metadata::LinkMetadata;
pub use link_operation::LinkOperation;

use access_time::{AccessTimeWriter, FlushHandle};
use link_kind::LinkKind;
use link_ops::tx_error_to_meta;
use referrer_resolver::resolve_referrer_descriptor;
use sharded::{
    SHARD_READ_CONCURRENCY, apply_blob_index_operations, collect_blob_index_shards,
    decode_blob_index_shard_namespace, namespace_links_from_index, non_empty_links_or_not_found,
};

/// Canonical key for the coarse `blob-data:{digest}` lock.
///
/// This is the single source of truth for the lock string. Both the directly
/// acquired lock ([`MetadataStore::acquire_blob_data_lock`]) and the
/// transaction-level `coarse_lock(...)` declarations in `link_ops` build their
/// key here, so the two can never drift out of sync.
pub fn blob_data_lock_key(digest: &Digest) -> String {
    format!("blob-data:{digest}")
}

// ───────────────────────────────────────────────────────────────────────────
// MetadataStore (concrete implementation)
// ───────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct MetadataStore {
    /// Storage façade: reads, read-for-update, and coordinated writes all flow
    /// through here. Owns the object store and the transaction executor.
    store: Arc<Store>,
    cache: Option<Arc<Cache>>,
    link_cache_ttl: u64,
    access_time_writer: Option<AccessTimeWriter>,
    // Held for Drop side-effect: signals the flush task to exit when the last clone is dropped.
    _flush_handle: Option<Arc<FlushHandle>>,
}

pub struct Builder {
    store: Option<Arc<Store>>,
    cache: Option<Arc<Cache>>,
    link_cache_ttl: u64,
    access_time_debounce_secs: u64,
}

impl Builder {
    fn new() -> Self {
        Self {
            store: None,
            cache: None,
            link_cache_ttl: 30,
            access_time_debounce_secs: 0,
        }
    }

    /// Set the storage façade (required).
    ///
    /// The façade carries both the object store used for reads and the
    /// transaction executor that encapsulates the CAS-vs-Locked decision.
    pub fn store(mut self, store: Arc<Store>) -> Self {
        self.store = Some(store);
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

    pub fn build(self) -> Result<MetadataStore, Error> {
        let store = self
            .store
            .ok_or_else(|| Error::Coordination("MetadataStore requires a store".to_string()))?;

        let (access_time_writer, flush_handle) = if self.access_time_debounce_secs > 0 {
            let writer = AccessTimeWriter::new();
            let shutdown = Arc::new(AtomicBool::new(false));
            let interval = Duration::from_secs(self.access_time_debounce_secs);

            let flush_backend = MetadataStore {
                store: store.clone(),
                cache: None,
                link_cache_ttl: self.link_cache_ttl,
                access_time_writer: Some(writer.clone()),
                _flush_handle: None,
            };

            MetadataStore::spawn_flush_task(flush_backend, shutdown.clone(), interval);

            (Some(writer), Some(Arc::new(FlushHandle::new(shutdown))))
        } else {
            (None, None)
        };

        Ok(MetadataStore {
            store,
            cache: self.cache,
            link_cache_ttl: self.link_cache_ttl,
            access_time_writer,
            _flush_handle: flush_handle,
        })
    }
}

impl MetadataStore {
    pub fn builder() -> Builder {
        Builder::new()
    }

    /// Returns the storage façade used for all reads and coordinated writes.
    pub fn store(&self) -> &Store {
        self.store.as_ref()
    }

    /// Returns an owned handle to the storage façade, for closures and helpers
    /// that need to capture it across `await` points.
    pub fn store_arc(&self) -> Arc<Store> {
        self.store.clone()
    }

    /// Returns the transaction executor.
    pub fn executor(&self) -> &dyn TransactionExecutor {
        self.store.executor().as_ref()
    }

    /// Acquire the coarse [`blob_data_lock_key`] lock for `digest`.
    ///
    /// The lock lives on the METADATA executor — the single lock domain for
    /// blob-data — even though the bytes guarded by it may actually be mutated
    /// on the separate BLOB engine (the metadata and blob subsystems each
    /// instantiate their own engine with its own lock domain). Anchoring the
    /// lock here is deliberate: it is the one executor that every blob-data
    /// participant agrees on, so that manifest pushes (which declare the same
    /// key via `coarse_lock` on their metadata transaction), upload completion,
    /// and reclamation/scrub all serialize on the same key/executor. Routing
    /// every acquisition through this method keeps the lock/engine pairing from
    /// drifting across the production call sites.
    pub async fn acquire_blob_data_lock(&self, digest: &Digest) -> Result<LockSession, Error> {
        let keys = [blob_data_lock_key(digest)];
        self.executor()
            .acquire(&keys)
            .await
            .map_err(|e| Error::Coordination(format!("blob-data lock acquire failed: {e}")))
    }

    /// Return this deployment's persisted instance identifier, generating and
    /// persisting one on first call.
    ///
    /// The id is stored at [`path_builder::instance_id_path`] as a raw UTF-8
    /// UUID v4 string. On a fresh store the first caller wins the
    /// [`Mutation::PutIfAbsent`] CAS and returns the value it generated; a
    /// racing concurrent boot loses the CAS (surfaced as
    /// [`TxError::Precondition`]/[`TxError::Conflict`]) and re-reads the winner's
    /// id, so every caller — across boots and across racing boots — observes the
    /// same stable id.
    ///
    /// Wired into server setup so the registry can stamp and filter its own
    /// replication origin.
    pub async fn get_or_init_instance_id(&self) -> Result<String, Error> {
        let path = path_builder::instance_id_path();

        match self.store().get(&path).await {
            Ok(data) => {
                return String::from_utf8(data)
                    .map_err(|e| Error::InvalidData(format!("instance_id not valid UTF-8: {e}")));
            }
            Err(StorageError::NotFound) => {}
            Err(e) => return Err(e.into()),
        }

        let new_id = Uuid::new_v4().to_string();
        let tx = Transaction::builder()
            .mutation(Mutation::PutIfAbsent {
                key: path.clone(),
                body: Bytes::from(new_id.clone().into_bytes()),
            })
            .build();

        match self.store().execute(tx).await {
            Ok(_) => Ok(new_id),
            // A racing boot persisted its own id first: re-read and adopt it so
            // both processes converge on the same value.
            Err(TxError::Precondition | TxError::Conflict) => {
                let data = self.store().get(&path).await?;
                String::from_utf8(data)
                    .map_err(|e| Error::InvalidData(format!("instance_id not valid UTF-8: {e}")))
            }
            Err(e) => Err(Error::Coordination(format!(
                "failed to persist instance_id: {e}"
            ))),
        }
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

    // ── Public API ────────────────────────────────────────────────────────

    #[instrument(skip(self))]
    pub async fn list_namespaces(
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

        Ok(pagination::paginate_sorted(&namespaces, n, last.as_deref()))
    }

    #[instrument(skip(self))]
    pub async fn list_tags(
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

    /// Returns the `LinkKind::Tag` entries in `namespace` that currently point
    /// at `digest`.
    #[instrument(skip(self))]
    pub async fn find_tags_pointing_at(
        &self,
        namespace: &str,
        digest: &Digest,
    ) -> Result<Vec<LinkKind>, Error> {
        let all_tags =
            collect_all_pages(|marker| async move { self.list_tags(namespace, 100, marker).await })
                .await?;

        let matching = stream::iter(all_tags)
            .map(|tag| async move {
                let result = self
                    .read_link(namespace, &LinkKind::Tag(tag.clone()), false)
                    .await;
                (tag, result)
            })
            .buffer_unordered(20)
            .filter_map(|(tag, result)| async move {
                if let Ok(metadata) = result
                    && &metadata.target == digest
                {
                    Some(LinkKind::Tag(tag))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
            .await;

        Ok(matching)
    }

    #[instrument(skip(self))]
    pub async fn list_referrers(
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

    pub async fn has_referrers(&self, namespace: &str, subject: &Digest) -> Result<bool, Error> {
        let referrers_dir = path_builder::manifest_referrers_dir(namespace, subject);
        let page = self.store().list(&referrers_dir, 1, None).await?;
        Ok(!page.items.is_empty())
    }

    pub async fn list_revisions(
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

    pub async fn count_manifests(&self, namespace: &str) -> Result<usize, Error> {
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
    pub async fn read_blob_index(&self, digest: &Digest) -> Result<BlobIndex, Error> {
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
    pub async fn has_blob_references(&self, digest: &Digest) -> Result<bool, Error> {
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
    pub async fn read_blob_index_namespace(
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
    pub async fn migrate_blob_index(&self, digest: &Digest) -> Result<(), Error> {
        let legacy_path = path_builder::blob_index_path(digest);

        // Read the legacy file once outside the retry loop; if absent, nothing
        // to migrate. On conflict the engine re-reads under the lock, so a
        // concurrent migrator wins and we bail cleanly.
        let data = match self.store().get(&legacy_path).await {
            Ok(d) => d,
            Err(StorageError::NotFound) => return Ok(()),
            Err(e) => return Err(e.into()),
        };

        let blob_index: BlobIndex = serde_json::from_slice(&data).map_err(Error::from)?;
        let namespace_count = blob_index.namespace.len();
        let raw_legacy = Bytes::from(data);

        execute_with_retry(
            self.executor(),
            || async {
                let mut builder =
                    Transaction::builder().read(legacy_path.clone(), raw_legacy.clone());

                for (namespace, links) in &blob_index.namespace {
                    let shard_path = path_builder::blob_index_shard_path(digest, namespace);
                    let operations: Vec<BlobIndexOperation> = links
                        .iter()
                        .map(|l| BlobIndexOperation::Insert(l.clone()))
                        .collect();

                    // Re-read each shard inside the closure so the
                    // fingerprint is fresh on every retry attempt.
                    match self.store().get(&shard_path).await {
                        Ok(shard_data) => {
                            let shard_raw = Bytes::from(shard_data.clone());
                            let mut existing: HashSet<LinkKind> =
                                serde_json::from_slice(&shard_data).unwrap_or_default();
                            apply_blob_index_operations(&mut existing, &operations);
                            builder = builder.read(shard_path.clone(), shard_raw);
                            if existing.is_empty() {
                                builder = builder.mutation(Mutation::Delete {
                                    key: shard_path,
                                    expected: None,
                                });
                            } else {
                                let body = Bytes::from(
                                    serde_json::to_vec(&existing).map_err(TxError::Serde)?,
                                );
                                builder = builder.mutation(Mutation::Put {
                                    key: shard_path,
                                    body,
                                    expected: None,
                                });
                            }
                        }
                        Err(StorageError::NotFound) => {
                            let mut new_links = HashSet::new();
                            apply_blob_index_operations(&mut new_links, &operations);
                            if !new_links.is_empty() {
                                let body = Bytes::from(
                                    serde_json::to_vec(&new_links).map_err(TxError::Serde)?,
                                );
                                builder = builder.mutation(Mutation::PutIfAbsent {
                                    key: shard_path,
                                    body,
                                });
                            }
                        }
                        Err(e) => return Err(TxError::Storage(e)),
                    }
                }

                builder = builder.mutation(Mutation::Delete {
                    key: legacy_path.clone(),
                    expected: None,
                });

                Ok(builder.build())
            },
            DEFAULT_RETRY_BUDGET,
        )
        .await
        .map_err(tx_error_to_meta)?;

        info!("Migrated legacy blob index for '{digest}' ({namespace_count} namespaces)",);
        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn migrate_namespace_registry(&self) -> Result<(), Error> {
        self.rebuild_namespace_registry().await?;
        self.store()
            .delete(&path_builder::namespace_registry_path())
            .await?;
        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn read_link(
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
                let link_data = self.update_link_access_time(namespace, link).await?;
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

    /// Mark the access time for `link` in `namespace` using a read-modify-write
    /// transaction, returning the updated [`LinkMetadata`].
    ///
    /// Uses the same retry loop as all other engine-backed writes. Concurrent
    /// updaters resolve via content-hash conflict detection; the one that commits
    /// last wins, which is the desired behaviour for advisory access-time stamps.
    async fn update_link_access_time(
        &self,
        namespace: &str,
        link: &LinkKind,
    ) -> Result<LinkMetadata, Error> {
        let link_path = path_builder::link_path(link, namespace);
        let keys = [link_path.clone()];

        let (_, link_data) = self
            .store()
            .update_with_payload(
                &keys,
                |snaps| {
                    let link_path = link_path.clone();
                    async move {
                        let snap = &snaps[0];
                        if !snap.present {
                            return Err(TxError::Storage(StorageError::NotFound));
                        }
                        let link_data = LinkMetadata::from_bytes(snap.body.to_vec())
                            .map_err(|e| TxError::Build(e.to_string()))?
                            .accessed();
                        let serialized =
                            Bytes::from(serde_json::to_vec(&link_data).map_err(TxError::Serde)?);
                        Ok((
                            vec![Mutation::Put {
                                key: link_path,
                                body: serialized,
                                expected: None,
                            }],
                            link_data,
                        ))
                    }
                },
                DEFAULT_RETRY_BUDGET,
            )
            .await
            .map_err(tx_error_to_meta)?;

        Ok(link_data)
    }

    pub async fn flush_access_times(&self) {
        if let Some(writer) = &self.access_time_writer {
            writer.flush(self).await;
        }
    }
}

#[cfg(test)]
mod instance_id_tests {
    use std::sync::Arc;

    use angos_storage::MemoryObjectStore;
    use uuid::Uuid;

    use crate::registry::{
        metadata_store::MetadataStore,
        test_utils::{locked_executor_over, metadata_store_over},
    };

    fn in_memory_store() -> Arc<MetadataStore> {
        let object = Arc::new(MemoryObjectStore::new());
        let executor = locked_executor_over(object.clone());
        metadata_store_over(object, executor)
    }

    #[tokio::test]
    async fn get_or_init_instance_id_is_idempotent() {
        let store = in_memory_store();

        let first = store.get_or_init_instance_id().await.unwrap();
        let second = store.get_or_init_instance_id().await.unwrap();

        assert!(!first.is_empty());
        assert_eq!(first, second, "repeat calls must return the same id");
        // It is a parseable UUID.
        assert!(Uuid::parse_str(&first).is_ok());
    }
}
