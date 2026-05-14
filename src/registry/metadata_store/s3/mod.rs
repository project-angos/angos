use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
pub use config::BackendConfig;
use futures_util::stream::{self, StreamExt};
use tokio::sync::Mutex;
use tracing::{debug, info, instrument, warn};

use crate::{
    cache::Cache,
    oci::{Descriptor, Digest},
    registry::{
        data_store,
        metadata_store::{
            BlobIndex, BlobIndexOperation, ConditionalCapabilities, Error, LinkMetadata,
            LinkOperation, LockStrategy, MetadataStore, link_kind::LinkKind, lock_ops::LockOps,
            referrer_resolver::resolve_referrer_descriptor,
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
mod probe;

#[cfg(test)]
mod tests;

use coordinator::WriteCoordinator;
pub use probe::probe_conditional_capabilities;

#[derive(Clone)]
pub struct Backend {
    pub store: data_store::s3::Backend,
    cache: Option<Arc<Cache>>,
    link_cache_ttl: u64,
    access_time_writer: Option<access_time::AccessTimeWriter>,
    coordinator: Arc<dyn WriteCoordinator>,
    pub known_namespaces: Arc<Mutex<HashSet<String>>>,
    // Held for Drop side-effect: signals the flush task to exit when the last Backend is dropped.
    #[allow(dead_code)]
    flush_handle: Option<Arc<access_time::FlushHandle>>,
}

const MAX_BLOB_INDEX_CAS_RETRIES: u32 = 20;

impl Backend {
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

        let coordinator = coordinator::build_coordinator(config, &caps)?;

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

    pub fn with_cache(mut self, cache: Arc<Cache>) -> Self {
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
            .list_prefixes(&tags_dir, "/", n, None, last)
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

            let digest_entries: Vec<Digest> = objects
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
                            |path| async move { self.store.read(&path).await },
                        )
                        .await
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
            .list_prefixes(&revisions_dir, "/", n, continuation_token, None)
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
        self.read_blob_index_impl(digest).await
    }

    #[instrument(skip(self))]
    async fn read_blob_index_namespace(
        &self,
        namespace: &str,
        digest: &Digest,
    ) -> Result<HashSet<LinkKind>, Error> {
        self.read_blob_index_namespace_impl(namespace, digest).await
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
