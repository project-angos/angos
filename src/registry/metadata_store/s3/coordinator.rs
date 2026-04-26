use std::{collections::HashMap, io::ErrorKind, sync::Arc, time::Duration};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::future::join_all;
use tracing::{debug, instrument, warn};

use super::{Backend, MAX_BLOB_INDEX_CAS_RETRIES};
use crate::{
    oci::Digest,
    registry::{
        data_store,
        metadata_store::{
            BlobIndexOperation, Error, LinkMetadata, LinkOperation, ResolvedCreate, ResolvedDelete,
            link_kind::LinkKind,
            lock::{LockBackend, S3LockBackend},
            lock_ops::{LockOps, ValidationResult},
            simple_jitter,
        },
        path_builder,
    },
};

#[async_trait]
pub trait WriteCoordinator: Send + Sync + std::fmt::Debug {
    async fn update_links(
        &self,
        backend: &Backend,
        namespace: &str,
        operations: &[LinkOperation],
    ) -> Result<(), Error>;

    async fn update_blob_index(
        &self,
        backend: &Backend,
        namespace: &str,
        digest: &Digest,
        operation: BlobIndexOperation,
    ) -> Result<(), Error>;

    async fn register_namespace(&self, backend: &Backend, namespace: &str) -> Result<(), Error>;

    async fn rebuild_namespace_registry_shard(
        &self,
        backend: &Backend,
        shard_key: &str,
        path: &str,
        content: Bytes,
    ) -> Result<(), Error>;

    async fn touch_link_access_time(
        &self,
        backend: &Backend,
        namespace: &str,
        link: &LinkKind,
    ) -> Result<LinkMetadata, Error>;

    async fn flush_access_time(
        &self,
        backend: &Backend,
        namespace: &str,
        link: &LinkKind,
    ) -> Result<(), Error>;
}

#[derive(Debug)]
pub struct CasCoordinator {
    pub lock: Arc<S3LockBackend>,
}

#[derive(Debug)]
pub struct LockCoordinator {
    pub lock: Arc<dyn LockBackend + Send + Sync>,
}

impl CasCoordinator {
    /// Executes link operations under a distributed lock, returning pending
    /// blob index operations for CAS updates after the lock is released.
    async fn execute_locked_cas_updates(
        &self,
        backend: &Backend,
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
            let metadata = backend.read_link_reference(namespace, link).await.ok();
            (op, metadata)
        }))
        .await;

        let mut link_cache: HashMap<LinkKind, LinkMetadata> = HashMap::new();
        let mut creates: Vec<ResolvedCreate> = Vec::new();
        let mut deletes: Vec<ResolvedDelete> = Vec::new();

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
                    creates.push(ResolvedCreate {
                        link: link.clone(),
                        target: target.clone(),
                        old_target,
                        referrer: referrer.clone(),
                        media_type: media_type.clone(),
                        descriptor: descriptor.as_deref().cloned(),
                    });
                }
                LinkOperation::Delete { link, referrer } => {
                    if let Some(m) = metadata {
                        link_cache.insert(link.clone(), m.clone());
                        deletes.push(ResolvedDelete {
                            link: link.clone(),
                            target: m.target.clone(),
                            referrer: referrer.clone(),
                        });
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

        let pending_blob_ops = backend
            .apply_link_operations(namespace, &creates, &deletes, &mut link_cache)
            .await?;

        guard.release().await;

        Ok(pending_blob_ops)
    }
}

#[async_trait]
impl WriteCoordinator for CasCoordinator {
    #[instrument(name = "update_links_cas", skip(self, backend, operations))]
    async fn update_links(
        &self,
        backend: &Backend,
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
                        .with_descriptor(descriptor.as_deref().cloned());
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

        let link_results = join_all(optimistic.iter().map(|(_, link, _, metadata)| {
            backend.write_link_if_not_exists(namespace, link, metadata)
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
                    backend.cache_put(namespace, &link, &metadata).await;
                }
                Ok(false) => {
                    locked_ops.push(op);
                }
                Err(e) => return Err(e),
            }
        }

        let locked_blob_ops = self
            .execute_locked_cas_updates(backend, namespace, &locked_ops)
            .await?;

        let mut pending_blob_ops = lockfree_blob_ops;
        for (digest, ops) in locked_blob_ops {
            pending_blob_ops.entry(digest).or_default().extend(ops);
        }

        join_all(
            pending_blob_ops
                .iter()
                .map(|(digest, ops)| backend.update_blob_index_cas(namespace, digest, ops)),
        )
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

        if any_creates && let Err(e) = backend.register_namespace(namespace).await {
            warn!(namespace, error = %e, "Failed to register namespace");
        }

        Ok(())
    }

    #[instrument(name = "update_blob_index_cas", skip(self, backend))]
    async fn update_blob_index(
        &self,
        backend: &Backend,
        namespace: &str,
        digest: &Digest,
        operation: BlobIndexOperation,
    ) -> Result<(), Error> {
        backend
            .update_blob_index_cas(namespace, digest, &[operation])
            .await
    }

    #[instrument(name = "register_namespace_cas", skip(self, backend))]
    async fn register_namespace(&self, backend: &Backend, namespace: &str) -> Result<(), Error> {
        let path = path_builder::namespace_registry_shard_path(namespace);

        for attempt in 0..MAX_BLOB_INDEX_CAS_RETRIES {
            let (mut registry, etag) = match backend.store.read_with_etag(&path).await {
                Ok((data, etag)) => {
                    match serde_json::from_slice::<super::namespace_registry::NamespaceRegistry>(
                        &data,
                    ) {
                        Ok(r) => (r, etag),
                        Err(_) => (
                            super::namespace_registry::NamespaceRegistry::default(),
                            etag,
                        ),
                    }
                }
                Err(e) if e.kind() == ErrorKind::NotFound => (
                    super::namespace_registry::NamespaceRegistry::default(),
                    None,
                ),
                Err(e) => return Err(Error::from(e)),
            };

            let Err(pos) = registry.namespaces.binary_search(&namespace.to_string()) else {
                backend
                    .known_namespaces
                    .lock()
                    .await
                    .insert(namespace.to_string());
                return Ok(());
            };
            registry.namespaces.insert(pos, namespace.to_string());

            let content = Bytes::from(serde_json::to_vec(&registry)?);
            let write_result = if let Some(ref etag) = etag {
                backend
                    .store
                    .put_object_if_match(&path, etag, content)
                    .await
                    .map(|_| ())
            } else {
                backend
                    .store
                    .put_object_if_not_exists(&path, content)
                    .await
                    .map(|_| ())
            };

            match write_result {
                Ok(()) => {
                    backend
                        .known_namespaces
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

    #[instrument(
        name = "rebuild_namespace_registry_shard_cas",
        skip(self, backend, content)
    )]
    async fn rebuild_namespace_registry_shard(
        &self,
        backend: &Backend,
        shard_key: &str,
        path: &str,
        content: Bytes,
    ) -> Result<(), Error> {
        let etag = match backend.store.read_with_etag(path).await {
            Ok((_, etag)) => etag,
            Err(e) if e.kind() == ErrorKind::NotFound => None,
            Err(e) => return Err(Error::from(e)),
        };
        let write_result = if let Some(ref etag) = etag {
            backend
                .store
                .put_object_if_match(path, etag, content)
                .await
                .map(|_| ())
        } else {
            backend
                .store
                .put_object_if_not_exists(path, content)
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
        Ok(())
    }

    #[instrument(name = "touch_link_access_time_cas", skip(self, backend))]
    async fn touch_link_access_time(
        &self,
        backend: &Backend,
        namespace: &str,
        link: &LinkKind,
    ) -> Result<LinkMetadata, Error> {
        let link_data = backend
            .read_and_spawn_access_time_update(namespace, link)
            .await?;
        backend.cache_put(namespace, link, &link_data).await;
        Ok(link_data)
    }

    #[instrument(name = "flush_access_time_cas", skip(self, backend))]
    async fn flush_access_time(
        &self,
        backend: &Backend,
        namespace: &str,
        link: &LinkKind,
    ) -> Result<(), Error> {
        let link_path = path_builder::link_path(link, namespace);
        let (data, etag) = backend.store.read_with_etag(&link_path).await?;
        let link_data = LinkMetadata::from_bytes(data)?.accessed();
        if let Some(etag) = etag {
            let content =
                serde_json::to_vec(&link_data).map_err(|e| Error::InvalidData(e.to_string()))?;
            backend
                .store
                .put_object_if_match(&link_path, &etag, content)
                .await?;
        }
        Ok(())
    }
}

#[async_trait]
impl WriteCoordinator for LockCoordinator {
    #[instrument(name = "update_links_locked", skip(self, backend, operations))]
    async fn update_links(
        &self,
        backend: &Backend,
        namespace: &str,
        operations: &[LinkOperation],
    ) -> Result<(), Error> {
        let mut update_retries = super::MAX_UPDATE_RETRIES;
        loop {
            let mut link_cache: HashMap<LinkKind, LinkMetadata> = HashMap::new();

            let (creates, deletes, lock_keys) = backend
                .prelock_resolve_operations(namespace, operations)
                .await;

            if creates.is_empty() && deletes.is_empty() {
                return Ok(());
            }

            let guard = self.lock.acquire(&lock_keys).await?;

            if backend
                .validate_creates_under_lock(namespace, &creates, &mut link_cache)
                .await
                == ValidationResult::NeedsRetry
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

            if !guard.is_valid() {
                guard.release().await;
                return Err(Error::Lock("lock invalidated during operation".into()));
            }

            let (valid_deletes, delete_status) = backend
                .validate_deletes_under_lock(namespace, deletes, &mut link_cache)
                .await?;

            if delete_status == ValidationResult::NeedsRetry {
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

            let pending_blob_ops = backend
                .apply_link_operations(namespace, &creates, &valid_deletes, &mut link_cache)
                .await?;

            if !guard.is_valid() {
                guard.release().await;
                return Err(Error::Lock("lock invalidated during operation".into()));
            }

            join_all(
                pending_blob_ops
                    .iter()
                    .map(|(digest, ops)| backend.update_blob_index_shard(namespace, digest, ops)),
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
                && let Err(e) = backend.register_namespace(namespace).await
            {
                warn!(namespace, error = %e, "Failed to register namespace");
            }

            return Ok(());
        }
    }

    #[instrument(name = "update_blob_index_locked", skip(self, backend))]
    async fn update_blob_index(
        &self,
        backend: &Backend,
        namespace: &str,
        digest: &Digest,
        operation: BlobIndexOperation,
    ) -> Result<(), Error> {
        backend
            .update_blob_index_shard(namespace, digest, &[operation])
            .await
    }

    #[instrument(name = "register_namespace_locked", skip(self, backend))]
    async fn register_namespace(&self, backend: &Backend, namespace: &str) -> Result<(), Error> {
        let shard_key = path_builder::namespace_shard_key(namespace);
        let path = path_builder::namespace_registry_shard_path(namespace);
        let lock_key = format!("namespace_registry_shard_{shard_key}");
        let guard = self.lock.acquire(&[lock_key]).await?;

        let mut registry = match backend.store.read(&path).await {
            Ok(data) => {
                serde_json::from_slice::<super::namespace_registry::NamespaceRegistry>(&data)
                    .unwrap_or_default()
            }
            Err(e) if e.kind() == ErrorKind::NotFound => {
                super::namespace_registry::NamespaceRegistry::default()
            }
            Err(e) => {
                guard.release().await;
                return Err(Error::from(e));
            }
        };

        if let Err(pos) = registry.namespaces.binary_search(&namespace.to_string()) {
            registry.namespaces.insert(pos, namespace.to_string());
            let content = Bytes::from(serde_json::to_vec(&registry)?);
            if let Err(e) = backend.store.put_object(&path, content).await {
                guard.release().await;
                return Err(e.into());
            }
        }

        guard.release().await;
        backend
            .known_namespaces
            .lock()
            .await
            .insert(namespace.to_string());
        Ok(())
    }

    #[instrument(
        name = "rebuild_namespace_registry_shard_locked",
        skip(self, backend, content)
    )]
    async fn rebuild_namespace_registry_shard(
        &self,
        backend: &Backend,
        shard_key: &str,
        path: &str,
        content: Bytes,
    ) -> Result<(), Error> {
        let lock_key = format!("namespace_registry_shard_{shard_key}");
        let guard = self.lock.acquire(&[lock_key]).await?;
        backend.store.put_object(path, content).await?;
        guard.release().await;
        Ok(())
    }

    #[instrument(name = "touch_link_access_time_locked", skip(self, backend))]
    async fn touch_link_access_time(
        &self,
        backend: &Backend,
        namespace: &str,
        link: &LinkKind,
    ) -> Result<LinkMetadata, Error> {
        let guard = self.lock.acquire(&[format!("{namespace}:{link}")]).await?;
        let link_data = backend
            .read_link_reference(namespace, link)
            .await?
            .accessed();
        if !guard.is_valid() {
            return Err(Error::Lock(
                "lock invalidated during access time update".into(),
            ));
        }
        backend
            .write_link_reference(namespace, link, &link_data)
            .await?;
        backend.cache_put(namespace, link, &link_data).await;
        guard.release().await;
        Ok(link_data)
    }

    #[instrument(name = "flush_access_time_locked", skip(self, backend))]
    async fn flush_access_time(
        &self,
        backend: &Backend,
        namespace: &str,
        link: &LinkKind,
    ) -> Result<(), Error> {
        let guard = self.lock.acquire(&[format!("{namespace}:{link}")]).await?;
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
