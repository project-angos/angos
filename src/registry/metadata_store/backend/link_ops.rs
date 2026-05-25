use std::collections::HashMap;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::future::join_all;
use tracing::warn;

use angos_storage::Error as StorageError;

use crate::{
    oci::Digest,
    registry::{
        metadata_store::{
            BlobIndexOperation, Error, LinkMetadata, LinkOperation, ResolvedCreate, ResolvedDelete,
            backend::Backend,
            link_kind::LinkKind,
            lock_ops::{LockOps, link_lock_key},
        },
        path_builder,
    },
};

// ───────────────────────────────────────────────────────────────────────────
// LockOps: the storage-primitive bridge used by `update_links`
// ───────────────────────────────────────────────────────────────────────────

#[async_trait]
impl LockOps for Backend {
    async fn read_link_reference(
        &self,
        namespace: &str,
        link: &LinkKind,
    ) -> Result<LinkMetadata, Error> {
        let link_path = path_builder::link_path(link, namespace);
        match self.store().get(&link_path).await {
            Ok(data) => LinkMetadata::from_bytes(data),
            Err(StorageError::NotFound) => Err(Error::ReferenceNotFound),
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
        let serialized = Bytes::from(serde_json::to_vec(metadata)?);
        self.store()
            .put(&link_path, serialized)
            .await
            .map_err(Error::from)
    }

    async fn delete_link_reference(&self, namespace: &str, link: &LinkKind) -> Result<(), Error> {
        let container = path_builder::link_container_path(link, namespace);

        // Delete the container prefix (FS: recursive dir removal; S3: removes the single
        // object). For `Tag` links the container is `tags/{name}/current`, and the parent
        // `tags/{name}` is unique to this link, so remove it too so that FS
        // `list_children("tags/")` does not continue to report the deleted tag name as a
        // sub_prefix. All other link kinds share their parent directory with sibling links
        // so the parent must not be touched.
        self.store()
            .delete_prefix(&container)
            .await
            .map_err(Error::from)?;

        if matches!(link, LinkKind::Tag(_))
            && let Some((parent, _)) = container.rsplit_once('/')
        {
            let _ = self.store().delete_prefix(parent).await;
        }

        Ok(())
    }

    async fn apply_pending_blob_index_ops(
        &self,
        namespace: &str,
        pending_blob_ops: HashMap<Digest, Vec<BlobIndexOperation>>,
    ) -> Result<(), Error> {
        if self.coordinator.is_cas() {
            join_all(
                pending_blob_ops
                    .iter()
                    .map(|(digest, ops)| self.update_blob_index_cas(namespace, digest, ops)),
            )
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        } else {
            join_all(
                pending_blob_ops
                    .iter()
                    .map(|(digest, ops)| self.update_blob_index_locked(namespace, digest, ops)),
            )
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        }
        Ok(())
    }

    async fn after_update(&self, namespace: &str, had_creates: bool) -> Result<(), Error> {
        if had_creates && let Err(e) = self.register_namespace(namespace).await {
            warn!(namespace, error = %e, "Failed to register namespace");
        }
        Ok(())
    }

    async fn cache_put(&self, namespace: &str, link: &LinkKind, metadata: &LinkMetadata) {
        Backend::cache_put(self, namespace, link, metadata).await;
    }

    async fn cache_invalidate(&self, namespace: &str, link: &LinkKind) {
        Backend::cache_invalidate(self, namespace, link).await;
    }
}

// ───────────────────────────────────────────────────────────────────────────
// Link-operation helpers on Backend
// ───────────────────────────────────────────────────────────────────────────

impl Backend {
    pub async fn write_link_if_not_exists(
        &self,
        namespace: &str,
        link: &LinkKind,
        metadata: &LinkMetadata,
    ) -> Result<bool, Error> {
        let Some(cs) = self.coordinator.conditional_store() else {
            return Ok(false);
        };
        let link_path = path_builder::link_path(link, namespace);
        let serialized = Bytes::from(serde_json::to_vec(metadata)?);
        match cs.put_if_absent(&link_path, serialized).await {
            Ok(_) => Ok(true),
            Err(StorageError::PreconditionFailed) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// CAS-first link update path.
    ///
    /// Creates are attempted optimistically with `put_if_absent`; conflicts and
    /// all deletes fall through to the lock-guarded path. Blob-index updates
    /// run as CAS after the lock is released.
    pub async fn update_links_cas(
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

        let locked_blob_ops = self
            .execute_locked_cas_updates(namespace, &locked_ops)
            .await?;

        let mut pending_blob_ops = lockfree_blob_ops;
        for (digest, ops) in locked_blob_ops {
            pending_blob_ops.entry(digest).or_default().extend(ops);
        }

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

    pub async fn execute_locked_cas_updates(
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
                link_lock_key(namespace, link)
            })
            .collect();
        lock_keys.sort();
        lock_keys.dedup();

        let guard = self.lock().acquire(&lock_keys).await?;

        let read_results = join_all(operations.iter().map(|op| async move {
            let link = match op {
                LinkOperation::Create { link, .. } | LinkOperation::Delete { link, .. } => link,
            };
            let metadata = self.read_link_reference(namespace, link).await.ok();
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

        let pending_blob_ops = self
            .apply_link_operations(namespace, &creates, &deletes, &mut link_cache)
            .await?;

        guard.release().await;

        Ok(pending_blob_ops)
    }
}
