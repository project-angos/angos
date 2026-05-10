//! Transaction builder and lock-coordinated executor for `update_links`.
//!
//! The public face of this module is [`Transaction`] (obtained via
//! [`MetadataStoreExt::begin_transaction`]) and its associated builder
//! [`CreateLinkBuilder`]. The [`run_link_transaction`] function is the shared
//! distributed-lock engine used by the filesystem and S3 lock-coordinator
//! backends.
//!
//! Both the filesystem and S3 (lock-coordinator) backends delegate their
//! `update_links` implementation to [`run_link_transaction`]. The differences
//! between the two paths — how blob-index operations are applied, and whether
//! a namespace registration hook is needed — are expressed through the two
//! extension methods added to [`LockOps`]:
//! [`apply_pending_blob_index_ops`](LockOps::apply_pending_blob_index_ops) and
//! [`after_update`](LockOps::after_update).
//!
//! `CasCoordinator::update_links` (the S3 optimistic-CAS path) is a
//! fundamentally different algorithm and is **not** handled here.

use std::collections::HashMap;

use crate::{
    oci::{Descriptor, Digest},
    registry::metadata_store::{
        Error, MetadataStore,
        link_kind::LinkKind,
        lock::LockBackend,
        lock_ops::{LockOps, ValidationResult},
    },
};

/// Maximum number of retry attempts when concurrent modifications invalidate
/// the pre-lock state during `update_links`. Shared by all backends.
pub const MAX_UPDATE_RETRIES: u32 = 10;

/// Runs the distributed-lock-protected `update_links` algorithm.
///
/// Executes pre-lock resolution, acquires the lock, validates under the lock,
/// applies link and blob-index operations, releases the lock, and invokes the
/// optional post-update hook. Retries up to [`MAX_UPDATE_RETRIES`] times when
/// concurrent modifications invalidate the pre-lock state.
///
/// The differences between the filesystem and S3 lock-coordinator paths flow
/// through the two extension points on `L`:
/// - [`LockOps::apply_pending_blob_index_ops`] — FS performs per-operation
///   sequential writes; S3 performs per-digest concurrent updates.
/// - [`LockOps::after_update`] — S3 registers the namespace; FS is a no-op.
///
/// # Errors
///
/// Returns `Error::Lock` when the retry count is exhausted or when the lock is
/// invalidated by a heartbeat failure. Propagates any storage error from the
/// underlying reads, writes, or blob-index updates.
pub async fn run_link_transaction<L: LockOps>(
    backend: &L,
    lock: &(dyn LockBackend + Send + Sync),
    namespace: &str,
    operations: &[LinkOperation],
) -> Result<(), Error> {
    if operations.is_empty() {
        return Ok(());
    }

    let mut update_retries = MAX_UPDATE_RETRIES;
    loop {
        let mut link_cache = HashMap::new();

        let (creates, deletes, lock_keys) = backend
            .prelock_resolve_operations(namespace, operations)
            .await;

        if creates.is_empty() && deletes.is_empty() {
            return Ok(());
        }

        let guard = lock.acquire(&lock_keys).await?;

        if backend
            .validate_creates_under_lock(namespace, &creates, &mut link_cache)
            .await
            == ValidationResult::NeedsRetry
        {
            guard.release().await;
            if update_retries == 0 {
                return Err(retry_exceeded_error());
            }
            update_retries -= 1;
            continue;
        }

        if !guard.is_valid() {
            guard.release().await;
            return Err(lock_invalidated_error());
        }

        let (valid_deletes, delete_status) = backend
            .validate_deletes_under_lock(namespace, deletes, &mut link_cache)
            .await?;

        if delete_status == ValidationResult::NeedsRetry {
            guard.release().await;
            if update_retries == 0 {
                return Err(retry_exceeded_error());
            }
            update_retries -= 1;
            continue;
        }

        if !guard.is_valid() {
            guard.release().await;
            return Err(lock_invalidated_error());
        }

        let pending_blob_ops = backend
            .apply_link_operations(namespace, &creates, &valid_deletes, &mut link_cache)
            .await?;

        if !guard.is_valid() {
            guard.release().await;
            return Err(lock_invalidated_error());
        }

        backend
            .apply_pending_blob_index_ops(namespace, pending_blob_ops)
            .await?;

        if !guard.is_valid() {
            guard.release().await;
            return Err(lock_invalidated_error());
        }

        guard.release().await;

        backend.after_update(namespace, !creates.is_empty()).await?;

        return Ok(());
    }
}

fn retry_exceeded_error() -> Error {
    Error::Lock("update_links exceeded maximum retries due to concurrent modifications".into())
}

fn lock_invalidated_error() -> Error {
    Error::Lock("lock invalidated during operation".into())
}

pub struct ResolvedCreate {
    pub link: LinkKind,
    pub target: Digest,
    pub old_target: Option<Digest>,
    pub referrer: Option<Digest>,
    pub media_type: Option<String>,
    pub descriptor: Option<Descriptor>,
}

pub struct ResolvedDelete {
    pub link: LinkKind,
    pub target: Digest,
    pub referrer: Option<Digest>,
}

#[derive(Debug, Clone)]
pub enum LinkOperation {
    Create {
        link: LinkKind,
        target: Digest,
        referrer: Option<Digest>,
        media_type: Option<String>,
        descriptor: Option<Box<Descriptor>>,
    },
    Delete {
        link: LinkKind,
        referrer: Option<Digest>,
    },
}

pub struct Transaction<'a> {
    store: &'a (dyn MetadataStore + Send + Sync),
    namespace: String,
    operations: Vec<LinkOperation>,
}

/// Builder for a single `Create` link operation within a [`Transaction`].
///
/// Obtain one via [`Transaction::create_link`] and finalize it by calling [`add`](Self::add).
pub struct CreateLinkBuilder<'tx, 'store: 'tx> {
    tx: &'tx mut Transaction<'store>,
    link: LinkKind,
    target: Digest,
    referrer: Option<Digest>,
    media_type: Option<String>,
    descriptor: Option<Descriptor>,
}

impl<'tx, 'store: 'tx> CreateLinkBuilder<'tx, 'store> {
    /// Associates a referrer digest with this link.
    pub fn with_referrer(mut self, referrer: &Digest) -> Self {
        self.referrer = Some(referrer.clone());
        self
    }

    /// Associates a media type with this link.
    pub fn with_media_type(mut self, media_type: &str) -> Self {
        self.media_type = Some(media_type.to_string());
        self
    }

    /// Associates a media type with this link when `media_type` is `Some`.
    pub fn with_optional_media_type(self, media_type: Option<&str>) -> Self {
        match media_type {
            Some(mt) => self.with_media_type(mt),
            None => self,
        }
    }

    /// Associates an OCI descriptor with this link.
    pub fn with_descriptor(mut self, descriptor: Descriptor) -> Self {
        self.descriptor = Some(descriptor);
        self
    }

    /// Appends the operation to the enclosing transaction.
    pub fn add(self) {
        self.tx.operations.push(LinkOperation::Create {
            link: self.link,
            target: self.target,
            referrer: self.referrer,
            media_type: self.media_type,
            descriptor: self.descriptor.map(Box::new),
        });
    }
}

impl<'store> Transaction<'store> {
    pub fn new(store: &'store (dyn MetadataStore + Send + Sync), namespace: String) -> Self {
        Transaction {
            store,
            namespace,
            operations: Vec::new(),
        }
    }

    pub fn create_link<'tx>(
        &'tx mut self,
        link: &LinkKind,
        target: &Digest,
    ) -> CreateLinkBuilder<'tx, 'store> {
        CreateLinkBuilder {
            tx: self,
            link: link.clone(),
            target: target.clone(),
            referrer: None,
            media_type: None,
            descriptor: None,
        }
    }

    pub fn delete_link(&mut self, link: &LinkKind) {
        self.operations.push(LinkOperation::Delete {
            link: link.clone(),
            referrer: None,
        });
    }

    pub fn delete_link_with_referrer(&mut self, link: &LinkKind, referrer: &Digest) {
        self.operations.push(LinkOperation::Delete {
            link: link.clone(),
            referrer: Some(referrer.clone()),
        });
    }

    pub async fn commit(self) -> Result<(), Error> {
        if self.operations.is_empty() {
            return Ok(());
        }
        self.store
            .update_links(&self.namespace, &self.operations)
            .await
    }
}
