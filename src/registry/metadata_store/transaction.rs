//! Shared distributed-lock transaction engine for `update_links`.
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

use crate::registry::metadata_store::{
    Error, LinkOperation,
    lock::LockBackend,
    lock_ops::{LockOps, ValidationResult},
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
