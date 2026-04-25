use std::collections::HashMap;

use async_trait::async_trait;
use futures_util::future::join_all;

use crate::registry::metadata_store::{
    Error, LinkMetadata, LinkOperation, ResolvedCreate, ResolvedDelete, link_kind::LinkKind,
};

/// Shared result type for the lock-validation step.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ValidationResult {
    Valid,
    NeedsRetry,
}

/// Abstracts the two hook points that differ between FS and S3 backends:
///
/// - `read_link_reference`: how a link is fetched from storage.
/// - `lock_key_for_link`: how a link name is formatted as a distributed-lock key
///   (FS uses bare `link.to_string()`, S3 prefixes with `{namespace}:`).
///
/// The three shared pre-lock / under-lock helpers are provided as default methods
/// so each backend can delete its own copies and simply `impl LockOps`.
#[async_trait]
pub trait LockOps: Send + Sync {
    /// Read the stored [`LinkMetadata`] for `link` within `namespace`.
    async fn read_link_reference(
        &self,
        namespace: &str,
        link: &LinkKind,
    ) -> Result<LinkMetadata, Error>;

    /// Format a lock key for the given `link` within `namespace`.
    ///
    /// FS omits the namespace prefix; S3 includes it.
    fn lock_key_for_link(namespace: &str, link: &LinkKind) -> String
    where
        Self: Sized;

    /// Reads every operation before acquiring locks, resolving current link
    /// state from storage and building the sorted, deduplicated lock key list.
    ///
    /// Returns `(creates, deletes, lock_keys)`. When both `creates` and
    /// `deletes` are empty there is nothing to do and the caller should return
    /// early.
    ///
    /// Lock keys include both link names and `blob:{digest}` for every target
    /// digest. This ensures blob index updates (which perform read-modify-write
    /// on per-digest files) are serialized across concurrent `update_links` calls.
    async fn prelock_resolve_operations(
        &self,
        namespace: &str,
        operations: &[LinkOperation],
    ) -> (Vec<ResolvedCreate>, Vec<ResolvedDelete>, Vec<String>)
    where
        Self: Sized,
    {
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
                        Some(ResolvedCreate {
                            link: link.clone(),
                            target: target.clone(),
                            old_target,
                            referrer: referrer.clone(),
                            media_type: media_type.clone(),
                            descriptor: descriptor.as_deref().cloned(),
                        }),
                        None,
                    )
                }
                LinkOperation::Delete { link, referrer } => {
                    let metadata = self.read_link_reference(namespace, link).await.ok();
                    (None, Some((link.clone(), metadata, referrer.clone())))
                }
            }
        }))
        .await;

        let mut lock_keys: Vec<String> = Vec::new();
        let mut creates: Vec<ResolvedCreate> = Vec::new();
        let mut deletes: Vec<ResolvedDelete> = Vec::new();

        for (create_data, delete_data) in prelock_results {
            if let Some(op) = create_data {
                lock_keys.push(Self::lock_key_for_link(namespace, &op.link));
                lock_keys.push(format!("blob:{}", op.target));
                if let Some(ref old) = op.old_target {
                    lock_keys.push(format!("blob:{old}"));
                }
                creates.push(op);
            } else if let Some((link, Some(meta), referrer)) = delete_data {
                lock_keys.push(Self::lock_key_for_link(namespace, &link));
                lock_keys.push(format!("blob:{}", meta.target));
                deletes.push(ResolvedDelete {
                    link,
                    target: meta.target,
                    referrer,
                });
            }
        }

        lock_keys.sort();
        lock_keys.dedup();
        (creates, deletes, lock_keys)
    }

    /// Re-reads all create operations under the lock and checks whether any
    /// concurrent modification has changed the link state since the pre-lock
    /// read. Populates `link_cache` with the current metadata for each link.
    ///
    /// Returns [`ValidationResult::NeedsRetry`] when a discrepancy is detected,
    /// [`ValidationResult::Valid`] otherwise.
    async fn validate_creates_under_lock(
        &self,
        namespace: &str,
        creates: &[ResolvedCreate],
        link_cache: &mut HashMap<LinkKind, LinkMetadata>,
    ) -> ValidationResult
    where
        Self: Sized,
    {
        let validation_results = join_all(creates.iter().map(|op| async move {
            let current = self.read_link_reference(namespace, &op.link).await.ok();
            let current_target = current.as_ref().map(|m| m.target.clone());
            (
                op.link.clone(),
                current,
                current_target,
                op.old_target.clone(),
            )
        }))
        .await;

        if validation_results
            .iter()
            .any(|(_, _, current_target, expected)| *current_target != *expected)
        {
            return ValidationResult::NeedsRetry;
        }

        for (link, metadata, _, _) in validation_results {
            if let Some(m) = metadata {
                link_cache.insert(link, m);
            }
        }

        ValidationResult::Valid
    }

    /// Re-reads all delete operations under the lock and checks whether the
    /// target digest still matches what was observed before the lock was
    /// acquired. Populates `link_cache` with the current metadata for each
    /// confirmed delete.
    ///
    /// Returns `(valid_deletes, ValidationResult::NeedsRetry)` when a
    /// discrepancy is detected, `(valid_deletes, ValidationResult::Valid)`
    /// otherwise. Links that are already gone ([`Error::ReferenceNotFound`]) are
    /// silently dropped rather than triggering a retry.
    async fn validate_deletes_under_lock(
        &self,
        namespace: &str,
        deletes: Vec<ResolvedDelete>,
        link_cache: &mut HashMap<LinkKind, LinkMetadata>,
    ) -> Result<(Vec<ResolvedDelete>, ValidationResult), Error>
    where
        Self: Sized,
    {
        let delete_results = join_all(deletes.into_iter().map(|op| async move {
            let result = self.read_link_reference(namespace, &op.link).await;
            (op, result)
        }))
        .await;

        let mut valid_deletes: Vec<ResolvedDelete> = Vec::new();
        for (op, result) in delete_results {
            match result {
                Ok(metadata) if metadata.target == op.target => {
                    link_cache.insert(op.link.clone(), metadata);
                    valid_deletes.push(op);
                }
                Ok(_) => {
                    return Ok((valid_deletes, ValidationResult::NeedsRetry));
                }
                Err(Error::ReferenceNotFound) => {}
                Err(e) => return Err(e),
            }
        }

        Ok((valid_deletes, ValidationResult::Valid))
    }
}
