use std::{collections::HashMap, future::Future, pin::Pin};

use async_trait::async_trait;
use futures_util::future::join_all;

use crate::{
    oci::Digest,
    registry::metadata_store::{
        BlobIndexOperation, Error, LinkMetadata, LinkOperation, ResolvedCreate, ResolvedDelete,
        link_kind::LinkKind,
    },
};

/// Shared result type for the lock-validation step.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ValidationResult {
    Valid,
    NeedsRetry,
}

/// Return type of [`build_create_ops`].
type CreateOpsResult = (
    HashMap<Digest, Vec<BlobIndexOperation>>,
    Vec<(LinkKind, LinkMetadata)>,
    Vec<(LinkKind, LinkMetadata)>,
);

/// Return type of [`build_delete_ops`].
type DeleteOpsResult = (
    HashMap<Digest, Vec<BlobIndexOperation>>,
    Vec<(LinkKind, LinkMetadata)>,
    Vec<LinkKind>,
    Vec<LinkKind>,
);

/// Pure data transform: resolves blob index operations and categorises link
/// writes for a set of create operations.
///
/// Returns `(pending_blob_ops, tracked_create_writes, non_tracked_create_writes)`.
pub fn build_create_ops(
    creates: &[ResolvedCreate],
    link_cache: &mut HashMap<LinkKind, LinkMetadata>,
) -> CreateOpsResult {
    let mut pending_blob_ops: HashMap<Digest, Vec<BlobIndexOperation>> = HashMap::new();
    let mut tracked_create_writes: Vec<(LinkKind, LinkMetadata)> = Vec::new();
    let mut non_tracked_create_writes: Vec<(LinkKind, LinkMetadata)> = Vec::new();

    for op in creates {
        if op.link.is_tracked() && op.referrer.is_some() {
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
            // Only update the blob index when the target actually changes: skip
            // the insert when re-pushing the same link to the same digest.
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

/// Pure data transform: resolves blob index operations and categorises link
/// writes/deletes for a set of delete operations.
///
/// Returns `(pending_blob_ops, tracked_delete_writes, tracked_delete_removes, non_tracked_delete_links)`.
pub fn build_delete_ops(
    deletes: &[ResolvedDelete],
    link_cache: &mut HashMap<LinkKind, LinkMetadata>,
    mut pending_blob_ops: HashMap<Digest, Vec<BlobIndexOperation>>,
) -> DeleteOpsResult {
    let mut tracked_delete_writes: Vec<(LinkKind, LinkMetadata)> = Vec::new();
    let mut tracked_delete_removes: Vec<LinkKind> = Vec::new();
    let mut non_tracked_delete_links: Vec<LinkKind> = Vec::new();

    for op in deletes {
        if op.link.is_tracked() && op.referrer.is_some() {
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

/// Abstracts the hook points that differ between FS and S3 backends:
///
/// - `read_link_reference`: how a link is fetched from storage.
/// - `write_link_reference`: how a link is persisted to storage.
/// - `delete_link_reference`: how a link is removed from storage.
/// - `lock_key_for_link`: how a link name is formatted as a distributed-lock
///   key (FS uses bare `link.to_string()`, S3 prefixes with `{namespace}:`).
/// - `cache_put` / `cache_invalidate`: cache integration (no-op default for FS).
/// - `apply_pending_blob_index_ops`: how accumulated blob-index operations are
///   flushed after `apply_link_operations` completes.
/// - `after_update`: optional hook called after a successful transaction; S3
///   uses this to register the namespace, FS does nothing.
///
/// The shared pre-lock / under-lock / apply helpers are provided as default
/// methods so each backend only needs to implement the storage primitives.
#[async_trait]
pub trait LockOps: Send + Sync {
    /// Read the stored [`LinkMetadata`] for `link` within `namespace`.
    async fn read_link_reference(
        &self,
        namespace: &str,
        link: &LinkKind,
    ) -> Result<LinkMetadata, Error>;

    /// Persist `metadata` for `link` within `namespace`.
    async fn write_link_reference(
        &self,
        namespace: &str,
        link: &LinkKind,
        metadata: &LinkMetadata,
    ) -> Result<(), Error>;

    /// Remove the stored link for `link` within `namespace`.
    async fn delete_link_reference(&self, namespace: &str, link: &LinkKind) -> Result<(), Error>;

    /// Format a lock key for the given `link` within `namespace`.
    ///
    /// FS omits the namespace prefix; S3 includes it.
    fn lock_key_for_link(namespace: &str, link: &LinkKind) -> String
    where
        Self: Sized;

    /// Apply the accumulated blob-index operations after `apply_link_operations`
    /// completes and before the distributed lock is released.
    ///
    /// FS performs per-operation sequential writes; S3 (lock coordinator)
    /// performs per-digest concurrent updates via `update_blob_index_shard`.
    async fn apply_pending_blob_index_ops(
        &self,
        namespace: &str,
        pending_blob_ops: HashMap<Digest, Vec<BlobIndexOperation>>,
    ) -> Result<(), Error>;

    /// Hook called after a successful transaction, outside the lock.
    ///
    /// S3 uses this to register the namespace in the namespace registry when
    /// creates were part of the transaction. FS does nothing. The default
    /// implementation is a no-op that always returns `Ok(())`.
    async fn after_update(&self, _namespace: &str, _had_creates: bool) -> Result<(), Error> {
        Ok(())
    }

    /// Store `metadata` in the link cache. No-op by default (FS has no cache).
    async fn cache_put(&self, _namespace: &str, _link: &LinkKind, _metadata: &LinkMetadata) {}

    /// Evict `link` from the link cache. No-op by default (FS has no cache).
    async fn cache_invalidate(&self, _namespace: &str, _link: &LinkKind) {}

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

    /// Executes the write+delete+cache-update sequence for a set of create and
    /// delete link operations. All writes and deletes are issued concurrently.
    ///
    /// Returns the accumulated pending blob index operations for the caller to apply.
    async fn apply_link_operations(
        &self,
        namespace: &str,
        creates: &[ResolvedCreate],
        deletes: &[ResolvedDelete],
        link_cache: &mut HashMap<LinkKind, LinkMetadata>,
    ) -> Result<HashMap<Digest, Vec<BlobIndexOperation>>, Error>
    where
        Self: Sized,
    {
        let (pending_blob_ops, tracked_create_writes, non_tracked_create_writes) =
            build_create_ops(creates, link_cache);

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
        ) = build_delete_ops(deletes, link_cache, pending_blob_ops);

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
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Mutex};

    use async_trait::async_trait;

    use super::*;
    use crate::{
        oci::Digest,
        registry::metadata_store::{
            BlobIndexOperation, LinkMetadata, LinkOperation, link_kind::LinkKind,
        },
    };

    fn digest(hex: &str) -> Digest {
        let padded = format!("{hex:0<64}");
        format!("sha256:{padded}").parse().unwrap()
    }

    struct MockBackend {
        store: Mutex<HashMap<(String, LinkKind), LinkMetadata>>,
    }

    impl MockBackend {
        fn new() -> Self {
            Self {
                store: Mutex::new(HashMap::new()),
            }
        }

        fn with_link(self, namespace: &str, link: LinkKind, metadata: LinkMetadata) -> Self {
            self.store
                .lock()
                .unwrap()
                .insert((namespace.to_string(), link), metadata);
            self
        }
    }

    #[async_trait]
    impl LockOps for MockBackend {
        async fn read_link_reference(
            &self,
            namespace: &str,
            link: &LinkKind,
        ) -> Result<LinkMetadata, Error> {
            self.store
                .lock()
                .unwrap()
                .get(&(namespace.to_string(), link.clone()))
                .cloned()
                .ok_or(Error::ReferenceNotFound)
        }

        async fn write_link_reference(
            &self,
            namespace: &str,
            link: &LinkKind,
            metadata: &LinkMetadata,
        ) -> Result<(), Error> {
            self.store
                .lock()
                .unwrap()
                .insert((namespace.to_string(), link.clone()), metadata.clone());
            Ok(())
        }

        async fn delete_link_reference(
            &self,
            namespace: &str,
            link: &LinkKind,
        ) -> Result<(), Error> {
            self.store
                .lock()
                .unwrap()
                .remove(&(namespace.to_string(), link.clone()));
            Ok(())
        }

        fn lock_key_for_link(namespace: &str, link: &LinkKind) -> String
        where
            Self: Sized,
        {
            format!("{namespace}:{link}")
        }

        async fn apply_pending_blob_index_ops(
            &self,
            _namespace: &str,
            _pending_blob_ops: HashMap<Digest, Vec<BlobIndexOperation>>,
        ) -> Result<(), Error> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn prelock_resolves_create_with_no_prior_link() {
        let backend = MockBackend::new();
        let target = digest("aa");
        let link = LinkKind::Tag("v1".to_string());

        let ops = vec![LinkOperation::Create {
            link: link.clone(),
            target: target.clone(),
            referrer: None,
            media_type: None,
            descriptor: None,
        }];

        let (creates, deletes, lock_keys) = backend.prelock_resolve_operations("ns", &ops).await;

        assert_eq!(creates.len(), 1);
        assert!(creates[0].old_target.is_none());
        assert_eq!(creates[0].target, target);
        assert!(deletes.is_empty());
        assert!(lock_keys.contains(&"ns:tag:v1".to_string()));
        assert!(lock_keys.contains(&format!("blob:{target}")));
    }

    #[tokio::test]
    async fn prelock_resolves_create_with_existing_link() {
        let prior_target = digest("bb");
        let new_target = digest("cc");
        let link = LinkKind::Tag("v1".to_string());
        let prior_metadata = LinkMetadata::from_digest(prior_target.clone());

        let backend = MockBackend::new().with_link("ns", link.clone(), prior_metadata);

        let ops = vec![LinkOperation::Create {
            link: link.clone(),
            target: new_target.clone(),
            referrer: None,
            media_type: None,
            descriptor: None,
        }];

        let (creates, deletes, lock_keys) = backend.prelock_resolve_operations("ns", &ops).await;

        assert_eq!(creates.len(), 1);
        assert_eq!(creates[0].old_target, Some(prior_target.clone()));
        assert_eq!(creates[0].target, new_target);
        assert!(deletes.is_empty());
        assert!(lock_keys.contains(&"ns:tag:v1".to_string()));
        assert!(lock_keys.contains(&format!("blob:{new_target}")));
        assert!(lock_keys.contains(&format!("blob:{prior_target}")));
    }

    #[tokio::test]
    async fn prelock_resolves_delete_for_existing_link() {
        let target = digest("dd");
        let link = LinkKind::Tag("v2".to_string());
        let metadata = LinkMetadata::from_digest(target.clone());

        let backend = MockBackend::new().with_link("ns", link.clone(), metadata);

        let ops = vec![LinkOperation::Delete {
            link: link.clone(),
            referrer: None,
        }];

        let (creates, deletes, lock_keys) = backend.prelock_resolve_operations("ns", &ops).await;

        assert!(creates.is_empty());
        assert_eq!(deletes.len(), 1);
        assert_eq!(deletes[0].target, target);
        assert!(lock_keys.contains(&"ns:tag:v2".to_string()));
        assert!(lock_keys.contains(&format!("blob:{target}")));
    }

    #[tokio::test]
    async fn prelock_drops_delete_for_missing_link() {
        let backend = MockBackend::new();
        let link = LinkKind::Tag("ghost".to_string());

        let ops = vec![LinkOperation::Delete {
            link,
            referrer: None,
        }];

        let (creates, deletes, lock_keys) = backend.prelock_resolve_operations("ns", &ops).await;

        assert!(creates.is_empty());
        assert!(deletes.is_empty());
        assert!(lock_keys.is_empty());
    }

    #[tokio::test]
    async fn prelock_handles_mixed_create_and_delete() {
        let create_target = digest("ee");
        let delete_target = digest("ff");
        let create_link = LinkKind::Tag("new".to_string());
        let delete_link = LinkKind::Tag("old".to_string());
        let delete_metadata = LinkMetadata::from_digest(delete_target.clone());

        let backend = MockBackend::new().with_link("ns", delete_link.clone(), delete_metadata);

        let ops = vec![
            LinkOperation::Create {
                link: create_link.clone(),
                target: create_target.clone(),
                referrer: None,
                media_type: None,
                descriptor: None,
            },
            LinkOperation::Delete {
                link: delete_link.clone(),
                referrer: None,
            },
        ];

        let (creates, deletes, lock_keys) = backend.prelock_resolve_operations("ns", &ops).await;

        assert_eq!(creates.len(), 1);
        assert_eq!(creates[0].target, create_target);
        assert!(creates[0].old_target.is_none());

        assert_eq!(deletes.len(), 1);
        assert_eq!(deletes[0].target, delete_target);

        assert!(lock_keys.contains(&"ns:tag:new".to_string()));
        assert!(lock_keys.contains(&"ns:tag:old".to_string()));
        assert!(lock_keys.contains(&format!("blob:{create_target}")));
        assert!(lock_keys.contains(&format!("blob:{delete_target}")));
    }

    #[tokio::test]
    async fn prelock_dedupes_lock_keys() {
        let shared_target = digest("1234");
        let link_a = LinkKind::Tag("a".to_string());
        let link_b = LinkKind::Tag("b".to_string());

        let backend = MockBackend::new();

        let ops = vec![
            LinkOperation::Create {
                link: link_a.clone(),
                target: shared_target.clone(),
                referrer: None,
                media_type: None,
                descriptor: None,
            },
            LinkOperation::Create {
                link: link_b.clone(),
                target: shared_target.clone(),
                referrer: None,
                media_type: None,
                descriptor: None,
            },
        ];

        let (creates, _deletes, lock_keys) = backend.prelock_resolve_operations("ns", &ops).await;

        assert_eq!(creates.len(), 2);

        let blob_key = format!("blob:{shared_target}");
        let blob_key_count = lock_keys.iter().filter(|k| *k == &blob_key).count();
        assert_eq!(
            blob_key_count, 1,
            "blob key must appear exactly once after dedup"
        );

        let is_sorted = lock_keys.windows(2).all(|w| w[0] <= w[1]);
        assert!(is_sorted, "lock_keys must be sorted");
    }

    #[tokio::test]
    async fn prelock_returns_empty_for_empty_input() {
        let backend = MockBackend::new();

        let (creates, deletes, lock_keys) = backend.prelock_resolve_operations("ns", &[]).await;

        assert!(creates.is_empty());
        assert!(deletes.is_empty());
        assert!(lock_keys.is_empty());
    }

    // -------------------------------------------------------------------------
    // Helpers for inspecting BlobIndexOperation without PartialEq
    // -------------------------------------------------------------------------

    fn is_insert(op: &BlobIndexOperation, expected_link: &LinkKind) -> bool {
        matches!(op, BlobIndexOperation::Insert(l) if l == expected_link)
    }

    fn is_remove(op: &BlobIndexOperation, expected_link: &LinkKind) -> bool {
        matches!(op, BlobIndexOperation::Remove(l) if l == expected_link)
    }

    // =========================================================================
    // build_create_ops tests
    // =========================================================================

    #[test]
    fn build_create_ops_empty_returns_all_empty() {
        let mut cache: HashMap<LinkKind, LinkMetadata> = HashMap::new();
        let (blob_ops, tracked, non_tracked) = build_create_ops(&[], &mut cache);

        assert!(blob_ops.is_empty());
        assert!(tracked.is_empty());
        assert!(non_tracked.is_empty());
        assert!(cache.is_empty(), "cache must remain untouched");
    }

    #[test]
    fn build_create_ops_tracked_link_no_prior_target() {
        // Layer is tracked; providing a referrer puts the op on the tracked path.
        // old_target = None → an Insert must be emitted for the new target.
        let link = LinkKind::Layer(digest("1100"));
        let target = digest("1101");
        let referrer = digest("1102");

        let creates = [ResolvedCreate {
            link: link.clone(),
            target: target.clone(),
            old_target: None,
            referrer: Some(referrer.clone()),
            media_type: None,
            descriptor: None,
        }];
        let mut cache: HashMap<LinkKind, LinkMetadata> = HashMap::new();

        let (blob_ops, tracked, non_tracked) = build_create_ops(&creates, &mut cache);

        assert_eq!(tracked.len(), 1, "one tracked write expected");
        assert!(non_tracked.is_empty());

        let (written_link, written_meta) = &tracked[0];
        assert_eq!(written_link, &link);
        assert!(
            written_meta.referenced_by.contains(&referrer),
            "referrer must appear in referenced_by"
        );

        let ops = blob_ops.get(&target).expect("blob ops for target expected");
        assert_eq!(ops.len(), 1);
        assert!(is_insert(&ops[0], &link));
    }

    #[test]
    fn build_create_ops_tracked_link_same_old_target() {
        // When old_target == target, no blob Insert should be emitted.
        let link = LinkKind::Layer(digest("1200"));
        let target = digest("1201");
        let referrer = digest("1202");

        let creates = [ResolvedCreate {
            link: link.clone(),
            target: target.clone(),
            old_target: Some(target.clone()),
            referrer: Some(referrer.clone()),
            media_type: None,
            descriptor: None,
        }];
        let mut cache: HashMap<LinkKind, LinkMetadata> = HashMap::new();

        let (blob_ops, tracked, non_tracked) = build_create_ops(&creates, &mut cache);

        assert_eq!(tracked.len(), 1);
        assert!(non_tracked.is_empty());
        assert!(
            blob_ops.is_empty(),
            "no blob ops when old_target equals new target on tracked path"
        );

        let (_, written_meta) = &tracked[0];
        assert!(written_meta.referenced_by.contains(&referrer));
    }

    #[test]
    fn build_create_ops_non_tracked_new_target() {
        // Tag is non-tracked; old_target = None → Insert emitted.
        let link = LinkKind::Tag("v3".to_string());
        let target = digest("1300");

        let creates = [ResolvedCreate {
            link: link.clone(),
            target: target.clone(),
            old_target: None,
            referrer: None,
            media_type: None,
            descriptor: None,
        }];
        let mut cache: HashMap<LinkKind, LinkMetadata> = HashMap::new();

        let (blob_ops, tracked, non_tracked) = build_create_ops(&creates, &mut cache);

        assert!(tracked.is_empty());
        assert_eq!(non_tracked.len(), 1);
        assert_eq!(non_tracked[0].0, link);

        let ops = blob_ops.get(&target).expect("blob ops for target expected");
        assert_eq!(ops.len(), 1);
        assert!(is_insert(&ops[0], &link));
    }

    #[test]
    fn build_create_ops_non_tracked_replaces_prior_target() {
        // old_target differs from new target → Insert for new + Remove for old.
        let link = LinkKind::Tag("v4".to_string());
        let old_target = digest("1400");
        let new_target = digest("1401");

        let creates = [ResolvedCreate {
            link: link.clone(),
            target: new_target.clone(),
            old_target: Some(old_target.clone()),
            referrer: None,
            media_type: None,
            descriptor: None,
        }];
        let mut cache: HashMap<LinkKind, LinkMetadata> = HashMap::new();

        let (blob_ops, tracked, non_tracked) = build_create_ops(&creates, &mut cache);

        assert!(tracked.is_empty());
        assert_eq!(non_tracked.len(), 1);

        let new_ops = blob_ops
            .get(&new_target)
            .expect("blob ops for new target expected");
        assert_eq!(new_ops.len(), 1);
        assert!(is_insert(&new_ops[0], &link));

        let old_ops = blob_ops
            .get(&old_target)
            .expect("blob ops for old target expected");
        assert_eq!(old_ops.len(), 1);
        assert!(is_remove(&old_ops[0], &link));
    }

    #[test]
    fn build_create_ops_non_tracked_same_old_target() {
        // Re-pushing a non-tracked link to the same digest: no blob ops, but
        // the link metadata is still written (idempotent metadata refresh).
        let link = LinkKind::Tag("v5".to_string());
        let target = digest("1500");

        let creates = [ResolvedCreate {
            link: link.clone(),
            target: target.clone(),
            old_target: Some(target.clone()),
            referrer: None,
            media_type: None,
            descriptor: None,
        }];
        let mut cache: HashMap<LinkKind, LinkMetadata> = HashMap::new();

        let (blob_ops, tracked, non_tracked) = build_create_ops(&creates, &mut cache);

        assert!(tracked.is_empty());
        assert_eq!(non_tracked.len(), 1, "link metadata is still written");
        assert!(
            blob_ops.is_empty(),
            "no blob ops when old_target equals new target on non-tracked path"
        );
    }

    #[test]
    fn build_create_ops_uses_cached_metadata_when_present() {
        // Pre-populate the cache for a tracked link. The function should consume
        // the cache entry and add the new referrer into it.
        let link = LinkKind::Layer(digest("1600"));
        let target = digest("1601");
        let existing_referrer = digest("1602");
        let new_referrer = digest("1603");

        let mut cached_meta = LinkMetadata::from_digest(target.clone());
        cached_meta.add_referrer(existing_referrer.clone());
        let mut cache: HashMap<LinkKind, LinkMetadata> = HashMap::new();
        cache.insert(link.clone(), cached_meta);

        let creates = [ResolvedCreate {
            link: link.clone(),
            target: target.clone(),
            old_target: Some(target.clone()),
            referrer: Some(new_referrer.clone()),
            media_type: None,
            descriptor: None,
        }];

        let (blob_ops, tracked, non_tracked) = build_create_ops(&creates, &mut cache);

        assert!(non_tracked.is_empty());
        assert_eq!(tracked.len(), 1);

        let (_, written_meta) = &tracked[0];
        assert!(
            written_meta.referenced_by.contains(&existing_referrer),
            "existing referrer must be preserved from cache"
        );
        assert!(
            written_meta.referenced_by.contains(&new_referrer),
            "new referrer must be added"
        );

        assert!(
            !cache.contains_key(&link),
            "cache entry must be consumed by build_create_ops"
        );

        assert!(blob_ops.is_empty());
    }

    // =========================================================================
    // build_delete_ops tests
    // =========================================================================

    #[test]
    fn build_delete_ops_empty_preserves_input_blob_ops() {
        // Empty deletes slice must leave pending_blob_ops unchanged.
        let target = digest("1700");
        let link = LinkKind::Tag("pre".to_string());
        let mut input_ops: HashMap<Digest, Vec<BlobIndexOperation>> = HashMap::new();
        input_ops
            .entry(target.clone())
            .or_default()
            .push(BlobIndexOperation::Insert(link.clone()));

        let mut cache: HashMap<LinkKind, LinkMetadata> = HashMap::new();

        let (blob_ops, tracked_writes, tracked_removes, non_tracked) =
            build_delete_ops(&[], &mut cache, input_ops);

        assert_eq!(blob_ops.len(), 1, "pre-existing blob op must be preserved");
        let ops = blob_ops.get(&target).unwrap();
        assert_eq!(ops.len(), 1);
        assert!(is_insert(&ops[0], &link));

        assert!(tracked_writes.is_empty());
        assert!(tracked_removes.is_empty());
        assert!(non_tracked.is_empty());
    }

    #[test]
    fn build_delete_ops_tracked_with_remaining_referrers_writes() {
        // Two referrers in cache; removing one should produce a write (not a remove).
        let link = LinkKind::Layer(digest("1800"));
        let target = digest("1801");
        let referrer_a = digest("1802");
        let referrer_b = digest("1803");

        let mut meta = LinkMetadata::from_digest(target.clone());
        meta.add_referrer(referrer_a.clone());
        meta.add_referrer(referrer_b.clone());

        let mut cache: HashMap<LinkKind, LinkMetadata> = HashMap::new();
        cache.insert(link.clone(), meta);

        let deletes = [ResolvedDelete {
            link: link.clone(),
            target: target.clone(),
            referrer: Some(referrer_a.clone()),
        }];

        let (blob_ops, tracked_writes, tracked_removes, non_tracked) =
            build_delete_ops(&deletes, &mut cache, HashMap::new());

        assert_eq!(tracked_writes.len(), 1, "must write updated metadata");
        assert!(tracked_removes.is_empty(), "link must not be removed yet");
        assert!(non_tracked.is_empty());
        assert!(blob_ops.is_empty(), "no blob Remove when referrer remains");

        let (written_link, written_meta) = &tracked_writes[0];
        assert_eq!(written_link, &link);
        assert!(
            !written_meta.referenced_by.contains(&referrer_a),
            "deleted referrer must be absent"
        );
        assert!(
            written_meta.referenced_by.contains(&referrer_b),
            "surviving referrer must remain"
        );
    }

    #[test]
    fn build_delete_ops_tracked_last_referrer_removes() {
        // One referrer in cache; removing it empties referenced_by → link removed.
        let link = LinkKind::Layer(digest("1900"));
        let target = digest("1901");
        let referrer = digest("1902");

        let mut meta = LinkMetadata::from_digest(target.clone());
        meta.add_referrer(referrer.clone());

        let mut cache: HashMap<LinkKind, LinkMetadata> = HashMap::new();
        cache.insert(link.clone(), meta);

        let deletes = [ResolvedDelete {
            link: link.clone(),
            target: target.clone(),
            referrer: Some(referrer.clone()),
        }];

        let (blob_ops, tracked_writes, tracked_removes, non_tracked) =
            build_delete_ops(&deletes, &mut cache, HashMap::new());

        assert!(
            tracked_writes.is_empty(),
            "no partial write when last referrer removed"
        );
        assert_eq!(tracked_removes.len(), 1);
        assert_eq!(tracked_removes[0], link);
        assert!(non_tracked.is_empty());

        let ops = blob_ops.get(&target).expect("blob Remove expected");
        assert_eq!(ops.len(), 1);
        assert!(is_remove(&ops[0], &link));
    }

    #[test]
    fn build_delete_ops_tracked_no_metadata_in_cache_skipped() {
        // Delete for a tracked link not present in the cache is silently dropped.
        let link = LinkKind::Layer(digest("2000"));
        let target = digest("2001");

        let mut cache: HashMap<LinkKind, LinkMetadata> = HashMap::new();

        let deletes = [ResolvedDelete {
            link: link.clone(),
            target: target.clone(),
            referrer: Some(digest("2002")),
        }];

        let (blob_ops, tracked_writes, tracked_removes, non_tracked) =
            build_delete_ops(&deletes, &mut cache, HashMap::new());

        assert!(blob_ops.is_empty());
        assert!(tracked_writes.is_empty());
        assert!(tracked_removes.is_empty());
        assert!(non_tracked.is_empty());
    }

    #[test]
    fn build_delete_ops_non_tracked_emits_remove() {
        // Non-tracked link (Tag with no referrer) must land in non_tracked_delete_links
        // and emit a blob Remove.
        let link = LinkKind::Tag("v11".to_string());
        let target = digest("2100");

        let mut cache: HashMap<LinkKind, LinkMetadata> = HashMap::new();

        let deletes = [ResolvedDelete {
            link: link.clone(),
            target: target.clone(),
            referrer: None,
        }];

        let (blob_ops, tracked_writes, tracked_removes, non_tracked) =
            build_delete_ops(&deletes, &mut cache, HashMap::new());

        assert!(tracked_writes.is_empty());
        assert!(tracked_removes.is_empty());
        assert_eq!(non_tracked.len(), 1);
        assert_eq!(non_tracked[0], link);

        let ops = blob_ops.get(&target).expect("blob Remove expected");
        assert_eq!(ops.len(), 1);
        assert!(is_remove(&ops[0], &link));
    }

    #[test]
    fn build_delete_ops_combines_with_input_blob_ops() {
        // blob ops from a prior build_create_ops call must be extended, not replaced.
        let pre_link = LinkKind::Tag("pre12".to_string());
        let pre_target = digest("2200");
        let del_link = LinkKind::Tag("del12".to_string());
        let del_target = digest("2201");

        let mut prior_ops: HashMap<Digest, Vec<BlobIndexOperation>> = HashMap::new();
        prior_ops
            .entry(pre_target.clone())
            .or_default()
            .push(BlobIndexOperation::Insert(pre_link.clone()));

        let mut cache: HashMap<LinkKind, LinkMetadata> = HashMap::new();

        let deletes = [ResolvedDelete {
            link: del_link.clone(),
            target: del_target.clone(),
            referrer: None,
        }];

        let (blob_ops, _, _, _) = build_delete_ops(&deletes, &mut cache, prior_ops);

        let pre_ops = blob_ops
            .get(&pre_target)
            .expect("pre-existing blob op must survive");
        assert_eq!(pre_ops.len(), 1);
        assert!(is_insert(&pre_ops[0], &pre_link));

        let del_ops = blob_ops
            .get(&del_target)
            .expect("new blob Remove must be added");
        assert_eq!(del_ops.len(), 1);
        assert!(is_remove(&del_ops[0], &del_link));
    }
}
