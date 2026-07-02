//! The tracked-grant lock protocol: `update_links` refuses tracked inserts,
//! `store_manifest` reports `needs_locks` outside its granted set, and the
//! multi-key blob-data acquisition is one all-or-nothing session.

use std::{collections::HashSet, sync::Arc};

use chrono::{Duration, Utc};

use angos_storage::{ObjectStore, fs::Backend as StorageFsBackend};

use crate::{
    oci::{Digest, Namespace, Tag},
    registry::{
        metadata_store::{Error, LinkKind, LinkOperation, blob_data_lock_key},
        test_utils::{self, backends, put_blob_direct},
    },
};

/// A tracked create whose link is absent inserts a grant, which `update_links`
/// must refuse outright, naming the digest and committing nothing.
#[tokio::test]
async fn update_links_refuses_tracked_insert_without_lock() {
    for test_case in backends() {
        let m = test_case.metadata_store();
        let namespace = &Namespace::new("granted/refuse").unwrap();
        let layer = put_blob_direct(m.store(), b"refused layer").await;
        let manifest = put_blob_direct(m.store(), b"refusing manifest").await;

        let result = m
            .update_links(
                namespace,
                &[LinkOperation::create_with_referrer(
                    LinkKind::Layer(layer.clone()),
                    layer.clone(),
                    manifest.clone(),
                )],
            )
            .await;
        assert_eq!(
            result,
            Err(Error::TrackedInsertWithoutLock(vec![layer.clone()])),
            "an unlocked tracked insert must be refused with the digest named"
        );
        assert!(
            m.read_link(namespace, &LinkKind::Layer(layer.clone()))
                .await
                .is_err(),
            "the refused transaction must commit no link"
        );
        assert!(
            m.read_blob_index(&layer).await.is_err(),
            "the refused transaction must commit no grant"
        );
        test_case.cleanup().await;
    }
}

/// A merge into an existing tracked link inserts no grant, so `update_links`
/// commits it without any blob-data lock.
#[tokio::test]
async fn update_links_allows_tracked_merge_without_lock() {
    for test_case in backends() {
        let m = test_case.metadata_store();
        let namespace = &Namespace::new("granted/merge").unwrap();
        let layer = put_blob_direct(m.store(), b"merged layer").await;
        let first = put_blob_direct(m.store(), b"first manifest").await;
        let second = put_blob_direct(m.store(), b"second manifest").await;

        m.seed_links(
            namespace,
            &[LinkOperation::create_with_referrer(
                LinkKind::Layer(layer.clone()),
                layer.clone(),
                first.clone(),
            )],
        )
        .await
        .unwrap();

        m.update_links(
            namespace,
            &[LinkOperation::create_with_referrer(
                LinkKind::Layer(layer.clone()),
                layer.clone(),
                second.clone(),
            )],
        )
        .await
        .unwrap();

        let metadata = m
            .read_link(namespace, &LinkKind::Layer(layer.clone()))
            .await
            .unwrap();
        assert!(metadata.referenced_by.contains(&first));
        assert!(metadata.referenced_by.contains(&second));
        test_case.cleanup().await;
    }
}

/// A `store_manifest` whose tracked insert targets a digest outside its granted
/// set commits nothing (not even the ungated self-link) and reports the digest
/// through `needs_locks`; the granted retry commits the full set.
#[tokio::test]
async fn store_manifest_reports_needs_locks_for_ungranted_insert() {
    for test_case in backends() {
        let m = test_case.metadata_store();
        let namespace = &Namespace::new("granted/needs-locks").unwrap();
        let layer = put_blob_direct(m.store(), b"needs-locks layer").await;
        let manifest = put_blob_direct(m.store(), b"needs-locks manifest").await;

        let ops = [
            LinkOperation::create(LinkKind::Digest(manifest.clone()), manifest.clone()),
            LinkOperation::create_with_referrer(
                LinkKind::Layer(layer.clone()),
                layer.clone(),
                manifest.clone(),
            ),
        ];

        let commit = m
            .store_manifest(namespace, &ops, None, &HashSet::new())
            .await
            .unwrap();
        assert_eq!(
            commit.needs_locks,
            vec![layer.clone()],
            "the ungranted insert digest must be reported"
        );
        assert!(
            m.read_link(namespace, &LinkKind::Digest(manifest.clone()))
                .await
                .is_err(),
            "a needs_locks response must have committed nothing"
        );
        assert!(
            m.read_link(namespace, &LinkKind::Layer(layer.clone()))
                .await
                .is_err(),
            "a needs_locks response must have committed nothing"
        );

        let granted: HashSet<Digest> = [layer.clone()].into_iter().collect();
        let commit = m
            .store_manifest(namespace, &ops, None, &granted)
            .await
            .unwrap();
        assert!(commit.needs_locks.is_empty());
        let link = m
            .read_link(namespace, &LinkKind::Layer(layer.clone()))
            .await
            .unwrap();
        assert!(link.referenced_by.contains(&manifest));
        let grant = m
            .read_blob_index_namespace(namespace, &layer)
            .await
            .unwrap();
        assert!(grant.contains(&LinkKind::Layer(layer.clone())));
        test_case.cleanup().await;
    }
}

/// A same-target non-tracked re-create carries both `created_at` and
/// `accessed_at` forward; a real binding change resets the pull history.
#[tokio::test]
async fn same_target_create_preserves_accessed_at() {
    for test_case in backends() {
        let m = test_case.metadata_store();
        let namespace = &Namespace::new("granted/accessed-at").unwrap();
        let digest_a = put_blob_direct(m.store(), b"accessed content a").await;
        let digest_b = put_blob_direct(m.store(), b"accessed content b").await;
        let tag = LinkKind::Tag(Tag::new("keep").unwrap());

        m.update_links(
            namespace,
            &[LinkOperation::create(tag.clone(), digest_a.clone())],
        )
        .await
        .unwrap();

        // Simulate a pull stamp on the stored link.
        let mut meta = m.read_link_reference(namespace, &tag).await.unwrap();
        let pulled_at = Utc::now() - Duration::hours(3);
        meta.accessed_at = Some(pulled_at);
        m.write_link_reference(namespace, &tag, &meta)
            .await
            .unwrap();
        let created_at = meta.created_at;

        m.update_links(
            namespace,
            &[LinkOperation::create(tag.clone(), digest_a.clone())],
        )
        .await
        .unwrap();
        let after = m.read_link_reference(namespace, &tag).await.unwrap();
        assert_eq!(
            after.accessed_at,
            Some(pulled_at),
            "a same-target re-create must keep the pull history"
        );
        assert_eq!(after.created_at, created_at);

        m.update_links(
            namespace,
            &[LinkOperation::create(tag.clone(), digest_b.clone())],
        )
        .await
        .unwrap();
        let moved = m.read_link_reference(namespace, &tag).await.unwrap();
        assert_eq!(
            moved.accessed_at, None,
            "a binding change is new content and resets the pull history"
        );
        test_case.cleanup().await;
    }
}

/// One `acquire_blob_data_locks` call holds every digest's key (duplicates
/// deduplicated) and releases them all together.
#[tokio::test]
async fn acquire_blob_data_locks_holds_and_releases_every_key() {
    let dir = tempfile::TempDir::new().unwrap();
    let root = dir.path().to_str().unwrap();
    let object: Arc<dyn ObjectStore> = Arc::new(StorageFsBackend::builder(root).build());
    let executor = test_utils::build_test_fs_executor(root, false);
    let m = test_utils::metadata_store_over(object, executor);

    let d1 = Digest::sha256_of_bytes(b"lock a");
    let d2 = Digest::sha256_of_bytes(b"lock b");

    let session = m
        .acquire_blob_data_locks(&[d2.clone(), d1.clone(), d1.clone()])
        .await
        .unwrap();
    for digest in [&d1, &d2] {
        let contended = m
            .executor()
            .try_acquire(&[blob_data_lock_key(digest)])
            .await
            .unwrap();
        assert!(
            contended.is_none(),
            "'{digest}' must be held by the multi-key session"
        );
    }

    session.release().await;
    for digest in [&d1, &d2] {
        let free = m
            .executor()
            .try_acquire(&[blob_data_lock_key(digest)])
            .await
            .unwrap()
            .expect("released key must be acquirable");
        free.release().await;
    }
}
