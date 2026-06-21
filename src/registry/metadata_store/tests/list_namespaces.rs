use bytes::Bytes;

use crate::{
    oci::{Namespace, Tag},
    registry::{
        metadata_store::{LinkKind, LinkOperation},
        path_builder,
        test_utils::{self, backends, put_blob_direct},
    },
};

/// The catalog is derived directly from stored content: a namespace appears in
/// `list_namespaces` exactly when it holds at least one revision or tag (a
/// `_manifests` child), and disappears once all of them are deleted, with no
/// scrub or rebuild step in between.
#[tokio::test]
async fn list_namespaces_is_derived_from_content() {
    for test_case in backends() {
        let registry = test_case.registry();
        let metadata_store = test_case.metadata_store();
        let namespace = &Namespace::new("derived-catalog/repo").unwrap();

        let (digest, _) = test_utils::create_test_blob(registry, namespace, b"content").await;

        let (listed, _) = metadata_store.list_namespaces(1000, None).await.unwrap();
        assert!(
            listed.contains(&namespace.to_string()),
            "a namespace with content must appear in the catalog; got: {listed:?}"
        );

        // Remove every revision and tag, the only `_manifests` content this
        // namespace holds, with no scrub or rebuild call afterwards.
        metadata_store
            .update_links(
                namespace,
                &[
                    LinkOperation::delete(LinkKind::Tag(Tag::new("latest").unwrap())),
                    LinkOperation::delete(LinkKind::Layer(digest.clone())),
                ],
            )
            .await
            .unwrap();

        let (listed, _) = metadata_store.list_namespaces(1000, None).await.unwrap();
        assert!(
            !listed.contains(&namespace.to_string()),
            "a namespace whose revisions and tags were all deleted must \
             disappear from the catalog; got: {listed:?}"
        );

        test_case.cleanup().await;
    }
}

/// A namespace that holds only an in-progress upload (an `_uploads` child but no
/// `_manifests`) is not a catalog entry.
#[tokio::test]
async fn list_namespaces_excludes_upload_only_namespace() {
    for test_case in backends() {
        let metadata_store = test_case.metadata_store();
        let namespace = Namespace::new("upload-only/repo").unwrap();
        let uuid = uuid::Uuid::new_v4().to_string();

        let upload_data_path = path_builder::upload_path(&namespace, &uuid);
        metadata_store
            .store()
            .put(&upload_data_path, Bytes::from_static(b"partial"))
            .await
            .unwrap();

        let (listed, _) = metadata_store.list_namespaces(1000, None).await.unwrap();
        assert!(
            !listed.contains(&namespace.to_string()),
            "a namespace with only an _uploads artifact must not appear in the \
             catalog; got: {listed:?}"
        );

        test_case.cleanup().await;
    }
}

/// `list_upload_namespaces` keys off the `_uploads` child, the mirror of
/// `list_namespaces` (which keys off `_manifests`), so an upload-only namespace
/// surfaces here but not in the catalog, and a manifest-only one the converse.
#[tokio::test]
async fn list_upload_namespaces_keys_off_uploads_not_manifests() {
    for test_case in backends() {
        let registry = test_case.registry();
        let metadata_store = test_case.metadata_store();

        let manifest_only = &Namespace::new("upload-marker/manifest-only").unwrap();
        let upload_only = &Namespace::new("upload-marker/upload-only").unwrap();
        let mixed = &Namespace::new("upload-marker/mixed").unwrap();

        // Manifest-only: a `_manifests` child and no upload.
        test_utils::create_test_blob(registry, manifest_only, b"manifest-only").await;

        // Upload-only: an `_uploads` artifact and no manifest content.
        let upload_only_path =
            path_builder::upload_path(upload_only, &uuid::Uuid::new_v4().to_string());
        metadata_store
            .store()
            .put(&upload_only_path, Bytes::from_static(b"partial"))
            .await
            .unwrap();

        // Mixed: both a `_manifests` child and an `_uploads` artifact.
        test_utils::create_test_blob(registry, mixed, b"mixed").await;
        let mixed_upload_path = path_builder::upload_path(mixed, &uuid::Uuid::new_v4().to_string());
        metadata_store
            .store()
            .put(&mixed_upload_path, Bytes::from_static(b"partial"))
            .await
            .unwrap();

        let (upload_listed, _) = metadata_store
            .list_upload_namespaces(1000, None)
            .await
            .unwrap();
        assert!(
            upload_listed.contains(&upload_only.to_string()),
            "an upload-only namespace must appear in list_upload_namespaces; got: {upload_listed:?}"
        );
        assert!(
            upload_listed.contains(&mixed.to_string()),
            "a namespace with an upload must appear in list_upload_namespaces; got: {upload_listed:?}"
        );
        assert!(
            !upload_listed.contains(&manifest_only.to_string()),
            "a manifest-only namespace must not appear in list_upload_namespaces; got: {upload_listed:?}"
        );

        let (manifest_listed, _) = metadata_store.list_namespaces(1000, None).await.unwrap();
        assert!(
            manifest_listed.contains(&manifest_only.to_string()),
            "a manifest-only namespace must appear in the catalog; got: {manifest_listed:?}"
        );
        assert!(
            manifest_listed.contains(&mixed.to_string()),
            "a namespace with content must appear in the catalog; got: {manifest_listed:?}"
        );
        assert!(
            !manifest_listed.contains(&upload_only.to_string()),
            "an upload-only namespace must not appear in the catalog; got: {manifest_listed:?}"
        );

        test_case.cleanup().await;
    }
}

/// `scrub` prunes the dead pre-1.3 namespace-registry index objects under
/// `_registry/`, and the prune is idempotent.
#[tokio::test]
async fn delete_legacy_namespace_registry_prunes_dead_index() {
    for test_case in backends() {
        let metadata_store = test_case.metadata_store();
        for key in ["_registry/namespaces.json", "_registry/ns/ab.json"] {
            metadata_store
                .store()
                .put(key, Bytes::from_static(b"{}"))
                .await
                .expect("seed legacy registry object");
        }

        metadata_store
            .delete_legacy_namespace_registry()
            .await
            .expect("prune legacy registry");

        for key in ["_registry/namespaces.json", "_registry/ns/ab.json"] {
            assert!(
                metadata_store.store().get(key).await.is_err(),
                "legacy registry object '{key}' must be pruned"
            );
        }

        // Idempotent: a second prune with nothing left is a no-op.
        metadata_store
            .delete_legacy_namespace_registry()
            .await
            .expect("prune is idempotent");

        test_case.cleanup().await;
    }
}

/// `list_all_namespaces` is the union over every registry subtree: a
/// manifest-only, an upload-only, and a layer/config-only namespace all appear,
/// while `list_namespaces` (keyed off `_manifests` only) still excludes the
/// upload-only and layer/config-only ones, proving the additive method does not
/// change the catalog's behaviour.
#[tokio::test]
async fn list_all_namespaces_unions_every_subtree() {
    for test_case in backends() {
        let registry = test_case.registry();
        let metadata_store = test_case.metadata_store();

        let manifest_only = &Namespace::new("list-all/manifest-only").unwrap();
        let upload_only = &Namespace::new("list-all/upload-only").unwrap();
        let link_only = &Namespace::new("list-all/link-only").unwrap();

        // Manifest-only: a tag + layer via the normal content path (`_manifests`).
        test_utils::create_test_blob(registry, manifest_only, b"manifest-only").await;

        // Upload-only: an `_uploads` artifact and nothing else.
        let upload_path = path_builder::upload_path(upload_only, &uuid::Uuid::new_v4().to_string());
        metadata_store
            .store()
            .put(&upload_path, Bytes::from_static(b"partial"))
            .await
            .unwrap();

        // Layer/config-only: a layer and a config link, but no tag or revision,
        // so the namespace holds `_layers` and `_config` but no `_manifests`.
        let layer_digest = put_blob_direct(metadata_store.store(), b"layer-only-bytes").await;
        let config_digest = put_blob_direct(metadata_store.store(), b"config-only-bytes").await;
        metadata_store
            .update_links(
                link_only,
                &[
                    LinkOperation::create(
                        LinkKind::Layer(layer_digest.clone()),
                        layer_digest.clone(),
                    ),
                    LinkOperation::create(
                        LinkKind::Config(config_digest.clone()),
                        config_digest.clone(),
                    ),
                ],
            )
            .await
            .unwrap();

        let (all, _) = metadata_store
            .list_all_namespaces(1000, None)
            .await
            .unwrap();
        for ns in [manifest_only, upload_only, link_only] {
            assert!(
                all.contains(&ns.to_string()),
                "list_all_namespaces must union every subtree; '{ns}' missing from {all:?}"
            );
        }

        // The catalog (unchanged) keys off `_manifests`: only the manifest-only
        // namespace qualifies; the upload-only and link-only ones are excluded.
        let (catalog, _) = metadata_store.list_namespaces(1000, None).await.unwrap();
        assert!(
            catalog.contains(&manifest_only.to_string()),
            "list_namespaces must still include the manifest-only ns; got {catalog:?}"
        );
        assert!(
            !catalog.contains(&upload_only.to_string()),
            "list_namespaces must still EXCLUDE the upload-only ns; got {catalog:?}"
        );
        assert!(
            !catalog.contains(&link_only.to_string()),
            "list_namespaces must still EXCLUDE the layer/config-only ns; got {catalog:?}"
        );

        test_case.cleanup().await;
    }
}

/// `list_all_namespaces` paginates exactly like `list_namespaces`: equal page
/// sizes, exclusive continuation tokens, the full set surfaced once in sorted
/// order across pages.
#[tokio::test]
async fn list_all_namespaces_paginates() {
    for test_case in backends() {
        let metadata_store = test_case.metadata_store();

        // Five upload-only namespaces (no `_manifests`) so this also exercises the
        // union path rather than the content catalog.
        let names: Vec<String> = (0..5).map(|i| format!("list-all-page/ns-{i:02}")).collect();
        for name in &names {
            let ns = Namespace::new(name).unwrap();
            let upload_path = path_builder::upload_path(&ns, &uuid::Uuid::new_v4().to_string());
            metadata_store
                .store()
                .put(&upload_path, Bytes::from_static(b"partial"))
                .await
                .unwrap();
        }

        let mut walked = Vec::new();
        let mut token = None;
        loop {
            let (page, next) = metadata_store.list_all_namespaces(2, token).await.unwrap();
            assert!(page.len() <= 2, "page must not exceed n");
            walked.extend(page);
            match next {
                Some(t) => token = Some(t),
                None => break,
            }
        }

        // The union may include namespaces seeded by other tests on a shared S3
        // backend, so assert our five appear, in sorted order, exactly once.
        let mut ours: Vec<String> = walked
            .iter()
            .filter(|n| names.contains(n))
            .cloned()
            .collect();
        assert_eq!(ours.len(), names.len(), "every seeded ns surfaced once");
        let mut expected = names.clone();
        expected.sort();
        ours.sort();
        assert_eq!(ours, expected, "seeded namespaces returned in sorted order");

        test_case.cleanup().await;
    }
}

/// An empty store yields an empty `list_all_namespaces`. Uses a fresh FS-backed
/// store (rather than the shared S3 backend) so no other test's namespaces can
/// leak in.
#[tokio::test]
async fn list_all_namespaces_empty_store_is_empty() {
    use std::sync::Arc;

    use angos_storage::{ObjectStore, fs::Backend as StorageFsBackend};

    let dir = tempfile::TempDir::new().unwrap();
    let root = dir.path().to_str().unwrap();
    let object: Arc<dyn ObjectStore> = Arc::new(StorageFsBackend::builder(root).build());
    let executor = test_utils::build_test_fs_executor(root, false);
    let metadata_store = test_utils::metadata_store_over(object, executor);

    let (all, token) = metadata_store
        .list_all_namespaces(1000, None)
        .await
        .unwrap();
    assert!(
        all.is_empty(),
        "an empty store must return no namespaces; got {all:?}"
    );
    assert!(
        token.is_none(),
        "an empty store must return no continuation token"
    );
}
