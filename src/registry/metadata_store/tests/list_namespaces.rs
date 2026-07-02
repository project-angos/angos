use bytes::Bytes;

use crate::{
    oci::{Namespace, Tag},
    registry::{
        metadata_store::{LinkKind, LinkOperation},
        path_builder,
        test_utils::{self, backends, put_blob_direct},
    },
};

/// The catalog is content-derived: a namespace appears in `list_namespaces` when
/// it holds a `_manifests` child and disappears once all are deleted, with no
/// scrub or rebuild in between.
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

        // Remove the only `_manifests` content, with no scrub or rebuild after.
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

/// `collect_all_namespaces` unions every registry subtree (manifest-only,
/// upload-only, layer/config-only all appear) in sorted order, while
/// `list_namespaces` still excludes the upload-only and layer/config-only ones.
#[tokio::test]
async fn collect_all_namespaces_unions_every_subtree() {
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

        // The walk-once collector sees every subtree's namespace, sorted.
        let collected = metadata_store.collect_all_namespaces().await.unwrap();
        for ns in [manifest_only, upload_only, link_only] {
            assert!(
                collected.contains(&ns.to_string()),
                "collect_all_namespaces must union every subtree; '{ns}' missing"
            );
        }
        assert!(
            collected.windows(2).all(|pair| pair[0] <= pair[1]),
            "collect_all_namespaces must return sorted names; got {collected:?}"
        );

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
