use bytes::Bytes;

use crate::{
    oci::Namespace,
    registry::{
        metadata_store::{LinkOperation, link_kind::LinkKind},
        path_builder,
        test_utils::{self, backends},
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
                    LinkOperation::delete(LinkKind::Tag("latest".to_string())),
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
        let namespace = "upload-only/repo";
        let uuid = uuid::Uuid::new_v4().to_string();

        let upload_data_path = path_builder::upload_path(namespace, &uuid);
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
