use bytes::Bytes;

use crate::{
    oci::{Namespace, Tag},
    registry::{
        metadata_store::{LinkKind, LinkOperation},
        path_builder,
        test_utils::{self, for_each_backend},
    },
};

/// The catalog is derived directly from stored content: a namespace appears in
/// `list_namespaces` exactly when it holds at least one revision or tag (a
/// `_manifests` child), and disappears once all of them are deleted, with no
/// scrub or rebuild step in between.
#[tokio::test]
async fn list_namespaces_is_derived_from_content() {
    for_each_backend(async |test_case| {
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
    })
    .await;
}

/// A namespace that holds only an in-progress upload (an `_uploads` child but no
/// `_manifests`) is not a catalog entry.
#[tokio::test]
async fn list_namespaces_excludes_upload_only_namespace() {
    for_each_backend(async |test_case| {
        let metadata_store = test_case.metadata_store();
        let namespace = Namespace::new("upload-only/repo").unwrap();
        let uuid = uuid::Uuid::new_v4().to_string();

        let upload_data_path = path_builder::upload_path(&namespace, &uuid);
        metadata_store
            .store()
            .object_store()
            .put(&upload_data_path, Bytes::from_static(b"partial"))
            .await
            .unwrap();

        let (listed, _) = metadata_store.list_namespaces(1000, None).await.unwrap();
        assert!(
            !listed.contains(&namespace.to_string()),
            "a namespace with only an _uploads artifact must not appear in the \
             catalog; got: {listed:?}"
        );
    })
    .await;
}

/// The blob store's `list_upload_namespaces` keys off the `_uploads` child,
/// the mirror of the catalog's `list_namespaces` (which keys off
/// `_manifests`), so an upload-only namespace surfaces there but not in the
/// catalog, and a manifest-only one the converse.
#[tokio::test]
async fn list_upload_namespaces_keys_off_uploads_not_manifests() {
    for_each_backend(async |test_case| {
        let registry = test_case.registry();
        let metadata_store = test_case.metadata_store();
        let blob_store = test_case.blob_store();

        let manifest_only = &Namespace::new("upload-marker/manifest-only").unwrap();
        let upload_only = &Namespace::new("upload-marker/upload-only").unwrap();
        let mixed = &Namespace::new("upload-marker/mixed").unwrap();

        // Manifest-only: a `_manifests` child and no upload.
        test_utils::create_test_blob(registry, manifest_only, b"manifest-only").await;

        // Upload-only: an upload session and no manifest content.
        blob_store
            .create_upload(upload_only, &uuid::Uuid::new_v4().to_string())
            .await
            .unwrap();

        // Mixed: both a `_manifests` child and an upload session.
        test_utils::create_test_blob(registry, mixed, b"mixed").await;
        blob_store
            .create_upload(mixed, &uuid::Uuid::new_v4().to_string())
            .await
            .unwrap();

        let (upload_listed, _) = blob_store.list_upload_namespaces(1000, None).await.unwrap();
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

    })
    .await;
}
