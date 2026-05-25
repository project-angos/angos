use bytes::Bytes;

use angos_storage::Error as StorageError;

use crate::registry::metadata_store::backend::tests::test_config;
use crate::registry::metadata_store::sharded::NamespaceRegistry;
use crate::registry::{metadata_store::MetadataStore, path_builder};

#[tokio::test]
async fn test_rebuild_namespace_registry_includes_upload_only_namespace() {
    let config = test_config();
    let backend = config.to_backend(None, None).unwrap();
    let namespace = "upload-only-ns/repo";
    let uuid = uuid::Uuid::new_v4().to_string();

    let upload_data_path = path_builder::upload_path(namespace, &uuid);
    backend
        .store()
        .put(&upload_data_path, Bytes::from_static(b"partial"))
        .await
        .unwrap();

    backend.rebuild_namespace_registry().await.unwrap();

    let (namespaces, _) = backend.list_namespaces(100, None).await.unwrap();
    assert!(
        namespaces.contains(&namespace.to_string()),
        "list_namespaces must include a namespace that has only an _uploads artifact; got: {namespaces:?}"
    );

    backend
        .store()
        .delete_prefix(&config.connection.key_prefix)
        .await
        .unwrap();
}

/// When no namespace shards exist, `list_namespaces` must fall back to the
/// legacy `namespace_registry.json` file and return the namespaces it contains.
#[tokio::test]
async fn test_list_namespaces_falls_back_to_legacy_namespace_registry() {
    let config = test_config();
    let backend = config.to_backend(None, None).unwrap();

    let legacy = NamespaceRegistry {
        namespaces: vec!["legacy-ns/alpha".to_string(), "legacy-ns/beta".to_string()],
    };
    let body = Bytes::from(serde_json::to_vec(&legacy).unwrap());
    backend
        .store()
        .put(&path_builder::namespace_registry_path(), body)
        .await
        .unwrap();

    let (namespaces, _) = backend.list_namespaces(100, None).await.unwrap();
    assert!(
        namespaces.contains(&"legacy-ns/alpha".to_string()),
        "list_namespaces must return legacy-ns/alpha from the legacy file; got: {namespaces:?}"
    );
    assert!(
        namespaces.contains(&"legacy-ns/beta".to_string()),
        "list_namespaces must return legacy-ns/beta from the legacy file; got: {namespaces:?}"
    );

    backend
        .store()
        .delete_prefix(&config.connection.key_prefix)
        .await
        .unwrap();
}

/// `migrate_namespace_registry` must write namespace shards and then remove the
/// legacy `namespace_registry.json`. After migration, `list_namespaces` must
/// still return the original namespaces (now served from shards).
#[tokio::test]
async fn test_migrate_namespace_registry_rebuilds_shards_and_removes_legacy() {
    let config = test_config();
    let backend = config.to_backend(None, None).unwrap();

    // Seed actual repository data so `rebuild_namespace_registry` can discover
    // the namespaces by scanning `v2/repositories/`.
    let uuid = uuid::Uuid::new_v4().to_string();
    for ns in ["migrate-ns/one", "migrate-ns/two"] {
        backend
            .store()
            .put(
                &path_builder::upload_path(ns, &uuid),
                Bytes::from_static(b"data"),
            )
            .await
            .unwrap();
    }

    // Also write a legacy registry file so we exercise the pre-migration state.
    let legacy = NamespaceRegistry {
        namespaces: vec!["migrate-ns/one".to_string(), "migrate-ns/two".to_string()],
    };
    let body = Bytes::from(serde_json::to_vec(&legacy).unwrap());
    let legacy_path = path_builder::namespace_registry_path();
    backend.store().put(&legacy_path, body).await.unwrap();

    backend.migrate_namespace_registry().await.unwrap();

    // Legacy file must be gone.
    match backend.store().get(&legacy_path).await {
        Err(StorageError::NotFound) => {}
        Ok(_) => panic!("legacy namespace_registry.json must be deleted after migration"),
        Err(e) => panic!("unexpected error checking legacy path: {e}"),
    }

    // Namespaces are still discoverable via shards.
    let (namespaces, _) = backend.list_namespaces(100, None).await.unwrap();
    assert!(
        namespaces.contains(&"migrate-ns/one".to_string()),
        "migrate-ns/one must be present after migration; got: {namespaces:?}"
    );
    assert!(
        namespaces.contains(&"migrate-ns/two".to_string()),
        "migrate-ns/two must be present after migration; got: {namespaces:?}"
    );

    backend
        .store()
        .delete_prefix(&config.connection.key_prefix)
        .await
        .unwrap();
}
