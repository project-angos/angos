use std::{str::FromStr, sync::Arc};

use angos_s3_client as s3_client;
use angos_storage::s3::Backend as StorageS3Backend;

use super::{legacy_blob_index_with, put_legacy_index, test_config};
use crate::{
    oci::Digest,
    registry::{
        metadata_store::{BlobIndex, BlobIndexOperation, LinkKind, MetadataStore},
        path_builder,
        test_utils::{locked_executor_over, metadata_store_over},
    },
};

/// Build a backend using a shared in-memory lock over the given storage.
fn make_backend(storage: Arc<StorageS3Backend>) -> Arc<MetadataStore> {
    metadata_store_over(storage.clone(), locked_executor_over(storage))
}

/// Verify that a blob-index update applies the operation correctly to a legacy
/// `index.json` file when one is present.
#[tokio::test]
async fn test_update_blob_index_legacy_applies_correctly() {
    let config = test_config();

    let http = s3_client::Backend::new(&config.connection.to_client_config()).unwrap();
    let storage = Arc::new(StorageS3Backend::builder(Arc::new(http)).build());
    let backend = make_backend(storage);

    let digest =
        Digest::from_str("sha256:4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b")
            .unwrap();
    let namespace = "legacy-ns";
    let seed_link = LinkKind::Tag("seed".into());
    let new_link = LinkKind::Tag("new".into());

    let legacy = legacy_blob_index_with(vec![(namespace, vec![seed_link.clone()])]);
    put_legacy_index(&backend, &digest, &legacy).await;

    backend
        .update_blob_index(
            namespace,
            &digest,
            BlobIndexOperation::Insert(new_link.clone()),
        )
        .await
        .unwrap();

    // Verify the legacy file picked up the new link.
    let raw = backend
        .store()
        .get(&path_builder::blob_index_path(&digest))
        .await
        .unwrap();
    let stored: BlobIndex = serde_json::from_slice(&raw).unwrap();
    let links = stored
        .namespace
        .get(namespace)
        .expect("namespace must still be present in legacy file");
    assert!(
        links.contains(&seed_link),
        "seed link must remain after legacy update"
    );
    assert!(
        links.contains(&new_link),
        "new link must be present in legacy file after update"
    );

    backend
        .store()
        .delete_prefix(&config.connection.key_prefix)
        .await
        .unwrap();
}
