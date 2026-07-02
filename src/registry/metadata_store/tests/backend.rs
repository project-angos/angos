use std::{str::FromStr, sync::Arc};

use angos_s3_client as s3_client;
use angos_storage::s3::Backend as StorageS3Backend;
use angos_tx_engine::StorageError;

use super::{legacy_blob_index_with, put_legacy_index, test_config};
use crate::{
    oci::{Digest, Namespace, Tag},
    registry::{
        metadata_store::{BlobIndexOperation, LinkKind, MetadataStore},
        path_builder,
        test_utils::{locked_executor_over, metadata_store_over},
    },
};

/// Build a backend using a shared in-memory lock over the given storage.
fn make_backend(storage: Arc<StorageS3Backend>) -> Arc<MetadataStore> {
    metadata_store_over(storage.clone(), locked_executor_over(storage))
}

/// Verify that a blob-index update on a digest still carrying a legacy
/// `index.json` drains the legacy file into the per-namespace shard rather than
/// routing the write back into it.
#[tokio::test]
async fn test_update_blob_index_drains_legacy() {
    let config = test_config();

    let http = s3_client::Backend::new(&config.connection.to_client_config()).unwrap();
    let storage = Arc::new(StorageS3Backend::builder(Arc::new(http)).build());
    let backend = make_backend(storage);

    let digest =
        Digest::from_str("sha256:4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b")
            .unwrap();
    let namespace = Namespace::new("legacy-ns").unwrap();
    let seed_link = LinkKind::Tag(Tag::new("seed").unwrap());
    let new_link = LinkKind::Tag(Tag::new("new").unwrap());

    let legacy = legacy_blob_index_with(vec![(namespace.as_ref(), vec![seed_link.clone()])]);
    put_legacy_index(&backend, &digest, &legacy).await;

    backend
        .update_blob_index(
            &namespace,
            &digest,
            BlobIndexOperation::Insert(new_link.clone()),
        )
        .await
        .unwrap();

    // The shard holds both the folded legacy seed and the new link.
    let links = backend
        .read_blob_index_namespace(&namespace, &digest)
        .await
        .unwrap();
    assert!(
        links.contains(&seed_link),
        "seed link must fold into the shard"
    );
    assert!(
        links.contains(&new_link),
        "new link must be written to the shard"
    );

    // The legacy file is drained, not updated in place.
    match backend
        .store()
        .get(&path_builder::blob_index_path(&digest))
        .await
    {
        Err(StorageError::NotFound) => {}
        Ok(_) => panic!("legacy file must be drained after the update"),
        Err(e) => panic!("unexpected error checking legacy path: {e}"),
    }

    backend
        .store()
        .delete_prefix(&config.connection.key_prefix)
        .await
        .unwrap();
}
