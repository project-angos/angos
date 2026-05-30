use std::{str::FromStr, sync::Arc};

use angos_s3_client as s3_client;
use angos_storage::s3::Backend as StorageS3Backend;
use angos_tx_engine::{
    executor::locked::LockedExecutor,
    lock::{primitive::Lock, storage::memory::MemoryLockStorage},
    store::Store,
};

use super::{legacy_blob_index_with, put_legacy_index, test_config};
use crate::{
    oci::Digest,
    registry::{
        metadata_store::{BlobIndex, BlobIndexOperation, MetadataStore, link_kind::LinkKind},
        path_builder,
    },
};

/// Build a backend using a shared in-memory lock over the given storage.
fn make_backend(storage: Arc<StorageS3Backend>) -> MetadataStore {
    let shared_lock = Arc::new(
        Lock::builder()
            .storage(Arc::new(MemoryLockStorage::new()))
            .build()
            .expect("lock"),
    );

    let executor = Arc::new(
        LockedExecutor::builder()
            .store(storage.clone())
            .lock(shared_lock)
            .build()
            .unwrap(),
    );
    let facade = Arc::new(
        Store::builder()
            .object(storage)
            .executor(executor)
            .build()
            .unwrap(),
    );
    MetadataStore::builder()
        .store(facade)
        .link_cache_ttl(0)
        .access_time_debounce_secs(0)
        .build()
        .unwrap()
}

/// Verify that a blob-index update applies the operation correctly to a legacy
/// `index.json` file when one is present.
#[tokio::test]
async fn test_update_blob_index_legacy_applies_correctly() {
    let config = test_config();

    let http = s3_client::Backend::new(&config.connection.to_client_config()).unwrap();
    let storage = Arc::new(
        StorageS3Backend::builder()
            .client(Arc::new(http))
            .build()
            .unwrap(),
    );
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
