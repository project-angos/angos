use std::{collections::HashSet, str::FromStr};

use crate::registry::metadata_store::backend::tests::{
    legacy_blob_index_with, put_legacy_index, test_config,
};
use crate::{
    oci::Digest,
    registry::{
        metadata_store::{
            BlobIndex, ConditionalCapabilities, LinkOperation, LockStrategy, MetadataStore,
            link_kind::LinkKind, lock::s3::S3LockConfig, s3::BackendConfig,
        },
        path_builder,
    },
};
use angos_storage::Error as StorageError;

fn cas_test_backend(config: &BackendConfig) -> crate::registry::metadata_store::Backend {
    let mut cfg = config.clone();
    cfg.lock_strategy = LockStrategy::S3(S3LockConfig::default());
    cfg.to_backend(
        Some(ConditionalCapabilities {
            put_if_none_match: true,
            put_if_match: true,
            delete_if_match: true,
        }),
        None,
    )
    .unwrap()
}

#[tokio::test]
async fn test_read_blob_index_falls_back_to_legacy_when_no_shards() {
    let config = test_config();
    let backend = config.to_backend(None, None).unwrap();
    let namespace = "legacy-fallback-1";
    let digest =
        Digest::from_str("sha256:1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a")
            .unwrap();
    let link = LinkKind::Tag("v1".into());

    let legacy = legacy_blob_index_with(vec![(namespace, vec![link.clone()])]);
    put_legacy_index(&backend, &digest, &legacy).await;

    let read = backend.read_blob_index(&digest).await.unwrap();
    let links = read
        .namespace
        .get(namespace)
        .expect("namespace must be present");
    assert!(links.contains(&link));

    backend
        .store()
        .delete_prefix(&config.key_prefix)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_read_blob_index_namespace_falls_back_to_legacy() {
    let config = test_config();
    let backend = config.to_backend(None, None).unwrap();
    let namespace_a = "legacy-fallback-2a";
    let namespace_b = "legacy-fallback-2b";
    let digest =
        Digest::from_str("sha256:1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b")
            .unwrap();
    let link_a = LinkKind::Tag("a".into());
    let link_b = LinkKind::Tag("b".into());

    let legacy = legacy_blob_index_with(vec![
        (namespace_a, vec![link_a.clone()]),
        (namespace_b, vec![link_b.clone()]),
    ]);
    put_legacy_index(&backend, &digest, &legacy).await;

    let a_links = backend
        .read_blob_index_namespace(namespace_a, &digest)
        .await
        .unwrap();
    assert_eq!(a_links.len(), 1);
    assert!(a_links.contains(&link_a));

    let b_links = backend
        .read_blob_index_namespace(namespace_b, &digest)
        .await
        .unwrap();
    assert_eq!(b_links.len(), 1);
    assert!(b_links.contains(&link_b));

    backend
        .store()
        .delete_prefix(&config.key_prefix)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_has_blob_references_sees_legacy() {
    let config = test_config();
    let backend = config.to_backend(None, None).unwrap();
    let namespace = "legacy-fallback-3";
    let digest =
        Digest::from_str("sha256:1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c")
            .unwrap();
    let link = LinkKind::Tag("ref".into());

    let legacy = legacy_blob_index_with(vec![(namespace, vec![link.clone()])]);
    put_legacy_index(&backend, &digest, &legacy).await;

    assert!(backend.has_blob_references(&digest).await.unwrap());

    // Replace with an empty legacy index — `has_blob_references` must be false.
    let empty = BlobIndex::default();
    put_legacy_index(&backend, &digest, &empty).await;
    assert!(!backend.has_blob_references(&digest).await.unwrap());

    backend
        .store()
        .delete_prefix(&config.key_prefix)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_update_links_writes_to_legacy_when_present_locked() {
    let config = test_config();
    let backend = config.to_backend(None, None).unwrap();
    let namespace = "legacy-fallback-4";
    let digest =
        Digest::from_str("sha256:1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d")
            .unwrap();
    let tag = LinkKind::Tag("new-tag".into());

    // Seed an empty legacy file so the dispatcher routes to it.
    let seed = legacy_blob_index_with(vec![(namespace, vec![LinkKind::Tag("seed".into())])]);
    put_legacy_index(&backend, &digest, &seed).await;

    let ops = vec![LinkOperation::Create {
        link: tag.clone(),
        target: digest.clone(),
        referrer: None,
        media_type: None,
        descriptor: None,
    }];
    backend.update_links(namespace, &ops).await.unwrap();

    let raw = backend
        .store()
        .get(&path_builder::blob_index_path(&digest))
        .await
        .unwrap();
    let stored: BlobIndex = serde_json::from_slice(&raw).unwrap();
    let ns_links = stored
        .namespace
        .get(namespace)
        .expect("namespace stays in legacy file");
    assert!(ns_links.contains(&tag));

    // No sharded shard should have been written.
    let shard_path = path_builder::blob_index_shard_path(&digest, namespace);
    match backend.store().get(&shard_path).await {
        Err(StorageError::NotFound) => {}
        Ok(_) => panic!("shard must not exist when legacy file took the write"),
        Err(e) => panic!("unexpected error checking shard: {e}"),
    }

    backend
        .store()
        .delete_prefix(&config.key_prefix)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_update_links_writes_to_legacy_when_present_cas() {
    let config = test_config();
    let backend = cas_test_backend(&config);
    let namespace = "legacy-fallback-5";
    let digest =
        Digest::from_str("sha256:1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e")
            .unwrap();
    let tag = LinkKind::Tag("cas-new".into());

    let seed = legacy_blob_index_with(vec![(namespace, vec![LinkKind::Tag("seed".into())])]);
    put_legacy_index(&backend, &digest, &seed).await;

    let ops = vec![LinkOperation::Create {
        link: tag.clone(),
        target: digest.clone(),
        referrer: None,
        media_type: None,
        descriptor: None,
    }];
    backend.update_links(namespace, &ops).await.unwrap();

    let raw = backend
        .store()
        .get(&path_builder::blob_index_path(&digest))
        .await
        .unwrap();
    let stored: BlobIndex = serde_json::from_slice(&raw).unwrap();
    let ns_links = stored
        .namespace
        .get(namespace)
        .expect("namespace stays in legacy file");
    assert!(ns_links.contains(&tag));

    let shard_path = path_builder::blob_index_shard_path(&digest, namespace);
    match backend.store().get(&shard_path).await {
        Err(StorageError::NotFound) => {}
        Ok(_) => panic!("shard must not exist when legacy file took the write"),
        Err(e) => panic!("unexpected error checking shard: {e}"),
    }

    backend
        .store()
        .delete_prefix(&config.key_prefix)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_update_links_deletes_legacy_when_emptied() {
    let config = test_config();
    let backend = config.to_backend(None, None).unwrap();
    let namespace = "legacy-fallback-6";
    let digest =
        Digest::from_str("sha256:1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f")
            .unwrap();
    let tag = LinkKind::Tag("only".into());

    // Stage the link normally so its `link.json` actually exists.
    let create_ops = vec![LinkOperation::Create {
        link: tag.clone(),
        target: digest.clone(),
        referrer: None,
        media_type: None,
        descriptor: None,
    }];
    backend.update_links(namespace, &create_ops).await.unwrap();

    // Replace the shard with a legacy `index.json` carrying the same link.
    let shard_path = path_builder::blob_index_shard_path(&digest, namespace);
    backend.store().delete(&shard_path).await.unwrap();
    let legacy = legacy_blob_index_with(vec![(namespace, vec![tag.clone()])]);
    put_legacy_index(&backend, &digest, &legacy).await;

    let delete_ops = vec![LinkOperation::Delete {
        link: tag.clone(),
        referrer: None,
    }];
    backend.update_links(namespace, &delete_ops).await.unwrap();

    let legacy_path = path_builder::blob_index_path(&digest);
    match backend.store().get(&legacy_path).await {
        Err(StorageError::NotFound) => {}
        Ok(_) => panic!("legacy file should be deleted once empty"),
        Err(e) => panic!("unexpected error checking legacy path: {e}"),
    }
    match backend.store().get(&shard_path).await {
        Err(StorageError::NotFound) => {}
        Ok(_) => panic!("no shard should have been written"),
        Err(e) => panic!("unexpected error checking shard: {e}"),
    }

    backend
        .store()
        .delete_prefix(&config.key_prefix)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_no_legacy_writes_still_use_shards() {
    let config = test_config();
    let backend = config.to_backend(None, None).unwrap();
    let namespace = "legacy-fallback-7";
    let digest =
        Digest::from_str("sha256:2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a")
            .unwrap();
    let tag = LinkKind::Tag("shardy".into());

    let ops = vec![LinkOperation::Create {
        link: tag.clone(),
        target: digest.clone(),
        referrer: None,
        media_type: None,
        descriptor: None,
    }];
    backend.update_links(namespace, &ops).await.unwrap();

    let shard_path = path_builder::blob_index_shard_path(&digest, namespace);
    let data = backend.store().get(&shard_path).await.unwrap();
    let links: HashSet<LinkKind> = serde_json::from_slice(&data).unwrap();
    assert!(links.contains(&tag));

    match backend
        .store()
        .get(&path_builder::blob_index_path(&digest))
        .await
    {
        Err(StorageError::NotFound) => {}
        Ok(_) => panic!("no legacy file should be created"),
        Err(e) => panic!("unexpected error checking legacy path: {e}"),
    }

    backend
        .store()
        .delete_prefix(&config.key_prefix)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_migrate_blob_index_layout_writes_shards_and_deletes_legacy() {
    let config = test_config();
    let backend = config.to_backend(None, None).unwrap();
    let namespace_a = "legacy-fallback-8a";
    let namespace_b = "legacy-fallback-8b";
    let namespace_c = "legacy-fallback-8c";
    let digest =
        Digest::from_str("sha256:2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b")
            .unwrap();
    let link_a = LinkKind::Tag("a".into());
    let link_b = LinkKind::Tag("b".into());

    let legacy = legacy_blob_index_with(vec![
        (namespace_a, vec![link_a.clone()]),
        (namespace_b, vec![link_b.clone()]),
    ]);
    put_legacy_index(&backend, &digest, &legacy).await;

    // Before migration: an update should still route into the legacy file.
    let extra = LinkKind::Tag("extra-a".into());
    let pre_ops = vec![LinkOperation::Create {
        link: extra.clone(),
        target: digest.clone(),
        referrer: None,
        media_type: None,
        descriptor: None,
    }];
    backend.update_links(namespace_a, &pre_ops).await.unwrap();
    let pre_raw = backend
        .store()
        .get(&path_builder::blob_index_path(&digest))
        .await
        .unwrap();
    let pre_stored: BlobIndex = serde_json::from_slice(&pre_raw).unwrap();
    assert!(
        pre_stored
            .namespace
            .get(namespace_a)
            .is_some_and(|s| s.contains(&extra)),
        "pre-migration update must land in the legacy file"
    );

    backend.migrate_blob_index(&digest).await.unwrap();

    // Legacy file is gone.
    match backend
        .store()
        .get(&path_builder::blob_index_path(&digest))
        .await
    {
        Err(StorageError::NotFound) => {}
        Ok(_) => panic!("legacy file should be deleted after migration"),
        Err(e) => panic!("unexpected error checking legacy path: {e}"),
    }

    // Shards exist with the right link sets.
    let shard_first = backend
        .store()
        .get(&path_builder::blob_index_shard_path(&digest, namespace_a))
        .await
        .unwrap();
    let links_first: HashSet<LinkKind> = serde_json::from_slice(&shard_first).unwrap();
    assert!(links_first.contains(&link_a));
    assert!(links_first.contains(&extra));

    let shard_second = backend
        .store()
        .get(&path_builder::blob_index_shard_path(&digest, namespace_b))
        .await
        .unwrap();
    let links_second: HashSet<LinkKind> = serde_json::from_slice(&shard_second).unwrap();
    assert!(links_second.contains(&link_b));

    // A fresh update now hits a shard (proves the fallback target is gone).
    let post_link = LinkKind::Tag("post".into());
    let post_ops = vec![LinkOperation::Create {
        link: post_link.clone(),
        target: digest.clone(),
        referrer: None,
        media_type: None,
        descriptor: None,
    }];
    backend.update_links(namespace_c, &post_ops).await.unwrap();
    let shard_third = backend
        .store()
        .get(&path_builder::blob_index_shard_path(&digest, namespace_c))
        .await
        .unwrap();
    let links_third: HashSet<LinkKind> = serde_json::from_slice(&shard_third).unwrap();
    assert!(links_third.contains(&post_link));
    match backend
        .store()
        .get(&path_builder::blob_index_path(&digest))
        .await
    {
        Err(StorageError::NotFound) => {}
        Ok(_) => panic!("legacy file must not be re-created after migration"),
        Err(e) => panic!("unexpected error checking legacy path: {e}"),
    }

    backend
        .store()
        .delete_prefix(&config.key_prefix)
        .await
        .unwrap();
}
