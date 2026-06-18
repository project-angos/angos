use std::{str::FromStr, time::Duration};

use super::{test_backend_with_cache, test_config};
use crate::{
    oci::Digest,
    registry::{
        metadata_store::{Error, LinkKind, LinkMetadata, LinkOperation},
        path_builder,
    },
};

#[tokio::test]
async fn test_read_link_cache_hit_skips_storage() {
    let config = test_config();
    let (backend, _cache) = test_backend_with_cache(&config);
    let namespace = "cache-hit-ns";
    let digest =
        Digest::from_str("sha256:a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2")
            .unwrap();
    let tag = LinkKind::Tag("latest".into());

    let ops = vec![LinkOperation::Create {
        link: tag.clone(),
        target: digest.clone(),
        referrer: None,
        media_type: None,
        descriptor: None,
    }];
    backend.update_links(namespace, &ops).await.unwrap();

    // First read populates cache
    let meta = backend.read_link(namespace, &tag).await.unwrap();
    assert_eq!(meta.target, digest);

    // Delete the storage object directly
    let link_path = path_builder::link_path(&tag, namespace);
    backend.store().delete(&link_path).await.unwrap();

    // Second read should succeed from cache
    let meta = backend.read_link(namespace, &tag).await.unwrap();
    assert_eq!(meta.target, digest);
}

#[tokio::test]
async fn test_read_link_cache_miss_fetches_from_storage() {
    let config = test_config();
    let (backend, cache) = test_backend_with_cache(&config);
    let namespace = "cache-miss-ns";
    let digest =
        Digest::from_str("sha256:b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3")
            .unwrap();
    let tag = LinkKind::Tag("latest".into());

    let ops = vec![LinkOperation::Create {
        link: tag.clone(),
        target: digest.clone(),
        referrer: None,
        media_type: None,
        descriptor: None,
    }];
    backend.update_links(namespace, &ops).await.unwrap();

    // First read should return correct data from storage
    let meta = backend.read_link(namespace, &tag).await.unwrap();
    assert_eq!(meta.target, digest);

    // Verify cache was populated
    let cache_key = format!("link:{namespace}:{tag}");
    let cached: Option<LinkMetadata> = cache.retrieve(&cache_key).await.unwrap();
    assert!(cached.is_some(), "Cache should be populated after read");
    assert_eq!(cached.unwrap().target, digest);
}

#[tokio::test]
async fn test_read_link_cache_expired_refetches() {
    let mut config = test_config();
    config.link_cache_ttl = 1;
    let (backend, _cache) = test_backend_with_cache(&config);
    let namespace = "cache-expired-ns";
    let digest_a =
        Digest::from_str("sha256:c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4")
            .unwrap();
    let digest_b =
        Digest::from_str("sha256:d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5")
            .unwrap();
    let tag = LinkKind::Tag("latest".into());

    let ops = vec![LinkOperation::Create {
        link: tag.clone(),
        target: digest_a.clone(),
        referrer: None,
        media_type: None,
        descriptor: None,
    }];
    backend.update_links(namespace, &ops).await.unwrap();

    let meta = backend.read_link(namespace, &tag).await.unwrap();
    assert_eq!(meta.target, digest_a);

    tokio::time::sleep(Duration::from_millis(1100)).await;

    // Write new data directly to storage (bypassing cache invalidation)
    let new_metadata = LinkMetadata::from_digest(digest_b.clone());
    backend
        .write_link_reference(namespace, &tag, &new_metadata)
        .await
        .unwrap();

    let meta = backend.read_link(namespace, &tag).await.unwrap();
    assert_eq!(meta.target, digest_b);
}

#[tokio::test]
async fn test_update_links_populates_cache_on_overwrite() {
    let config = test_config();
    let (backend, _cache) = test_backend_with_cache(&config);
    let namespace = "cache-invalidate-ns";
    let digest_a =
        Digest::from_str("sha256:e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6")
            .unwrap();
    let digest_b =
        Digest::from_str("sha256:f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1")
            .unwrap();
    let tag = LinkKind::Tag("latest".into());

    let ops = vec![LinkOperation::Create {
        link: tag.clone(),
        target: digest_a.clone(),
        referrer: None,
        media_type: None,
        descriptor: None,
    }];
    backend.update_links(namespace, &ops).await.unwrap();

    let meta = backend.read_link(namespace, &tag).await.unwrap();
    assert_eq!(meta.target, digest_a);

    let ops = vec![LinkOperation::Create {
        link: tag.clone(),
        target: digest_b.clone(),
        referrer: None,
        media_type: None,
        descriptor: None,
    }];
    backend.update_links(namespace, &ops).await.unwrap();

    // Delete the storage object to prove the read comes from cache
    let link_path = path_builder::link_path(&tag, namespace);
    backend.store().delete(&link_path).await.unwrap();

    let meta = backend.read_link(namespace, &tag).await.unwrap();
    assert_eq!(meta.target, digest_b);
}

#[tokio::test]
async fn test_update_links_populates_cache_on_create() {
    let config = test_config();
    let (backend, _cache) = test_backend_with_cache(&config);
    let namespace = "cache-populate-create-ns";
    let digest =
        Digest::from_str("sha256:a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1")
            .unwrap();
    let tag = LinkKind::Tag("v1".into());

    let ops = vec![LinkOperation::Create {
        link: tag.clone(),
        target: digest.clone(),
        referrer: None,
        media_type: None,
        descriptor: None,
    }];
    backend.update_links(namespace, &ops).await.unwrap();

    // Delete the storage object to prove the read comes from cache
    let link_path = path_builder::link_path(&tag, namespace);
    backend.store().delete(&link_path).await.unwrap();

    let meta = backend.read_link(namespace, &tag).await.unwrap();
    assert_eq!(meta.target, digest);
}

#[tokio::test]
async fn test_update_links_invalidates_cache_on_delete() {
    let config = test_config();
    let (backend, _cache) = test_backend_with_cache(&config);
    let namespace = "cache-invalidate-delete-ns";
    let digest =
        Digest::from_str("sha256:b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2")
            .unwrap();
    let tag = LinkKind::Tag("to-delete".into());

    let ops = vec![LinkOperation::Create {
        link: tag.clone(),
        target: digest.clone(),
        referrer: None,
        media_type: None,
        descriptor: None,
    }];
    backend.update_links(namespace, &ops).await.unwrap();

    let meta = backend.read_link(namespace, &tag).await.unwrap();
    assert_eq!(meta.target, digest);

    let ops = vec![LinkOperation::Delete {
        link: tag.clone(),
        referrer: None,
    }];
    backend.update_links(namespace, &ops).await.unwrap();

    let result = backend.read_link(namespace, &tag).await;
    assert!(
        matches!(result, Err(Error::ReferenceNotFound)),
        "Should get ReferenceNotFound after deleting a tag via update_links"
    );
}

#[tokio::test]
async fn test_read_link_with_access_time_update_populates_cache() {
    let config = test_config();
    let (backend, _cache) = test_backend_with_cache(&config);
    let namespace = "cache-access-time-ns";
    let digest =
        Digest::from_str("sha256:a1a2a3a4a5a6a7a8a1a2a3a4a5a6a7a8a1a2a3a4a5a6a7a8a1a2a3a4a5a6a7a8")
            .unwrap();
    let tag = LinkKind::Tag("latest".into());

    let ops = vec![LinkOperation::Create {
        link: tag.clone(),
        target: digest.clone(),
        referrer: None,
        media_type: None,
        descriptor: None,
    }];
    backend.update_links(namespace, &ops).await.unwrap();

    let meta = backend
        .read_link_recording_access(namespace, &tag)
        .await
        .unwrap();
    assert_eq!(meta.target, digest);
    assert!(
        meta.accessed_at.is_some(),
        "accessed_at should be set after a recording read"
    );

    let meta = backend.read_link(namespace, &tag).await.unwrap();
    assert_eq!(meta.target, digest);
    assert!(
        meta.accessed_at.is_some(),
        "accessed_at should be present in cached value"
    );
}

#[tokio::test]
async fn test_cache_disabled_when_ttl_zero() {
    let mut config = test_config();
    config.link_cache_ttl = 0;
    let (backend, _cache) = test_backend_with_cache(&config);
    let namespace = "cache-disabled-ns";
    let digest =
        Digest::from_str("sha256:b1b2b3b4b5b6b7b8b1b2b3b4b5b6b7b8b1b2b3b4b5b6b7b8b1b2b3b4b5b6b7b8")
            .unwrap();
    let tag = LinkKind::Tag("latest".into());

    let ops = vec![LinkOperation::Create {
        link: tag.clone(),
        target: digest.clone(),
        referrer: None,
        media_type: None,
        descriptor: None,
    }];
    backend.update_links(namespace, &ops).await.unwrap();

    let meta = backend.read_link(namespace, &tag).await.unwrap();
    assert_eq!(meta.target, digest);

    let link_path = path_builder::link_path(&tag, namespace);
    backend.store().delete(&link_path).await.unwrap();

    let result = backend.read_link(namespace, &tag).await;
    assert!(
        matches!(result, Err(Error::ReferenceNotFound)),
        "Should get ReferenceNotFound when cache is disabled and storage object is deleted"
    );
}

#[tokio::test]
async fn test_cache_keys_are_namespace_scoped() {
    let config = test_config();
    let (backend, _cache) = test_backend_with_cache(&config);
    let namespace_a = "cache-scope-ns-a";
    let namespace_b = "cache-scope-ns-b";
    let digest_a =
        Digest::from_str("sha256:c1c2c3c4c5c6c7c8c1c2c3c4c5c6c7c8c1c2c3c4c5c6c7c8c1c2c3c4c5c6c7c8")
            .unwrap();
    let digest_b =
        Digest::from_str("sha256:d1d2d3d4d5d6d7d8d1d2d3d4d5d6d7d8d1d2d3d4d5d6d7d8d1d2d3d4d5d6d7d8")
            .unwrap();
    let tag = LinkKind::Tag("latest".into());

    let ops_a = vec![LinkOperation::Create {
        link: tag.clone(),
        target: digest_a.clone(),
        referrer: None,
        media_type: None,
        descriptor: None,
    }];
    backend.update_links(namespace_a, &ops_a).await.unwrap();

    let ops_b = vec![LinkOperation::Create {
        link: tag.clone(),
        target: digest_b.clone(),
        referrer: None,
        media_type: None,
        descriptor: None,
    }];
    backend.update_links(namespace_b, &ops_b).await.unwrap();

    let meta_a = backend.read_link(namespace_a, &tag).await.unwrap();
    let meta_b = backend.read_link(namespace_b, &tag).await.unwrap();

    assert_eq!(meta_a.target, digest_a);
    assert_eq!(meta_b.target, digest_b);
}
