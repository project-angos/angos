use std::{
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use super::*;
use crate::{
    cache,
    cache::CacheExt,
    registry::metadata_store::{LinkOperation, MetadataStore},
};

fn test_config() -> BackendConfig {
    BackendConfig {
        access_key_id: "root".to_string(),
        secret_key: "roottoor".to_string(),
        endpoint: "http://127.0.0.1:9000".to_string(),
        region: "region".to_string(),
        bucket: "registry".to_string(),
        key_prefix: format!("test-cache-{}", uuid::Uuid::new_v4()),
        lock_strategy: LockStrategy::Memory,
        link_cache_ttl: 30,
        access_time_debounce_secs: 0,
        capabilities: None,
    }
}

fn test_backend_with_cache(config: &BackendConfig) -> (Backend, Arc<dyn cache::Cache>) {
    let cache: Arc<dyn cache::Cache> = Arc::new(cache::memory::Backend::new());
    let backend = Backend::new(config, None)
        .unwrap()
        .with_cache(cache.clone());
    (backend, cache)
}

#[tokio::test]
async fn test_read_link_cache_hit_skips_s3() {
    let config = test_config();
    let (backend, _cache) = test_backend_with_cache(&config);
    let namespace = "cache-hit-ns";
    let digest =
        Digest::from_str("sha256:a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2")
            .unwrap();
    let tag = LinkKind::Tag("latest".into());

    // Create link via update_links
    let ops = vec![LinkOperation::Create {
        link: tag.clone(),
        target: digest.clone(),
        referrer: None,
        media_type: None,
        descriptor: None,
    }];
    backend.update_links(namespace, &ops).await.unwrap();

    // First read populates cache
    let meta = backend.read_link(namespace, &tag, false).await.unwrap();
    assert_eq!(meta.target, digest);

    // Delete the S3 object directly
    let link_path = path_builder::link_path(&tag, namespace);
    backend.store.delete(&link_path).await.unwrap();

    // Second read should succeed from cache
    let meta = backend.read_link(namespace, &tag, false).await.unwrap();
    assert_eq!(meta.target, digest);
}

#[tokio::test]
async fn test_read_link_cache_miss_fetches_from_s3() {
    let config = test_config();
    let (backend, cache) = test_backend_with_cache(&config);
    let namespace = "cache-miss-ns";
    let digest =
        Digest::from_str("sha256:b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3")
            .unwrap();
    let tag = LinkKind::Tag("latest".into());

    // Create link via update_links
    let ops = vec![LinkOperation::Create {
        link: tag.clone(),
        target: digest.clone(),
        referrer: None,
        media_type: None,
        descriptor: None,
    }];
    backend.update_links(namespace, &ops).await.unwrap();

    // First read should return correct data from S3
    let meta = backend.read_link(namespace, &tag, false).await.unwrap();
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

    // Create link pointing to digest_a
    let ops = vec![LinkOperation::Create {
        link: tag.clone(),
        target: digest_a.clone(),
        referrer: None,
        media_type: None,
        descriptor: None,
    }];
    backend.update_links(namespace, &ops).await.unwrap();

    // Read to populate cache
    let meta = backend.read_link(namespace, &tag, false).await.unwrap();
    assert_eq!(meta.target, digest_a);

    // Wait for cache to expire
    tokio::time::sleep(Duration::from_millis(1100)).await;

    // Write new data directly to S3 (bypassing cache invalidation)
    let new_metadata = LinkMetadata::from_digest(digest_b.clone());
    backend
        .write_link_reference(namespace, &tag, &new_metadata)
        .await
        .unwrap();

    // Read again should get new digest (cache expired)
    let meta = backend.read_link(namespace, &tag, false).await.unwrap();
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

    // Create tag pointing to digest_a
    let ops = vec![LinkOperation::Create {
        link: tag.clone(),
        target: digest_a.clone(),
        referrer: None,
        media_type: None,
        descriptor: None,
    }];
    backend.update_links(namespace, &ops).await.unwrap();

    // Read to populate cache
    let meta = backend.read_link(namespace, &tag, false).await.unwrap();
    assert_eq!(meta.target, digest_a);

    // Overwrite tag to point to digest_b via update_links
    let ops = vec![LinkOperation::Create {
        link: tag.clone(),
        target: digest_b.clone(),
        referrer: None,
        media_type: None,
        descriptor: None,
    }];
    backend.update_links(namespace, &ops).await.unwrap();

    // Delete the S3 object to prove the read comes from cache
    let link_path = path_builder::link_path(&tag, namespace);
    backend.store.delete(&link_path).await.unwrap();

    // Read should return digest_b from cache (populated by update_links)
    let meta = backend.read_link(namespace, &tag, false).await.unwrap();
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

    // Create a new tag via update_links
    let ops = vec![LinkOperation::Create {
        link: tag.clone(),
        target: digest.clone(),
        referrer: None,
        media_type: None,
        descriptor: None,
    }];
    backend.update_links(namespace, &ops).await.unwrap();

    // Delete the S3 object to prove the read comes from cache
    let link_path = path_builder::link_path(&tag, namespace);
    backend.store.delete(&link_path).await.unwrap();

    // Read should succeed from cache (populated by update_links, not just invalidated)
    let meta = backend.read_link(namespace, &tag, false).await.unwrap();
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

    // Create a tag
    let ops = vec![LinkOperation::Create {
        link: tag.clone(),
        target: digest.clone(),
        referrer: None,
        media_type: None,
        descriptor: None,
    }];
    backend.update_links(namespace, &ops).await.unwrap();

    // Read to populate cache
    let meta = backend.read_link(namespace, &tag, false).await.unwrap();
    assert_eq!(meta.target, digest);

    // Delete the tag via update_links
    let ops = vec![LinkOperation::Delete {
        link: tag.clone(),
        referrer: None,
    }];
    backend.update_links(namespace, &ops).await.unwrap();

    // Read should return ReferenceNotFound (cache was invalidated, not stale)
    let result = backend.read_link(namespace, &tag, false).await;
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

    // Create link
    let ops = vec![LinkOperation::Create {
        link: tag.clone(),
        target: digest.clone(),
        referrer: None,
        media_type: None,
        descriptor: None,
    }];
    backend.update_links(namespace, &ops).await.unwrap();

    // Read with access time update
    let meta = backend.read_link(namespace, &tag, true).await.unwrap();
    assert_eq!(meta.target, digest);
    assert!(
        meta.accessed_at.is_some(),
        "accessed_at should be set after read with update_access_time=true"
    );

    // Subsequent read without access time update should return cached value with accessed_at
    let meta = backend.read_link(namespace, &tag, false).await.unwrap();
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

    // Create link
    let ops = vec![LinkOperation::Create {
        link: tag.clone(),
        target: digest.clone(),
        referrer: None,
        media_type: None,
        descriptor: None,
    }];
    backend.update_links(namespace, &ops).await.unwrap();

    // Read once
    let meta = backend.read_link(namespace, &tag, false).await.unwrap();
    assert_eq!(meta.target, digest);

    // Delete S3 object directly
    let link_path = path_builder::link_path(&tag, namespace);
    backend.store.delete(&link_path).await.unwrap();

    // Read again should fail (no caching when ttl is 0)
    let result = backend.read_link(namespace, &tag, false).await;
    assert!(
        matches!(result, Err(Error::ReferenceNotFound)),
        "Should get ReferenceNotFound when cache is disabled and S3 object is deleted"
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

    // Create same tag in two namespaces pointing to different digests
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

    // Read both - each should return its own digest
    let meta_a = backend.read_link(namespace_a, &tag, false).await.unwrap();
    let meta_b = backend.read_link(namespace_b, &tag, false).await.unwrap();

    assert_eq!(meta_a.target, digest_a);
    assert_eq!(meta_b.target, digest_b);
}

fn test_backend_with_debounce(config: &BackendConfig, debounce_secs: u64) -> Backend {
    let mut cfg = config.clone();
    cfg.access_time_debounce_secs = debounce_secs;
    Backend::new(&cfg, None).unwrap()
}

#[tokio::test]
async fn test_deferred_access_time_returns_data_immediately() {
    let config = test_config();
    let backend = test_backend_with_debounce(&config, 60);
    let namespace = "deferred-test-1";
    let digest =
        Digest::from_str("sha256:da01a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6")
            .unwrap();
    let tag = LinkKind::Tag("deferred-v1".into());

    let ops = vec![LinkOperation::Create {
        link: tag.clone(),
        target: digest.clone(),
        referrer: None,
        media_type: None,
        descriptor: None,
    }];
    backend.update_links(namespace, &ops).await.unwrap();

    // Read with access time update; debounce=60s means write is deferred
    let meta = backend.read_link(namespace, &tag, true).await.unwrap();
    assert_eq!(meta.target, digest);

    // Read directly from S3 to verify the access time was NOT written synchronously
    let raw = backend.read_link_reference(namespace, &tag).await.unwrap();
    assert!(
        raw.accessed_at.is_none(),
        "accessed_at should still be None in S3 because the write was deferred"
    );
}

#[tokio::test]
async fn test_deferred_access_time_writes_eventually() {
    let config = test_config();
    let backend = test_backend_with_debounce(&config, 1);
    let namespace = "deferred-test-2";
    let digest =
        Digest::from_str("sha256:da02b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1")
            .unwrap();
    let tag = LinkKind::Tag("deferred-v2".into());

    let ops = vec![LinkOperation::Create {
        link: tag.clone(),
        target: digest.clone(),
        referrer: None,
        media_type: None,
        descriptor: None,
    }];
    backend.update_links(namespace, &ops).await.unwrap();

    // Trigger deferred access time update
    backend.read_link(namespace, &tag, true).await.unwrap();

    // Wait for the background flush (debounce = 1s, wait 1.5s)
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Read directly from S3: accessed_at should now be set
    let raw = backend.read_link_reference(namespace, &tag).await.unwrap();
    assert!(
        raw.accessed_at.is_some(),
        "accessed_at should be set in S3 after background flush"
    );
}

#[tokio::test]
async fn test_deferred_access_time_coalesces_writes() {
    let config = test_config();
    let backend = test_backend_with_debounce(&config, 2);
    let namespace = "deferred-test-3";
    let digest =
        Digest::from_str("sha256:da03c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2")
            .unwrap();
    let tag = LinkKind::Tag("deferred-v3".into());

    let ops = vec![LinkOperation::Create {
        link: tag.clone(),
        target: digest.clone(),
        referrer: None,
        media_type: None,
        descriptor: None,
    }];
    backend.update_links(namespace, &ops).await.unwrap();

    // Call read_link with update_access_time 10 times rapidly
    let start = Instant::now();
    for _ in 0..10 {
        let meta = backend.read_link(namespace, &tag, true).await.unwrap();
        assert_eq!(meta.target, digest);
    }
    let elapsed = start.elapsed();

    // All 10 reads should complete quickly since writes are deferred
    assert!(
        elapsed < Duration::from_secs(1),
        "10 deferred reads should complete in under 1 second, took {elapsed:?}"
    );

    // Explicitly flush pending access time writes
    backend.flush_access_times().await;

    // Verify access_at is set in S3
    let raw = backend.read_link_reference(namespace, &tag).await.unwrap();
    assert!(
        raw.accessed_at.is_some(),
        "accessed_at should be set after flush"
    );
}

#[tokio::test]
async fn test_deferred_access_time_different_links_independent() {
    let config = test_config();
    let backend = test_backend_with_debounce(&config, 60);
    let namespace = "deferred-test-4";
    let digest1 =
        Digest::from_str("sha256:da04d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3")
            .unwrap();
    let digest2 =
        Digest::from_str("sha256:da04e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4")
            .unwrap();
    let tag1 = LinkKind::Tag("tag1".into());
    let tag2 = LinkKind::Tag("tag2".into());

    let ops = vec![
        LinkOperation::Create {
            link: tag1.clone(),
            target: digest1.clone(),
            referrer: None,
            media_type: None,
            descriptor: None,
        },
        LinkOperation::Create {
            link: tag2.clone(),
            target: digest2.clone(),
            referrer: None,
            media_type: None,
            descriptor: None,
        },
    ];
    backend.update_links(namespace, &ops).await.unwrap();

    // Read both with access time update
    backend.read_link(namespace, &tag1, true).await.unwrap();
    backend.read_link(namespace, &tag2, true).await.unwrap();

    // Flush all pending writes
    backend.flush_access_times().await;

    // Both should have independent accessed_at values
    let raw1 = backend.read_link_reference(namespace, &tag1).await.unwrap();
    let raw2 = backend.read_link_reference(namespace, &tag2).await.unwrap();
    assert!(
        raw1.accessed_at.is_some(),
        "tag1 accessed_at should be set after flush"
    );
    assert!(
        raw2.accessed_at.is_some(),
        "tag2 accessed_at should be set after flush"
    );
}

#[tokio::test]
async fn test_deferred_access_time_flush_on_explicit_call() {
    let config = test_config();
    let backend = test_backend_with_debounce(&config, 60);
    let namespace = "deferred-test-5";
    let digest =
        Digest::from_str("sha256:da05f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5")
            .unwrap();
    let tag = LinkKind::Tag("deferred-v5".into());

    let ops = vec![LinkOperation::Create {
        link: tag.clone(),
        target: digest.clone(),
        referrer: None,
        media_type: None,
        descriptor: None,
    }];
    backend.update_links(namespace, &ops).await.unwrap();

    // Read with access time update (deferred, won't auto-flush for 60s)
    backend.read_link(namespace, &tag, true).await.unwrap();

    // Explicitly flush
    backend.flush_access_times().await;

    // Verify S3 has the updated accessed_at
    let raw = backend.read_link_reference(namespace, &tag).await.unwrap();
    assert!(
        raw.accessed_at.is_some(),
        "accessed_at should be set in S3 after explicit flush"
    );
}

#[tokio::test]
async fn test_deferred_access_time_zero_debounce_writes_synchronously() {
    let config = test_config();
    let backend = test_backend_with_debounce(&config, 0);
    let namespace = "deferred-test-6";
    let digest =
        Digest::from_str("sha256:da06a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6")
            .unwrap();
    let tag = LinkKind::Tag("deferred-v6".into());

    let ops = vec![LinkOperation::Create {
        link: tag.clone(),
        target: digest.clone(),
        referrer: None,
        media_type: None,
        descriptor: None,
    }];
    backend.update_links(namespace, &ops).await.unwrap();

    // Read with access time update; debounce=0 means synchronous write
    backend.read_link(namespace, &tag, true).await.unwrap();

    // Immediately read directly from S3 without any flush
    let raw = backend.read_link_reference(namespace, &tag).await.unwrap();
    assert!(
        raw.accessed_at.is_some(),
        "accessed_at should be set immediately when debounce is 0 (synchronous mode)"
    );
}

#[tokio::test]
async fn test_deferred_access_time_does_not_block_read_path() {
    let config = test_config();
    let backend = test_backend_with_debounce(&config, 60);
    let namespace = "deferred-test-7";
    let digest =
        Digest::from_str("sha256:da07b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1")
            .unwrap();
    let tag = LinkKind::Tag("deferred-v7".into());

    let ops = vec![LinkOperation::Create {
        link: tag.clone(),
        target: digest.clone(),
        referrer: None,
        media_type: None,
        descriptor: None,
    }];
    backend.update_links(namespace, &ops).await.unwrap();

    // Spawn 50 concurrent read_link calls with update_access_time=true
    let start = Instant::now();
    let mut handles = Vec::new();
    for _ in 0..50 {
        let backend = backend.clone();
        let tag = tag.clone();
        let digest = digest.clone();
        let handle = tokio::spawn(async move {
            let meta = backend.read_link(namespace, &tag, true).await.unwrap();
            assert_eq!(meta.target, digest);
        });
        handles.push(handle);
    }
    for handle in handles {
        handle.await.unwrap();
    }
    let elapsed = start.elapsed();

    assert!(
        elapsed < Duration::from_secs(2),
        "50 concurrent deferred reads should complete within 2 seconds, took {elapsed:?}"
    );
}

#[tokio::test]
async fn test_flush_processes_entries_concurrently() {
    let config = test_config();
    let backend = test_backend_with_debounce(&config, 60);
    let namespace = "deferred-test-concurrent";
    let entry_count = 20;

    // Create 20 independent tags
    let mut tags = Vec::new();
    for i in 0..entry_count {
        let digest = Digest::from_str(&format!("sha256:{:0>64}", format!("cc{i:02}"))).unwrap();
        let tag = LinkKind::Tag(format!("concurrent-{i}"));

        let ops = vec![LinkOperation::Create {
            link: tag.clone(),
            target: digest,
            referrer: None,
            media_type: None,
            descriptor: None,
        }];
        backend.update_links(namespace, &ops).await.unwrap();
        tags.push(tag);
    }

    // Record access times for all 20 tags (deferred)
    for tag in &tags {
        backend.read_link(namespace, tag, true).await.unwrap();
    }

    // Flush and measure time; concurrent flush should be significantly
    // faster than sequential because each flush_one involves lock + read + write
    let start = Instant::now();
    backend.flush_access_times().await;
    let elapsed = start.elapsed();

    // Verify all 20 tags have their access times flushed
    for tag in &tags {
        let raw = backend.read_link_reference(namespace, tag).await.unwrap();
        assert!(
            raw.accessed_at.is_some(),
            "accessed_at should be set for {tag} after concurrent flush"
        );
    }

    // With 20 entries and concurrent processing (limit ~10), the flush
    // should complete in roughly 2 batches worth of time. Sequential
    // processing of 20 entries would take much longer. This is a soft
    // assertion to validate concurrency is happening.
    assert!(
        elapsed < Duration::from_secs(5),
        "Flushing 20 entries concurrently should complete within 5 seconds, took {elapsed:?}"
    );
}

#[tokio::test]
async fn test_flush_errors_do_not_prevent_other_entries() {
    let config = test_config();
    let backend = test_backend_with_debounce(&config, 60);
    let namespace = "deferred-test-error-isolation";

    // Create two valid tags
    let digest1 =
        Digest::from_str("sha256:ee01a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6")
            .unwrap();
    let digest2 =
        Digest::from_str("sha256:ee02b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1")
            .unwrap();
    let tag1 = LinkKind::Tag("error-iso-1".into());
    let tag2 = LinkKind::Tag("error-iso-2".into());

    let ops = vec![
        LinkOperation::Create {
            link: tag1.clone(),
            target: digest1,
            referrer: None,
            media_type: None,
            descriptor: None,
        },
        LinkOperation::Create {
            link: tag2.clone(),
            target: digest2,
            referrer: None,
            media_type: None,
            descriptor: None,
        },
    ];
    backend.update_links(namespace, &ops).await.unwrap();

    // Record access times for both
    backend.read_link(namespace, &tag1, true).await.unwrap();
    backend.read_link(namespace, &tag2, true).await.unwrap();

    // Also record a bogus entry that will fail during flush
    // (non-existent namespace/tag combo)
    backend
        .access_time_writer
        .as_ref()
        .unwrap()
        .record("nonexistent-namespace", &LinkKind::Tag("bogus".into()))
        .await;

    // Flush should succeed for the valid entries despite the bogus one failing
    backend.flush_access_times().await;

    // Both valid entries should have their access times set
    let raw1 = backend.read_link_reference(namespace, &tag1).await.unwrap();
    let raw2 = backend.read_link_reference(namespace, &tag2).await.unwrap();
    assert!(
        raw1.accessed_at.is_some(),
        "tag1 accessed_at should be set despite another entry failing"
    );
    assert!(
        raw2.accessed_at.is_some(),
        "tag2 accessed_at should be set despite another entry failing"
    );
}

#[tokio::test]
async fn test_blob_index_updates_multiple_digests() {
    let config = test_config();
    let backend = Backend::new(&config, None).unwrap();
    let namespace = "blob-index-multi-digest-test";

    let digests: Vec<Digest> = (0..5)
        .map(|i| {
            Digest::from_str(&format!(
                "sha256:a{i}a0000000000000000000000000000000000000000000000000000000000000"
            ))
            .unwrap()
        })
        .collect();

    let ops: Vec<LinkOperation> = digests
        .iter()
        .enumerate()
        .map(|(i, digest)| LinkOperation::Create {
            link: LinkKind::Tag(format!("tag-bim-{i}")),
            target: digest.clone(),
            referrer: None,
            media_type: None,
            descriptor: None,
        })
        .collect();

    backend.update_links(namespace, &ops).await.unwrap();

    for (i, digest) in digests.iter().enumerate() {
        let blob_index = backend.read_blob_index(digest).await.unwrap();
        let ns_links = blob_index.namespace.get(namespace).unwrap();
        let expected_link = LinkKind::Tag(format!("tag-bim-{i}"));
        assert!(
            ns_links.contains(&expected_link),
            "Blob index for digest {digest} should contain {expected_link}"
        );
    }
}

#[tokio::test]
async fn test_tracked_link_creates_with_referrers() {
    let config = test_config();
    let backend = Backend::new(&config, None).unwrap();
    let namespace = "tracked-creates-referrer-test";

    let referrer_digest =
        Digest::from_str("sha256:aa00000000000000000000000000000000000000000000000000000000000001")
            .unwrap();

    let layer_digests: Vec<Digest> = (0..3)
        .map(|i| {
            Digest::from_str(&format!(
                "sha256:b{i}b0000000000000000000000000000000000000000000000000000000000000"
            ))
            .unwrap()
        })
        .collect();

    let config_digest =
        Digest::from_str("sha256:bb00000000000000000000000000000000000000000000000000000000000001")
            .unwrap();

    let mut ops: Vec<LinkOperation> = layer_digests
        .iter()
        .map(|d| LinkOperation::Create {
            link: LinkKind::Layer(d.clone()),
            target: d.clone(),
            referrer: Some(referrer_digest.clone()),
            media_type: None,
            descriptor: None,
        })
        .collect();

    ops.push(LinkOperation::Create {
        link: LinkKind::Config(config_digest.clone()),
        target: config_digest.clone(),
        referrer: Some(referrer_digest.clone()),
        media_type: None,
        descriptor: None,
    });

    backend.update_links(namespace, &ops).await.unwrap();

    for layer_digest in &layer_digests {
        let link = LinkKind::Layer(layer_digest.clone());
        let meta = backend.read_link_reference(namespace, &link).await.unwrap();
        assert_eq!(meta.target, *layer_digest);
        assert!(
            meta.referenced_by.contains(&referrer_digest),
            "Layer link {link} should have referrer {referrer_digest}"
        );
    }

    let config_link = LinkKind::Config(config_digest.clone());
    let meta = backend
        .read_link_reference(namespace, &config_link)
        .await
        .unwrap();
    assert_eq!(meta.target, config_digest);
    assert!(
        meta.referenced_by.contains(&referrer_digest),
        "Config link should have referrer {referrer_digest}"
    );
}

#[tokio::test]
async fn test_tracked_link_deletes_with_referrers() {
    let config = test_config();
    let backend = Backend::new(&config, None).unwrap();
    let namespace = "tracked-deletes-referrer-test";

    let referrer_digest =
        Digest::from_str("sha256:cc00000000000000000000000000000000000000000000000000000000000001")
            .unwrap();

    let layer_digests: Vec<Digest> = (0..3)
        .map(|i| {
            Digest::from_str(&format!(
                "sha256:c{i}c0000000000000000000000000000000000000000000000000000000000000"
            ))
            .unwrap()
        })
        .collect();

    // Create tracked links with referrers
    let create_ops: Vec<LinkOperation> = layer_digests
        .iter()
        .map(|d| LinkOperation::Create {
            link: LinkKind::Layer(d.clone()),
            target: d.clone(),
            referrer: Some(referrer_digest.clone()),
            media_type: None,
            descriptor: None,
        })
        .collect();
    backend.update_links(namespace, &create_ops).await.unwrap();

    // Verify they exist
    for d in &layer_digests {
        let link = LinkKind::Layer(d.clone());
        let meta = backend.read_link_reference(namespace, &link).await.unwrap();
        assert_eq!(meta.target, *d);
    }

    // Delete all tracked links with referrer
    let delete_ops: Vec<LinkOperation> = layer_digests
        .iter()
        .map(|d| LinkOperation::Delete {
            link: LinkKind::Layer(d.clone()),
            referrer: Some(referrer_digest.clone()),
        })
        .collect();
    backend.update_links(namespace, &delete_ops).await.unwrap();

    // Verify links are deleted and blob indices cleaned up
    for d in &layer_digests {
        let link = LinkKind::Layer(d.clone());
        let result = backend.read_link_reference(namespace, &link).await;
        assert!(
            matches!(result, Err(Error::ReferenceNotFound)),
            "Tracked link {link} should be deleted"
        );

        let result = backend.read_blob_index(d).await;
        assert!(
            matches!(result, Err(Error::ReferenceNotFound)),
            "Blob index for {d} should be removed after all links deleted"
        );
    }
}

#[tokio::test]
async fn test_mixed_creates_and_deletes_across_digests() {
    let config = test_config();
    let backend = Backend::new(&config, None).unwrap();
    let namespace = "mixed-ops-across-digests-test";

    let digest_keep =
        Digest::from_str("sha256:dd00000000000000000000000000000000000000000000000000000000000001")
            .unwrap();
    let digest_remove =
        Digest::from_str("sha256:dd00000000000000000000000000000000000000000000000000000000000002")
            .unwrap();
    let digest_add =
        Digest::from_str("sha256:dd00000000000000000000000000000000000000000000000000000000000003")
            .unwrap();

    // Setup: create tags for digest_keep and digest_remove
    let setup_ops = vec![
        LinkOperation::Create {
            link: LinkKind::Tag("keep-tag".into()),
            target: digest_keep.clone(),
            referrer: None,
            media_type: None,
            descriptor: None,
        },
        LinkOperation::Create {
            link: LinkKind::Tag("remove-tag".into()),
            target: digest_remove.clone(),
            referrer: None,
            media_type: None,
            descriptor: None,
        },
    ];
    backend.update_links(namespace, &setup_ops).await.unwrap();

    // Mixed operation: delete remove-tag, add new-tag pointing to digest_add
    let mixed_ops = vec![
        LinkOperation::Delete {
            link: LinkKind::Tag("remove-tag".into()),
            referrer: None,
        },
        LinkOperation::Create {
            link: LinkKind::Tag("new-tag".into()),
            target: digest_add.clone(),
            referrer: None,
            media_type: None,
            descriptor: None,
        },
    ];
    backend.update_links(namespace, &mixed_ops).await.unwrap();

    // Verify: keep-tag still exists
    let keep_index = backend.read_blob_index(&digest_keep).await.unwrap();
    let keep_links = keep_index.namespace.get(namespace).unwrap();
    assert!(keep_links.contains(&LinkKind::Tag("keep-tag".into())));

    // Verify: remove-tag blob index no longer has it
    match backend.read_blob_index(&digest_remove).await {
        Ok(idx) => {
            let links = idx.namespace.get(namespace);
            assert!(
                links.is_none() || !links.unwrap().contains(&LinkKind::Tag("remove-tag".into())),
                "remove-tag should not be in blob index after delete"
            );
        }
        Err(Error::ReferenceNotFound) => {}
        Err(e) => panic!("Unexpected error reading blob index: {e}"),
    }

    // Verify: new-tag exists in digest_add's blob index
    let add_index = backend.read_blob_index(&digest_add).await.unwrap();
    let add_links = add_index.namespace.get(namespace).unwrap();
    assert!(add_links.contains(&LinkKind::Tag("new-tag".into())));

    // Verify: remove-tag link itself is gone
    let result = backend
        .read_link_reference(namespace, &LinkKind::Tag("remove-tag".into()))
        .await;
    assert!(matches!(result, Err(Error::ReferenceNotFound)));

    // Verify: new-tag link exists and points to digest_add
    let new_meta = backend
        .read_link_reference(namespace, &LinkKind::Tag("new-tag".into()))
        .await
        .unwrap();
    assert_eq!(new_meta.target, digest_add);
}

#[tokio::test]
async fn test_read_link_with_access_time_debounce_uses_cache() {
    let config = test_config();
    let mut cfg = config.clone();
    cfg.access_time_debounce_secs = 60;
    let cache: Arc<dyn cache::Cache> = Arc::new(cache::memory::Backend::new());
    let backend = Backend::new(&cfg, None).unwrap().with_cache(cache.clone());
    let namespace = "cache-debounce-hit-ns";
    let digest =
        Digest::from_str("sha256:db01a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6")
            .unwrap();
    let tag = LinkKind::Tag("debounce-cached".into());

    // Create link via update_links (populates cache)
    let ops = vec![LinkOperation::Create {
        link: tag.clone(),
        target: digest.clone(),
        referrer: None,
        media_type: None,
        descriptor: None,
    }];
    backend.update_links(namespace, &ops).await.unwrap();

    // First read with access time update (debounce path)
    let meta = backend.read_link(namespace, &tag, true).await.unwrap();
    assert_eq!(meta.target, digest);

    // Delete the S3 object to prove the next read must come from cache
    let link_path = path_builder::link_path(&tag, namespace);
    backend.store.delete(&link_path).await.unwrap();

    // Second read with access time update should succeed from cache
    let meta = backend.read_link(namespace, &tag, true).await.unwrap();
    assert_eq!(meta.target, digest);

    // Verify writer.record() was still called on cache hit
    let writer = backend.access_time_writer.as_ref().unwrap();
    let pending = writer.pending.lock().await;
    assert!(
        !pending.is_empty(),
        "writer.record() should have been called even on cache hit"
    );
}

#[tokio::test]
async fn test_probe_conditional_capabilities() {
    let config = test_config();
    let store = data_store::s3::Backend::new(&data_store::s3::BackendConfig {
        access_key_id: config.access_key_id.clone(),
        secret_key: config.secret_key.clone(),
        endpoint: config.endpoint.clone(),
        bucket: config.bucket.clone(),
        region: config.region.clone(),
        key_prefix: config.key_prefix.clone(),
        ..Default::default()
    })
    .unwrap();

    let result = Backend::probe_conditional_capabilities(&store).await;
    assert!(result.is_ok(), "Probe should pass on MinIO: {result:?}");
    let caps = result.unwrap();
    assert!(caps.put_if_none_match, "MinIO should support If-None-Match");
    assert!(caps.put_if_match, "MinIO should support If-Match");
    // MinIO does not enforce If-Match on DeleteObject (returns 200 regardless of ETag),
    // so delete_if_match is expected to be false.
    assert!(
        !caps.delete_if_match,
        "MinIO does not support conditional delete (ignores If-Match on DeleteObject)"
    );
}
