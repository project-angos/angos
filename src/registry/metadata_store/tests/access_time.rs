use std::{
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use angos_tx_engine::ConditionalCapabilities;

use super::{test_backend_with_debounce, test_config};
use crate::{
    cache::Cache as CacheEnum,
    cache::memory::Backend as CacheMemoryBackend,
    oci::{Digest, Namespace, Tag},
    registry::{
        metadata_store::{LinkKind, LinkOperation},
        path_builder,
    },
};

#[tokio::test]
async fn test_deferred_access_time_returns_data_immediately() {
    let config = test_config();
    let backend = test_backend_with_debounce(&config, 60);
    let namespace = Namespace::new("deferred-test-1").unwrap();
    let digest =
        Digest::from_str("sha256:da01a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6")
            .unwrap();
    let tag = LinkKind::Tag(Tag::new("deferred-v1").unwrap());

    let ops = vec![LinkOperation::Create {
        link: tag.clone(),
        target: digest.clone(),
        referrer: None,
        media_type: None,
        descriptor: None,
    }];
    backend.update_links(&namespace, &ops).await.unwrap();

    let meta = backend
        .read_link_recording_access(&namespace, &tag)
        .await
        .unwrap();
    assert_eq!(meta.target, digest);

    let raw = backend.read_link_reference(&namespace, &tag).await.unwrap();
    assert!(
        raw.accessed_at.is_none(),
        "accessed_at should still be None in storage because the write was deferred"
    );
}

#[tokio::test]
async fn test_deferred_access_time_writes_eventually() {
    let config = test_config();
    let backend = test_backend_with_debounce(&config, 1);
    let namespace = Namespace::new("deferred-test-2").unwrap();
    let digest =
        Digest::from_str("sha256:da02b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1")
            .unwrap();
    let tag = LinkKind::Tag(Tag::new("deferred-v2").unwrap());

    let ops = vec![LinkOperation::Create {
        link: tag.clone(),
        target: digest.clone(),
        referrer: None,
        media_type: None,
        descriptor: None,
    }];
    backend.update_links(&namespace, &ops).await.unwrap();

    backend
        .read_link_recording_access(&namespace, &tag)
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(1500)).await;

    let raw = backend.read_link_reference(&namespace, &tag).await.unwrap();
    assert!(
        raw.accessed_at.is_some(),
        "accessed_at should be set in storage after background flush"
    );
}

#[tokio::test]
async fn test_deferred_access_time_coalesces_writes() {
    let config = test_config();
    let backend = test_backend_with_debounce(&config, 2);
    let namespace = Namespace::new("deferred-test-3").unwrap();
    let digest =
        Digest::from_str("sha256:da03c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2")
            .unwrap();
    let tag = LinkKind::Tag(Tag::new("deferred-v3").unwrap());

    let ops = vec![LinkOperation::Create {
        link: tag.clone(),
        target: digest.clone(),
        referrer: None,
        media_type: None,
        descriptor: None,
    }];
    backend.update_links(&namespace, &ops).await.unwrap();

    let start = Instant::now();
    for _ in 0..10 {
        let meta = backend
            .read_link_recording_access(&namespace, &tag)
            .await
            .unwrap();
        assert_eq!(meta.target, digest);
    }
    let elapsed = start.elapsed();

    assert!(
        elapsed < Duration::from_secs(1),
        "10 deferred reads should complete in under 1 second, took {elapsed:?}"
    );

    backend.flush_access_times().await;

    let raw = backend.read_link_reference(&namespace, &tag).await.unwrap();
    assert!(
        raw.accessed_at.is_some(),
        "accessed_at should be set after flush"
    );
}

#[tokio::test]
async fn test_deferred_access_time_different_links_independent() {
    let config = test_config();
    let backend = test_backend_with_debounce(&config, 60);
    let namespace = Namespace::new("deferred-test-4").unwrap();
    let digest1 =
        Digest::from_str("sha256:da04d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3")
            .unwrap();
    let digest2 =
        Digest::from_str("sha256:da04e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4")
            .unwrap();
    let tag1 = LinkKind::Tag(Tag::new("tag1").unwrap());
    let tag2 = LinkKind::Tag(Tag::new("tag2").unwrap());

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
    backend.update_links(&namespace, &ops).await.unwrap();

    backend
        .read_link_recording_access(&namespace, &tag1)
        .await
        .unwrap();
    backend
        .read_link_recording_access(&namespace, &tag2)
        .await
        .unwrap();

    backend.flush_access_times().await;

    let raw1 = backend
        .read_link_reference(&namespace, &tag1)
        .await
        .unwrap();
    let raw2 = backend
        .read_link_reference(&namespace, &tag2)
        .await
        .unwrap();
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
    let namespace = Namespace::new("deferred-test-5").unwrap();
    let digest =
        Digest::from_str("sha256:da05f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5")
            .unwrap();
    let tag = LinkKind::Tag(Tag::new("deferred-v5").unwrap());

    let ops = vec![LinkOperation::Create {
        link: tag.clone(),
        target: digest.clone(),
        referrer: None,
        media_type: None,
        descriptor: None,
    }];
    backend.update_links(&namespace, &ops).await.unwrap();

    backend
        .read_link_recording_access(&namespace, &tag)
        .await
        .unwrap();

    backend.flush_access_times().await;

    let raw = backend.read_link_reference(&namespace, &tag).await.unwrap();
    assert!(
        raw.accessed_at.is_some(),
        "accessed_at should be set in storage after explicit flush"
    );
}

#[tokio::test]
async fn test_deferred_access_time_zero_debounce_writes_synchronously() {
    let config = test_config();
    let backend = test_backend_with_debounce(&config, 0);
    let namespace = Namespace::new("deferred-test-6").unwrap();
    let digest =
        Digest::from_str("sha256:da06a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6")
            .unwrap();
    let tag = LinkKind::Tag(Tag::new("deferred-v6").unwrap());

    let ops = vec![LinkOperation::Create {
        link: tag.clone(),
        target: digest.clone(),
        referrer: None,
        media_type: None,
        descriptor: None,
    }];
    backend.update_links(&namespace, &ops).await.unwrap();

    backend
        .read_link_recording_access(&namespace, &tag)
        .await
        .unwrap();

    let raw = backend.read_link_reference(&namespace, &tag).await.unwrap();
    assert!(
        raw.accessed_at.is_some(),
        "accessed_at should be set immediately when debounce is 0 (synchronous mode)"
    );
}

#[tokio::test]
async fn test_deferred_access_time_does_not_block_read_path() {
    let config = test_config();
    let backend = test_backend_with_debounce(&config, 60);
    let namespace = Namespace::new("deferred-test-7").unwrap();
    let digest =
        Digest::from_str("sha256:da07b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1")
            .unwrap();
    let tag = LinkKind::Tag(Tag::new("deferred-v7").unwrap());

    let ops = vec![LinkOperation::Create {
        link: tag.clone(),
        target: digest.clone(),
        referrer: None,
        media_type: None,
        descriptor: None,
    }];
    backend.update_links(&namespace, &ops).await.unwrap();

    let start = Instant::now();
    let mut handles = Vec::new();
    for _ in 0..50 {
        let backend = backend.clone();
        let namespace = namespace.clone();
        let tag = tag.clone();
        let digest = digest.clone();
        let handle = tokio::spawn(async move {
            let meta = backend
                .read_link_recording_access(&namespace, &tag)
                .await
                .unwrap();
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
    let namespace = Namespace::new("deferred-test-concurrent").unwrap();
    let entry_count = 20;

    let mut tags = Vec::new();
    for i in 0..entry_count {
        let digest = Digest::from_str(&format!("sha256:{:0>64}", format!("cc{i:02}"))).unwrap();
        let tag = LinkKind::Tag(Tag::try_from(format!("concurrent-{i}")).unwrap());

        let ops = vec![LinkOperation::Create {
            link: tag.clone(),
            target: digest,
            referrer: None,
            media_type: None,
            descriptor: None,
        }];
        backend.update_links(&namespace, &ops).await.unwrap();
        tags.push(tag);
    }

    for tag in &tags {
        backend
            .read_link_recording_access(&namespace, tag)
            .await
            .unwrap();
    }

    let start = Instant::now();
    backend.flush_access_times().await;
    let elapsed = start.elapsed();

    for tag in &tags {
        let raw = backend.read_link_reference(&namespace, tag).await.unwrap();
        assert!(
            raw.accessed_at.is_some(),
            "accessed_at should be set for {tag} after concurrent flush"
        );
    }

    assert!(
        elapsed < Duration::from_secs(5),
        "Flushing 20 entries concurrently should complete within 5 seconds, took {elapsed:?}"
    );
}

#[tokio::test]
async fn test_flush_errors_do_not_prevent_other_entries() {
    let config = test_config();
    let backend = test_backend_with_debounce(&config, 60);
    let namespace = Namespace::new("deferred-test-error-isolation").unwrap();

    let digest1 =
        Digest::from_str("sha256:ee01a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6")
            .unwrap();
    let digest2 =
        Digest::from_str("sha256:ee02b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1")
            .unwrap();
    let tag1 = LinkKind::Tag(Tag::new("error-iso-1").unwrap());
    let tag2 = LinkKind::Tag(Tag::new("error-iso-2").unwrap());

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
    backend.update_links(&namespace, &ops).await.unwrap();

    backend
        .read_link_recording_access(&namespace, &tag1)
        .await
        .unwrap();
    backend
        .read_link_recording_access(&namespace, &tag2)
        .await
        .unwrap();

    // Inject a bogus entry that will fail during flush (non-existent namespace/tag combo).
    backend
        .access_time_writer
        .as_ref()
        .unwrap()
        .record(
            &Namespace::new("nonexistent-namespace").unwrap(),
            &LinkKind::Tag(Tag::new("bogus").unwrap()),
        )
        .await;

    backend.flush_access_times().await;

    let raw1 = backend
        .read_link_reference(&namespace, &tag1)
        .await
        .unwrap();
    let raw2 = backend
        .read_link_reference(&namespace, &tag2)
        .await
        .unwrap();
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
async fn test_read_link_with_access_time_debounce_uses_cache() {
    let config = test_config();
    let mut cfg = config.clone();
    cfg.access_time_debounce_secs = 60;
    let cache = Arc::new(CacheEnum::Memory(CacheMemoryBackend::new()));
    let backend = cfg
        .to_backend(
            Some(ConditionalCapabilities {
                put_if_none_match: true,
                put_if_match: true,
                delete_if_match: false,
            }),
            Some(cache.clone()),
        )
        .unwrap();
    let namespace = Namespace::new("cache-debounce-hit-ns").unwrap();
    let digest =
        Digest::from_str("sha256:db01a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6")
            .unwrap();
    let tag = LinkKind::Tag(Tag::new("debounce-cached").unwrap());

    let ops = vec![LinkOperation::Create {
        link: tag.clone(),
        target: digest.clone(),
        referrer: None,
        media_type: None,
        descriptor: None,
    }];
    backend.update_links(&namespace, &ops).await.unwrap();

    let meta = backend
        .read_link_recording_access(&namespace, &tag)
        .await
        .unwrap();
    assert_eq!(meta.target, digest);

    // Delete the storage object to prove the next read must come from cache.
    let link_path = path_builder::link_path(&tag, &namespace);
    backend.store().delete(&link_path).await.unwrap();

    let meta = backend
        .read_link_recording_access(&namespace, &tag)
        .await
        .unwrap();
    assert_eq!(meta.target, digest);

    // Verify writer.record() was still called on cache hit.
    let writer = backend.access_time_writer.as_ref().unwrap();
    let pending = writer.pending.lock().await;
    assert!(
        !pending.is_empty(),
        "writer.record() should have been called even on cache hit"
    );
}
