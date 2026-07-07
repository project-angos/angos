use std::str::FromStr;

use angos_tx_engine::lock::{LockStrategy, S3LockConfig};

use super::test_config;
use crate::{
    oci::{Digest, Namespace, Tag},
    registry::metadata_store::{BlobIndexOperation, Error, LinkKind, LinkOperation},
};

#[tokio::test]
async fn test_blob_index_updates_multiple_digests() {
    let config = test_config();
    let backend = config.to_backend(false, None).unwrap();
    let namespace = Namespace::new("blob-index-multi-digest-test").unwrap();

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
            link: LinkKind::Tag(Tag::try_from(format!("tag-bim-{i}")).unwrap()),
            target: digest.clone(),
            referrer: None,
            media_type: None,
            descriptor: None,
        })
        .collect();

    backend.update_links(&namespace, &ops).await.unwrap();

    for (i, digest) in digests.iter().enumerate() {
        let blob_index = backend.read_blob_index(digest).await.unwrap();
        let ns_links = blob_index.namespace.get(&namespace).unwrap();
        let expected_link = LinkKind::Tag(Tag::try_from(format!("tag-bim-{i}")).unwrap());
        assert!(
            ns_links.contains(&expected_link),
            "Blob index for digest {digest} should contain {expected_link}"
        );
    }
}

#[tokio::test]
async fn test_tracked_link_creates_with_referrers() {
    let config = test_config();
    let backend = config.to_backend(false, None).unwrap();
    let namespace = Namespace::new("tracked-creates-referrer-test").unwrap();

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

    backend.update_links(&namespace, &ops).await.unwrap();

    for layer_digest in &layer_digests {
        let link = LinkKind::Layer(layer_digest.clone());
        let meta = backend
            .read_link_reference(&namespace, &link)
            .await
            .unwrap();
        assert_eq!(meta.target, *layer_digest);
        assert!(
            meta.referenced_by.contains(&referrer_digest),
            "Layer link {link} should have referrer {referrer_digest}"
        );
    }

    let config_link = LinkKind::Config(config_digest.clone());
    let meta = backend
        .read_link_reference(&namespace, &config_link)
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
    let backend = config.to_backend(false, None).unwrap();
    let namespace = Namespace::new("tracked-deletes-referrer-test").unwrap();

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
    backend.update_links(&namespace, &create_ops).await.unwrap();

    for d in &layer_digests {
        let link = LinkKind::Layer(d.clone());
        let meta = backend
            .read_link_reference(&namespace, &link)
            .await
            .unwrap();
        assert_eq!(meta.target, *d);
    }

    let delete_ops: Vec<LinkOperation> = layer_digests
        .iter()
        .map(|d| LinkOperation::Delete {
            link: LinkKind::Layer(d.clone()),
            referrer: Some(referrer_digest.clone()),
        })
        .collect();
    backend.update_links(&namespace, &delete_ops).await.unwrap();

    for d in &layer_digests {
        let link = LinkKind::Layer(d.clone());
        let result = backend.read_link_reference(&namespace, &link).await;
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
    let backend = config.to_backend(false, None).unwrap();
    let namespace = Namespace::new("mixed-ops-across-digests-test").unwrap();

    let digest_keep =
        Digest::from_str("sha256:dd00000000000000000000000000000000000000000000000000000000000001")
            .unwrap();
    let digest_remove =
        Digest::from_str("sha256:dd00000000000000000000000000000000000000000000000000000000000002")
            .unwrap();
    let digest_add =
        Digest::from_str("sha256:dd00000000000000000000000000000000000000000000000000000000000003")
            .unwrap();

    let setup_ops = vec![
        LinkOperation::Create {
            link: LinkKind::Tag(Tag::new("keep-tag").unwrap()),
            target: digest_keep.clone(),
            referrer: None,
            media_type: None,
            descriptor: None,
        },
        LinkOperation::Create {
            link: LinkKind::Tag(Tag::new("remove-tag").unwrap()),
            target: digest_remove.clone(),
            referrer: None,
            media_type: None,
            descriptor: None,
        },
    ];
    backend.update_links(&namespace, &setup_ops).await.unwrap();

    let mixed_ops = vec![
        LinkOperation::Delete {
            link: LinkKind::Tag(Tag::new("remove-tag").unwrap()),
            referrer: None,
        },
        LinkOperation::Create {
            link: LinkKind::Tag(Tag::new("new-tag").unwrap()),
            target: digest_add.clone(),
            referrer: None,
            media_type: None,
            descriptor: None,
        },
    ];
    backend.update_links(&namespace, &mixed_ops).await.unwrap();

    let keep_index = backend.read_blob_index(&digest_keep).await.unwrap();
    let keep_links = keep_index.namespace.get(&namespace).unwrap();
    assert!(keep_links.contains(&LinkKind::Tag(Tag::new("keep-tag").unwrap())));

    match backend.read_blob_index(&digest_remove).await {
        Ok(idx) => {
            let links = idx.namespace.get(&namespace);
            assert!(
                links.is_none()
                    || !links
                        .unwrap()
                        .contains(&LinkKind::Tag(Tag::new("remove-tag").unwrap())),
                "remove-tag should not be in blob index after delete"
            );
        }
        Err(Error::ReferenceNotFound) => {}
        Err(e) => panic!("Unexpected error reading blob index: {e}"),
    }

    let add_index = backend.read_blob_index(&digest_add).await.unwrap();
    let add_links = add_index.namespace.get(&namespace).unwrap();
    assert!(add_links.contains(&LinkKind::Tag(Tag::new("new-tag").unwrap())));

    let result = backend
        .read_link_reference(&namespace, &LinkKind::Tag(Tag::new("remove-tag").unwrap()))
        .await;
    assert!(matches!(result, Err(Error::ReferenceNotFound)));

    let new_meta = backend
        .read_link_reference(&namespace, &LinkKind::Tag(Tag::new("new-tag").unwrap()))
        .await
        .unwrap();
    assert_eq!(new_meta.target, digest_add);
}

#[tokio::test]
async fn test_has_blob_references_ignores_empty_cas_shards() {
    // CAS shard updates only run when the backend's coordinator is `Cas`,
    // which the constructor selects exclusively for `LockStrategy::S3` with
    // CAS-capable conditional caps.
    let mut config = test_config();
    config.lock_strategy = LockStrategy::S3(S3LockConfig::default());
    let backend = config.to_backend(true, None).unwrap();

    let digest =
        Digest::from_str("sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
            .unwrap();
    let link = LinkKind::Blob(digest.clone());
    let namespace = Namespace::new("empty-cas-shard").unwrap();

    backend
        .update_blob_index(
            &namespace,
            &digest,
            BlobIndexOperation::Insert(link.clone()),
        )
        .await
        .unwrap();
    backend
        .update_blob_index(&namespace, &digest, BlobIndexOperation::Remove(link))
        .await
        .unwrap();

    assert!(
        !backend.has_blob_references(&digest).await.unwrap(),
        "empty CAS shards must not keep blob data alive"
    );

    backend
        .store()
        .object_store()
        .delete_prefix(&config.connection.key_prefix)
        .await
        .unwrap();
}
