use std::{io::Cursor, slice};

use futures_util::TryStreamExt;
use http_body_util::BodyExt;
use serde_json::json;
use tokio::io::AsyncReadExt;
use tokio_util::io::StreamReader;

use super::*;
use crate::{
    command::server::request_ext::HeaderExt,
    oci::Namespace,
    registry::tests::{FSRegistryTestCase, backends},
};

fn create_test_manifest() -> (Vec<u8>, String) {
    let manifest = json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
        "config": {
            "mediaType": "application/vnd.docker.container.image.v1+json",
            "digest": "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            "size": 1234
        },
        "layers": [
            {
                "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
                "digest": "sha256:abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
                "size": 5678
            }
        ]
    });

    let content = serde_json::to_vec(&manifest).unwrap();
    let media_type = "application/vnd.docker.distribution.manifest.v2+json".to_string();
    (content, media_type)
}

fn create_test_manifest_with_subject() -> (Vec<u8>, String) {
    let manifest = json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
        "subject": {
            "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
            "digest": "sha256:9876543210fedcba9876543210fedcba9876543210fedcba9876543210fedcba",
            "size": 1234
        },
        "config": {
            "mediaType": "application/vnd.docker.container.image.v1+json",
            "digest": "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            "size": 1234
        },
        "layers": [
            {
                "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
                "digest": "sha256:abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
                "size": 5678
            }
        ]
    });

    let content = serde_json::to_vec(&manifest).unwrap();
    let media_type = "application/vnd.docker.distribution.manifest.v2+json".to_string();
    (content, media_type)
}

#[tokio::test]
async fn test_put_manifest() {
    for test_case in backends() {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo").unwrap();
        let tag = "latest";
        let (content, media_type) = create_test_manifest();

        // Test put manifest with tag
        let response = registry
            .put_manifest(
                namespace,
                &Reference::Tag(tag.to_string()),
                Some(&media_type),
                &content,
            )
            .await
            .unwrap();

        // Verify manifest was stored
        let stored_manifest = registry
            .get_manifest(
                registry.get_repository_for_namespace(namespace).unwrap(),
                slice::from_ref(&media_type),
                namespace,
                Reference::Tag(tag.to_string()),
                false,
            )
            .await
            .unwrap();

        assert_eq!(stored_manifest.content, content);
        assert_eq!(stored_manifest.media_type.unwrap(), media_type);
        assert_eq!(stored_manifest.digest, response.digest);

        // Test put manifest with digest
        let digest = response.digest.clone();
        let response = registry
            .put_manifest(
                namespace,
                &Reference::Digest(digest.clone()),
                Some(&media_type),
                &content,
            )
            .await
            .unwrap();

        assert_eq!(response.digest, digest);
    }
}

#[tokio::test]
async fn test_get_manifest() {
    for test_case in backends() {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo").unwrap();
        let tag = "latest";
        let (content, media_type) = create_test_manifest();

        // Put manifest first
        let response = registry
            .put_manifest(
                namespace,
                &Reference::Tag(tag.to_string()),
                Some(&media_type),
                &content,
            )
            .await
            .unwrap();

        // Test get manifest by tag
        let manifest = registry
            .get_manifest(
                registry.get_repository_for_namespace(namespace).unwrap(),
                slice::from_ref(&media_type),
                namespace,
                Reference::Tag(tag.to_string()),
                false,
            )
            .await
            .unwrap();

        assert_eq!(manifest.content, content);
        assert_eq!(manifest.media_type.unwrap(), media_type);
        assert_eq!(manifest.digest, response.digest);

        // Test get manifest by digest
        let manifest = registry
            .get_manifest(
                registry.get_repository_for_namespace(namespace).unwrap(),
                slice::from_ref(&media_type),
                namespace,
                Reference::Digest(response.digest.clone()),
                false,
            )
            .await
            .unwrap();

        assert_eq!(manifest.content, content);
        assert_eq!(manifest.media_type.unwrap(), media_type);
        assert_eq!(manifest.digest, response.digest);
    }
}

#[tokio::test]
async fn test_head_manifest() {
    for test_case in backends() {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo").unwrap();
        let tag = "latest";
        let (content, media_type) = create_test_manifest();

        // Put manifest first
        let response = registry
            .put_manifest(
                namespace,
                &Reference::Tag(tag.to_string()),
                Some(&media_type),
                &content,
            )
            .await
            .unwrap();

        // Test head manifest by tag
        let manifest = registry
            .head_manifest(
                registry.get_repository_for_namespace(namespace).unwrap(),
                slice::from_ref(&media_type),
                namespace,
                Reference::Tag(tag.to_string()),
                false,
            )
            .await
            .unwrap();

        assert_eq!(manifest.media_type.unwrap(), media_type);
        assert_eq!(manifest.digest, response.digest);
        assert_eq!(manifest.size, content.len());

        // Test head manifest by digest
        let manifest = registry
            .head_manifest(
                registry.get_repository_for_namespace(namespace).unwrap(),
                slice::from_ref(&media_type),
                namespace,
                Reference::Digest(response.digest.clone()),
                false,
            )
            .await
            .unwrap();

        assert_eq!(manifest.media_type.unwrap(), media_type);
        assert_eq!(manifest.digest, response.digest);
        assert_eq!(manifest.size, content.len());
    }
}

#[tokio::test]
async fn test_delete_manifest() {
    for test_case in backends() {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo").unwrap();
        let tag = "latest";
        let (content, media_type) = create_test_manifest();

        // Put manifest first
        let response = registry
            .put_manifest(
                namespace,
                &Reference::Tag(tag.to_string()),
                Some(&media_type),
                &content,
            )
            .await
            .unwrap();

        // Test delete manifest by tag
        registry
            .delete_manifest(namespace, &Reference::Tag(tag.to_string()))
            .await
            .unwrap();

        // Verify tag is deleted
        assert!(
            registry
                .get_manifest(
                    registry.get_repository_for_namespace(namespace).unwrap(),
                    slice::from_ref(&media_type),
                    namespace,
                    Reference::Tag(tag.to_string()),
                    false,
                )
                .await
                .is_err()
        );

        // Test delete manifest by digest
        registry
            .delete_manifest(namespace, &Reference::Digest(response.digest.clone()))
            .await
            .unwrap();

        // Verify digest is deleted
        assert!(
            registry
                .get_manifest(
                    registry.get_repository_for_namespace(namespace).unwrap(),
                    slice::from_ref(&media_type),
                    namespace,
                    Reference::Digest(response.digest),
                    false,
                )
                .await
                .is_err()
        );
    }
}

#[test]
fn test_parse_manifest_digests() {
    // Test regular manifest
    let (content, media_type) = create_test_manifest();
    let digests = parse_manifest_digests(&content, Some(&media_type)).unwrap();

    assert!(digests.subject.is_none());
    assert_eq!(
        digests.config.unwrap().to_string(),
        "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
    );
    assert_eq!(
        digests.layers[0].to_string(),
        "sha256:abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
    );

    // Test manifest with subject
    let (content, media_type) = create_test_manifest_with_subject();
    let digests = parse_manifest_digests(&content, Some(&media_type)).unwrap();

    assert_eq!(
        digests.subject.unwrap().to_string(),
        "sha256:9876543210fedcba9876543210fedcba9876543210fedcba9876543210fedcba"
    );
    assert_eq!(
        digests.config.unwrap().to_string(),
        "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
    );
    assert_eq!(
        digests.layers[0].to_string(),
        "sha256:abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
    );

    // Test media type mismatch
    let wrong_media_type = "application/wrong.media.type".to_string();
    assert!(parse_manifest_digests(&content, Some(&wrong_media_type)).is_err());
}

#[tokio::test]
async fn test_handle_head_manifest() {
    for test_case in backends() {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo").unwrap();
        let tag = "latest";
        let (content, media_type) = create_test_manifest();

        // Put manifest first
        let put_response = registry
            .put_manifest(
                namespace,
                &Reference::Tag(tag.to_string()),
                Some(&media_type),
                &content,
            )
            .await
            .unwrap();

        let mime_types = Vec::new();

        let reference = Reference::Tag(tag.to_string());

        let response = registry
            .handle_head_manifest(namespace, reference, &mime_types, false)
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let (parts, _) = response.into_parts();

        assert_eq!(
            parts.get_header(DOCKER_CONTENT_DIGEST),
            Some(put_response.digest.to_string())
        );
        assert_eq!(
            parts.get_header(CONTENT_LENGTH),
            Some(content.len().to_string())
        );
        assert_eq!(parts.get_header(CONTENT_TYPE), Some(media_type));
    }
}

#[tokio::test]
async fn test_handle_get_manifest() {
    for test_case in backends() {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo").unwrap();
        let tag = "latest";
        let (content, media_type) = create_test_manifest();

        // Put manifest first
        let put_response = registry
            .put_manifest(
                namespace,
                &Reference::Tag(tag.to_string()),
                Some(&media_type),
                &content,
            )
            .await
            .unwrap();

        let reference = Reference::Tag(tag.to_string());
        let accepted_types = Vec::new();

        let response = registry
            .handle_get_manifest(namespace, reference, &accepted_types, false)
            .await
            .unwrap();

        let status = response.status();
        let (parts, body) = response.into_parts();

        assert_eq!(
            parts.get_header(DOCKER_CONTENT_DIGEST),
            Some(put_response.digest.to_string())
        );

        if status == StatusCode::TEMPORARY_REDIRECT {
            assert!(parts.headers.get(LOCATION).is_some());
            assert_eq!(parts.get_header(CONTENT_TYPE), Some(media_type));
        } else {
            assert_eq!(parts.status, StatusCode::OK);
            assert_eq!(parts.get_header(CONTENT_TYPE), Some(media_type));

            let stream = body.into_data_stream().map_err(std::io::Error::other);
            let mut reader = StreamReader::new(stream);
            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).await.unwrap();
            assert_eq!(buf, content);
        }
    }
}

#[tokio::test]
async fn test_handle_put_manifest() {
    for test_case in backends() {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo").unwrap();
        let tag = "latest";
        let (content, media_type) = create_test_manifest();

        let reference = Reference::Tag(tag.to_string());

        let manifest_stream = Cursor::new(content.clone());

        let response = registry
            .handle_put_manifest(namespace, reference, media_type.clone(), manifest_stream)
            .await
            .expect("put manifest failed");

        assert_eq!(response.status(), StatusCode::CREATED);
        let (parts, _) = response.into_parts();

        let digest = parts.get_header(DOCKER_CONTENT_DIGEST).unwrap();

        assert_eq!(
            parts.get_header(LOCATION),
            Some(format!("/v2/{namespace}/manifests/{tag}"))
        );

        // Verify manifest was stored
        let repository = registry
            .get_repository_for_namespace(namespace)
            .expect("get repository failed");
        let stored_manifest = registry
            .get_manifest(
                repository,
                slice::from_ref(&media_type),
                namespace,
                Reference::Tag(tag.to_string()),
                false,
            )
            .await
            .expect("get manifest failed");

        assert_eq!(stored_manifest.content, content);
        assert_eq!(stored_manifest.media_type.unwrap(), media_type);
        assert_eq!(stored_manifest.digest.to_string(), digest);
    }
}

#[tokio::test]
async fn test_handle_delete_manifest() {
    for test_case in backends() {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo").unwrap();
        let tag = "latest";
        let (content, media_type) = create_test_manifest();

        // Put manifest first
        let _put_response = registry
            .put_manifest(
                namespace,
                &Reference::Tag(tag.to_string()),
                Some(&media_type),
                &content,
            )
            .await
            .unwrap();

        let reference = Reference::Tag(tag.to_string());

        let response = registry
            .handle_delete_manifest(namespace, reference)
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::ACCEPTED);

        // Verify manifest is deleted
        assert!(
            registry
                .get_manifest(
                    registry.get_repository_for_namespace(namespace).unwrap(),
                    slice::from_ref(&media_type),
                    namespace,
                    Reference::Tag(tag.to_string()),
                    false,
                )
                .await
                .is_err()
        );
    }
}

async fn test_pull_through_cache_optimization_impl(test_case: &mut FSRegistryTestCase) {
    let namespace = &Namespace::new("test-repo").unwrap();
    let (content, media_type) = create_test_manifest();

    let repositories = crate::registry::test_utils::create_test_repositories();

    test_case.set_repositories(repositories);
    let registry = test_case.registry();

    let immutable_tag = "v1.0.0";
    let put_result = registry
        .put_manifest(
            namespace,
            &Reference::Tag(immutable_tag.to_string()),
            Some(&media_type),
            &content,
        )
        .await;
    assert!(put_result.is_ok());

    let repository = registry.get_repository_for_namespace(namespace).unwrap();

    let get_result = registry
        .get_manifest(
            repository,
            slice::from_ref(&media_type),
            namespace,
            Reference::Tag(immutable_tag.to_string()),
            false,
        )
        .await;
    assert!(get_result.is_ok());

    let mutable_tag = "latest";
    let _ = registry
        .put_manifest(
            namespace,
            &Reference::Tag(mutable_tag.to_string()),
            Some(&media_type),
            &content,
        )
        .await
        .unwrap();

    let get_mutable = registry
        .get_manifest(
            repository,
            slice::from_ref(&media_type),
            namespace,
            Reference::Tag(mutable_tag.to_string()),
            false,
        )
        .await;
    assert!(get_mutable.is_ok());
}

#[tokio::test]
async fn test_pull_through_cache_optimization_fs() {
    let mut t = FSRegistryTestCase::new();
    test_pull_through_cache_optimization_impl(&mut t).await;
}

#[tokio::test]
async fn test_delete_manifest_by_digest_removes_multiple_tags() {
    for test_case in backends() {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo/delete-multi-tags").unwrap();
        let (content, media_type) = create_test_manifest();

        let response = registry
            .put_manifest(
                namespace,
                &Reference::Tag("latest".to_string()),
                Some(&media_type),
                &content,
            )
            .await
            .unwrap();

        registry
            .put_manifest(
                namespace,
                &Reference::Tag("v1.0".to_string()),
                Some(&media_type),
                &content,
            )
            .await
            .unwrap();

        registry
            .delete_manifest(namespace, &Reference::Digest(response.digest.clone()))
            .await
            .unwrap();

        let repository = registry.get_repository_for_namespace(namespace).unwrap();

        assert!(
            registry
                .get_manifest(
                    repository,
                    slice::from_ref(&media_type),
                    namespace,
                    Reference::Tag("latest".to_string()),
                    false,
                )
                .await
                .is_err()
        );

        assert!(
            registry
                .get_manifest(
                    repository,
                    slice::from_ref(&media_type),
                    namespace,
                    Reference::Tag("v1.0".to_string()),
                    false,
                )
                .await
                .is_err()
        );

        assert!(
            registry
                .get_manifest(
                    repository,
                    slice::from_ref(&media_type),
                    namespace,
                    Reference::Digest(response.digest.clone()),
                    false,
                )
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn test_delete_manifest_by_digest_preserves_unrelated_tags() {
    for test_case in backends() {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo/delete-preserve").unwrap();
        let (content_a, media_type_a) = create_test_manifest();
        let (content_b, media_type_b) = create_test_manifest_with_subject();

        let response_a = registry
            .put_manifest(
                namespace,
                &Reference::Tag("v1.0".to_string()),
                Some(&media_type_a),
                &content_a,
            )
            .await
            .unwrap();

        registry
            .put_manifest(
                namespace,
                &Reference::Tag("v1.1".to_string()),
                Some(&media_type_a),
                &content_a,
            )
            .await
            .unwrap();

        let response_b = registry
            .put_manifest(
                namespace,
                &Reference::Tag("v2.0".to_string()),
                Some(&media_type_b),
                &content_b,
            )
            .await
            .unwrap();

        registry
            .delete_manifest(namespace, &Reference::Digest(response_a.digest.clone()))
            .await
            .unwrap();

        let repository = registry.get_repository_for_namespace(namespace).unwrap();

        assert!(
            registry
                .get_manifest(
                    repository,
                    slice::from_ref(&media_type_a),
                    namespace,
                    Reference::Tag("v1.0".to_string()),
                    false,
                )
                .await
                .is_err()
        );

        assert!(
            registry
                .get_manifest(
                    repository,
                    slice::from_ref(&media_type_a),
                    namespace,
                    Reference::Tag("v1.1".to_string()),
                    false,
                )
                .await
                .is_err()
        );

        let manifest_b = registry
            .get_manifest(
                repository,
                slice::from_ref(&media_type_b),
                namespace,
                Reference::Tag("v2.0".to_string()),
                false,
            )
            .await
            .unwrap();

        assert_eq!(manifest_b.digest, response_b.digest);
    }
}

#[tokio::test]
async fn test_delete_manifest_with_many_tags() {
    for test_case in backends() {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo/delete-many-tags").unwrap();
        let (content_a, media_type_a) = create_test_manifest();
        let (content_b, media_type_b) = create_test_manifest_with_subject();

        let response_a = registry
            .put_manifest(
                namespace,
                &Reference::Tag("tag-0".to_string()),
                Some(&media_type_a),
                &content_a,
            )
            .await
            .unwrap();

        for i in 1..20 {
            registry
                .put_manifest(
                    namespace,
                    &Reference::Tag(format!("tag-{i}")),
                    Some(&media_type_a),
                    &content_a,
                )
                .await
                .unwrap();
        }

        for i in 0..20 {
            registry
                .put_manifest(
                    namespace,
                    &Reference::Tag(format!("other-{i}")),
                    Some(&media_type_b),
                    &content_b,
                )
                .await
                .unwrap();
        }

        registry
            .delete_manifest(namespace, &Reference::Digest(response_a.digest.clone()))
            .await
            .unwrap();

        let repository = registry.get_repository_for_namespace(namespace).unwrap();

        for i in 0..20 {
            assert!(
                registry
                    .get_manifest(
                        repository,
                        slice::from_ref(&media_type_a),
                        namespace,
                        Reference::Tag(format!("tag-{i}")),
                        false,
                    )
                    .await
                    .is_err(),
                "tag-{i} should have been deleted"
            );
        }

        for i in 0..20 {
            assert!(
                registry
                    .get_manifest(
                        repository,
                        slice::from_ref(&media_type_b),
                        namespace,
                        Reference::Tag(format!("other-{i}")),
                        false,
                    )
                    .await
                    .is_ok(),
                "other-{i} should still exist"
            );
        }

        let (tags, _) = registry
            .metadata_store
            .list_tags(namespace, 100, None)
            .await
            .unwrap();
        assert_eq!(tags.len(), 20, "expected exactly 20 remaining tags");
    }
}

#[tokio::test]
async fn test_put_manifest_stores_media_type() {
    for test_case in backends() {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo/media-type-store").unwrap();
        let tag = "latest";
        let (content, media_type) = create_test_manifest();

        let response = registry
            .put_manifest(
                namespace,
                &Reference::Tag(tag.to_string()),
                Some(&media_type),
                &content,
            )
            .await
            .unwrap();

        let digest_link = LinkKind::Digest(response.digest.clone());
        let link_meta = registry
            .metadata_store
            .read_link(namespace, &digest_link, false)
            .await
            .unwrap();
        assert_eq!(
            link_meta.media_type,
            Some(media_type.clone()),
            "Digest link should have media_type stored"
        );

        let tag_link = LinkKind::Tag(tag.to_string());
        let tag_meta = registry
            .metadata_store
            .read_link(namespace, &tag_link, false)
            .await
            .unwrap();
        assert_eq!(
            tag_meta.media_type,
            Some(media_type),
            "Tag link should have media_type stored"
        );
    }
}

#[tokio::test]
async fn test_head_manifest_returns_correct_media_type() {
    for test_case in backends() {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo/head-media-type").unwrap();
        let (content, media_type) = create_test_manifest();

        let put_response = registry
            .put_manifest(
                namespace,
                &Reference::Tag("latest".to_string()),
                Some(&media_type),
                &content,
            )
            .await
            .unwrap();

        let repository = registry.get_repository_for_namespace(namespace).unwrap();

        let head = registry
            .head_manifest(
                repository,
                slice::from_ref(&media_type),
                namespace,
                Reference::Tag("latest".to_string()),
                false,
            )
            .await
            .unwrap();

        assert_eq!(head.media_type, Some(media_type.clone()));
        assert_eq!(head.digest, put_response.digest);
        assert_eq!(head.size, content.len());

        let head_by_digest = registry
            .head_manifest(
                repository,
                slice::from_ref(&media_type),
                namespace,
                Reference::Digest(put_response.digest.clone()),
                false,
            )
            .await
            .unwrap();

        assert_eq!(head_by_digest.media_type, Some(media_type));
        assert_eq!(head_by_digest.digest, put_response.digest);
        assert_eq!(head_by_digest.size, content.len());
    }
}

#[tokio::test]
async fn test_head_manifest_fallback_without_media_type() {
    for test_case in backends() {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo/head-fallback").unwrap();
        let (content, media_type) = create_test_manifest();

        let digest = registry.blob_store.create_blob(&content).await.unwrap();

        let mut tx = registry.metadata_store.begin_transaction(namespace);
        tx.create_link(&LinkKind::Digest(digest.clone()), &digest);
        tx.create_link(&LinkKind::Tag("latest".to_string()), &digest);
        tx.commit().await.unwrap();

        let link_meta = registry
            .metadata_store
            .read_link(namespace, &LinkKind::Digest(digest.clone()), false)
            .await
            .unwrap();
        assert_eq!(
            link_meta.media_type, None,
            "Link created without media_type should have None"
        );

        let repository = registry.get_repository_for_namespace(namespace).unwrap();

        let head = registry
            .head_manifest(
                repository,
                slice::from_ref(&media_type),
                namespace,
                Reference::Tag("latest".to_string()),
                false,
            )
            .await
            .unwrap();

        assert_eq!(
            head.media_type,
            Some(media_type),
            "HEAD should fall back to reading blob when media_type not in link"
        );
        assert_eq!(head.digest, digest);
        assert_eq!(head.size, content.len());
    }
}

#[tokio::test]
async fn test_delete_manifest_no_tags_by_digest() {
    for test_case in backends() {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo/delete-no-tags").unwrap();
        let (content, media_type) = create_test_manifest();

        let response = registry
            .put_manifest(
                namespace,
                &Reference::Tag("temp".to_string()),
                Some(&media_type),
                &content,
            )
            .await
            .unwrap();

        registry
            .delete_manifest(namespace, &Reference::Tag("temp".to_string()))
            .await
            .unwrap();

        registry
            .delete_manifest(namespace, &Reference::Digest(response.digest.clone()))
            .await
            .unwrap();

        let repository = registry.get_repository_for_namespace(namespace).unwrap();

        assert!(
            registry
                .get_manifest(
                    repository,
                    slice::from_ref(&media_type),
                    namespace,
                    Reference::Digest(response.digest.clone()),
                    false,
                )
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn test_put_manifest_stores_media_type_in_links() {
    for test_case in backends() {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo/media-type-links").unwrap();
        let (content, media_type) = create_test_manifest();

        let response = registry
            .put_manifest(
                namespace,
                &Reference::Tag("latest".to_string()),
                Some(&media_type),
                &content,
            )
            .await
            .unwrap();

        let digest_link = registry
            .metadata_store
            .read_link(namespace, &LinkKind::Digest(response.digest.clone()), false)
            .await
            .unwrap();
        assert_eq!(
            digest_link.media_type,
            Some(media_type.clone()),
            "Digest link should have media_type stored"
        );

        let tag_link = registry
            .metadata_store
            .read_link(namespace, &LinkKind::Tag("latest".to_string()), false)
            .await
            .unwrap();
        assert_eq!(
            tag_link.media_type,
            Some(media_type.clone()),
            "Tag link should have media_type stored"
        );
    }
}

#[tokio::test]
async fn test_head_local_manifest_uses_metadata_media_type() {
    for test_case in backends() {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo/head-optimized").unwrap();
        let (content, media_type) = create_test_manifest();

        let response = registry
            .put_manifest(
                namespace,
                &Reference::Tag("v1.0".to_string()),
                Some(&media_type),
                &content,
            )
            .await
            .unwrap();

        let head = registry
            .head_manifest(
                registry.get_repository_for_namespace(namespace).unwrap(),
                slice::from_ref(&media_type),
                namespace,
                Reference::Tag("v1.0".to_string()),
                false,
            )
            .await
            .unwrap();

        assert_eq!(head.media_type, Some(media_type.clone()));
        assert_eq!(head.digest, response.digest);
        assert_eq!(head.size, content.len());

        let head = registry
            .head_manifest(
                registry.get_repository_for_namespace(namespace).unwrap(),
                slice::from_ref(&media_type),
                namespace,
                Reference::Digest(response.digest.clone()),
                false,
            )
            .await
            .unwrap();

        assert_eq!(head.media_type, Some(media_type.clone()));
        assert_eq!(head.digest, response.digest);
        assert_eq!(head.size, content.len());
    }
}

#[tokio::test]
async fn test_put_manifest_without_content_type_stores_manifest_media_type() {
    for test_case in backends() {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo/no-content-type").unwrap();
        let (content, _media_type) = create_test_manifest();

        let response = registry
            .put_manifest(
                namespace,
                &Reference::Tag("latest".to_string()),
                None,
                &content,
            )
            .await
            .unwrap();

        let digest_link = registry
            .metadata_store
            .read_link(namespace, &LinkKind::Digest(response.digest.clone()), false)
            .await
            .unwrap();

        assert_eq!(
            digest_link.media_type,
            Some("application/vnd.docker.distribution.manifest.v2+json".to_string()),
            "Digest link should have media_type from manifest body"
        );
    }
}

#[tokio::test]
async fn test_handle_get_manifest_redirect_includes_content_type() {
    for test_case in backends() {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo/redirect-ct").unwrap();
        let (content, media_type) = create_test_manifest();

        registry
            .put_manifest(
                namespace,
                &Reference::Tag("latest".to_string()),
                Some(&media_type),
                &content,
            )
            .await
            .unwrap();

        let response = registry
            .handle_get_manifest(
                namespace,
                Reference::Tag("latest".to_string()),
                slice::from_ref(&media_type),
                false,
            )
            .await
            .unwrap();

        if response.status() == StatusCode::TEMPORARY_REDIRECT {
            assert!(
                response.headers().contains_key(LOCATION),
                "Redirect response should have Location header"
            );
            assert_eq!(
                response
                    .headers()
                    .get(CONTENT_TYPE)
                    .map(|v| v.to_str().unwrap()),
                Some(media_type.as_str()),
                "Redirect response should include Content-Type from stored media_type"
            );
            assert!(
                response.headers().contains_key(DOCKER_CONTENT_DIGEST),
                "Redirect response should have Docker-Content-Digest header"
            );
        } else {
            assert_eq!(response.status(), StatusCode::OK);
        }
    }
}

#[tokio::test]
async fn test_handle_get_manifest_redirect_fallback_without_media_type() {
    for test_case in backends() {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo/redirect-fallback").unwrap();
        let (content, media_type) = create_test_manifest();

        let digest = registry.blob_store.create_blob(&content).await.unwrap();

        let mut tx = registry.metadata_store.begin_transaction(namespace);
        tx.create_link(&LinkKind::Digest(digest.clone()), &digest);
        tx.create_link(&LinkKind::Tag("latest".to_string()), &digest);
        tx.commit().await.unwrap();

        let response = registry
            .handle_get_manifest(
                namespace,
                Reference::Tag("latest".to_string()),
                slice::from_ref(&media_type),
                false,
            )
            .await
            .unwrap();

        if response.status() == StatusCode::TEMPORARY_REDIRECT {
            assert!(
                response.headers().contains_key(LOCATION),
                "Redirect should have Location header"
            );
            assert_eq!(
                response
                    .headers()
                    .get(CONTENT_TYPE)
                    .map(|v| v.to_str().unwrap()),
                Some(media_type.as_str()),
                "Redirect should still include Content-Type via fallback"
            );
        } else {
            assert_eq!(response.status(), StatusCode::OK);
        }
    }
}

#[tokio::test]
async fn test_handle_get_manifest_no_redirect_returns_body() {
    for test_case in backends() {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo/no-redirect").unwrap();
        let (content, media_type) = create_test_manifest();

        registry
            .put_manifest(
                namespace,
                &Reference::Tag("latest".to_string()),
                Some(&media_type),
                &content,
            )
            .await
            .unwrap();

        let response = registry
            .handle_get_manifest(
                namespace,
                Reference::Tag("latest".to_string()),
                slice::from_ref(&media_type),
                false,
            )
            .await
            .unwrap();

        if response.status() == StatusCode::OK {
            assert_eq!(
                response
                    .headers()
                    .get(CONTENT_TYPE)
                    .map(|v| v.to_str().unwrap()),
                Some(media_type.as_str()),
                "GET response should include Content-Type header"
            );
            assert!(
                response.headers().contains_key(DOCKER_CONTENT_DIGEST),
                "GET response should include Docker-Content-Digest header"
            );
        }
    }
}
