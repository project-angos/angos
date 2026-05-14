use std::{collections::HashMap, io::Cursor, slice};

use futures_util::future::join_all;
use hyper::header::{CONTENT_LENGTH, CONTENT_TYPE, LOCATION};
use serde_json::json;
use uuid::Uuid;

use super::{parse::manifest_meta_from_body, *};
use crate::{
    oci::Namespace,
    registry::{
        Error, Registry,
        metadata_store::link_kind::LinkKind,
        test_utils::{FSRegistryTestCase, RegistryTestCase, backends},
    },
    util::sha256,
};

fn header_digest(headers: &HashMap<&'static str, String>) -> Digest {
    headers[DOCKER_CONTENT_DIGEST].parse().unwrap()
}

const IMAGE_MANIFEST_MEDIA_TYPE: &str = "application/vnd.docker.distribution.manifest.v2+json";
const CONFIG_MEDIA_TYPE: &str = "application/vnd.docker.container.image.v1+json";
const LAYER_MEDIA_TYPE: &str = "application/vnd.docker.image.rootfs.diff.tar.gzip";
const MISSING_SUBJECT_DIGEST: &str =
    "sha256:9876543210fedcba9876543210fedcba9876543210fedcba9876543210fedcba";

fn create_raw_test_manifest() -> (Vec<u8>, String) {
    let manifest = json!({
        "schemaVersion": 2,
        "mediaType": IMAGE_MANIFEST_MEDIA_TYPE,
        "config": {
            "mediaType": CONFIG_MEDIA_TYPE,
            "digest": "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            "size": 1234
        },
        "layers": [
            {
                "mediaType": LAYER_MEDIA_TYPE,
                "digest": "sha256:abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
                "size": 5678
            }
        ]
    });

    let content = serde_json::to_vec(&manifest).unwrap();
    let media_type = IMAGE_MANIFEST_MEDIA_TYPE.to_string();
    (content, media_type)
}

fn create_raw_test_manifest_with_subject() -> (Vec<u8>, String) {
    let manifest = json!({
        "schemaVersion": 2,
        "mediaType": IMAGE_MANIFEST_MEDIA_TYPE,
        "subject": {
            "mediaType": IMAGE_MANIFEST_MEDIA_TYPE,
            "digest": MISSING_SUBJECT_DIGEST,
            "size": 1234
        },
        "config": {
            "mediaType": CONFIG_MEDIA_TYPE,
            "digest": "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            "size": 1234
        },
        "layers": [
            {
                "mediaType": LAYER_MEDIA_TYPE,
                "digest": "sha256:abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
                "size": 5678
            }
        ]
    });

    let content = serde_json::to_vec(&manifest).unwrap();
    let media_type = IMAGE_MANIFEST_MEDIA_TYPE.to_string();
    (content, media_type)
}

fn manifest_with_references(
    config_digest: &Digest,
    config_size: usize,
    layer_digest: &Digest,
    layer_size: usize,
) -> (Vec<u8>, String) {
    let manifest = json!({
        "schemaVersion": 2,
        "mediaType": IMAGE_MANIFEST_MEDIA_TYPE,
        "config": {
            "mediaType": CONFIG_MEDIA_TYPE,
            "digest": config_digest,
            "size": config_size
        },
        "layers": [
            {
                "mediaType": LAYER_MEDIA_TYPE,
                "digest": layer_digest,
                "size": layer_size
            }
        ]
    });

    let content = serde_json::to_vec(&manifest).unwrap();
    (content, IMAGE_MANIFEST_MEDIA_TYPE.to_string())
}

fn manifest_with_subject_and_references(
    config_digest: &Digest,
    config_size: usize,
    layer_digest: &Digest,
    layer_size: usize,
) -> (Vec<u8>, String) {
    let manifest = json!({
        "schemaVersion": 2,
        "mediaType": IMAGE_MANIFEST_MEDIA_TYPE,
        "subject": {
            "mediaType": IMAGE_MANIFEST_MEDIA_TYPE,
            "digest": MISSING_SUBJECT_DIGEST,
            "size": 1234
        },
        "config": {
            "mediaType": CONFIG_MEDIA_TYPE,
            "digest": config_digest,
            "size": config_size
        },
        "layers": [
            {
                "mediaType": LAYER_MEDIA_TYPE,
                "digest": layer_digest,
                "size": layer_size
            }
        ]
    });

    let content = serde_json::to_vec(&manifest).unwrap();
    (content, IMAGE_MANIFEST_MEDIA_TYPE.to_string())
}

fn index_manifest_with_child(child_digest: &Digest) -> (Vec<u8>, String) {
    let media_type = "application/vnd.oci.image.index.v1+json".to_string();
    let manifest = json!({
        "schemaVersion": 2,
        "mediaType": media_type,
        "manifests": [
            {
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "digest": child_digest,
                "size": 512,
                "platform": { "architecture": "amd64", "os": "linux" }
            }
        ]
    });

    let content = serde_json::to_vec(&manifest).unwrap();
    (content, media_type)
}

async fn create_test_manifest(registry: &Registry, namespace: &Namespace) -> (Vec<u8>, String) {
    let config_content = br#"{"architecture":"amd64","os":"linux"}"#;
    let layer_content = b"test layer content";
    let config_digest = upload_blob(registry, namespace, config_content).await;
    let layer_digest = upload_blob(registry, namespace, layer_content).await;

    manifest_with_references(
        &config_digest,
        config_content.len(),
        &layer_digest,
        layer_content.len(),
    )
}

async fn create_test_manifest_with_subject(
    registry: &Registry,
    namespace: &Namespace,
) -> (Vec<u8>, String) {
    let config_content = br#"{"architecture":"amd64","os":"linux"}"#;
    let layer_content = b"test layer content";
    let config_digest = upload_blob(registry, namespace, config_content).await;
    let layer_digest = upload_blob(registry, namespace, layer_content).await;

    manifest_with_subject_and_references(
        &config_digest,
        config_content.len(),
        &layer_digest,
        layer_content.len(),
    )
}

async fn upload_blob(registry: &Registry, namespace: &Namespace, content: &[u8]) -> Digest {
    let session_id = Uuid::new_v4();
    registry
        .upload_store
        .create(namespace, &session_id.to_string())
        .await
        .unwrap();

    let body = content.to_vec();
    let digest = sha256::digest(&body);
    registry
        .complete_upload(
            None,
            namespace,
            session_id,
            &digest,
            body.len() as u64,
            Cursor::new(body),
        )
        .await
        .unwrap();
    digest
}

#[tokio::test]
async fn test_put_manifest() {
    for test_case in backends() {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo").unwrap();
        let tag = "latest";
        let (content, media_type) = create_test_manifest(registry, namespace).await;

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
        assert_eq!(stored_manifest.digest, header_digest(&response.headers));

        // Test put manifest with digest
        let digest = header_digest(&response.headers);
        let response = registry
            .put_manifest(
                namespace,
                &Reference::Digest(digest.clone()),
                Some(&media_type),
                &content,
            )
            .await
            .unwrap();

        assert_eq!(header_digest(&response.headers), digest);
        test_case.cleanup().await;
    }
}

#[tokio::test]
async fn put_manifest_rejects_missing_config_reference() {
    for test_case in backends() {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo/missing-config").unwrap();
        let missing_config = fixed_digest();
        let layer_content = b"existing layer";
        let layer_digest = upload_blob(registry, namespace, layer_content).await;
        let (content, media_type) =
            manifest_with_references(&missing_config, 256, &layer_digest, layer_content.len());

        let Err(err) = registry
            .put_manifest(
                namespace,
                &Reference::Tag("latest".to_string()),
                Some(&media_type),
                &content,
            )
            .await
        else {
            panic!("missing config must reject manifest push");
        };

        assert!(matches!(err, Error::ManifestBlobUnknown));
        let manifest_digest = sha256::digest(&content);
        assert!(registry.blob_store.read(&manifest_digest).await.is_err());
        assert!(
            registry
                .metadata_store
                .read_link(namespace, &LinkKind::Tag("latest".to_string()), false)
                .await
                .is_err()
        );
        test_case.cleanup().await;
    }
}

#[tokio::test]
async fn put_manifest_rejects_missing_layer_reference() {
    for test_case in backends() {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo/missing-layer").unwrap();
        let config_content = br#"{"architecture":"amd64","os":"linux"}"#;
        let config_digest = upload_blob(registry, namespace, config_content).await;
        let missing_layer = fixed_digest();
        let (content, media_type) =
            manifest_with_references(&config_digest, config_content.len(), &missing_layer, 512);

        let Err(err) = registry
            .put_manifest(
                namespace,
                &Reference::Tag("latest".to_string()),
                Some(&media_type),
                &content,
            )
            .await
        else {
            panic!("missing layer must reject manifest push");
        };

        assert!(matches!(err, Error::ManifestBlobUnknown));
        test_case.cleanup().await;
    }
}

#[tokio::test]
async fn put_manifest_rejects_missing_child_manifest_reference() {
    for test_case in backends() {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo/missing-child").unwrap();
        let missing_child = fixed_digest();
        let (content, media_type) = index_manifest_with_child(&missing_child);

        let Err(err) = registry
            .put_manifest(
                namespace,
                &Reference::Tag("latest".to_string()),
                Some(&media_type),
                &content,
            )
            .await
        else {
            panic!("missing child manifest must reject index push");
        };

        assert!(matches!(err, Error::ManifestBlobUnknown));
        test_case.cleanup().await;
    }
}

#[tokio::test]
async fn put_manifest_rejects_references_owned_by_another_namespace() {
    for test_case in backends() {
        let registry = test_case.registry();
        let owner_namespace = &Namespace::new("test-repo/source").unwrap();
        let target_namespace = &Namespace::new("test-repo/target").unwrap();
        let config_content = br#"{"architecture":"amd64","os":"linux"}"#;
        let layer_content = b"shared layer bytes";
        let config_digest = upload_blob(registry, owner_namespace, config_content).await;
        let layer_digest = upload_blob(registry, owner_namespace, layer_content).await;
        let (content, media_type) = manifest_with_references(
            &config_digest,
            config_content.len(),
            &layer_digest,
            layer_content.len(),
        );

        let Err(err) = registry
            .put_manifest(
                target_namespace,
                &Reference::Tag("latest".to_string()),
                Some(&media_type),
                &content,
            )
            .await
        else {
            panic!("cross-namespace references must reject manifest push");
        };

        assert!(matches!(err, Error::ManifestBlobUnknown));
        test_case.cleanup().await;
    }
}

#[tokio::test]
async fn put_manifest_allows_missing_subject_reference() {
    for test_case in backends() {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo/missing-subject").unwrap();
        let (content, media_type) = create_test_manifest_with_subject(registry, namespace).await;

        let response = registry
            .put_manifest(
                namespace,
                &Reference::Tag("latest".to_string()),
                Some(&media_type),
                &content,
            )
            .await
            .expect("missing subject should not reject manifest push");

        let subject = MISSING_SUBJECT_DIGEST.parse().unwrap();
        let digest = header_digest(&response.headers);
        let link = LinkKind::Referrer(subject, digest.clone());
        let metadata = registry
            .metadata_store
            .read_link(namespace, &link, false)
            .await
            .unwrap();
        assert_eq!(metadata.target, digest);
        test_case.cleanup().await;
    }
}

#[tokio::test]
async fn test_get_manifest() {
    for test_case in backends() {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo").unwrap();
        let tag = "latest";
        let (content, media_type) = create_test_manifest(registry, namespace).await;

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
        assert_eq!(manifest.digest, header_digest(&response.headers));

        // Test get manifest by digest
        let manifest = registry
            .get_manifest(
                registry.get_repository_for_namespace(namespace).unwrap(),
                slice::from_ref(&media_type),
                namespace,
                Reference::Digest(header_digest(&response.headers)),
                false,
            )
            .await
            .unwrap();

        assert_eq!(manifest.content, content);
        assert_eq!(manifest.media_type.unwrap(), media_type);
        assert_eq!(manifest.digest, header_digest(&response.headers));
        test_case.cleanup().await;
    }
}

#[tokio::test]
async fn test_head_manifest() {
    for test_case in backends() {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo").unwrap();
        let tag = "latest";
        let (content, media_type) = create_test_manifest(registry, namespace).await;

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

        assert_eq!(manifest.headers[CONTENT_TYPE.as_str()], media_type);
        assert_eq!(
            header_digest(&manifest.headers),
            header_digest(&response.headers)
        );
        assert_eq!(
            manifest.headers[CONTENT_LENGTH.as_str()],
            content.len().to_string()
        );

        // Test head manifest by digest
        let manifest = registry
            .head_manifest(
                registry.get_repository_for_namespace(namespace).unwrap(),
                slice::from_ref(&media_type),
                namespace,
                Reference::Digest(header_digest(&response.headers)),
                false,
            )
            .await
            .unwrap();

        assert_eq!(manifest.headers[CONTENT_TYPE.as_str()], media_type);
        assert_eq!(
            header_digest(&manifest.headers),
            header_digest(&response.headers)
        );
        assert_eq!(
            manifest.headers[CONTENT_LENGTH.as_str()],
            content.len().to_string()
        );
        test_case.cleanup().await;
    }
}

#[tokio::test]
async fn test_delete_manifest() {
    for test_case in backends() {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo").unwrap();
        let tag = "latest";
        let (content, media_type) = create_test_manifest(registry, namespace).await;

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
            .delete_manifest(None, namespace, &Reference::Tag(tag.to_string()))
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
            .delete_manifest(
                None,
                namespace,
                &Reference::Digest(header_digest(&response.headers)),
            )
            .await
            .unwrap();

        // Verify digest is deleted
        assert!(
            registry
                .get_manifest(
                    registry.get_repository_for_namespace(namespace).unwrap(),
                    slice::from_ref(&media_type),
                    namespace,
                    Reference::Digest(header_digest(&response.headers)),
                    false,
                )
                .await
                .is_err()
        );
        test_case.cleanup().await;
    }
}

#[tokio::test]
async fn delete_manifest_then_delete_uploaded_blobs() {
    for test_case in backends() {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo/zot-cleanup").unwrap();
        let layer_content = b"zot benchmark layer content";
        let config_content = br#"{"architecture":"amd64","os":"linux"}"#;
        let layer_digest = upload_blob(registry, namespace, layer_content).await;
        let config_digest = upload_blob(registry, namespace, config_content).await;
        let media_type = "application/vnd.oci.image.manifest.v1+json".to_string();
        let manifest = json!({
            "schemaVersion": 2,
            "mediaType": media_type,
            "config": {
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "digest": config_digest,
                "size": config_content.len()
            },
            "layers": [
                {
                    "mediaType": "application/vnd.oci.image.layer.v1.tar",
                    "digest": layer_digest,
                    "size": layer_content.len()
                }
            ]
        });
        let manifest_content = serde_json::to_vec(&manifest).unwrap();

        let response = registry
            .put_manifest(
                namespace,
                &Reference::Tag("latest".to_string()),
                Some(&media_type),
                &manifest_content,
            )
            .await
            .unwrap();
        let manifest_digest = header_digest(&response.headers);

        let manifest_blob_result = registry.delete_blob(namespace, &manifest_digest).await;
        assert!(matches!(manifest_blob_result, Err(Error::BlobReferenced)));

        let layer_result = registry.delete_blob(namespace, &layer_digest).await;
        assert!(matches!(layer_result, Err(Error::BlobReferenced)));

        registry
            .delete_manifest(None, namespace, &Reference::Digest(manifest_digest.clone()))
            .await
            .unwrap();

        assert!(registry.blob_store.read(&manifest_digest).await.is_err());
        assert_eq!(
            registry.blob_store.read(&layer_digest).await.unwrap(),
            layer_content
        );
        assert_eq!(
            registry.blob_store.read(&config_digest).await.unwrap(),
            config_content
        );

        registry
            .delete_blob(namespace, &layer_digest)
            .await
            .unwrap();
        registry
            .delete_blob(namespace, &config_digest)
            .await
            .unwrap();

        assert!(registry.blob_store.read(&layer_digest).await.is_err());
        assert!(registry.blob_store.read(&config_digest).await.is_err());
        test_case.cleanup().await;
    }
}

#[tokio::test]
async fn concurrent_same_digest_pushes_keep_upload_ownership() {
    let test_case = FSRegistryTestCase::new();
    let registry = test_case.registry();
    let media_type = "application/vnd.oci.image.manifest.v1+json".to_string();
    let layer_content = b"shared zot benchmark layer content";
    let config_content = br#"{"architecture":"amd64","os":"linux"}"#;

    let namespaces = (0..32)
        .map(|index| Namespace::new(&format!("test-repo/zot-{index}")).unwrap())
        .collect::<Vec<_>>();

    let pushes = namespaces.iter().map(|namespace| async {
        let layer_digest = upload_blob(registry, namespace, layer_content).await;
        let config_digest = upload_blob(registry, namespace, config_content).await;
        let manifest = json!({
            "schemaVersion": 2,
            "mediaType": media_type,
            "config": {
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "digest": config_digest,
                "size": config_content.len()
            },
            "layers": [
                {
                    "mediaType": "application/vnd.oci.image.layer.v1.tar",
                    "digest": layer_digest,
                    "size": layer_content.len()
                }
            ]
        });
        let manifest_content = serde_json::to_vec(&manifest).unwrap();
        registry
            .put_manifest(
                namespace,
                &Reference::Tag("latest".to_string()),
                Some(&media_type),
                &manifest_content,
            )
            .await
            .unwrap();
        layer_digest
    });

    let digests = join_all(pushes).await;
    let layer_digest = &digests[0];
    let blob_index = registry
        .metadata_store
        .read_blob_index(layer_digest)
        .await
        .unwrap();

    for namespace in namespaces {
        let links = blob_index.namespace.get(namespace.as_ref()).unwrap();
        assert!(links.contains(&LinkKind::Blob(layer_digest.clone())));
        assert!(links.contains(&LinkKind::Layer(layer_digest.clone())));
    }

    test_case.cleanup().await;
}

#[test]
fn test_parse_manifest_digests() {
    // Test regular manifest
    let (content, media_type) = create_raw_test_manifest();
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
    let (content, media_type) = create_raw_test_manifest_with_subject();
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
async fn test_malformed_json_yields_same_error_shape() {
    let malformed = b"not json";

    let parse_err = parse_manifest_digests(malformed, None)
        .err()
        .expect("expected Err from parse_manifest_digests");
    match parse_err {
        crate::registry::Error::ManifestInvalid(s) => {
            assert!(
                s.starts_with("invalid manifest JSON:"),
                "parse_manifest_digests error should start with 'invalid manifest JSON:'; got: {s}"
            );
        }
        other => panic!("expected ManifestInvalid from parse_manifest_digests, got {other:?}"),
    }

    // Parse failure is detected before any blob/metadata store access, so a
    // single backend exercises the full path.
    let test_case = FSRegistryTestCase::new();
    let registry = test_case.registry();
    let namespace = &Namespace::new("test-repo").unwrap();
    let put_err = registry
        .put_manifest(
            namespace,
            &Reference::Tag("latest".to_string()),
            None,
            malformed,
        )
        .await
        .err()
        .expect("expected Err from put_manifest");
    match put_err {
        crate::registry::Error::ManifestInvalid(s) => {
            assert!(
                s.starts_with("invalid manifest JSON:"),
                "put_manifest error should start with 'invalid manifest JSON:'; got: {s}"
            );
        }
        other => panic!("expected ManifestInvalid from put_manifest, got {other:?}"),
    }
}

#[test]
fn parse_manifest_digests_media_type_mismatch_returns_manifest_invalid() {
    let (content, _) = create_raw_test_manifest();
    let wrong_type = "application/vnd.oci.image.manifest.v1+json".to_string();

    let err = parse_manifest_digests(&content, Some(&wrong_type))
        .err()
        .expect("expected error on media type mismatch");
    assert!(
        matches!(err, crate::registry::Error::ManifestInvalid(_)),
        "expected ManifestInvalid for media type mismatch, got: {err:?}"
    );
}

#[tokio::test]
async fn accept_put_manifest_rejects_body_above_limit() {
    let test_case = FSRegistryTestCase::new();
    let registry = test_case.registry();
    let namespace = &Namespace::new("test-repo").unwrap();
    let body = vec![b' '; DEFAULT_MAX_MANIFEST_SIZE_BYTES + 1];

    let err = registry
        .accept_put_manifest(
            None,
            namespace,
            Reference::Tag("latest".to_string()),
            "application/vnd.oci.image.manifest.v1+json".to_string(),
            Cursor::new(body),
        )
        .await
        .err()
        .expect("expected oversized manifest upload to fail");

    assert!(matches!(
        err,
        Error::ManifestBodyTooLarge {
            limit: DEFAULT_MAX_MANIFEST_SIZE_BYTES
        }
    ));
}

#[test]
fn parse_manifest_digests_empty_layers_succeeds_with_empty_vec() {
    let body = serde_json::to_vec(&serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
        "config": {
            "mediaType": "application/vnd.docker.container.image.v1+json",
            "digest": "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            "size": 100
        },
        "layers": []
    }))
    .unwrap();

    let digests =
        parse_manifest_digests(&body, None).expect("empty layers must parse successfully");
    assert!(digests.layers.is_empty(), "layers must be empty");
    assert!(digests.config.is_some(), "config must be present");
}

#[test]
fn parse_manifest_digests_only_subject_succeeds() {
    // A manifest carrying only a subject (no config/layers) must parse successfully.
    let body = serde_json::to_vec(&serde_json::json!({
        "schemaVersion": 2,
        "subject": {
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "digest": "sha256:9876543210fedcba9876543210fedcba9876543210fedcba9876543210fedcba",
            "size": 512
        }
    }))
    .unwrap();

    let digests = parse_manifest_digests(&body, None).expect("subject-only manifest must parse");
    assert!(digests.subject.is_some(), "subject must be populated");
    assert!(digests.config.is_none());
    assert!(digests.layers.is_empty());
    assert!(digests.manifests.is_empty());
}

#[test]
fn parse_manifest_digests_index_manifest_populates_manifests_vec() {
    // An OCI image index carries a `manifests` array, no `layers`.
    let body = serde_json::to_vec(&serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.index.v1+json",
        "manifests": [
            {
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "digest": "sha256:aaaa0000bbbb1111cccc2222dddd3333eeee4444ffff555500001111aaaabbbb",
                "size": 100,
                "platform": { "architecture": "amd64", "os": "linux" }
            },
            {
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "digest": "sha256:bbbb1111cccc2222dddd3333eeee4444ffff555500001111aaaabbbbccccdddd",
                "size": 200,
                "platform": { "architecture": "arm64", "os": "linux" }
            }
        ]
    }))
    .unwrap();

    let digests = parse_manifest_digests(&body, None).expect("index manifest must parse");
    assert_eq!(
        digests.manifests.len(),
        2,
        "both child manifests must be collected"
    );
    assert!(digests.layers.is_empty());
    assert!(digests.config.is_none());
}

#[tokio::test]
async fn test_handle_head_manifest() {
    for test_case in backends() {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo").unwrap();
        let tag = "latest";
        let (content, media_type) = create_test_manifest(registry, namespace).await;

        let put_response = registry
            .put_manifest(
                namespace,
                &Reference::Tag(tag.to_string()),
                Some(&media_type),
                &content,
            )
            .await
            .unwrap();

        let repository = registry.get_repository_for_namespace(namespace).unwrap();
        let head = registry
            .head_manifest(
                repository,
                &[],
                namespace,
                Reference::Tag(tag.to_string()),
                false,
            )
            .await
            .unwrap();

        assert_eq!(
            header_digest(&head.headers),
            header_digest(&put_response.headers)
        );
        assert_eq!(
            head.headers[CONTENT_LENGTH.as_str()],
            content.len().to_string()
        );
        assert_eq!(head.headers[CONTENT_TYPE.as_str()], media_type);
        test_case.cleanup().await;
    }
}

#[tokio::test]
async fn test_handle_get_manifest() {
    for test_case in backends() {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo").unwrap();
        let tag = "latest";
        let (content, media_type) = create_test_manifest(registry, namespace).await;

        let put_response = registry
            .put_manifest(
                namespace,
                &Reference::Tag(tag.to_string()),
                Some(&media_type),
                &content,
            )
            .await
            .unwrap();

        let response = registry
            .resolve_get_manifest(namespace, Reference::Tag(tag.to_string()), &[], false)
            .await
            .unwrap();

        match response {
            GetManifestResponse::Redirect { headers } => {
                assert_eq!(
                    header_digest(&headers),
                    header_digest(&put_response.headers)
                );
                assert_eq!(headers[CONTENT_TYPE.as_str()], media_type);
            }
            GetManifestResponse::Body {
                headers,
                content: body,
            } => {
                assert_eq!(
                    header_digest(&headers),
                    header_digest(&put_response.headers)
                );
                assert_eq!(headers[CONTENT_TYPE.as_str()], media_type);
                assert_eq!(body, content);
            }
        }
        test_case.cleanup().await;
    }
}

#[tokio::test]
async fn test_handle_put_manifest() {
    for test_case in backends() {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo").unwrap();
        let tag = "latest";
        let (content, media_type) = create_test_manifest(registry, namespace).await;

        let manifest_stream = Cursor::new(content.clone());
        let response = registry
            .accept_put_manifest(
                None,
                namespace,
                Reference::Tag(tag.to_string()),
                media_type.clone(),
                manifest_stream,
            )
            .await
            .expect("put manifest failed");

        assert_eq!(
            response.headers[LOCATION.as_str()],
            format!("/v2/{namespace}/manifests/{tag}")
        );

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
        assert_eq!(stored_manifest.digest, header_digest(&response.headers));
        test_case.cleanup().await;
    }
}

#[tokio::test]
async fn test_handle_delete_manifest() {
    for test_case in backends() {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo").unwrap();
        let tag = "latest";
        let (content, media_type) = create_test_manifest(registry, namespace).await;

        registry
            .put_manifest(
                namespace,
                &Reference::Tag(tag.to_string()),
                Some(&media_type),
                &content,
            )
            .await
            .unwrap();

        registry
            .delete_manifest(None, namespace, &Reference::Tag(tag.to_string()))
            .await
            .unwrap();

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
        test_case.cleanup().await;
    }
}

async fn test_pull_through_cache_optimization_impl(test_case: &mut FSRegistryTestCase) {
    let namespace = &Namespace::new("test-repo").unwrap();

    let repositories = crate::registry::test_utils::create_test_repositories();

    test_case.set_repositories(repositories);
    let registry = test_case.registry();
    let (content, media_type) = create_test_manifest(registry, namespace).await;

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
        let (content, media_type) = create_test_manifest(registry, namespace).await;

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
            .delete_manifest(
                None,
                namespace,
                &Reference::Digest(header_digest(&response.headers)),
            )
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
                    Reference::Digest(header_digest(&response.headers)),
                    false,
                )
                .await
                .is_err()
        );
        test_case.cleanup().await;
    }
}

#[tokio::test]
async fn test_delete_manifest_by_digest_preserves_unrelated_tags() {
    for test_case in backends() {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo/delete-preserve").unwrap();
        let (content_a, media_type_a) = create_test_manifest(registry, namespace).await;
        let (content_b, media_type_b) =
            create_test_manifest_with_subject(registry, namespace).await;

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
            .delete_manifest(
                None,
                namespace,
                &Reference::Digest(header_digest(&response_a.headers)),
            )
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

        assert_eq!(manifest_b.digest, header_digest(&response_b.headers));
        test_case.cleanup().await;
    }
}

#[tokio::test]
async fn test_delete_manifest_with_many_tags() {
    for test_case in backends() {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo/delete-many-tags").unwrap();
        let (content_a, media_type_a) = create_test_manifest(registry, namespace).await;
        let (content_b, media_type_b) =
            create_test_manifest_with_subject(registry, namespace).await;

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
            .delete_manifest(
                None,
                namespace,
                &Reference::Digest(header_digest(&response_a.headers)),
            )
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
        test_case.cleanup().await;
    }
}

#[tokio::test]
async fn test_put_manifest_stores_media_type() {
    for test_case in backends() {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo/media-type-store").unwrap();
        let tag = "latest";
        let (content, media_type) = create_test_manifest(registry, namespace).await;

        let response = registry
            .put_manifest(
                namespace,
                &Reference::Tag(tag.to_string()),
                Some(&media_type),
                &content,
            )
            .await
            .unwrap();

        let digest_link = LinkKind::Digest(header_digest(&response.headers));
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
        test_case.cleanup().await;
    }
}

#[tokio::test]
async fn test_head_manifest_returns_correct_media_type() {
    for test_case in backends() {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo/head-media-type").unwrap();
        let (content, media_type) = create_test_manifest(registry, namespace).await;

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

        assert_eq!(head.headers[CONTENT_TYPE.as_str()], media_type);
        assert_eq!(
            header_digest(&head.headers),
            header_digest(&put_response.headers)
        );
        assert_eq!(
            head.headers[CONTENT_LENGTH.as_str()],
            content.len().to_string()
        );

        let head_by_digest = registry
            .head_manifest(
                repository,
                slice::from_ref(&media_type),
                namespace,
                Reference::Digest(header_digest(&put_response.headers)),
                false,
            )
            .await
            .unwrap();

        assert_eq!(head_by_digest.headers[CONTENT_TYPE.as_str()], media_type);
        assert_eq!(
            header_digest(&head_by_digest.headers),
            header_digest(&put_response.headers)
        );
        assert_eq!(
            head_by_digest.headers[CONTENT_LENGTH.as_str()],
            content.len().to_string()
        );
        test_case.cleanup().await;
    }
}

#[tokio::test]
async fn test_head_manifest_fallback_without_media_type() {
    for test_case in backends() {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo/head-fallback").unwrap();
        let (content, media_type) = create_test_manifest(registry, namespace).await;

        let digest = registry.blob_store.create(&content).await.unwrap();

        let mut tx = registry.metadata_store.begin_transaction(namespace);
        tx.create_link(&LinkKind::Digest(digest.clone()), &digest)
            .add();
        tx.create_link(&LinkKind::Tag("latest".to_string()), &digest)
            .add();
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
            head.headers[CONTENT_TYPE.as_str()],
            media_type,
            "HEAD should fall back to reading blob when media_type not in link"
        );
        assert_eq!(header_digest(&head.headers), digest);
        assert_eq!(
            head.headers[CONTENT_LENGTH.as_str()],
            content.len().to_string()
        );
        test_case.cleanup().await;
    }
}

#[tokio::test]
async fn test_delete_manifest_no_tags_by_digest() {
    for test_case in backends() {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo/delete-no-tags").unwrap();
        let (content, media_type) = create_test_manifest(registry, namespace).await;

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
            .delete_manifest(None, namespace, &Reference::Tag("temp".to_string()))
            .await
            .unwrap();

        registry
            .delete_manifest(
                None,
                namespace,
                &Reference::Digest(header_digest(&response.headers)),
            )
            .await
            .unwrap();

        let repository = registry.get_repository_for_namespace(namespace).unwrap();

        assert!(
            registry
                .get_manifest(
                    repository,
                    slice::from_ref(&media_type),
                    namespace,
                    Reference::Digest(header_digest(&response.headers)),
                    false,
                )
                .await
                .is_err()
        );
        test_case.cleanup().await;
    }
}

#[tokio::test]
async fn test_put_manifest_stores_media_type_in_links() {
    for test_case in backends() {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo/media-type-links").unwrap();
        let (content, media_type) = create_test_manifest(registry, namespace).await;

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
            .read_link(
                namespace,
                &LinkKind::Digest(header_digest(&response.headers)),
                false,
            )
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
        test_case.cleanup().await;
    }
}

#[tokio::test]
async fn test_head_local_manifest_uses_metadata_media_type() {
    for test_case in backends() {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo/head-optimized").unwrap();
        let (content, media_type) = create_test_manifest(registry, namespace).await;

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

        assert_eq!(head.headers[CONTENT_TYPE.as_str()], media_type);
        assert_eq!(
            header_digest(&head.headers),
            header_digest(&response.headers)
        );
        assert_eq!(
            head.headers[CONTENT_LENGTH.as_str()],
            content.len().to_string()
        );

        let head = registry
            .head_manifest(
                registry.get_repository_for_namespace(namespace).unwrap(),
                slice::from_ref(&media_type),
                namespace,
                Reference::Digest(header_digest(&response.headers)),
                false,
            )
            .await
            .unwrap();

        assert_eq!(head.headers[CONTENT_TYPE.as_str()], media_type);
        assert_eq!(
            header_digest(&head.headers),
            header_digest(&response.headers)
        );
        assert_eq!(
            head.headers[CONTENT_LENGTH.as_str()],
            content.len().to_string()
        );
        test_case.cleanup().await;
    }
}

#[tokio::test]
async fn test_put_manifest_without_content_type_stores_manifest_media_type() {
    for test_case in backends() {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo/no-content-type").unwrap();
        let (content, _media_type) = create_test_manifest(registry, namespace).await;

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
            .read_link(
                namespace,
                &LinkKind::Digest(header_digest(&response.headers)),
                false,
            )
            .await
            .unwrap();

        assert_eq!(
            digest_link.media_type,
            Some("application/vnd.docker.distribution.manifest.v2+json".to_string()),
            "Digest link should have media_type from manifest body"
        );
        test_case.cleanup().await;
    }
}

#[tokio::test]
async fn test_handle_get_manifest_redirect_includes_content_type() {
    for test_case in backends() {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo/redirect-ct").unwrap();
        let (content, media_type) = create_test_manifest(registry, namespace).await;

        let put_response = registry
            .put_manifest(
                namespace,
                &Reference::Tag("latest".to_string()),
                Some(&media_type),
                &content,
            )
            .await
            .unwrap();

        let response = registry
            .resolve_get_manifest(
                namespace,
                Reference::Tag("latest".to_string()),
                slice::from_ref(&media_type),
                false,
            )
            .await
            .unwrap();

        match response {
            GetManifestResponse::Redirect { headers } => {
                assert_eq!(
                    header_digest(&headers),
                    header_digest(&put_response.headers),
                    "Redirect digest must match"
                );
                assert_eq!(
                    headers[CONTENT_TYPE.as_str()],
                    media_type,
                    "Redirect should carry Content-Type from stored media_type"
                );
            }
            GetManifestResponse::Body { headers, .. } => {
                assert_eq!(headers[CONTENT_TYPE.as_str()], media_type);
            }
        }
        test_case.cleanup().await;
    }
}

#[tokio::test]
async fn test_handle_get_manifest_redirect_fallback_without_media_type() {
    for test_case in backends() {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo/redirect-fallback").unwrap();
        let (content, media_type) = create_test_manifest(registry, namespace).await;

        let digest = registry.blob_store.create(&content).await.unwrap();

        let mut tx = registry.metadata_store.begin_transaction(namespace);
        tx.create_link(&LinkKind::Digest(digest.clone()), &digest)
            .add();
        tx.create_link(&LinkKind::Tag("latest".to_string()), &digest)
            .add();
        tx.commit().await.unwrap();

        let response = registry
            .resolve_get_manifest(
                namespace,
                Reference::Tag("latest".to_string()),
                slice::from_ref(&media_type),
                false,
            )
            .await
            .unwrap();

        // When the redirect path fires, the media_type must still be present
        // (via fallback reading the blob body). When the blob backend does not
        // support presigned URLs (FS backend), we get a Body response — both are
        // valid; in the Body case the media_type comes from the manifest JSON.
        match response {
            GetManifestResponse::Redirect { headers } => {
                assert!(
                    headers.contains_key(CONTENT_TYPE.as_str()),
                    "Redirect should still include Content-Type via fallback"
                );
            }
            GetManifestResponse::Body { headers, .. } => {
                assert!(headers.contains_key(CONTENT_TYPE.as_str()));
            }
        }
        test_case.cleanup().await;
    }
}

#[tokio::test]
async fn test_handle_get_manifest_no_redirect_returns_body() {
    for test_case in backends() {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo/no-redirect").unwrap();
        let (content, media_type) = create_test_manifest(registry, namespace).await;

        let put_response = registry
            .put_manifest(
                namespace,
                &Reference::Tag("latest".to_string()),
                Some(&media_type),
                &content,
            )
            .await
            .unwrap();

        let response = registry
            .resolve_get_manifest(
                namespace,
                Reference::Tag("latest".to_string()),
                slice::from_ref(&media_type),
                false,
            )
            .await
            .unwrap();

        // Both redirect and body responses are valid depending on backend capabilities.
        // Verify the content is correct in either case.
        match response {
            GetManifestResponse::Redirect { headers } => {
                assert_eq!(
                    header_digest(&headers),
                    header_digest(&put_response.headers)
                );
                assert_eq!(headers[CONTENT_TYPE.as_str()], media_type);
            }
            GetManifestResponse::Body {
                headers,
                content: body,
            } => {
                assert_eq!(
                    header_digest(&headers),
                    header_digest(&put_response.headers)
                );
                assert_eq!(headers[CONTENT_TYPE.as_str()], media_type);
                assert_eq!(body, content);
            }
        }
        test_case.cleanup().await;
    }
}

fn fixed_digest() -> Digest {
    "sha256:0000000000000000000000000000000000000000000000000000000000000000"
        .parse()
        .unwrap()
}

#[test]
fn manifest_meta_from_body_returns_meta_for_valid_image_manifest_with_media_type() {
    let body = serde_json::to_vec(&json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            "size": 256
        },
        "layers": []
    }))
    .unwrap();

    let target = fixed_digest();
    let meta = manifest_meta_from_body(&target, &body).unwrap();

    assert_eq!(
        meta.media_type.as_deref(),
        Some("application/vnd.oci.image.manifest.v1+json")
    );
    assert_eq!(meta.digest, target);
    assert_eq!(meta.size, body.len() as u64);
}

#[test]
fn manifest_meta_from_body_returns_meta_for_image_manifest_without_media_type() {
    let body = serde_json::to_vec(&json!({
        "schemaVersion": 2,
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            "size": 256
        },
        "layers": []
    }))
    .unwrap();

    let target = fixed_digest();
    let meta = manifest_meta_from_body(&target, &body).unwrap();

    assert_eq!(meta.media_type, None);
    assert_eq!(meta.digest, target);
    assert_eq!(meta.size, body.len() as u64);
}

#[test]
fn manifest_meta_from_body_returns_meta_for_oci_index() {
    let body = serde_json::to_vec(&json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.index.v1+json",
        "manifests": [
            {
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "digest": "sha256:abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
                "size": 512,
                "platform": { "architecture": "amd64", "os": "linux" }
            }
        ]
    }))
    .unwrap();

    let target = fixed_digest();
    let meta = manifest_meta_from_body(&target, &body).unwrap();

    assert_eq!(
        meta.media_type.as_deref(),
        Some("application/vnd.oci.image.index.v1+json")
    );
    assert_eq!(meta.digest, target);
    assert_eq!(meta.size, body.len() as u64);
}

#[test]
fn manifest_meta_from_body_errors_on_malformed_json() {
    let target = fixed_digest();
    let result = manifest_meta_from_body(&target, b"not json");
    assert!(result.is_err());
}
