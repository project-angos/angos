use std::{
    collections::BTreeMap,
    io::Cursor,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use futures_util::future::join_all;
use http::{
    StatusCode,
    header::{CONTENT_LENGTH, CONTENT_TYPE, LOCATION},
};
use serde_json::json;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

use angos_oci::header::{DOCKER_CONTENT_DIGEST, OCI_TAG};
use angos_oci::request::{DeleteBlobRequest, PutManifestRequest};
use angos_oci::{Algorithm, MediaType, Namespace, Tag};
use angos_storage::{
    Error as StorageError, ObjectStore,
    test_util::{HookedStore, StoreHook, StoreOp},
};

use crate::registry::keys::{DigestKeys, NamespaceKeys};
use crate::registry::manifest::*;
use crate::{
    command::server::Error as ServerError,
    metrics_provider,
    registry::{
        Error, Registry,
        metadata_store::{LinkKind, LinkOperation, link::tag::TagEntryBody, tag_ord},
        repository::Config as RepositoryConfig,
        test_utils::{
            FSRegistryTestCase, RegistryTestCase, create_test_registry,
            create_test_registry_recording_pulls, create_test_registry_with, for_each_backend,
            get_blob, metadata_store_over, response_body, response_digest, response_header,
            upload_blob,
        },
    },
    registry_client::REPLICATION_SUPERSEDED_CODE,
    test_fixtures::client::test_client_config,
};

const IMAGE_MANIFEST_MEDIA_TYPE: &str = "application/vnd.docker.distribution.manifest.v2+json";
const CONFIG_MEDIA_TYPE: &str = "application/vnd.docker.container.image.v1+json";
const LAYER_MEDIA_TYPE: &str = "application/vnd.docker.image.rootfs.diff.tar.gzip";
const MISSING_SUBJECT_DIGEST: &str =
    "sha256:9876543210fedcba9876543210fedcba9876543210fedcba9876543210fedcba";

fn create_raw_test_manifest() -> (Vec<u8>, MediaType) {
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
    let media_type = MediaType::new(IMAGE_MANIFEST_MEDIA_TYPE).unwrap();
    (content, media_type)
}

fn create_raw_test_manifest_with_subject() -> (Vec<u8>, MediaType) {
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
    let media_type = MediaType::new(IMAGE_MANIFEST_MEDIA_TYPE).unwrap();
    (content, media_type)
}

fn manifest_with_references(
    config_digest: &Digest,
    config_size: usize,
    layer_digest: &Digest,
    layer_size: usize,
) -> (Vec<u8>, MediaType) {
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
    (content, MediaType::new(IMAGE_MANIFEST_MEDIA_TYPE).unwrap())
}

fn manifest_with_subject_and_references(
    config_digest: &Digest,
    config_size: usize,
    layer_digest: &Digest,
    layer_size: usize,
) -> (Vec<u8>, MediaType) {
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
    (content, MediaType::new(IMAGE_MANIFEST_MEDIA_TYPE).unwrap())
}

fn index_manifest_with_child(child_digest: &Digest) -> (Vec<u8>, MediaType) {
    let media_type = MediaType::new("application/vnd.oci.image.index.v1+json").unwrap();
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

async fn create_test_manifest(registry: &Registry, namespace: &Namespace) -> (Vec<u8>, MediaType) {
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
) -> (Vec<u8>, MediaType) {
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

/// `[repository]` entries configure namespaces rather than admitting them, so a
/// namespace none of them match is pushable and pullable like any other.
#[tokio::test]
async fn an_unconfigured_namespace_round_trips() {
    for_each_backend(async |test_case| {
        let registry = test_case.registry();
        let namespace = &Namespace::new("never/configured").unwrap();
        assert!(registry.get_repository_for_namespace(namespace).is_err());

        let (content, media_type) = create_test_manifest(registry, namespace).await;
        let tag = Reference::Tag(Tag::new("latest").unwrap());
        registry
            .put_manifest(namespace, &tag, Some(&media_type), &content)
            .await
            .unwrap();

        let stored = registry
            .resolve_get_manifest(
                None,
                GetManifestRequest {
                    namespace: namespace.clone(),
                    reference: tag,
                    accepted_types: vec![MediaRange::from(media_type)],
                },
                false,
            )
            .await
            .unwrap();

        assert_eq!(response_body(stored).await, content);
    })
    .await;
}

#[tokio::test]
async fn test_put_manifest() {
    for_each_backend(async |test_case| {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo").unwrap();
        let tag = "latest";
        let (content, media_type) = create_test_manifest(registry, namespace).await;

        let response = registry
            .put_manifest(
                namespace,
                &Reference::Tag(Tag::new(tag).unwrap()),
                Some(&media_type),
                &content,
            )
            .await
            .unwrap();

        let stored_manifest = registry
            .get_manifest(
                registry.get_repository_for_namespace(namespace).ok(),
                &[MediaRange::from(media_type.clone())],
                namespace,
                Reference::Tag(Tag::new(tag).unwrap()),
                false,
                "test-client",
            )
            .await
            .unwrap();

        assert_eq!(stored_manifest.content, content);
        assert_eq!(stored_manifest.media_type.unwrap(), media_type);
        assert_eq!(stored_manifest.digest, response.digest);

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

        assert_eq!(response.digest.clone(), digest);
    })
    .await;
}

/// A by-digest push with `?tag=` must create each listed tag, name both in
/// `OCI-Tag`, and resolve each tag to the pushed digest. The digest is sha512
/// to lock the regression where the suite fell back to a sha256 by-tag push.
#[tokio::test]
async fn accept_put_manifest_by_sha512_digest_with_tag_params_creates_tags() {
    let case = FSRegistryTestCase::new();
    let registry = case.registry();
    let namespace = Namespace::new("test-repo").unwrap();
    let (content, media_type) = create_test_manifest(registry, &namespace).await;
    let digest = Digest::from_bytes(Algorithm::Sha512, &content);
    assert_eq!(digest.algorithm(), Algorithm::Sha512);

    let response = registry
        .accept_put_manifest(
            None,
            PutManifestRequest {
                namespace: namespace.clone(),
                reference: Reference::Digest(digest.clone()),
                content_type: Some(media_type.clone()),
                tags: vec![Tag::new("1.2.3").unwrap(), Tag::new("latest").unwrap()],
                source_ts: None,
            },
            Cursor::new(content.clone()),
        )
        .await
        .expect("by-digest push with tag params must succeed");

    assert_eq!(
        *response_header(&response, &OCI_TAG),
        "1.2.3, latest",
        "OCI-Tag must list the created tags in creation order"
    );
    assert_eq!(response_digest(&response), digest);

    for tag in ["1.2.3", "latest"] {
        let head = registry
            .head_manifest(
                None,
                HeadManifestRequest {
                    namespace: namespace.clone(),
                    reference: Reference::Tag(Tag::new(tag).unwrap()),
                    accepted_types: vec![MediaRange::from(media_type.clone())],
                },
            )
            .await
            .expect("each created tag must resolve");
        assert_eq!(
            response_digest(&head),
            digest,
            "tag '{tag}' must point at the sha512 digest"
        );
    }
}

/// A by-tag push must ignore any tag query parameters: only the path tag is
/// created and no `OCI-Tag` header is emitted.
#[tokio::test]
async fn accept_put_manifest_by_tag_ignores_tag_params() {
    let case = FSRegistryTestCase::new();
    let registry = case.registry();
    let namespace = Namespace::new("test-repo").unwrap();
    let (content, media_type) = create_test_manifest(registry, &namespace).await;

    let response = registry
        .accept_put_manifest(
            None,
            PutManifestRequest {
                namespace: namespace.clone(),
                reference: Reference::Tag(Tag::new("v1").unwrap()),
                content_type: Some(media_type.clone()),
                tags: vec![Tag::new("ignored").unwrap()],
                source_ts: None,
            },
            Cursor::new(content.clone()),
        )
        .await
        .expect("by-tag push must succeed");

    assert!(
        !response.headers().contains_key(&OCI_TAG),
        "a by-tag push must not create any extra tags"
    );

    let ignored = registry
        .head_manifest(
            None,
            HeadManifestRequest {
                namespace: namespace.clone(),
                reference: Reference::Tag(Tag::new("ignored").unwrap()),
                accepted_types: vec![MediaRange::from(media_type.clone())],
            },
        )
        .await;
    assert!(
        ignored.is_err(),
        "a tag param on a by-tag push must not be created"
    );
}

#[tokio::test]
async fn put_manifest_rejects_missing_config_reference() {
    for_each_backend(async |test_case| {
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
                &Reference::Tag(Tag::new("latest").unwrap()),
                Some(&media_type),
                &content,
            )
            .await
        else {
            panic!("missing config must reject manifest push");
        };

        assert!(matches!(err, Error::ManifestBlobUnknown));
        let manifest_digest = Digest::sha256_of_bytes(&content);
        assert!(registry.blob_store.read(&manifest_digest).await.is_err());
        assert!(
            registry
                .metadata_store
                .read_link(namespace, &LinkKind::Tag(Tag::new("latest").unwrap()))
                .await
                .is_err()
        );
    })
    .await;
}

#[tokio::test]
async fn put_manifest_rejects_missing_layer_reference() {
    for_each_backend(async |test_case| {
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
                &Reference::Tag(Tag::new("latest").unwrap()),
                Some(&media_type),
                &content,
            )
            .await
        else {
            panic!("missing layer must reject manifest push");
        };

        assert!(matches!(err, Error::ManifestBlobUnknown));
    })
    .await;
}

#[tokio::test]
async fn put_manifest_rejects_missing_child_manifest_reference() {
    for_each_backend(async |test_case| {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo/missing-child").unwrap();
        let missing_child = fixed_digest();
        let (content, media_type) = index_manifest_with_child(&missing_child);

        let Err(err) = registry
            .put_manifest(
                namespace,
                &Reference::Tag(Tag::new("latest").unwrap()),
                Some(&media_type),
                &content,
            )
            .await
        else {
            panic!("missing child manifest must reject index push");
        };

        assert!(matches!(err, Error::ManifestBlobUnknown));
    })
    .await;
}

#[tokio::test]
async fn put_manifest_rejects_references_owned_by_another_namespace() {
    for_each_backend(async |test_case| {
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
                &Reference::Tag(Tag::new("latest").unwrap()),
                Some(&media_type),
                &content,
            )
            .await
        else {
            panic!("cross-namespace references must reject manifest push");
        };

        assert!(matches!(err, Error::ManifestBlobUnknown));
    })
    .await;
}

/// The image spec pins `schemaVersion` to 2, and a manifest angos accepts
/// without one links no layers, leaving its blobs to be reclaimed as orphans.
#[tokio::test]
async fn put_manifest_rejects_a_foreign_schema_version() {
    let case = FSRegistryTestCase::new();
    let namespace = Namespace::new("test-repo").unwrap();
    let body = serde_json::to_vec(&json!({
        "schemaVersion": 1,
        "mediaType": IMAGE_MANIFEST_MEDIA_TYPE,
    }))
    .unwrap();

    let error = case
        .registry()
        .put_manifest(
            &namespace,
            &Reference::Tag(Tag::new("legacy").unwrap()),
            Some(&MediaType::new(IMAGE_MANIFEST_MEDIA_TYPE).unwrap()),
            &body,
        )
        .await
        .err()
        .expect("a schemaVersion other than 2 must not be stored");

    let Error::ManifestInvalid(message) = error else {
        panic!("expected ManifestInvalid, got {error:?}");
    };
    assert!(message.contains("schemaVersion"), "message: {message}");
}

#[tokio::test]
async fn put_manifest_allows_missing_subject_reference() {
    for_each_backend(async |test_case| {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo/missing-subject").unwrap();
        let (content, media_type) = create_test_manifest_with_subject(registry, namespace).await;

        let response = registry
            .put_manifest(
                namespace,
                &Reference::Tag(Tag::new("latest").unwrap()),
                Some(&media_type),
                &content,
            )
            .await
            .expect("missing subject should not reject manifest push");

        let subject = MISSING_SUBJECT_DIGEST.parse().unwrap();
        let digest = response.digest.clone();
        let link = LinkKind::Referrer {
            subject,
            referrer: digest.clone(),
        };
        let metadata = registry
            .metadata_store
            .read_link(namespace, &link)
            .await
            .unwrap();
        assert_eq!(metadata.target, digest);
    })
    .await;
}

/// `accept_put_manifest` honors `validate_manifest_references`: permissive
/// stores an index whose child manifest is absent, strict rejects the identical
/// push with `MANIFEST_BLOB_UNKNOWN`.
#[tokio::test]
async fn accept_put_manifest_honors_reference_validation_flag() {
    let missing_child = fixed_digest();
    let namespace = Namespace::new("test-repo/ref-validation").unwrap();
    let (content, media_type) = index_manifest_with_child(&missing_child);

    let permissive_case = FSRegistryTestCase::new();
    let permissive = create_test_registry_with(
        permissive_case.blob_store(),
        permissive_case.metadata_store(),
        false,
    );
    permissive
        .accept_put_manifest(
            None,
            PutManifestRequest {
                namespace: namespace.clone(),
                reference: Reference::Tag(Tag::new("latest").unwrap()),
                content_type: Some(media_type.clone()),
                tags: Vec::new(),
                source_ts: None,
            },
            Cursor::new(content.clone()),
        )
        .await
        .expect("permissive registry must accept a missing child manifest reference");

    let strict_case = FSRegistryTestCase::new();
    let strict =
        create_test_registry_with(strict_case.blob_store(), strict_case.metadata_store(), true);
    let Err(err) = strict
        .accept_put_manifest(
            None,
            PutManifestRequest {
                namespace: namespace.clone(),
                reference: Reference::Tag(Tag::new("latest").unwrap()),
                content_type: Some(media_type),
                tags: Vec::new(),
                source_ts: None,
            },
            Cursor::new(content),
        )
        .await
    else {
        panic!("strict registry must reject a missing child manifest reference");
    };
    assert!(matches!(err, Error::ManifestBlobUnknown));
}

/// A permissive registry accepts a manifest referencing content the pushing
/// namespace does not own, but must not grant it read access: the references
/// stay dangling so a later pull resolves as unknown.
#[tokio::test]
async fn permissive_push_does_not_grant_read_of_unowned_referenced_blob() {
    let case = FSRegistryTestCase::new();
    let permissive = create_test_registry_with(case.blob_store(), case.metadata_store(), false);

    let owner = Namespace::new("test-repo/owner").unwrap();
    let config_content = br#"{"architecture":"amd64","os":"linux"}"#;
    let layer_content = b"private layer bytes";
    let config_digest = upload_blob(&permissive, &owner, config_content).await;
    let layer_digest = upload_blob(&permissive, &owner, layer_content).await;

    let attacker = Namespace::new("test-repo/attacker").unwrap();
    let (content, media_type) = manifest_with_references(
        &config_digest,
        config_content.len(),
        &layer_digest,
        layer_content.len(),
    );
    permissive
        .accept_put_manifest(
            None,
            PutManifestRequest {
                namespace: attacker.clone(),
                reference: Reference::Tag(Tag::new("latest").unwrap()),
                content_type: Some(media_type),
                tags: Vec::new(),
                source_ts: None,
            },
            Cursor::new(content),
        )
        .await
        .expect("permissive registry accepts a push referencing unowned blobs");

    let ownership = permissive.metadata_store.as_ref();
    assert!(ownership.can_read(&owner, &layer_digest).await.unwrap());
    assert!(ownership.can_read(&owner, &config_digest).await.unwrap());
    assert!(
        !ownership.can_read(&attacker, &layer_digest).await.unwrap(),
        "attacker must not gain read access to a layer it never uploaded"
    );
    assert!(
        !ownership.can_read(&attacker, &config_digest).await.unwrap(),
        "attacker must not gain read access to a config it never uploaded"
    );

    let repository = permissive.get_repository_for_namespace(&attacker).unwrap();
    let outcome = get_blob(&permissive, repository, &[], &attacker, &layer_digest, None)
        .await
        .map(|_| ());
    assert!(
        matches!(outcome, Err(Error::BlobUnknown)),
        "attacker pull of the unowned blob must be unknown, got {outcome:?}"
    );
}

/// The child-manifest analogue of the blob isolation guard: a permissive
/// registry accepts an index whose child manifest the pushing namespace does
/// not own (the docker buildx/bake scenario), but must not grant it read access
/// to the child, which stays dangling.
#[tokio::test]
async fn permissive_push_does_not_grant_read_of_unowned_child_manifest() {
    let case = FSRegistryTestCase::new();
    let permissive = create_test_registry_with(case.blob_store(), case.metadata_store(), false);

    let owner = Namespace::new("test-repo/owner").unwrap();
    let child_content = br#"{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef","size":1},"layers":[]}"#;
    let child_digest = upload_blob(&permissive, &owner, child_content).await;

    let attacker = Namespace::new("test-repo/attacker").unwrap();
    let (content, media_type) = index_manifest_with_child(&child_digest);
    permissive
        .accept_put_manifest(
            None,
            PutManifestRequest {
                namespace: attacker.clone(),
                reference: Reference::Tag(Tag::new("latest").unwrap()),
                content_type: Some(media_type),
                tags: Vec::new(),
                source_ts: None,
            },
            Cursor::new(content),
        )
        .await
        .expect("permissive registry accepts an index referencing an unowned child manifest");

    let ownership = permissive.metadata_store.as_ref();
    assert!(ownership.can_read(&owner, &child_digest).await.unwrap());
    assert!(
        !ownership.can_read(&attacker, &child_digest).await.unwrap(),
        "attacker must not gain read access to a child manifest it never uploaded"
    );

    let repository = permissive.get_repository_for_namespace(&attacker).unwrap();
    let outcome = permissive
        .get_manifest(
            Some(repository),
            &[],
            &attacker,
            Reference::Digest(child_digest.clone()),
            false,
            "test-client",
        )
        .await
        .map(|_| ());
    assert!(
        matches!(outcome, Err(Error::ManifestUnknown)),
        "attacker pull of the unowned child manifest must be unknown, got {outcome:?}"
    );
}

/// The positive branch: a namespace pushing a manifest that references blobs it
/// already owns keeps every reference link, so the manifest and both blobs stay
/// pullable.
#[tokio::test]
async fn permissive_push_of_owned_references_yields_a_pullable_manifest() {
    let case = FSRegistryTestCase::new();
    let permissive = create_test_registry_with(case.blob_store(), case.metadata_store(), false);

    let namespace = Namespace::new("test-repo/owner").unwrap();
    let config_content = br#"{"architecture":"amd64","os":"linux"}"#;
    let layer_content = b"owned layer bytes";
    let config_digest = upload_blob(&permissive, &namespace, config_content).await;
    let layer_digest = upload_blob(&permissive, &namespace, layer_content).await;

    let (content, media_type) = manifest_with_references(
        &config_digest,
        config_content.len(),
        &layer_digest,
        layer_content.len(),
    );
    permissive
        .accept_put_manifest(
            None,
            PutManifestRequest {
                namespace: namespace.clone(),
                reference: Reference::Tag(Tag::new("latest").unwrap()),
                content_type: Some(media_type),
                tags: Vec::new(),
                source_ts: None,
            },
            Cursor::new(content),
        )
        .await
        .expect("permissive registry accepts a push referencing owned blobs");

    let ownership = permissive.metadata_store.as_ref();
    assert!(
        ownership
            .can_read(&namespace, &config_digest)
            .await
            .unwrap()
    );
    assert!(ownership.can_read(&namespace, &layer_digest).await.unwrap());

    let repository = permissive.get_repository_for_namespace(&namespace).unwrap();
    permissive
        .get_manifest(
            Some(repository),
            &[],
            &namespace,
            Reference::Tag(Tag::new("latest").unwrap()),
            false,
            "test-client",
        )
        .await
        .expect("the pushed manifest must be pullable by tag");
    get_blob(
        &permissive,
        repository,
        &[],
        &namespace,
        &config_digest,
        None,
    )
    .await
    .expect("the owned config blob must be pullable");
    get_blob(
        &permissive,
        repository,
        &[],
        &namespace,
        &layer_digest,
        None,
    )
    .await
    .expect("the owned layer blob must be pullable");
}

async fn pull_through_repository(server: &MockServer) -> Repository {
    let cache = angos_cache::Config::Memory.to_backend().unwrap();
    let config = RepositoryConfig {
        upstream: vec![test_client_config(server.uri())],
        ..Default::default()
    };
    Repository::new(
        "test-repo",
        &config,
        &cache,
        DEFAULT_MAX_MANIFEST_SIZE_BYTES,
    )
    .await
    .unwrap()
}

async fn mount_manifest_without_digest_header(server: &MockServer, reference: &str, body: &[u8]) {
    Mock::given(method("GET"))
        .and(path(format!("/v2/test-repo/manifests/{reference}")))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_bytes(body)
                .insert_header("Content-Type", IMAGE_MANIFEST_MEDIA_TYPE),
        )
        .mount(server)
        .await;
}

/// A pull-through fill from an upstream that omits `Docker-Content-Digest`
/// must succeed, hashing the body it received rather than failing the pull.
#[tokio::test]
async fn pull_through_computes_the_digest_when_the_upstream_omits_the_header() {
    let case = FSRegistryTestCase::new();
    let namespace = Namespace::new("test-repo").unwrap();
    let (content, _) = create_raw_test_manifest();

    let upstream = MockServer::start().await;
    mount_manifest_without_digest_header(&upstream, "latest", &content).await;
    let repository = pull_through_repository(&upstream).await;

    let manifest = case
        .registry()
        .get_manifest(
            Some(&repository),
            &[MediaRange::from(MediaType::docker_manifest())],
            &namespace,
            Reference::Tag(Tag::new("latest").unwrap()),
            false,
            "test-client",
        )
        .await
        .expect("an upstream omitting Docker-Content-Digest must not fail the pull");

    assert_eq!(manifest.content, content);
    assert_eq!(
        manifest.digest,
        Digest::sha256_of_bytes(&content),
        "a tag names no algorithm, so the body hashes under the spec's mandatory one"
    );
}

fn pull_through_count(repository: &str, outcome: &str) -> u64 {
    metrics_provider::metrics_provider()
        .pull_through_total
        .with_label_values(&[repository, "manifest", outcome])
        .get()
}

/// Every pull of a cached repository records exactly one outcome: the first
/// misses, the second serves the stored copy, and a mutable tag the upstream
/// re-pointed refreshes. The repository name is this test's alone, so no other
/// test can move the counters it reads.
#[tokio::test]
async fn pull_through_counts_a_miss_then_a_hit_then_a_refresh() {
    const REPOSITORY: &str = "pull-through-metrics";
    let case = FSRegistryTestCase::new();
    let namespace = Namespace::new(REPOSITORY).unwrap();
    let (content, _) = create_raw_test_manifest();
    let accepted = [MediaRange::from(MediaType::docker_manifest())];

    let upstream = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/v2/{REPOSITORY}/manifests/latest")))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_bytes(content.clone())
                .insert_header(CONTENT_TYPE, IMAGE_MANIFEST_MEDIA_TYPE),
        )
        .mount(&upstream)
        .await;

    let cache_backend = angos_cache::Config::Memory.to_backend().unwrap();
    let repository = Repository::new(
        REPOSITORY,
        &RepositoryConfig {
            upstream: vec![test_client_config(upstream.uri())],
            ..Default::default()
        },
        &cache_backend,
        DEFAULT_MAX_MANIFEST_SIZE_BYTES,
    )
    .await
    .unwrap();

    let pull = async |immutable_tag: bool| {
        case.registry()
            .get_manifest(
                Some(&repository),
                &accepted,
                &namespace,
                Reference::Tag(Tag::new("latest").unwrap()),
                immutable_tag,
                "test-client",
            )
            .await
            .expect("the pull must be served")
    };

    pull(true).await;
    assert_eq!(
        pull_through_count(REPOSITORY, "miss"),
        1,
        "the first pull has nothing stored to serve"
    );

    pull(true).await;
    assert_eq!(
        pull_through_count(REPOSITORY, "hit"),
        1,
        "an immutable tag is served from the stored copy"
    );

    // The upstream re-points the tag, so the stored copy no longer answers it.
    Mock::given(method("HEAD"))
        .and(path(format!("/v2/{REPOSITORY}/manifests/latest")))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header(
                    DOCKER_CONTENT_DIGEST,
                    Digest::sha256_of_bytes(b"moved upstream")
                        .to_string()
                        .as_str(),
                )
                .insert_header(CONTENT_LENGTH, "0")
                .insert_header(CONTENT_TYPE, IMAGE_MANIFEST_MEDIA_TYPE),
        )
        .mount(&upstream)
        .await;

    pull(false).await;
    assert_eq!(
        pull_through_count(REPOSITORY, "refresh"),
        1,
        "a mutable tag the upstream moved is refetched"
    );
    assert_eq!(
        pull_through_count(REPOSITORY, "miss"),
        1,
        "a refresh is not counted as a miss"
    );
}

/// The recomputed digest follows the algorithm the reference asked for, so a
/// by-digest pull of a sha512 manifest is not answered with a sha256 digest.
#[tokio::test]
async fn pull_through_recomputes_under_the_requested_digest_algorithm() {
    let case = FSRegistryTestCase::new();
    let namespace = Namespace::new("test-repo").unwrap();
    let (content, _) = create_raw_test_manifest();
    let requested = Digest::from_bytes(Algorithm::Sha512, &content);

    let upstream = MockServer::start().await;
    mount_manifest_without_digest_header(&upstream, &requested.to_string(), &content).await;
    let repository = pull_through_repository(&upstream).await;

    let manifest = case
        .registry()
        .get_manifest(
            Some(&repository),
            &[MediaRange::from(MediaType::docker_manifest())],
            &namespace,
            Reference::Digest(requested.clone()),
            false,
            "test-client",
        )
        .await
        .expect("a by-digest pull must survive a missing Docker-Content-Digest");

    assert_eq!(
        manifest.digest, requested,
        "the recomputed digest must use the requested algorithm, not a sha256 default"
    );
}

/// A delete cascades the manifest's config, layer and child links, which it can
/// only name by reading the body. A fault that read as "no children" would drop
/// the revision while those links survive, pinning blobs with no body left to
/// replan the cascade from.
#[tokio::test]
async fn a_blob_fault_aborts_a_digest_delete_instead_of_half_committing() {
    let case = FSRegistryTestCase::new();
    let namespace = &Namespace::new("test-repo/delete-fault").unwrap();
    let tag = Tag::new("latest").unwrap();

    let (content, media_type) = create_test_manifest(case.registry(), namespace).await;
    let response = case
        .registry()
        .put_manifest(
            namespace,
            &Reference::Tag(tag.clone()),
            Some(&media_type),
            &content,
        )
        .await
        .unwrap();

    let hooked: Arc<dyn ObjectStore> = Arc::new(HookedStore::new(
        case.blob_store().object_store().clone(),
        FailReadsOf {
            key: response.digest.blob_path(),
        },
    ));
    let registry = create_test_registry(
        Arc::new(BlobStore::new(hooked, None)),
        case.metadata_store(),
    );

    registry
        .delete_manifest(
            None,
            None,
            namespace,
            &Reference::Digest(response.digest.clone()),
        )
        .await
        .expect_err("a blob-store fault must abort the delete, not commit a partial cascade");

    let repository = case
        .registry()
        .get_repository_for_namespace(namespace)
        .unwrap();
    case.registry()
        .get_manifest(
            Some(repository),
            &[MediaRange::from(media_type)],
            namespace,
            Reference::Tag(tag),
            false,
            "test-client",
        )
        .await
        .expect("the aborted delete must leave the tag resolvable");
}

/// Fails every read of one object, standing in for a backend outage rather than
/// genuinely absent content.
struct FailReadsOf {
    key: String,
}

#[async_trait::async_trait]
impl StoreHook for FailReadsOf {
    async fn before(&self, op: StoreOp<'_>) -> Result<(), StorageError> {
        match op {
            StoreOp::Get { key } if key == self.key => {
                Err(StorageError::Backend("store is down".to_string()))
            }
            StoreOp::List { prefix } if prefix == self.key => {
                Err(StorageError::Backend("store is down".to_string()))
            }
            _ => Ok(()),
        }
    }
}

/// A storage outage must not reach the client as a deleted image: collapsing
/// it into `ManifestUnknown` tells clients and CI the tag is gone.
#[tokio::test]
async fn a_backend_fault_is_not_reported_as_a_missing_manifest() {
    let case = FSRegistryTestCase::new();
    let namespace = Namespace::new("test-repo").unwrap();
    let tag = Tag::new("latest").unwrap();

    let inner: Arc<dyn ObjectStore> = case.metadata_store().object_store().clone();
    let hooked: Arc<dyn ObjectStore> = Arc::new(HookedStore::new(
        inner,
        FailReadsOf {
            key: namespace.tag_entry_dir(&tag),
        },
    ));
    let registry = create_test_registry(case.blob_store(), metadata_store_over(hooked));
    let repository = registry.get_repository_for_namespace(&namespace).unwrap();

    let error = registry
        .get_manifest(
            Some(repository),
            &[],
            &namespace,
            Reference::Tag(tag),
            false,
            "test-client",
        )
        .await
        .err()
        .expect("a failing metadata store must not read as a successful lookup");

    assert!(
        !matches!(error, Error::ManifestUnknown),
        "a backend fault must not be reported as a missing manifest, got: {error:?}"
    );
}

#[tokio::test]
async fn test_get_manifest() {
    for_each_backend(async |test_case| {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo").unwrap();
        let tag = "latest";
        let (content, media_type) = create_test_manifest(registry, namespace).await;

        let response = registry
            .put_manifest(
                namespace,
                &Reference::Tag(Tag::new(tag).unwrap()),
                Some(&media_type),
                &content,
            )
            .await
            .unwrap();

        let manifest = registry
            .get_manifest(
                registry.get_repository_for_namespace(namespace).ok(),
                &[MediaRange::from(media_type.clone())],
                namespace,
                Reference::Tag(Tag::new(tag).unwrap()),
                false,
                "test-client",
            )
            .await
            .unwrap();

        assert_eq!(manifest.content, content);
        assert_eq!(manifest.media_type.unwrap(), media_type);
        assert_eq!(manifest.digest, response.digest.clone());

        let manifest = registry
            .get_manifest(
                registry.get_repository_for_namespace(namespace).ok(),
                &[MediaRange::from(media_type.clone())],
                namespace,
                Reference::Digest(response.digest.clone()),
                false,
                "test-client",
            )
            .await
            .unwrap();

        assert_eq!(manifest.content, content);
        assert_eq!(manifest.media_type.unwrap(), media_type);
        assert_eq!(manifest.digest, response.digest.clone());
    })
    .await;
}

#[tokio::test]
async fn test_head_manifest() {
    for_each_backend(async |test_case| {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo").unwrap();
        let tag = "latest";
        let (content, media_type) = create_test_manifest(registry, namespace).await;

        let response = registry
            .put_manifest(
                namespace,
                &Reference::Tag(Tag::new(tag).unwrap()),
                Some(&media_type),
                &content,
            )
            .await
            .unwrap();
        let pushed_digest = response.digest.clone();

        let manifest = registry
            .head_manifest(
                None,
                HeadManifestRequest {
                    namespace: namespace.clone(),
                    reference: Reference::Tag(Tag::new(tag).unwrap()),
                    accepted_types: vec![MediaRange::from(media_type.clone())],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            *response_header(&manifest, &CONTENT_TYPE),
            media_type.as_ref()
        );
        assert_eq!(response_digest(&manifest), pushed_digest);
        assert_eq!(
            *response_header(&manifest, &CONTENT_LENGTH),
            content.len().to_string()
        );

        let manifest = registry
            .head_manifest(
                None,
                HeadManifestRequest {
                    namespace: namespace.clone(),
                    reference: Reference::Digest(pushed_digest.clone()),
                    accepted_types: vec![MediaRange::from(media_type.clone())],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            *response_header(&manifest, &CONTENT_TYPE),
            media_type.as_ref()
        );
        assert_eq!(response_digest(&manifest), pushed_digest);
        assert_eq!(
            *response_header(&manifest, &CONTENT_LENGTH),
            content.len().to_string()
        );
    })
    .await;
}

/// Counts the access entries written under one tag's atime directory, so a
/// stamp is observed even when two land on the same key.
struct CountAtimeStamps {
    dir: String,
    stamps: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl StoreHook for CountAtimeStamps {
    async fn before(&self, op: StoreOp<'_>) -> Result<(), StorageError> {
        if let StoreOp::Put { key, .. } = op
            && key.starts_with(&self.dir)
        {
            self.stamps.fetch_add(1, Ordering::SeqCst);
        }
        Ok(())
    }
}

/// One served request appends exactly one access entry. The redirect probe and
/// the HEAD metadata read both resolve the same link before the body path does,
/// so a stamp taken at every link read counted a single pull twice.
#[tokio::test]
async fn a_served_request_records_exactly_one_pull() {
    let test_case = FSRegistryTestCase::new();
    let namespace = &Namespace::new("pull-count-ns").unwrap();
    let tag_name = Tag::new("latest").unwrap();

    let stamps = Arc::new(AtomicUsize::new(0));
    let inner: Arc<dyn ObjectStore> = test_case.metadata_store().object_store().clone();
    let hooked: Arc<dyn ObjectStore> = Arc::new(HookedStore::new(
        inner,
        CountAtimeStamps {
            dir: namespace.tag_atime_entry_dir(&tag_name),
            stamps: stamps.clone(),
        },
    ));
    let registry =
        create_test_registry_recording_pulls(test_case.blob_store(), metadata_store_over(hooked));
    let (content, media_type) = create_test_manifest(&registry, namespace).await;
    registry
        .put_manifest(
            namespace,
            &Reference::Tag(tag_name.clone()),
            Some(&media_type),
            &content,
        )
        .await
        .unwrap();

    // Counting stamps rather than entry keys: two stamps by one client inside
    // the same millisecond share a key by design, so a key count cannot tell
    // one pull from two.
    assert_eq!(stamps.load(Ordering::SeqCst), 0, "a push is not a pull");

    registry
        .resolve_get_manifest(
            None,
            GetManifestRequest {
                namespace: namespace.clone(),
                reference: Reference::Tag(tag_name.clone()),
                accepted_types: vec![MediaRange::from(media_type.clone())],
            },
            true,
        )
        .await
        .unwrap();
    assert_eq!(stamps.load(Ordering::SeqCst), 1, "one GET records one pull");

    registry
        .head_manifest(
            None,
            HeadManifestRequest {
                namespace: namespace.clone(),
                reference: Reference::Tag(tag_name.clone()),
                accepted_types: vec![MediaRange::from(media_type)],
            },
        )
        .await
        .unwrap();
    assert_eq!(
        stamps.load(Ordering::SeqCst),
        2,
        "one HEAD records one more pull"
    );
}

#[tokio::test]
async fn test_delete_manifest() {
    for_each_backend(async |test_case| {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo").unwrap();
        let tag = "latest";
        let (content, media_type) = create_test_manifest(registry, namespace).await;

        let response = registry
            .put_manifest(
                namespace,
                &Reference::Tag(Tag::new(tag).unwrap()),
                Some(&media_type),
                &content,
            )
            .await
            .unwrap();

        registry
            .delete_manifest(
                None,
                None,
                namespace,
                &Reference::Tag(Tag::new(tag).unwrap()),
            )
            .await
            .unwrap();

        assert!(
            registry
                .get_manifest(
                    registry.get_repository_for_namespace(namespace).ok(),
                    &[MediaRange::from(media_type.clone())],
                    namespace,
                    Reference::Tag(Tag::new(tag).unwrap()),
                    false,
                    "test-client",
                )
                .await
                .is_err()
        );

        registry
            .delete_manifest(
                None,
                None,
                namespace,
                &Reference::Digest(response.digest.clone()),
            )
            .await
            .unwrap();

        assert!(
            registry
                .get_manifest(
                    registry.get_repository_for_namespace(namespace).ok(),
                    &[MediaRange::from(media_type.clone())],
                    namespace,
                    Reference::Digest(response.digest.clone()),
                    false,
                    "test-client",
                )
                .await
                .is_err()
        );
    })
    .await;
}

/// A digest `delete_manifest` never reclaims bytes: the collector does, once
/// every reference is stale, so a concurrent grant from another repository
/// can never be stranded (conformance `MANIFEST_BLOB_UNKNOWN`).
#[tokio::test]
async fn delete_manifest_leaves_bytes_for_the_collector() {
    for_each_backend(async |test_case| {
        let registry = test_case.registry();
        let first = &Namespace::new("test-repo/first").unwrap();
        let second = &Namespace::new("test-repo/second").unwrap();

        let layer_content = b"shared layer content";
        let config_content = br#"{"architecture":"amd64","os":"linux"}"#;
        let layer_digest = upload_blob(registry, first, layer_content).await;
        let config_digest = upload_blob(registry, first, config_content).await;
        let media_type = MediaType::new("application/vnd.oci.image.manifest.v1+json").unwrap();
        let manifest = json!({
            "schemaVersion": 2,
            "mediaType": media_type,
            "config": {
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "digest": config_digest.to_string(),
                "size": config_content.len()
            },
            "layers": [{
                "mediaType": "application/vnd.oci.image.layer.v1.tar",
                "digest": layer_digest.to_string(),
                "size": layer_content.len()
            }]
        });
        let manifest_content = serde_json::to_vec(&manifest).unwrap();
        let response = registry
            .put_manifest(
                first,
                &Reference::Tag(Tag::new("latest").unwrap()),
                Some(&media_type),
                &manifest_content,
            )
            .await
            .unwrap();
        let digest = response.digest.clone();

        // A second repo holds a reference; the delete must not touch the bytes.
        let ownership = registry.metadata_store.as_ref();
        ownership.grant(second, &digest).await.unwrap();
        let reference = Reference::Digest(digest.clone());
        registry
            .delete_manifest(None, None, first, &reference)
            .await
            .unwrap();

        assert!(
            registry.blob_store.read(&digest).await.is_ok(),
            "the bytes are the collector's to reclaim, never the delete's"
        );
    })
    .await;
}

/// A tag racing a digest delete may survive as an entry pointing at the
/// deleted revision; the revision-existence probe makes it read as 404 on
/// every path, and the next push heals it.
#[tokio::test]
async fn a_tag_racing_a_digest_delete_reads_as_gone() {
    for_each_backend(async |test_case| {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo").unwrap();

        let (manifest_content, media_type) = create_test_manifest(registry, namespace).await;
        let digest = registry
            .put_manifest(
                namespace,
                &Reference::Tag(Tag::new("latest").unwrap()),
                Some(&media_type),
                &manifest_content,
            )
            .await
            .unwrap()
            .digest;

        let reference = Reference::Digest(digest.clone());
        registry
            .delete_manifest(None, None, namespace, &reference)
            .await
            .unwrap();

        // A racing push lands its tag entry after the delete's tag scan: the
        // entry survives, pointing at the deleted revision.
        let fresh = Tag::new("fresh").unwrap();
        registry
            .metadata_store
            .update_links(
                namespace,
                &[LinkOperation::create_with_media_type(
                    LinkKind::Tag(fresh.clone()),
                    digest.clone(),
                    Some(media_type.clone()),
                    None,
                    None,
                )],
            )
            .await
            .unwrap();

        let revision = registry
            .metadata_store
            .read_link(namespace, &LinkKind::Digest(digest.clone()))
            .await;
        assert!(
            matches!(revision, Err(Error::NotFound)),
            "the digest delete must remove the revision record, got: {revision:?}"
        );
        let resolved = registry
            .get_manifest(
                registry.get_repository_for_namespace(namespace).ok(),
                &[MediaRange::from(media_type.clone())],
                namespace,
                Reference::Tag(fresh),
                false,
                "test-client",
            )
            .await;
        assert!(
            resolved.is_err(),
            "a surviving racing tag must read as gone, not serve the deleted manifest"
        );
    })
    .await;
}

#[tokio::test]
async fn delete_manifest_then_delete_uploaded_blobs() {
    for_each_backend(async |test_case| {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo/zot-cleanup").unwrap();
        let layer_content = b"zot benchmark layer content";
        let config_content = br#"{"architecture":"amd64","os":"linux"}"#;
        let layer_digest = upload_blob(registry, namespace, layer_content).await;
        let config_digest = upload_blob(registry, namespace, config_content).await;
        let media_type = MediaType::new("application/vnd.oci.image.manifest.v1+json").unwrap();
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
                &Reference::Tag(Tag::new("latest").unwrap()),
                Some(&media_type),
                &manifest_content,
            )
            .await
            .unwrap();
        let manifest_digest = response.digest.clone();

        let manifest_blob_result = registry
            .delete_blob(DeleteBlobRequest {
                namespace: namespace.clone(),
                digest: manifest_digest.clone(),
            })
            .await;
        assert!(matches!(manifest_blob_result, Err(Error::BlobReferenced)));

        let layer_result = registry
            .delete_blob(DeleteBlobRequest {
                namespace: namespace.clone(),
                digest: layer_digest.clone(),
            })
            .await;
        assert!(matches!(layer_result, Err(Error::BlobReferenced)));

        registry
            .delete_manifest(
                None,
                None,
                namespace,
                &Reference::Digest(manifest_digest.clone()),
            )
            .await
            .unwrap();

        // The manifest body stays for the collector; only the revision is gone.
        assert!(registry.blob_store.read(&manifest_digest).await.is_ok());
        assert!(
            registry
                .metadata_store
                .read_link(namespace, &LinkKind::Digest(manifest_digest.clone()))
                .await
                .is_err()
        );
        assert_eq!(
            registry.blob_store.read(&layer_digest).await.unwrap(),
            layer_content
        );
        assert_eq!(
            registry.blob_store.read(&config_digest).await.unwrap(),
            config_content
        );

        registry
            .delete_blob(DeleteBlobRequest {
                namespace: namespace.clone(),
                digest: layer_digest.clone(),
            })
            .await
            .unwrap();
        registry
            .delete_blob(DeleteBlobRequest {
                namespace: namespace.clone(),
                digest: config_digest.clone(),
            })
            .await
            .unwrap();

        // Ownership revoked; the stale entries and the bytes wait for the collector.
        let ownership = registry.metadata_store();
        for digest in [&layer_digest, &config_digest] {
            let refs = ownership.references(namespace, digest).await.unwrap();
            assert!(
                !refs.contains(&LinkKind::Blob(digest.clone())),
                "the ownership key must be revoked for {digest}"
            );
        }
    })
    .await;
}

#[tokio::test]
async fn concurrent_same_digest_pushes_keep_upload_ownership() {
    let test_case = FSRegistryTestCase::new();
    let registry = test_case.registry();
    let media_type = MediaType::new("application/vnd.oci.image.manifest.v1+json").unwrap();
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
        let response = registry
            .put_manifest(
                namespace,
                &Reference::Tag(Tag::new("latest").unwrap()),
                Some(&media_type),
                &manifest_content,
            )
            .await
            .unwrap();
        (layer_digest, response.digest)
    });

    let digests = join_all(pushes).await;
    let (layer_digest, manifest_digest) = &digests[0];
    let blob_index = registry
        .metadata_store
        .read_blob_index(layer_digest)
        .await
        .unwrap();

    for namespace in namespaces {
        let links = blob_index.namespace.get(&namespace).unwrap();
        assert!(links.contains(&LinkKind::Blob(layer_digest.clone())));
        assert!(links.contains(&LinkKind::ReferencedBy(manifest_digest.clone())));
    }

    test_case.cleanup().await;
}

#[test]
fn revision_links_cover_the_config_layers_and_subject_of_a_manifest() {
    let revision = Digest::sha256_of_bytes(b"revision");
    let subject =
        Digest::sha256("9876543210fedcba9876543210fedcba9876543210fedcba9876543210fedcba").unwrap();
    let config =
        Digest::sha256("1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef").unwrap();
    let layer =
        Digest::sha256("abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890").unwrap();

    let (content, _) = create_raw_test_manifest();
    let manifest = Manifest::from_slice(&content).unwrap();
    assert_eq!(
        link_plan::revision_links(&manifest, &revision),
        vec![
            (LinkKind::Config(config.clone()), config.clone()),
            (LinkKind::Layer(layer.clone()), layer.clone()),
        ]
    );

    let (content, _) = create_raw_test_manifest_with_subject();
    let manifest = Manifest::from_slice(&content).unwrap();
    assert_eq!(
        link_plan::revision_links(&manifest, &revision),
        vec![
            (LinkKind::Config(config.clone()), config),
            (LinkKind::Layer(layer.clone()), layer),
            (
                LinkKind::Referrer {
                    subject,
                    referrer: revision.clone(),
                },
                revision,
            ),
        ],
        "a subject-bearing manifest also links the referrer back to its subject"
    );
}

#[tokio::test]
async fn test_malformed_json_yields_same_error_shape() {
    let malformed = b"not json";

    let parse_err = Manifest::from_slice(malformed)
        .map_err(|e| Error::manifest_invalid(&e))
        .expect_err("expected Err from a malformed body");
    match parse_err {
        crate::registry::Error::ManifestInvalid(s) => {
            assert!(
                s.starts_with("invalid manifest JSON:"),
                "the parse error should start with 'invalid manifest JSON:'; got: {s}"
            );
        }
        other => panic!("expected ManifestInvalid from the parse error mapper, got {other:?}"),
    }

    // Parse failure is detected before any blob/metadata store access, so a
    // single backend exercises the full path.
    let test_case = FSRegistryTestCase::new();
    let registry = test_case.registry();
    let namespace = &Namespace::new("test-repo").unwrap();
    let put_err = registry
        .put_manifest(
            namespace,
            &Reference::Tag(Tag::new("latest").unwrap()),
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

/// A body whose `mediaType` contradicts the `Content-Type` it was pushed under
/// is refused before any store access, as a manifest the client got wrong.
#[tokio::test]
async fn put_manifest_media_type_mismatch_returns_manifest_invalid() {
    let (content, _) = create_raw_test_manifest();
    let wrong_type = MediaType::new("application/vnd.oci.image.manifest.v1+json").unwrap();
    let test_case = FSRegistryTestCase::new();

    let err = test_case
        .registry()
        .put_manifest(
            &Namespace::new("test-repo").unwrap(),
            &Reference::Tag(Tag::new("latest").unwrap()),
            Some(&wrong_type),
            &content,
        )
        .await
        .err()
        .expect("expected error on media type mismatch");
    assert!(
        matches!(err, Error::ManifestInvalid(_)),
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
            PutManifestRequest {
                namespace: namespace.clone(),
                reference: Reference::Tag(Tag::new("latest").unwrap()),
                content_type: Some(
                    MediaType::new("application/vnd.oci.image.manifest.v1+json").unwrap(),
                ),
                tags: Vec::new(),
                source_ts: None,
            },
            Cursor::new(body),
        )
        .await
        .expect_err("expected oversized manifest upload to fail");

    assert!(matches!(
        err,
        Error::ManifestBodyTooLarge {
            limit: DEFAULT_MAX_MANIFEST_SIZE_BYTES
        }
    ));
}

#[test]
fn a_manifest_without_layers_links_only_its_config() {
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

    let manifest = Manifest::from_slice(&body).expect("empty layers must parse successfully");
    let links = link_plan::referenced_links(&manifest, &Digest::sha256_of_bytes(b"revision"));
    assert_eq!(links.len(), 1, "only the config is referenced: {links:?}");
    assert!(matches!(links[0].0, LinkKind::Config(_)));
}

#[test]
fn a_subject_only_manifest_links_nothing_but_its_referrer_back_link() {
    let body = serde_json::to_vec(&serde_json::json!({
        "schemaVersion": 2,
        "subject": {
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "digest": "sha256:9876543210fedcba9876543210fedcba9876543210fedcba9876543210fedcba",
            "size": 512
        }
    }))
    .unwrap();

    let revision = Digest::sha256_of_bytes(b"revision");
    let manifest = Manifest::from_slice(&body).expect("subject-only manifest must parse");
    assert!(link_plan::referenced_links(&manifest, &revision).is_empty());
    let links = link_plan::revision_links(&manifest, &revision);
    assert_eq!(links.len(), 1, "only the referrer back-link: {links:?}");
    assert!(matches!(links[0].0, LinkKind::Referrer { .. }));
}

#[test]
fn an_index_links_each_of_its_children() {
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

    let revision = Digest::sha256_of_bytes(b"revision");
    let manifest = Manifest::from_slice(&body).expect("index manifest must parse");
    let links = link_plan::referenced_links(&manifest, &revision);
    assert_eq!(links.len(), 2, "both children must be linked: {links:?}");
    assert!(links.iter().all(|(link, target)| matches!(
        link,
        LinkKind::Manifest { index, child } if index == &revision && child == target
    )));
}

#[tokio::test]
async fn test_handle_get_manifest() {
    for_each_backend(async |test_case| {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo").unwrap();
        let tag = "latest";
        let (content, media_type) = create_test_manifest(registry, namespace).await;

        let put_response = registry
            .put_manifest(
                namespace,
                &Reference::Tag(Tag::new(tag).unwrap()),
                Some(&media_type),
                &content,
            )
            .await
            .unwrap();

        let response = registry
            .resolve_get_manifest(
                None,
                GetManifestRequest {
                    namespace: namespace.clone(),
                    reference: Reference::Tag(Tag::new(tag).unwrap()),
                    accepted_types: Vec::new(),
                },
                true,
            )
            .await
            .unwrap();

        // Inline or redirect, the response names the same manifest and type.
        assert_eq!(response_digest(&response), put_response.digest);
        assert_eq!(
            *response_header(&response, &CONTENT_TYPE),
            media_type.as_ref()
        );
        if response.status() == StatusCode::OK {
            assert_eq!(response_body(response).await, content);
        }
    })
    .await;
}

#[tokio::test]
async fn test_handle_put_manifest() {
    for_each_backend(async |test_case| {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo").unwrap();
        let tag = "latest";
        let (content, media_type) = create_test_manifest(registry, namespace).await;

        let manifest_stream = Cursor::new(content.clone());
        let response = registry
            .accept_put_manifest(
                None,
                PutManifestRequest {
                    namespace: namespace.clone(),
                    reference: Reference::Tag(Tag::new(tag).unwrap()),
                    content_type: Some(media_type.clone()),
                    tags: Vec::new(),
                    source_ts: None,
                },
                manifest_stream,
            )
            .await
            .expect("put manifest failed");

        assert_eq!(
            *response_header(&response, &LOCATION),
            format!("/v2/{namespace}/manifests/{tag}")
        );

        let repository = registry
            .get_repository_for_namespace(namespace)
            .expect("get repository failed");
        let stored_manifest = registry
            .get_manifest(
                Some(repository),
                &[MediaRange::from(media_type.clone())],
                namespace,
                Reference::Tag(Tag::new(tag).unwrap()),
                false,
                "test-client",
            )
            .await
            .expect("get manifest failed");

        assert_eq!(stored_manifest.content, content);
        assert_eq!(stored_manifest.media_type.unwrap(), media_type);
        assert_eq!(stored_manifest.digest, response_digest(&response));
    })
    .await;
}

#[tokio::test]
async fn test_delete_manifest_with_many_tags() {
    for_each_backend(async |test_case| {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo/delete-many-tags").unwrap();
        let (content_a, media_type_a) = create_test_manifest(registry, namespace).await;
        let (content_b, media_type_b) =
            create_test_manifest_with_subject(registry, namespace).await;

        let response_a = registry
            .put_manifest(
                namespace,
                &Reference::Tag(Tag::new("tag-0").unwrap()),
                Some(&media_type_a),
                &content_a,
            )
            .await
            .unwrap();

        for i in 1..20 {
            registry
                .put_manifest(
                    namespace,
                    &Reference::Tag(Tag::new(&format!("tag-{i}")).unwrap()),
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
                    &Reference::Tag(Tag::new(&format!("other-{i}")).unwrap()),
                    Some(&media_type_b),
                    &content_b,
                )
                .await
                .unwrap();
        }

        registry
            .delete_manifest(
                None,
                None,
                namespace,
                &Reference::Digest(response_a.digest.clone()),
            )
            .await
            .unwrap();

        let repository = registry.get_repository_for_namespace(namespace).unwrap();

        for i in 0..20 {
            assert!(
                registry
                    .get_manifest(
                        Some(repository),
                        &[MediaRange::from(media_type_a.clone())],
                        namespace,
                        Reference::Tag(Tag::new(&format!("tag-{i}")).unwrap()),
                        false,
                        "test-client",
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
                        Some(repository),
                        &[MediaRange::from(media_type_b.clone())],
                        namespace,
                        Reference::Tag(Tag::new(&format!("other-{i}")).unwrap()),
                        false,
                        "test-client",
                    )
                    .await
                    .is_ok(),
                "other-{i} should still exist"
            );
        }

        let tags = registry
            .metadata_store
            .list_tags(namespace, 100, None)
            .await
            .unwrap()
            .items;
        assert_eq!(tags.len(), 20, "expected exactly 20 remaining tags");
    })
    .await;
}

#[tokio::test]
async fn test_put_manifest_stores_media_type() {
    for_each_backend(async |test_case| {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo/media-type-store").unwrap();
        let tag = "latest";
        let (content, media_type) = create_test_manifest(registry, namespace).await;

        let response = registry
            .put_manifest(
                namespace,
                &Reference::Tag(Tag::new(tag).unwrap()),
                Some(&media_type),
                &content,
            )
            .await
            .unwrap();

        let digest_link = LinkKind::Digest(response.digest.clone());
        let link_meta = registry
            .metadata_store
            .read_link(namespace, &digest_link)
            .await
            .unwrap();
        assert_eq!(
            link_meta.media_type,
            Some(media_type.clone()),
            "Digest link should have media_type stored"
        );

        // Tag resolution carries no media type: the revision record above
        // serves it, and the tag's entry body stores it for tag history.
        let tag_link = LinkKind::Tag(Tag::new(tag).unwrap());
        let tag_meta = registry
            .metadata_store
            .read_link(namespace, &tag_link)
            .await
            .unwrap();
        assert_eq!(tag_meta.target, response.digest);
        assert_eq!(tag_meta.media_type, None);
    })
    .await;
}

#[tokio::test]
async fn test_put_manifest_without_content_type_stores_manifest_media_type() {
    for_each_backend(async |test_case| {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo/no-content-type").unwrap();
        let (content, _media_type) = create_test_manifest(registry, namespace).await;

        let response = registry
            .put_manifest(
                namespace,
                &Reference::Tag(Tag::new("latest").unwrap()),
                None,
                &content,
            )
            .await
            .unwrap();

        let digest_link = registry
            .metadata_store
            .read_link(namespace, &LinkKind::Digest(response.digest.clone()))
            .await
            .unwrap();

        assert_eq!(
            digest_link.media_type,
            Some(MediaType::new("application/vnd.docker.distribution.manifest.v2+json").unwrap()),
            "Digest link should have media_type from manifest body"
        );
    })
    .await;
}

fn fixed_digest() -> Digest {
    "sha256:0000000000000000000000000000000000000000000000000000000000000000"
        .parse()
        .unwrap()
}

#[tokio::test]
async fn store_manifest_writes_blob_and_links() {
    let test_case = FSRegistryTestCase::new();
    let registry = test_case.registry();
    let namespace = Namespace::new("test-repo").unwrap();

    let (manifest_bytes, media_type) = create_test_manifest(registry, &namespace).await;
    let expected_digest = Digest::sha256_of_bytes(&manifest_bytes);

    let response = registry
        .put_manifest(
            &namespace,
            &Reference::Tag(Tag::new("v1").unwrap()),
            Some(&media_type),
            &manifest_bytes,
        )
        .await
        .unwrap();

    let stored_digest = response.digest.clone();
    assert_eq!(stored_digest, expected_digest);

    let link = registry
        .metadata_store
        .read_link(&namespace, &LinkKind::Tag(Tag::new("v1").unwrap()))
        .await
        .unwrap();
    assert_eq!(link.target, expected_digest);

    let body = registry.blob_store.read(&expected_digest).await.unwrap();
    assert_eq!(body, manifest_bytes);

    let blob_index = registry
        .metadata_store
        .read_blob_index(&expected_digest)
        .await
        .unwrap();
    assert!(
        blob_index.namespace.contains_key(&namespace),
        "blob-index must contain namespace after manifest push"
    );
}

/// Regression for the cross-store isolation bug: with the blob and metadata
/// stores on separate backends, a manifest must be stored as a blob in the blob
/// store, where reads look, and not in the metadata store.
#[tokio::test]
async fn manifest_blob_lives_in_blob_store_with_split_backends() {
    let test_case = FSRegistryTestCase::with_split_backends();
    let registry = test_case.registry();
    let namespace = Namespace::new("split-repo").unwrap();

    let (manifest_bytes, media_type) = create_test_manifest(registry, &namespace).await;
    let digest = Digest::sha256_of_bytes(&manifest_bytes);

    registry
        .put_manifest(
            &namespace,
            &Reference::Tag(Tag::new("v1").unwrap()),
            Some(&media_type),
            &manifest_bytes,
        )
        .await
        .expect("split-backend manifest push must succeed");

    assert_eq!(
        registry.blob_store.read(&digest).await.unwrap(),
        manifest_bytes,
        "manifest body must be readable from the blob store",
    );
    assert!(
        test_case
            .metadata_store()
            .object_store()
            .get(&digest.blob_path())
            .await
            .is_err(),
        "manifest body must not land in the metadata store",
    );

    registry
        .delete_manifest(None, None, &namespace, &Reference::Digest(digest.clone()))
        .await
        .expect("delete by digest must succeed");
    assert!(
        registry
            .metadata_store
            .read_link(&namespace, &LinkKind::Digest(digest.clone()))
            .await
            .is_err(),
        "the revision must be gone after the delete",
    );
    assert!(
        registry.blob_store.read(&digest).await.is_ok(),
        "the manifest body is the collector's to reclaim",
    );
}

#[tokio::test]
async fn store_manifest_is_idempotent() {
    // Pushing the same manifest bytes twice must not fail: PutIfAbsent for
    // blob-data is idempotent; link writes overwrite with the same data.
    let test_case = FSRegistryTestCase::new();
    let registry = test_case.registry();
    let namespace = Namespace::new("test-repo").unwrap();

    let (manifest_bytes, media_type) = create_test_manifest(registry, &namespace).await;

    registry
        .put_manifest(
            &namespace,
            &Reference::Tag(Tag::new("v1").unwrap()),
            Some(&media_type),
            &manifest_bytes,
        )
        .await
        .unwrap();

    registry
        .put_manifest(
            &namespace,
            &Reference::Tag(Tag::new("v1").unwrap()),
            Some(&media_type),
            &manifest_bytes,
        )
        .await
        .unwrap();

    let digest = Digest::sha256_of_bytes(&manifest_bytes);
    let link = registry
        .metadata_store
        .read_link(&namespace, &LinkKind::Tag(Tag::new("v1").unwrap()))
        .await
        .unwrap();
    assert_eq!(link.target, digest);
}

#[tokio::test]
async fn delete_manifest_removes_the_revision_link() {
    let test_case = FSRegistryTestCase::new();
    let registry = test_case.registry();
    let namespace = Namespace::new("test-repo").unwrap();

    let (manifest_bytes, media_type) = create_test_manifest(registry, &namespace).await;
    let digest = Digest::sha256_of_bytes(&manifest_bytes);

    registry
        .put_manifest(
            &namespace,
            &Reference::Tag(Tag::new("v1").unwrap()),
            Some(&media_type),
            &manifest_bytes,
        )
        .await
        .unwrap();

    registry
        .metadata_store
        .read_link(&namespace, &LinkKind::Tag(Tag::new("v1").unwrap()))
        .await
        .unwrap();

    registry
        .delete_manifest(None, None, &namespace, &Reference::Digest(digest.clone()))
        .await
        .unwrap();

    let result = registry
        .metadata_store
        .read_link(&namespace, &LinkKind::Digest(digest.clone()))
        .await;
    assert!(
        matches!(result, Err(Error::NotFound)),
        "digest link should be gone after delete, got: {result:?}"
    );
}

// Receiver-side last-writer-wins (LWW)

async fn seed_tag(registry: &Registry, namespace: &Namespace, tag: &str) -> (Vec<u8>, MediaType) {
    let (content, media_type) = create_test_manifest(registry, namespace).await;
    registry
        .accept_put_manifest(
            None,
            PutManifestRequest {
                namespace: namespace.clone(),
                reference: Reference::Tag(Tag::new(tag).unwrap()),
                content_type: Some(media_type.clone()),
                tags: Vec::new(),
                source_ts: None,
            },
            Cursor::new(content.clone()),
        )
        .await
        .expect("seed tag push");
    (content, media_type)
}

/// Tag state persists its timestamp at millisecond precision (the entry
/// ordinal), so exact round-trip assertions feed ms-precision inputs.
fn entry_ms(ts: chrono::DateTime<chrono::Utc>) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::from_timestamp_millis(ts.timestamp_millis()).unwrap()
}

async fn local_created_at(
    registry: &Registry,
    namespace: &Namespace,
    tag: &str,
) -> chrono::DateTime<chrono::Utc> {
    registry
        .metadata_store
        .read_link(namespace, &LinkKind::Tag(Tag::new(tag).unwrap()))
        .await
        .expect("read seeded tag link")
        .created_at
        .expect("seeded tag has created_at")
}

#[tokio::test]
async fn accept_put_manifest_stamps_created_at_from_source_ts() {
    // LWW and retention track author time across hops, not the receiver's clock.
    let test_case = FSRegistryTestCase::new();
    let registry = test_case.registry();
    let namespace = &Namespace::new("lww-repo").unwrap();
    let tag = "latest";

    let (content, media_type) = create_test_manifest(registry, namespace).await;
    let source_ts = entry_ms(chrono::Utc::now() - chrono::Duration::hours(3));

    registry
        .accept_put_manifest(
            None,
            PutManifestRequest {
                namespace: namespace.clone(),
                reference: Reference::Tag(Tag::new(tag).unwrap()),
                content_type: Some(media_type),
                tags: Vec::new(),
                source_ts: Some(source_ts),
            },
            Cursor::new(content),
        )
        .await
        .expect("replicated push must store");

    assert_eq!(
        local_created_at(registry, namespace, tag).await,
        source_ts,
        "a replicated write must stamp created_at = source_ts (author time)"
    );
}

#[tokio::test]
async fn accept_put_manifest_without_source_ts_stamps_local_clock() {
    let test_case = FSRegistryTestCase::new();
    let registry = test_case.registry();
    let namespace = &Namespace::new("lww-repo").unwrap();
    let tag = "latest";

    let before = chrono::Utc::now();
    let (content, media_type) = create_test_manifest(registry, namespace).await;
    registry
        .accept_put_manifest(
            None,
            PutManifestRequest {
                namespace: namespace.clone(),
                reference: Reference::Tag(Tag::new(tag).unwrap()),
                content_type: Some(media_type),
                tags: Vec::new(),
                source_ts: None,
            },
            Cursor::new(content),
        )
        .await
        .expect("client push must store");

    assert!(
        local_created_at(registry, namespace, tag).await >= before,
        "a client write (no source_ts) must stamp the local clock"
    );
}

#[tokio::test]
async fn accept_put_manifest_rejects_lww_older_source_ts() {
    let test_case = FSRegistryTestCase::new();
    let registry = test_case.registry();
    let namespace = &Namespace::new("lww-repo").unwrap();
    let tag = "latest";

    let (content, media_type) = seed_tag(registry, namespace, tag).await;
    let created_at = local_created_at(registry, namespace, tag).await;
    let older = created_at - chrono::Duration::seconds(60);

    let result = registry
        .accept_put_manifest(
            None,
            PutManifestRequest {
                namespace: namespace.clone(),
                reference: Reference::Tag(Tag::new(tag).unwrap()),
                content_type: Some(media_type),
                tags: Vec::new(),
                source_ts: Some(older),
            },
            Cursor::new(content),
        )
        .await
        .err();

    assert!(
        matches!(result, Some(Error::ReplicationSuperseded(_))),
        "older source_ts must be superseded by the newer local tag, got: {result:?}"
    );
}

#[tokio::test]
async fn accept_put_manifest_accepts_lww_newer_source_ts() {
    let test_case = FSRegistryTestCase::new();
    let registry = test_case.registry();
    let namespace = &Namespace::new("lww-repo").unwrap();
    let tag = "latest";

    let (content, media_type) = seed_tag(registry, namespace, tag).await;
    let created_at = local_created_at(registry, namespace, tag).await;
    let newer = created_at + chrono::Duration::seconds(60);

    registry
        .accept_put_manifest(
            None,
            PutManifestRequest {
                namespace: namespace.clone(),
                reference: Reference::Tag(Tag::new(tag).unwrap()),
                content_type: Some(media_type),
                tags: Vec::new(),
                source_ts: Some(newer),
            },
            Cursor::new(content),
        )
        .await
        .expect("newer source_ts must win over the older local tag");
}

#[tokio::test]
async fn accept_put_manifest_accepts_lww_equal_source_ts() {
    // Rejecting an equal-timestamp converged replay (a regression to `>=`)
    // would make two converged nodes bounce 409s.
    let test_case = FSRegistryTestCase::new();
    let registry = test_case.registry();
    let namespace = &Namespace::new("lww-repo").unwrap();
    let tag = "latest";

    let (content, media_type) = seed_tag(registry, namespace, tag).await;
    let created_at = local_created_at(registry, namespace, tag).await;

    registry
        .accept_put_manifest(
            None,
            PutManifestRequest {
                namespace: namespace.clone(),
                reference: Reference::Tag(Tag::new(tag).unwrap()),
                content_type: Some(media_type),
                tags: Vec::new(),
                source_ts: Some(created_at),
            },
            Cursor::new(content),
        )
        .await
        .expect("equal source_ts must converge (not be superseded)");
}

/// Two distinct manifests in `namespace`, returned larger digest first.
async fn two_manifests_by_digest_order(
    registry: &Registry,
    namespace: &Namespace,
) -> ((Vec<u8>, MediaType), (Vec<u8>, MediaType)) {
    let first = create_test_manifest(registry, namespace).await;

    let config_content = br#"{"architecture":"arm64","os":"linux"}"#;
    let layer_content = b"a different layer content";
    let config_digest = upload_blob(registry, namespace, config_content).await;
    let layer_digest = upload_blob(registry, namespace, layer_content).await;
    let second = manifest_with_references(
        &config_digest,
        config_content.len(),
        &layer_digest,
        layer_content.len(),
    );

    let first_digest = Digest::sha256_of_bytes(&first.0);
    let second_digest = Digest::sha256_of_bytes(&second.0);
    if first_digest > second_digest {
        (first, second)
    } else {
        (second, first)
    }
}

#[tokio::test]
async fn accept_put_manifest_lww_equal_ts_tie_breaks_on_digest() {
    // Without the digest tie-break an equal-timestamp A<->B pair would swap
    // digests forever; the larger digest must win on every node.
    let test_case = FSRegistryTestCase::new();
    let registry = test_case.registry();
    let namespace = &Namespace::new("lww-repo").unwrap();
    let tag = "latest";

    let ((larger, larger_mt), (smaller, smaller_mt)) =
        two_manifests_by_digest_order(registry, namespace).await;
    let ts = chrono::Utc::now() - chrono::Duration::hours(1);

    registry
        .accept_put_manifest(
            None,
            PutManifestRequest {
                namespace: namespace.clone(),
                reference: Reference::Tag(Tag::new(tag).unwrap()),
                content_type: Some(larger_mt),
                tags: Vec::new(),
                source_ts: Some(ts),
            },
            Cursor::new(larger),
        )
        .await
        .expect("seed the larger-digest manifest");

    let result = registry
        .accept_put_manifest(
            None,
            PutManifestRequest {
                namespace: namespace.clone(),
                reference: Reference::Tag(Tag::new(tag).unwrap()),
                content_type: Some(smaller_mt),
                tags: Vec::new(),
                source_ts: Some(ts),
            },
            Cursor::new(smaller),
        )
        .await
        .err();
    assert!(
        matches!(result, Some(Error::ReplicationSuperseded(_))),
        "an equal-timestamp write with a smaller digest must be superseded, got: {result:?}"
    );
}

#[tokio::test]
async fn accept_put_manifest_lww_equal_ts_accepts_larger_digest() {
    let test_case = FSRegistryTestCase::new();
    let registry = test_case.registry();
    let namespace = &Namespace::new("lww-repo").unwrap();
    let tag = "latest";

    let ((larger, larger_mt), (smaller, smaller_mt)) =
        two_manifests_by_digest_order(registry, namespace).await;
    let larger_digest = Digest::sha256_of_bytes(&larger);
    let ts = chrono::Utc::now() - chrono::Duration::hours(1);

    registry
        .accept_put_manifest(
            None,
            PutManifestRequest {
                namespace: namespace.clone(),
                reference: Reference::Tag(Tag::new(tag).unwrap()),
                content_type: Some(smaller_mt),
                tags: Vec::new(),
                source_ts: Some(ts),
            },
            Cursor::new(smaller),
        )
        .await
        .expect("seed the smaller-digest manifest");

    registry
        .accept_put_manifest(
            None,
            PutManifestRequest {
                namespace: namespace.clone(),
                reference: Reference::Tag(Tag::new(tag).unwrap()),
                content_type: Some(larger_mt),
                tags: Vec::new(),
                source_ts: Some(ts),
            },
            Cursor::new(larger),
        )
        .await
        .expect("an equal-timestamp write with a larger digest must win");

    let target = registry
        .metadata_store
        .read_link_reference(namespace, &LinkKind::Tag(Tag::new(tag).unwrap()))
        .await
        .expect("tag link after the tie-break")
        .target;
    assert_eq!(
        target, larger_digest,
        "the pair must converge on the larger digest"
    );
}

#[tokio::test]
async fn accept_put_manifest_lww_reads_bypass_the_link_cache() {
    // The per-process link cache can lag a sibling replica's write by up to
    // its TTL, so the LWW gate must read the backend link, not the cached one.
    let test_case = FSRegistryTestCase::with_link_cache_ttl(300);
    let registry = test_case.registry();
    let namespace = &Namespace::new("lww-repo").unwrap();
    let tag = "latest";

    let (content, media_type) = seed_tag(registry, namespace, tag).await;
    let created_at = local_created_at(registry, namespace, tag).await;

    // Assert the seed warmed the cache so the stale-cache scenario stays honest.
    let store = test_case.metadata_store();
    let link = LinkKind::Tag(Tag::new(tag).unwrap());
    let cached = store
        .cache_get(namespace, &link)
        .await
        .expect("seed write must warm the link cache");
    assert_eq!(cached.created_at, Some(created_at));

    // Simulate a sibling replica moving created_at forward behind this
    // process's cache.
    let sibling_ts = created_at + chrono::Duration::seconds(120);
    let mut sibling = store
        .read_link_reference(namespace, &link)
        .await
        .expect("seeded tag link");
    sibling.created_at = Some(sibling_ts);
    store
        .write_tag_state(namespace, &Tag::new(tag).unwrap(), &sibling)
        .await
        .expect("sibling write behind the cache");

    // Newer than the cached created_at but older than the sibling's, so only
    // an uncached gate read rejects.
    let incoming = created_at + chrono::Duration::seconds(60);
    let result = registry
        .accept_put_manifest(
            None,
            PutManifestRequest {
                namespace: namespace.clone(),
                reference: Reference::Tag(Tag::new(tag).unwrap()),
                content_type: Some(media_type),
                tags: Vec::new(),
                source_ts: Some(incoming),
            },
            Cursor::new(content),
        )
        .await
        .err();

    assert!(
        matches!(result, Some(Error::ReplicationSuperseded(_))),
        "LWW must compare against the backend link, not the stale cached one, got: {result:?}"
    );
}

#[tokio::test]
async fn find_tags_pointing_at_bypasses_the_link_cache() {
    // This set gates the digest-delete LWW guard, so a tag re-pointed at the
    // digest on a sibling replica within the cache TTL must still be found, not
    // omitted because the local cache lags and would then be left dangling.
    let test_case = FSRegistryTestCase::with_link_cache_ttl(300);
    let store = test_case.metadata_store();
    let namespace = Namespace::new("cascade-repo").unwrap();
    let link = LinkKind::Tag(Tag::new("latest").unwrap());

    let old_digest = Digest::sha256_of_bytes(b"old");
    let new_digest = Digest::sha256_of_bytes(b"new");

    store
        .store_manifest(
            &namespace,
            &[LinkOperation::create(link.clone(), old_digest.clone())],
            None,
            ReferencePolicy::Strict,
        )
        .await
        .expect("seed the tag and warm the cache");
    assert_eq!(
        store.cache_get(&namespace, &link).await.map(|m| m.target),
        Some(old_digest.clone()),
        "seed write must warm the link cache",
    );

    // A sibling replica re-points the tag at new_digest behind this cache,
    // one millisecond later so its entry wins resolution outright.
    let mut sibling = store.read_link_reference(&namespace, &link).await.unwrap();
    sibling.target = new_digest.clone();
    sibling.created_at = sibling
        .created_at
        .map(|ts| ts + chrono::Duration::milliseconds(1));
    store
        .write_tag_state(&namespace, &Tag::new("latest").unwrap(), &sibling)
        .await
        .expect("sibling re-point behind the cache");
    assert_eq!(
        store.cache_get(&namespace, &link).await.map(|m| m.target),
        Some(old_digest),
        "cache must still be stale for an honest test",
    );

    let pointing = store
        .find_tags_pointing_at(&namespace, &new_digest)
        .await
        .unwrap();
    assert_eq!(
        pointing,
        vec![link],
        "must find the tag by its backend target, not the stale cached one"
    );
}

/// The pre-write LWW gate in `accept_put_manifest` is check-then-write, so a
/// racing writer can pass it and still commit out of order. The commit itself
/// must reject a superseded replicated write, so calling the store directly (as
/// if the gate had been raced) must fail and leave the winning link untouched.
#[tokio::test]
async fn store_manifest_enforces_lww_inside_the_link_transaction() {
    let test_case = FSRegistryTestCase::new();
    let store = test_case.metadata_store();
    let namespace = Namespace::new("lww-tx-repo").unwrap();
    let link = LinkKind::Tag(Tag::new("latest").unwrap());

    let newer_body = br#"{"newer":true}"#.to_vec();
    let newer_digest = Digest::sha256_of_bytes(&newer_body);
    let newer_ts = entry_ms(chrono::Utc::now());
    store
        .store_manifest(
            &namespace,
            &[LinkOperation::create(link.clone(), newer_digest.clone())],
            Some(newer_ts),
            ReferencePolicy::Strict,
        )
        .await
        .expect("seed the newer replicated write");

    let older_body = br#"{"older":true}"#.to_vec();
    let older_digest = Digest::sha256_of_bytes(&older_body);
    let result = store
        .store_manifest(
            &namespace,
            &[LinkOperation::create(link.clone(), older_digest.clone())],
            Some(newer_ts - chrono::Duration::hours(1)),
            ReferencePolicy::Strict,
        )
        .await
        .err();
    assert!(
        matches!(result, Some(Error::ReplicationSuperseded(_))),
        "an older replicated write must be rejected by the transaction itself, got: {result:?}"
    );

    let metadata = store
        .read_link_reference(&namespace, &link)
        .await
        .expect("the winning link must survive");
    assert_eq!(metadata.target, newer_digest);
    assert_eq!(metadata.created_at, Some(newer_ts));
}

/// A delete or prune can reclaim a referenced blob while a push is in flight.
/// The commit owns the ownership check, so a strict push whose reference lost
/// its shard entry must be rejected rather than commit a manifest whose layer
/// bytes are gone.
#[tokio::test]
async fn store_manifest_strict_rejects_a_reference_whose_grant_was_reclaimed() {
    let test_case = FSRegistryTestCase::new();
    let store = test_case.metadata_store();
    let namespace = Namespace::new("guard-repo").unwrap();

    let manifest_digest = Digest::sha256_of_bytes(b"guard-manifest");
    let layer_digest = Digest::sha256_of_bytes(b"guard-layer");
    let ops = [
        LinkOperation::create(
            LinkKind::Digest(manifest_digest.clone()),
            manifest_digest.clone(),
        ),
        LinkOperation::create_with_referrer(
            LinkKind::Layer(layer_digest.clone()),
            layer_digest.clone(),
            manifest_digest.clone(),
        ),
    ];

    let result = store
        .store_manifest(&namespace, &ops, None, ReferencePolicy::Strict)
        .await
        .err();
    assert!(
        matches!(result, Some(Error::ManifestBlobUnknown)),
        "a strict reference without a live grant must fail the push, got: {result:?}"
    );
    assert!(
        store
            .read_link_reference(&namespace, &LinkKind::Digest(manifest_digest))
            .await
            .is_err(),
        "the rejected push must not commit any link"
    );
}

#[tokio::test]
async fn store_manifest_strict_accepts_a_reference_with_a_live_grant() {
    let test_case = FSRegistryTestCase::new();
    let store = test_case.metadata_store();
    let namespace = Namespace::new("guard-repo").unwrap();

    let manifest_digest = Digest::sha256_of_bytes(b"granted-manifest");
    let layer_digest = Digest::sha256_of_bytes(b"granted-layer");
    store
        .grant(&namespace, &layer_digest)
        .await
        .expect("seed the layer's ownership grant");

    let ops = [
        LinkOperation::create(
            LinkKind::Digest(manifest_digest.clone()),
            manifest_digest.clone(),
        ),
        LinkOperation::create_with_referrer(
            LinkKind::Layer(layer_digest.clone()),
            layer_digest.clone(),
            manifest_digest.clone(),
        ),
    ];

    store
        .store_manifest(&namespace, &ops, None, ReferencePolicy::Strict)
        .await
        .expect("a strict push with a live grant must commit");
    let links = store
        .read_blob_index_namespace(&namespace, &layer_digest)
        .await
        .expect("the layer's reference entries must be readable");
    assert!(
        links.contains(&LinkKind::ReferencedBy(manifest_digest)),
        "the per-referrer entry must be written"
    );
}

/// A manifest delete leaves its reference keys behind, and the advisory link
/// file until the collector prunes it. Once that file is pruned the stale keys
/// alone must not pass the Strict ownership gate, or a push could mint a backed
/// reference to a layer it never uploaded.
#[tokio::test]
async fn store_manifest_strict_rejects_stale_references_of_a_deleted_manifest() {
    let test_case = FSRegistryTestCase::new();
    let store = test_case.metadata_store();
    let namespace = Namespace::new("stale-guard-repo").unwrap();

    let layer_digest = Digest::sha256_of_bytes(b"stale-guard-layer");
    let push_ops = |manifest: &Digest| {
        [
            LinkOperation::create(LinkKind::Digest(manifest.clone()), manifest.clone()),
            LinkOperation::create_with_referrer(
                LinkKind::Layer(layer_digest.clone()),
                layer_digest.clone(),
                manifest.clone(),
            ),
        ]
    };

    let first = Digest::sha256_of_bytes(b"stale-guard-first");
    store
        .store_manifest(
            &namespace,
            &push_ops(&first),
            None,
            ReferencePolicy::Trusted,
        )
        .await
        .expect("seed the first manifest");

    // While the first manifest lives its entry backs the reference, so the
    // same push passes the Strict gate.
    let second = Digest::sha256_of_bytes(b"stale-guard-second");
    store
        .store_manifest(
            &namespace,
            &push_ops(&second),
            None,
            ReferencePolicy::Strict,
        )
        .await
        .expect("a Strict push backed by a live reference must commit");

    for manifest in [&first, &second] {
        store
            .delete_manifest(
                &namespace,
                &[
                    LinkOperation::delete(LinkKind::Digest((*manifest).clone())),
                    LinkOperation::delete_with_referrer(
                        LinkKind::Layer(layer_digest.clone()),
                        (*manifest).clone(),
                    ),
                ],
                None,
            )
            .await
            .expect("delete the manifest");
    }
    // The collector's prune of the advisory link file; the reference keys
    // stay behind for its slower age-out.
    store
        .update_links(
            &namespace,
            &[LinkOperation::delete_with_referrer(
                LinkKind::Layer(layer_digest.clone()),
                first.clone(),
            )],
        )
        .await
        .expect("prune the advisory link file");

    let stale = store
        .read_blob_index_namespace(&namespace, &layer_digest)
        .await
        .expect("the stale reference keys must still exist");
    assert!(
        stale.contains(&LinkKind::ReferencedBy(first.clone())),
        "the deleted manifest's reference key is the collector's to prune, got: {stale:?}"
    );

    let third = Digest::sha256_of_bytes(b"stale-guard-third");
    let result = store
        .store_manifest(&namespace, &push_ops(&third), None, ReferencePolicy::Strict)
        .await
        .err();
    assert!(
        matches!(result, Some(Error::ManifestBlobUnknown)),
        "stale reference keys must not pass the Strict ownership gate, got: {result:?}"
    );
    assert!(
        store
            .read_link_reference(&namespace, &LinkKind::Digest(third))
            .await
            .is_err(),
        "the rejected push must not commit any link"
    );
}

/// A Trusted (pull-through) push references content whose grants may not exist
/// yet, so it must keep creating first grants on an absent shard.
#[tokio::test]
async fn store_manifest_trusted_creates_first_grant_without_prior_entry() {
    let test_case = FSRegistryTestCase::new();
    let store = test_case.metadata_store();
    let namespace = Namespace::new("guard-repo").unwrap();

    let manifest_digest = Digest::sha256_of_bytes(b"trusted-manifest");
    let layer_digest = Digest::sha256_of_bytes(b"trusted-layer");
    let ops = [
        LinkOperation::create(
            LinkKind::Digest(manifest_digest.clone()),
            manifest_digest.clone(),
        ),
        LinkOperation::create_with_referrer(
            LinkKind::Layer(layer_digest.clone()),
            layer_digest.clone(),
            manifest_digest.clone(),
        ),
    ];

    store
        .store_manifest(&namespace, &ops, None, ReferencePolicy::Trusted)
        .await
        .expect("a trusted push must commit without a prior grant");
    let links = store
        .read_blob_index_namespace(&namespace, &layer_digest)
        .await
        .expect("the layer's reference entries must be readable");
    assert!(
        links.contains(&LinkKind::ReferencedBy(manifest_digest)),
        "the per-referrer entry must be written"
    );
}

/// A permissive push must not grant access to an unowned reference: the commit
/// drops the unowned tracked link and keeps the rest.
#[tokio::test]
async fn store_manifest_permissive_drops_an_unowned_reference() {
    let test_case = FSRegistryTestCase::new();
    let store = test_case.metadata_store();
    let namespace = Namespace::new("guard-repo").unwrap();

    let manifest_digest = Digest::sha256_of_bytes(b"permissive-manifest");
    let layer_digest = Digest::sha256_of_bytes(b"permissive-layer");
    let ops = [
        LinkOperation::create(
            LinkKind::Digest(manifest_digest.clone()),
            manifest_digest.clone(),
        ),
        LinkOperation::create_with_referrer(
            LinkKind::Layer(layer_digest.clone()),
            layer_digest.clone(),
            manifest_digest.clone(),
        ),
    ];

    store
        .store_manifest(&namespace, &ops, None, ReferencePolicy::Permissive)
        .await
        .expect("a permissive push must commit without the unowned link");
    store
        .read_link_reference(&namespace, &LinkKind::Digest(manifest_digest))
        .await
        .expect("the digest self-link must be written");
    assert!(
        store
            .read_link_reference(&namespace, &LinkKind::Layer(layer_digest))
            .await
            .is_err(),
        "the unowned layer link must be dropped"
    );
}

/// The pre-write delete gate in `delete_manifest` is also check-then-write, so a
/// newer re-put can land between the gate and the commit. The commit must reject
/// a superseded replicated delete itself, leaving the tag intact.
#[tokio::test]
async fn delete_links_enforces_lww_inside_the_link_transaction() {
    let test_case = FSRegistryTestCase::new();
    let store = test_case.metadata_store();
    let namespace = Namespace::new("lww-tx-repo").unwrap();
    let link = LinkKind::Tag(Tag::new("latest").unwrap());

    let body = br#"{"winner":true}"#.to_vec();
    let digest = Digest::sha256_of_bytes(&body);
    let newer_ts = entry_ms(chrono::Utc::now());
    store
        .store_manifest(
            &namespace,
            &[LinkOperation::create(link.clone(), digest.clone())],
            Some(newer_ts),
            ReferencePolicy::Strict,
        )
        .await
        .expect("seed the newer tag");

    let result = store
        .delete_links(
            &namespace,
            &[LinkOperation::delete(link.clone())],
            Some(newer_ts - chrono::Duration::hours(1)),
        )
        .await
        .err();
    assert!(
        matches!(result, Some(Error::ReplicationSuperseded(_))),
        "an older replicated delete must be rejected by the transaction itself, got: {result:?}"
    );

    let metadata = store
        .read_link_reference(&namespace, &link)
        .await
        .expect("the tag must survive the superseded delete");
    assert_eq!(metadata.target, digest);
    assert_eq!(metadata.created_at, Some(newer_ts));
}

#[tokio::test]
async fn put_manifest_reports_changed_from_the_committed_transaction() {
    let test_case = FSRegistryTestCase::new();
    let registry = test_case.registry();
    let namespace = &Namespace::new("test-repo").unwrap();
    let tag_ref = Reference::Tag(Tag::new("latest").unwrap());

    let (content_a, media_type_a) = create_test_manifest(registry, namespace).await;

    let first = registry
        .put_manifest(namespace, &tag_ref, Some(&media_type_a), &content_a)
        .await
        .expect("fresh tag push");
    assert!(first.changed, "a fresh tag push must report changed");

    let replay = registry
        .put_manifest(namespace, &tag_ref, Some(&media_type_a), &content_a)
        .await
        .expect("tag re-assert");
    assert!(
        !replay.changed,
        "a tag re-asserted to the same digest must report unchanged"
    );

    let digest_ref = Reference::Digest(first.digest.clone());
    let digest_replay = registry
        .put_manifest(namespace, &digest_ref, Some(&media_type_a), &content_a)
        .await
        .expect("digest re-push");
    assert!(
        !digest_replay.changed,
        "re-pushing an already-present revision must report unchanged"
    );

    let layer_content = b"a different layer content";
    let config_content = br#"{"architecture":"arm64","os":"linux"}"#;
    let config_digest = upload_blob(registry, namespace, config_content).await;
    let layer_digest = upload_blob(registry, namespace, layer_content).await;
    let (content_b, media_type_b) = manifest_with_references(
        &config_digest,
        config_content.len(),
        &layer_digest,
        layer_content.len(),
    );
    let moved = registry
        .put_manifest(namespace, &tag_ref, Some(&media_type_b), &content_b)
        .await
        .expect("tag move");
    assert!(
        moved.changed,
        "moving the tag to a different digest must report changed"
    );
}

#[tokio::test]
async fn accept_put_manifest_accepts_lww_when_local_absent() {
    let test_case = FSRegistryTestCase::new();
    let registry = test_case.registry();
    let namespace = &Namespace::new("lww-repo").unwrap();
    let tag = "fresh";

    let (content, media_type) = create_test_manifest(registry, namespace).await;
    let very_old = chrono::Utc::now() - chrono::Duration::days(3650);

    registry
        .accept_put_manifest(
            None,
            PutManifestRequest {
                namespace: namespace.clone(),
                reference: Reference::Tag(Tag::new(tag).unwrap()),
                content_type: Some(media_type),
                tags: Vec::new(),
                source_ts: Some(very_old),
            },
            Cursor::new(content),
        )
        .await
        .expect("absent local tag must accept any source_ts");
}

#[tokio::test]
async fn accept_put_manifest_without_source_ts_skips_lww() {
    let test_case = FSRegistryTestCase::new();
    let registry = test_case.registry();
    let namespace = &Namespace::new("lww-repo").unwrap();
    let tag = "latest";

    let (content, media_type) = seed_tag(registry, namespace, tag).await;

    registry
        .accept_put_manifest(
            None,
            PutManifestRequest {
                namespace: namespace.clone(),
                reference: Reference::Tag(Tag::new(tag).unwrap()),
                content_type: Some(media_type),
                tags: Vec::new(),
                source_ts: None,
            },
            Cursor::new(content),
        )
        .await
        .expect("client write (no source_ts) must skip LWW");
}

#[tokio::test]
async fn accept_put_manifest_digest_reference_skips_lww() {
    let test_case = FSRegistryTestCase::new();
    let registry = test_case.registry();
    let namespace = &Namespace::new("lww-repo").unwrap();

    let (content, media_type) = create_test_manifest(registry, namespace).await;
    let digest = Digest::sha256_of_bytes(&content);
    let very_old = chrono::Utc::now() - chrono::Duration::days(3650);

    registry
        .accept_put_manifest(
            None,
            PutManifestRequest {
                namespace: namespace.clone(),
                reference: Reference::Digest(digest),
                content_type: Some(media_type),
                tags: Vec::new(),
                source_ts: Some(very_old),
            },
            Cursor::new(content),
        )
        .await
        .expect("digest reference must skip LWW");
}

#[tokio::test]
async fn delete_manifest_rejects_lww_older_source_ts() {
    let test_case = FSRegistryTestCase::new();
    let registry = test_case.registry();
    let namespace = &Namespace::new("lww-repo").unwrap();
    let tag = "latest";

    seed_tag(registry, namespace, tag).await;
    let created_at = local_created_at(registry, namespace, tag).await;
    let older = created_at - chrono::Duration::seconds(60);

    let result = registry
        .delete_manifest(
            None,
            Some(older),
            namespace,
            &Reference::Tag(Tag::new(tag).unwrap()),
        )
        .await
        .err();

    assert!(
        matches!(result, Some(Error::ReplicationSuperseded(_))),
        "older source_ts delete must be superseded, got: {result:?}"
    );

    registry
        .metadata_store
        .read_link(namespace, &LinkKind::Tag(Tag::new(tag).unwrap()))
        .await
        .expect("tag must survive a superseded delete");
}

#[tokio::test]
async fn delete_manifest_accepts_lww_newer_source_ts() {
    let test_case = FSRegistryTestCase::new();
    let registry = test_case.registry();
    let namespace = &Namespace::new("lww-repo").unwrap();
    let tag = "latest";

    seed_tag(registry, namespace, tag).await;
    let created_at = local_created_at(registry, namespace, tag).await;
    let newer = created_at + chrono::Duration::seconds(60);

    registry
        .delete_manifest(
            None,
            Some(newer),
            namespace,
            &Reference::Tag(Tag::new(tag).unwrap()),
        )
        .await
        .expect("newer source_ts delete must win");

    let result = registry
        .metadata_store
        .read_link(namespace, &LinkKind::Tag(Tag::new(tag).unwrap()))
        .await;
    assert!(
        matches!(result, Err(Error::NotFound)),
        "tag must be gone after an accepted delete, got: {result:?}"
    );
}

#[tokio::test]
async fn same_digest_re_push_preserves_created_at() {
    // An idempotent re-push (e.g. CI re-pushing an unchanged image) must not
    // advance the tag's LWW timestamp: the binding is unchanged so dispatch is
    // suppressed, and a bumped created_at would let an interleaved peer write
    // lose locally yet win on peers.
    let test_case = FSRegistryTestCase::new();
    let registry = test_case.registry();
    let namespace = &Namespace::new("lww-repo").unwrap();
    let tag = "latest";

    let (content, media_type) = seed_tag(registry, namespace, tag).await;
    let created_at = local_created_at(registry, namespace, tag).await;

    registry
        .accept_put_manifest(
            None,
            PutManifestRequest {
                namespace: namespace.clone(),
                reference: Reference::Tag(Tag::new(tag).unwrap()),
                content_type: Some(media_type),
                tags: Vec::new(),
                source_ts: None,
            },
            Cursor::new(content),
        )
        .await
        .expect("idempotent re-push of the same digest");

    assert_eq!(
        local_created_at(registry, namespace, tag).await,
        created_at,
        "a same-digest re-push must not bump the tag's created_at"
    );
}

#[tokio::test]
async fn replicated_delete_not_superseded_by_a_timestamp_less_tag_entry() {
    // A tag entry carrying the never-wins ordinal (no author timestamp) must
    // never win LWW; a synthesised now() would re-stamp fresher on every read
    // and block every replicated write to the tag forever.
    let test_case = FSRegistryTestCase::new();
    let registry = test_case.registry();
    let namespace = &Namespace::new("legacy-repo").unwrap();
    let link = LinkKind::Tag(Tag::new("latest").unwrap());

    let legacy_digest = Digest::sha256_of_bytes(b"legacy-manifest");
    registry
        .metadata_store
        .object_store()
        .put(
            &namespace.tag_entry_path(
                &Tag::new("latest").unwrap(),
                tag_ord(None),
                false,
                &legacy_digest,
            ),
            Bytes::from_static(b"{}"),
        )
        .await
        .unwrap();

    let seeded = registry
        .metadata_store
        .read_link(namespace, &link)
        .await
        .expect("the seeded entry must resolve before the delete");
    assert_eq!(seeded.target, legacy_digest);
    assert!(
        seeded.created_at.is_none(),
        "the never-wins ordinal must decode to no author timestamp"
    );

    let ancient = chrono::DateTime::from_timestamp(0, 0).unwrap();
    registry
        .delete_manifest(
            None,
            Some(ancient),
            namespace,
            &Reference::Tag(Tag::new("latest").unwrap()),
        )
        .await
        .expect("a legacy tag (no created_at) must never supersede a replicated write");

    let result = registry.metadata_store.read_link(namespace, &link).await;
    assert!(
        matches!(result, Err(Error::NotFound)),
        "the timestamp-less tag must be gone after the non-superseded delete, got: {result:?}"
    );
}

#[tokio::test]
async fn delete_manifest_digest_rejects_lww_when_pointing_tag_newer() {
    // The cascade must not drop a tag re-pointed after the delete was
    // authored, nor the revision it still references.
    let test_case = FSRegistryTestCase::new();
    let registry = test_case.registry();
    let namespace = &Namespace::new("lww-repo").unwrap();
    let tag = "latest";

    let (content, _) = seed_tag(registry, namespace, tag).await;
    let digest = Digest::sha256_of_bytes(&content);
    let older = local_created_at(registry, namespace, tag).await - chrono::Duration::seconds(60);

    let result = registry
        .delete_manifest(
            None,
            Some(older),
            namespace,
            &Reference::Digest(digest.clone()),
        )
        .await
        .err();

    assert!(
        matches!(result, Some(Error::ReplicationSuperseded(_))),
        "digest delete older than a pointing tag must be superseded, got: {result:?}"
    );

    registry
        .metadata_store
        .read_link(namespace, &LinkKind::Tag(Tag::new(tag).unwrap()))
        .await
        .expect("pointing tag must survive a superseded digest delete");
    registry
        .metadata_store
        .read_link(namespace, &LinkKind::Digest(digest))
        .await
        .expect("revision must survive a superseded digest delete");
}

#[tokio::test]
async fn delete_manifest_digest_accepts_lww_when_newer_than_pointing_tags() {
    let test_case = FSRegistryTestCase::new();
    let registry = test_case.registry();
    let namespace = &Namespace::new("lww-repo").unwrap();
    let tag = "latest";

    let (content, _) = seed_tag(registry, namespace, tag).await;
    let digest = Digest::sha256_of_bytes(&content);
    let newer = local_created_at(registry, namespace, tag).await + chrono::Duration::seconds(60);

    registry
        .delete_manifest(None, Some(newer), namespace, &Reference::Digest(digest))
        .await
        .expect("digest delete newer than every pointing tag must win");

    let tag_result = registry
        .metadata_store
        .read_link(namespace, &LinkKind::Tag(Tag::new(tag).unwrap()))
        .await;
    assert!(
        matches!(tag_result, Err(Error::NotFound)),
        "pointing tag must be removed by an accepted digest delete, got: {tag_result:?}"
    );
}

#[tokio::test]
async fn prune_delete_stamped_source_ts_suppressed_when_local_tag_newer_else_proceeds() {
    // Exercises the prune wire round trip: source_ts is stamped, serialized to
    // RFC 3339 (the `X-Angos-Source-Timestamp` header), and reparsed before
    // reaching `delete_manifest`.
    let test_case = FSRegistryTestCase::new();
    let registry = test_case.registry();
    let namespace = &Namespace::new("prune-repo").unwrap();
    let tag = "stray";

    seed_tag(registry, namespace, tag).await;
    let created_at = local_created_at(registry, namespace, tag).await;

    // Suppressed: the stamped source_ts predates the local tag.
    let decided_before = created_at - chrono::Duration::seconds(60);
    let stamped = decided_before.to_rfc3339();
    let reparsed = chrono::DateTime::parse_from_rfc3339(&stamped)
        .expect("stamped source_ts must be valid RFC 3339")
        .with_timezone(&chrono::Utc);

    let result = registry
        .delete_manifest(
            None,
            Some(reparsed),
            namespace,
            &Reference::Tag(Tag::new(tag).unwrap()),
        )
        .await
        .err();
    assert!(
        matches!(result, Some(Error::ReplicationSuperseded(_))),
        "a prune delete must be suppressed when the downstream tag is strictly newer, got: {result:?}"
    );
    registry
        .metadata_store
        .read_link(namespace, &LinkKind::Tag(Tag::new(tag).unwrap()))
        .await
        .expect("a superseded prune delete must preserve the downstream tag");

    // Proceeds: the stamped source_ts is newer than the local tag.
    let decided_after = created_at + chrono::Duration::seconds(60);
    let stamped = decided_after.to_rfc3339();
    let reparsed = chrono::DateTime::parse_from_rfc3339(&stamped)
        .expect("stamped source_ts must be valid RFC 3339")
        .with_timezone(&chrono::Utc);

    registry
        .delete_manifest(
            None,
            Some(reparsed),
            namespace,
            &Reference::Tag(Tag::new(tag).unwrap()),
        )
        .await
        .expect("a prune delete must proceed when its source_ts is newer than the tag");
    let result = registry
        .metadata_store
        .read_link(namespace, &LinkKind::Tag(Tag::new(tag).unwrap()))
        .await;
    assert!(
        matches!(result, Err(Error::NotFound)),
        "the downstream tag must be gone after an applied prune delete, got: {result:?}"
    );
}

#[tokio::test]
async fn replication_superseded_maps_to_distinct_oci_code() {
    let superseded: ServerError = Error::ReplicationSuperseded("newer".to_string()).into();
    let conflict: ServerError = Error::Conflict("immutable".to_string()).into();

    // Both are 409, but the OCI codes differ so the sender can disambiguate.
    assert_eq!(superseded.status_code(), http::StatusCode::CONFLICT);
    assert_eq!(conflict.status_code(), http::StatusCode::CONFLICT);

    let superseded_json = serde_json::to_value(superseded.error_body(None)).unwrap();
    let conflict_json = serde_json::to_value(conflict.error_body(None)).unwrap();
    assert_eq!(
        superseded_json["errors"][0]["code"],
        REPLICATION_SUPERSEDED_CODE
    );
    assert_eq!(conflict_json["errors"][0]["code"], "DENIED");
    assert_ne!(
        superseded_json["errors"][0]["code"],
        conflict_json["errors"][0]["code"]
    );
}

#[cfg(test)]
mod noop_suppression_tests {
    //! No-op suppression (loop prevention): an inbound manifest write that does
    //! not change local state must not be re-dispatched. The harness shares one
    //! object store and spawns no drain, so enqueued jobs persist for counting.

    use std::{io::Cursor, sync::Arc};

    use chrono::{Duration, Utc};
    use tempfile::TempDir;

    use angos_oci::request::PutManifestRequest;
    use angos_oci::{Digest, MediaType, Namespace, Reference, Tag};

    use crate::registry::manifest::tests::{create_test_manifest, manifest_with_references};
    use crate::{
        jobs::{
            Queue,
            store::{ClaimMode, JobStore},
        },
        registry::{
            Registry, RegistryConfig,
            metadata_store::{LinkKind, LinkOperation},
            test_utils::{
                FsTestStack, downstream_client, fs_test_stack, repository_with_downstream,
                single_repo_resolver, sole_pending_payload, upload_blob,
            },
        },
        replication::REPLICATION_DELETE_MANIFEST_KIND,
    };

    const REPO: &str = "nginx";
    const NAMESPACE: &str = "nginx";

    /// A `Registry` sharing one FS object store with a caller-held `JobStore`,
    /// carrying one event+reconcile downstream so `dispatch_replication` enqueues.
    fn build_registry() -> (Arc<Registry>, Arc<JobStore>, TempDir) {
        let FsTestStack {
            dir,
            store,
            metadata_store,
            blob_store,
        } = fs_test_stack();
        let resolver = single_repo_resolver(
            REPO,
            repository_with_downstream(REPO, downstream_client("https://unused.test")),
        );

        let job_store: Arc<JobStore> = Arc::new(JobStore::new(store, "test", ClaimMode::Atomic));

        let config = RegistryConfig::new(job_store.clone());
        let registry = Registry::new(blob_store, metadata_store, resolver, config);
        (registry, job_store, dir)
    }

    async fn pending(job_store: &JobStore) -> u64 {
        job_store
            .count_pending(Queue::Replication, 0)
            .await
            .unwrap()
    }

    /// Drains one pending replication job, clearing its `lock_key` dedup index.
    /// Pending pushes for the same tag coalesce on that index, so draining
    /// isolates the dispatch gate under test from the queue's coalescing.
    async fn drain_one(job_store: &JobStore) {
        let claimed = job_store
            .claim_one(Queue::Replication)
            .await
            .unwrap()
            .claimed
            .expect("expected one claimable job");
        job_store.complete(claimed).await.unwrap();
    }

    /// A second, distinct manifest, with its blobs pre-uploaded so the push
    /// validates.
    async fn create_second_manifest(
        registry: &Registry,
        namespace: &Namespace,
    ) -> (Vec<u8>, MediaType) {
        let config_content = br#"{"architecture":"arm64","os":"linux"}"#;
        let layer_content = b"a different layer content";
        let config_digest = upload_blob(registry, namespace, config_content).await;
        let layer_digest = upload_blob(registry, namespace, layer_content).await;
        manifest_with_references(
            &config_digest,
            config_content.len(),
            &layer_digest,
            layer_content.len(),
        )
    }

    #[tokio::test]
    async fn tagged_push_dispatches_only_when_tag_moves() {
        let (registry, job_store, _dir) = build_registry();
        let namespace = Namespace::new(NAMESPACE).unwrap();
        let tag = "latest";

        let (content_a, media_type) = create_test_manifest(&registry, &namespace).await;

        registry
            .accept_put_manifest(
                None,
                PutManifestRequest {
                    namespace: namespace.clone(),
                    reference: Reference::Tag(Tag::new(tag).unwrap()),
                    content_type: Some(media_type.clone()),
                    tags: Vec::new(),
                    source_ts: None,
                },
                Cursor::new(content_a.clone()),
            )
            .await
            .expect("first tag push");
        assert_eq!(
            pending(&job_store).await,
            1,
            "a first tag push (tag absent) must enqueue one job"
        );

        // Drain so dedup coalescing cannot mask the gate.
        drain_one(&job_store).await;
        assert_eq!(pending(&job_store).await, 0, "queue drained");

        // With the queue empty the gate itself, not the dedup index, must
        // suppress the replay. This per-node drop is what terminates mesh
        // cycles without origin tracking.
        registry
            .accept_put_manifest(
                None,
                PutManifestRequest {
                    namespace: namespace.clone(),
                    reference: Reference::Tag(Tag::new(tag).unwrap()),
                    content_type: Some(media_type.clone()),
                    tags: Vec::new(),
                    source_ts: None,
                },
                Cursor::new(content_a.clone()),
            )
            .await
            .expect("re-assert same tag->digest");
        assert_eq!(
            pending(&job_store).await,
            0,
            "re-asserting the same tag->digest must enqueue nothing (no-op replay)"
        );

        let (content_b, media_type_b) = create_second_manifest(&registry, &namespace).await;
        registry
            .accept_put_manifest(
                None,
                PutManifestRequest {
                    namespace: namespace.clone(),
                    reference: Reference::Tag(Tag::new(tag).unwrap()),
                    content_type: Some(media_type_b),
                    tags: Vec::new(),
                    source_ts: None,
                },
                Cursor::new(content_b),
            )
            .await
            .expect("move tag to a new digest");
        assert_eq!(
            pending(&job_store).await,
            1,
            "moving the tag to a new digest must enqueue a job"
        );
    }

    /// An A<->B digest bounce must not loop: re-pushing an already-present
    /// revision is not re-dispatched.
    #[tokio::test]
    async fn digest_push_dispatches_only_when_revision_is_new() {
        let (registry, job_store, _dir) = build_registry();
        let namespace = Namespace::new(NAMESPACE).unwrap();

        let (content, media_type) = create_test_manifest(&registry, &namespace).await;
        let digest = Digest::sha256_of_bytes(&content);

        registry
            .accept_put_manifest(
                None,
                PutManifestRequest {
                    namespace: namespace.clone(),
                    reference: Reference::Digest(digest.clone()),
                    content_type: Some(media_type.clone()),
                    tags: Vec::new(),
                    source_ts: None,
                },
                Cursor::new(content.clone()),
            )
            .await
            .expect("first digest push");
        assert_eq!(
            pending(&job_store).await,
            1,
            "a first-time digest push must enqueue one job"
        );

        // Drain so dedup coalescing cannot mask the gate.
        drain_one(&job_store).await;
        assert_eq!(pending(&job_store).await, 0, "queue drained");

        // With the queue empty a broken gate would freshly enqueue, so pending
        // staying 0 proves the gate, not the dedup index, suppressed the replay.
        registry
            .accept_put_manifest(
                None,
                PutManifestRequest {
                    namespace: namespace.clone(),
                    reference: Reference::Digest(digest),
                    content_type: Some(media_type),
                    tags: Vec::new(),
                    source_ts: None,
                },
                Cursor::new(content),
            )
            .await
            .expect("re-push same revision");
        assert_eq!(
            pending(&job_store).await,
            0,
            "re-pushing an already-present revision must enqueue nothing \
             (the gate, not the dedup index, suppresses it)"
        );
    }

    /// A by-digest push that adds a new `?tag=` to an already-present digest must
    /// still dispatch so the new tag link replicates, even though the digest itself
    /// did not change; re-adding the same tag is a converged no-op.
    #[tokio::test]
    async fn digest_push_with_new_tag_dispatches_when_tag_is_added() {
        let (registry, job_store, _dir) = build_registry();
        let namespace = Namespace::new(NAMESPACE).unwrap();
        let tag = "extra";

        let (content, media_type) = create_test_manifest(&registry, &namespace).await;
        let digest = Digest::sha256_of_bytes(&content);

        registry
            .accept_put_manifest(
                None,
                PutManifestRequest {
                    namespace: namespace.clone(),
                    reference: Reference::Digest(digest.clone()),
                    content_type: Some(media_type.clone()),
                    tags: Vec::new(),
                    source_ts: None,
                },
                Cursor::new(content.clone()),
            )
            .await
            .expect("first digest push");
        assert_eq!(
            pending(&job_store).await,
            1,
            "a first-time digest push must enqueue one job"
        );

        // Drain so dedup coalescing cannot mask the gate.
        drain_one(&job_store).await;
        assert_eq!(pending(&job_store).await, 0, "queue drained");

        // The digest is already present, so only the new tag link changed; the
        // gate must still dispatch so that tag replicates. The OR-gate re-dispatches
        // the unchanged digest push once alongside the changed tag, so two jobs land.
        registry
            .accept_put_manifest(
                None,
                PutManifestRequest {
                    namespace: namespace.clone(),
                    reference: Reference::Digest(digest.clone()),
                    content_type: Some(media_type.clone()),
                    tags: vec![Tag::new(tag).unwrap()],
                    source_ts: None,
                },
                Cursor::new(content.clone()),
            )
            .await
            .expect("re-push present digest with a new tag");
        assert_eq!(
            pending(&job_store).await,
            2,
            "adding a new tag to a present digest must enqueue the tag push \
             plus the re-dispatched digest push"
        );

        // Drain both so dedup coalescing cannot mask the gate on the converged push.
        drain_one(&job_store).await;
        drain_one(&job_store).await;
        assert_eq!(pending(&job_store).await, 0, "queue drained");

        // Both the digest and the tag are now present, so nothing changed.
        registry
            .accept_put_manifest(
                None,
                PutManifestRequest {
                    namespace: namespace.clone(),
                    reference: Reference::Digest(digest),
                    content_type: Some(media_type),
                    tags: vec![Tag::new(tag).unwrap()],
                    source_ts: None,
                },
                Cursor::new(content),
            )
            .await
            .expect("re-push present digest with the same present tag");
        assert_eq!(
            pending(&job_store).await,
            0,
            "re-adding an already-present tag must enqueue nothing"
        );
    }

    #[tokio::test]
    async fn tag_delete_dispatches_only_when_something_was_removed() {
        let (registry, job_store, _dir) = build_registry();
        let namespace = Namespace::new(NAMESPACE).unwrap();
        let tag = "latest";

        let (content, media_type) = create_test_manifest(&registry, &namespace).await;
        registry
            .accept_put_manifest(
                None,
                PutManifestRequest {
                    namespace: namespace.clone(),
                    reference: Reference::Tag(Tag::new(tag).unwrap()),
                    content_type: Some(media_type),
                    tags: Vec::new(),
                    source_ts: None,
                },
                Cursor::new(content),
            )
            .await
            .expect("seed tag push");
        let baseline = pending(&job_store).await;

        registry
            .delete_manifest(
                None,
                None,
                &namespace,
                &Reference::Tag(Tag::new(tag).unwrap()),
            )
            .await
            .expect("delete existing tag");
        assert_eq!(
            pending(&job_store).await,
            baseline + 1,
            "deleting an existing tag must enqueue one delete job"
        );

        let after_delete = pending(&job_store).await;
        let _ = registry
            .delete_manifest(
                None,
                None,
                &namespace,
                &Reference::Tag(Tag::new("does-not-exist").unwrap()),
            )
            .await;
        assert_eq!(
            pending(&job_store).await,
            after_delete,
            "deleting an absent tag must enqueue nothing (no-op delete)"
        );
    }

    /// The second delete must get its own job rather than coalescing into the
    /// still-pending first. A delete cannot re-derive its timestamp at execute
    /// time (the link is gone), so a coalesced older job would ship a stale
    /// `source_ts` and lose receiver-side LWW against the in-between re-push.
    #[tokio::test]
    async fn second_delete_after_repush_enqueues_its_own_job() {
        let (registry, job_store, _dir) = build_registry();
        let namespace = Namespace::new(NAMESPACE).unwrap();
        let tag = "latest";

        let (content, media_type) = create_test_manifest(&registry, &namespace).await;
        registry
            .accept_put_manifest(
                None,
                PutManifestRequest {
                    namespace: namespace.clone(),
                    reference: Reference::Tag(Tag::new(tag).unwrap()),
                    content_type: Some(media_type.clone()),
                    tags: Vec::new(),
                    source_ts: None,
                },
                Cursor::new(content.clone()),
            )
            .await
            .expect("seed tag push");
        drain_one(&job_store).await;
        assert_eq!(pending(&job_store).await, 0, "queue drained");

        // First delete, deliberately left pending.
        registry
            .delete_manifest(
                None,
                None,
                &namespace,
                &Reference::Tag(Tag::new(tag).unwrap()),
            )
            .await
            .expect("first delete");
        assert_eq!(
            pending(&job_store).await,
            1,
            "the first delete must enqueue one job"
        );

        registry
            .accept_put_manifest(
                None,
                PutManifestRequest {
                    namespace: namespace.clone(),
                    reference: Reference::Tag(Tag::new(tag).unwrap()),
                    content_type: Some(media_type),
                    tags: Vec::new(),
                    source_ts: None,
                },
                Cursor::new(content),
            )
            .await
            .expect("re-push tag");
        assert_eq!(
            pending(&job_store).await,
            2,
            "re-creating the tag must enqueue one push job"
        );

        registry
            .delete_manifest(
                None,
                None,
                &namespace,
                &Reference::Tag(Tag::new(tag).unwrap()),
            )
            .await
            .expect("second delete");
        assert_eq!(
            pending(&job_store).await,
            3,
            "the second delete must enqueue its own job (per-event lock key), \
             not coalesce into the pending older delete"
        );
    }

    /// An inbound replicated delete must re-dispatch with its author
    /// timestamp verbatim. A re-stamped `now()` would let the bounced delete
    /// win LWW over a recreate authored between the original delete and the
    /// bounce, destroying the acknowledged recreate on every node.
    #[tokio::test]
    async fn replicated_delete_redispatches_author_source_ts_verbatim() {
        let (registry, job_store, _dir) = build_registry();
        let namespace = Namespace::new(NAMESPACE).unwrap();
        let tag = "latest";

        // Seed via a replicated push so the tag's created_at predates the delete.
        let (content, media_type) = create_test_manifest(&registry, &namespace).await;
        let push_ts = Utc::now() - Duration::hours(2);
        registry
            .accept_put_manifest(
                None,
                PutManifestRequest {
                    namespace: namespace.clone(),
                    reference: Reference::Tag(Tag::new(tag).unwrap()),
                    content_type: Some(media_type),
                    tags: Vec::new(),
                    source_ts: Some(push_ts),
                },
                Cursor::new(content),
            )
            .await
            .expect("seed replicated tag push");
        drain_one(&job_store).await;

        let delete_ts = Utc::now() - Duration::hours(1);
        registry
            .delete_manifest(
                None,
                Some(delete_ts),
                &namespace,
                &Reference::Tag(Tag::new(tag).unwrap()),
            )
            .await
            .expect("replicated delete newer than the tag must proceed");

        let payload = sole_pending_payload(&job_store).await;
        assert_eq!(payload.kind(), REPLICATION_DELETE_MANIFEST_KIND);
        assert_eq!(
            payload.target().source_ts,
            Some(delete_ts),
            "the delete job must carry the author timestamp verbatim, \
             not a re-stamped now()"
        );
    }

    /// The suppression gate must key on the cascade tags, not the revision
    /// link alone.
    #[tokio::test]
    async fn digest_delete_dispatches_when_only_cascade_tags_remain() {
        let (registry, job_store, _dir) = build_registry();
        let namespace = Namespace::new(NAMESPACE).unwrap();

        let (content, media_type) = create_test_manifest(&registry, &namespace).await;
        let digest = Digest::sha256_of_bytes(&content);
        registry
            .accept_put_manifest(
                None,
                PutManifestRequest {
                    namespace: namespace.clone(),
                    reference: Reference::Tag(Tag::new("latest").unwrap()),
                    content_type: Some(media_type),
                    tags: Vec::new(),
                    source_ts: None,
                },
                Cursor::new(content),
            )
            .await
            .expect("seed tag push");
        drain_one(&job_store).await;

        // Drop only the revision link, leaving the tag pointing at it.
        registry
            .metadata_store
            .update_links(
                &namespace,
                &[LinkOperation::delete(LinkKind::Digest(digest.clone()))],
            )
            .await
            .expect("drop the revision link");

        let baseline = pending(&job_store).await;
        registry
            .delete_manifest(None, None, &namespace, &Reference::Digest(digest))
            .await
            .expect("digest delete");
        assert_eq!(
            pending(&job_store).await,
            baseline + 1,
            "a digest delete that drops only cascade tag links must enqueue a replication delete"
        );
    }

    /// The digest bounce-back that terminates a delete cycle: a converged node
    /// (revision gone, no pointing tags) stops re-dispatching the delete.
    #[tokio::test]
    async fn digest_delete_suppressed_once_converged() {
        let (registry, job_store, _dir) = build_registry();
        let namespace = Namespace::new(NAMESPACE).unwrap();

        let (content, media_type) = create_test_manifest(&registry, &namespace).await;
        let digest = Digest::sha256_of_bytes(&content);
        registry
            .accept_put_manifest(
                None,
                PutManifestRequest {
                    namespace: namespace.clone(),
                    reference: Reference::Tag(Tag::new("latest").unwrap()),
                    content_type: Some(media_type),
                    tags: Vec::new(),
                    source_ts: None,
                },
                Cursor::new(content),
            )
            .await
            .expect("seed tag push");
        drain_one(&job_store).await;

        let baseline = pending(&job_store).await;
        registry
            .delete_manifest(None, None, &namespace, &Reference::Digest(digest.clone()))
            .await
            .expect("delete existing revision");
        assert_eq!(
            pending(&job_store).await,
            baseline + 1,
            "deleting an existing revision must enqueue one delete job"
        );
        drain_one(&job_store).await;

        // With the queue empty a broken gate would freshly enqueue; staying at
        // `after` proves the gate suppressed it.
        let after = pending(&job_store).await;
        let _ = registry
            .delete_manifest(None, None, &namespace, &Reference::Digest(digest))
            .await;
        assert_eq!(
            pending(&job_store).await,
            after,
            "a converged digest delete (revision gone, no pointing tags) must enqueue nothing"
        );
    }
}

#[cfg(test)]
mod dispatch_replication_tests {
    use std::sync::Arc;

    use chrono::{Duration, Utc};
    use regex::Regex;
    use tempfile::TempDir;

    use angos_oci::{Digest, Namespace, Reference, Tag};

    use crate::metrics_provider::init_for_tests;
    use crate::registry::manifest::tests::{
        DispatchTarget, MISSING_SUBJECT_DIGEST, create_test_manifest_with_subject,
    };
    use crate::{
        jobs::{
            Queue,
            store::{ClaimMode, JobStore},
        },
        registry::{
            Registry, RegistryConfig, Repository,
            test_utils::{
                FsTestStack, downstream_client, fs_test_stack, repository_with_replication,
                single_repo_resolver, sole_pending_payload,
            },
        },
        replication::{
            REPLICATION_DELETE_MANIFEST_KIND, REPLICATION_PUSH_MANIFEST_KIND,
            ReplicationDownstream, ReplicationJob, ReplicationMode,
        },
    };

    const REPO: &str = "nginx";
    const NAMESPACE: &str = "nginx";
    const DOWNSTREAM: &str = "eu-region";
    const SAMPLE_DIGEST: &str =
        "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";

    fn downstream_with(
        name: &str,
        mode: ReplicationMode,
        namespace_filter: Vec<Regex>,
    ) -> ReplicationDownstream {
        ReplicationDownstream {
            mode,
            namespace_filter,
            ..ReplicationDownstream::new(
                name.to_string(),
                downstream_client("https://unused.test"),
                4,
            )
        }
    }

    fn repository_with(mode: ReplicationMode, namespace_filter: Vec<Regex>) -> Repository {
        repository_with_replication(
            REPO,
            vec![downstream_with(DOWNSTREAM, mode, namespace_filter)],
        )
    }

    fn repository_with_downstream() -> Repository {
        repository_with(ReplicationMode::EventReconcile, Vec::new())
    }

    /// A `Registry` whose job store is caller-held so the test can count
    /// pending jobs.
    fn build_registry_with(repository: Repository) -> (Arc<Registry>, Arc<JobStore>, TempDir) {
        let FsTestStack {
            dir,
            store,
            metadata_store,
            blob_store,
        } = fs_test_stack();
        let resolver = single_repo_resolver(REPO, repository);

        // No drain spawned: the bare JobStore only persists envelopes; these tests assert enqueue only.
        let job_store: Arc<JobStore> = Arc::new(JobStore::new(store, "test", ClaimMode::Atomic));

        let config = RegistryConfig::new(job_store.clone());
        let registry = Registry::new(blob_store, metadata_store, resolver, config);

        (registry, job_store, dir)
    }

    /// [`build_registry_with`] with one `event+reconcile` downstream.
    fn build_registry() -> (Arc<Registry>, Arc<JobStore>, TempDir) {
        build_registry_with(repository_with_downstream())
    }

    /// The payload carries the correct downstream/namespace/tag/digest/kind and
    /// a populated `source_ts`.
    #[tokio::test]
    async fn dispatch_replication_payload_is_well_formed() {
        init_for_tests();
        let (registry, job_store, _dir) = build_registry();

        let namespace = Namespace::new(NAMESPACE).unwrap();
        let digest: Digest = SAMPLE_DIGEST.parse().unwrap();

        let repository = registry.resolver.resolve(&namespace);
        let tag = Tag::new("v1").unwrap();
        registry
            .dispatch_replication(
                repository,
                &namespace,
                DispatchTarget::Push {
                    tag: Some(&tag),
                    digest: &digest,
                },
                None,
            )
            .await;

        let payload = sole_pending_payload(&job_store).await;
        let target = payload.target();
        assert_eq!(target.downstream, DOWNSTREAM);
        assert_eq!(target.namespace, NAMESPACE);
        assert_eq!(target.tag.as_deref(), Some("v1"));
        assert_eq!(
            target.digest.as_ref().map(ToString::to_string).as_deref(),
            Some(SAMPLE_DIGEST)
        );
        assert_eq!(payload.kind(), REPLICATION_PUSH_MANIFEST_KIND);
        assert!(
            target.source_ts.is_some(),
            "source_ts must be present for receiver-side LWW"
        );
    }

    /// The fan-out enqueues concurrently (one index GET plus CAS transaction
    /// per downstream); every matching downstream must still get exactly one
    /// job carrying its own name.
    #[tokio::test]
    async fn dispatch_replication_enqueues_one_job_per_downstream() {
        init_for_tests();
        let (registry, job_store, _dir) = build_registry_with(repository_with_replication(
            REPO,
            vec![
                downstream_with(DOWNSTREAM, ReplicationMode::EventReconcile, Vec::new()),
                downstream_with("us-region", ReplicationMode::EventReconcile, Vec::new()),
            ],
        ));

        let namespace = Namespace::new(NAMESPACE).unwrap();
        let digest: Digest = SAMPLE_DIGEST.parse().unwrap();

        let repository = registry.resolver.resolve(&namespace);
        let tag = Tag::new("v1").unwrap();
        registry
            .dispatch_replication(
                repository,
                &namespace,
                DispatchTarget::Push {
                    tag: Some(&tag),
                    digest: &digest,
                },
                None,
            )
            .await;

        let keys = job_store
            .list_pending(Queue::Replication, 16)
            .await
            .unwrap();
        assert_eq!(keys.len(), 2, "each matching downstream must get one job");
        let mut downstreams = Vec::new();
        for key in &keys {
            let envelope = job_store
                .read_pending(Queue::Replication, key)
                .await
                .unwrap();
            let payload: ReplicationJob =
                serde_json::from_value(envelope.payload).expect("decode ReplicationJob");
            downstreams.push(payload.target().downstream.clone());
        }
        downstreams.sort();
        assert_eq!(
            downstreams,
            vec![DOWNSTREAM.to_string(), "us-region".to_string()],
            "one job per downstream, each addressed to its own downstream"
        );
    }

    /// Once the manifest is gone neither the job nor its retries can name the
    /// subject still listing the referrer, so the job has to carry it.
    #[tokio::test]
    async fn a_digest_delete_carries_the_referrer_subject() {
        init_for_tests();
        let (registry, job_store, _dir) = build_registry();

        let namespace = Namespace::new(NAMESPACE).unwrap();
        let (body, media_type) = create_test_manifest_with_subject(&registry, &namespace).await;
        let referrer = registry
            .put_manifest(
                &namespace,
                &Reference::Tag(Tag::new("v1").unwrap()),
                Some(&media_type),
                &body,
            )
            .await
            .expect("the referrer manifest must push")
            .digest;

        registry
            .delete_manifest(None, None, &namespace, &Reference::Digest(referrer))
            .await
            .expect("the digest delete must succeed");

        // The push enqueued by `put_manifest` is still pending alongside it.
        let mut deletes = Vec::new();
        for key in job_store
            .list_pending(Queue::Replication, 16)
            .await
            .unwrap()
        {
            let envelope = job_store
                .read_pending(Queue::Replication, &key)
                .await
                .unwrap();
            let payload: ReplicationJob =
                serde_json::from_value(envelope.payload).expect("decode payload");
            if let ReplicationJob::Delete { subject, .. } = payload {
                deletes.push(subject.map(|digest| digest.to_string()));
            }
        }
        assert_eq!(
            deletes,
            vec![Some(MISSING_SUBJECT_DIGEST.to_string())],
            "the delete job must carry the subject read before the manifest went away"
        );
    }

    /// A caller-provided timestamp (an inbound replicated delete's author
    /// time) propagates verbatim; a re-stamped `now()` would let the bounced
    /// delete outrank a recreate authored in between.
    #[tokio::test]
    async fn dispatch_replication_uses_provided_source_ts_verbatim() {
        init_for_tests();
        let (registry, job_store, _dir) = build_registry();

        let namespace = Namespace::new(NAMESPACE).unwrap();
        let author_ts = Utc::now() - Duration::hours(3);

        let repository = registry.resolver.resolve(&namespace);
        let tag = Tag::new("v1").unwrap();
        registry
            .dispatch_replication(
                repository,
                &namespace,
                DispatchTarget::TagDelete { tag: &tag },
                Some(author_ts),
            )
            .await;

        let payload = sole_pending_payload(&job_store).await;
        assert_eq!(payload.kind(), REPLICATION_DELETE_MANIFEST_KIND);
        assert_eq!(
            payload.target().source_ts,
            Some(author_ts),
            "a provided source_ts must propagate verbatim, not be re-stamped"
        );
    }

    #[tokio::test]
    async fn dispatch_replication_skips_reconcile_only_downstream() {
        init_for_tests();
        let (registry, job_store, _dir) =
            build_registry_with(repository_with(ReplicationMode::ReconcileOnly, Vec::new()));

        let namespace = Namespace::new(NAMESPACE).unwrap();
        let digest: Digest = SAMPLE_DIGEST.parse().unwrap();

        let repository = registry.resolver.resolve(&namespace);
        let tag = Tag::new("v1").unwrap();
        registry
            .dispatch_replication(
                repository,
                &namespace,
                DispatchTarget::Push {
                    tag: Some(&tag),
                    digest: &digest,
                },
                None,
            )
            .await;

        assert_eq!(
            job_store
                .count_pending(Queue::Replication, 0)
                .await
                .unwrap(),
            0,
            "a reconcile-only downstream must not enqueue on the event path"
        );
    }

    #[tokio::test]
    async fn dispatch_replication_skips_non_matching_namespace_filter() {
        init_for_tests();
        let (registry, job_store, _dir) = build_registry_with(repository_with(
            ReplicationMode::EventReconcile,
            vec![Regex::new("^other/.*").unwrap()],
        ));

        let namespace = Namespace::new(NAMESPACE).unwrap();
        let digest: Digest = SAMPLE_DIGEST.parse().unwrap();

        let repository = registry.resolver.resolve(&namespace);
        let tag = Tag::new("v1").unwrap();
        registry
            .dispatch_replication(
                repository,
                &namespace,
                DispatchTarget::Push {
                    tag: Some(&tag),
                    digest: &digest,
                },
                None,
            )
            .await;

        assert_eq!(
            job_store
                .count_pending(Queue::Replication, 0)
                .await
                .unwrap(),
            0,
            "a downstream whose filter excludes the namespace must not enqueue"
        );
    }
}

/// A replicated write whose `X-Angos-Source-Timestamp` predates the local tag
/// must lose: the newer local manifest stays, and the writer is told the write
/// was superseded.
#[tokio::test]
async fn backdated_source_ts_loses_to_newer_local_tag() {
    let test_case = FSRegistryTestCase::new();
    let registry = test_case.registry();
    let namespace = Namespace::new("test-repo/lww").unwrap();
    let media_type = MediaType::oci_manifest();
    let tag = || Reference::Tag(Tag::new("latest").unwrap());

    // Distinct, reference-free manifests yield distinct digests with no blob
    // uploads of their own.
    let manifest_a =
        br#"{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","annotations":{"seam":"A"}}"#.to_vec();
    let manifest_b =
        br#"{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","annotations":{"seam":"B"}}"#.to_vec();

    // Seed the newer local tag at a known recent source_ts so created_at is
    // stamped deterministically rather than from the wall clock.
    let newer_ts = Utc::now() - chrono::Duration::seconds(10);
    let seeded = registry
        .accept_put_manifest(
            None,
            PutManifestRequest {
                namespace: namespace.clone(),
                reference: tag(),
                content_type: Some(media_type.clone()),
                tags: Vec::new(),
                source_ts: Some(newer_ts),
            },
            Cursor::new(manifest_b),
        )
        .await
        .expect("seeding the newer local tag must succeed");
    let kept_digest = response_digest(&seeded);

    let result = registry
        .accept_put_manifest(
            None,
            PutManifestRequest {
                namespace: namespace.clone(),
                reference: tag(),
                content_type: Some(media_type.clone()),
                tags: Vec::new(),
                source_ts: Some(newer_ts - chrono::Duration::seconds(60)),
            },
            Cursor::new(manifest_a),
        )
        .await;

    assert!(
        matches!(result, Err(Error::ReplicationSuperseded(_))),
        "a backdated write must be reported as superseded, got {result:?}"
    );

    // Kill criterion: the tag must still point at the manifest seeded above. If
    // the source_ts were dropped, the backdated put would have overwritten it.
    let head = registry
        .head_manifest(
            None,
            HeadManifestRequest {
                namespace: namespace.clone(),
                reference: tag(),
                accepted_types: vec![MediaRange::from(media_type)],
            },
        )
        .await
        .expect("tag must still resolve");
    assert_eq!(
        response_digest(&head),
        kept_digest,
        "a backdated push must not overwrite the newer local tag"
    );
}

/// A manifest and a rebuild of it, differing only by an annotation.
const RELEASE: &[u8] =
    br#"{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json"}"#;
const REBUILD: &[u8] = br#"{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","annotations":{"build":"2"}}"#;

/// Immutability protects a tag from being moved, not from being created: the
/// how-to promises the first push succeeds and only a second with different
/// content fails. Refusing the create would make a release tag impossible to
/// publish with the flag on.
#[tokio::test]
async fn an_immutable_tag_refuses_an_overwrite_but_not_its_first_push() {
    let test_case = FSRegistryTestCase::with_immutable_tags();
    let registry = test_case.registry();
    let namespace = Namespace::new("test-repo/app").unwrap();
    let tag = Tag::new("v1.0.0").unwrap();

    let push = async |body: &'static [u8]| {
        registry
            .accept_put_manifest(
                None,
                PutManifestRequest {
                    namespace: namespace.clone(),
                    reference: Reference::Tag(tag.clone()),
                    content_type: Some(MediaType::oci_manifest()),
                    tags: Vec::new(),
                    source_ts: None,
                },
                Cursor::new(body.to_vec()),
            )
            .await
    };

    push(RELEASE)
        .await
        .expect("the first push of an immutable tag must succeed");
    push(RELEASE)
        .await
        .expect("a re-push of what the tag already holds moves nothing");

    let result = push(REBUILD).await;
    let Err(Error::Conflict(msg)) = result else {
        panic!("moving an immutable tag must be refused, got: {result:?}");
    };
    assert!(
        msg.contains("v1.0.0") && msg.contains("immutable"),
        "the refusal must name the tag, got: {msg}"
    );
}

/// The `?tag=` path carries the same rule: a by-digest push may create the
/// immutable tag, and may not point it somewhere else afterwards.
#[tokio::test]
async fn a_by_digest_push_may_create_an_immutable_tag_but_not_move_it() {
    let test_case = FSRegistryTestCase::with_immutable_tags();
    let registry = test_case.registry();
    let namespace = Namespace::new("test-repo/app").unwrap();

    let push = async |body: &'static [u8]| {
        registry
            .accept_put_manifest(
                None,
                PutManifestRequest {
                    namespace: namespace.clone(),
                    reference: Reference::Digest(Digest::sha256_of_bytes(body)),
                    content_type: Some(MediaType::oci_manifest()),
                    tags: vec![Tag::new("v1.0.0").unwrap()],
                    source_ts: None,
                },
                Cursor::new(body.to_vec()),
            )
            .await
    };

    push(RELEASE)
        .await
        .expect("a `?tag=` creating an immutable tag must succeed");

    let result = push(REBUILD).await;
    assert!(
        matches!(result, Err(Error::Conflict(_))),
        "a `?tag=` moving an immutable tag must be refused, got: {result:?}"
    );
}

/// An excluded tag stays writable while the rest of the repository is frozen.
#[tokio::test]
async fn an_excluded_tag_remains_writable() {
    let test_case = FSRegistryTestCase::with_immutable_tags();
    let registry = test_case.registry();
    let namespace = Namespace::new("test-repo/app").unwrap();

    let response = registry
        .accept_put_manifest(
            None,
            PutManifestRequest {
                namespace: namespace.clone(),
                reference: Reference::Tag(Tag::new("latest").unwrap()),
                content_type: Some(MediaType::oci_manifest()),
                tags: Vec::new(),
                source_ts: None,
            },
            Cursor::new(
                br#"{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json"}"#
                    .to_vec(),
            ),
        )
        .await;

    let response = response.expect("an excluded tag must stay writable");
    assert_eq!(response.status(), StatusCode::CREATED);
    assert_eq!(
        *response_header(&response, &LOCATION),
        format!("/v2/{namespace}/manifests/latest"),
        "the write must be reported at the tag it created"
    );
}

/// A tag push persists the manifest's size and annotations into its entry
/// body for tag history, dropping annotation keys under the reserved
/// `org.opencontainers.distribution` prefix at write time.
#[tokio::test]
async fn a_tag_push_persists_descriptor_fields_in_its_entry_body() {
    let test_case = FSRegistryTestCase::new();
    let registry = test_case.registry();
    let namespace = &Namespace::new("tag-entry-descriptor").unwrap();
    let manifest = json!({
        "schemaVersion": 2,
        "mediaType": IMAGE_MANIFEST_MEDIA_TYPE,
        "layers": [],
        "annotations": {
            "org.opencontainers.distribution.internal": "reserved",
            "com.example.team": "kept",
        },
    });
    let content = serde_json::to_vec(&manifest).unwrap();
    let media_type = MediaType::new(IMAGE_MANIFEST_MEDIA_TYPE).unwrap();
    let tag = Tag::new("annotated").unwrap();

    registry
        .put_manifest(
            namespace,
            &Reference::Tag(tag.clone()),
            Some(&media_type),
            &content,
        )
        .await
        .unwrap();

    let store = test_case.metadata_store();
    let dir = namespace.tag_entry_dir(&tag);
    let entries = store.object_store().list(&dir, 10, None).await.unwrap();
    let entry = entries
        .items
        .iter()
        .find(|name| name.contains(".set."))
        .expect("the push must write one set entry");
    let body = store
        .object_store()
        .get(&format!("{dir}/{entry}"))
        .await
        .unwrap();
    let parsed: TagEntryBody = serde_json::from_slice(&body).unwrap();
    assert_eq!(parsed.media_type, Some(media_type));
    assert_eq!(parsed.size, Some(content.len() as u64));
    assert_eq!(
        parsed.annotations,
        Some(BTreeMap::from([(
            "com.example.team".to_string(),
            "kept".to_string()
        )])),
        "only the non-reserved annotation survives"
    );
}
