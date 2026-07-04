use std::{
    collections::{HashMap, HashSet},
    io::Cursor,
};

use futures_util::future::join_all;
use hyper::header::{CONTENT_LENGTH, CONTENT_TYPE, LOCATION};
use serde_json::json;

use super::{parse::manifest_meta_from_body, *};
use crate::{
    command::server::Error as ServerError,
    event_webhook::event::EventKind,
    oci::{Algorithm, MediaType, Namespace, Tag},
    registry::{
        Error, OCI_TAG, Registry,
        metadata_store::{self, LinkKind, LinkMetadata, LinkOperation},
        path_builder::blob_path,
        test_utils::{
            FSRegistryTestCase, RegistryTestCase, for_each_backend, put_blob_direct, put_link_raw,
            upload_blob,
        },
    },
    replication::REPLICATION_SUPERSEDED_CODE,
};

fn header_digest(headers: &HashMap<&'static str, String>) -> Digest {
    headers[DOCKER_CONTENT_DIGEST].parse().unwrap()
}

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
                registry.get_repository_for_namespace(namespace).unwrap(),
                &[media_type.to_string()],
                namespace,
                Reference::Tag(Tag::new(tag).unwrap()),
                false,
            )
            .await
            .unwrap();

        assert_eq!(stored_manifest.content, content);
        assert_eq!(stored_manifest.media_type.unwrap(), media_type);
        assert_eq!(stored_manifest.digest, header_digest(&response.headers));

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
    })
    .await;
}

/// A by-digest push with `?tag=` query parameters must create each listed tag,
/// return an `OCI-Tag` header naming both, and resolve each tag to the pushed
/// digest. The digest is sha512 to lock the distribution-spec regression where
/// the suite fell back to a sha256-defaulted by-tag push.
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
            None,
            &namespace,
            Reference::Digest(digest.clone()),
            media_type.clone(),
            Cursor::new(content.clone()),
            vec![Tag::new("1.2.3").unwrap(), Tag::new("latest").unwrap()],
        )
        .await
        .expect("by-digest push with tag params must succeed");

    let oci_tag = response
        .headers
        .get(OCI_TAG)
        .expect("OCI-Tag header must be present");
    assert_eq!(
        oci_tag.as_str(),
        "1.2.3, latest",
        "OCI-Tag must list created tags comma and space separated in created order"
    );
    assert_eq!(header_digest(&response.headers), digest);

    let digest_str = digest.to_string();
    assert!(
        response
            .events
            .iter()
            .any(|e| e.kind == EventKind::ManifestPush && e.digest.as_deref() == Some(&digest_str)),
        "a by-digest push must emit a ManifestPush event carrying the digest"
    );
    for tag in ["1.2.3", "latest"] {
        let count = response
            .events
            .iter()
            .filter(|e| {
                e.kind == EventKind::TagCreate
                    && e.tag.as_deref() == Some(tag)
                    && e.digest.as_deref() == Some(&digest_str)
            })
            .count();
        assert_eq!(
            count, 1,
            "exactly one TagCreate event carrying the digest must fire for tag '{tag}'"
        );
    }

    let repository = registry.get_repository_for_namespace(&namespace).unwrap();
    for tag in ["1.2.3", "latest"] {
        let head = registry
            .head_manifest(
                repository,
                &[media_type.to_string()],
                &namespace,
                Reference::Tag(Tag::new(tag).unwrap()),
                false,
            )
            .await
            .expect("each created tag must resolve");
        assert_eq!(
            header_digest(&head.headers),
            digest,
            "tag '{tag}' must point at the sha512 digest"
        );
    }
}

// Invalid `?tag=` values are now rejected at the router (a generic 400) before
// the handler runs, so the handler always receives pre-validated `Tag`s. The
// router rejection is covered by
// `router::tests::test_parse_put_manifest_by_digest_invalid_tag_param_rejected`.

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
            None,
            &namespace,
            Reference::Tag(Tag::new("v1").unwrap()),
            media_type.clone(),
            Cursor::new(content.clone()),
            vec![Tag::new("ignored").unwrap()],
        )
        .await
        .expect("by-tag push must succeed");

    assert!(
        !response.headers.contains_key(OCI_TAG),
        "a by-tag push must not emit an OCI-Tag header"
    );

    let repository = registry.get_repository_for_namespace(&namespace).unwrap();
    let ignored = registry
        .head_manifest(
            repository,
            &[media_type.to_string()],
            &namespace,
            Reference::Tag(Tag::new("ignored").unwrap()),
            false,
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
        let digest = header_digest(&response.headers);
        let link = LinkKind::Referrer(subject, digest.clone());
        let metadata = registry
            .metadata_store
            .read_link(namespace, &link)
            .await
            .unwrap();
        assert_eq!(metadata.target, digest);
    })
    .await;
}

/// The live `accept_put_manifest` path honors the registry's
/// `validate_manifest_references` flag (set from `[global]
/// allow_missing_manifest_references`): the permissive registry stores an index
/// whose child manifest is absent (pre-1.2.0 behavior), while the strict
/// registry rejects the identical push with `MANIFEST_BLOB_UNKNOWN`.
#[tokio::test]
async fn accept_put_manifest_honors_reference_validation_flag() {
    use crate::registry::test_utils::create_test_registry_with;

    let missing_child = fixed_digest();
    let namespace = Namespace::new("test-repo/ref-validation").unwrap();
    let (content, media_type) = index_manifest_with_child(&missing_child);

    // Permissive: the missing child reference is accepted.
    let permissive_case = FSRegistryTestCase::new();
    let permissive = create_test_registry_with(
        permissive_case.blob_store(),
        permissive_case.metadata_store(),
        false,
    );
    permissive
        .accept_put_manifest(
            None,
            None,
            &namespace,
            Reference::Tag(Tag::new("latest").unwrap()),
            media_type.clone(),
            Cursor::new(content.clone()),
            Vec::new(),
        )
        .await
        .expect("permissive registry must accept a missing child manifest reference");

    // Strict: the identical push is rejected before anything is stored.
    let strict_case = FSRegistryTestCase::new();
    let strict =
        create_test_registry_with(strict_case.blob_store(), strict_case.metadata_store(), true);
    let Err(err) = strict
        .accept_put_manifest(
            None,
            None,
            &namespace,
            Reference::Tag(Tag::new("latest").unwrap()),
            media_type,
            Cursor::new(content),
            Vec::new(),
        )
        .await
    else {
        panic!("strict registry must reject a missing child manifest reference");
    };
    assert!(matches!(err, Error::ManifestBlobUnknown));
}

/// A permissive registry accepts a manifest that references content the pushing
/// namespace does not own, but it must not grant that namespace read access to
/// the referenced blobs: the references stay dangling and a later pull resolves
/// as unknown. This guards the namespace-isolation boundary against a client
/// minting a readable reference to another namespace's blob.
#[tokio::test]
async fn permissive_push_does_not_grant_read_of_unowned_referenced_blob() {
    use crate::registry::{blob_ownership::BlobOwnership, test_utils::create_test_registry_with};

    let case = FSRegistryTestCase::new();
    let permissive = create_test_registry_with(case.blob_store(), case.metadata_store(), false);

    // The owner uploads a config and a layer, gaining ownership of both.
    let owner = Namespace::new("test-repo/owner").unwrap();
    let config_content = br#"{"architecture":"amd64","os":"linux"}"#;
    let layer_content = b"private layer bytes";
    let config_digest = upload_blob(&permissive, &owner, config_content).await;
    let layer_digest = upload_blob(&permissive, &owner, layer_content).await;

    // An attacker in a different namespace pushes a manifest referencing the
    // owner's blobs by digest. The permissive push is accepted.
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
            None,
            &attacker,
            Reference::Tag(Tag::new("latest").unwrap()),
            media_type,
            Cursor::new(content),
            Vec::new(),
        )
        .await
        .expect("permissive registry accepts a push referencing unowned blobs");

    // The owner still reads its blobs; the attacker gained no read access.
    let ownership = BlobOwnership::new(permissive.metadata_store.as_ref());
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
    let outcome = permissive
        .get_blob(repository, &[], &attacker, &layer_digest, None)
        .await
        .map(|_| ());
    assert!(
        matches!(outcome, Err(Error::BlobUnknown)),
        "attacker pull of the unowned blob must be unknown, got {outcome:?}"
    );
}

/// The child-manifest analogue of the blob isolation guard: a permissive
/// registry accepts an image index whose child manifest digest the pushing
/// namespace does not own (the docker buildx/bake scenario), but it must not
/// grant that namespace read access to the child. The `LinkKind::Manifest` drop
/// branch of `retain_owned_reference_links` keeps the child reference dangling
/// so a later pull of the child digest resolves as unknown.
#[tokio::test]
async fn permissive_push_does_not_grant_read_of_unowned_child_manifest() {
    use crate::registry::{blob_ownership::BlobOwnership, test_utils::create_test_registry_with};

    let case = FSRegistryTestCase::new();
    let permissive = create_test_registry_with(case.blob_store(), case.metadata_store(), false);

    // The owner uploads the child manifest content, gaining ownership of it.
    let owner = Namespace::new("test-repo/owner").unwrap();
    let child_content = br#"{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef","size":1},"layers":[]}"#;
    let child_digest = upload_blob(&permissive, &owner, child_content).await;

    // An attacker in a different namespace pushes an index referencing the
    // owner's child manifest by digest. The permissive push is accepted.
    let attacker = Namespace::new("test-repo/attacker").unwrap();
    let (content, media_type) = index_manifest_with_child(&child_digest);
    permissive
        .accept_put_manifest(
            None,
            None,
            &attacker,
            Reference::Tag(Tag::new("latest").unwrap()),
            media_type,
            Cursor::new(content),
            Vec::new(),
        )
        .await
        .expect("permissive registry accepts an index referencing an unowned child manifest");

    // The owner still reads the child; the attacker gained no read access.
    let ownership = BlobOwnership::new(permissive.metadata_store.as_ref());
    assert!(ownership.can_read(&owner, &child_digest).await.unwrap());
    assert!(
        !ownership.can_read(&attacker, &child_digest).await.unwrap(),
        "attacker must not gain read access to a child manifest it never uploaded"
    );

    // The attacker's index push left the child reference dangling, so pulling
    // the child digest in the attacker namespace resolves as unknown.
    let repository = permissive.get_repository_for_namespace(&attacker).unwrap();
    let outcome = permissive
        .get_manifest(
            repository,
            &[],
            &attacker,
            Reference::Digest(child_digest.clone()),
            false,
        )
        .await
        .map(|_| ());
    assert!(
        matches!(outcome, Err(Error::ManifestUnknown)),
        "attacker pull of the unowned child manifest must be unknown, got {outcome:?}"
    );
}

/// The positive branch of `retain_owned_reference_links` on the permissive
/// default: a namespace pushing a manifest that references blobs it already owns
/// keeps every reference link, so the manifest and all its blobs stay fully
/// pullable. Guards against a regression where permissive mode strips links for
/// owned content and silently breaks normal image pushes.
#[tokio::test]
async fn permissive_push_of_owned_references_yields_a_pullable_manifest() {
    use crate::registry::{blob_ownership::BlobOwnership, test_utils::create_test_registry_with};

    let case = FSRegistryTestCase::new();
    let permissive = create_test_registry_with(case.blob_store(), case.metadata_store(), false);

    // One namespace uploads its own config and layer, then pushes a manifest
    // referencing those owned blobs.
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
            None,
            &namespace,
            Reference::Tag(Tag::new("latest").unwrap()),
            media_type,
            Cursor::new(content),
            Vec::new(),
        )
        .await
        .expect("permissive registry accepts a push referencing owned blobs");

    // Every reference link is retained: the namespace reads its blobs and the
    // manifest plus both blobs are pullable.
    let ownership = BlobOwnership::new(permissive.metadata_store.as_ref());
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
            repository,
            &[],
            &namespace,
            Reference::Tag(Tag::new("latest").unwrap()),
            false,
        )
        .await
        .expect("the pushed manifest must be pullable by tag");
    permissive
        .get_blob(repository, &[], &namespace, &config_digest, None)
        .await
        .expect("the owned config blob must be pullable");
    permissive
        .get_blob(repository, &[], &namespace, &layer_digest, None)
        .await
        .expect("the owned layer blob must be pullable");
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
                registry.get_repository_for_namespace(namespace).unwrap(),
                &[media_type.to_string()],
                namespace,
                Reference::Tag(Tag::new(tag).unwrap()),
                false,
            )
            .await
            .unwrap();

        assert_eq!(manifest.content, content);
        assert_eq!(manifest.media_type.unwrap(), media_type);
        assert_eq!(manifest.digest, header_digest(&response.headers));

        let manifest = registry
            .get_manifest(
                registry.get_repository_for_namespace(namespace).unwrap(),
                &[media_type.to_string()],
                namespace,
                Reference::Digest(header_digest(&response.headers)),
                false,
            )
            .await
            .unwrap();

        assert_eq!(manifest.content, content);
        assert_eq!(manifest.media_type.unwrap(), media_type);
        assert_eq!(manifest.digest, header_digest(&response.headers));
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

        let manifest = registry
            .head_manifest(
                registry.get_repository_for_namespace(namespace).unwrap(),
                &[media_type.to_string()],
                namespace,
                Reference::Tag(Tag::new(tag).unwrap()),
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

        let manifest = registry
            .head_manifest(
                registry.get_repository_for_namespace(namespace).unwrap(),
                &[media_type.to_string()],
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
    })
    .await;
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
                    registry.get_repository_for_namespace(namespace).unwrap(),
                    &[media_type.to_string()],
                    namespace,
                    Reference::Tag(Tag::new(tag).unwrap()),
                    false,
                )
                .await
                .is_err()
        );

        registry
            .delete_manifest(
                None,
                None,
                namespace,
                &Reference::Digest(header_digest(&response.headers)),
            )
            .await
            .unwrap();

        assert!(
            registry
                .get_manifest(
                    registry.get_repository_for_namespace(namespace).unwrap(),
                    &[media_type.to_string()],
                    namespace,
                    Reference::Digest(header_digest(&response.headers)),
                    false,
                )
                .await
                .is_err()
        );
    })
    .await;
}

/// Regression: a digest `delete_manifest` must hold the `blob-data:{digest}`
/// lock across its unreferenced-check + reclaim. Otherwise a concurrent grant
/// from another repository is missed and a shared blob is reclaimed while a tag
/// still points at it (conformance `MANIFEST_BLOB_UNKNOWN`).
#[tokio::test]
async fn delete_manifest_holds_blob_data_lock_against_concurrent_grant() {
    for_each_backend(async |test_case| {
        use std::time::Duration;

        use tokio::time::sleep;

        use crate::registry::blob_ownership::BlobOwnership;

        let registry = test_case.registry();
        let first = &Namespace::new("test-repo/first").unwrap();
        let second = &Namespace::new("test-repo/second").unwrap();

        // Push a manifest to `first`; its blob is shared across namespaces.
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
        let digest = header_digest(&response.headers);

        // Hold the lock, then start the delete: it must block, not reclaim.
        let session = registry.acquire_blob_data_lock(&digest).await.unwrap();
        let reference = Reference::Digest(digest.clone());
        let delete = registry.delete_manifest(None, None, first, &reference);
        tokio::pin!(delete);
        tokio::select! {
            result = &mut delete => {
                panic!(
                    "delete_manifest completed while blob-data lock was held (ok={})",
                    result.is_ok()
                );
            }
            () = sleep(Duration::from_millis(25)) => {}
        }

        // A second repo grants a reference while the delete is parked.
        let ownership = BlobOwnership::new(registry.metadata_store.as_ref());
        ownership.grant(second, &digest).await.unwrap();
        session.release().await;
        delete.await.unwrap();

        // The blob survives: `second` still references it.
        assert!(
            registry.blob_store.read(&digest).await.is_ok(),
            "shared manifest blob was wrongly reclaimed despite a concurrent grant"
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
        let manifest_digest = header_digest(&response.headers);

        let manifest_blob_result = registry.delete_blob(namespace, &manifest_digest).await;
        assert!(matches!(manifest_blob_result, Err(Error::BlobReferenced)));

        let layer_result = registry.delete_blob(namespace, &layer_digest).await;
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
        registry
            .put_manifest(
                namespace,
                &Reference::Tag(Tag::new("latest").unwrap()),
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
        let links = blob_index.namespace.get(&namespace).unwrap();
        assert!(links.contains(&LinkKind::Blob(layer_digest.clone())));
        assert!(links.contains(&LinkKind::Layer(layer_digest.clone())));
    }

    test_case.cleanup().await;
}

#[test]
fn test_parse_manifest_digests() {
    let (content, media_type) = create_raw_test_manifest();
    let digests = parse_manifest_digests(&content, Some(&media_type)).unwrap();

    assert_eq!(
        digests.media_type.as_ref(),
        Some(&media_type),
        "the parse must surface the body's declared mediaType"
    );
    assert!(digests.subject.is_none());
    assert_eq!(
        digests.config.unwrap().to_string(),
        "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
    );
    assert_eq!(
        digests.layers[0].to_string(),
        "sha256:abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
    );

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

    let wrong_media_type = MediaType::new("application/wrong.media.type").unwrap();
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

#[test]
fn parse_manifest_digests_media_type_mismatch_returns_manifest_invalid() {
    let (content, _) = create_raw_test_manifest();
    let wrong_type = MediaType::new("application/vnd.oci.image.manifest.v1+json").unwrap();

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
            None,
            namespace,
            Reference::Tag(Tag::new("latest").unwrap()),
            MediaType::new("application/vnd.oci.image.manifest.v1+json").unwrap(),
            Cursor::new(body),
            Vec::new(),
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
                namespace,
                Reference::Tag(Tag::new(tag).unwrap()),
                &[],
                false,
            )
            .await
            .unwrap();

        match response {
            GetManifestResponse::Redirect { headers, digest } => {
                assert_eq!(digest, header_digest(&put_response.headers));
                assert_eq!(
                    header_digest(&headers),
                    header_digest(&put_response.headers)
                );
                assert_eq!(headers[CONTENT_TYPE.as_str()], media_type);
            }
            GetManifestResponse::Body {
                headers,
                content: body,
                digest,
            } => {
                assert_eq!(digest, header_digest(&put_response.headers));
                assert_eq!(
                    header_digest(&headers),
                    header_digest(&put_response.headers)
                );
                assert_eq!(headers[CONTENT_TYPE.as_str()], media_type);
                assert_eq!(body, content);
            }
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
                None,
                namespace,
                Reference::Tag(Tag::new(tag).unwrap()),
                media_type.clone(),
                manifest_stream,
                Vec::new(),
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
                &[media_type.to_string()],
                namespace,
                Reference::Tag(Tag::new(tag).unwrap()),
                false,
            )
            .await
            .expect("get manifest failed");

        assert_eq!(stored_manifest.content, content);
        assert_eq!(stored_manifest.media_type.unwrap(), media_type);
        assert_eq!(stored_manifest.digest, header_digest(&response.headers));
    })
    .await;
}

#[tokio::test]
async fn test_delete_manifest_by_digest_removes_multiple_tags() {
    for_each_backend(async |test_case| {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo/delete-multi-tags").unwrap();
        let (content, media_type) = create_test_manifest(registry, namespace).await;

        let response = registry
            .put_manifest(
                namespace,
                &Reference::Tag(Tag::new("latest").unwrap()),
                Some(&media_type),
                &content,
            )
            .await
            .unwrap();

        registry
            .put_manifest(
                namespace,
                &Reference::Tag(Tag::new("v1.0").unwrap()),
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
                &Reference::Digest(header_digest(&response.headers)),
            )
            .await
            .unwrap();

        let repository = registry.get_repository_for_namespace(namespace).unwrap();

        assert!(
            registry
                .get_manifest(
                    repository,
                    &[media_type.to_string()],
                    namespace,
                    Reference::Tag(Tag::new("latest").unwrap()),
                    false,
                )
                .await
                .is_err()
        );

        assert!(
            registry
                .get_manifest(
                    repository,
                    &[media_type.to_string()],
                    namespace,
                    Reference::Tag(Tag::new("v1.0").unwrap()),
                    false,
                )
                .await
                .is_err()
        );

        assert!(
            registry
                .get_manifest(
                    repository,
                    &[media_type.to_string()],
                    namespace,
                    Reference::Digest(header_digest(&response.headers)),
                    false,
                )
                .await
                .is_err()
        );
    })
    .await;
}

#[tokio::test]
async fn test_delete_manifest_by_digest_preserves_unrelated_tags() {
    for_each_backend(async |test_case| {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo/delete-preserve").unwrap();
        let (content_a, media_type_a) = create_test_manifest(registry, namespace).await;
        let (content_b, media_type_b) =
            create_test_manifest_with_subject(registry, namespace).await;

        let response_a = registry
            .put_manifest(
                namespace,
                &Reference::Tag(Tag::new("v1.0").unwrap()),
                Some(&media_type_a),
                &content_a,
            )
            .await
            .unwrap();

        registry
            .put_manifest(
                namespace,
                &Reference::Tag(Tag::new("v1.1").unwrap()),
                Some(&media_type_a),
                &content_a,
            )
            .await
            .unwrap();

        let response_b = registry
            .put_manifest(
                namespace,
                &Reference::Tag(Tag::new("v2.0").unwrap()),
                Some(&media_type_b),
                &content_b,
            )
            .await
            .unwrap();

        registry
            .delete_manifest(
                None,
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
                    &[media_type_a.to_string()],
                    namespace,
                    Reference::Tag(Tag::new("v1.0").unwrap()),
                    false,
                )
                .await
                .is_err()
        );

        assert!(
            registry
                .get_manifest(
                    repository,
                    &[media_type_a.to_string()],
                    namespace,
                    Reference::Tag(Tag::new("v1.1").unwrap()),
                    false,
                )
                .await
                .is_err()
        );

        let manifest_b = registry
            .get_manifest(
                repository,
                &[media_type_b.to_string()],
                namespace,
                Reference::Tag(Tag::new("v2.0").unwrap()),
                false,
            )
            .await
            .unwrap();

        assert_eq!(manifest_b.digest, header_digest(&response_b.headers));
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
                        &[media_type_a.to_string()],
                        namespace,
                        Reference::Tag(Tag::new(&format!("tag-{i}")).unwrap()),
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
                        &[media_type_b.to_string()],
                        namespace,
                        Reference::Tag(Tag::new(&format!("other-{i}")).unwrap()),
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

        let digest_link = LinkKind::Digest(header_digest(&response.headers));
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

        let tag_link = LinkKind::Tag(Tag::new(tag).unwrap());
        let tag_meta = registry
            .metadata_store
            .read_link(namespace, &tag_link)
            .await
            .unwrap();
        assert_eq!(
            tag_meta.media_type,
            Some(media_type),
            "Tag link should have media_type stored"
        );
    })
    .await;
}

#[tokio::test]
async fn test_head_manifest_fallback_without_media_type() {
    for_each_backend(async |test_case| {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo/head-fallback").unwrap();
        let (content, media_type) = create_test_manifest(registry, namespace).await;

        let digest = put_blob_direct(registry.metadata_store.store(), &content).await;

        registry
            .metadata_store
            .update_links(
                namespace,
                &[
                    LinkOperation::create(LinkKind::Digest(digest.clone()), digest.clone()),
                    LinkOperation::create(
                        LinkKind::Tag(Tag::new("latest").unwrap()),
                        digest.clone(),
                    ),
                ],
            )
            .await
            .unwrap();

        let link_meta = registry
            .metadata_store
            .read_link(namespace, &LinkKind::Digest(digest.clone()))
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
                &[media_type.to_string()],
                namespace,
                Reference::Tag(Tag::new("latest").unwrap()),
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
            .read_link(
                namespace,
                &LinkKind::Digest(header_digest(&response.headers)),
            )
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

#[tokio::test]
async fn test_handle_get_manifest_redirect_fallback_without_media_type() {
    for_each_backend(async |test_case| {
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo/redirect-fallback").unwrap();
        let (content, media_type) = create_test_manifest(registry, namespace).await;

        let digest = put_blob_direct(registry.metadata_store.store(), &content).await;

        registry
            .metadata_store
            .update_links(
                namespace,
                &[
                    LinkOperation::create(LinkKind::Digest(digest.clone()), digest.clone()),
                    LinkOperation::create(
                        LinkKind::Tag(Tag::new("latest").unwrap()),
                        digest.clone(),
                    ),
                ],
            )
            .await
            .unwrap();

        let response = registry
            .resolve_get_manifest(
                namespace,
                Reference::Tag(Tag::new("latest").unwrap()),
                &[media_type.to_string()],
                false,
            )
            .await
            .unwrap();

        // When the redirect path fires, the media_type must still be present
        // (via fallback reading the blob body). When the blob backend does not
        // support presigned URLs (FS backend), we get a Body response, both are
        // valid; in the Body case the media_type comes from the manifest JSON.
        match response {
            GetManifestResponse::Redirect { headers, .. } => {
                assert!(
                    headers.contains_key(CONTENT_TYPE.as_str()),
                    "Redirect should still include Content-Type via fallback"
                );
            }
            GetManifestResponse::Body { headers, .. } => {
                assert!(headers.contains_key(CONTENT_TYPE.as_str()));
            }
        }
    })
    .await;
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

// Manifest write/delete path tests

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

    let stored_digest = header_digest(&response.headers);
    assert_eq!(stored_digest, expected_digest);

    // The link should be readable via the metadata store.
    let link = registry
        .metadata_store
        .read_link(&namespace, &LinkKind::Tag(Tag::new("v1").unwrap()))
        .await
        .unwrap();
    assert_eq!(link.target, expected_digest);

    // The blob-data should be retrievable via the blob store.
    let body = registry.blob_store.read(&expected_digest).await.unwrap();
    assert_eq!(body, manifest_bytes);

    // Blob-index should record the namespace reference.
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
/// store (where reads look), not inside the metadata transaction. Before the fix
/// the push "succeeded" but the manifest was unreadable (GET 404), and delete
/// could not reclaim it.
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

    // Reads through the blob store (the path GET uses); 404 before the fix.
    assert_eq!(
        registry.blob_store.read(&digest).await.unwrap(),
        manifest_bytes,
        "manifest body must be readable from the blob store",
    );
    // The bytes must not be written under the metadata store's blob path.
    assert!(
        test_case
            .metadata_store()
            .store()
            .get(&blob_path(&digest))
            .await
            .is_err(),
        "manifest body must not land in the metadata store",
    );

    // Symmetric delete: reclaiming the manifest removes it from the blob store.
    registry
        .delete_manifest(None, None, &namespace, &Reference::Digest(digest.clone()))
        .await
        .expect("delete by digest must succeed");
    assert!(
        registry.blob_store.read(&digest).await.is_err(),
        "manifest blob must be reclaimed from the blob store on delete",
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

    // Link and blob-index must still be consistent.
    let digest = Digest::sha256_of_bytes(&manifest_bytes);
    let link = registry
        .metadata_store
        .read_link(&namespace, &LinkKind::Tag(Tag::new("v1").unwrap()))
        .await
        .unwrap();
    assert_eq!(link.target, digest);
}

#[tokio::test]
async fn delete_manifest_removes_links_and_blob_data() {
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
        matches!(result, Err(metadata_store::Error::ReferenceNotFound)),
        "digest link should be gone after delete, got: {result:?}"
    );
}

// --- Receiver-side last-writer-wins (LWW) ------------------------------------

async fn seed_tag(registry: &Registry, namespace: &Namespace, tag: &str) -> (Vec<u8>, MediaType) {
    let (content, media_type) = create_test_manifest(registry, namespace).await;
    registry
        .accept_put_manifest(
            None,
            None,
            namespace,
            Reference::Tag(Tag::new(tag).unwrap()),
            media_type.clone(),
            Cursor::new(content.clone()),
            Vec::new(),
        )
        .await
        .expect("seed tag push");
    (content, media_type)
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
    let source_ts = chrono::Utc::now() - chrono::Duration::hours(3);

    registry
        .accept_put_manifest(
            None,
            Some(source_ts),
            namespace,
            Reference::Tag(Tag::new(tag).unwrap()),
            media_type,
            Cursor::new(content),
            Vec::new(),
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
            None,
            namespace,
            Reference::Tag(Tag::new(tag).unwrap()),
            media_type,
            Cursor::new(content),
            Vec::new(),
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
            Some(older),
            namespace,
            Reference::Tag(Tag::new(tag).unwrap()),
            media_type,
            Cursor::new(content),
            Vec::new(),
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
            Some(newer),
            namespace,
            Reference::Tag(Tag::new(tag).unwrap()),
            media_type,
            Cursor::new(content),
            Vec::new(),
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
            Some(created_at),
            namespace,
            Reference::Tag(Tag::new(tag).unwrap()),
            media_type,
            Cursor::new(content),
            Vec::new(),
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
            Some(ts),
            namespace,
            Reference::Tag(Tag::new(tag).unwrap()),
            larger_mt,
            Cursor::new(larger),
            Vec::new(),
        )
        .await
        .expect("seed the larger-digest manifest");

    let result = registry
        .accept_put_manifest(
            None,
            Some(ts),
            namespace,
            Reference::Tag(Tag::new(tag).unwrap()),
            smaller_mt,
            Cursor::new(smaller),
            Vec::new(),
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
            Some(ts),
            namespace,
            Reference::Tag(Tag::new(tag).unwrap()),
            smaller_mt,
            Cursor::new(smaller),
            Vec::new(),
        )
        .await
        .expect("seed the smaller-digest manifest");

    registry
        .accept_put_manifest(
            None,
            Some(ts),
            namespace,
            Reference::Tag(Tag::new(tag).unwrap()),
            larger_mt,
            Cursor::new(larger),
            Vec::new(),
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
        .write_link_reference(namespace, &link, &sibling)
        .await
        .expect("sibling write behind the cache");

    // Newer than the cached created_at but older than the sibling's, so only
    // an uncached gate read rejects.
    let incoming = created_at + chrono::Duration::seconds(60);
    let result = registry
        .accept_put_manifest(
            None,
            Some(incoming),
            namespace,
            Reference::Tag(Tag::new(tag).unwrap()),
            media_type,
            Cursor::new(content),
            Vec::new(),
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
        )
        .await
        .expect("seed the tag and warm the cache");
    assert_eq!(
        store.cache_get(&namespace, &link).await.map(|m| m.target),
        Some(old_digest.clone()),
        "seed write must warm the link cache",
    );

    // A sibling replica re-points the tag at new_digest behind this cache.
    let mut sibling = store.read_link_reference(&namespace, &link).await.unwrap();
    sibling.target = new_digest.clone();
    store
        .write_link_reference(&namespace, &link, &sibling)
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

/// The pre-write LWW gate in `accept_put_manifest` is check-then-write: a
/// racing writer can pass it and still commit out of order. The link
/// transaction itself must reject a superseded replicated write, so calling
/// the store directly (as if the gate had been raced) must fail and leave
/// the winning link untouched.
#[tokio::test]
async fn store_manifest_enforces_lww_inside_the_link_transaction() {
    let test_case = FSRegistryTestCase::new();
    let store = test_case.metadata_store();
    let namespace = Namespace::new("lww-tx-repo").unwrap();
    let link = LinkKind::Tag(Tag::new("latest").unwrap());

    let newer_body = br#"{"newer":true}"#.to_vec();
    let newer_digest = Digest::sha256_of_bytes(&newer_body);
    let newer_ts = chrono::Utc::now();
    store
        .store_manifest(
            &namespace,
            &[LinkOperation::create(link.clone(), newer_digest.clone())],
            Some(newer_ts),
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
        )
        .await
        .err();
    assert!(
        matches!(
            result,
            Some(metadata_store::Error::ReplicationSuperseded(_))
        ),
        "an older replicated write must be rejected by the transaction itself, got: {result:?}"
    );

    let metadata = store
        .read_link_reference(&namespace, &link)
        .await
        .expect("the winning link must survive");
    assert_eq!(metadata.target, newer_digest);
    assert_eq!(metadata.created_at, Some(newer_ts));
}

/// The pre-write delete gate in `delete_manifest` is also check-then-write: a
/// newer re-put can land between the gate and the commit. The link transaction
/// must reject a superseded replicated delete itself, so calling the store
/// directly (as if the gate had been raced) must fail and leave the tag intact.
#[tokio::test]
async fn delete_links_enforces_lww_inside_the_link_transaction() {
    let test_case = FSRegistryTestCase::new();
    let store = test_case.metadata_store();
    let namespace = Namespace::new("lww-tx-repo").unwrap();
    let link = LinkKind::Tag(Tag::new("latest").unwrap());

    let body = br#"{"winner":true}"#.to_vec();
    let digest = Digest::sha256_of_bytes(&body);
    let newer_ts = chrono::Utc::now();
    store
        .store_manifest(
            &namespace,
            &[LinkOperation::create(link.clone(), digest.clone())],
            Some(newer_ts),
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
        matches!(
            result,
            Some(metadata_store::Error::ReplicationSuperseded(_))
        ),
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
            Some(very_old),
            namespace,
            Reference::Tag(Tag::new(tag).unwrap()),
            media_type,
            Cursor::new(content),
            Vec::new(),
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
            None,
            namespace,
            Reference::Tag(Tag::new(tag).unwrap()),
            media_type,
            Cursor::new(content),
            Vec::new(),
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
            Some(very_old),
            namespace,
            Reference::Digest(digest),
            media_type,
            Cursor::new(content),
            Vec::new(),
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
        matches!(result, Err(metadata_store::Error::ReferenceNotFound)),
        "tag must be gone after an accepted delete, got: {result:?}"
    );
}

#[tokio::test]
async fn delete_manifest_not_superseded_when_local_tag_has_no_created_at() {
    // The write path always stamps `created_at`, so a legacy no-created_at
    // link is injected directly via `write_link_reference`.
    let test_case = FSRegistryTestCase::new();
    let registry = test_case.registry();
    let namespace = &Namespace::new("lww-repo").unwrap();
    let tag = "latest";

    registry
        .metadata_store
        .write_link_reference(
            namespace,
            &LinkKind::Tag(Tag::new(tag).unwrap()),
            &LinkMetadata {
                target: Digest::sha256_of_bytes(b"manifest-bytes"),
                created_at: None,
                accessed_at: None,
                referenced_by: HashSet::new(),
                media_type: None,
                descriptor: None,
            },
        )
        .await
        .expect("seed a tag link with no created_at");

    // An epoch source_ts would lose LWW to any real created_at.
    let ancient = chrono::DateTime::from_timestamp(0, 0).unwrap();
    registry
        .delete_manifest(
            None,
            Some(ancient),
            namespace,
            &Reference::Tag(Tag::new(tag).unwrap()),
        )
        .await
        .expect("a delete must not be superseded by a tag with no created_at");

    let result = registry
        .metadata_store
        .read_link(namespace, &LinkKind::Tag(Tag::new(tag).unwrap()))
        .await;
    assert!(
        matches!(result, Err(metadata_store::Error::ReferenceNotFound)),
        "tag must be gone after the non-superseded delete, got: {result:?}"
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
            None,
            namespace,
            Reference::Tag(Tag::new(tag).unwrap()),
            media_type,
            Cursor::new(content),
            Vec::new(),
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
async fn replicated_delete_not_superseded_by_a_legacy_link() {
    // A pre-JSON distribution-era link (bare digest string) parses to
    // created_at=None, so it must never win LWW; a synthesised now() would
    // re-stamp fresher on every read and block every replicated write to a
    // migrated tag forever.
    let test_case = FSRegistryTestCase::new();
    let registry = test_case.registry();
    let namespace = &Namespace::new("legacy-repo").unwrap();
    let link = LinkKind::Tag(Tag::new("latest").unwrap());

    let legacy_digest = Digest::sha256_of_bytes(b"legacy-manifest");
    put_link_raw(
        registry.metadata_store.store(),
        namespace,
        &link,
        legacy_digest.to_string().as_bytes(),
    )
    .await;

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
        matches!(result, Err(metadata_store::Error::ReferenceNotFound)),
        "the legacy tag must be gone after the non-superseded delete, got: {result:?}"
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
        matches!(tag_result, Err(metadata_store::Error::ReferenceNotFound)),
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
        matches!(result, Err(metadata_store::Error::ReferenceNotFound)),
        "the downstream tag must be gone after an applied prune delete, got: {result:?}"
    );
}

#[tokio::test]
async fn replication_superseded_maps_to_distinct_oci_code() {
    let superseded: ServerError = Error::ReplicationSuperseded("newer".to_string()).into();
    let conflict: ServerError = Error::Conflict("immutable".to_string()).into();

    // Both are 409, but the OCI codes differ so the sender can disambiguate.
    assert_eq!(superseded.status_code(), hyper::StatusCode::CONFLICT);
    assert_eq!(conflict.status_code(), hyper::StatusCode::CONFLICT);

    let superseded_json = superseded.as_json(None);
    let conflict_json = conflict.as_json(None);
    assert_eq!(
        superseded_json["errors"][0]["code"],
        REPLICATION_SUPERSEDED_CODE
    );
    assert_eq!(conflict_json["errors"][0]["code"], "CONFLICT");
    assert_ne!(
        superseded_json["errors"][0]["code"],
        conflict_json["errors"][0]["code"]
    );
}

#[cfg(test)]
mod noop_suppression_tests {
    //! No-op suppression (loop prevention): an inbound manifest write that does
    //! not change local state must not be re-dispatched for replication. The
    //! harness shares one `Store` between the blob store, metadata store, and a
    //! caller-held `JobStore`, and spawns no drain, so enqueued jobs persist
    //! for counting.

    use std::{io::Cursor, sync::Arc};

    use chrono::{Duration, Utc};
    use tempfile::TempDir;

    use angos_tx_engine::transaction::Transaction;

    use crate::{
        event_webhook::event::EventKind,
        oci::{Digest, MediaType, Namespace, Reference, Tag},
        registry::{
            Registry, RegistryConfig,
            job_store::{JobStore, Queue},
            metadata_store::{LinkKind, LinkOperation},
            test_utils::{
                FsTestStack, downstream_client, fs_test_stack, repository_with_downstream,
                single_repo_resolver, sole_pending_payload, upload_blob,
            },
        },
        replication::REPLICATION_DELETE_MANIFEST_KIND,
    };

    use super::{create_test_manifest, manifest_with_references};

    const REPO: &str = "nginx";
    const NAMESPACE: &str = "nginx";

    /// Build a `Registry` whose blob store, metadata store, and a caller-held
    /// `JobStore` all share one FS-backed `Store`, carrying a single
    /// event+reconcile downstream so `dispatch_replication` enqueues.
    fn build_registry() -> (Registry, Arc<JobStore>, TempDir) {
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

        let job_store: Arc<JobStore> = Arc::new(JobStore::new(store, "test"));

        let config = RegistryConfig::default().job_queue(job_store.clone());
        let registry = Registry::new(blob_store, metadata_store, resolver, config).unwrap();
        (registry, job_store, dir)
    }

    async fn pending(job_store: &JobStore) -> u64 {
        job_store
            .count_pending(Queue::Replication, 0)
            .await
            .unwrap()
    }

    /// Drains (claims + completes) one pending replication job, clearing its
    /// `lock_key` dedup index. Pending pushes for the same tag coalesce on
    /// that index, so draining isolates the dispatch gate under test from the
    /// queue's coalescing.
    async fn drain_one(job_store: &JobStore) {
        let claimed = job_store
            .claim_one(Queue::Replication)
            .await
            .unwrap()
            .claimed
            .expect("expected one claimable job");
        job_store
            .complete(claimed, Transaction::builder().build())
            .await
            .unwrap();
    }

    /// Builds a second, distinct manifest, pre-uploading its blobs so the push
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
                None,
                &namespace,
                Reference::Tag(Tag::new(tag).unwrap()),
                media_type.clone(),
                Cursor::new(content_a.clone()),
                Vec::new(),
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
                None,
                &namespace,
                Reference::Tag(Tag::new(tag).unwrap()),
                media_type.clone(),
                Cursor::new(content_a.clone()),
                Vec::new(),
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
                None,
                &namespace,
                Reference::Tag(Tag::new(tag).unwrap()),
                media_type_b,
                Cursor::new(content_b),
                Vec::new(),
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
                None,
                &namespace,
                Reference::Digest(digest.clone()),
                media_type.clone(),
                Cursor::new(content.clone()),
                Vec::new(),
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
                None,
                &namespace,
                Reference::Digest(digest),
                media_type,
                Cursor::new(content),
                Vec::new(),
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
                None,
                &namespace,
                Reference::Digest(digest.clone()),
                media_type.clone(),
                Cursor::new(content.clone()),
                Vec::new(),
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
                None,
                &namespace,
                Reference::Digest(digest.clone()),
                media_type.clone(),
                Cursor::new(content.clone()),
                vec![Tag::new(tag).unwrap()],
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
                None,
                &namespace,
                Reference::Digest(digest),
                media_type,
                Cursor::new(content),
                vec![Tag::new(tag).unwrap()],
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
                None,
                &namespace,
                Reference::Tag(Tag::new(tag).unwrap()),
                media_type,
                Cursor::new(content),
                Vec::new(),
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
                None,
                &namespace,
                Reference::Tag(Tag::new(tag).unwrap()),
                media_type.clone(),
                Cursor::new(content.clone()),
                Vec::new(),
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
                None,
                &namespace,
                Reference::Tag(Tag::new(tag).unwrap()),
                media_type,
                Cursor::new(content),
                Vec::new(),
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
                Some(push_ts),
                &namespace,
                Reference::Tag(Tag::new(tag).unwrap()),
                media_type,
                Cursor::new(content),
                Vec::new(),
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
        assert_eq!(payload.kind, REPLICATION_DELETE_MANIFEST_KIND);
        assert_eq!(
            payload.source_ts.as_deref(),
            Some(delete_ts.to_rfc3339().as_str()),
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
                None,
                &namespace,
                Reference::Tag(Tag::new("latest").unwrap()),
                media_type,
                Cursor::new(content),
                Vec::new(),
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
                None,
                &namespace,
                Reference::Tag(Tag::new("latest").unwrap()),
                media_type,
                Cursor::new(content),
                Vec::new(),
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

    #[tokio::test]
    async fn noop_push_still_emits_webhook_events() {
        let (registry, job_store, _dir) = build_registry();
        let namespace = Namespace::new(NAMESPACE).unwrap();
        let tag = "latest";

        let (content, media_type) = create_test_manifest(&registry, &namespace).await;
        registry
            .accept_put_manifest(
                None,
                None,
                &namespace,
                Reference::Tag(Tag::new(tag).unwrap()),
                media_type.clone(),
                Cursor::new(content.clone()),
                Vec::new(),
            )
            .await
            .expect("seed tag push");
        let baseline = pending(&job_store).await;

        let response = registry
            .accept_put_manifest(
                None,
                None,
                &namespace,
                Reference::Tag(Tag::new(tag).unwrap()),
                media_type,
                Cursor::new(content),
                Vec::new(),
            )
            .await
            .expect("re-assert same tag->digest");

        assert_eq!(
            pending(&job_store).await,
            baseline,
            "the no-op replay must NOT enqueue a replication job"
        );
        assert!(
            response
                .events
                .iter()
                .any(|e| e.kind == EventKind::ManifestPush),
            "a no-op push must still emit ManifestPush"
        );
        assert!(
            response
                .events
                .iter()
                .any(|e| e.kind == EventKind::TagCreate),
            "a no-op tag push must still emit TagCreate"
        );
    }
}

#[cfg(test)]
mod dispatch_replication_tests {
    use std::sync::Arc;

    use tempfile::TempDir;

    use chrono::{DateTime, Duration, Utc};
    use regex::Regex;

    use crate::{
        oci::{Digest, Namespace, Tag},
        registry::{
            Registry, RegistryConfig, Repository,
            job_store::{JobStore, Queue},
            test_utils::{
                FsTestStack, downstream_client, fs_test_stack, repository_with_replication,
                single_repo_resolver, sole_pending_payload,
            },
        },
        replication::{
            REPLICATION_DELETE_MANIFEST_KIND, REPLICATION_PUSH_MANIFEST_KIND,
            ReplicationDownstream, ReplicationMode, ReplicationPushPayload,
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
        ReplicationDownstream::builder(
            name.to_string(),
            downstream_client("https://unused.test"),
            4,
        )
        .mode(mode)
        .namespace_filter(namespace_filter)
        .build()
    }

    /// Repository with exactly one downstream of the given mode and namespace filter.
    fn repository_with(mode: ReplicationMode, namespace_filter: Vec<Regex>) -> Repository {
        repository_with_replication(
            REPO,
            vec![downstream_with(DOWNSTREAM, mode, namespace_filter)],
        )
    }

    fn repository_with_downstream() -> Repository {
        repository_with(ReplicationMode::EventReconcile, Vec::new())
    }

    /// Build a `Registry` whose job store is a caller-held `JobStore` so the
    /// test can count pending jobs.
    fn build_registry_with(repository: Repository) -> (Registry, Arc<JobStore>, TempDir) {
        let FsTestStack {
            dir,
            store,
            metadata_store,
            blob_store,
        } = fs_test_stack();
        let resolver = single_repo_resolver(REPO, repository);

        // No drain spawned: the bare JobStore only persists envelopes; these tests assert enqueue only.
        let job_store: Arc<JobStore> = Arc::new(JobStore::new(store, "test"));

        let config = RegistryConfig::default().job_queue(job_store.clone());
        let registry = Registry::new(blob_store, metadata_store, resolver, config).unwrap();

        (registry, job_store, dir)
    }

    /// [`build_registry_with`] with one `event+reconcile` downstream.
    fn build_registry() -> (Registry, Arc<JobStore>, TempDir) {
        build_registry_with(repository_with_downstream())
    }

    #[tokio::test]
    async fn dispatch_replication_enqueues_for_matching_downstream() {
        crate::metrics_provider::init_for_tests();
        let (registry, job_store, _dir) = build_registry();

        let namespace = Namespace::new(NAMESPACE).unwrap();
        let digest: Digest = SAMPLE_DIGEST.parse().unwrap();

        let repository = registry.resolver.resolve(&namespace);
        let tag = Tag::new("v1").unwrap();
        registry
            .dispatch_replication(
                repository,
                &namespace,
                REPLICATION_PUSH_MANIFEST_KIND,
                Some(&tag),
                Some(&digest),
                None,
            )
            .await;

        assert_eq!(
            job_store
                .count_pending(Queue::Replication, 0)
                .await
                .unwrap(),
            1,
            "a fresh local change must enqueue one push job"
        );
    }

    /// The payload carries the correct downstream/namespace/tag/digest/kind and
    /// a populated `source_ts`.
    #[tokio::test]
    async fn dispatch_replication_payload_is_well_formed() {
        crate::metrics_provider::init_for_tests();
        let (registry, job_store, _dir) = build_registry();

        let namespace = Namespace::new(NAMESPACE).unwrap();
        let digest: Digest = SAMPLE_DIGEST.parse().unwrap();

        let repository = registry.resolver.resolve(&namespace);
        let tag = Tag::new("v1").unwrap();
        registry
            .dispatch_replication(
                repository,
                &namespace,
                REPLICATION_PUSH_MANIFEST_KIND,
                Some(&tag),
                Some(&digest),
                None,
            )
            .await;

        let payload = sole_pending_payload(&job_store).await;
        assert_eq!(payload.downstream, DOWNSTREAM);
        assert_eq!(payload.namespace, NAMESPACE);
        assert_eq!(payload.tag.as_deref(), Some("v1"));
        assert_eq!(payload.digest.as_deref(), Some(SAMPLE_DIGEST));
        assert_eq!(payload.kind, REPLICATION_PUSH_MANIFEST_KIND);
        let source_ts = payload.source_ts.expect("source_ts must be present");
        assert!(
            DateTime::parse_from_rfc3339(&source_ts).is_ok(),
            "source_ts must be a valid RFC 3339 timestamp; got {source_ts}"
        );
    }

    /// The fan-out enqueues concurrently (one index GET plus CAS transaction
    /// per downstream); every matching downstream must still get exactly one
    /// job carrying its own name.
    #[tokio::test]
    async fn dispatch_replication_enqueues_one_job_per_downstream() {
        crate::metrics_provider::init_for_tests();
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
                REPLICATION_PUSH_MANIFEST_KIND,
                Some(&tag),
                Some(&digest),
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
            let payload: ReplicationPushPayload =
                serde_json::from_value(envelope.payload).expect("decode ReplicationPushPayload");
            downstreams.push(payload.downstream);
        }
        downstreams.sort();
        assert_eq!(
            downstreams,
            vec![DOWNSTREAM.to_string(), "us-region".to_string()],
            "one job per downstream, each addressed to its own downstream"
        );
    }

    /// A caller-provided timestamp (an inbound replicated delete's author
    /// time) propagates verbatim; a re-stamped `now()` would let the bounced
    /// delete outrank a recreate authored in between.
    #[tokio::test]
    async fn dispatch_replication_uses_provided_source_ts_verbatim() {
        crate::metrics_provider::init_for_tests();
        let (registry, job_store, _dir) = build_registry();

        let namespace = Namespace::new(NAMESPACE).unwrap();
        let author_ts = Utc::now() - Duration::hours(3);

        let repository = registry.resolver.resolve(&namespace);
        let tag = Tag::new("v1").unwrap();
        registry
            .dispatch_replication(
                repository,
                &namespace,
                REPLICATION_DELETE_MANIFEST_KIND,
                Some(&tag),
                None,
                Some(author_ts),
            )
            .await;

        let payload = sole_pending_payload(&job_store).await;
        assert_eq!(payload.kind, REPLICATION_DELETE_MANIFEST_KIND);
        assert_eq!(
            payload.source_ts.as_deref(),
            Some(author_ts.to_rfc3339().as_str()),
            "a provided source_ts must propagate verbatim, not be re-stamped"
        );
    }

    #[tokio::test]
    async fn dispatch_replication_skips_reconcile_only_downstream() {
        crate::metrics_provider::init_for_tests();
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
                REPLICATION_PUSH_MANIFEST_KIND,
                Some(&tag),
                Some(&digest),
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
        crate::metrics_provider::init_for_tests();
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
                REPLICATION_PUSH_MANIFEST_KIND,
                Some(&tag),
                Some(&digest),
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
