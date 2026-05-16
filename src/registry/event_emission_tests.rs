//! Tests verifying event emission (and its suppression) for put/delete manifest
//! operations based on whether the reference is a tag or a digest.
//!
//! The suppression logic lives in `Registry::accept_put_manifest` and
//! `Registry::delete_manifest`: tag-specific events (`TagCreate`, `TagDelete`)
//! are emitted only when the reference is a `Reference::Tag`; they are
//! suppressed for `Reference::Digest` references.
//!
//! These tests exercise the event-construction helpers on `Registry` directly
//! (the "unit-test pivot" path), which avoids the need to stand up a full
//! `ServerContext` or HTTP listener while still covering the suppression logic.

use std::{io::Cursor, sync::Arc};

use chrono::Utc;
use serde_json::json;
use tempfile::TempDir;
use url::Url;
use uuid::Uuid;
use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

use crate::{
    event_webhook::{
        config::{DeliveryPolicy, EventWebhookConfig},
        dispatcher::EventDispatcher,
        event::EventKind,
    },
    oci::{Digest, Namespace, Reference},
    registry::{
        Registry, blob_store, data_store, metadata_store, test_utils::create_test_registry,
    },
    util::sha256,
};

// ---------------------------------------------------------------------------
// Test fixture helpers
// ---------------------------------------------------------------------------

struct FsRegistryFixture {
    registry: Registry,
    _temp_dir: TempDir,
}

impl FsRegistryFixture {
    fn new() -> Self {
        let temp_dir = TempDir::new().expect("tempdir");
        let path = temp_dir.path().to_string_lossy().to_string();

        let blob_store = Arc::new(blob_store::fs::Backend::new(
            &data_store::fs::BackendConfig {
                root_dir: path.clone(),
                sync_to_disk: false,
            },
        ));

        let metadata_store_backend = Arc::new(
            metadata_store::fs::Backend::new(&metadata_store::fs::BackendConfig {
                root_dir: path,
                sync_to_disk: false,
                lock_strategy: metadata_store::LockStrategy::Memory,
            })
            .unwrap(),
        );

        let registry = create_test_registry(
            blob_store.clone(),
            blob_store.clone(),
            None,
            metadata_store_backend,
        );

        Self {
            registry,
            _temp_dir: temp_dir,
        }
    }
}

/// Build an `EventDispatcher` wired to a wiremock server and subscribing to
/// all manifest and tag event kinds.
fn build_dispatcher_for_server(server_uri: &str) -> EventDispatcher {
    use std::collections::HashMap;

    let config = EventWebhookConfig {
        url: Url::parse(server_uri).unwrap(),
        policy: DeliveryPolicy::Required,
        token: None,
        timeout_ms: 5_000,
        max_retries: 0,
        events: vec![
            EventKind::ManifestPush,
            EventKind::ManifestDelete,
            EventKind::TagCreate,
            EventKind::TagDelete,
        ],
        repository_filter: None,
    };
    let mut webhooks = HashMap::new();
    webhooks.insert("test-hook".to_string(), config);
    EventDispatcher::new(webhooks).expect("dispatcher build")
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

/// Minimal valid OCI manifest bytes and its media-type string.
async fn test_manifest_bytes(registry: &Registry, namespace: &Namespace) -> (Vec<u8>, String) {
    let config_content = b"{}";
    let config_digest = upload_blob(registry, namespace, config_content).await;
    let manifest = json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": config_digest,
            "size": config_content.len()
        },
        "layers": []
    });
    let bytes = serde_json::to_vec(&manifest).unwrap();
    let mime = "application/vnd.oci.image.manifest.v1+json".to_string();
    (bytes, mime)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Tag-based manifest push emits a `ManifestPush` event and a `TagCreate`
/// event; the `TagCreate` event carries the tag name.
#[tokio::test]
async fn tag_push_emits_manifest_push_and_tag_create_events() {
    let fixture = FsRegistryFixture::new();
    let namespace = Namespace::new("test-repo").unwrap();
    let (manifest_bytes, mime_type) = test_manifest_bytes(&fixture.registry, &namespace).await;

    let response = fixture
        .registry
        .accept_put_manifest(
            None,
            &namespace,
            Reference::Tag("latest".to_string()),
            mime_type,
            Cursor::new(manifest_bytes),
        )
        .await
        .expect("accept_put_manifest");

    let kinds: Vec<&EventKind> = response.events.iter().map(|e| &e.kind).collect();
    assert!(
        kinds.contains(&&EventKind::ManifestPush),
        "ManifestPush event must be present for tag-based push; got {kinds:?}"
    );
    assert!(
        kinds.contains(&&EventKind::TagCreate),
        "TagCreate event must be present for tag-based push; got {kinds:?}"
    );

    let tag_create = response
        .events
        .iter()
        .find(|e| e.kind == EventKind::TagCreate)
        .unwrap();
    assert_eq!(
        tag_create.tag.as_deref(),
        Some("latest"),
        "TagCreate event must carry the tag name"
    );
    assert!(
        tag_create.reference.is_some(),
        "TagCreate event must carry a reference"
    );
}

/// Digest-based manifest push emits only a `ManifestPush` event; no
/// `TagCreate` event is emitted (suppression on digest references).
#[tokio::test]
async fn digest_push_suppresses_tag_create_event() {
    let fixture = FsRegistryFixture::new();
    let namespace = Namespace::new("test-repo").unwrap();
    let (manifest_bytes, mime_type) = test_manifest_bytes(&fixture.registry, &namespace).await;

    // First push with a tag to obtain the digest.
    let tag_response = fixture
        .registry
        .accept_put_manifest(
            None,
            &namespace,
            Reference::Tag("seed".to_string()),
            mime_type.clone(),
            Cursor::new(manifest_bytes.clone()),
        )
        .await
        .expect("seed push");

    let digest_str = tag_response
        .headers
        .get("Docker-Content-Digest")
        .cloned()
        .expect("digest header");
    let digest: crate::oci::Digest = digest_str.parse().expect("parse digest");

    // Push the same manifest addressed by its digest.
    let response = fixture
        .registry
        .accept_put_manifest(
            None,
            &namespace,
            Reference::Digest(digest),
            mime_type,
            Cursor::new(manifest_bytes),
        )
        .await
        .expect("digest push");

    let has_tag_create = response
        .events
        .iter()
        .any(|e| e.kind == EventKind::TagCreate);
    assert!(
        !has_tag_create,
        "TagCreate must NOT be emitted for a digest-based push; got {:?}",
        response.events
    );

    let has_manifest_push = response
        .events
        .iter()
        .any(|e| e.kind == EventKind::ManifestPush);
    assert!(
        has_manifest_push,
        "ManifestPush must still be emitted even for a digest-based push"
    );
}

/// Tag-based manifest delete emits both `ManifestDelete` and `TagDelete`
/// events; the `TagDelete` event carries the tag name.
#[tokio::test]
async fn tag_delete_emits_manifest_delete_and_tag_delete_events() {
    let fixture = FsRegistryFixture::new();
    let namespace = Namespace::new("test-repo").unwrap();
    let (manifest_bytes, mime_type) = test_manifest_bytes(&fixture.registry, &namespace).await;

    fixture
        .registry
        .accept_put_manifest(
            None,
            &namespace,
            Reference::Tag("v1".to_string()),
            mime_type,
            Cursor::new(manifest_bytes),
        )
        .await
        .expect("put manifest");

    let reference = Reference::Tag("v1".to_string());
    let response = fixture
        .registry
        .delete_manifest(None, &namespace, &reference)
        .await
        .expect("delete_manifest");

    let kinds: Vec<&EventKind> = response.events.iter().map(|e| &e.kind).collect();
    assert!(
        kinds.contains(&&EventKind::ManifestDelete),
        "ManifestDelete must be emitted for tag-based delete; got {kinds:?}"
    );
    assert!(
        kinds.contains(&&EventKind::TagDelete),
        "TagDelete must be emitted for tag-based delete; got {kinds:?}"
    );

    let tag_delete = response
        .events
        .iter()
        .find(|e| e.kind == EventKind::TagDelete)
        .unwrap();
    assert_eq!(
        tag_delete.tag.as_deref(),
        Some("v1"),
        "TagDelete must carry the tag name"
    );
}

/// Digest-based manifest delete emits only `ManifestDelete`; no `TagDelete`
/// event is emitted (suppression on digest references).
#[tokio::test]
async fn digest_delete_suppresses_tag_delete_event() {
    let fixture = FsRegistryFixture::new();
    let namespace = Namespace::new("test-repo").unwrap();
    let (manifest_bytes, mime_type) = test_manifest_bytes(&fixture.registry, &namespace).await;

    let push = fixture
        .registry
        .accept_put_manifest(
            None,
            &namespace,
            Reference::Tag("to-delete".to_string()),
            mime_type,
            Cursor::new(manifest_bytes),
        )
        .await
        .expect("put manifest");

    let digest_str = push
        .headers
        .get("Docker-Content-Digest")
        .cloned()
        .expect("digest header");
    let digest: crate::oci::Digest = digest_str.parse().expect("parse digest");

    let reference = Reference::Digest(digest);
    let response = fixture
        .registry
        .delete_manifest(None, &namespace, &reference)
        .await
        .expect("delete_manifest");

    let has_tag_delete = response
        .events
        .iter()
        .any(|e| e.kind == EventKind::TagDelete);
    assert!(
        !has_tag_delete,
        "TagDelete must NOT be emitted for a digest-based delete; got {:?}",
        response.events
    );

    let has_manifest_delete = response
        .events
        .iter()
        .any(|e| e.kind == EventKind::ManifestDelete);
    assert!(
        has_manifest_delete,
        "ManifestDelete must still be emitted for a digest-based delete"
    );
}

/// All required fields (`id`, `timestamp`, `repository`, `namespace`, `kind`,
/// `tag`, `reference`, `digest`) are present and valid on the `TagCreate`
/// event produced by a tag-based push.
#[tokio::test]
async fn tag_push_event_payload_has_all_required_fields() {
    let fixture = FsRegistryFixture::new();
    let namespace = Namespace::new("test-repo").unwrap();
    let (manifest_bytes, mime_type) = test_manifest_bytes(&fixture.registry, &namespace).await;

    let before = Utc::now();
    let response = fixture
        .registry
        .accept_put_manifest(
            None,
            &namespace,
            Reference::Tag("stable".to_string()),
            mime_type,
            Cursor::new(manifest_bytes),
        )
        .await
        .expect("accept_put_manifest");
    let after = Utc::now();

    let tag_create = response
        .events
        .iter()
        .find(|e| e.kind == EventKind::TagCreate)
        .expect("TagCreate event must exist for tag push");

    assert_ne!(
        tag_create.id,
        Uuid::nil(),
        "Event id must be a non-nil UUID"
    );

    assert!(
        tag_create.timestamp >= before && tag_create.timestamp <= after,
        "Event timestamp {ts} must lie between before ({before}) and after ({after})",
        ts = tag_create.timestamp,
    );

    assert!(
        !tag_create.repository.is_empty(),
        "Event repository must not be empty"
    );
    assert_eq!(
        tag_create.namespace,
        namespace.to_string(),
        "Event namespace must match the pushed namespace"
    );
    assert_eq!(tag_create.tag.as_deref(), Some("stable"));
    assert!(
        tag_create.reference.is_some(),
        "Event reference must be set"
    );
    assert!(
        tag_create.digest.is_some(),
        "Event digest must be set on TagCreate"
    );
}

/// Events produced by a tag-based push are delivered via a
/// `Required`-policy `EventDispatcher` to a wiremock endpoint.
/// The mock server must receive exactly two requests: one for `ManifestPush`
/// and one for `TagCreate`.
#[tokio::test]
async fn tag_push_events_delivered_to_webhook_endpoint() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let dispatcher = build_dispatcher_for_server(&server.uri());
    let fixture = FsRegistryFixture::new();
    let namespace = Namespace::new("test-repo").unwrap();
    let (manifest_bytes, mime_type) = test_manifest_bytes(&fixture.registry, &namespace).await;

    let response = fixture
        .registry
        .accept_put_manifest(
            None,
            &namespace,
            Reference::Tag("webhook-test".to_string()),
            mime_type,
            Cursor::new(manifest_bytes),
        )
        .await
        .expect("accept_put_manifest");

    for event in &response.events {
        dispatcher
            .dispatch(event)
            .await
            .expect("event dispatch must succeed");
    }

    let received = server.received_requests().await.expect("received_requests");
    assert_eq!(
        received.len(),
        2,
        "Expected exactly 2 webhook deliveries (ManifestPush + TagCreate); got {}",
        received.len()
    );
}

/// Events produced by a digest-based push are delivered via a
/// `Required`-policy `EventDispatcher` to a wiremock endpoint.
/// The mock server must receive exactly one request (`ManifestPush` only;
/// no `TagCreate` is suppressed for digest references).
#[tokio::test]
async fn digest_push_events_delivered_to_webhook_endpoint() {
    let fixture = FsRegistryFixture::new();
    let namespace = Namespace::new("test-repo").unwrap();
    let (manifest_bytes, mime_type) = test_manifest_bytes(&fixture.registry, &namespace).await;

    // Seed push to get a concrete digest.
    let seed = fixture
        .registry
        .accept_put_manifest(
            None,
            &namespace,
            Reference::Tag("seed2".to_string()),
            mime_type.clone(),
            Cursor::new(manifest_bytes.clone()),
        )
        .await
        .expect("seed push");

    let digest_str = seed
        .headers
        .get("Docker-Content-Digest")
        .cloned()
        .expect("digest header");
    let digest: crate::oci::Digest = digest_str.parse().expect("parse digest");

    let response = fixture
        .registry
        .accept_put_manifest(
            None,
            &namespace,
            Reference::Digest(digest),
            mime_type,
            Cursor::new(manifest_bytes),
        )
        .await
        .expect("digest push");

    // Stand up a fresh mock server for this specific dispatch so we count
    // only the digest-push deliveries.
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let dispatcher = build_dispatcher_for_server(&server.uri());
    for event in &response.events {
        dispatcher.dispatch(event).await.expect("event dispatch");
    }

    let received = server.received_requests().await.expect("received_requests");
    assert_eq!(
        received.len(),
        1,
        "Expected exactly 1 webhook delivery (ManifestPush only) for digest-based push; got {}",
        received.len()
    );
}
