//! Tests verifying event emission (and its suppression) for put/delete manifest
//! operations based on whether the reference is a tag or a digest.
//!
//! The suppression logic lives in `Registry::accept_put_manifest` and
//! `Registry::delete_manifest`: tag-specific events (`TagCreate`, `TagDelete`)
//! are emitted only when the reference is a `Reference::Tag`; they are
//! suppressed for `Reference::Digest` references.
//!
//! Operations dispatch their own events, so the tests observe emission through
//! a `required`-policy webhook wired into the registry and pointed at a
//! wiremock endpoint.

use std::{collections::HashMap, io::Cursor, sync::Arc};

use chrono::{DateTime, Utc};
use serde_json::json;
use tempfile::TempDir;
use url::Url;
use uuid::Uuid;
use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

use crate::{
    event_webhook::{
        config::{DeliveryPolicy, EventWebhookConfig},
        dispatcher::EventDispatcher,
        event::{EventActor, EventKind},
    },
    oci::{Digest, MediaType, Namespace, Reference, Tag},
    registry::{
        BlobMount, Registry, RegistryConfig, Repository, StartUploadResponse,
        blob_ownership::BlobOwnership,
        job_store::{JobStore, Queue},
        metadata_store::LinkKind,
        repository_resolver::RepositoryResolver,
        test_utils::{
            FsTestStack, create_test_repositories, downstream_client, fs_test_stack,
            put_blob_direct, repository_with_downstream, repository_with_replication,
            single_repo_resolver, sole_pending_payload, upload_blob,
        },
    },
    replication::{
        REPLICATION_DELETE_MANIFEST_KIND, REPLICATION_PUSH_MANIFEST_KIND, ReplicationDownstream,
        ReplicationMode,
    },
};

// ---------------------------------------------------------------------------
// Test fixture helpers
// ---------------------------------------------------------------------------

struct FsRegistryFixture {
    registry: Arc<Registry>,
    _temp_dir: TempDir,
}

/// The manifest and tag event kinds, the subscription of most tests here.
/// Blob pushes stay unsubscribed so `upload_blob` seeding does not show up in
/// the delivery counts.
fn manifest_and_tag_kinds() -> Vec<EventKind> {
    vec![
        EventKind::ManifestPush,
        EventKind::ManifestDelete,
        EventKind::TagCreate,
        EventKind::TagDelete,
    ]
}

impl FsRegistryFixture {
    /// Registry wired to a `required`-policy webhook subscribing to `kinds`,
    /// so every matching emitted event is observable at the wiremock endpoint.
    fn with_webhook(server_uri: &str, kinds: Vec<EventKind>) -> Self {
        let webhook = EventWebhookConfig {
            url: Url::parse(server_uri).unwrap(),
            policy: DeliveryPolicy::Required,
            token: None,
            timeout_ms: 5_000,
            max_retries: 0,
            events: kinds,
            repository_filter: None,
        };
        let mut webhooks = HashMap::new();
        webhooks.insert("test-hook".to_string(), webhook);
        let dispatcher = EventDispatcher::new(webhooks).expect("dispatcher build");

        let FsTestStack {
            dir,
            store: _,
            metadata_store,
            blob_store,
        } = fs_test_stack();
        let resolver = Arc::new(
            RepositoryResolver::new(create_test_repositories())
                .expect("test repositories must not have overlapping prefixes"),
        );
        let config = RegistryConfig {
            event_dispatcher: Some(Arc::new(dispatcher)),
            ..RegistryConfig::default()
        };
        let registry = Registry::new(blob_store, metadata_store, resolver, config).unwrap();

        Self {
            registry,
            _temp_dir: dir,
        }
    }
}

/// The event payloads the wiremock endpoint received, in delivery order.
async fn received_events(server: &MockServer) -> Vec<serde_json::Value> {
    server
        .received_requests()
        .await
        .expect("received_requests")
        .iter()
        .map(|request| serde_json::from_slice(&request.body).expect("event payload must be JSON"))
        .collect()
}

fn kinds_of(events: &[serde_json::Value]) -> Vec<String> {
    events
        .iter()
        .map(|e| e["kind"].as_str().unwrap_or_default().to_string())
        .collect()
}

/// Minimal valid OCI manifest bytes and its media type.
async fn test_manifest_bytes(registry: &Registry, namespace: &Namespace) -> (Vec<u8>, MediaType) {
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
    let mime = MediaType::new("application/vnd.oci.image.manifest.v1+json").unwrap();
    (bytes, mime)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Tag-based manifest push delivers exactly a `ManifestPush` and a `TagCreate`
/// event; the `TagCreate` event carries the tag name.
#[tokio::test]
async fn tag_push_emits_manifest_push_and_tag_create_events() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    let fixture = FsRegistryFixture::with_webhook(&server.uri(), manifest_and_tag_kinds());
    let namespace = Namespace::new("test-repo").unwrap();
    let (manifest_bytes, mime_type) = test_manifest_bytes(&fixture.registry, &namespace).await;

    fixture
        .registry
        .accept_put_manifest(
            None,
            None,
            &namespace,
            Reference::Tag(Tag::new("latest").unwrap()),
            mime_type,
            Cursor::new(manifest_bytes),
            Vec::new(),
        )
        .await
        .expect("accept_put_manifest");

    let events = received_events(&server).await;
    let kinds = kinds_of(&events);
    assert_eq!(
        kinds.len(),
        2,
        "a tag push must deliver exactly two events; got {kinds:?}"
    );
    assert!(
        kinds.contains(&"manifest.push".to_string()),
        "ManifestPush event must be present for tag-based push; got {kinds:?}"
    );
    assert!(
        kinds.contains(&"tag.create".to_string()),
        "TagCreate event must be present for tag-based push; got {kinds:?}"
    );

    let tag_create = events.iter().find(|e| e["kind"] == "tag.create").unwrap();
    assert_eq!(
        tag_create["tag"], "latest",
        "TagCreate event must carry the tag name"
    );
    assert!(
        tag_create["reference"].is_string(),
        "TagCreate event must carry a reference"
    );
}

/// Digest-based manifest push delivers only a `ManifestPush` event; no
/// `TagCreate` event is emitted (suppression on digest references).
#[tokio::test]
async fn digest_push_suppresses_tag_create_event() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    let fixture = FsRegistryFixture::with_webhook(&server.uri(), manifest_and_tag_kinds());
    let namespace = Namespace::new("test-repo").unwrap();
    let (manifest_bytes, mime_type) = test_manifest_bytes(&fixture.registry, &namespace).await;

    // First push with a tag to obtain the digest.
    let tag_response = fixture
        .registry
        .accept_put_manifest(
            None,
            None,
            &namespace,
            Reference::Tag(Tag::new("seed").unwrap()),
            mime_type.clone(),
            Cursor::new(manifest_bytes.clone()),
            Vec::new(),
        )
        .await
        .expect("seed push");
    let seed_event_count = received_events(&server).await.len();

    let digest_str = tag_response
        .headers
        .get("Docker-Content-Digest")
        .cloned()
        .expect("digest header");
    let digest: Digest = digest_str.parse().expect("parse digest");

    // Push the same manifest addressed by its digest.
    fixture
        .registry
        .accept_put_manifest(
            None,
            None,
            &namespace,
            Reference::Digest(digest),
            mime_type,
            Cursor::new(manifest_bytes),
            Vec::new(),
        )
        .await
        .expect("digest push");

    let events = received_events(&server).await.split_off(seed_event_count);
    let kinds = kinds_of(&events);
    assert!(
        !kinds.contains(&"tag.create".to_string()),
        "TagCreate must NOT be emitted for a digest-based push; got {kinds:?}"
    );
    assert_eq!(
        kinds,
        vec!["manifest.push".to_string()],
        "ManifestPush must still be the only event for a digest-based push"
    );
}

/// Tag-based manifest delete delivers both `ManifestDelete` and `TagDelete`
/// events; the `TagDelete` event carries the tag name.
#[tokio::test]
async fn tag_delete_emits_manifest_delete_and_tag_delete_events() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    let fixture = FsRegistryFixture::with_webhook(&server.uri(), manifest_and_tag_kinds());
    let namespace = Namespace::new("test-repo").unwrap();
    let (manifest_bytes, mime_type) = test_manifest_bytes(&fixture.registry, &namespace).await;

    fixture
        .registry
        .accept_put_manifest(
            None,
            None,
            &namespace,
            Reference::Tag(Tag::new("v1").unwrap()),
            mime_type,
            Cursor::new(manifest_bytes),
            Vec::new(),
        )
        .await
        .expect("put manifest");
    let push_event_count = received_events(&server).await.len();

    let reference = Reference::Tag(Tag::new("v1").unwrap());
    fixture
        .registry
        .delete_manifest(None, None, &namespace, &reference)
        .await
        .expect("delete_manifest");

    let events = received_events(&server).await.split_off(push_event_count);
    let kinds = kinds_of(&events);
    assert!(
        kinds.contains(&"manifest.delete".to_string()),
        "ManifestDelete must be emitted for tag-based delete; got {kinds:?}"
    );
    assert!(
        kinds.contains(&"tag.delete".to_string()),
        "TagDelete must be emitted for tag-based delete; got {kinds:?}"
    );

    let tag_delete = events.iter().find(|e| e["kind"] == "tag.delete").unwrap();
    assert_eq!(tag_delete["tag"], "v1", "TagDelete must carry the tag name");
}

/// Digest-based manifest delete delivers only `ManifestDelete`; no `TagDelete`
/// event is emitted (suppression on digest references).
#[tokio::test]
async fn digest_delete_suppresses_tag_delete_event() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    let fixture = FsRegistryFixture::with_webhook(&server.uri(), manifest_and_tag_kinds());
    let namespace = Namespace::new("test-repo").unwrap();
    let (manifest_bytes, mime_type) = test_manifest_bytes(&fixture.registry, &namespace).await;

    let push = fixture
        .registry
        .accept_put_manifest(
            None,
            None,
            &namespace,
            Reference::Tag(Tag::new("to-delete").unwrap()),
            mime_type,
            Cursor::new(manifest_bytes),
            Vec::new(),
        )
        .await
        .expect("put manifest");
    let push_event_count = received_events(&server).await.len();

    let digest_str = push
        .headers
        .get("Docker-Content-Digest")
        .cloned()
        .expect("digest header");
    let digest: Digest = digest_str.parse().expect("parse digest");

    let reference = Reference::Digest(digest);
    fixture
        .registry
        .delete_manifest(None, None, &namespace, &reference)
        .await
        .expect("delete_manifest");

    let events = received_events(&server).await.split_off(push_event_count);
    let kinds = kinds_of(&events);
    assert!(
        !kinds.contains(&"tag.delete".to_string()),
        "TagDelete must NOT be emitted for a digest-based delete; got {kinds:?}"
    );
    assert_eq!(
        kinds,
        vec!["manifest.delete".to_string()],
        "ManifestDelete must still be the only event for a digest-based delete"
    );
}

/// All required fields (`id`, `timestamp`, `repository`, `namespace`, `kind`,
/// `tag`, `reference`, `digest`) are present and valid on the `TagCreate`
/// event produced by a tag-based push.
#[tokio::test]
async fn tag_push_event_payload_has_all_required_fields() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    let fixture = FsRegistryFixture::with_webhook(&server.uri(), manifest_and_tag_kinds());
    let namespace = Namespace::new("test-repo").unwrap();
    let (manifest_bytes, mime_type) = test_manifest_bytes(&fixture.registry, &namespace).await;

    let before = Utc::now();
    fixture
        .registry
        .accept_put_manifest(
            None,
            None,
            &namespace,
            Reference::Tag(Tag::new("stable").unwrap()),
            mime_type,
            Cursor::new(manifest_bytes),
            Vec::new(),
        )
        .await
        .expect("accept_put_manifest");
    let after = Utc::now();

    let events = received_events(&server).await;
    let tag_create = events
        .iter()
        .find(|e| e["kind"] == "tag.create")
        .expect("TagCreate event must exist for tag push");

    let id: Uuid = tag_create["id"]
        .as_str()
        .expect("id must be a string")
        .parse()
        .expect("id must parse as a UUID");
    assert_ne!(id, Uuid::nil(), "Event id must be a non-nil UUID");

    let timestamp: DateTime<Utc> = tag_create["timestamp"]
        .as_str()
        .expect("timestamp must be a string")
        .parse()
        .expect("timestamp must parse");
    assert!(
        timestamp >= before && timestamp <= after,
        "Event timestamp {timestamp} must lie between before ({before}) and after ({after})",
    );

    assert!(
        !tag_create["repository"]
            .as_str()
            .unwrap_or_default()
            .is_empty(),
        "Event repository must not be empty"
    );
    assert_eq!(
        tag_create["namespace"],
        namespace.as_ref(),
        "Event namespace must match the pushed namespace"
    );
    assert_eq!(tag_create["tag"], "stable");
    assert!(
        tag_create["reference"].is_string(),
        "Event reference must be set"
    );
    assert!(
        tag_create["digest"].is_string(),
        "Event digest must be set on TagCreate"
    );
}

/// A by-digest push with `?tag=` parameters emits one `TagCreate` per created
/// tag, each carrying the pushed digest, alongside the `ManifestPush`.
#[tokio::test]
async fn digest_push_with_tag_params_emits_tag_create_per_tag() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    let fixture = FsRegistryFixture::with_webhook(&server.uri(), manifest_and_tag_kinds());
    let namespace = Namespace::new("test-repo").unwrap();
    let (manifest_bytes, mime_type) = test_manifest_bytes(&fixture.registry, &namespace).await;
    let digest = Digest::sha256_of_bytes(&manifest_bytes);

    fixture
        .registry
        .accept_put_manifest(
            None,
            None,
            &namespace,
            Reference::Digest(digest.clone()),
            mime_type,
            Cursor::new(manifest_bytes),
            vec![Tag::new("1.2.3").unwrap(), Tag::new("latest").unwrap()],
        )
        .await
        .expect("by-digest push with tag params must succeed");

    let events = received_events(&server).await;
    let digest_str = digest.to_string();
    assert!(
        events
            .iter()
            .any(|e| e["kind"] == "manifest.push" && e["digest"] == digest_str.as_str()),
        "a by-digest push must emit a ManifestPush event carrying the digest"
    );
    for tag in ["1.2.3", "latest"] {
        let count = events
            .iter()
            .filter(|e| {
                e["kind"] == "tag.create" && e["tag"] == tag && e["digest"] == digest_str.as_str()
            })
            .count();
        assert_eq!(
            count, 1,
            "exactly one TagCreate event carrying the digest must fire for tag '{tag}'"
        );
    }
}

/// A converged replay (same tag, same digest) suppresses replication but must
/// still emit its webhook events.
#[tokio::test]
async fn noop_push_still_emits_events() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    let fixture = FsRegistryFixture::with_webhook(&server.uri(), manifest_and_tag_kinds());
    let namespace = Namespace::new("test-repo").unwrap();
    let (manifest_bytes, mime_type) = test_manifest_bytes(&fixture.registry, &namespace).await;

    fixture
        .registry
        .accept_put_manifest(
            None,
            None,
            &namespace,
            Reference::Tag(Tag::new("latest").unwrap()),
            mime_type.clone(),
            Cursor::new(manifest_bytes.clone()),
            Vec::new(),
        )
        .await
        .expect("seed tag push");
    let seed_event_count = received_events(&server).await.len();

    fixture
        .registry
        .accept_put_manifest(
            None,
            None,
            &namespace,
            Reference::Tag(Tag::new("latest").unwrap()),
            mime_type,
            Cursor::new(manifest_bytes),
            Vec::new(),
        )
        .await
        .expect("re-assert same tag->digest");

    let events = received_events(&server).await.split_off(seed_event_count);
    let kinds = kinds_of(&events);
    assert!(
        kinds.contains(&"manifest.push".to_string()),
        "a no-op push must still emit ManifestPush; got {kinds:?}"
    );
    assert!(
        kinds.contains(&"tag.create".to_string()),
        "a no-op tag push must still emit TagCreate; got {kinds:?}"
    );
}

/// A completed blob upload emits one `blob.push` event carrying the digest.
#[tokio::test]
async fn completed_upload_emits_blob_push_event() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    let fixture = FsRegistryFixture::with_webhook(&server.uri(), vec![EventKind::BlobPush]);
    let namespace = Namespace::new("test-repo").unwrap();

    let digest = upload_blob(&fixture.registry, &namespace, b"pushed bytes").await;

    let events = received_events(&server).await;
    assert_eq!(events.len(), 1, "a completed upload must emit one event");
    assert_eq!(events[0]["kind"], "blob.push");
    assert_eq!(events[0]["digest"], digest.to_string());
}

/// A satisfied cross-repo mount emits `blob.push` so webhook consumers see the
/// mounted blob, just as a completed upload does.
#[tokio::test]
async fn mount_emits_blob_push_event() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    let fixture = FsRegistryFixture::with_webhook(&server.uri(), vec![EventKind::BlobPush]);
    let source = &Namespace::new("test-repo/source").unwrap();
    let target = &Namespace::new("test-repo/target").unwrap();

    let digest = put_blob_direct(fixture.registry.metadata_store.store(), b"mountable").await;
    BlobOwnership::new(fixture.registry.metadata_store.as_ref())
        .grant(source, &digest)
        .await
        .unwrap();

    let mount = BlobMount {
        digest: digest.clone(),
        from: Some(source.clone()),
    };
    let response = fixture
        .registry
        .mount_blob(None, target, &mount, source)
        .await
        .unwrap();

    assert!(matches!(response, StartUploadResponse::ExistingBlob { .. }));
    let events = received_events(&server).await;
    assert_eq!(events.len(), 1, "a satisfied mount must emit one event");
    assert_eq!(events[0]["kind"], "blob.push");
    assert_eq!(events[0]["namespace"], target.as_ref());
    assert_eq!(events[0]["digest"], digest.to_string());
}

/// The `blob.push` intent fires before the mount attempt, so the session
/// fallback still leaves the (false-positive) event behind.
#[tokio::test]
async fn mount_fallback_still_emits_intent_event() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    let fixture = FsRegistryFixture::with_webhook(&server.uri(), vec![EventKind::BlobPush]);
    let source = &Namespace::new("test-repo/source").unwrap();
    let target = &Namespace::new("test-repo/target").unwrap();

    // Present but unowned by `source` -> the mount falls back to a session.
    let digest = put_blob_direct(fixture.registry.metadata_store.store(), b"not owned").await;
    let mount = BlobMount {
        digest: digest.clone(),
        from: Some(source.clone()),
    };
    let response = fixture
        .registry
        .mount_blob(None, target, &mount, source)
        .await
        .unwrap();

    assert!(matches!(response, StartUploadResponse::Session { .. }));
    let events = received_events(&server).await;
    assert_eq!(
        events.len(),
        1,
        "the mount intent must be emitted even when the mount falls back"
    );
    assert_eq!(events[0]["kind"], "blob.push");
    assert_eq!(events[0]["digest"], digest.to_string());
}

/// Intent-first emission gates the action on a `required` webhook: when the
/// delivery fails, the push is rejected before anything is stored.
#[tokio::test]
async fn failing_required_webhook_blocks_the_write() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;
    let fixture = FsRegistryFixture::with_webhook(&server.uri(), manifest_and_tag_kinds());
    let namespace = Namespace::new("test-repo").unwrap();
    let (manifest_bytes, mime_type) = test_manifest_bytes(&fixture.registry, &namespace).await;

    let tag = Tag::new("gated").unwrap();
    let result = fixture
        .registry
        .accept_put_manifest(
            None,
            None,
            &namespace,
            Reference::Tag(tag.clone()),
            mime_type,
            Cursor::new(manifest_bytes),
            Vec::new(),
        )
        .await;
    assert!(
        result.is_err(),
        "a failing required webhook must reject the push"
    );

    fixture
        .registry
        .metadata_store
        .read_link(&namespace, &LinkKind::Tag(tag))
        .await
        .expect_err("the manifest must not have been stored after the rejected intent");
}

// ---------------------------------------------------------------------------
// Replication-dispatch coverage (end-to-end through accept_put_manifest)
// ---------------------------------------------------------------------------

const REPLICATION_REPO: &str = "test-repo";

/// A `Registry` with one `event+reconcile` downstream and a caller-held
/// `JobStore`. No drain is spawned: these tests assert enqueue only.
struct ReplicationFixture {
    registry: Arc<Registry>,
    job_store: Arc<JobStore>,
    _temp_dir: TempDir,
}

impl ReplicationFixture {
    fn new() -> Self {
        Self::with_repository(repository_with_downstream(
            REPLICATION_REPO,
            downstream_client("https://unused.test"),
        ))
    }

    fn with_repository(repository: Repository) -> Self {
        let FsTestStack {
            dir,
            store,
            metadata_store,
            blob_store,
        } = fs_test_stack();
        let resolver = single_repo_resolver(REPLICATION_REPO, repository);

        let job_store: Arc<JobStore> = Arc::new(JobStore::new(store, "test"));

        let config = RegistryConfig {
            job_queue: Some(job_store.clone()),
            ..RegistryConfig::default()
        };
        let registry = Registry::new(blob_store, metadata_store, resolver, config).unwrap();

        Self {
            registry,
            job_store,
            _temp_dir: dir,
        }
    }
}

/// One authoritative `prune = true` mirror plus one additive downstream, both
/// enqueueing on events.
fn mirror_and_additive_downstreams() -> Vec<ReplicationDownstream> {
    let client = downstream_client("https://unused.test");
    vec![
        ReplicationDownstream::builder("mirror".to_string(), client.clone(), 4)
            .mode(ReplicationMode::EventReconcile)
            .namespace_filter(Vec::new())
            .prune(true)
            .build(),
        ReplicationDownstream::builder("additive".to_string(), client, 4)
            .mode(ReplicationMode::EventReconcile)
            .namespace_filter(Vec::new())
            .build(),
    ]
}

#[tokio::test]
async fn fresh_local_tag_push_enqueues_replication_job() {
    crate::metrics_provider::init_for_tests();
    let fixture = ReplicationFixture::new();
    let namespace = Namespace::new(REPLICATION_REPO).unwrap();
    let (manifest_bytes, mime_type) = test_manifest_bytes(&fixture.registry, &namespace).await;

    fixture
        .registry
        .accept_put_manifest(
            None,
            None,
            &namespace,
            Reference::Tag(Tag::new("latest").unwrap()),
            mime_type,
            Cursor::new(manifest_bytes),
            Vec::new(),
        )
        .await
        .expect("accept_put_manifest");

    assert_eq!(
        fixture
            .job_store
            .count_pending(Queue::Replication, 0)
            .await
            .unwrap(),
        1,
        "a fresh local tagged push must enqueue exactly one replication job"
    );

    // Proves the accept_put_manifest -> dispatch_replication -> envelope path
    // threads the right fields.
    let payload = sole_pending_payload(&fixture.job_store).await;
    assert_eq!(payload.downstream, "eu-region");
    assert_eq!(payload.namespace, REPLICATION_REPO);
    assert_eq!(payload.tag.as_deref(), Some("latest"));
    assert_eq!(payload.kind, REPLICATION_PUSH_MANIFEST_KIND);
    assert!(
        payload.source_ts.is_some(),
        "the payload must carry a source_ts for receiver-side LWW"
    );
}

#[tokio::test]
async fn tag_delete_enqueues_replication_delete_job() {
    crate::metrics_provider::init_for_tests();
    let fixture = ReplicationFixture::new();
    let namespace = Namespace::new(REPLICATION_REPO).unwrap();
    let (manifest_bytes, mime_type) = test_manifest_bytes(&fixture.registry, &namespace).await;

    // Seed via the non-dispatching put_manifest so the delete's job is the only one enqueued.
    fixture
        .registry
        .put_manifest(
            &namespace,
            &Reference::Tag(Tag::new("doomed").unwrap()),
            Some(&mime_type),
            &manifest_bytes,
        )
        .await
        .expect("seed put_manifest");
    assert_eq!(
        fixture
            .job_store
            .count_pending(Queue::Replication, 0)
            .await
            .unwrap(),
        0,
        "put_manifest must not enqueue any replication job"
    );

    fixture
        .registry
        .delete_manifest(
            None,
            None,
            &namespace,
            &Reference::Tag(Tag::new("doomed").unwrap()),
        )
        .await
        .expect("delete_manifest");

    assert_eq!(
        fixture
            .job_store
            .count_pending(Queue::Replication, 0)
            .await
            .unwrap(),
        1,
        "a tagged delete must enqueue exactly one replication delete job"
    );

    let payload = sole_pending_payload(&fixture.job_store).await;
    assert_eq!(payload.kind, REPLICATION_DELETE_MANIFEST_KIND);
    assert_eq!(payload.tag.as_deref(), Some("doomed"));
    assert_eq!(payload.namespace, REPLICATION_REPO);
}

/// A retention-initiated delete (internal actor) mirrors only to the
/// `prune = true` downstream; the additive downstream keeps its copy.
#[tokio::test]
async fn retention_delete_mirrors_only_to_prune_downstreams() {
    crate::metrics_provider::init_for_tests();
    let fixture = ReplicationFixture::with_repository(repository_with_replication(
        REPLICATION_REPO,
        mirror_and_additive_downstreams(),
    ));
    let namespace = Namespace::new(REPLICATION_REPO).unwrap();
    let (manifest_bytes, mime_type) = test_manifest_bytes(&fixture.registry, &namespace).await;

    fixture
        .registry
        .put_manifest(
            &namespace,
            &Reference::Tag(Tag::new("expired").unwrap()),
            Some(&mime_type),
            &manifest_bytes,
        )
        .await
        .expect("seed put_manifest");

    fixture
        .registry
        .delete_manifest(
            Some(EventActor::internal("prune")),
            None,
            &namespace,
            &Reference::Tag(Tag::new("expired").unwrap()),
        )
        .await
        .expect("retention delete");

    assert_eq!(
        fixture
            .job_store
            .count_pending(Queue::Replication, 0)
            .await
            .unwrap(),
        1,
        "a retention delete must enqueue for the prune = true downstream only"
    );
    let payload = sole_pending_payload(&fixture.job_store).await;
    assert_eq!(payload.downstream, "mirror");
    assert_eq!(payload.kind, REPLICATION_DELETE_MANIFEST_KIND);
    assert_eq!(payload.tag.as_deref(), Some("expired"));
}

/// A client-initiated delete fans out to every matching downstream.
#[tokio::test]
async fn client_delete_mirrors_to_all_downstreams() {
    crate::metrics_provider::init_for_tests();
    let fixture = ReplicationFixture::with_repository(repository_with_replication(
        REPLICATION_REPO,
        mirror_and_additive_downstreams(),
    ));
    let namespace = Namespace::new(REPLICATION_REPO).unwrap();
    let (manifest_bytes, mime_type) = test_manifest_bytes(&fixture.registry, &namespace).await;

    fixture
        .registry
        .put_manifest(
            &namespace,
            &Reference::Tag(Tag::new("doomed").unwrap()),
            Some(&mime_type),
            &manifest_bytes,
        )
        .await
        .expect("seed put_manifest");

    fixture
        .registry
        .delete_manifest(
            None,
            None,
            &namespace,
            &Reference::Tag(Tag::new("doomed").unwrap()),
        )
        .await
        .expect("client delete");

    assert_eq!(
        fixture
            .job_store
            .count_pending(Queue::Replication, 0)
            .await
            .unwrap(),
        2,
        "a client delete must enqueue for every matching downstream"
    );
}
