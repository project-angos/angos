//! Event emission and its suppression across the manifest and blob operations,
//! plus the replication jobs they enqueue. Tag-specific events fire only for a
//! `Reference::Tag`; emission is observed through a `required`-policy webhook
//! pointed at a wiremock endpoint.

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use http::StatusCode;
use serde_json::{Value, json};
use tempfile::TempDir;
use url::Url;
use uuid::Uuid;
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

use angos_mtls_client::ClientTls;
use angos_oci::request::{
    BlobMount, GetBlobRequest, GetManifestRequest, MountBlobRequest, PutManifestRequest,
};
use angos_oci::{Digest, MediaRange, MediaType, Namespace, Reference, Tag};
use angos_oci_service::ManifestWritten;

use crate::event_webhook::config::{DeliveryPolicy, EventWebhookConfig};
use crate::event_webhook::dispatcher::EventDispatcher;
use crate::event_webhook::event::{EventActor, EventKind};
use crate::jobs::Queue;
use crate::jobs::store::JobStore;
use crate::registry::metadata_store::LinkKind;
use crate::registry::repository_resolver::RepositoryResolver;
use crate::registry::test_utils::{
    FsTestStack, create_test_repositories, downstream_client, fs_test_stack, put_blob_direct,
    put_manifest_direct, repository_with_downstream, repository_with_replication, response_digest,
    single_repo_resolver, sole_pending_payload, test_downstream, test_job_store, upload_blob,
};
use crate::registry::{Error, Registry, RegistryConfig, Repository};
use crate::replication::{
    REPLICATION_DELETE_MANIFEST_KIND, REPLICATION_PUSH_MANIFEST_KIND, ReplicationDownstream,
};

/// A registry wired to a `required`-policy webhook, and the wiremock
/// endpoint it delivers to.
struct FsRegistryFixture {
    registry: Arc<Registry>,
    server: MockServer,
    _temp_dir: TempDir,
}

/// The subscription of most tests here. Blob pushes stay out of it so
/// `upload_blob` seeding does not show up in the delivery counts.
fn manifest_and_tag_kinds() -> Vec<EventKind> {
    vec![
        EventKind::ManifestPush,
        EventKind::ManifestDelete,
        EventKind::TagCreate,
        EventKind::TagDelete,
    ]
}

impl FsRegistryFixture {
    /// A webhook subscribing to `kinds` whose endpoint answers `status`.
    async fn new(status: u16, kinds: Vec<EventKind>) -> Self {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(status))
            .mount(&server)
            .await;
        let webhook = EventWebhookConfig {
            url: Url::parse(&server.uri()).unwrap(),
            policy: DeliveryPolicy::Required,
            token: None,
            timeout_ms: 5_000,
            max_retries: Some(0),
            events: kinds,
            repository_filter: None,
            tls: ClientTls::default(),
        };
        let mut webhooks = HashMap::new();
        webhooks.insert("test-hook".to_string(), webhook);
        let dispatcher =
            EventDispatcher::new(webhooks, vec!["test-hook".to_string()], HashMap::new())
                .expect("dispatcher build");

        let FsTestStack {
            dir,
            metadata_store,
            blob_store,
        } = fs_test_stack();
        let resolver = Arc::new(
            RepositoryResolver::new(create_test_repositories())
                .expect("test repositories must not have overlapping prefixes"),
        );
        let config = RegistryConfig {
            event_dispatcher: Some(Arc::new(dispatcher)),
            ..RegistryConfig::new(test_job_store(&metadata_store))
        };
        let registry = Registry::new(blob_store, metadata_store, resolver, config);

        Self {
            registry,
            server,
            _temp_dir: dir,
        }
    }

    /// The event payloads the endpoint received, in delivery order.
    async fn events(&self) -> Vec<Value> {
        self.server
            .received_requests()
            .await
            .expect("received_requests")
            .iter()
            .map(|request| {
                serde_json::from_slice(&request.body).expect("event payload must be JSON")
            })
            .collect()
    }
}

fn kinds_of(events: &[Value]) -> Vec<String> {
    events
        .iter()
        .map(|e| e["kind"].as_str().unwrap_or_default().to_string())
        .collect()
}

/// Push a minimal OCI manifest by `reference`, also tagging `tags`. The bytes
/// are the same on every call, so a repeated push names the same digest.
async fn push_manifest(
    registry: &Registry,
    namespace: &Namespace,
    reference: Reference,
    tags: Vec<Tag>,
) -> Result<ManifestWritten, Error> {
    let (bytes, media_type) = test_manifest_bytes(registry, namespace).await;
    registry
        .handle_put_manifest(
            None,
            PutManifestRequest {
                namespace: namespace.clone(),
                reference,
                content_type: Some(media_type),
                tags,
                source_ts: None,
            },
            Cursor::new(bytes),
        )
        .await
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

fn tag(name: &str) -> Reference {
    Reference::Tag(Tag::new(name).unwrap())
}

#[tokio::test]
async fn tag_push_emits_manifest_push_and_a_complete_tag_create_event() {
    let fixture = FsRegistryFixture::new(200, manifest_and_tag_kinds()).await;
    let namespace = Namespace::new("test-repo").unwrap();

    let before = Utc::now();
    push_manifest(&fixture.registry, &namespace, tag("stable"), Vec::new())
        .await
        .expect("handle_put_manifest");
    let after = Utc::now();

    let events = fixture.events().await;
    let mut kinds = kinds_of(&events);
    kinds.sort();
    assert_eq!(kinds, ["manifest.push", "tag.create"]);

    let tag_create = events.iter().find(|e| e["kind"] == "tag.create").unwrap();
    let id: Uuid = tag_create["id"]
        .as_str()
        .expect("id must be a string")
        .parse()
        .expect("id must parse as a UUID");
    assert_ne!(id, Uuid::nil());
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
        "no repository in {tag_create}"
    );
    assert_eq!(tag_create["namespace"], namespace.as_ref());
    assert_eq!(tag_create["tag"], "stable");
    assert!(
        tag_create["reference"].is_string(),
        "no reference in {tag_create}"
    );
    assert!(
        tag_create["digest"].is_string(),
        "no digest in {tag_create}"
    );
}

#[tokio::test]
async fn digest_push_suppresses_tag_create_event() {
    let fixture = FsRegistryFixture::new(200, manifest_and_tag_kinds()).await;
    let namespace = Namespace::new("test-repo").unwrap();
    let digest = push_manifest(&fixture.registry, &namespace, tag("seed"), Vec::new())
        .await
        .expect("seed push")
        .digest;
    let seed_event_count = fixture.events().await.len();

    push_manifest(
        &fixture.registry,
        &namespace,
        Reference::Digest(digest),
        Vec::new(),
    )
    .await
    .expect("digest push");

    let events = fixture.events().await.split_off(seed_event_count);
    assert_eq!(
        kinds_of(&events),
        ["manifest.push"],
        "a digest push must emit ManifestPush alone"
    );
}

#[tokio::test]
async fn tag_delete_emits_manifest_delete_and_tag_delete_events() {
    let fixture = FsRegistryFixture::new(200, manifest_and_tag_kinds()).await;
    let namespace = Namespace::new("test-repo").unwrap();
    push_manifest(&fixture.registry, &namespace, tag("v1"), Vec::new())
        .await
        .expect("put manifest");
    let push_event_count = fixture.events().await.len();

    fixture
        .registry
        .remove_manifest(None, None, &namespace, &tag("v1"))
        .await
        .expect("delete_manifest");

    let events = fixture.events().await.split_off(push_event_count);
    let kinds = kinds_of(&events);
    assert!(
        kinds.contains(&"manifest.delete".to_string()),
        "ManifestDelete must be emitted for tag-based delete; got {kinds:?}"
    );
    let tag_delete = events
        .iter()
        .find(|e| e["kind"] == "tag.delete")
        .unwrap_or_else(|| panic!("TagDelete must be emitted for tag-based delete; got {kinds:?}"));
    assert_eq!(tag_delete["tag"], "v1", "TagDelete must carry the tag name");
}

#[tokio::test]
async fn digest_delete_suppresses_tag_delete_event() {
    let fixture = FsRegistryFixture::new(200, manifest_and_tag_kinds()).await;
    let namespace = Namespace::new("test-repo").unwrap();
    let digest = push_manifest(&fixture.registry, &namespace, tag("to-delete"), Vec::new())
        .await
        .expect("put manifest")
        .digest;
    let push_event_count = fixture.events().await.len();

    fixture
        .registry
        .remove_manifest(None, None, &namespace, &Reference::Digest(digest))
        .await
        .expect("delete_manifest");

    let events = fixture.events().await.split_off(push_event_count);
    assert_eq!(
        kinds_of(&events),
        ["manifest.delete"],
        "a digest delete must emit ManifestDelete alone"
    );
}

#[tokio::test]
async fn digest_push_with_tag_params_emits_tag_create_per_tag() {
    let fixture = FsRegistryFixture::new(200, manifest_and_tag_kinds()).await;
    let namespace = Namespace::new("test-repo").unwrap();
    let (manifest_bytes, _) = test_manifest_bytes(&fixture.registry, &namespace).await;
    let digest = Digest::sha256_of_bytes(&manifest_bytes);

    push_manifest(
        &fixture.registry,
        &namespace,
        Reference::Digest(digest.clone()),
        vec![Tag::new("1.2.3").unwrap(), Tag::new("latest").unwrap()],
    )
    .await
    .expect("by-digest push with tag params must succeed");

    let events = fixture.events().await;
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
    let fixture = FsRegistryFixture::new(200, manifest_and_tag_kinds()).await;
    let namespace = Namespace::new("test-repo").unwrap();
    push_manifest(&fixture.registry, &namespace, tag("latest"), Vec::new())
        .await
        .expect("seed tag push");
    let seed_event_count = fixture.events().await.len();

    push_manifest(&fixture.registry, &namespace, tag("latest"), Vec::new())
        .await
        .expect("re-assert same tag->digest");

    let mut kinds = kinds_of(&fixture.events().await.split_off(seed_event_count));
    kinds.sort();
    assert_eq!(
        kinds,
        ["manifest.push", "tag.create"],
        "a no-op tag push must still emit its events"
    );
}

#[tokio::test]
async fn completed_upload_emits_blob_push_event() {
    let fixture = FsRegistryFixture::new(200, vec![EventKind::BlobPush]).await;
    let namespace = Namespace::new("test-repo").unwrap();

    let digest = upload_blob(&fixture.registry, &namespace, b"pushed bytes").await;

    let events = fixture.events().await;
    assert_eq!(events.len(), 1, "a completed upload must emit one event");
    assert_eq!(events[0]["kind"], "blob.push");
    assert_eq!(events[0]["digest"], digest.to_string());
}

/// A cross-repo mount emits `blob.push` just as an upload does, and since the
/// intent fires before the attempt, a mount falling back to a session still
/// leaves its (false-positive) event behind.
#[tokio::test]
async fn mount_emits_blob_push_event_even_when_it_falls_back() {
    let fixture = FsRegistryFixture::new(200, vec![EventKind::BlobPush]).await;
    let source = &Namespace::new("test-repo/source").unwrap();
    let target = &Namespace::new("test-repo/target").unwrap();

    let owned = put_blob_direct(&fixture.registry.blob_store, b"mountable").await;
    fixture
        .registry
        .metadata_store
        .as_ref()
        .grant(source, &owned)
        .await
        .unwrap();
    // Present but unowned by `source`, so its mount falls back to a session.
    let unowned = put_blob_direct(&fixture.registry.blob_store, b"not owned").await;

    for (digest, status) in [
        (&owned, StatusCode::CREATED),
        (&unowned, StatusCode::ACCEPTED),
    ] {
        let response = fixture
            .registry
            .handle_mount_blob(
                None,
                MountBlobRequest {
                    namespace: target.clone(),
                    mount: BlobMount {
                        digest: digest.clone(),
                        from: Some(source.clone()),
                    },
                },
                Some(source.clone()),
            )
            .await
            .unwrap()
            .into_response()
            .unwrap();
        assert_eq!(response.status(), status);
    }

    let events = fixture.events().await;
    assert_eq!(events.len(), 2, "each mount must emit one event");
    for (event, digest) in events.iter().zip([&owned, &unowned]) {
        assert_eq!(event["kind"], "blob.push");
        assert_eq!(event["namespace"], target.as_ref());
        assert_eq!(event["digest"], digest.to_string());
    }
}

/// Intent-first emission gates the action on a `required` webhook: when the
/// delivery fails, the push is rejected before anything is stored.
#[tokio::test]
async fn failing_required_webhook_blocks_the_write() {
    let fixture = FsRegistryFixture::new(500, manifest_and_tag_kinds()).await;
    let namespace = Namespace::new("test-repo").unwrap();

    let result = push_manifest(&fixture.registry, &namespace, tag("gated"), Vec::new()).await;
    assert!(
        result.is_err(),
        "a failing required webhook must reject the push"
    );

    fixture
        .registry
        .metadata_store
        .read_link(&namespace, &LinkKind::Tag(Tag::new("gated").unwrap()))
        .await
        .expect_err("the manifest must not have been stored after the rejected intent");
}

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
            metadata_store,
            blob_store,
        } = fs_test_stack();
        let resolver = single_repo_resolver(REPLICATION_REPO, repository);

        let job_store = test_job_store(&metadata_store);

        let config = RegistryConfig::new(job_store.clone());
        let registry = Registry::new(blob_store, metadata_store, resolver, config);

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
        ReplicationDownstream {
            prune: true,
            ..test_downstream("mirror".to_string(), client.clone())
        },
        test_downstream("additive".to_string(), client),
    ]
}

#[tokio::test]
async fn fresh_local_tag_push_enqueues_replication_job() {
    let fixture = ReplicationFixture::new();
    let namespace = Namespace::new(REPLICATION_REPO).unwrap();
    push_manifest(&fixture.registry, &namespace, tag("latest"), Vec::new())
        .await
        .expect("handle_put_manifest");

    assert_eq!(
        fixture
            .job_store
            .count_pending(Queue::Replication, 0)
            .await
            .unwrap(),
        1,
        "a fresh local tagged push must enqueue exactly one replication job"
    );

    let payload = sole_pending_payload(&fixture.job_store).await;
    let target = payload.target();
    assert_eq!(target.downstream, "eu-region");
    assert_eq!(target.namespace, REPLICATION_REPO);
    assert_eq!(target.tag.as_deref(), Some("latest"));
    assert_eq!(payload.kind(), REPLICATION_PUSH_MANIFEST_KIND);
    assert!(
        target.source_ts.is_some(),
        "the payload must carry a source_ts for receiver-side LWW"
    );
}

#[tokio::test]
async fn tag_delete_enqueues_replication_delete_job() {
    let fixture = ReplicationFixture::new();
    let namespace = Namespace::new(REPLICATION_REPO).unwrap();
    let (manifest_bytes, mime_type) = test_manifest_bytes(&fixture.registry, &namespace).await;

    // Seed through the non-dispatching put_manifest so the delete's job is the
    // only one enqueued.
    put_manifest_direct(
        &fixture.registry,
        &namespace,
        &tag("doomed"),
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
        .remove_manifest(None, None, &namespace, &tag("doomed"))
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
    assert_eq!(payload.kind(), REPLICATION_DELETE_MANIFEST_KIND);
    assert_eq!(payload.target().tag.as_deref(), Some("doomed"));
    assert_eq!(payload.target().namespace, REPLICATION_REPO);
}

/// A retention-initiated delete (internal actor) mirrors only to the
/// `prune = true` downstream; the additive downstream keeps its copy.
#[tokio::test]
async fn retention_delete_mirrors_only_to_prune_downstreams() {
    let fixture = ReplicationFixture::with_repository(repository_with_replication(
        REPLICATION_REPO,
        mirror_and_additive_downstreams(),
    ));
    let namespace = Namespace::new(REPLICATION_REPO).unwrap();
    let (manifest_bytes, mime_type) = test_manifest_bytes(&fixture.registry, &namespace).await;

    put_manifest_direct(
        &fixture.registry,
        &namespace,
        &tag("expired"),
        Some(&mime_type),
        &manifest_bytes,
    )
    .await
    .expect("seed put_manifest");

    fixture
        .registry
        .remove_manifest(
            Some(EventActor::internal("prune")),
            None,
            &namespace,
            &tag("expired"),
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
    assert_eq!(payload.target().downstream, "mirror");
    assert_eq!(payload.kind(), REPLICATION_DELETE_MANIFEST_KIND);
    assert_eq!(payload.target().tag.as_deref(), Some("expired"));
}

#[tokio::test]
async fn client_delete_mirrors_to_all_downstreams() {
    let fixture = ReplicationFixture::with_repository(repository_with_replication(
        REPLICATION_REPO,
        mirror_and_additive_downstreams(),
    ));
    let namespace = Namespace::new(REPLICATION_REPO).unwrap();
    let (manifest_bytes, mime_type) = test_manifest_bytes(&fixture.registry, &namespace).await;

    put_manifest_direct(
        &fixture.registry,
        &namespace,
        &tag("doomed"),
        Some(&mime_type),
        &manifest_bytes,
    )
    .await
    .expect("seed put_manifest");

    fixture
        .registry
        .remove_manifest(None, None, &namespace, &tag("doomed"))
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

#[tokio::test]
async fn get_blob_emits_pull_event() {
    let fixture = FsRegistryFixture::new(200, vec![EventKind::BlobPull]).await;
    let namespace = Namespace::new("test-repo/pulled").unwrap();
    let digest = upload_blob(&fixture.registry, &namespace, b"pull event blob").await;

    let response = fixture
        .registry
        .handle_get_blob(
            None,
            GetBlobRequest {
                namespace: namespace.clone(),
                digest: digest.clone(),
                accepted_types: Vec::new(),
                range: None,
            },
            true,
        )
        .await
        .expect("the pull must succeed")
        .into_response(fixture.registry.blob_stream_frame_size())
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let events = fixture.events().await;
    assert_eq!(events.len(), 1, "exactly one pull event must be posted");
    assert_eq!(events[0]["kind"], "blob.pull");
    assert_eq!(events[0]["digest"], digest.to_string());
}

#[tokio::test]
async fn get_manifest_emits_pull_event() {
    let fixture = FsRegistryFixture::new(200, vec![EventKind::ManifestPull]).await;
    let namespace = Namespace::new("test-repo/pulled").unwrap();
    let media_type = MediaType::oci_manifest();
    let manifest_bytes =
        br#"{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json"}"#.to_vec();

    let digest = fixture
        .registry
        .handle_put_manifest(
            None,
            PutManifestRequest {
                namespace: namespace.clone(),
                reference: tag("latest"),
                content_type: Some(media_type.clone()),
                tags: Vec::new(),
                source_ts: None,
            },
            Cursor::new(manifest_bytes),
        )
        .await
        .expect("seeding the manifest must succeed")
        .digest;

    let response = fixture
        .registry
        .handle_get_manifest(
            None,
            GetManifestRequest {
                namespace: namespace.clone(),
                reference: tag("latest"),
                accepted_types: vec![MediaRange::from(media_type)],
            },
            true,
        )
        .await
        .expect("the pull must succeed")
        .into_response()
        .unwrap();

    let events = fixture.events().await;
    assert_eq!(events.len(), 1, "exactly one pull event must be posted");
    assert_eq!(events[0]["kind"], "manifest.pull");
    assert_eq!(events[0]["reference"], "latest");
    assert_eq!(events[0]["tag"], "latest");
    assert_eq!(events[0]["digest"], digest.to_string());
    assert_eq!(response_digest(&response), digest);
}
