use std::{collections::HashMap, sync::Arc};

use serde_json::json;
use tempfile::TempDir;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{header, method, path},
};

use angos_storage::{ObjectStore, fs::Backend as StorageFsBackend};

use angos_tx_engine::store::Store;

use crate::{
    cache, metrics_provider,
    oci::Digest,
    registry::{
        DOCKER_CONTENT_DIGEST, Repository,
        blob_store::BlobStore,
        job_store::{JobEnvelope, JobHandler, Queue},
        metadata_store::{LinkKind, LinkOperation, MetadataStore},
        repository_resolver::RepositoryResolver,
        test_utils::{
            build_store, build_test_fs_executor, downstream_client, put_blob_direct,
            repository_with_replication, seed_manifest,
        },
    },
    registry_client::RegistryClient,
    replication::{
        REPLICATION_DELETE_MANIFEST_KIND, REPLICATION_PUSH_MANIFEST_KIND,
        REPLICATION_SUPERSEDED_CODE, ReplicationDownstream, X_ANGOS_SOURCE_TIMESTAMP,
        handler::{
            ReplicationJobHandler, ReplicationPushPayload, build_envelope,
            build_prune_delete_envelope, replication_lock_key,
        },
    },
};

const NAMESPACE: &str = "nginx";
const REPO: &str = "nginx";
const DOWNSTREAM: &str = "eu-region";

fn sample_payload() -> ReplicationPushPayload {
    ReplicationPushPayload {
        downstream: DOWNSTREAM.to_string(),
        namespace: NAMESPACE.to_string(),
        tag: Some("v1".to_string()),
        digest: Some(
            "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef".to_string(),
        ),
        kind: REPLICATION_PUSH_MANIFEST_KIND.to_string(),
        source_ts: Some("2026-06-03T00:00:00Z".to_string()),
    }
}

/// Current value of `angos_replication_push_total{downstream, outcome}`.
/// The prometheus registry is process-global, so tests assert deltas, not
/// absolutes.
fn push_total(downstream: &str, outcome: &str) -> u64 {
    metrics_provider::metrics_provider()
        .replication_push_total
        .with_label_values(&[downstream, outcome])
        .get()
}

/// Current value of `angos_replication_last_success_timestamp_seconds{downstream}`.
fn last_success(downstream: &str) -> i64 {
    metrics_provider::metrics_provider()
        .replication_last_success_timestamp
        .with_label_values(&[downstream])
        .get()
}

#[test]
fn lock_key_uses_tag_when_set() {
    let payload = sample_payload();
    assert_eq!(
        replication_lock_key(&payload),
        "replication.push.eu-region:nginx:v1"
    );
}

#[test]
fn lock_key_falls_back_to_digest_without_tag() {
    let mut payload = sample_payload();
    payload.tag = None;
    assert_eq!(
        replication_lock_key(&payload),
        format!(
            "replication.push.eu-region:nginx:{}",
            payload.digest.as_deref().unwrap()
        )
    );
}

/// A delete must never coalesce into (and be swallowed by) a pending push.
#[test]
fn lock_key_distinguishes_push_from_delete() {
    let push = sample_payload();
    let mut delete = sample_payload();
    delete.kind = REPLICATION_DELETE_MANIFEST_KIND.to_string();
    assert_eq!(
        replication_lock_key(&push),
        "replication.push.eu-region:nginx:v1"
    );
    assert_eq!(
        replication_lock_key(&delete),
        "replication.delete.eu-region:nginx:v1@2026-06-03T00:00:00Z"
    );
    assert_ne!(
        replication_lock_key(&push),
        replication_lock_key(&delete),
        "a push and a delete for the same tag must not coalesce"
    );
}

/// Deletes with different `source_ts` are distinct events and must not
/// coalesce (the stale timestamp would lose receiver-side LWW), while
/// retries of the same event still do.
#[test]
fn lock_key_separates_distinct_delete_events() {
    let mut first = sample_payload();
    first.kind = REPLICATION_DELETE_MANIFEST_KIND.to_string();
    let mut second = first.clone();
    second.source_ts = Some("2026-06-03T00:01:00Z".to_string());

    assert_ne!(
        replication_lock_key(&first),
        replication_lock_key(&second),
        "deletes with different source_ts must each get their own job"
    );
    assert_eq!(
        replication_lock_key(&first),
        replication_lock_key(&first.clone()),
        "a retry of the same deletion event must still coalesce"
    );
}

/// A scrub prune delete keys on the bare reference: repeated reconcile runs
/// stamp fresh `source_ts` values, yet must coalesce into one pending job,
/// and must never coalesce with a timestamped event-path delete.
#[test]
fn prune_delete_envelope_coalesces_on_bare_reference() {
    let mut payload = sample_payload();
    payload.kind = REPLICATION_DELETE_MANIFEST_KIND.to_string();
    let mut later = payload.clone();
    later.source_ts = Some("2026-06-03T00:01:00Z".to_string());

    let first = build_prune_delete_envelope(&payload).unwrap();
    let second = build_prune_delete_envelope(&later).unwrap();
    assert_eq!(first.lock_key, "replication.delete.eu-region:nginx:v1");
    assert_eq!(
        first.lock_key, second.lock_key,
        "prune deletes with different source_ts must coalesce on the bare reference"
    );
    assert_ne!(
        first.lock_key,
        replication_lock_key(&payload),
        "a prune delete must not coalesce with an event-path delete"
    );
}

#[test]
fn lock_key_handles_missing_tag_and_digest() {
    let mut payload = sample_payload();
    payload.tag = None;
    payload.digest = None;
    assert_eq!(
        replication_lock_key(&payload),
        "replication.push.eu-region:nginx:"
    );
}

#[test]
fn build_envelope_sets_queue_kind_and_lock_key() {
    let payload = sample_payload();
    let envelope = build_envelope(&payload).unwrap();
    assert_eq!(envelope.queue, Queue::Replication);
    assert_eq!(envelope.kind, REPLICATION_PUSH_MANIFEST_KIND);
    assert_eq!(envelope.lock_key, "replication.push.eu-region:nginx:v1");
    let round_trip: ReplicationPushPayload = serde_json::from_value(envelope.payload).unwrap();
    assert_eq!(round_trip, payload);
}

fn repository_with_downstream(client: Arc<RegistryClient>) -> Repository {
    repository_with_named_downstream(DOWNSTREAM, client)
}

/// Lets a test pick the downstream name so its metric label set is isolated
/// from other tests.
fn repository_with_named_downstream(name: &str, client: Arc<RegistryClient>) -> Repository {
    repository_with_replication(
        REPO,
        vec![ReplicationDownstream::builder(name.to_string(), client, 4).build()],
    )
}

async fn put_blob(store: &Arc<Store>, content: &[u8]) -> Digest {
    put_blob_direct(store, content).await
}

#[tokio::test]
async fn execute_rejects_unknown_kind() {
    metrics_provider::init_for_tests();
    let dir = TempDir::new().unwrap();
    let root = dir.path().to_str().unwrap();
    let object: Arc<dyn ObjectStore> = Arc::new(StorageFsBackend::builder(root).build());
    let executor = build_test_fs_executor(root, false);
    let store = build_store(object, executor);
    let metadata_store = Arc::new(
        MetadataStore::builder(store.clone())
            .link_cache_ttl(0)
            .access_time_debounce_secs(0)
            .build(),
    );
    let blob_store = Arc::new(BlobStore::new(store.clone()));

    let mut repositories = HashMap::new();
    repositories.insert(
        REPO.to_string(),
        repository_with_downstream(downstream_client("https://unused.test")),
    );
    let resolver = Arc::new(RepositoryResolver::new(Arc::new(repositories)).unwrap());

    let handler = ReplicationJobHandler::new(resolver, blob_store, metadata_store);

    let envelope = JobEnvelope::new(
        Queue::Replication,
        "replication.bogus",
        "lock",
        &sample_payload(),
    )
    .unwrap();
    let result = handler.execute(&envelope).await;
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("unsupported job kind")
    );
}

/// A job for a downstream no longer in config fails loudly (and so
/// dead-letters after max attempts) instead of silently completing.
#[tokio::test]
async fn execute_errors_on_removed_downstream() {
    metrics_provider::init_for_tests();
    let dir = TempDir::new().unwrap();
    let root = dir.path().to_str().unwrap();
    let object: Arc<dyn ObjectStore> = Arc::new(StorageFsBackend::builder(root).build());
    let executor = build_test_fs_executor(root, false);
    let store = build_store(object, executor);
    let metadata_store = Arc::new(
        MetadataStore::builder(store.clone())
            .link_cache_ttl(0)
            .access_time_debounce_secs(0)
            .build(),
    );
    let blob_store = Arc::new(BlobStore::new(store.clone()));

    let mut repositories = HashMap::new();
    repositories.insert(
        REPO.to_string(),
        repository_with_downstream(downstream_client("https://unused.test")),
    );
    let resolver = Arc::new(RepositoryResolver::new(Arc::new(repositories)).unwrap());

    let handler = ReplicationJobHandler::new(resolver, blob_store, metadata_store);

    let mut payload = sample_payload();
    payload.downstream = "removed-region".to_string();
    let envelope = build_envelope(&payload).unwrap();
    let result = handler.execute(&envelope).await;
    assert!(
        result
            .as_ref()
            .is_err_and(|e| e.to_string().contains("no downstream 'removed-region'")),
        "a job for a de-configured downstream must error, got: {:?}",
        result.map(|_| ())
    );
}

#[tokio::test]
async fn execute_pushes_manifest_with_head_before_put() {
    metrics_provider::init_for_tests();
    let mock_server = MockServer::start().await;

    let dir = TempDir::new().unwrap();
    let root = dir.path().to_str().unwrap();
    let object: Arc<dyn ObjectStore> = Arc::new(StorageFsBackend::builder(root).build());
    let executor = build_test_fs_executor(root, false);
    let store = build_store(object, executor);
    let metadata_store = Arc::new(
        MetadataStore::builder(store.clone())
            .link_cache_ttl(0)
            .access_time_debounce_secs(0)
            .build(),
    );
    let blob_store = Arc::new(BlobStore::new(store.clone()));

    let (manifest_digest, config_digest, layer_digest) =
        seed_manifest(&store, &metadata_store, NAMESPACE).await;

    // Downstream is missing both blobs (404 on HEAD) -> upload sequence runs.
    for blob in [&config_digest, &layer_digest] {
        Mock::given(method("HEAD"))
            .and(path(format!("/v2/{NAMESPACE}/blobs/{blob}")))
            .respond_with(ResponseTemplate::new(404))
            .expect(1)
            .mount(&mock_server)
            .await;
    }
    // Each missing blob: POST start, PATCH chunk, PUT complete.
    Mock::given(method("POST"))
        .and(path(format!("/v2/{NAMESPACE}/blobs/uploads/")))
        .respond_with(
            ResponseTemplate::new(202)
                .insert_header("Location", format!("/v2/{NAMESPACE}/blobs/uploads/s1")),
        )
        .expect(2)
        .mount(&mock_server)
        .await;
    Mock::given(method("PATCH"))
        .and(path(format!("/v2/{NAMESPACE}/blobs/uploads/s1")))
        .respond_with(
            ResponseTemplate::new(202)
                .insert_header("Location", format!("/v2/{NAMESPACE}/blobs/uploads/s1")),
        )
        .expect(2)
        .mount(&mock_server)
        .await;
    Mock::given(method("PUT"))
        .and(path(format!("/v2/{NAMESPACE}/blobs/uploads/s1")))
        .respond_with(ResponseTemplate::new(201))
        .expect(2)
        .mount(&mock_server)
        .await;
    // The manifest itself is PUT by tag (no OCI-Subject -> no fallback).
    Mock::given(method("PUT"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
        .respond_with(
            ResponseTemplate::new(201)
                .insert_header(DOCKER_CONTENT_DIGEST, manifest_digest.to_string().as_str()),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let mut repositories = HashMap::new();
    repositories.insert(
        REPO.to_string(),
        repository_with_downstream(downstream_client(&mock_server.uri())),
    );
    let resolver = Arc::new(RepositoryResolver::new(Arc::new(repositories)).unwrap());

    let handler = ReplicationJobHandler::new(resolver, blob_store, metadata_store);

    let envelope = build_envelope(&sample_payload()).unwrap();
    let tx = handler.execute(&envelope).await.unwrap();
    assert!(tx.mutations.is_empty(), "push returns an empty transaction");
    // wiremock `.expect(...)` assertions are verified on MockServer drop.
    drop(mock_server);
}

/// The execute-time tag resolve must read the backend link, not the
/// per-process cache: a worker's cache can lag a sibling process's write
/// by up to its TTL, and a stale resolve would replicate the old digest
/// and complete the job.
#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn execute_push_resolves_tag_past_the_link_cache() {
    metrics_provider::init_for_tests();
    let mock_server = MockServer::start().await;

    let dir = TempDir::new().unwrap();
    let root = dir.path().to_str().unwrap();
    let object: Arc<dyn ObjectStore> = Arc::new(StorageFsBackend::builder(root).build());
    let executor = build_test_fs_executor(root, false);
    let store = build_store(object, executor);
    let metadata_store = Arc::new(
        MetadataStore::builder(store.clone())
            .cache(cache::Config::Memory.to_backend().unwrap())
            .link_cache_ttl(300)
            .access_time_debounce_secs(0)
            .build(),
    );
    let blob_store = Arc::new(BlobStore::new(store.clone()));

    // Two manifests sharing the same blobs; the tag starts on `stale`.
    let config_bytes = br#"{"config":true}"#.to_vec();
    let layer_bytes = b"layer-bytes".to_vec();
    let config_digest = put_blob(&store, &config_bytes).await;
    let layer_digest = put_blob(&store, &layer_bytes).await;
    let manifest_json = |rev: &str| {
        json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "digest": config_digest.to_string(),
                "size": config_bytes.len(),
            },
            "layers": [{
                "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                "digest": layer_digest.to_string(),
                "size": layer_bytes.len(),
            }],
            "annotations": {"rev": rev},
        })
    };
    let stale_bytes = serde_json::to_vec(&manifest_json("stale")).unwrap();
    let stale_digest = put_blob(&store, &stale_bytes).await;
    let current_bytes = serde_json::to_vec(&manifest_json("current")).unwrap();
    let current_digest = put_blob(&store, &current_bytes).await;

    let link = LinkKind::Tag("v1".to_string());
    metadata_store
        .update_links(
            NAMESPACE,
            &[
                LinkOperation::create(link.clone(), stale_digest.clone()),
                LinkOperation::create(
                    LinkKind::Config(config_digest.clone()),
                    config_digest.clone(),
                ),
                LinkOperation::create(LinkKind::Layer(layer_digest.clone()), layer_digest.clone()),
            ],
        )
        .await
        .unwrap();

    // Warm this process's cache with the stale target, then simulate a
    // sibling process re-pointing the tag behind it.
    metadata_store.read_link(NAMESPACE, &link).await.unwrap();
    assert_eq!(
        metadata_store
            .cache_get(NAMESPACE, &link)
            .await
            .expect("the resolve under test must start from a warm cache")
            .target,
        stale_digest
    );
    let mut sibling = metadata_store
        .read_link_reference(NAMESPACE, &link)
        .await
        .unwrap();
    sibling.target = current_digest.clone();
    metadata_store
        .write_link_reference(NAMESPACE, &link, &sibling)
        .await
        .unwrap();

    // Both blobs already present downstream; unmatched manifest HEAD 404s
    // so the converged skip never fires and the PUT body is observable.
    for blob in [&config_digest, &layer_digest] {
        Mock::given(method("HEAD"))
            .and(path(format!("/v2/{NAMESPACE}/blobs/{blob}")))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header(DOCKER_CONTENT_DIGEST, blob.to_string().as_str())
                    .insert_header("Content-Length", "10"),
            )
            .mount(&mock_server)
            .await;
    }
    Mock::given(method("PUT"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
        .respond_with(
            ResponseTemplate::new(201)
                .insert_header(DOCKER_CONTENT_DIGEST, current_digest.to_string().as_str()),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let mut repositories = HashMap::new();
    repositories.insert(
        REPO.to_string(),
        repository_with_downstream(downstream_client(&mock_server.uri())),
    );
    let resolver = Arc::new(RepositoryResolver::new(Arc::new(repositories)).unwrap());

    let handler = ReplicationJobHandler::new(resolver, blob_store, metadata_store);

    let payload = ReplicationPushPayload {
        downstream: DOWNSTREAM.to_string(),
        namespace: NAMESPACE.to_string(),
        tag: Some("v1".to_string()),
        digest: Some(stale_digest.to_string()),
        kind: REPLICATION_PUSH_MANIFEST_KIND.to_string(),
        source_ts: Some("2026-06-03T00:00:00Z".to_string()),
    };
    let envelope = build_envelope(&payload).unwrap();
    handler.execute(&envelope).await.unwrap();

    let manifest_path = format!("/v2/{NAMESPACE}/manifests/v1");
    let received = mock_server.received_requests().await.unwrap_or_default();
    let put = received
        .iter()
        .find(|r| r.method.as_str() == "PUT" && r.url.path() == manifest_path)
        .expect("the push must PUT the manifest");
    assert_eq!(
        put.body, current_bytes,
        "the resolve must read the backend link, not the stale cached one"
    );
    drop(mock_server);
}

#[tokio::test]
async fn execute_skips_blob_present_on_downstream() {
    metrics_provider::init_for_tests();
    let mock_server = MockServer::start().await;

    let dir = TempDir::new().unwrap();
    let root = dir.path().to_str().unwrap();
    let object: Arc<dyn ObjectStore> = Arc::new(StorageFsBackend::builder(root).build());
    let executor = build_test_fs_executor(root, false);
    let store = build_store(object, executor);
    let metadata_store = Arc::new(
        MetadataStore::builder(store.clone())
            .link_cache_ttl(0)
            .access_time_debounce_secs(0)
            .build(),
    );
    let blob_store = Arc::new(BlobStore::new(store.clone()));

    let (manifest_digest, config_digest, layer_digest) =
        seed_manifest(&store, &metadata_store, NAMESPACE).await;

    // Both blobs already present (200 on HEAD) -> NO upload sequence at all.
    for blob in [&config_digest, &layer_digest] {
        Mock::given(method("HEAD"))
            .and(path(format!("/v2/{NAMESPACE}/blobs/{blob}")))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header(DOCKER_CONTENT_DIGEST, blob.to_string().as_str())
                    .insert_header("Content-Length", "10"),
            )
            .mount(&mock_server)
            .await;
    }
    // No POST/PATCH mounted: if the pipeline tried to upload, the request
    // would 404 and the push would error.
    Mock::given(method("PUT"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
        .respond_with(
            ResponseTemplate::new(201)
                .insert_header(DOCKER_CONTENT_DIGEST, manifest_digest.to_string().as_str()),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let mut repositories = HashMap::new();
    repositories.insert(
        REPO.to_string(),
        repository_with_downstream(downstream_client(&mock_server.uri())),
    );
    let resolver = Arc::new(RepositoryResolver::new(Arc::new(repositories)).unwrap());

    let handler = ReplicationJobHandler::new(resolver, blob_store, metadata_store);

    let envelope = build_envelope(&sample_payload()).unwrap();
    handler.execute(&envelope).await.unwrap();
}

/// The manifest PUT must carry an `X-Angos-Source-Timestamp` derived from
/// the resolved tag's `created_at`, not the stale payload timestamp, so a
/// coalesced push cannot ship a stale last-writer-wins version.
#[tokio::test]
async fn execute_push_stamps_resolved_source_timestamp() {
    metrics_provider::init_for_tests();
    let mock_server = MockServer::start().await;

    let dir = TempDir::new().unwrap();
    let root = dir.path().to_str().unwrap();
    let object: Arc<dyn ObjectStore> = Arc::new(StorageFsBackend::builder(root).build());
    let executor = build_test_fs_executor(root, false);
    let store = build_store(object, executor);
    let metadata_store = Arc::new(
        MetadataStore::builder(store.clone())
            .link_cache_ttl(0)
            .access_time_debounce_secs(0)
            .build(),
    );
    let blob_store = Arc::new(BlobStore::new(store.clone()));

    let (manifest_digest, config_digest, layer_digest) =
        seed_manifest(&store, &metadata_store, NAMESPACE).await;

    // Read back the tag's created_at to assert the exact stamped value.
    let expected_ts = metadata_store
        .read_link(NAMESPACE, &LinkKind::Tag("v1".to_string()))
        .await
        .unwrap()
        .created_at
        .unwrap()
        .to_rfc3339();

    // Blobs already present (200 on HEAD): the only mutating request is the
    // manifest PUT.
    for blob in [&config_digest, &layer_digest] {
        Mock::given(method("HEAD"))
            .and(path(format!("/v2/{NAMESPACE}/blobs/{blob}")))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header(DOCKER_CONTENT_DIGEST, blob.to_string().as_str())
                    .insert_header("Content-Length", "10"),
            )
            .mount(&mock_server)
            .await;
    }
    // The PUT must carry the source timestamp; if the header is absent or
    // wrong, this mock does not match and the push fails.
    Mock::given(method("PUT"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
        .and(header(X_ANGOS_SOURCE_TIMESTAMP, expected_ts.as_str()))
        .respond_with(
            ResponseTemplate::new(201)
                .insert_header(DOCKER_CONTENT_DIGEST, manifest_digest.to_string().as_str()),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let mut repositories = HashMap::new();
    repositories.insert(
        REPO.to_string(),
        repository_with_downstream(downstream_client(&mock_server.uri())),
    );
    let resolver = Arc::new(RepositoryResolver::new(Arc::new(repositories)).unwrap());

    let handler = ReplicationJobHandler::new(resolver, blob_store, metadata_store);

    let envelope = build_envelope(&sample_payload()).unwrap();
    handler.execute(&envelope).await.unwrap();
    // wiremock `.expect(1)` on the header-matched PUT is verified on drop.
    drop(mock_server);
}

/// A reconcile push enqueues with `source_ts = None`; the handler must still
/// stamp the resolved tag's `created_at` so the receiver runs
/// last-writer-wins instead of overwriting unconditionally.
#[tokio::test]
async fn execute_reconcile_push_derives_source_timestamp_from_local_tag() {
    metrics_provider::init_for_tests();
    let mock_server = MockServer::start().await;

    let dir = TempDir::new().unwrap();
    let root = dir.path().to_str().unwrap();
    let object: Arc<dyn ObjectStore> = Arc::new(StorageFsBackend::builder(root).build());
    let executor = build_test_fs_executor(root, false);
    let store = build_store(object, executor);
    let metadata_store = Arc::new(
        MetadataStore::builder(store.clone())
            .link_cache_ttl(0)
            .access_time_debounce_secs(0)
            .build(),
    );
    let blob_store = Arc::new(BlobStore::new(store.clone()));

    let (manifest_digest, config_digest, layer_digest) =
        seed_manifest(&store, &metadata_store, NAMESPACE).await;
    let expected_ts = metadata_store
        .read_link(NAMESPACE, &LinkKind::Tag("v1".to_string()))
        .await
        .unwrap()
        .created_at
        .unwrap()
        .to_rfc3339();

    for blob in [&config_digest, &layer_digest] {
        Mock::given(method("HEAD"))
            .and(path(format!("/v2/{NAMESPACE}/blobs/{blob}")))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header(DOCKER_CONTENT_DIGEST, blob.to_string().as_str())
                    .insert_header("Content-Length", "10"),
            )
            .mount(&mock_server)
            .await;
    }
    // A missing or wrong header would not match this mock and the push fails.
    Mock::given(method("PUT"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
        .and(header(X_ANGOS_SOURCE_TIMESTAMP, expected_ts.as_str()))
        .respond_with(
            ResponseTemplate::new(201)
                .insert_header(DOCKER_CONTENT_DIGEST, manifest_digest.to_string().as_str()),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let mut repositories = HashMap::new();
    repositories.insert(
        REPO.to_string(),
        repository_with_downstream(downstream_client(&mock_server.uri())),
    );
    let resolver = Arc::new(RepositoryResolver::new(Arc::new(repositories)).unwrap());

    let handler = ReplicationJobHandler::new(resolver, blob_store, metadata_store);

    let mut payload = sample_payload();
    payload.source_ts = None;
    let envelope = build_envelope(&payload).unwrap();
    handler.execute(&envelope).await.unwrap();
    drop(mock_server);
}

/// A non-superseded `409 CONFLICT` (e.g. an immutable-tag rejection) must
/// surface as `Err` so the queue retries instead of silently dropping the job.
#[tokio::test]
async fn execute_push_surfaces_immutable_conflict_409_as_error() {
    metrics_provider::init_for_tests();
    let mock_server = MockServer::start().await;

    let dir = TempDir::new().unwrap();
    let root = dir.path().to_str().unwrap();
    let object: Arc<dyn ObjectStore> = Arc::new(StorageFsBackend::builder(root).build());
    let executor = build_test_fs_executor(root, false);
    let store = build_store(object, executor);
    let metadata_store = Arc::new(
        MetadataStore::builder(store.clone())
            .link_cache_ttl(0)
            .access_time_debounce_secs(0)
            .build(),
    );
    let blob_store = Arc::new(BlobStore::new(store.clone()));

    let (_manifest_digest, config_digest, layer_digest) =
        seed_manifest(&store, &metadata_store, NAMESPACE).await;

    for blob in [&config_digest, &layer_digest] {
        Mock::given(method("HEAD"))
            .and(path(format!("/v2/{NAMESPACE}/blobs/{blob}")))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header(DOCKER_CONTENT_DIGEST, blob.to_string().as_str())
                    .insert_header("Content-Length", "10"),
            )
            .mount(&mock_server)
            .await;
    }
    Mock::given(method("PUT"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
        .respond_with(
            ResponseTemplate::new(409).set_body_string(
                r#"{"errors":[{"code":"CONFLICT","message":"tag is immutable"}]}"#,
            ),
        )
        .mount(&mock_server)
        .await;

    let mut repositories = HashMap::new();
    repositories.insert(
        REPO.to_string(),
        repository_with_downstream(downstream_client(&mock_server.uri())),
    );
    let resolver = Arc::new(RepositoryResolver::new(Arc::new(repositories)).unwrap());

    let handler = ReplicationJobHandler::new(resolver, blob_store, metadata_store);

    let envelope = build_envelope(&sample_payload()).unwrap();
    let result = handler.execute(&envelope).await;
    assert!(
        result.is_err(),
        "a non-superseded 409 CONFLICT must surface as an error so the job retries"
    );
}

/// A `409` carrying `REPLICATION_SUPERSEDED` is a last-writer-wins loss,
/// i.e. convergence, so `execute()` returns `Ok` and the job completes.
#[tokio::test]
async fn execute_push_treats_superseded_409_as_success() {
    metrics_provider::init_for_tests();
    let mock_server = MockServer::start().await;

    let dir = TempDir::new().unwrap();
    let root = dir.path().to_str().unwrap();
    let object: Arc<dyn ObjectStore> = Arc::new(StorageFsBackend::builder(root).build());
    let executor = build_test_fs_executor(root, false);
    let store = build_store(object, executor);
    let metadata_store = Arc::new(
        MetadataStore::builder(store.clone())
            .link_cache_ttl(0)
            .access_time_debounce_secs(0)
            .build(),
    );
    let blob_store = Arc::new(BlobStore::new(store.clone()));

    let (_manifest_digest, config_digest, layer_digest) =
        seed_manifest(&store, &metadata_store, NAMESPACE).await;

    for blob in [&config_digest, &layer_digest] {
        Mock::given(method("HEAD"))
            .and(path(format!("/v2/{NAMESPACE}/blobs/{blob}")))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header(DOCKER_CONTENT_DIGEST, blob.to_string().as_str())
                    .insert_header("Content-Length", "10"),
            )
            .mount(&mock_server)
            .await;
    }
    Mock::given(method("PUT"))
            .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
            .respond_with(ResponseTemplate::new(409).set_body_string(format!(
                r#"{{"errors":[{{"code":"{REPLICATION_SUPERSEDED_CODE}","message":"local copy is newer"}}]}}"#,
            )))
            .mount(&mock_server)
            .await;

    let mut repositories = HashMap::new();
    repositories.insert(
        REPO.to_string(),
        repository_with_downstream(downstream_client(&mock_server.uri())),
    );
    let resolver = Arc::new(RepositoryResolver::new(Arc::new(repositories)).unwrap());

    let handler = ReplicationJobHandler::new(resolver, blob_store, metadata_store);

    let envelope = build_envelope(&sample_payload()).unwrap();
    let tx = handler
        .execute(&envelope)
        .await
        .expect("a superseded 409 is convergence, not failure -> Ok so the job drops");
    assert!(
        tx.mutations.is_empty(),
        "a superseded push returns an empty transaction"
    );
}

#[tokio::test]
async fn execute_delete_manifest_calls_downstream_delete() {
    metrics_provider::init_for_tests();
    let mock_server = MockServer::start().await;

    Mock::given(method("DELETE"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
        .respond_with(ResponseTemplate::new(202))
        .expect(1)
        .mount(&mock_server)
        .await;

    let dir = TempDir::new().unwrap();
    let root = dir.path().to_str().unwrap();
    let object: Arc<dyn ObjectStore> = Arc::new(StorageFsBackend::builder(root).build());
    let executor = build_test_fs_executor(root, false);
    let store = build_store(object, executor);
    let metadata_store = Arc::new(
        MetadataStore::builder(store.clone())
            .link_cache_ttl(0)
            .access_time_debounce_secs(0)
            .build(),
    );
    let blob_store = Arc::new(BlobStore::new(store.clone()));

    let mut repositories = HashMap::new();
    repositories.insert(
        REPO.to_string(),
        repository_with_downstream(downstream_client(&mock_server.uri())),
    );
    let resolver = Arc::new(RepositoryResolver::new(Arc::new(repositories)).unwrap());

    let handler = ReplicationJobHandler::new(resolver, blob_store, metadata_store);

    let mut payload = sample_payload();
    payload.kind = REPLICATION_DELETE_MANIFEST_KIND.to_string();
    let envelope = build_envelope(&payload).unwrap();
    handler.execute(&envelope).await.unwrap();
}

/// Builds FS-backed stores, a seeded `v1` manifest, and a handler with one
/// downstream named `downstream` at `uri`. The `TempDir` is returned so the
/// caller keeps the backing storage alive for the test's duration.
async fn handler_with_downstream(
    downstream: &str,
    uri: &str,
) -> (ReplicationJobHandler, Digest, Digest, TempDir) {
    let dir = TempDir::new().unwrap();
    let root = dir.path().to_str().unwrap();
    let object: Arc<dyn ObjectStore> = Arc::new(StorageFsBackend::builder(root).build());
    let executor = build_test_fs_executor(root, false);
    let store = build_store(object, executor);
    let metadata_store = Arc::new(
        MetadataStore::builder(store.clone())
            .link_cache_ttl(0)
            .access_time_debounce_secs(0)
            .build(),
    );
    let blob_store = Arc::new(BlobStore::new(store.clone()));

    let (_manifest_digest, config_digest, layer_digest) =
        seed_manifest(&store, &metadata_store, NAMESPACE).await;

    let mut repositories = HashMap::new();
    repositories.insert(
        REPO.to_string(),
        repository_with_named_downstream(downstream, downstream_client(uri)),
    );
    let resolver = Arc::new(RepositoryResolver::new(Arc::new(repositories)).unwrap());

    let handler = ReplicationJobHandler::new(resolver, blob_store, metadata_store);

    (handler, config_digest, layer_digest, dir)
}

/// Mounts a HEAD per blob that returns 200 (already present) so a push skips
/// the upload sequence and only PUTs the manifest.
async fn mock_blobs_present(mock_server: &MockServer, blobs: &[&Digest]) {
    for blob in blobs {
        Mock::given(method("HEAD"))
            .and(path(format!("/v2/{NAMESPACE}/blobs/{blob}")))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header(DOCKER_CONTENT_DIGEST, blob.to_string().as_str())
                    .insert_header("Content-Length", "10"),
            )
            .mount(mock_server)
            .await;
    }
}

#[tokio::test]
async fn execute_push_records_pushed_metric_and_last_success() {
    metrics_provider::init_for_tests();
    let downstream = "metric-pushed";
    let mock_server = MockServer::start().await;
    let (handler, config_digest, layer_digest, _dir) =
        handler_with_downstream(downstream, &mock_server.uri()).await;
    mock_blobs_present(&mock_server, &[&config_digest, &layer_digest]).await;
    Mock::given(method("PUT"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
        .respond_with(ResponseTemplate::new(201).insert_header(
            DOCKER_CONTENT_DIGEST,
            "sha256:0000000000000000000000000000000000000000000000000000000000000000",
        ))
        .mount(&mock_server)
        .await;

    let pushed_before = push_total(downstream, "pushed");

    let mut payload = sample_payload();
    payload.downstream = downstream.to_string();
    let envelope = build_envelope(&payload).unwrap();
    handler.execute(&envelope).await.unwrap();

    assert_eq!(
        push_total(downstream, "pushed"),
        pushed_before + 1,
        "a successful push must increment the pushed counter exactly once"
    );
    assert_eq!(
        push_total(downstream, "superseded"),
        0,
        "a plain push must not touch the superseded counter"
    );
    assert_eq!(
        push_total(downstream, "failed"),
        0,
        "a successful push must not touch the failed counter"
    );
    assert!(
        last_success(downstream) > 0,
        "a successful push must set the last-success timestamp gauge"
    );
    drop(mock_server);
}

#[tokio::test]
async fn execute_push_records_superseded_metric_and_last_success() {
    metrics_provider::init_for_tests();
    let downstream = "metric-superseded";
    let mock_server = MockServer::start().await;
    let (handler, config_digest, layer_digest, _dir) =
        handler_with_downstream(downstream, &mock_server.uri()).await;
    mock_blobs_present(&mock_server, &[&config_digest, &layer_digest]).await;
    Mock::given(method("PUT"))
            .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
            .respond_with(ResponseTemplate::new(409).set_body_string(format!(
                r#"{{"errors":[{{"code":"{REPLICATION_SUPERSEDED_CODE}","message":"local copy is newer"}}]}}"#,
            )))
            .mount(&mock_server)
            .await;

    let superseded_before = push_total(downstream, "superseded");
    let pushed_before = push_total(downstream, "pushed");

    let mut payload = sample_payload();
    payload.downstream = downstream.to_string();
    let envelope = build_envelope(&payload).unwrap();
    handler.execute(&envelope).await.unwrap();

    assert_eq!(
        push_total(downstream, "superseded"),
        superseded_before + 1,
        "an LWW-superseded push must increment the superseded counter"
    );
    assert_eq!(
        push_total(downstream, "pushed"),
        pushed_before,
        "an LWW-superseded push must NOT increment the pushed counter"
    );
    assert!(
        last_success(downstream) > 0,
        "a superseded push is convergence and must set the last-success gauge"
    );
    drop(mock_server);
}

#[tokio::test]
async fn execute_push_records_failed_metric_on_error() {
    metrics_provider::init_for_tests();
    let downstream = "metric-failed";
    let mock_server = MockServer::start().await;
    let (handler, config_digest, layer_digest, _dir) =
        handler_with_downstream(downstream, &mock_server.uri()).await;
    mock_blobs_present(&mock_server, &[&config_digest, &layer_digest]).await;
    Mock::given(method("PUT"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
        .respond_with(
            ResponseTemplate::new(409).set_body_string(
                r#"{"errors":[{"code":"CONFLICT","message":"tag is immutable"}]}"#,
            ),
        )
        .mount(&mock_server)
        .await;

    let failed_before = push_total(downstream, "failed");
    let pushed_before = push_total(downstream, "pushed");
    let last_before = last_success(downstream);

    let mut payload = sample_payload();
    payload.downstream = downstream.to_string();
    let envelope = build_envelope(&payload).unwrap();
    let result = handler.execute(&envelope).await;

    assert!(result.is_err(), "a non-superseded 409 must surface as Err");
    assert_eq!(
        push_total(downstream, "failed"),
        failed_before + 1,
        "a failed push must increment the failed counter"
    );
    assert_eq!(
        push_total(downstream, "pushed"),
        pushed_before,
        "a failed push must NOT increment the pushed counter"
    );
    assert_eq!(
        last_success(downstream),
        last_before,
        "a failed push must NOT advance the last-success gauge"
    );
    drop(mock_server);
}

/// A converged no-op must not contact the downstream nor record any metric.
#[tokio::test]
async fn execute_push_with_deleted_tag_is_noop_success_records_no_failed() {
    metrics_provider::init_for_tests();
    let downstream = "metric-deleted-tag";

    let dir = TempDir::new().unwrap();
    let root = dir.path().to_str().unwrap();
    let object: Arc<dyn ObjectStore> = Arc::new(StorageFsBackend::builder(root).build());
    let executor = build_test_fs_executor(root, false);
    let store = build_store(object, executor);
    let metadata_store = Arc::new(
        MetadataStore::builder(store.clone())
            .link_cache_ttl(0)
            .access_time_debounce_secs(0)
            .build(),
    );
    let blob_store = Arc::new(BlobStore::new(store.clone()));

    // No tag seeded, so the resolve short-circuits; the unreachable
    // downstream URL makes any wrongful push error.
    let mut repositories = HashMap::new();
    repositories.insert(
        REPO.to_string(),
        repository_with_named_downstream(downstream, downstream_client("http://127.0.0.1:1")),
    );
    let resolver = Arc::new(RepositoryResolver::new(Arc::new(repositories)).unwrap());

    let handler = ReplicationJobHandler::new(resolver, blob_store, metadata_store);

    let failed_before = push_total(downstream, "failed");
    let pushed_before = push_total(downstream, "pushed");

    let mut payload = sample_payload();
    payload.downstream = downstream.to_string();
    let envelope = build_envelope(&payload).unwrap();
    let tx = handler
        .execute(&envelope)
        .await
        .expect("a deleted-tag push must be a converged no-op success");
    assert!(
        tx.mutations.is_empty(),
        "a no-op push returns an empty transaction"
    );
    assert_eq!(
        push_total(downstream, "failed"),
        failed_before,
        "a converged no-op must NOT increment the failed counter"
    );
    assert_eq!(
        push_total(downstream, "pushed"),
        pushed_before,
        "a converged no-op must NOT increment the pushed counter"
    );
}

#[tokio::test]
async fn execute_tagless_push_with_deleted_revision_is_noop_success() {
    metrics_provider::init_for_tests();
    let downstream = "metric-deleted-revision";

    let dir = TempDir::new().unwrap();
    let root = dir.path().to_str().unwrap();
    let object: Arc<dyn ObjectStore> = Arc::new(StorageFsBackend::builder(root).build());
    let executor = build_test_fs_executor(root, false);
    let store = build_store(object, executor);
    let metadata_store = Arc::new(
        MetadataStore::builder(store.clone())
            .link_cache_ttl(0)
            .access_time_debounce_secs(0)
            .build(),
    );
    let blob_store = Arc::new(BlobStore::new(store.clone()));

    // No revision link seeded, so the resolve short-circuits; the
    // unreachable downstream URL makes any wrongful push error.
    let mut repositories = HashMap::new();
    repositories.insert(
        REPO.to_string(),
        repository_with_named_downstream(downstream, downstream_client("http://127.0.0.1:1")),
    );
    let resolver = Arc::new(RepositoryResolver::new(Arc::new(repositories)).unwrap());

    let handler = ReplicationJobHandler::new(resolver, blob_store, metadata_store);

    let failed_before = push_total(downstream, "failed");
    let pushed_before = push_total(downstream, "pushed");

    let mut payload = sample_payload();
    payload.downstream = downstream.to_string();
    payload.tag = None;
    let envelope = build_envelope(&payload).unwrap();
    let tx = handler
        .execute(&envelope)
        .await
        .expect("a deleted-revision by-digest push must be a converged no-op success");
    assert!(
        tx.mutations.is_empty(),
        "a no-op push returns an empty transaction"
    );
    assert_eq!(
        push_total(downstream, "failed"),
        failed_before,
        "a converged no-op must NOT increment the failed counter"
    );
    assert_eq!(
        push_total(downstream, "pushed"),
        pushed_before,
        "a converged no-op must NOT increment the pushed counter"
    );
}
