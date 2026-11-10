use std::sync::Arc;

use chrono::{DateTime, Utc};
use tempfile::TempDir;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use angos_oci::header::{DOCKER_CONTENT_DIGEST, X_ANGOS_SOURCE_TIMESTAMP};
use angos_oci::response::REPLICATION_SUPERSEDED_CODE;
use angos_oci::{Digest, Namespace, Tag};
use angos_oci_client::RegistryClient;

use crate::jobs::Queue;
use crate::jobs::store::{Error, JobHandler};
use crate::metrics_provider;
use crate::registry::Repository;
use crate::registry::metadata_store::LinkKind;
use crate::registry::test_utils::{
    FsTestStack, downstream_client, fs_test_stack, repository_with_replication, seed_manifest,
    single_repo_resolver, test_downstream,
};
use crate::replication::handler::{
    ReplicationJob, ReplicationJobHandler, ReplicationTarget, build_envelope,
    build_prune_delete_envelope, job_error, replication_lock_key,
};
use crate::replication::{
    Error as ReplicationError, REPLICATION_DELETE_MANIFEST_KIND, REPLICATION_PUSH_MANIFEST_KIND,
    ReplicationDownstream,
};
use crate::test_fixtures::mocks::{mount_blob_upload_accepted, mount_blobs_present};

const NAMESPACE: &str = "nginx";
const REPO: &str = "nginx";
const DOWNSTREAM: &str = "eu-region";

fn sample_target() -> ReplicationTarget {
    ReplicationTarget {
        downstream: DOWNSTREAM.to_string(),
        namespace: Namespace::new(NAMESPACE).unwrap(),
        tag: Some(Tag::new("v1").unwrap()),
        digest: Some(
            "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
                .parse()
                .unwrap(),
        ),
        source_ts: Some(
            DateTime::parse_from_rfc3339("2026-06-03T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
        ),
    }
}

fn sample_payload() -> ReplicationJob {
    ReplicationJob::Push {
        target: sample_target(),
    }
}

fn instant(rfc3339: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(rfc3339)
        .unwrap()
        .with_timezone(&Utc)
}

/// A delete of the sample target, stamped at `source_ts`.
fn sample_delete(source_ts: &str) -> ReplicationJob {
    ReplicationJob::Delete {
        target: ReplicationTarget {
            source_ts: Some(instant(source_ts)),
            ..sample_target()
        },
        subject: None,
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
fn lock_key_falls_back_to_digest_without_tag() {
    let mut target = sample_target();
    target.tag = None;
    let digest = target.digest.clone().unwrap();
    let payload = ReplicationJob::Push { target };
    assert_eq!(
        replication_lock_key(&payload),
        format!("replication.push.eu-region:nginx:{digest}")
    );
}

/// A delete must never coalesce into (and be swallowed by) a pending push, and
/// keys on its source instant spelled exactly as stored payloads did, so an
/// upgraded replica still coalesces with a job a peer already queued.
#[test]
fn lock_key_distinguishes_push_from_delete() {
    let legacy_source_ts = "2024-01-01T00:00:00.123456789+00:00";
    assert_eq!(
        replication_lock_key(&sample_payload()),
        "replication.push.eu-region:nginx:v1"
    );
    assert_eq!(
        replication_lock_key(&sample_delete(legacy_source_ts)),
        format!("replication.delete.eu-region:nginx:v1@{legacy_source_ts}")
    );
}

/// Repeated reconcile runs stamp fresh `source_ts` values, yet their prune
/// deletes must coalesce into one pending job and never coalesce with a
/// timestamped event-path delete.
#[test]
fn prune_delete_envelope_coalesces_on_bare_reference() {
    let payload = sample_delete("2026-06-03T00:00:00Z");
    let later = sample_delete("2026-06-03T00:01:00Z");

    let first = build_prune_delete_envelope(&payload).unwrap();
    let second = build_prune_delete_envelope(&later).unwrap();
    assert_eq!(
        first.lock_key.as_str(),
        "replication.delete.eu-region:nginx:v1"
    );
    assert_eq!(
        first.lock_key, second.lock_key,
        "prune deletes with different source_ts must coalesce on the bare reference"
    );
    assert_ne!(
        first.lock_key.as_str(),
        replication_lock_key(&payload),
        "a prune delete must not coalesce with an event-path delete"
    );
}

#[test]
fn lock_key_handles_missing_tag_and_digest() {
    let payload = ReplicationJob::Push {
        target: ReplicationTarget {
            tag: None,
            digest: None,
            ..sample_target()
        },
    };
    assert_eq!(
        replication_lock_key(&payload),
        "replication.push.eu-region:nginx:"
    );
}

#[test]
fn build_envelope_sets_queue_and_kind() {
    let envelope = build_envelope(&sample_payload()).unwrap();
    assert_eq!(envelope.queue, Queue::Replication);
    assert_eq!(envelope.kind, REPLICATION_PUSH_MANIFEST_KIND);
}

#[test]
fn job_error_dead_letters_invalid_manifest_content() {
    // A body that will never parse must not spend the retry budget, while a
    // replication-internal fault stays retryable.
    assert!(matches!(
        job_error(ReplicationError::InvalidManifest(
            "manifest parse failed".to_string()
        )),
        Error::Terminal(_)
    ));
    assert!(matches!(
        job_error(ReplicationError::Internal(
            "namespace mapping failed".to_string()
        )),
        Error::Execution(_)
    ));
}

fn repository_with_downstream(client: Arc<RegistryClient>) -> Repository {
    repository_with_named_downstream(DOWNSTREAM, client)
}

/// Lets a test pick the downstream name so its metric label set is isolated
/// from other tests.
fn repository_with_named_downstream(name: &str, client: Arc<RegistryClient>) -> Repository {
    repository_with_replication(REPO, vec![test_downstream(name.to_string(), client)])
}

/// A job for a downstream no longer in config fails loudly (and so
/// dead-letters after max attempts) instead of silently completing.
#[tokio::test]
async fn execute_errors_on_removed_downstream() {
    metrics_provider::initialize_metrics();
    let FsTestStack {
        dir: _dir,
        metadata_store,
        blob_store,
    } = fs_test_stack();

    let resolver = single_repo_resolver(
        REPO,
        repository_with_downstream(downstream_client("https://unused.test")),
    );

    let handler = ReplicationJobHandler::new(resolver, blob_store, metadata_store);

    let payload = ReplicationJob::Push {
        target: ReplicationTarget {
            downstream: "removed-region".to_string(),
            ..sample_target()
        },
    };
    let envelope = build_envelope(&payload).unwrap();
    let result = handler.execute(&envelope).await;
    assert!(
        result
            .as_ref()
            .is_err_and(|e| e.to_string().contains("no downstream 'removed-region'")),
        "a job for a de-configured downstream must error, got: {result:?}",
    );
}

/// A downstream `403` is terminal, so the worker dead-letters it instead of
/// retrying against revoked credentials.
#[tokio::test]
async fn execute_maps_downstream_403_to_terminal() {
    metrics_provider::initialize_metrics();
    let mock_server = MockServer::start().await;
    let (handler, config_digest, layer_digest, _dir) =
        handler_with_downstream(DOWNSTREAM, &mock_server.uri()).await;

    mount_blobs_present(&mock_server, NAMESPACE, &[&config_digest, &layer_digest]).await;
    Mock::given(method("PUT"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
        .respond_with(ResponseTemplate::new(403))
        .mount(&mock_server)
        .await;

    let envelope = build_envelope(&sample_payload()).unwrap();
    let result = handler.execute(&envelope).await;
    assert!(
        matches!(result, Err(Error::Terminal(_))),
        "a downstream 403 must surface as a terminal failure, got {result:?}"
    );
}

/// A prefixed downstream must push at the mapped path: local `nginx/app` strips
/// `nginx` and prepends `mirror`, so manifest and blobs land at `mirror/app`.
#[tokio::test]
async fn execute_pushes_prefixed_downstream_to_mapped_namespace() {
    metrics_provider::initialize_metrics();
    let mock_server = MockServer::start().await;

    let FsTestStack {
        dir: _dir,
        metadata_store,
        blob_store,
    } = fs_test_stack();

    let local_namespace = Namespace::new("nginx/app").unwrap();
    let mapped = "mirror/app";
    let (manifest_digest, config_digest, layer_digest) =
        seed_manifest(&blob_store, &metadata_store, &local_namespace).await;

    // Downstream is missing both blobs (404 on HEAD) -> upload sequence runs.
    mount_blob_upload_accepted(&mock_server, mapped, &[&config_digest, &layer_digest]).await;
    Mock::given(method("PUT"))
        .and(path(format!("/v2/{mapped}/manifests/v1")))
        .respond_with(
            ResponseTemplate::new(201)
                .insert_header(DOCKER_CONTENT_DIGEST, manifest_digest.to_string().as_str()),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    // REPO stays `nginx` so the resolver routes `nginx/app` to this repo; the
    // downstream carries the strip+prepend mapping.
    let downstream = ReplicationDownstream {
        local_namespace: Some(Namespace::new("nginx").unwrap()),
        target_namespace: Some(Namespace::new("mirror").unwrap()),
        ..test_downstream(
            DOWNSTREAM.to_string(),
            downstream_client(&mock_server.uri()),
        )
    };
    let resolver = single_repo_resolver(REPO, repository_with_replication(REPO, vec![downstream]));

    let handler = ReplicationJobHandler::new(resolver, blob_store, metadata_store);

    let payload = ReplicationJob::Push {
        target: ReplicationTarget {
            namespace: local_namespace,
            ..sample_target()
        },
    };
    let envelope = build_envelope(&payload).unwrap();
    handler.execute(&envelope).await.unwrap();

    drop(mock_server);
}

/// The manifest PUT must carry an `X-Angos-Source-Timestamp` derived from
/// the resolved tag's `created_at`, not the stale payload timestamp, so a
/// coalesced push cannot ship a stale last-writer-wins version.
#[tokio::test]
async fn execute_push_stamps_resolved_source_timestamp() {
    metrics_provider::initialize_metrics();
    let mock_server = MockServer::start().await;

    let FsTestStack {
        dir: _dir,
        metadata_store,
        blob_store,
    } = fs_test_stack();

    let (manifest_digest, config_digest, layer_digest) = seed_manifest(
        &blob_store,
        &metadata_store,
        &Namespace::new(NAMESPACE).unwrap(),
    )
    .await;

    let expected_ts = metadata_store
        .read_link(
            &Namespace::new(NAMESPACE).unwrap(),
            &LinkKind::Tag(Tag::new("v1").unwrap()),
        )
        .await
        .unwrap()
        .created_at
        .unwrap()
        .to_rfc3339();

    mount_blobs_present(&mock_server, NAMESPACE, &[&config_digest, &layer_digest]).await;
    // A missing or wrong header would not match this mock, failing the push.
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

    let resolver = single_repo_resolver(
        REPO,
        repository_with_downstream(downstream_client(&mock_server.uri())),
    );

    let handler = ReplicationJobHandler::new(resolver, blob_store, metadata_store);

    let envelope = build_envelope(&sample_payload()).unwrap();
    handler.execute(&envelope).await.unwrap();
    drop(mock_server);
}

#[tokio::test]
async fn execute_delete_manifest_calls_downstream_delete() {
    metrics_provider::initialize_metrics();
    let mock_server = MockServer::start().await;

    Mock::given(method("DELETE"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
        .respond_with(ResponseTemplate::new(202))
        .expect(1)
        .mount(&mock_server)
        .await;

    let FsTestStack {
        dir: _dir,
        metadata_store,
        blob_store,
    } = fs_test_stack();

    let resolver = single_repo_resolver(
        REPO,
        repository_with_downstream(downstream_client(&mock_server.uri())),
    );

    let handler = ReplicationJobHandler::new(resolver, blob_store, metadata_store);

    let payload = sample_delete("2026-06-03T00:00:00Z");
    let envelope = build_envelope(&payload).unwrap();
    handler.execute(&envelope).await.unwrap();
}

/// Builds FS-backed stores, a seeded `v1` manifest, and a handler with one
/// downstream named `downstream` at `uri`; the returned `TempDir` keeps the
/// backing storage alive.
async fn handler_with_downstream(
    downstream: &str,
    uri: &str,
) -> (ReplicationJobHandler, Digest, Digest, TempDir) {
    let FsTestStack {
        dir,
        metadata_store,
        blob_store,
    } = fs_test_stack();

    let (_manifest_digest, config_digest, layer_digest) = seed_manifest(
        &blob_store,
        &metadata_store,
        &Namespace::new(NAMESPACE).unwrap(),
    )
    .await;

    let resolver = single_repo_resolver(
        REPO,
        repository_with_named_downstream(downstream, downstream_client(uri)),
    );

    let handler = ReplicationJobHandler::new(resolver, blob_store, metadata_store);

    (handler, config_digest, layer_digest, dir)
}

#[tokio::test]
async fn execute_push_records_pushed_metric_and_last_success() {
    metrics_provider::initialize_metrics();
    let downstream = "metric-pushed";
    let mock_server = MockServer::start().await;
    let (handler, config_digest, layer_digest, _dir) =
        handler_with_downstream(downstream, &mock_server.uri()).await;
    mount_blobs_present(&mock_server, NAMESPACE, &[&config_digest, &layer_digest]).await;
    Mock::given(method("PUT"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
        .respond_with(ResponseTemplate::new(201).insert_header(
            DOCKER_CONTENT_DIGEST,
            "sha256:0000000000000000000000000000000000000000000000000000000000000000",
        ))
        .mount(&mock_server)
        .await;

    let pushed_before = push_total(downstream, "pushed");

    let payload = ReplicationJob::Push {
        target: ReplicationTarget {
            downstream: downstream.to_string(),
            ..sample_target()
        },
    };
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
    metrics_provider::initialize_metrics();
    let downstream = "metric-superseded";
    let mock_server = MockServer::start().await;
    let (handler, config_digest, layer_digest, _dir) =
        handler_with_downstream(downstream, &mock_server.uri()).await;
    mount_blobs_present(&mock_server, NAMESPACE, &[&config_digest, &layer_digest]).await;
    Mock::given(method("PUT"))
            .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
            .respond_with(ResponseTemplate::new(409).set_body_string(format!(
                r#"{{"errors":[{{"code":"{REPLICATION_SUPERSEDED_CODE}","message":"local copy is newer"}}]}}"#,
            )))
            .mount(&mock_server)
            .await;

    let superseded_before = push_total(downstream, "superseded");
    let pushed_before = push_total(downstream, "pushed");

    let payload = ReplicationJob::Push {
        target: ReplicationTarget {
            downstream: downstream.to_string(),
            ..sample_target()
        },
    };
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
    metrics_provider::initialize_metrics();
    let downstream = "metric-failed";
    let mock_server = MockServer::start().await;
    let (handler, config_digest, layer_digest, _dir) =
        handler_with_downstream(downstream, &mock_server.uri()).await;
    mount_blobs_present(&mock_server, NAMESPACE, &[&config_digest, &layer_digest]).await;
    Mock::given(method("PUT"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
        .respond_with(
            ResponseTemplate::new(409)
                .set_body_string(r#"{"errors":[{"code":"DENIED","message":"tag is immutable"}]}"#),
        )
        .mount(&mock_server)
        .await;

    let failed_before = push_total(downstream, "failed");
    let pushed_before = push_total(downstream, "pushed");
    let last_before = last_success(downstream);

    let payload = ReplicationJob::Push {
        target: ReplicationTarget {
            downstream: downstream.to_string(),
            ..sample_target()
        },
    };
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

/// A push whose target is gone locally is a converged no-op: it contacts no
/// downstream and records no metric.
#[tokio::test]
async fn execute_push_of_a_deleted_target_is_noop_success() {
    metrics_provider::initialize_metrics();
    let downstream = "metric-deleted-target";

    let FsTestStack {
        dir: _dir,
        metadata_store,
        blob_store,
    } = fs_test_stack();

    // Nothing seeded, so the resolve short-circuits; the unreachable
    // downstream URL makes any wrongful push error.
    let resolver = single_repo_resolver(
        REPO,
        repository_with_named_downstream(downstream, downstream_client("http://127.0.0.1:1")),
    );

    let handler = ReplicationJobHandler::new(resolver, blob_store, metadata_store);

    // A tag-scoped push misses the tag, a tag-less one the revision link.
    for tag in [sample_target().tag, None] {
        let failed_before = push_total(downstream, "failed");
        let pushed_before = push_total(downstream, "pushed");

        let payload = ReplicationJob::Push {
            target: ReplicationTarget {
                downstream: downstream.to_string(),
                tag,
                ..sample_target()
            },
        };
        handler
            .execute(&build_envelope(&payload).unwrap())
            .await
            .expect("a push of a deleted target must be a converged no-op success");
        assert_eq!(push_total(downstream, "failed"), failed_before);
        assert_eq!(push_total(downstream, "pushed"), pushed_before);
    }
}

/// The durable queue stores this payload, so its JSON is a stored format: these
/// exact bodies must still decode and still address the same work.
#[test]
fn a_queued_job_written_before_the_split_still_decodes() {
    let hash_a = "a".repeat(64);
    let hash_b = "b".repeat(64);
    let stored_push = format!(
        r#"{{"downstream":"eu","namespace":"ns/app","tag":"v1","digest":"sha256:{hash_a}",
             "kind":"replication.push_manifest","source_ts":"2024-01-01T00:00:00.123456789+00:00"}}"#
    );
    let stored_delete = format!(
        r#"{{"downstream":"eu","namespace":"ns/app","digest":"sha256:{hash_b}",
             "kind":"replication.delete_manifest","subject":"sha256:{hash_a}"}}"#
    );

    let push: ReplicationJob = serde_json::from_str(&stored_push).expect("stored push must decode");
    let ReplicationJob::Push { target } = &push else {
        panic!("a stored push must decode as a push, got {push:?}");
    };
    assert_eq!(target.downstream, "eu");
    assert_eq!(target.tag.as_deref(), Some("v1"));
    assert_eq!(
        target.digest.as_ref().map(ToString::to_string),
        Some(format!("sha256:{hash_a}"))
    );
    assert_eq!(
        target.source_ts,
        Some(instant("2024-01-01T00:00:00.123456789+00:00"))
    );
    assert_eq!(push.kind(), REPLICATION_PUSH_MANIFEST_KIND);

    let delete: ReplicationJob =
        serde_json::from_str(&stored_delete).expect("stored delete must decode");
    let ReplicationJob::Delete { target, subject } = &delete else {
        panic!("a stored delete must decode as a delete, got {delete:?}");
    };
    assert!(target.tag.is_none());
    assert_eq!(
        subject.as_ref().map(ToString::to_string),
        Some(format!("sha256:{hash_a}"))
    );
    assert_eq!(delete.kind(), REPLICATION_DELETE_MANIFEST_KIND);
}

/// A written job must keep the queue's stored shape, `kind` beside the flat
/// target fields, so a peer replica on the older build keeps draining it
/// through a rolling upgrade.
#[test]
fn a_written_job_keeps_the_stored_layout() {
    let json = serde_json::to_value(sample_delete("2024-01-01T00:00:00+00:00")).unwrap();
    let object = json.as_object().expect("a job serializes to an object");

    assert_eq!(object["kind"], REPLICATION_DELETE_MANIFEST_KIND);
    assert_eq!(object["downstream"], "eu-region");
    assert_eq!(object["namespace"], "nginx");
    assert_eq!(object["tag"], "v1");
    assert!(
        object["digest"]
            .as_str()
            .is_some_and(|d| d.starts_with("sha256:")),
        "the digest stays a plain digest string: {json}"
    );
    assert!(
        !object.contains_key("subject"),
        "an absent subject stays absent rather than serializing as null: {json}"
    );
}
