use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use serde_json::{Value, json};
use tempfile::TempDir;
use wiremock::{
    Mock, MockServer, Request, Respond, ResponseTemplate,
    matchers::{header, method, path, query_param},
};

use angos_storage::{ObjectStore, fs::Backend as StorageFsBackend};
use angos_tx_engine::store::Store;

use crate::{
    cache, metrics_provider,
    oci::{
        DOCKER_MANIFEST_LIST_MEDIA_TYPE, DOCKER_MANIFEST_MEDIA_TYPE, Digest, OCI_INDEX_MEDIA_TYPE,
        OCI_MANIFEST_MEDIA_TYPE, Reference, Tag,
    },
    registry::{
        DOCKER_CONTENT_DIGEST, OCI_SUBJECT,
        blob_store::BlobStore,
        manifest::DEFAULT_MAX_MANIFEST_SIZE_BYTES,
        metadata_store::{BlobIndexOperation, LinkKind, LinkOperation, MetadataStore},
        test_utils::{build_store, build_test_fs_executor, put_blob_direct},
    },
    registry_client::{RegistryClient, UploadSession},
    replication::{
        REPLICATION_SUPERSEDED_CODE, X_ANGOS_SOURCE_TIMESTAMP,
        pipeline::{PushContext, PushOutcome, delete_manifest, push_manifest},
    },
};

const NAMESPACE: &str = "nginx";

fn downstream_client(uri: &str) -> RegistryClient {
    let backend = cache::Config::Memory.to_backend().unwrap();
    RegistryClient::builder(uri.to_string(), reqwest::Client::new(), backend)
        .max_manifest_size_bytes(DEFAULT_MAX_MANIFEST_SIZE_BYTES)
        .build()
}

fn test_blob_store(root: &str) -> (Arc<BlobStore>, Arc<MetadataStore>, Arc<Store>) {
    let object: Arc<dyn ObjectStore> = Arc::new(StorageFsBackend::builder(root).build());
    let executor = build_test_fs_executor(root, false);
    let store = build_store(object, executor);
    let blob_store = Arc::new(BlobStore::new(store.clone()));
    let metadata_store = Arc::new(
        MetadataStore::builder(store.clone())
            .link_cache_ttl(0)
            .access_time_debounce_secs(0)
            .build(),
    );
    (blob_store, metadata_store, store)
}

/// A wiremock responder recording the order in which the index vs. its
/// child manifest are PUT.
struct OrderRecorder {
    seen: Arc<AtomicUsize>,
    child_at: Arc<AtomicUsize>,
    index_at: Arc<AtomicUsize>,
    is_index: bool,
    digest: String,
}

impl Respond for OrderRecorder {
    fn respond(&self, _request: &Request) -> ResponseTemplate {
        let order = self.seen.fetch_add(1, Ordering::SeqCst);
        if self.is_index {
            self.index_at.store(order, Ordering::SeqCst);
        } else {
            self.child_at.store(order, Ordering::SeqCst);
        }
        ResponseTemplate::new(201).insert_header(DOCKER_CONTENT_DIGEST, self.digest.as_str())
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn push_referrers_fallback_when_downstream_is_oci_1_0() {
    metrics_provider::init_for_tests();
    let mock_server = MockServer::start().await;
    let dir = TempDir::new().unwrap();
    let (blob_store, metadata_store, store) = test_blob_store(dir.path().to_str().unwrap());

    let subject = put_blob_direct(&store, b"subject-bytes").await;
    let config = put_blob_direct(&store, br#"{"c":1}"#).await;

    let manifest = json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": config.to_string(),
            "size": 7,
        },
        "layers": [],
        "subject": {
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "digest": subject.to_string(),
            "size": 13,
        },
    });
    let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
    let manifest_digest = put_blob_direct(&store, &manifest_bytes).await;

    Mock::given(method("HEAD"))
        .and(path(format!("/v2/{NAMESPACE}/blobs/{config}")))
        .respond_with(ResponseTemplate::new(404))
        .mount(&mock_server)
        .await;
    Mock::given(method("POST"))
        .and(path(format!("/v2/{NAMESPACE}/blobs/uploads/")))
        .respond_with(
            ResponseTemplate::new(202)
                .insert_header("Location", format!("/v2/{NAMESPACE}/blobs/uploads/s")),
        )
        .mount(&mock_server)
        .await;
    Mock::given(method("PATCH"))
        .and(path(format!("/v2/{NAMESPACE}/blobs/uploads/s")))
        .respond_with(
            ResponseTemplate::new(202)
                .insert_header("Location", format!("/v2/{NAMESPACE}/blobs/uploads/s")),
        )
        .mount(&mock_server)
        .await;
    Mock::given(method("PUT"))
        .and(path(format!("/v2/{NAMESPACE}/blobs/uploads/s")))
        .respond_with(ResponseTemplate::new(201))
        .mount(&mock_server)
        .await;

    // No `OCI-Subject` response header => OCI-1.0 downstream => fallback
    // expected.
    Mock::given(method("PUT"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
        .respond_with(
            ResponseTemplate::new(201)
                .insert_header(DOCKER_CONTENT_DIGEST, manifest_digest.to_string().as_str()),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    // The pipeline GETs the existing fallback index first (404 => start fresh).
    let fallback_tag = format!("{}-{}", subject.algorithm(), subject.hash());
    Mock::given(method("GET"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/{fallback_tag}")))
        .respond_with(ResponseTemplate::new(404))
        .mount(&mock_server)
        .await;

    // Assert the PUT body is a merged image index so a regression back to
    // "PUT the referrer manifest body" cannot pass silently.
    let referrer_digest = Digest::sha256_of_bytes(&manifest_bytes);
    Mock::given(method("PUT"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/{fallback_tag}")))
        .respond_with(move |request: &Request| {
            let index: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
            assert_eq!(
                index["mediaType"], "application/vnd.oci.image.index.v1+json",
                "fallback body must be an image index, not the referrer manifest"
            );
            let manifests = index["manifests"].as_array().unwrap();
            assert_eq!(manifests.len(), 1, "exactly one referrer descriptor");
            assert_eq!(
                manifests[0]["digest"],
                referrer_digest.to_string(),
                "descriptor must reference the pushed referrer manifest by digest"
            );
            ResponseTemplate::new(201)
                .insert_header(DOCKER_CONTENT_DIGEST, referrer_digest.to_string().as_str())
        })
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = downstream_client(&mock_server.uri());
    let ctx = PushContext {
        downstream: &client,
        blob_store: &blob_store,
        metadata_store: &metadata_store,
        namespace: NAMESPACE,
        max_concurrent_pushes: 4,
        source_ts: None,
    };
    push_manifest(
        &ctx,
        &manifest_digest,
        Some("application/vnd.oci.image.manifest.v1+json".to_string()),
        Some("v1"),
        manifest_bytes,
    )
    .await
    .unwrap();

    drop(mock_server);
}

#[tokio::test]
async fn referrers_fallback_put_is_timestamp_less() {
    // The fallback tag is a merged set, not an LWW register: a stamped
    // fallback PUT could come back superseded and silently drop the
    // just-merged descriptor.
    metrics_provider::init_for_tests();
    let mock_server = MockServer::start().await;
    let dir = TempDir::new().unwrap();
    let (blob_store, metadata_store, store) = test_blob_store(dir.path().to_str().unwrap());

    let subject = put_blob_direct(&store, b"subject-bytes").await;
    let manifest = json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "layers": [],
        "subject": {
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "digest": subject.to_string(),
            "size": 13,
        },
    });
    let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
    let manifest_digest = put_blob_direct(&store, &manifest_bytes).await;

    // The primary PUT must carry the header; no `OCI-Subject` => fallback runs.
    Mock::given(method("PUT"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
        .and(header(X_ANGOS_SOURCE_TIMESTAMP, "2026-06-03T00:00:00Z"))
        .respond_with(
            ResponseTemplate::new(201)
                .insert_header(DOCKER_CONTENT_DIGEST, manifest_digest.to_string().as_str()),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    // The fallback-index PUT must not carry the source-timestamp header.
    let fallback_tag = format!("{}-{}", subject.algorithm(), subject.hash());
    Mock::given(method("GET"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/{fallback_tag}")))
        .respond_with(ResponseTemplate::new(404))
        .mount(&mock_server)
        .await;
    let digest_str = manifest_digest.to_string();
    Mock::given(method("PUT"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/{fallback_tag}")))
        .respond_with(move |request: &Request| {
            assert!(
                !request
                    .headers
                    .contains_key(X_ANGOS_SOURCE_TIMESTAMP.to_lowercase().as_str()),
                "the fallback-index PUT must be timestamp-less (set merge, not LWW)"
            );
            ResponseTemplate::new(201).insert_header(DOCKER_CONTENT_DIGEST, digest_str.as_str())
        })
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = downstream_client(&mock_server.uri());
    let ctx = PushContext {
        downstream: &client,
        blob_store: &blob_store,
        metadata_store: &metadata_store,
        namespace: NAMESPACE,
        max_concurrent_pushes: 4,
        source_ts: Some("2026-06-03T00:00:00Z"),
    };
    push_manifest(&ctx, &manifest_digest, None, Some("v1"), manifest_bytes)
        .await
        .expect("subject push with a stamped primary and timestamp-less fallback");
    drop(mock_server);
}

#[tokio::test]
async fn referrers_fallback_propagates_transient_get_error_without_clobbering() {
    // Treating a transient GET failure as "tag absent" would PUT an index
    // built from an empty base and drop the subject's sibling referrers.
    metrics_provider::init_for_tests();
    let mock_server = MockServer::start().await;
    let dir = TempDir::new().unwrap();
    let (blob_store, metadata_store, store) = test_blob_store(dir.path().to_str().unwrap());

    let subject = put_blob_direct(&store, b"subject-bytes").await;
    let config = put_blob_direct(&store, br#"{"c":1}"#).await;
    let manifest = json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": config.to_string(),
            "size": 7,
        },
        "layers": [],
        "subject": {
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "digest": subject.to_string(),
            "size": 13,
        },
    });
    let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
    let manifest_digest = put_blob_direct(&store, &manifest_bytes).await;

    Mock::given(method("HEAD"))
        .and(path(format!("/v2/{NAMESPACE}/blobs/{config}")))
        .respond_with(ResponseTemplate::new(404))
        .mount(&mock_server)
        .await;
    Mock::given(method("POST"))
        .and(path(format!("/v2/{NAMESPACE}/blobs/uploads/")))
        .respond_with(
            ResponseTemplate::new(202)
                .insert_header("Location", format!("/v2/{NAMESPACE}/blobs/uploads/s")),
        )
        .mount(&mock_server)
        .await;
    Mock::given(method("PATCH"))
        .and(path(format!("/v2/{NAMESPACE}/blobs/uploads/s")))
        .respond_with(
            ResponseTemplate::new(202)
                .insert_header("Location", format!("/v2/{NAMESPACE}/blobs/uploads/s")),
        )
        .mount(&mock_server)
        .await;
    Mock::given(method("PUT"))
        .and(path(format!("/v2/{NAMESPACE}/blobs/uploads/s")))
        .respond_with(ResponseTemplate::new(201))
        .mount(&mock_server)
        .await;

    // No `OCI-Subject` response => OCI-1.0 => fallback runs.
    Mock::given(method("PUT"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
        .respond_with(
            ResponseTemplate::new(201)
                .insert_header(DOCKER_CONTENT_DIGEST, manifest_digest.to_string().as_str()),
        )
        .mount(&mock_server)
        .await;

    // The fallback index GET fails with 500; the merge PUT must not run.
    let fallback_tag = format!("{}-{}", subject.algorithm(), subject.hash());
    Mock::given(method("GET"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/{fallback_tag}")))
        .respond_with(ResponseTemplate::new(500))
        .mount(&mock_server)
        .await;
    Mock::given(method("PUT"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/{fallback_tag}")))
        .respond_with(ResponseTemplate::new(201))
        .expect(0)
        .mount(&mock_server)
        .await;

    let client = downstream_client(&mock_server.uri());
    let ctx = PushContext {
        downstream: &client,
        blob_store: &blob_store,
        metadata_store: &metadata_store,
        namespace: NAMESPACE,
        max_concurrent_pushes: 4,
        source_ts: None,
    };
    let result = push_manifest(
        &ctx,
        &manifest_digest,
        Some("application/vnd.oci.image.manifest.v1+json".to_string()),
        Some("v1"),
        manifest_bytes,
    )
    .await;

    assert!(
        result.is_err(),
        "a transient fallback-index GET error must fail the push so the job retries, got: {result:?}"
    );

    drop(mock_server);
}

#[tokio::test]
async fn referrers_fallback_errors_on_unparseable_index_without_clobbering() {
    // Treating an unusable 200 body as an empty base would rebuild the index
    // from empty and drop the subject's existing sibling referrers.
    metrics_provider::init_for_tests();
    let mock_server = MockServer::start().await;
    let dir = TempDir::new().unwrap();
    let (blob_store, metadata_store, store) = test_blob_store(dir.path().to_str().unwrap());

    let subject = put_blob_direct(&store, b"subject-bytes").await;
    // Config-less manifest, so no blob upload mocks are needed.
    let manifest = json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "layers": [],
        "subject": {
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "digest": subject.to_string(),
            "size": 13,
        },
    });
    let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
    let manifest_digest = put_blob_direct(&store, &manifest_bytes).await;

    // No `OCI-Subject` response => OCI-1.0 => fallback runs.
    Mock::given(method("PUT"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
        .respond_with(
            ResponseTemplate::new(201)
                .insert_header(DOCKER_CONTENT_DIGEST, manifest_digest.to_string().as_str()),
        )
        .mount(&mock_server)
        .await;

    // The fallback GET returns a 200 whose JSON body has no `manifests`
    // array; the merge PUT must not run.
    let fallback_tag = format!("{}-{}", subject.algorithm(), subject.hash());
    Mock::given(method("GET"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/{fallback_tag}")))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header(DOCKER_CONTENT_DIGEST, manifest_digest.to_string().as_str())
                .set_body_json(json!({
                    "schemaVersion": 2,
                    "mediaType": OCI_INDEX_MEDIA_TYPE,
                })),
        )
        .mount(&mock_server)
        .await;
    Mock::given(method("PUT"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/{fallback_tag}")))
        .respond_with(ResponseTemplate::new(201))
        .expect(0)
        .mount(&mock_server)
        .await;

    let client = downstream_client(&mock_server.uri());
    let ctx = PushContext {
        downstream: &client,
        blob_store: &blob_store,
        metadata_store: &metadata_store,
        namespace: NAMESPACE,
        max_concurrent_pushes: 4,
        source_ts: None,
    };
    let result = push_manifest(&ctx, &manifest_digest, None, Some("v1"), manifest_bytes).await;

    assert!(
        result.is_err(),
        "a malformed fallback index must fail the push so it is not overwritten, got: {result:?}"
    );

    drop(mock_server);
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn concurrent_same_subject_referrers_merge_without_lost_update() {
    // Without the store lock both concurrent merges read the same base
    // index and one descriptor vanishes from the final PUT.
    metrics_provider::init_for_tests();
    let mock_server = MockServer::start().await;
    let dir = TempDir::new().unwrap();
    let (blob_store, metadata_store, store) = test_blob_store(dir.path().to_str().unwrap());

    let subject = put_blob_direct(&store, b"subject-bytes").await;
    let referrer = |name: &str| {
        json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "layers": [],
            "annotations": { "org.example.ref": name },
            "subject": {
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "digest": subject.to_string(),
                "size": 13,
            },
        })
    };
    let bytes_a = serde_json::to_vec(&referrer("a")).unwrap();
    let bytes_b = serde_json::to_vec(&referrer("b")).unwrap();
    let digest_a = put_blob_direct(&store, &bytes_a).await;
    let digest_b = put_blob_direct(&store, &bytes_b).await;

    // No `OCI-Subject` response => OCI-1.0 => the fallback runs.
    for (tag, digest) in [("v1", &digest_a), ("v2", &digest_b)] {
        Mock::given(method("PUT"))
            .and(path(format!("/v2/{NAMESPACE}/manifests/{tag}")))
            .respond_with(
                ResponseTemplate::new(201)
                    .insert_header(DOCKER_CONTENT_DIGEST, digest.to_string().as_str()),
            )
            .expect(1)
            .mount(&mock_server)
            .await;
    }

    // A stateful fallback endpoint: GET serves what the last PUT stored, so
    // the second merge sees the first descriptor only if the merges serialized.
    let fallback_tag = format!("{}-{}", subject.algorithm(), subject.hash());
    let stored: Arc<Mutex<Vec<serde_json::Value>>> = Arc::new(Mutex::new(Vec::new()));
    let get_stored = stored.clone();
    let get_digest = subject.to_string();
    Mock::given(method("GET"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/{fallback_tag}")))
        .respond_with(move |_: &Request| {
            let manifests = get_stored.lock().unwrap();
            if manifests.is_empty() {
                ResponseTemplate::new(404)
            } else {
                ResponseTemplate::new(200)
                    .insert_header(DOCKER_CONTENT_DIGEST, get_digest.as_str())
                    .set_body_json(json!({
                        "schemaVersion": 2,
                        "mediaType": OCI_INDEX_MEDIA_TYPE,
                        "manifests": manifests.clone(),
                    }))
            }
        })
        .mount(&mock_server)
        .await;
    let put_stored = stored.clone();
    let put_digest = subject.to_string();
    Mock::given(method("PUT"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/{fallback_tag}")))
        .respond_with(move |request: &Request| {
            let index: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
            *put_stored.lock().unwrap() = index["manifests"].as_array().unwrap().clone();
            ResponseTemplate::new(201).insert_header(DOCKER_CONTENT_DIGEST, put_digest.as_str())
        })
        .expect(2)
        .mount(&mock_server)
        .await;

    let client = downstream_client(&mock_server.uri());
    let ctx = PushContext {
        downstream: &client,
        blob_store: &blob_store,
        metadata_store: &metadata_store,
        namespace: NAMESPACE,
        max_concurrent_pushes: 4,
        source_ts: None,
    };
    let (a, b) = tokio::join!(
        push_manifest(&ctx, &digest_a, None, Some("v1"), bytes_a),
        push_manifest(&ctx, &digest_b, None, Some("v2"), bytes_b),
    );
    a.unwrap();
    b.unwrap();

    let merged: Vec<String> = stored
        .lock()
        .unwrap()
        .iter()
        .map(|m| m["digest"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        merged.len(),
        2,
        "both referrer descriptors must survive the merge, got: {merged:?}"
    );
    assert!(merged.contains(&digest_a.to_string()));
    assert!(merged.contains(&digest_b.to_string()));
    drop(mock_server);
}

#[tokio::test]
async fn no_referrers_fallback_when_downstream_indexes_subject() {
    metrics_provider::init_for_tests();
    let mock_server = MockServer::start().await;
    let dir = TempDir::new().unwrap();
    let (blob_store, metadata_store, store) = test_blob_store(dir.path().to_str().unwrap());

    let subject = put_blob_direct(&store, b"subject-bytes").await;
    let manifest = json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "layers": [],
        "subject": {
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "digest": subject.to_string(),
            "size": 13,
        },
    });
    let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
    let manifest_digest = put_blob_direct(&store, &manifest_bytes).await;

    // PUT echoes back `OCI-Subject` => OCI-1.1 downstream => no fallback.
    Mock::given(method("PUT"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
        .respond_with(
            ResponseTemplate::new(201)
                .insert_header(DOCKER_CONTENT_DIGEST, manifest_digest.to_string().as_str())
                .insert_header(OCI_SUBJECT, subject.to_string().as_str()),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    // Any fallback-tag PUT would 404 (no mock) and surface as an error.
    let client = downstream_client(&mock_server.uri());
    let ctx = PushContext {
        downstream: &client,
        blob_store: &blob_store,
        metadata_store: &metadata_store,
        namespace: NAMESPACE,
        max_concurrent_pushes: 4,
        source_ts: None,
    };
    push_manifest(
        &ctx,
        &manifest_digest,
        Some("application/vnd.oci.image.manifest.v1+json".to_string()),
        Some("v1"),
        manifest_bytes,
    )
    .await
    .unwrap();

    drop(mock_server);
}

#[tokio::test]
async fn index_lands_after_its_child_manifest() {
    metrics_provider::init_for_tests();
    let mock_server = MockServer::start().await;
    let dir = TempDir::new().unwrap();
    let (blob_store, metadata_store, store) = test_blob_store(dir.path().to_str().unwrap());

    let child = json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "layers": [],
    });
    let child_bytes = serde_json::to_vec(&child).unwrap();
    let child_digest = put_blob_direct(&store, &child_bytes).await;

    let index = json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.index.v1+json",
        "manifests": [{
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "digest": child_digest.to_string(),
            "size": child_bytes.len(),
        }],
    });
    let index_bytes = serde_json::to_vec(&index).unwrap();
    let index_digest = put_blob_direct(&store, &index_bytes).await;

    let seen = Arc::new(AtomicUsize::new(0));
    let child_at = Arc::new(AtomicUsize::new(usize::MAX));
    let index_at = Arc::new(AtomicUsize::new(usize::MAX));

    // The recursion pushes the child by digest, not by tag.
    Mock::given(method("PUT"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/{child_digest}")))
        .respond_with(OrderRecorder {
            seen: seen.clone(),
            child_at: child_at.clone(),
            index_at: index_at.clone(),
            is_index: false,
            digest: child_digest.to_string(),
        })
        .expect(1)
        .mount(&mock_server)
        .await;
    Mock::given(method("PUT"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
        .respond_with(OrderRecorder {
            seen: seen.clone(),
            child_at: child_at.clone(),
            index_at: index_at.clone(),
            is_index: true,
            digest: index_digest.to_string(),
        })
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = downstream_client(&mock_server.uri());
    let ctx = PushContext {
        downstream: &client,
        blob_store: &blob_store,
        metadata_store: &metadata_store,
        namespace: NAMESPACE,
        max_concurrent_pushes: 4,
        source_ts: None,
    };
    push_manifest(
        &ctx,
        &index_digest,
        Some("application/vnd.oci.image.index.v1+json".to_string()),
        Some("v1"),
        index_bytes,
    )
    .await
    .unwrap();

    assert!(
        child_at.load(Ordering::SeqCst) < index_at.load(Ordering::SeqCst),
        "child manifest must land before the parent index"
    );
    drop(mock_server);
}

#[tokio::test]
async fn index_lands_after_all_children_when_fanned_out() {
    // Children push concurrently; the parent index must still land only
    // after every child, and no child may be dropped by the fan-out.
    metrics_provider::init_for_tests();
    let mock_server = MockServer::start().await;
    let dir = TempDir::new().unwrap();
    let (blob_store, metadata_store, store) = test_blob_store(dir.path().to_str().unwrap());

    let mut child_digests = Vec::new();
    let mut manifests = Vec::new();
    for i in 0..3 {
        let child = json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "layers": [],
            "annotations": { "idx": i.to_string() },
        });
        let child_bytes = serde_json::to_vec(&child).unwrap();
        let child_digest = put_blob_direct(&store, &child_bytes).await;
        Mock::given(method("PUT"))
            .and(path(format!("/v2/{NAMESPACE}/manifests/{child_digest}")))
            .respond_with(ResponseTemplate::new(201))
            .expect(1)
            .mount(&mock_server)
            .await;
        manifests.push(json!({
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "digest": child_digest.to_string(),
            "size": child_bytes.len(),
        }));
        child_digests.push(child_digest);
    }

    let index = json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.index.v1+json",
        "manifests": manifests,
    });
    let index_bytes = serde_json::to_vec(&index).unwrap();
    let index_digest = put_blob_direct(&store, &index_bytes).await;
    Mock::given(method("PUT"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
        .respond_with(ResponseTemplate::new(201))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = downstream_client(&mock_server.uri());
    let ctx = PushContext {
        downstream: &client,
        blob_store: &blob_store,
        metadata_store: &metadata_store,
        namespace: NAMESPACE,
        max_concurrent_pushes: 4,
        source_ts: None,
    };
    push_manifest(
        &ctx,
        &index_digest,
        Some("application/vnd.oci.image.index.v1+json".to_string()),
        Some("v1"),
        index_bytes,
    )
    .await
    .unwrap();

    // The manifest PUTs, in arrival order: the index must come last.
    let puts: Vec<String> = mock_server
        .received_requests()
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|r| r.method.as_str() == "PUT")
        .map(|r| r.url.path().to_string())
        .collect();
    let index_pos = puts.iter().position(|p| p.ends_with("/manifests/v1"));
    assert_eq!(
        index_pos,
        Some(puts.len() - 1),
        "the index must PUT after every child"
    );
    for child in &child_digests {
        assert!(
            puts.iter().any(|p| p.ends_with(&child.to_string())),
            "every child manifest must be pushed"
        );
    }
    drop(mock_server);
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn push_blob_mounts_cross_repo_when_sibling_namespace_holds_it() {
    // A sibling namespace already referencing the blobs becomes the mount `from`.
    const SIBLING: &str = "other/repo";

    metrics_provider::init_for_tests();
    let mock_server = MockServer::start().await;
    let dir = TempDir::new().unwrap();
    let (blob_store, metadata_store, store) = test_blob_store(dir.path().to_str().unwrap());

    let config = put_blob_direct(&store, br#"{"c":1}"#).await;
    let layer = put_blob_direct(&store, b"layer-bytes").await;
    let manifest = json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": config.to_string(),
            "size": 7,
        },
        "layers": [{
            "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
            "digest": layer.to_string(),
            "size": 11,
        }],
    });
    let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
    let manifest_digest = put_blob_direct(&store, &manifest_bytes).await;

    // Record the sibling's ownership so a mount `from` exists.
    for blob in [&config, &layer] {
        metadata_store
            .update_blob_index(
                SIBLING,
                blob,
                BlobIndexOperation::Insert(LinkKind::Blob((*blob).clone())),
            )
            .await
            .unwrap();
    }

    // Both blobs missing in the target namespace => mount attempt.
    for blob in [&config, &layer] {
        Mock::given(method("HEAD"))
            .and(path(format!("/v2/{NAMESPACE}/blobs/{blob}")))
            .respond_with(ResponseTemplate::new(404))
            .mount(&mock_server)
            .await;
    }
    // No PATCH/PUT upload mock is mounted, so a fall-back transfer would 404
    // and fail the push, proving the bytes were never streamed.
    Mock::given(method("POST"))
        .and(path(format!("/v2/{NAMESPACE}/blobs/uploads/")))
        .and(query_param("from", SIBLING))
        .respond_with(
            ResponseTemplate::new(201)
                .insert_header("Location", format!("/v2/{NAMESPACE}/blobs/mounted")),
        )
        .expect(2)
        .mount(&mock_server)
        .await;
    Mock::given(method("PUT"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
        .respond_with(
            ResponseTemplate::new(201)
                .insert_header(DOCKER_CONTENT_DIGEST, manifest_digest.to_string().as_str()),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = downstream_client(&mock_server.uri());
    let ctx = PushContext {
        downstream: &client,
        blob_store: &blob_store,
        metadata_store: &metadata_store,
        namespace: NAMESPACE,
        max_concurrent_pushes: 4,
        source_ts: None,
    };
    push_manifest(
        &ctx,
        &manifest_digest,
        Some("application/vnd.oci.image.manifest.v1+json".to_string()),
        Some("v1"),
        manifest_bytes,
    )
    .await
    .expect("both blobs must mount cross-repo and the manifest must push");

    drop(mock_server);
}

#[tokio::test]
async fn push_blob_falls_back_to_upload_when_mount_is_rejected() {
    const SIBLING: &str = "other/repo";

    metrics_provider::init_for_tests();
    let mock_server = MockServer::start().await;
    let dir = TempDir::new().unwrap();
    let (blob_store, metadata_store, store) = test_blob_store(dir.path().to_str().unwrap());

    let config = put_blob_direct(&store, br#"{"c":1}"#).await;
    let manifest = json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": config.to_string(),
            "size": 7,
        },
        "layers": [],
    });
    let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
    let manifest_digest = put_blob_direct(&store, &manifest_bytes).await;

    metadata_store
        .update_blob_index(
            SIBLING,
            &config,
            BlobIndexOperation::Insert(LinkKind::Blob(config.clone())),
        )
        .await
        .unwrap();

    // Blob missing in the target => mount attempt.
    Mock::given(method("HEAD"))
        .and(path(format!("/v2/{NAMESPACE}/blobs/{config}")))
        .respond_with(ResponseTemplate::new(404))
        .mount(&mock_server)
        .await;
    // Reject the mount POST (it carries `from`) with 403 and open a session
    // for the plain fall-back POST; `.expect(2)` asserts both happen.
    let session = format!("/v2/{NAMESPACE}/blobs/uploads/s");
    Mock::given(method("POST"))
        .and(path(format!("/v2/{NAMESPACE}/blobs/uploads/")))
        .respond_with(move |req: &Request| {
            if req.url.query_pairs().any(|(k, _)| k == "from") {
                ResponseTemplate::new(403)
            } else {
                ResponseTemplate::new(202).insert_header("Location", session.as_str())
            }
        })
        .expect(2)
        .mount(&mock_server)
        .await;
    Mock::given(method("PATCH"))
        .and(path(format!("/v2/{NAMESPACE}/blobs/uploads/s")))
        .respond_with(
            ResponseTemplate::new(202)
                .insert_header("Location", format!("/v2/{NAMESPACE}/blobs/uploads/s")),
        )
        .expect(1)
        .mount(&mock_server)
        .await;
    Mock::given(method("PUT"))
        .and(path(format!("/v2/{NAMESPACE}/blobs/uploads/s")))
        .respond_with(ResponseTemplate::new(201))
        .expect(1)
        .mount(&mock_server)
        .await;
    Mock::given(method("PUT"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
        .respond_with(
            ResponseTemplate::new(201)
                .insert_header(DOCKER_CONTENT_DIGEST, manifest_digest.to_string().as_str()),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = downstream_client(&mock_server.uri());
    let ctx = PushContext {
        downstream: &client,
        blob_store: &blob_store,
        metadata_store: &metadata_store,
        namespace: NAMESPACE,
        max_concurrent_pushes: 4,
        source_ts: None,
    };
    push_manifest(
        &ctx,
        &manifest_digest,
        Some("application/vnd.oci.image.manifest.v1+json".to_string()),
        Some("v1"),
        manifest_bytes,
    )
    .await
    .expect("a rejected mount must fall back to a normal upload");

    drop(mock_server);
}

/// Seeds a minimal blob-less image manifest locally, returning its digest
/// and serialized body.
async fn seed_blobless_manifest(store: &Arc<Store>) -> (Digest, Vec<u8>) {
    let manifest = json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "layers": [],
    });
    let bytes = serde_json::to_vec(&manifest).unwrap();
    let digest = put_blob_direct(store, &bytes).await;
    (digest, bytes)
}

/// An OCI error envelope body with the given `code`.
fn oci_error_body(code: &str) -> serde_json::Value {
    json!({ "errors": [{ "code": code, "message": "rejected" }] })
}

#[tokio::test]
async fn push_manifest_stamps_source_timestamp_header() {
    metrics_provider::init_for_tests();
    let mock_server = MockServer::start().await;
    let dir = TempDir::new().unwrap();
    let (blob_store, metadata_store, store) = test_blob_store(dir.path().to_str().unwrap());
    let (manifest_digest, manifest_bytes) = seed_blobless_manifest(&store).await;

    Mock::given(method("PUT"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
        .and(header(X_ANGOS_SOURCE_TIMESTAMP, "2026-06-03T00:00:00Z"))
        .respond_with(
            ResponseTemplate::new(201)
                .insert_header(DOCKER_CONTENT_DIGEST, manifest_digest.to_string().as_str()),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = downstream_client(&mock_server.uri());
    let ctx = PushContext {
        downstream: &client,
        blob_store: &blob_store,
        metadata_store: &metadata_store,
        namespace: NAMESPACE,
        max_concurrent_pushes: 4,
        source_ts: Some("2026-06-03T00:00:00Z"),
    };
    push_manifest(
        &ctx,
        &manifest_digest,
        Some("application/vnd.oci.image.manifest.v1+json".to_string()),
        Some("v1"),
        manifest_bytes,
    )
    .await
    .unwrap();

    drop(mock_server);
}

#[tokio::test]
async fn push_manifest_skips_put_when_downstream_already_converged() {
    // No manifest PUT mock is mounted, so a wrongly-issued PUT would 404 and
    // fail the push.
    metrics_provider::init_for_tests();
    let mock_server = MockServer::start().await;
    let dir = TempDir::new().unwrap();
    let (blob_store, metadata_store, store) = test_blob_store(dir.path().to_str().unwrap());
    let (manifest_digest, manifest_bytes) = seed_blobless_manifest(&store).await;

    Mock::given(method("HEAD"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header(DOCKER_CONTENT_DIGEST, manifest_digest.to_string().as_str())
                .insert_header("Content-Length", manifest_bytes.len().to_string().as_str()),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = downstream_client(&mock_server.uri());
    let ctx = PushContext {
        downstream: &client,
        blob_store: &blob_store,
        metadata_store: &metadata_store,
        namespace: NAMESPACE,
        max_concurrent_pushes: 4,
        source_ts: None,
    };
    let outcome = push_manifest(
        &ctx,
        &manifest_digest,
        Some("application/vnd.oci.image.manifest.v1+json".to_string()),
        Some("v1"),
        manifest_bytes,
    )
    .await
    .expect("a converged downstream must skip the PUT and succeed");
    assert_eq!(
        outcome,
        PushOutcome::Converged,
        "a HEAD-matched skip must report Converged, not Pushed, so the metric distinguishes a no-op"
    );

    drop(mock_server);
}

#[tokio::test]
async fn repeated_layer_digest_uploads_the_blob_once() {
    // Without dedup both entries HEAD-miss concurrently and both upload;
    // every mock is pinned to `.expect(1)`.
    metrics_provider::init_for_tests();
    let mock_server = MockServer::start().await;
    let dir = TempDir::new().unwrap();
    let (blob_store, metadata_store, store) = test_blob_store(dir.path().to_str().unwrap());

    let layer = put_blob_direct(&store, b"twice-listed layer").await;
    let manifest = json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "layers": [
            { "mediaType": "application/vnd.oci.image.layer.v1.tar", "digest": layer.to_string(), "size": 18 },
            { "mediaType": "application/vnd.oci.image.layer.v1.tar", "digest": layer.to_string(), "size": 18 },
        ],
    });
    let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
    let manifest_digest = put_blob_direct(&store, &manifest_bytes).await;

    Mock::given(method("HEAD"))
        .and(path(format!("/v2/{NAMESPACE}/blobs/{layer}")))
        .respond_with(ResponseTemplate::new(404))
        .expect(1)
        .mount(&mock_server)
        .await;
    Mock::given(method("POST"))
        .and(path(format!("/v2/{NAMESPACE}/blobs/uploads/")))
        .respond_with(
            ResponseTemplate::new(202)
                .insert_header("Location", format!("/v2/{NAMESPACE}/blobs/uploads/s")),
        )
        .expect(1)
        .mount(&mock_server)
        .await;
    Mock::given(method("PATCH"))
        .and(path(format!("/v2/{NAMESPACE}/blobs/uploads/s")))
        .respond_with(
            ResponseTemplate::new(202)
                .insert_header("Location", format!("/v2/{NAMESPACE}/blobs/uploads/s")),
        )
        .expect(1)
        .mount(&mock_server)
        .await;
    Mock::given(method("PUT"))
        .and(path(format!("/v2/{NAMESPACE}/blobs/uploads/s")))
        .respond_with(ResponseTemplate::new(201))
        .expect(1)
        .mount(&mock_server)
        .await;
    Mock::given(method("PUT"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
        .respond_with(
            ResponseTemplate::new(201)
                .insert_header(DOCKER_CONTENT_DIGEST, manifest_digest.to_string().as_str()),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = downstream_client(&mock_server.uri());
    let ctx = PushContext {
        downstream: &client,
        blob_store: &blob_store,
        metadata_store: &metadata_store,
        namespace: NAMESPACE,
        max_concurrent_pushes: 4,
        source_ts: None,
    };
    push_manifest(&ctx, &manifest_digest, None, Some("v1"), manifest_bytes)
        .await
        .expect("a manifest repeating a layer digest must push it once");

    drop(mock_server);
}

#[tokio::test]
async fn converged_skip_head_sends_standard_accept_headers() {
    // Without an `Accept` header a content-negotiating downstream may return
    // a converted representation whose digest never matches the local one,
    // so the converged skip would never fire.
    metrics_provider::init_for_tests();
    let mock_server = MockServer::start().await;
    let dir = TempDir::new().unwrap();
    let (blob_store, metadata_store, store) = test_blob_store(dir.path().to_str().unwrap());
    let (manifest_digest, manifest_bytes) = seed_blobless_manifest(&store).await;

    let digest_str = manifest_digest.to_string();
    let body_len = manifest_bytes.len();
    Mock::given(method("HEAD"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
        .respond_with(move |request: &Request| {
            let accepts: Vec<&str> = request
                .headers
                .get_all("accept")
                .iter()
                .map(|v| v.to_str().unwrap())
                .collect();
            for expected in [
                OCI_MANIFEST_MEDIA_TYPE,
                OCI_INDEX_MEDIA_TYPE,
                DOCKER_MANIFEST_MEDIA_TYPE,
                DOCKER_MANIFEST_LIST_MEDIA_TYPE,
            ] {
                assert!(
                    accepts.contains(&expected),
                    "manifest probe must accept '{expected}', got: {accepts:?}"
                );
            }
            ResponseTemplate::new(200)
                .insert_header(DOCKER_CONTENT_DIGEST, digest_str.as_str())
                .insert_header("Content-Length", body_len.to_string().as_str())
        })
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = downstream_client(&mock_server.uri());
    let ctx = PushContext {
        downstream: &client,
        blob_store: &blob_store,
        metadata_store: &metadata_store,
        namespace: NAMESPACE,
        max_concurrent_pushes: 4,
        source_ts: None,
    };
    let outcome = push_manifest(
        &ctx,
        &manifest_digest,
        Some("application/vnd.oci.image.manifest.v1+json".to_string()),
        Some("v1"),
        manifest_bytes,
    )
    .await
    .expect("the Accept-stamped HEAD must still drive the converged skip");
    assert_eq!(outcome, PushOutcome::Converged);
    drop(mock_server);
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn converged_manifest_with_blobs_sends_exactly_one_head() {
    // The converged skip runs before child recursion and the blob sweep,
    // so a redelivered already-converged manifest costs one manifest HEAD:
    // zero blob HEADs, zero uploads, zero PUTs.
    metrics_provider::init_for_tests();
    let mock_server = MockServer::start().await;
    let dir = TempDir::new().unwrap();
    let (blob_store, metadata_store, store) = test_blob_store(dir.path().to_str().unwrap());

    let config = put_blob_direct(&store, br#"{"c":1}"#).await;
    let layer_a = put_blob_direct(&store, b"layer-a-bytes").await;
    let layer_b = put_blob_direct(&store, b"layer-b-bytes").await;
    let manifest = json!({
        "schemaVersion": 2,
        "mediaType": OCI_MANIFEST_MEDIA_TYPE,
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": config.to_string(),
            "size": 7,
        },
        "layers": [
            {
                "mediaType": "application/vnd.oci.image.layer.v1.tar",
                "digest": layer_a.to_string(),
                "size": 13,
            },
            {
                "mediaType": "application/vnd.oci.image.layer.v1.tar",
                "digest": layer_b.to_string(),
                "size": 13,
            },
        ],
    });
    let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
    let manifest_digest = put_blob_direct(&store, &manifest_bytes).await;

    // The converged probe is the only request allowed to reach the downstream.
    Mock::given(method("HEAD"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header(DOCKER_CONTENT_DIGEST, manifest_digest.to_string().as_str())
                .insert_header("Content-Length", manifest_bytes.len().to_string().as_str()),
        )
        .expect(1)
        .mount(&mock_server)
        .await;
    for blob in [&config, &layer_a, &layer_b] {
        Mock::given(method("HEAD"))
            .and(path(format!("/v2/{NAMESPACE}/blobs/{blob}")))
            .respond_with(ResponseTemplate::new(404))
            .expect(0)
            .mount(&mock_server)
            .await;
    }
    Mock::given(method("POST"))
        .and(path(format!("/v2/{NAMESPACE}/blobs/uploads/")))
        .respond_with(ResponseTemplate::new(202))
        .expect(0)
        .mount(&mock_server)
        .await;
    Mock::given(method("PUT"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
        .respond_with(ResponseTemplate::new(201))
        .expect(0)
        .mount(&mock_server)
        .await;

    let client = downstream_client(&mock_server.uri());
    let ctx = PushContext {
        downstream: &client,
        blob_store: &blob_store,
        metadata_store: &metadata_store,
        namespace: NAMESPACE,
        max_concurrent_pushes: 4,
        source_ts: None,
    };
    let outcome = push_manifest(
        &ctx,
        &manifest_digest,
        Some(OCI_MANIFEST_MEDIA_TYPE.to_string()),
        Some("v1"),
        manifest_bytes,
    )
    .await
    .expect("a converged manifest must skip the blob sweep and the PUT");
    assert_eq!(
        outcome,
        PushOutcome::Converged,
        "a digest-matching HEAD before the blob sweep must converge"
    );
    drop(mock_server);
}

#[tokio::test]
async fn converged_child_skips_its_own_put_inside_index_recursion() {
    // Each recursed child runs its own converged HEAD-skip: a child the
    // downstream already holds is not re-PUT while the index still lands.
    metrics_provider::init_for_tests();
    let mock_server = MockServer::start().await;
    let dir = TempDir::new().unwrap();
    let (blob_store, metadata_store, store) = test_blob_store(dir.path().to_str().unwrap());

    let child = json!({
        "schemaVersion": 2,
        "mediaType": OCI_MANIFEST_MEDIA_TYPE,
        "layers": [],
    });
    let child_bytes = serde_json::to_vec(&child).unwrap();
    let child_digest = put_blob_direct(&store, &child_bytes).await;
    let index = json!({
        "schemaVersion": 2,
        "mediaType": OCI_INDEX_MEDIA_TYPE,
        "manifests": [{
            "mediaType": OCI_MANIFEST_MEDIA_TYPE,
            "digest": child_digest.to_string(),
            "size": child_bytes.len(),
        }],
    });
    let index_bytes = serde_json::to_vec(&index).unwrap();
    let index_digest = put_blob_direct(&store, &index_bytes).await;

    // The index probe misses (404), the child probe hits (converged).
    Mock::given(method("HEAD"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
        .respond_with(ResponseTemplate::new(404))
        .expect(1)
        .mount(&mock_server)
        .await;
    Mock::given(method("HEAD"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/{child_digest}")))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header(DOCKER_CONTENT_DIGEST, child_digest.to_string().as_str())
                .insert_header("Content-Length", child_bytes.len().to_string().as_str()),
        )
        .expect(1)
        .mount(&mock_server)
        .await;
    Mock::given(method("PUT"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/{child_digest}")))
        .respond_with(ResponseTemplate::new(201))
        .expect(0)
        .mount(&mock_server)
        .await;
    Mock::given(method("PUT"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
        .respond_with(
            ResponseTemplate::new(201)
                .insert_header(DOCKER_CONTENT_DIGEST, index_digest.to_string().as_str()),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = downstream_client(&mock_server.uri());
    let ctx = PushContext {
        downstream: &client,
        blob_store: &blob_store,
        metadata_store: &metadata_store,
        namespace: NAMESPACE,
        max_concurrent_pushes: 4,
        source_ts: None,
    };
    let outcome = push_manifest(
        &ctx,
        &index_digest,
        Some(OCI_INDEX_MEDIA_TYPE.to_string()),
        Some("v1"),
        index_bytes,
    )
    .await
    .expect("a converged child must skip its PUT while the index still lands");
    assert_eq!(outcome, PushOutcome::Pushed);
    drop(mock_server);
}

#[tokio::test]
async fn blob_head_503_fails_the_push_without_upload_attempt() {
    // A transient blob-probe failure must fail the push so the job
    // retries; treating it as absent would start a pointless full upload.
    metrics_provider::init_for_tests();
    let mock_server = MockServer::start().await;
    let dir = TempDir::new().unwrap();
    let (blob_store, metadata_store, store) = test_blob_store(dir.path().to_str().unwrap());

    let config = put_blob_direct(&store, br#"{"c":1}"#).await;
    let manifest = json!({
        "schemaVersion": 2,
        "mediaType": OCI_MANIFEST_MEDIA_TYPE,
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": config.to_string(),
            "size": 7,
        },
        "layers": [],
    });
    let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
    let manifest_digest = put_blob_direct(&store, &manifest_bytes).await;

    Mock::given(method("HEAD"))
        .and(path(format!("/v2/{NAMESPACE}/blobs/{config}")))
        .respond_with(ResponseTemplate::new(503))
        .expect(1)
        .mount(&mock_server)
        .await;
    Mock::given(method("POST"))
        .and(path(format!("/v2/{NAMESPACE}/blobs/uploads/")))
        .respond_with(ResponseTemplate::new(202))
        .expect(0)
        .mount(&mock_server)
        .await;
    Mock::given(method("PUT"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
        .respond_with(ResponseTemplate::new(201))
        .expect(0)
        .mount(&mock_server)
        .await;

    let client = downstream_client(&mock_server.uri());
    let ctx = PushContext {
        downstream: &client,
        blob_store: &blob_store,
        metadata_store: &metadata_store,
        namespace: NAMESPACE,
        max_concurrent_pushes: 4,
        source_ts: None,
    };
    let result = push_manifest(
        &ctx,
        &manifest_digest,
        Some(OCI_MANIFEST_MEDIA_TYPE.to_string()),
        Some("v1"),
        manifest_bytes,
    )
    .await;

    assert!(
        result.is_err(),
        "a 503 blob HEAD must fail the push (retryable), got: {result:?}"
    );
    drop(mock_server);
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn failed_patch_cancels_the_upload_session() {
    // A PATCH failure must best-effort DELETE the open session exactly
    // once and still propagate the original upload error.
    metrics_provider::init_for_tests();
    let mock_server = MockServer::start().await;
    let dir = TempDir::new().unwrap();
    let (blob_store, metadata_store, store) = test_blob_store(dir.path().to_str().unwrap());

    let config = put_blob_direct(&store, br#"{"c":1}"#).await;
    let manifest = json!({
        "schemaVersion": 2,
        "mediaType": OCI_MANIFEST_MEDIA_TYPE,
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": config.to_string(),
            "size": 7,
        },
        "layers": [],
    });
    let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
    let manifest_digest = put_blob_direct(&store, &manifest_bytes).await;

    Mock::given(method("HEAD"))
        .and(path(format!("/v2/{NAMESPACE}/blobs/{config}")))
        .respond_with(ResponseTemplate::new(404))
        .expect(1)
        .mount(&mock_server)
        .await;
    Mock::given(method("POST"))
        .and(path(format!("/v2/{NAMESPACE}/blobs/uploads/")))
        .respond_with(
            ResponseTemplate::new(202)
                .insert_header("Location", format!("/v2/{NAMESPACE}/blobs/uploads/s")),
        )
        .expect(1)
        .mount(&mock_server)
        .await;
    Mock::given(method("PATCH"))
        .and(path(format!("/v2/{NAMESPACE}/blobs/uploads/s")))
        .respond_with(ResponseTemplate::new(500))
        .expect(1)
        .mount(&mock_server)
        .await;
    Mock::given(method("DELETE"))
        .and(path(format!("/v2/{NAMESPACE}/blobs/uploads/s")))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock_server)
        .await;
    Mock::given(method("PUT"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
        .respond_with(ResponseTemplate::new(201))
        .expect(0)
        .mount(&mock_server)
        .await;

    let client = downstream_client(&mock_server.uri());
    let ctx = PushContext {
        downstream: &client,
        blob_store: &blob_store,
        metadata_store: &metadata_store,
        namespace: NAMESPACE,
        max_concurrent_pushes: 4,
        source_ts: None,
    };
    let result = push_manifest(
        &ctx,
        &manifest_digest,
        Some(OCI_MANIFEST_MEDIA_TYPE.to_string()),
        Some("v1"),
        manifest_bytes,
    )
    .await;

    assert!(
        result.is_err(),
        "the original PATCH failure must propagate past the session cancel, got: {result:?}"
    );
    drop(mock_server);
}

#[tokio::test]
async fn converged_subject_manifest_still_pushes_referrers_fallback() {
    // A prior attempt's primary PUT can land while its fallback PUT fails;
    // a blanket converged-skip would then never retry the fallback,
    // stranding the referrer.
    metrics_provider::init_for_tests();
    let mock_server = MockServer::start().await;
    let dir = TempDir::new().unwrap();
    let (blob_store, metadata_store, store) = test_blob_store(dir.path().to_str().unwrap());

    let subject = put_blob_direct(&store, b"subject-bytes").await;
    // Config-less manifest, so no blob mocks are needed.
    let manifest = json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "layers": [],
        "subject": {
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "digest": subject.to_string(),
            "size": 13,
        },
    });
    let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
    let manifest_digest = put_blob_direct(&store, &manifest_bytes).await;

    // The HEAD reports the converged state, but a subject-bearing manifest
    // bypasses the skip, so this mock carries no `.expect()`.
    Mock::given(method("HEAD"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header(DOCKER_CONTENT_DIGEST, manifest_digest.to_string().as_str())
                .insert_header("Content-Length", manifest_bytes.len().to_string().as_str()),
        )
        .mount(&mock_server)
        .await;

    // The re-issued primary PUT returns no `OCI-Subject` (OCI-1.0 downstream).
    Mock::given(method("PUT"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
        .respond_with(
            ResponseTemplate::new(201)
                .insert_header(DOCKER_CONTENT_DIGEST, manifest_digest.to_string().as_str()),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let fallback_tag = format!("{}-{}", subject.algorithm(), subject.hash());
    Mock::given(method("GET"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/{fallback_tag}")))
        .respond_with(ResponseTemplate::new(404))
        .mount(&mock_server)
        .await;
    Mock::given(method("PUT"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/{fallback_tag}")))
        .respond_with(ResponseTemplate::new(201))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = downstream_client(&mock_server.uri());
    let ctx = PushContext {
        downstream: &client,
        blob_store: &blob_store,
        metadata_store: &metadata_store,
        namespace: NAMESPACE,
        max_concurrent_pushes: 4,
        source_ts: None,
    };
    push_manifest(&ctx, &manifest_digest, None, Some("v1"), manifest_bytes)
        .await
        .expect("a converged subject-bearing manifest must re-push the referrers fallback");

    drop(mock_server);
}

#[tokio::test]
async fn push_manifest_puts_when_downstream_holds_a_different_digest() {
    // The PUT must still run so receiver-side LWW can arbitrate the divergence.
    metrics_provider::init_for_tests();
    let mock_server = MockServer::start().await;
    let dir = TempDir::new().unwrap();
    let (blob_store, metadata_store, store) = test_blob_store(dir.path().to_str().unwrap());
    let (manifest_digest, manifest_bytes) = seed_blobless_manifest(&store).await;

    let other_digest = "sha256:1111111111111111111111111111111111111111111111111111111111111111";
    Mock::given(method("HEAD"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header(DOCKER_CONTENT_DIGEST, other_digest)
                .insert_header("Content-Length", "2"),
        )
        .mount(&mock_server)
        .await;
    Mock::given(method("PUT"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
        .respond_with(
            ResponseTemplate::new(201)
                .insert_header(DOCKER_CONTENT_DIGEST, manifest_digest.to_string().as_str()),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = downstream_client(&mock_server.uri());
    let ctx = PushContext {
        downstream: &client,
        blob_store: &blob_store,
        metadata_store: &metadata_store,
        namespace: NAMESPACE,
        max_concurrent_pushes: 4,
        source_ts: None,
    };
    push_manifest(
        &ctx,
        &manifest_digest,
        Some("application/vnd.oci.image.manifest.v1+json".to_string()),
        Some("v1"),
        manifest_bytes,
    )
    .await
    .expect("a divergent downstream digest must still PUT");

    drop(mock_server);
}

#[tokio::test]
async fn push_manifest_puts_when_downstream_head_returns_404() {
    // The probe must fail open: no HEAD mock is mounted, so wiremock answers
    // 404 and the PUT must still run.
    metrics_provider::init_for_tests();
    let mock_server = MockServer::start().await;
    let dir = TempDir::new().unwrap();
    let (blob_store, metadata_store, store) = test_blob_store(dir.path().to_str().unwrap());
    let (manifest_digest, manifest_bytes) = seed_blobless_manifest(&store).await;

    Mock::given(method("PUT"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
        .respond_with(
            ResponseTemplate::new(201)
                .insert_header(DOCKER_CONTENT_DIGEST, manifest_digest.to_string().as_str()),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = downstream_client(&mock_server.uri());
    let ctx = PushContext {
        downstream: &client,
        blob_store: &blob_store,
        metadata_store: &metadata_store,
        namespace: NAMESPACE,
        max_concurrent_pushes: 4,
        source_ts: None,
    };
    push_manifest(
        &ctx,
        &manifest_digest,
        Some("application/vnd.oci.image.manifest.v1+json".to_string()),
        Some("v1"),
        manifest_bytes,
    )
    .await
    .expect("a 404 HEAD must fall through to the PUT");

    drop(mock_server);
}

#[tokio::test]
async fn push_manifest_recovers_content_type_from_the_link_for_a_typeless_body() {
    // Production passes `None` as the override and a body may omit
    // `mediaType`; without the link recovery the PUT would carry no
    // `Content-Type` and the receiver rejects it 400.
    metrics_provider::init_for_tests();
    let mock_server = MockServer::start().await;
    let dir = TempDir::new().unwrap();
    let (blob_store, metadata_store, store) = test_blob_store(dir.path().to_str().unwrap());

    let manifest = json!({ "schemaVersion": 2, "layers": [] });
    let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
    let manifest_digest = put_blob_direct(&store, &manifest_bytes).await;

    // Seed the revision link with the type `store_manifest` records from
    // the original push's `Content-Type`.
    let media_type = "application/vnd.oci.image.manifest.v1+json";
    metadata_store
        .update_links(
            NAMESPACE,
            &[LinkOperation::create_with_media_type(
                LinkKind::Digest(manifest_digest.clone()),
                manifest_digest.clone(),
                Some(media_type.to_string()),
            )],
        )
        .await
        .unwrap();

    Mock::given(method("PUT"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
        .and(header("Content-Type", media_type))
        .respond_with(
            ResponseTemplate::new(201)
                .insert_header(DOCKER_CONTENT_DIGEST, manifest_digest.to_string().as_str()),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = downstream_client(&mock_server.uri());
    let ctx = PushContext {
        downstream: &client,
        blob_store: &blob_store,
        metadata_store: &metadata_store,
        namespace: NAMESPACE,
        max_concurrent_pushes: 4,
        source_ts: None,
    };
    push_manifest(&ctx, &manifest_digest, None, Some("v1"), manifest_bytes)
        .await
        .expect("a typeless body must recover its Content-Type from the revision link");

    drop(mock_server);
}

#[tokio::test]
async fn push_index_recovers_typeless_child_content_type_from_link() {
    // Child-recursion counterpart of the test above: a typeless child pushed
    // by digest must recover its Content-Type from its own revision link.
    metrics_provider::init_for_tests();
    let mock_server = MockServer::start().await;
    let dir = TempDir::new().unwrap();
    let (blob_store, metadata_store, store) = test_blob_store(dir.path().to_str().unwrap());

    let child = json!({ "schemaVersion": 2, "layers": [] });
    let child_bytes = serde_json::to_vec(&child).unwrap();
    let child_digest = put_blob_direct(&store, &child_bytes).await;
    let index = json!({
        "schemaVersion": 2,
        "mediaType": OCI_INDEX_MEDIA_TYPE,
        "manifests": [{
            "mediaType": OCI_MANIFEST_MEDIA_TYPE,
            "digest": child_digest.to_string(),
            "size": child_bytes.len(),
        }],
    });
    let index_bytes = serde_json::to_vec(&index).unwrap();
    let index_digest = put_blob_direct(&store, &index_bytes).await;

    // Seed only the child's revision link with its stored media type.
    metadata_store
        .update_links(
            NAMESPACE,
            &[LinkOperation::create_with_media_type(
                LinkKind::Digest(child_digest.clone()),
                child_digest.clone(),
                Some(OCI_MANIFEST_MEDIA_TYPE.to_string()),
            )],
        )
        .await
        .unwrap();

    Mock::given(method("PUT"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/{child_digest}")))
        .and(header("Content-Type", OCI_MANIFEST_MEDIA_TYPE))
        .respond_with(
            ResponseTemplate::new(201)
                .insert_header(DOCKER_CONTENT_DIGEST, child_digest.to_string().as_str()),
        )
        .expect(1)
        .mount(&mock_server)
        .await;
    Mock::given(method("PUT"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
        .and(header("Content-Type", OCI_INDEX_MEDIA_TYPE))
        .respond_with(
            ResponseTemplate::new(201)
                .insert_header(DOCKER_CONTENT_DIGEST, index_digest.to_string().as_str()),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = downstream_client(&mock_server.uri());
    let ctx = PushContext {
        downstream: &client,
        blob_store: &blob_store,
        metadata_store: &metadata_store,
        namespace: NAMESPACE,
        max_concurrent_pushes: 4,
        source_ts: None,
    };
    push_manifest(&ctx, &index_digest, None, Some("v1"), index_bytes)
        .await
        .expect("a typeless child must recover its Content-Type and the index must land");

    drop(mock_server);
}

#[tokio::test]
async fn push_manifest_treats_lww_superseded_409_as_success() {
    metrics_provider::init_for_tests();
    let mock_server = MockServer::start().await;
    let dir = TempDir::new().unwrap();
    let (blob_store, metadata_store, store) = test_blob_store(dir.path().to_str().unwrap());
    let (manifest_digest, manifest_bytes) = seed_blobless_manifest(&store).await;

    Mock::given(method("PUT"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
        .respond_with(
            ResponseTemplate::new(409).set_body_json(oci_error_body(REPLICATION_SUPERSEDED_CODE)),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = downstream_client(&mock_server.uri());
    let ctx = PushContext {
        downstream: &client,
        blob_store: &blob_store,
        metadata_store: &metadata_store,
        namespace: NAMESPACE,
        max_concurrent_pushes: 4,
        source_ts: Some("2026-06-03T00:00:00Z"),
    };
    push_manifest(
        &ctx,
        &manifest_digest,
        Some("application/vnd.oci.image.manifest.v1+json".to_string()),
        Some("v1"),
        manifest_bytes,
    )
    .await
    .expect("an LWW-superseded 409 must be treated as success");

    drop(mock_server);
}

#[tokio::test]
async fn push_manifest_propagates_immutable_409_as_error() {
    metrics_provider::init_for_tests();
    let mock_server = MockServer::start().await;
    let dir = TempDir::new().unwrap();
    let (blob_store, metadata_store, store) = test_blob_store(dir.path().to_str().unwrap());
    let (manifest_digest, manifest_bytes) = seed_blobless_manifest(&store).await;

    // A 409 with the immutable-tag `CONFLICT` code is not an LWW loss.
    Mock::given(method("PUT"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
        .respond_with(ResponseTemplate::new(409).set_body_json(oci_error_body("CONFLICT")))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = downstream_client(&mock_server.uri());
    let ctx = PushContext {
        downstream: &client,
        blob_store: &blob_store,
        metadata_store: &metadata_store,
        namespace: NAMESPACE,
        max_concurrent_pushes: 4,
        source_ts: None,
    };
    let result = push_manifest(
        &ctx,
        &manifest_digest,
        Some("application/vnd.oci.image.manifest.v1+json".to_string()),
        Some("v1"),
        manifest_bytes,
    )
    .await;
    assert!(
        result.is_err(),
        "a non-superseded 409 (immutable conflict) must propagate as an error"
    );

    drop(mock_server);
}

#[tokio::test]
async fn delete_manifest_stamps_header_and_distinguishes_superseded() {
    metrics_provider::init_for_tests();
    let mock_server = MockServer::start().await;

    Mock::given(method("DELETE"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
        .and(header(X_ANGOS_SOURCE_TIMESTAMP, "2026-06-03T00:00:00Z"))
        .respond_with(
            ResponseTemplate::new(409).set_body_json(oci_error_body(REPLICATION_SUPERSEDED_CODE)),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let dir = TempDir::new().unwrap();
    let (_, metadata_store, _) = test_blob_store(dir.path().to_str().unwrap());
    delete_manifest(
        &downstream_client(&mock_server.uri()),
        &metadata_store,
        NAMESPACE,
        &Reference::Tag(Tag::new("v1").unwrap()),
        Some("2026-06-03T00:00:00Z"),
    )
    .await
    .expect("an LWW-superseded delete-409 must be treated as success");

    drop(mock_server);
}

#[tokio::test]
async fn delete_manifest_of_absent_target_is_converged_not_pushed() {
    metrics_provider::init_for_tests();
    let mock_server = MockServer::start().await;

    Mock::given(method("DELETE"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/gone")))
        .respond_with(ResponseTemplate::new(404))
        .expect(1)
        .mount(&mock_server)
        .await;

    let dir = TempDir::new().unwrap();
    let (_, metadata_store, _) = test_blob_store(dir.path().to_str().unwrap());
    let outcome = delete_manifest(
        &downstream_client(&mock_server.uri()),
        &metadata_store,
        NAMESPACE,
        &Reference::Tag(Tag::new("gone").unwrap()),
        None,
    )
    .await
    .expect("an already-absent delete must succeed");
    assert_eq!(
        outcome,
        PushOutcome::Converged,
        "an already-absent delete is a converged no-op, not an applied delete"
    );
    drop(mock_server);
}

#[tokio::test]
async fn delete_manifest_of_unsupported_downstream_is_unsupported_not_error() {
    // A downstream rejecting tag deletion with 405 must complete the job as
    // Unsupported, not error and dead-letter one job per deletion event.
    metrics_provider::init_for_tests();
    let mock_server = MockServer::start().await;

    Mock::given(method("DELETE"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
        .respond_with(ResponseTemplate::new(405))
        .mount(&mock_server)
        .await;

    let dir = TempDir::new().unwrap();
    let (_, metadata_store, _) = test_blob_store(dir.path().to_str().unwrap());
    let outcome = delete_manifest(
        &downstream_client(&mock_server.uri()),
        &metadata_store,
        NAMESPACE,
        &Reference::Tag(Tag::new("v1").unwrap()),
        None,
    )
    .await
    .expect("a 405 tag delete must complete, not error");
    assert_eq!(outcome, PushOutcome::Unsupported);
    drop(mock_server);
}

#[tokio::test]
async fn delete_manifest_propagates_non_superseded_409_as_error() {
    metrics_provider::init_for_tests();
    let mock_server = MockServer::start().await;

    Mock::given(method("DELETE"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
        .respond_with(ResponseTemplate::new(409).set_body_json(oci_error_body("CONFLICT")))
        .expect(1)
        .mount(&mock_server)
        .await;

    let dir = TempDir::new().unwrap();
    let (_, metadata_store, _) = test_blob_store(dir.path().to_str().unwrap());
    let result = delete_manifest(
        &downstream_client(&mock_server.uri()),
        &metadata_store,
        NAMESPACE,
        &Reference::Tag(Tag::new("v1").unwrap()),
        None,
    )
    .await;
    assert!(
        result.is_err(),
        "a non-superseded delete-409 must propagate as an error"
    );

    drop(mock_server);
}

#[tokio::test]
async fn upload_into_session_cancels_when_local_blob_read_fails() {
    // The session is already open; a missing local blob must cancel it, not
    // strand it on the downstream.
    metrics_provider::init_for_tests();
    let mock_server = MockServer::start().await;
    let dir = TempDir::new().unwrap();
    let (blob_store, metadata_store, _store) = test_blob_store(dir.path().to_str().unwrap());

    let absent = Digest::sha256_of_bytes(b"never-written-locally");
    let session = UploadSession {
        url: format!("{}/v2/{NAMESPACE}/blobs/uploads/sess-1", mock_server.uri()),
        auth: None,
    };
    Mock::given(method("DELETE"))
        .and(path(format!("/v2/{NAMESPACE}/blobs/uploads/sess-1")))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = downstream_client(&mock_server.uri());
    let ctx = PushContext {
        downstream: &client,
        blob_store: &blob_store,
        metadata_store: &metadata_store,
        namespace: NAMESPACE,
        max_concurrent_pushes: 4,
        source_ts: None,
    };
    let result = super::upload_into_session(&ctx, &absent, &session).await;
    assert!(result.is_err(), "a missing local blob must fail the upload");

    // .expect(1) on the DELETE verifies the session was cancelled.
    drop(mock_server);
}

/// A referrer manifest with a subject; returns `(body bytes, digest)`.
fn referrer_manifest(subject: &Digest) -> (Vec<u8>, Digest) {
    let body = serde_json::to_vec(&json!({
            "schemaVersion": 2,
            "mediaType": OCI_MANIFEST_MEDIA_TYPE,
            "subject": { "mediaType": OCI_MANIFEST_MEDIA_TYPE, "digest": subject.to_string(), "size": 2 },
            "layers": [],
        }))
        .unwrap();
    let digest = Digest::sha256_of_bytes(&body);
    (body, digest)
}

#[tokio::test]
async fn deleting_last_referrer_removes_the_fallback_tag() {
    metrics_provider::init_for_tests();
    let mock_server = MockServer::start().await;
    let dir = TempDir::new().unwrap();
    let (_, metadata_store, _) = test_blob_store(dir.path().to_str().unwrap());

    let subject = Digest::sha256_of_bytes(b"the-subject");
    let (referrer_body, referrer) = referrer_manifest(&subject);
    let fallback_tag = format!("{}-{}", subject.algorithm(), subject.hash());

    // The downstream still holds the referrer, so the delete can read its
    // subject before removing it.
    Mock::given(method("GET"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/{referrer}")))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header(DOCKER_CONTENT_DIGEST, referrer.to_string().as_str())
                .set_body_bytes(referrer_body),
        )
        .mount(&mock_server)
        .await;
    Mock::given(method("DELETE"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/{referrer}")))
        .respond_with(ResponseTemplate::new(202))
        .mount(&mock_server)
        .await;
    // The fallback index lists only this referrer.
    let index = json!({
        "schemaVersion": 2,
        "mediaType": OCI_INDEX_MEDIA_TYPE,
        "manifests": [{ "mediaType": OCI_MANIFEST_MEDIA_TYPE, "digest": referrer.to_string(), "size": 2 }],
    });
    Mock::given(method("GET"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/{fallback_tag}")))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header(
                    DOCKER_CONTENT_DIGEST,
                    "sha256:0000000000000000000000000000000000000000000000000000000000000000",
                )
                .set_body_json(index),
        )
        .mount(&mock_server)
        .await;
    // Emptied: the fallback tag itself must be deleted (expect verifies it ran).
    Mock::given(method("DELETE"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/{fallback_tag}")))
        .respond_with(ResponseTemplate::new(202))
        .expect(1)
        .mount(&mock_server)
        .await;

    let outcome = delete_manifest(
        &downstream_client(&mock_server.uri()),
        &metadata_store,
        NAMESPACE,
        &Reference::Digest(referrer.clone()),
        None,
    )
    .await
    .expect("the digest delete must succeed");
    assert_eq!(outcome, PushOutcome::Pushed);

    drop(mock_server);
}

#[tokio::test]
async fn deleting_a_referrer_keeps_its_siblings_in_the_fallback_index() {
    metrics_provider::init_for_tests();
    let mock_server = MockServer::start().await;
    let dir = TempDir::new().unwrap();
    let (_, metadata_store, _) = test_blob_store(dir.path().to_str().unwrap());

    let subject = Digest::sha256_of_bytes(b"shared-subject");
    let (referrer_body, referrer) = referrer_manifest(&subject);
    let sibling = Digest::sha256_of_bytes(b"sibling-referrer");
    let fallback_tag = format!("{}-{}", subject.algorithm(), subject.hash());

    Mock::given(method("GET"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/{referrer}")))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header(DOCKER_CONTENT_DIGEST, referrer.to_string().as_str())
                .set_body_bytes(referrer_body),
        )
        .mount(&mock_server)
        .await;
    Mock::given(method("DELETE"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/{referrer}")))
        .respond_with(ResponseTemplate::new(202))
        .mount(&mock_server)
        .await;
    let index = json!({
        "schemaVersion": 2,
        "mediaType": OCI_INDEX_MEDIA_TYPE,
        "manifests": [
            { "mediaType": OCI_MANIFEST_MEDIA_TYPE, "digest": referrer.to_string(), "size": 2 },
            { "mediaType": OCI_MANIFEST_MEDIA_TYPE, "digest": sibling.to_string(), "size": 3 },
        ],
    });
    Mock::given(method("GET"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/{fallback_tag}")))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header(
                    DOCKER_CONTENT_DIGEST,
                    "sha256:0000000000000000000000000000000000000000000000000000000000000000",
                )
                .set_body_json(index),
        )
        .mount(&mock_server)
        .await;
    // Sibling remains, so the index is re-PUT (not deleted).
    Mock::given(method("PUT"))
        .and(path(format!("/v2/{NAMESPACE}/manifests/{fallback_tag}")))
        .respond_with(ResponseTemplate::new(201))
        .expect(1)
        .mount(&mock_server)
        .await;

    delete_manifest(
        &downstream_client(&mock_server.uri()),
        &metadata_store,
        NAMESPACE,
        &Reference::Digest(referrer.clone()),
        None,
    )
    .await
    .expect("the digest delete must succeed");

    // The re-PUT index keeps the sibling and drops the deleted referrer.
    let put_body = mock_server
        .received_requests()
        .await
        .unwrap_or_default()
        .into_iter()
        .find(|r| r.method.as_str() == "PUT" && r.url.path().ends_with(&fallback_tag))
        .map(|r| r.body)
        .expect("the fallback index must be re-PUT");
    let digests: Vec<String> = serde_json::from_slice::<Value>(&put_body)
        .ok()
        .and_then(|v| v.get("manifests").and_then(Value::as_array).cloned())
        .unwrap_or_default()
        .iter()
        .filter_map(|m| m.get("digest").and_then(Value::as_str).map(str::to_string))
        .collect();
    assert_eq!(
        digests,
        vec![sibling.to_string()],
        "only the sibling must remain"
    );

    drop(mock_server);
}
