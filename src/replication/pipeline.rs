//! The replication push pipeline.
//!
//! One code path drives both the event-driven and the scrub-reconcile push: a
//! job resolved to a `(namespace, digest, tag?)` is mirrored to a single
//! downstream [`RegistryClient`]. Manifests are content-addressed, so the local
//! manifest body is read from the [`BlobStore`] by digest; its referenced blobs
//! and child manifests are enumerated with [`ParsedManifestDigests`].
//!
//! Idempotency is mandatory (the queue is at-least-once and an HTTP PUT cannot
//! be rolled back): every blob is `head_blob`-probed on the downstream before a
//! transfer, and child manifests land before the parent index, so a re-run is a
//! sequence of no-op HEADs.

use std::{str::FromStr, sync::Arc};

use futures_util::stream::{self, StreamExt, TryStreamExt};
use serde_json::{Value, json};
use tracing::{debug, info, instrument, warn};

use crate::{
    oci::{Digest, Reference},
    registry::{
        Error as RegistryError, ParsedManifestDigests, blob_store::BlobStore,
        parse_manifest_digests,
    },
    registry_client::{DeleteManifestOutcome, RegistryClient},
    replication::Error,
    util::sha256,
};

/// Media type of an OCI image index (the referrers fallback tag body).
const OCI_INDEX_MEDIA_TYPE: &str = "application/vnd.oci.image.index.v1+json";

/// Outcome of a successful replication push or delete.
///
/// Both arms are convergence (the system reached the intended state); the
/// distinction lets the caller record `pushed` vs `superseded` metrics. A push
/// the downstream actively applied is [`PushOutcome::Pushed`]; a last-writer-wins
/// loss (the downstream already holds a strictly-newer copy) is
/// [`PushOutcome::Superseded`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PushOutcome {
    /// The downstream accepted and applied the change.
    Pushed,
    /// The downstream rejected the change by last-writer-wins; it already holds a
    /// strictly-newer copy, so the system has converged.
    Superseded,
}

/// Pushes the manifest at `digest` (and everything it references) to
/// `downstream`, then binds `tag` to it when set.
///
/// Child manifests of an image index / manifest list are pushed first
/// (depth-first), so the parent index never lands on the downstream ahead of its
/// children. Referenced blobs are `head_blob`-probed and only transferred when
/// absent; the per-manifest blob fan-out is bounded by `max_concurrent_pushes`.
///
/// `origin` is forwarded verbatim as the `X-Angos-Origin` header value so a
/// multi-hop mesh attributes the change to its original author, and `source_ts`
/// is forwarded as the `X-Angos-Source-Timestamp` header so the receiver can
/// apply last-writer-wins. Both are stamped on every outgoing `put_manifest`
/// (the primary manifest and the referrers-fallback index).
///
/// 409 disambiguation: when the downstream rejects the push by last-writer-wins
/// (a `409` whose OCI code is the replication-superseded code, surfaced by the
/// client as `PutManifestResult { superseded: true, .. }`) the push is treated
/// as **success** — the downstream already holds a strictly-newer copy, so the
/// system has converged. The handler then completes the job rather than
/// retrying. Any *other* downstream error (including a different 409, e.g. an
/// immutable-tag conflict) propagates as [`Error::Registry`] so the job
/// retries/dead-letters.
///
/// On success returns [`PushOutcome::Pushed`] when the downstream applied the
/// change, or [`PushOutcome::Superseded`] when it lost a last-writer-wins
/// comparison (both are convergence; the caller records them as distinct
/// metrics outcomes).
///
/// # Errors
///
/// Returns [`Error::Registry`] when a local read fails or a downstream HTTP
/// operation fails with anything other than an LWW-superseded 409.
#[instrument(skip(downstream, blob_store, body))]
#[allow(clippy::too_many_arguments)]
pub async fn push_manifest(
    downstream: &RegistryClient,
    blob_store: &Arc<BlobStore>,
    namespace: &str,
    digest: &Digest,
    media_type: Option<&str>,
    tag: Option<&str>,
    body: Vec<u8>,
    max_concurrent_pushes: usize,
    origin: Option<&str>,
    source_ts: Option<&str>,
) -> Result<PushOutcome, Error> {
    let parsed = parse_manifest_digests(&body, media_type.map(ToString::to_string).as_ref())
        .map_err(Error::Registry)?;

    // 1. Recurse into child manifests FIRST (image index / manifest list). The
    //    parent index must not land on the downstream before its children.
    for child in &parsed.manifests {
        let (child_media_type, child_body) = read_local_manifest(blob_store, child).await?;
        Box::pin(push_manifest(
            downstream,
            blob_store,
            namespace,
            child,
            child_media_type.as_deref(),
            None,
            child_body,
            max_concurrent_pushes,
            origin,
            source_ts,
        ))
        .await?;
    }

    // 2. Push every referenced blob (config + layers), bounded by concurrency.
    push_blobs(
        downstream,
        blob_store,
        namespace,
        &parsed,
        max_concurrent_pushes,
    )
    .await?;

    // 3. Push the manifest itself. When a tag is set the reference is the tag so
    //    the downstream binds tag -> digest atomically; otherwise push by digest.
    let reference = match tag {
        Some(tag) => Reference::Tag(tag.to_string()),
        None => Reference::Digest(digest.clone()),
    };
    let location = downstream.get_manifest_path("", namespace, &reference);
    let result = downstream
        .put_manifest(&location, media_type, body.clone(), origin, source_ts)
        .await
        .map_err(Error::Registry)?;

    // 3a. LWW loss: the downstream already holds a strictly-newer copy. This is
    //     convergence, not failure — drop the push (and skip the referrers
    //     fallback, which would re-push a superseded artifact).
    if result.superseded {
        info!(
            namespace,
            %digest,
            ?tag,
            "Downstream superseded the push (last-writer-wins); treating as converged"
        );
        return Ok(PushOutcome::Superseded);
    }
    info!(namespace, %digest, ?tag, "Pushed manifest to downstream");

    // 4. Referrers fallback: a subject-bearing manifest on an OCI-1.0 downstream
    //    (no `OCI-Subject` response header) is not auto-indexed, so push the
    //    referrers fallback tag manifest. OCI-1.1 downstreams index it for free.
    if parsed.subject.is_some() && result.subject.is_none() {
        push_referrers_fallback(
            downstream, namespace, digest, &parsed, &body, origin, source_ts,
        )
        .await?;
    }

    Ok(PushOutcome::Pushed)
}

/// Reads a local manifest body (stored as a blob) and its recorded media type.
async fn read_local_manifest(
    blob_store: &Arc<BlobStore>,
    digest: &Digest,
) -> Result<(Option<String>, Vec<u8>), Error> {
    let body = blob_store.read(digest).await.map_err(|e| {
        Error::Registry(RegistryError::Internal(format!(
            "failed to read local manifest blob '{digest}': {e}"
        )))
    })?;
    // The blob body itself carries the manifest's `mediaType`; reading it back
    // out keeps the push self-contained without a metadata-store round-trip.
    let media_type = serde_json::from_slice::<serde_json::Value>(&body)
        .ok()
        .and_then(|v| {
            v.get("mediaType")
                .and_then(|m| m.as_str())
                .map(ToString::to_string)
        });
    Ok((media_type, body))
}

/// HEAD-before-PUT every referenced blob; transfer only the absent ones.
async fn push_blobs(
    downstream: &RegistryClient,
    blob_store: &Arc<BlobStore>,
    namespace: &str,
    parsed: &ParsedManifestDigests,
    max_concurrent_pushes: usize,
) -> Result<(), Error> {
    let blobs: Vec<Digest> = parsed
        .config
        .iter()
        .chain(parsed.layers.iter())
        .cloned()
        .collect();

    stream::iter(blobs)
        .map(|blob| async move { push_one_blob(downstream, blob_store, namespace, &blob).await })
        .buffer_unordered(max_concurrent_pushes.max(1))
        .try_collect::<Vec<()>>()
        .await?;

    Ok(())
}

/// Transfers a single blob to the downstream if it is not already present.
async fn push_one_blob(
    downstream: &RegistryClient,
    blob_store: &Arc<BlobStore>,
    namespace: &str,
    digest: &Digest,
) -> Result<(), Error> {
    // Belt-and-suspenders idempotency: skip the transfer when the downstream
    // already holds the bytes (HEAD-before-PUT).
    let head_location = downstream.get_blob_path("", namespace, digest);
    if downstream.head_blob(&[], &head_location).await.is_ok() {
        debug!(namespace, %digest, "Blob already present on downstream; skipping");
        return Ok(());
    }

    let (reader, content_length) = blob_store.reader(digest, None).await.map_err(|e| {
        Error::Registry(RegistryError::Internal(format!(
            "failed to open local blob '{digest}': {e}"
        )))
    })?;

    // No cross-repo mount: open a fresh session and stream.
    let start_location = downstream.get_uploads_start_path("", namespace);
    let session_url = downstream
        .start_upload(&start_location)
        .await
        .map_err(Error::Registry)?;
    let session_url = downstream
        .patch_upload(&session_url, content_length, reader)
        .await
        .map_err(Error::Registry)?;
    downstream
        .complete_upload(&session_url, digest)
        .await
        .map_err(Error::Registry)?;

    info!(namespace, %digest, content_length, "Pushed blob to downstream");
    Ok(())
}

/// Pushes the OCI-1.0 referrers fallback tag index for a subject-bearing
/// manifest the downstream did not auto-index.
///
/// Implements the distribution-spec Referrers Tag Schema: the fallback tag
/// `<alg>-<hash>` of the SUBJECT digest holds an image **index** whose
/// `manifests[]` enumerate the referrer descriptors. A pushing client must GET
/// the existing index, append its own referrer descriptor, then PUT the merged
/// index — so a second referrer of the same subject does not clobber the first.
#[allow(clippy::too_many_arguments)]
async fn push_referrers_fallback(
    downstream: &RegistryClient,
    namespace: &str,
    digest: &Digest,
    parsed: &ParsedManifestDigests,
    body: &[u8],
    origin: Option<&str>,
    source_ts: Option<&str>,
) -> Result<(), Error> {
    let Some(subject) = &parsed.subject else {
        return Ok(());
    };
    let fallback_tag = format!("{}-{}", subject.algorithm(), subject.hash());
    warn!(
        namespace,
        %digest,
        %subject,
        fallback_tag,
        "Downstream did not index subject (OCI-1.0); merging referrers fallback index"
    );

    let reference = Reference::from_str(&fallback_tag).map_err(|e| {
        Error::Registry(RegistryError::Internal(format!(
            "invalid referrers fallback tag '{fallback_tag}': {e}"
        )))
    })?;
    let location = downstream.get_manifest_path("", namespace, &reference);

    // GET the existing fallback index; a missing/empty tag starts fresh. Any
    // pre-existing referrer descriptors are preserved so this push only adds.
    let mut manifests = fetch_fallback_manifests(downstream, &location).await;

    // Descriptor for the referrer manifest just pushed.
    let referrer_digest = Digest::Sha256(sha256::hex(body).into());
    let descriptor = referrer_descriptor(&referrer_digest, body);

    // Dedup-merge by digest so a re-run is idempotent.
    let already_present = manifests
        .iter()
        .any(|m| m.get("digest").and_then(Value::as_str) == Some(&referrer_digest.to_string()));
    if !already_present {
        manifests.push(descriptor);
    }

    let index = json!({
        "schemaVersion": 2,
        "mediaType": OCI_INDEX_MEDIA_TYPE,
        "manifests": manifests,
    });
    let index_body = serde_json::to_vec(&index).map_err(|e| {
        Error::Registry(RegistryError::Internal(format!(
            "failed to serialize referrers fallback index: {e}"
        )))
    })?;

    downstream
        .put_manifest(
            &location,
            Some(OCI_INDEX_MEDIA_TYPE),
            index_body,
            origin,
            source_ts,
        )
        .await
        .map_err(Error::Registry)?;
    Ok(())
}

/// GETs the existing referrers fallback index at `location` and returns its
/// `manifests[]` descriptors, or an empty list when the tag is absent or the
/// body cannot be parsed as an index.
async fn fetch_fallback_manifests(downstream: &RegistryClient, location: &str) -> Vec<Value> {
    let Ok((_, _, body)) = downstream.get_manifest(&[], location).await else {
        return Vec::new();
    };
    serde_json::from_slice::<Value>(&body)
        .ok()
        .and_then(|v| v.get("manifests").and_then(Value::as_array).cloned())
        .unwrap_or_default()
}

/// Builds an OCI descriptor for a referrer manifest to embed in the fallback
/// index: `mediaType`, `digest`, `size`, and `artifactType` (carried through
/// from the manifest body when present).
fn referrer_descriptor(referrer_digest: &Digest, body: &[u8]) -> Value {
    let mut descriptor = json!({
        "mediaType": OCI_INDEX_MEDIA_TYPE,
        "digest": referrer_digest.to_string(),
        "size": body.len(),
    });
    if let Ok(parsed) = serde_json::from_slice::<Value>(body) {
        if let Some(manifest_media_type) = parsed.get("mediaType").and_then(Value::as_str) {
            descriptor["mediaType"] = json!(manifest_media_type);
        }
        if let Some(artifact_type) = parsed.get("artifactType").and_then(Value::as_str) {
            descriptor["artifactType"] = json!(artifact_type);
        }
    }
    descriptor
}

/// Deletes the manifest bound to `reference` on the downstream.
///
/// `origin` / `source_ts` are stamped as the `X-Angos-Origin` /
/// `X-Angos-Source-Timestamp` headers so the receiver can loop-filter and apply
/// last-writer-wins.
///
/// 409 disambiguation: an LWW-loser 409 (the receiver's copy is strictly newer,
/// surfaced by the client as [`DeleteManifestOutcome::Superseded`]) is treated
/// as **success** — convergence, not failure. Any *other* downstream error
/// (including an immutable-tag conflict 409) propagates as [`Error::Registry`]
/// so the job retries/dead-letters.
///
/// On success returns [`PushOutcome::Pushed`] when the downstream applied the
/// delete, or [`PushOutcome::Superseded`] when it lost a last-writer-wins
/// comparison (both are convergence; the caller records them as distinct
/// metrics outcomes).
///
/// # Errors
///
/// Returns [`Error::Registry`] when the downstream HTTP delete fails with
/// anything other than an LWW-superseded 409.
#[instrument(skip(downstream))]
pub async fn delete_manifest(
    downstream: &RegistryClient,
    namespace: &str,
    reference: &Reference,
    origin: Option<&str>,
    source_ts: Option<&str>,
) -> Result<PushOutcome, Error> {
    let location = downstream.get_manifest_path("", namespace, reference);
    let outcome = downstream
        .delete_manifest(&location, origin, source_ts)
        .await
        .map_err(Error::Registry)?;
    match outcome {
        DeleteManifestOutcome::Deleted => {
            info!(namespace, %reference, "Deleted manifest on downstream");
            Ok(PushOutcome::Pushed)
        }
        DeleteManifestOutcome::Superseded => {
            info!(
                namespace,
                %reference,
                "Downstream superseded the delete (last-writer-wins); treating as converged"
            );
            Ok(PushOutcome::Superseded)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use serde_json::json;
    use tempfile::TempDir;
    use wiremock::{
        Mock, MockServer, Request, Respond, ResponseTemplate,
        matchers::{header, method, path},
    };

    use angos_storage::{ObjectStore, fs::Backend as StorageFsBackend};
    use angos_tx_engine::store::Store;

    use crate::{
        cache,
        oci::{Digest, Reference},
        registry::{
            DOCKER_CONTENT_DIGEST, OCI_SUBJECT,
            blob_store::BlobStore,
            manifest::DEFAULT_MAX_MANIFEST_SIZE_BYTES,
            test_utils::{build_store, build_test_fs_executor, put_blob_direct},
        },
        registry_client::RegistryClient,
        replication::{
            REPLICATION_SUPERSEDED_CODE, X_ANGOS_ORIGIN, X_ANGOS_SOURCE_TIMESTAMP,
            pipeline::{delete_manifest, push_manifest},
        },
        util::sha256,
    };

    const NAMESPACE: &str = "nginx";

    fn downstream_client(uri: &str) -> RegistryClient {
        let backend = cache::Config::Memory.to_backend().unwrap();
        RegistryClient::builder()
            .url(uri.to_string())
            .client(reqwest::Client::new())
            .cache(backend)
            .max_manifest_size_bytes(DEFAULT_MAX_MANIFEST_SIZE_BYTES)
            .build()
            .unwrap()
    }

    fn test_blob_store(root: &str) -> (Arc<BlobStore>, Arc<Store>) {
        let object: Arc<dyn ObjectStore> =
            Arc::new(StorageFsBackend::builder().root_dir(root).build().unwrap());
        let executor = build_test_fs_executor(root, false);
        let store = build_store(object, executor);
        let blob_store = Arc::new(BlobStore::builder().store(store.clone()).build().unwrap());
        (blob_store, store)
    }

    /// A wiremock responder that records the order in which the index vs. its
    /// child manifest are PUT, so the test can assert the child lands first.
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
        crate::metrics_provider::init_for_tests();
        let mock_server = MockServer::start().await;
        let dir = TempDir::new().unwrap();
        let (blob_store, store) = test_blob_store(dir.path().to_str().unwrap());

        // Subject digest the referrer manifest points at.
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

        // Config blob missing on downstream -> upload runs.
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

        // The primary manifest PUT by tag — NO `OCI-Subject` header => OCI-1.0
        // downstream => fallback expected.
        Mock::given(method("PUT"))
            .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
            .respond_with(
                ResponseTemplate::new(201)
                    .insert_header(DOCKER_CONTENT_DIGEST, manifest_digest.to_string().as_str()),
            )
            .expect(1)
            .mount(&mock_server)
            .await;

        // The referrers fallback tag: `<alg>-<hash>` of the subject digest. The
        // pipeline GETs the existing index first (none yet -> 404 -> start fresh).
        let fallback_tag = format!("{}-{}", subject.algorithm(), subject.hash());
        Mock::given(method("GET"))
            .and(path(format!("/v2/{NAMESPACE}/manifests/{fallback_tag}")))
            .respond_with(ResponseTemplate::new(404))
            .mount(&mock_server)
            .await;

        // ...then PUTs a MERGED image INDEX whose `manifests[]` enumerate the
        // referrer descriptors. Capture + assert the body shape so a regression
        // back to "PUT the referrer manifest body" cannot pass silently.
        let referrer_digest = Digest::Sha256(sha256::hex(&manifest_bytes).into());
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

        push_manifest(
            &downstream_client(&mock_server.uri()),
            &blob_store,
            NAMESPACE,
            &manifest_digest,
            Some("application/vnd.oci.image.manifest.v1+json"),
            Some("v1"),
            manifest_bytes,
            4,
            None,
            None,
        )
        .await
        .unwrap();

        // `.expect(1)` on the primary manifest PUT + the fallback index PUT (with
        // the body assertion above) is verified on MockServer drop.
        drop(mock_server);
    }

    #[tokio::test]
    async fn no_referrers_fallback_when_downstream_indexes_subject() {
        crate::metrics_provider::init_for_tests();
        let mock_server = MockServer::start().await;
        let dir = TempDir::new().unwrap();
        let (blob_store, store) = test_blob_store(dir.path().to_str().unwrap());

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

        // PUT echoes back `OCI-Subject` => OCI-1.1 downstream => NO fallback.
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
        push_manifest(
            &downstream_client(&mock_server.uri()),
            &blob_store,
            NAMESPACE,
            &manifest_digest,
            Some("application/vnd.oci.image.manifest.v1+json"),
            Some("v1"),
            manifest_bytes,
            4,
            None,
            None,
        )
        .await
        .unwrap();

        drop(mock_server);
    }

    #[tokio::test]
    async fn index_lands_after_its_child_manifest() {
        crate::metrics_provider::init_for_tests();
        let mock_server = MockServer::start().await;
        let dir = TempDir::new().unwrap();
        let (blob_store, store) = test_blob_store(dir.path().to_str().unwrap());

        // Child image manifest (no blobs, to keep the test focused on ordering).
        let child = json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "layers": [],
        });
        let child_bytes = serde_json::to_vec(&child).unwrap();
        let child_digest = put_blob_direct(&store, &child_bytes).await;

        // Index referencing the child.
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

        // Child manifest pushed by digest (no tag on recursion).
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
        // Index pushed by tag.
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

        push_manifest(
            &downstream_client(&mock_server.uri()),
            &blob_store,
            NAMESPACE,
            &index_digest,
            Some("application/vnd.oci.image.index.v1+json"),
            Some("v1"),
            index_bytes,
            4,
            None,
            None,
        )
        .await
        .unwrap();

        assert!(
            child_at.load(Ordering::SeqCst) < index_at.load(Ordering::SeqCst),
            "child manifest must land before the parent index"
        );
        drop(mock_server);
    }

    /// Seeds a minimal blob-less image manifest and stores it locally, returning
    /// its digest and serialized body (used by the header/409 tests below).
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
    async fn push_manifest_stamps_origin_and_source_timestamp_headers() {
        crate::metrics_provider::init_for_tests();
        let mock_server = MockServer::start().await;
        let dir = TempDir::new().unwrap();
        let (blob_store, store) = test_blob_store(dir.path().to_str().unwrap());
        let (manifest_digest, manifest_bytes) = seed_blobless_manifest(&store).await;

        // The PUT must carry BOTH replication headers with the expected values.
        Mock::given(method("PUT"))
            .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
            .and(header(X_ANGOS_ORIGIN, "instance-a"))
            .and(header(X_ANGOS_SOURCE_TIMESTAMP, "2026-06-03T00:00:00Z"))
            .respond_with(
                ResponseTemplate::new(201)
                    .insert_header(DOCKER_CONTENT_DIGEST, manifest_digest.to_string().as_str()),
            )
            .expect(1)
            .mount(&mock_server)
            .await;

        push_manifest(
            &downstream_client(&mock_server.uri()),
            &blob_store,
            NAMESPACE,
            &manifest_digest,
            Some("application/vnd.oci.image.manifest.v1+json"),
            Some("v1"),
            manifest_bytes,
            4,
            Some("instance-a"),
            Some("2026-06-03T00:00:00Z"),
        )
        .await
        .unwrap();

        // `.expect(1)` (verified on drop) only matches when both headers are
        // present with the expected values; a missing/wrong header => no match
        // => the PUT would 404 and the push would error.
        drop(mock_server);
    }

    #[tokio::test]
    async fn push_manifest_treats_lww_superseded_409_as_success() {
        crate::metrics_provider::init_for_tests();
        let mock_server = MockServer::start().await;
        let dir = TempDir::new().unwrap();
        let (blob_store, store) = test_blob_store(dir.path().to_str().unwrap());
        let (manifest_digest, manifest_bytes) = seed_blobless_manifest(&store).await;

        // Downstream rejects with a 409 whose OCI code is the superseded code:
        // the receiver already holds a strictly-newer copy => convergence.
        Mock::given(method("PUT"))
            .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
            .respond_with(
                ResponseTemplate::new(409)
                    .set_body_json(oci_error_body(REPLICATION_SUPERSEDED_CODE)),
            )
            .expect(1)
            .mount(&mock_server)
            .await;

        // The pipeline must treat this as Ok (the handler would `complete` the
        // job rather than retry).
        push_manifest(
            &downstream_client(&mock_server.uri()),
            &blob_store,
            NAMESPACE,
            &manifest_digest,
            Some("application/vnd.oci.image.manifest.v1+json"),
            Some("v1"),
            manifest_bytes,
            4,
            Some("instance-a"),
            Some("2026-06-03T00:00:00Z"),
        )
        .await
        .expect("an LWW-superseded 409 must be treated as success");

        drop(mock_server);
    }

    #[tokio::test]
    async fn push_manifest_propagates_immutable_409_as_error() {
        crate::metrics_provider::init_for_tests();
        let mock_server = MockServer::start().await;
        let dir = TempDir::new().unwrap();
        let (blob_store, store) = test_blob_store(dir.path().to_str().unwrap());
        let (manifest_digest, manifest_bytes) = seed_blobless_manifest(&store).await;

        // Downstream rejects with a 409 carrying the immutable-tag `CONFLICT`
        // code: operator misconfiguration, NOT an LWW loss => must surface so the
        // job retries/dead-letters.
        Mock::given(method("PUT"))
            .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
            .respond_with(ResponseTemplate::new(409).set_body_json(oci_error_body("CONFLICT")))
            .expect(1)
            .mount(&mock_server)
            .await;

        let result = push_manifest(
            &downstream_client(&mock_server.uri()),
            &blob_store,
            NAMESPACE,
            &manifest_digest,
            Some("application/vnd.oci.image.manifest.v1+json"),
            Some("v1"),
            manifest_bytes,
            4,
            None,
            None,
        )
        .await;
        assert!(
            result.is_err(),
            "a non-superseded 409 (immutable conflict) must propagate as an error"
        );

        drop(mock_server);
    }

    #[tokio::test]
    async fn delete_manifest_stamps_headers_and_distinguishes_superseded() {
        crate::metrics_provider::init_for_tests();
        let mock_server = MockServer::start().await;

        // A delete carrying both replication headers; respond with an
        // LWW-superseded 409 => the pipeline treats it as success.
        Mock::given(method("DELETE"))
            .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
            .and(header(X_ANGOS_ORIGIN, "instance-a"))
            .and(header(X_ANGOS_SOURCE_TIMESTAMP, "2026-06-03T00:00:00Z"))
            .respond_with(
                ResponseTemplate::new(409)
                    .set_body_json(oci_error_body(REPLICATION_SUPERSEDED_CODE)),
            )
            .expect(1)
            .mount(&mock_server)
            .await;

        delete_manifest(
            &downstream_client(&mock_server.uri()),
            NAMESPACE,
            &Reference::Tag("v1".to_string()),
            Some("instance-a"),
            Some("2026-06-03T00:00:00Z"),
        )
        .await
        .expect("an LWW-superseded delete-409 must be treated as success");

        drop(mock_server);
    }

    #[tokio::test]
    async fn delete_manifest_propagates_non_superseded_409_as_error() {
        crate::metrics_provider::init_for_tests();
        let mock_server = MockServer::start().await;

        Mock::given(method("DELETE"))
            .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
            .respond_with(ResponseTemplate::new(409).set_body_json(oci_error_body("CONFLICT")))
            .expect(1)
            .mount(&mock_server)
            .await;

        let result = delete_manifest(
            &downstream_client(&mock_server.uri()),
            NAMESPACE,
            &Reference::Tag("v1".to_string()),
            None,
            None,
        )
        .await;
        assert!(
            result.is_err(),
            "a non-superseded delete-409 must propagate as an error"
        );

        drop(mock_server);
    }
}
