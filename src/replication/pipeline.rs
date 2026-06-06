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
        Error as RegistryError, ParsedManifestDigests, blob_ownership::BlobOwnership,
        blob_store::BlobStore, metadata_store::MetadataStore, parse_manifest_digests,
    },
    registry_client::{DeleteManifestOutcome, NO_LOCAL_PREFIX, RegistryClient},
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
/// `source_ts` is forwarded as the `X-Angos-Source-Timestamp` header so the
/// receiver can apply last-writer-wins; it is stamped on every outgoing
/// `put_manifest` (the primary manifest and the referrers-fallback index).
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
#[instrument(skip(downstream, blob_store, metadata_store, body))]
#[allow(clippy::too_many_arguments)]
pub async fn push_manifest(
    downstream: &RegistryClient,
    blob_store: &Arc<BlobStore>,
    metadata_store: &Arc<MetadataStore>,
    namespace: &str,
    digest: &Digest,
    media_type: Option<&str>,
    tag: Option<&str>,
    body: Vec<u8>,
    max_concurrent_pushes: usize,
    source_ts: Option<&str>,
) -> Result<PushOutcome, Error> {
    let parsed = parse_manifest_digests(&body, media_type.map(ToString::to_string).as_ref())
        .map_err(Error::Registry)?;

    // The PUT content type is the explicit `media_type` override when given, else
    // the manifest body's own declared mediaType (surfaced by the parse above) —
    // so the body is parsed once, not again via a separate read.
    let effective_media_type = media_type
        .map(ToString::to_string)
        .or_else(|| parsed.media_type.clone());

    // 1. Recurse into child manifests FIRST (image index / manifest list). The
    //    parent index must not land on the downstream before its children.
    for child in &parsed.manifests {
        let child_body = read_local_manifest(blob_store, child).await?;
        Box::pin(push_manifest(
            downstream,
            blob_store,
            metadata_store,
            namespace,
            child,
            None,
            None,
            child_body,
            max_concurrent_pushes,
            source_ts,
        ))
        .await?;
    }

    // 2. Push every referenced blob (config + layers), bounded by concurrency.
    push_blobs(
        downstream,
        blob_store,
        metadata_store,
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
    let location = downstream.get_manifest_path(NO_LOCAL_PREFIX, namespace, &reference);

    // 3a. HEAD-before-PUT (bandwidth optimization, NOT loop prevention — the
    //     receiver-side no-op dispatch gate breaks cycles; this just avoids one
    //     wasted PUT once converged). Probe the target reference: if the
    //     downstream already resolves it to THIS digest, it is converged, so skip
    //     the manifest PUT (and the referrers fallback — see the converged
    //     branch below).
    let already_present = downstream
        .head_manifest(&[], &location)
        .await
        .is_ok_and(|(_, downstream_digest, _)| &downstream_digest == digest);

    if already_present {
        info!(
            namespace,
            %digest,
            ?tag,
            "Downstream already holds this manifest; skipping PUT (converged)"
        );
        // No referrers fallback on the converged path. The original
        // (non-converged) push already created the `<alg>-<hash>` fallback tag for
        // an OCI-1.0 downstream (gated on the PUT's missing `OCI-Subject`), and an
        // OCI-1.1 downstream auto-indexes the subject and never needs it. Re-running
        // it on every converged re-run only re-merged a redundant index — and
        // materialised a stray fallback tag on OCI-1.1 peers that already indexed
        // the subject.
        return Ok(PushOutcome::Pushed);
    }

    let result = downstream
        .put_manifest(
            &location,
            effective_media_type.as_deref(),
            body.clone(),
            source_ts,
        )
        .await
        .map_err(Error::Registry)?;

    // 3c. LWW loss: the downstream already holds a strictly-newer copy. This is
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
        push_referrers_fallback(downstream, namespace, digest, &parsed, &body, source_ts).await?;
    }

    Ok(PushOutcome::Pushed)
}

/// Reads a local manifest body (stored as a blob). The body carries the
/// manifest's own `mediaType`, which `push_manifest` derives from its single
/// parse, so this returns just the bytes.
async fn read_local_manifest(
    blob_store: &Arc<BlobStore>,
    digest: &Digest,
) -> Result<Vec<u8>, Error> {
    blob_store.read(digest).await.map_err(|e| {
        Error::Registry(RegistryError::Internal(format!(
            "failed to read local manifest blob '{digest}': {e}"
        )))
    })
}

/// HEAD-before-PUT every referenced blob; transfer only the absent ones.
async fn push_blobs(
    downstream: &RegistryClient,
    blob_store: &Arc<BlobStore>,
    metadata_store: &Arc<MetadataStore>,
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
        .map(|blob| async move {
            push_one_blob(downstream, blob_store, metadata_store, namespace, &blob).await
        })
        .buffer_unordered(max_concurrent_pushes.max(1))
        .try_collect::<Vec<()>>()
        .await?;

    Ok(())
}

/// Picks a cross-repo blob-mount source for `digest`: a LOCAL namespace, other
/// than the target, that already references the blob (per the blob index).
///
/// In a bidirectional mesh such a sibling is likely to hold the blob on the
/// downstream too, making it a good `from` hint — a single mount POST then grants
/// a reference with no body transfer. The candidate is only a hint: a wrong guess
/// costs nothing but a fall-back to the full upload (the server answers `202` and
/// opens a session). Returns `None` when no sibling references the blob (so no
/// mount is attempted) and, fail-safe, on any blob-index read error — a missing
/// optimization must never fail a push. The lexicographically smallest sibling is
/// chosen so the `from` is deterministic.
async fn mount_candidate(
    metadata_store: &Arc<MetadataStore>,
    namespace: &str,
    digest: &Digest,
) -> Option<String> {
    let candidates = BlobOwnership::new(metadata_store)
        .referencing_namespaces(digest)
        .await
        .ok()?;
    candidates
        .into_iter()
        .filter(|ns| ns.as_ref() != namespace)
        .min()
        .map(|ns| ns.to_string())
}

/// Transfers a single blob to the downstream if it is not already present,
/// attempting a cross-repo mount before a full upload.
async fn push_one_blob(
    downstream: &RegistryClient,
    blob_store: &Arc<BlobStore>,
    metadata_store: &Arc<MetadataStore>,
    namespace: &str,
    digest: &Digest,
) -> Result<(), Error> {
    // Belt-and-suspenders idempotency: skip the transfer when the downstream
    // already holds the bytes (HEAD-before-PUT).
    let head_location = downstream.get_blob_path(NO_LOCAL_PREFIX, namespace, digest);
    if downstream.head_blob(&[], &head_location).await.is_ok() {
        debug!(namespace, %digest, "Blob already present on downstream; skipping");
        return Ok(());
    }

    let start_location = downstream.get_uploads_start_path(NO_LOCAL_PREFIX, namespace);

    // Cross-repo mount first: when a sibling local namespace references the blob
    // it is likely present on the downstream under that repo, so a single mount
    // POST may grant a reference with no body transfer. The mount is a pure
    // optimization — a miss opens a session, and a rejection (the downstream's
    // `mount-blob` policy denies it) falls through to a plain upload, so it can
    // never fail the push.
    if let Some(from) = mount_candidate(metadata_store, namespace, digest).await {
        match downstream
            .mount_blob(&start_location, digest, Some(&from))
            .await
        {
            Ok(None) => {
                info!(namespace, %digest, %from, "Mounted blob cross-repo on downstream (no transfer)");
                return Ok(());
            }
            Ok(Some(session_url)) => {
                return upload_into_session(
                    downstream,
                    blob_store,
                    namespace,
                    digest,
                    &session_url,
                )
                .await;
            }
            Err(e) => {
                debug!(namespace, %digest, "Cross-repo mount unavailable ({e}); uploading instead");
            }
        }
    }

    // No mount candidate, or the mount was rejected: open a plain upload session.
    let session_url = downstream
        .start_upload(&start_location)
        .await
        .map_err(Error::Registry)?;
    upload_into_session(downstream, blob_store, namespace, digest, &session_url).await
}

/// Streams a local blob's bytes into an already-open upload session and finalizes
/// it (`patch_upload` + `complete_upload`).
async fn upload_into_session(
    downstream: &RegistryClient,
    blob_store: &Arc<BlobStore>,
    namespace: &str,
    digest: &Digest,
    session_url: &str,
) -> Result<(), Error> {
    let (reader, content_length) = blob_store.reader(digest, None).await.map_err(|e| {
        Error::Registry(RegistryError::Internal(format!(
            "failed to open local blob '{digest}': {e}"
        )))
    })?;
    let session_url = downstream
        .patch_upload(session_url, content_length, reader)
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
async fn push_referrers_fallback(
    downstream: &RegistryClient,
    namespace: &str,
    digest: &Digest,
    parsed: &ParsedManifestDigests,
    body: &[u8],
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
    let location = downstream.get_manifest_path(NO_LOCAL_PREFIX, namespace, &reference);

    // GET the existing fallback index; a missing tag (404) starts fresh, a
    // transient failure propagates so the job retries. Any pre-existing referrer
    // descriptors are preserved so this push only adds.
    let mut manifests = fetch_fallback_manifests(downstream, &location).await?;

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
        .put_manifest(&location, Some(OCI_INDEX_MEDIA_TYPE), index_body, source_ts)
        .await
        .map_err(Error::Registry)?;
    Ok(())
}

/// GETs the existing referrers fallback index at `location` and returns its
/// `manifests[]` descriptors.
///
/// A `404` (`ManifestUnknown`) means the fallback tag does not exist yet, so an
/// empty list is returned and the caller starts fresh. Any other error — a
/// transient `5xx`/timeout — is propagated so the caller retries instead of
/// overwriting an existing index from an empty base and dropping every sibling
/// referrer of the subject. A success whose body is not a parseable index yields
/// an empty list (a malformed body would not change on retry).
async fn fetch_fallback_manifests(
    downstream: &RegistryClient,
    location: &str,
) -> Result<Vec<Value>, Error> {
    let body = match downstream.get_manifest(&[], location).await {
        Ok((_, _, body)) => body,
        Err(RegistryError::ManifestUnknown) => return Ok(Vec::new()),
        Err(e) => return Err(Error::Registry(e)),
    };
    Ok(serde_json::from_slice::<Value>(&body)
        .ok()
        .and_then(|v| v.get("manifests").and_then(Value::as_array).cloned())
        .unwrap_or_default())
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
        if let Some(media_type) = parsed.get("mediaType").and_then(Value::as_str) {
            descriptor["mediaType"] = json!(media_type);
        }
        if let Some(artifact_type) = parsed.get("artifactType").and_then(Value::as_str) {
            descriptor["artifactType"] = json!(artifact_type);
        }
    }
    descriptor
}

/// Deletes the manifest bound to `reference` on the downstream.
///
/// `source_ts` is stamped as the `X-Angos-Source-Timestamp` header so the
/// receiver can apply last-writer-wins.
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
    source_ts: Option<&str>,
) -> Result<PushOutcome, Error> {
    let location = downstream.get_manifest_path(NO_LOCAL_PREFIX, namespace, reference);
    let outcome = downstream
        .delete_manifest(&location, source_ts)
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
        matchers::{header, method, path, query_param},
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
            metadata_store::{BlobIndexOperation, MetadataStore, link_kind::LinkKind},
            test_utils::{build_store, build_test_fs_executor, put_blob_direct},
        },
        registry_client::RegistryClient,
        replication::{
            REPLICATION_SUPERSEDED_CODE, X_ANGOS_SOURCE_TIMESTAMP,
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

    fn test_blob_store(root: &str) -> (Arc<BlobStore>, Arc<MetadataStore>, Arc<Store>) {
        let object: Arc<dyn ObjectStore> =
            Arc::new(StorageFsBackend::builder().root_dir(root).build().unwrap());
        let executor = build_test_fs_executor(root, false);
        let store = build_store(object, executor);
        let blob_store = Arc::new(BlobStore::builder().store(store.clone()).build().unwrap());
        let metadata_store = Arc::new(
            MetadataStore::builder()
                .store(store.clone())
                .link_cache_ttl(0)
                .access_time_debounce_secs(0)
                .build()
                .unwrap(),
        );
        (blob_store, metadata_store, store)
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
        let (blob_store, metadata_store, store) = test_blob_store(dir.path().to_str().unwrap());

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
            &metadata_store,
            NAMESPACE,
            &manifest_digest,
            Some("application/vnd.oci.image.manifest.v1+json"),
            Some("v1"),
            manifest_bytes,
            4,
            None,
        )
        .await
        .unwrap();

        // `.expect(1)` on the primary manifest PUT + the fallback index PUT (with
        // the body assertion above) is verified on MockServer drop.
        drop(mock_server);
    }

    #[tokio::test]
    async fn referrers_fallback_propagates_transient_get_error_without_clobbering() {
        // A transient (non-404) failure GETting the existing fallback index must
        // NOT be treated as "tag absent -> start fresh": that would PUT an index
        // built from an empty base and drop the subject's sibling referrers. The
        // push must surface the error so the durable job retries.
        crate::metrics_provider::init_for_tests();
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

        // Primary manifest PUT with NO `OCI-Subject` => OCI-1.0 => fallback runs.
        Mock::given(method("PUT"))
            .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
            .respond_with(
                ResponseTemplate::new(201)
                    .insert_header(DOCKER_CONTENT_DIGEST, manifest_digest.to_string().as_str()),
            )
            .mount(&mock_server)
            .await;

        // The fallback index GET fails transiently (500): the merge-PUT must NOT
        // be attempted and the push must surface the error.
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

        let result = push_manifest(
            &downstream_client(&mock_server.uri()),
            &blob_store,
            &metadata_store,
            NAMESPACE,
            &manifest_digest,
            Some("application/vnd.oci.image.manifest.v1+json"),
            Some("v1"),
            manifest_bytes,
            4,
            None,
        )
        .await;

        assert!(
            result.is_err(),
            "a transient fallback-index GET error must fail the push so the job retries, got: {result:?}"
        );

        // `.expect(0)` on the fallback PUT (no clobbering write) is verified on drop.
        drop(mock_server);
    }

    #[tokio::test]
    async fn no_referrers_fallback_when_downstream_indexes_subject() {
        crate::metrics_provider::init_for_tests();
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
            &metadata_store,
            NAMESPACE,
            &manifest_digest,
            Some("application/vnd.oci.image.manifest.v1+json"),
            Some("v1"),
            manifest_bytes,
            4,
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
        let (blob_store, metadata_store, store) = test_blob_store(dir.path().to_str().unwrap());

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
            &metadata_store,
            NAMESPACE,
            &index_digest,
            Some("application/vnd.oci.image.index.v1+json"),
            Some("v1"),
            index_bytes,
            4,
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

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn push_blob_mounts_cross_repo_when_sibling_namespace_holds_it() {
        // A SIBLING namespace that already references both blobs locally; the
        // pipeline offers it as the mount `from`.
        const SIBLING: &str = "other/repo";

        crate::metrics_provider::init_for_tests();
        let mock_server = MockServer::start().await;
        let dir = TempDir::new().unwrap();
        let (blob_store, metadata_store, store) = test_blob_store(dir.path().to_str().unwrap());

        // A config + layer blob referenced by a single image manifest.
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

        // Record the sibling's ownership of both blobs so a mount `from` exists.
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

        // Both blobs missing in the TARGET namespace -> HEAD 404 -> mount attempt.
        for blob in [&config, &layer] {
            Mock::given(method("HEAD"))
                .and(path(format!("/v2/{NAMESPACE}/blobs/{blob}")))
                .respond_with(ResponseTemplate::new(404))
                .mount(&mock_server)
                .await;
        }
        // The mount POST carries `from=<sibling>` and is granted (201). NO
        // PATCH/PUT upload is mounted, so a fall-back transfer would 404 and fail
        // the push — proving the bytes were never streamed.
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
        // The manifest itself still lands.
        Mock::given(method("PUT"))
            .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
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
            &metadata_store,
            NAMESPACE,
            &manifest_digest,
            Some("application/vnd.oci.image.manifest.v1+json"),
            Some("v1"),
            manifest_bytes,
            4,
            None,
        )
        .await
        .expect("both blobs must mount cross-repo and the manifest must push");

        // `.expect(2)` on the mount POST plus the absence of any PATCH/PUT upload
        // mock proves both blobs were mounted with no body transfer.
        drop(mock_server);
    }

    #[tokio::test]
    async fn push_blob_falls_back_to_upload_when_mount_is_rejected() {
        // A sibling references the blob, so a mount is attempted -- but the
        // downstream rejects it (its `mount-blob` policy denies it). The pipeline
        // must fall back to a normal upload rather than failing the push.
        const SIBLING: &str = "other/repo";

        crate::metrics_provider::init_for_tests();
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

        // Blob missing in the target -> HEAD 404 -> mount attempt.
        Mock::given(method("HEAD"))
            .and(path(format!("/v2/{NAMESPACE}/blobs/{config}")))
            .respond_with(ResponseTemplate::new(404))
            .mount(&mock_server)
            .await;
        // The upload POST: reject the mount (it carries `from`) with 403; open a
        // normal session for the plain (no-`from`) fall-back POST. `.expect(2)`
        // asserts both the mount attempt and the fall-back happen.
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
        // The fall-back upload streams the bytes.
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

        push_manifest(
            &downstream_client(&mock_server.uri()),
            &blob_store,
            &metadata_store,
            NAMESPACE,
            &manifest_digest,
            Some("application/vnd.oci.image.manifest.v1+json"),
            Some("v1"),
            manifest_bytes,
            4,
            None,
        )
        .await
        .expect("a rejected mount must fall back to a normal upload");

        // The `.expect(1)` PATCH + PUT prove the bytes were uploaded after the
        // mount was rejected.
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
    async fn push_manifest_stamps_source_timestamp_header() {
        crate::metrics_provider::init_for_tests();
        let mock_server = MockServer::start().await;
        let dir = TempDir::new().unwrap();
        let (blob_store, metadata_store, store) = test_blob_store(dir.path().to_str().unwrap());
        let (manifest_digest, manifest_bytes) = seed_blobless_manifest(&store).await;

        // The PUT must carry the source-timestamp header with the expected value.
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

        push_manifest(
            &downstream_client(&mock_server.uri()),
            &blob_store,
            &metadata_store,
            NAMESPACE,
            &manifest_digest,
            Some("application/vnd.oci.image.manifest.v1+json"),
            Some("v1"),
            manifest_bytes,
            4,
            Some("2026-06-03T00:00:00Z"),
        )
        .await
        .unwrap();

        // `.expect(1)` (verified on drop) only matches when the header is present
        // with the expected value; a missing/wrong header => no match => the PUT
        // would 404 and the push would error.
        drop(mock_server);
    }

    #[tokio::test]
    async fn push_manifest_skips_put_when_downstream_already_converged() {
        // HEAD-before-PUT optimization: when the downstream already resolves the
        // target tag to THIS digest, the manifest PUT is skipped. Mount ONLY a
        // HEAD returning the matching digest; deliberately mount NO manifest PUT
        // mock, so a wrongly-issued PUT would 404 and fail the push.
        crate::metrics_provider::init_for_tests();
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

        push_manifest(
            &downstream_client(&mock_server.uri()),
            &blob_store,
            &metadata_store,
            NAMESPACE,
            &manifest_digest,
            Some("application/vnd.oci.image.manifest.v1+json"),
            Some("v1"),
            manifest_bytes,
            4,
            None,
        )
        .await
        .expect("a converged downstream must skip the PUT and succeed");

        // No PUT mock exists; `.expect(1)` on the HEAD (verified on drop) proves
        // the probe ran and the absence of an error proves no PUT was attempted.
        drop(mock_server);
    }

    #[tokio::test]
    async fn no_referrers_fallback_on_converged_path() {
        // A subject-bearing manifest already converged on the downstream (HEAD
        // matches THIS digest) must NOT re-push the referrers fallback: the
        // original push created it for OCI-1.0, and OCI-1.1 auto-indexes. The
        // fallback-tag PUT is mounted with expect(0), so any re-push fails the test.
        crate::metrics_provider::init_for_tests();
        let mock_server = MockServer::start().await;
        let dir = TempDir::new().unwrap();
        let (blob_store, metadata_store, store) = test_blob_store(dir.path().to_str().unwrap());

        let subject = put_blob_direct(&store, b"subject-bytes").await;
        // Config-less subject manifest, so the converged push has no referenced
        // blob to probe; only the referrers fallback would (wrongly) re-run.
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

        // The manifest is already converged: HEAD by tag resolves to THIS digest.
        Mock::given(method("HEAD"))
            .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header(DOCKER_CONTENT_DIGEST, manifest_digest.to_string().as_str())
                    .insert_header("Content-Length", manifest_bytes.len().to_string().as_str()),
            )
            .mount(&mock_server)
            .await;

        // The fallback tag must NOT be touched on the converged path.
        let fallback_tag = format!("{}-{}", subject.algorithm(), subject.hash());
        Mock::given(method("PUT"))
            .and(path(format!("/v2/{NAMESPACE}/manifests/{fallback_tag}")))
            .respond_with(ResponseTemplate::new(201))
            .expect(0)
            .mount(&mock_server)
            .await;

        push_manifest(
            &downstream_client(&mock_server.uri()),
            &blob_store,
            &metadata_store,
            NAMESPACE,
            &manifest_digest,
            Some("application/vnd.oci.image.manifest.v1+json"),
            Some("v1"),
            manifest_bytes,
            4,
            None,
        )
        .await
        .expect("converged push must succeed");

        // `.expect(0)` on the fallback PUT (no re-push on the converged path).
        drop(mock_server);
    }

    #[tokio::test]
    async fn push_manifest_puts_when_downstream_holds_a_different_digest() {
        // HEAD returns a DIFFERENT digest (tag moved / divergence): the PUT must
        // still run so LWW on the receiver can arbitrate.
        crate::metrics_provider::init_for_tests();
        let mock_server = MockServer::start().await;
        let dir = TempDir::new().unwrap();
        let (blob_store, metadata_store, store) = test_blob_store(dir.path().to_str().unwrap());
        let (manifest_digest, manifest_bytes) = seed_blobless_manifest(&store).await;

        let other_digest =
            "sha256:1111111111111111111111111111111111111111111111111111111111111111";
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

        push_manifest(
            &downstream_client(&mock_server.uri()),
            &blob_store,
            &metadata_store,
            NAMESPACE,
            &manifest_digest,
            Some("application/vnd.oci.image.manifest.v1+json"),
            Some("v1"),
            manifest_bytes,
            4,
            None,
        )
        .await
        .expect("a divergent downstream digest must still PUT");

        // `.expect(1)` on the PUT (verified on drop) proves the PUT ran.
        drop(mock_server);
    }

    #[tokio::test]
    async fn push_manifest_puts_when_downstream_head_returns_404() {
        // HEAD-before-PUT must fail OPEN: when the target reference does not exist
        // downstream (HEAD 404 — the common first-replication case), `is_ok_and`
        // is false, so the manifest PUT still runs. Mount NO HEAD mock, so wiremock
        // answers HEAD with a default 404; mount the PUT with `.expect(1)`.
        crate::metrics_provider::init_for_tests();
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

        push_manifest(
            &downstream_client(&mock_server.uri()),
            &blob_store,
            &metadata_store,
            NAMESPACE,
            &manifest_digest,
            Some("application/vnd.oci.image.manifest.v1+json"),
            Some("v1"),
            manifest_bytes,
            4,
            None,
        )
        .await
        .expect("a 404 HEAD must fall through to the PUT");

        // `.expect(1)` on the PUT (verified on drop) proves the probe failed open
        // and the PUT ran.
        drop(mock_server);
    }

    #[tokio::test]
    async fn push_manifest_treats_lww_superseded_409_as_success() {
        crate::metrics_provider::init_for_tests();
        let mock_server = MockServer::start().await;
        let dir = TempDir::new().unwrap();
        let (blob_store, metadata_store, store) = test_blob_store(dir.path().to_str().unwrap());
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
            &metadata_store,
            NAMESPACE,
            &manifest_digest,
            Some("application/vnd.oci.image.manifest.v1+json"),
            Some("v1"),
            manifest_bytes,
            4,
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
        let (blob_store, metadata_store, store) = test_blob_store(dir.path().to_str().unwrap());
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
            &metadata_store,
            NAMESPACE,
            &manifest_digest,
            Some("application/vnd.oci.image.manifest.v1+json"),
            Some("v1"),
            manifest_bytes,
            4,
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
    async fn delete_manifest_stamps_header_and_distinguishes_superseded() {
        crate::metrics_provider::init_for_tests();
        let mock_server = MockServer::start().await;

        // A delete carrying the source-timestamp header; respond with an
        // LWW-superseded 409 => the pipeline treats it as success.
        Mock::given(method("DELETE"))
            .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
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
        )
        .await;
        assert!(
            result.is_err(),
            "a non-superseded delete-409 must propagate as an error"
        );

        drop(mock_server);
    }
}
