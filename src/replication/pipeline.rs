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

use std::{collections::HashSet, str::FromStr, sync::Arc};

use futures_util::stream::{self, StreamExt, TryStreamExt};
use serde_json::{Value, json};
use tracing::{debug, info, instrument, warn};

use crate::{
    oci::{Digest, OCI_INDEX_MEDIA_TYPE, OCI_MANIFEST_MEDIA_TYPE, Reference},
    registry::{
        Error as RegistryError, ParsedManifestDigests,
        blob_ownership::BlobOwnership,
        blob_store::BlobStore,
        metadata_store::{MetadataStore, link_kind::LinkKind},
        parse_manifest_digests,
    },
    registry_client::{DeleteManifestOutcome, NO_LOCAL_PREFIX, RegistryClient},
    replication::{Error, manifest_accept_types},
};

/// Outcome of a successful replication push or delete.
///
/// Both arms are convergence (the system reached the intended state); the
/// distinction lets the caller record `pushed` vs `superseded` metrics. A push
/// the downstream actively applied is [`PushOutcome::Pushed`]; a last-writer-wins
/// loss (the downstream already holds a strictly-newer copy) is
/// [`PushOutcome::Superseded`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PushOutcome {
    /// The downstream accepted and applied the change (a PUT/DELETE was issued).
    Pushed,
    /// The downstream already held this exact digest, so the PUT was skipped
    /// (HEAD-before-PUT convergence). Nothing was transferred.
    Converged,
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
/// receiver can apply last-writer-wins; it is stamped on the primary manifest
/// PUT only. The referrers-fallback index PUT is deliberately timestamp-less:
/// the fallback tag is a merged set, not an LWW register (see
/// [`push_referrers_fallback`]).
///
/// 409 disambiguation: when the downstream rejects the push by last-writer-wins
/// (a `409` whose OCI code is the replication-superseded code, surfaced by the
/// client as `PutManifestResult { superseded: true, .. }`) the push is treated
/// as **success**: the downstream already holds a strictly-newer copy, so the
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
    media_type: Option<String>,
    tag: Option<&str>,
    body: Vec<u8>,
    max_concurrent_pushes: usize,
    source_ts: Option<&str>,
) -> Result<PushOutcome, Error> {
    let parsed = parse_manifest_digests(&body, media_type.as_ref()).map_err(Error::Registry)?;

    // 1. Recurse into child manifests FIRST (image index / manifest list). The
    //    parent index must not land on the downstream before its children.
    for child in &parsed.manifests {
        let child_body = blob_store.read(child).await.map_err(|e| {
            Error::Registry(RegistryError::Internal(format!(
                "failed to read local manifest blob '{child}': {e}"
            )))
        })?;
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

    // 3a. HEAD-before-PUT (bandwidth optimization, NOT loop prevention: the
    //     receiver-side no-op dispatch gate breaks cycles; this just avoids one
    //     wasted PUT once converged). Probe the target reference: if the
    //     downstream already resolves it to THIS digest, it is converged, so skip
    //     the manifest PUT.
    //
    //     Only a manifest WITHOUT a subject may take this skip. A subject-bearing
    //     manifest must always run the PUT below, because the PUT's `OCI-Subject`
    //     response is what tells us whether the downstream auto-indexed the
    //     subject (OCI-1.1) or still needs the referrers fallback tag (OCI-1.0):
    //     a converged primary does NOT imply the fallback landed: the original
    //     push's fallback PUT can fail after the primary succeeded, and skipping
    //     here would strand the referrer while the job reports success. The PUT is
    //     idempotent and a referrer manifest is small, so re-issuing it is cheap.
    if parsed.subject.is_none()
        && downstream
            .head_manifest(&manifest_accept_types(), &location)
            .await
            .is_ok_and(|(_, downstream_digest, _)| &downstream_digest == digest)
    {
        info!(
            namespace,
            %digest,
            ?tag,
            "Downstream already holds this manifest; skipping PUT (converged)"
        );
        return Ok(PushOutcome::Converged);
    }

    // The referrers fallback below borrows the manifest body, but only a
    // subject-bearing manifest can reach it, so retain a copy for that path alone
    // and move the body straight into the PUT on the common (no-subject) path.
    let fallback_body = parsed.subject.is_some().then(|| body.clone());

    // The PUT content type is the explicit `media_type` override when given, else
    // the manifest body's own declared mediaType (surfaced by the parse above so
    // the body is parsed once), else the type recorded on the local revision link
    // when the manifest was stored. A body may legitimately omit `mediaType` while
    // the original push carried it in the `Content-Type` header (which
    // `store_manifest` records on the link), and the receiver rejects a manifest
    // PUT that carries no `Content-Type`, so recovering it from the link keeps a
    // body-typeless manifest replicable instead of stalling on a 400. Resolved
    // here, after the converged-skip, so a skipped push never does the link read.
    let effective_media_type = match media_type.or_else(|| parsed.media_type.clone()) {
        Some(media_type) => Some(media_type),
        None => metadata_store
            .read_link(namespace, &LinkKind::Digest(digest.clone()), false)
            .await
            .ok()
            .and_then(|link| link.media_type),
    };

    let result = downstream
        .put_manifest(&location, effective_media_type.as_deref(), body, source_ts)
        .await
        .map_err(Error::Registry)?;

    // 3c. LWW loss: the downstream already holds a strictly-newer copy. This is
    //     convergence, not failure: drop the push (and skip the referrers
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
    //    `fallback_body` is `Some` only when the manifest carries a subject.
    if let Some(body) = fallback_body.filter(|_| result.subject.is_none()) {
        push_referrers_fallback(
            downstream,
            metadata_store,
            namespace,
            digest,
            &parsed,
            &body,
        )
        .await?;
    }

    Ok(PushOutcome::Pushed)
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
    // Dedup while preserving order: a manifest may legally repeat a digest
    // (e.g. an identical layer listed twice), and two concurrent pushes of the
    // same absent blob would both HEAD-miss and both run the full upload.
    let mut seen = HashSet::new();
    let blobs: Vec<Digest> = parsed
        .config
        .iter()
        .chain(parsed.layers.iter())
        .filter(|digest| seen.insert(*digest))
        .cloned()
        .collect();

    stream::iter(blobs)
        .map(|blob| async move {
            push_one_blob(downstream, blob_store, metadata_store, namespace, &blob).await
        })
        // 0 is impossible from config (rejected at parse); the `.max(1)` floor
        // guards against a direct builder misuse so `buffer_unordered` is never 0.
        .buffer_unordered(max_concurrent_pushes.max(1))
        .try_collect::<Vec<()>>()
        .await?;

    Ok(())
}

/// Picks a cross-repo blob-mount source for `digest`: a LOCAL namespace, other
/// than the target, that already references the blob (per the blob index).
///
/// In a bidirectional mesh such a sibling is likely to hold the blob on the
/// downstream too, making it a good `from` hint: a single mount POST then grants
/// a reference with no body transfer. The candidate is only a hint: a wrong guess
/// costs nothing but a fall-back to the full upload (the server answers `202` and
/// opens a session). Returns `None` when no sibling references the blob (so no
/// mount is attempted) and, fail-safe, on any blob-index read error (a missing
/// optimization must never fail a push). The lexicographically smallest sibling is
/// chosen so the `from` is deterministic.
async fn mount_candidate(
    metadata_store: &Arc<MetadataStore>,
    namespace: &str,
    digest: &Digest,
) -> Option<String> {
    BlobOwnership::new(metadata_store)
        .smallest_referencing_namespace(digest, namespace)
        .await
        .ok()
        .flatten()
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
    // optimization: a miss opens a session, and a rejection (the downstream's
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
/// index, so a second referrer of the same subject does not clobber the first.
///
/// The fallback PUT carries NO `source_ts`: the fallback tag is a merged SET,
/// not a last-writer-wins register, so it must never lose an LWW comparison.
/// A `409 REPLICATION_SUPERSEDED` here would silently drop the just-merged
/// descriptor while the job reports success (a stranded referrer).
async fn push_referrers_fallback(
    downstream: &RegistryClient,
    metadata_store: &Arc<MetadataStore>,
    namespace: &str,
    digest: &Digest,
    parsed: &ParsedManifestDigests,
    body: &[u8],
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

    // Serialize the GET→merge→PUT below across concurrent replication jobs.
    // Two referrers of the same subject are distinct jobs (distinct job
    // `lock_key`s, so the queue runs them concurrently) merging into the same
    // fallback tag: unserialized, both read the same base index and the loser's
    // descriptor vanishes from the winner's PUT. The lock lives on the metadata
    // executor, the lock domain every drain of this store (server in-process
    // queue, `angos worker`, scrub) shares. It cannot cover an unrelated sender
    // registry pushing to the same downstream; that residual race needs
    // conditional-request support on the downstream.
    let lock_keys = [referrers_fallback_lock_key(namespace, subject)];
    let session = metadata_store
        .executor()
        .acquire(&lock_keys)
        .await
        .map_err(|e| {
            Error::Registry(RegistryError::Internal(format!(
                "referrers fallback lock acquire failed for '{fallback_tag}': {e}"
            )))
        })?;
    let result = merge_referrers_fallback(downstream, &location, digest, parsed, body).await;
    session.release().await;
    result
}

/// Lock key serializing the fallback-index read-modify-write for one subject.
///
/// Deliberately downstream-agnostic (same-subject merges to two different
/// downstreams also serialize) because the critical section is two short HTTP
/// calls and a per-downstream key would thread the downstream name through the
/// pipeline for contention that never matters in practice.
fn referrers_fallback_lock_key(namespace: &str, subject: &Digest) -> String {
    format!("replication-referrers:{namespace}:{subject}")
}

/// The locked critical section of [`push_referrers_fallback`]: fetches the
/// downstream's current fallback index, appends this referrer's descriptor when
/// absent, and PUTs the merged index back.
async fn merge_referrers_fallback(
    downstream: &RegistryClient,
    location: &str,
    digest: &Digest,
    parsed: &ParsedManifestDigests,
    body: &[u8],
) -> Result<(), Error> {
    // GET the existing fallback index; a missing tag (404) starts fresh, a
    // transient failure propagates so the job retries. Any pre-existing referrer
    // descriptors are preserved so this push only adds.
    let mut manifests = fetch_fallback_manifests(downstream, location).await?;

    // Descriptor for the referrer manifest just pushed. `body`'s digest is the
    // `digest` already resolved on the push path (the blob store is
    // content-addressed), so reuse it rather than re-hashing the body; its
    // media/artifact type come from the single parse, not a re-parse of `body`.
    let descriptor = referrer_descriptor(
        digest,
        body.len(),
        parsed.media_type.as_deref(),
        parsed.artifact_type.as_deref(),
    );

    // Dedup-merge by digest so a re-run is idempotent. Render the digest once
    // rather than per scanned descriptor.
    let digest_str = digest.to_string();
    let already_present = manifests
        .iter()
        .any(|m| m.get("digest").and_then(Value::as_str) == Some(digest_str.as_str()));
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

    // Timestamp-less PUT: with no `X-Angos-Source-Timestamp` the receiver skips
    // LWW entirely, so the merged index can never come back `superseded` and
    // silently drop a descriptor (the fallback tag is a set, not a register).
    downstream
        .put_manifest(location, Some(OCI_INDEX_MEDIA_TYPE), index_body, None)
        .await
        .map_err(Error::Registry)?;
    Ok(())
}

/// GETs the existing referrers fallback index at `location` and returns its
/// `manifests[]` descriptors.
///
/// A `404` (`ManifestUnknown`) means the fallback tag does not exist yet, so an
/// empty list is returned and the caller starts fresh. Any other GET error (a
/// transient `5xx`/timeout) is propagated so the caller retries instead of
/// overwriting an existing index from an empty base and dropping every sibling
/// referrer of the subject. A `200` whose body is not a parseable image index
/// (unparseable JSON, or a missing/non-array `manifests`) is an error too, not an
/// empty base: starting fresh would clobber the existing referrers, so a corrupt
/// downstream index is surfaced (the job retries / dead-letters) rather than
/// silently overwritten.
async fn fetch_fallback_manifests(
    downstream: &RegistryClient,
    location: &str,
) -> Result<Vec<Value>, Error> {
    let body = match downstream
        .get_manifest(&manifest_accept_types(), location)
        .await
    {
        Ok((_, _, body)) => body,
        Err(RegistryError::ManifestUnknown) => return Ok(Vec::new()),
        Err(e) => return Err(Error::Registry(e)),
    };
    serde_json::from_slice::<Value>(&body)
        .ok()
        .and_then(|v| v.get("manifests").and_then(Value::as_array).cloned())
        .ok_or_else(|| {
            Error::Registry(RegistryError::Internal(format!(
                "downstream referrers fallback index at '{location}' is not a parseable image \
                 index (missing or non-array `manifests`); refusing to overwrite it"
            )))
        })
}

/// Builds an OCI descriptor for a referrer manifest to embed in the fallback
/// index: `mediaType`, `digest`, `size`, and `artifactType`. `media_type` and
/// `artifact_type` come from the referrer body's single `parse_manifest_digests`,
/// so the descriptor is built without re-parsing the body; `media_type` defaults
/// to the OCI image-manifest type when the body declared none.
fn referrer_descriptor(
    referrer_digest: &Digest,
    size: usize,
    media_type: Option<&str>,
    artifact_type: Option<&str>,
) -> Value {
    let mut descriptor = json!({
        "mediaType": media_type.unwrap_or(OCI_MANIFEST_MEDIA_TYPE),
        "digest": referrer_digest.to_string(),
        "size": size,
    });
    if let Some(artifact_type) = artifact_type {
        descriptor["artifactType"] = json!(artifact_type);
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
/// as **success** (convergence, not failure). Any *other* downstream error
/// (including an immutable-tag conflict 409) propagates as [`Error::Registry`]
/// so the job retries/dead-letters.
///
/// On success returns [`PushOutcome::Pushed`] when the downstream applied the
/// delete, [`PushOutcome::Converged`] when the target was already absent (a
/// no-op `404`, mirroring the push path's HEAD-converged skip), or
/// [`PushOutcome::Superseded`] when it lost a last-writer-wins comparison
/// (all three are convergence; the caller records them as distinct metrics
/// outcomes).
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
        DeleteManifestOutcome::AlreadyAbsent => {
            info!(
                namespace,
                %reference,
                "Downstream already lacked this manifest; delete is a no-op (converged)"
            );
            Ok(PushOutcome::Converged)
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
        Arc, Mutex,
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
        cache, metrics_provider,
        oci::{
            DOCKER_MANIFEST_LIST_MEDIA_TYPE, DOCKER_MANIFEST_MEDIA_TYPE, Digest,
            OCI_INDEX_MEDIA_TYPE, OCI_MANIFEST_MEDIA_TYPE, Reference,
        },
        registry::{
            DOCKER_CONTENT_DIGEST, OCI_SUBJECT,
            blob_store::BlobStore,
            manifest::DEFAULT_MAX_MANIFEST_SIZE_BYTES,
            metadata_store::{
                BlobIndexOperation, LinkOperation, MetadataStore, link_kind::LinkKind,
            },
            test_utils::{build_store, build_test_fs_executor, put_blob_direct},
        },
        registry_client::RegistryClient,
        replication::{
            REPLICATION_SUPERSEDED_CODE, X_ANGOS_SOURCE_TIMESTAMP,
            pipeline::{PushOutcome, delete_manifest, push_manifest},
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
        metrics_provider::init_for_tests();
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

        // The primary manifest PUT by tag: NO `OCI-Subject` header => OCI-1.0
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
            Some("application/vnd.oci.image.manifest.v1+json".to_string()),
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
    async fn referrers_fallback_put_is_timestamp_less() {
        // The fallback tag is a merged SET, not an LWW register: a stamped
        // fallback PUT could come back `409 REPLICATION_SUPERSEDED` and the
        // just-merged descriptor would silently vanish while the job reports
        // success. Pin that the primary PUT carries the source timestamp while
        // the fallback-index PUT does NOT.
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

        // Primary PUT: must carry the header; NO `OCI-Subject` => fallback runs.
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

        // Fallback index: GET 404 (fresh), then a PUT that must NOT carry the
        // source-timestamp header.
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

        push_manifest(
            &downstream_client(&mock_server.uri()),
            &blob_store,
            &metadata_store,
            NAMESPACE,
            &manifest_digest,
            None,
            Some("v1"),
            manifest_bytes,
            4,
            Some("2026-06-03T00:00:00Z"),
        )
        .await
        .expect("subject push with a stamped primary and timestamp-less fallback");
        drop(mock_server);
    }

    #[tokio::test]
    async fn referrers_fallback_propagates_transient_get_error_without_clobbering() {
        // A transient (non-404) failure GETting the existing fallback index must
        // NOT be treated as "tag absent -> start fresh": that would PUT an index
        // built from an empty base and drop the subject's sibling referrers. The
        // push must surface the error so the durable job retries.
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
            Some("application/vnd.oci.image.manifest.v1+json".to_string()),
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
    async fn referrers_fallback_errors_on_unparseable_index_without_clobbering() {
        // A 200 whose body is not a usable image index (here: valid JSON with no
        // `manifests` array) must NOT be treated as an empty base: rebuilding the
        // index from empty and PUTting it would drop the subject's existing sibling
        // referrers. The push must surface the error so the durable job retries /
        // dead-letters rather than silently overwriting a corrupt downstream index.
        metrics_provider::init_for_tests();
        let mock_server = MockServer::start().await;
        let dir = TempDir::new().unwrap();
        let (blob_store, metadata_store, store) = test_blob_store(dir.path().to_str().unwrap());

        let subject = put_blob_direct(&store, b"subject-bytes").await;
        // Config-less subject manifest: only the primary manifest and the fallback
        // are pushed (no blob upload to mock).
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

        // Primary manifest PUT with NO `OCI-Subject` => OCI-1.0 => fallback runs.
        Mock::given(method("PUT"))
            .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
            .respond_with(
                ResponseTemplate::new(201)
                    .insert_header(DOCKER_CONTENT_DIGEST, manifest_digest.to_string().as_str()),
            )
            .mount(&mock_server)
            .await;

        // The fallback index GET returns a 200 (with a valid Docker-Content-Digest,
        // so the client accepts the response) whose body parses as JSON but has no
        // `manifests` array (a corrupt/unexpected index). The merge-PUT must NOT run.
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

        let result = push_manifest(
            &downstream_client(&mock_server.uri()),
            &blob_store,
            &metadata_store,
            NAMESPACE,
            &manifest_digest,
            None,
            Some("v1"),
            manifest_bytes,
            4,
            None,
        )
        .await;

        assert!(
            result.is_err(),
            "a malformed fallback index must fail the push so it is not overwritten, got: {result:?}"
        );

        // `.expect(0)` on the fallback PUT (no clobbering write) is verified on drop.
        drop(mock_server);
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn concurrent_same_subject_referrers_merge_without_lost_update() {
        // Two referrers of the same subject are distinct jobs with distinct job
        // `lock_key`s, so the queue runs them concurrently, and both
        // GET→merge→PUT the same fallback tag. The store lock inside
        // `push_referrers_fallback` serializes the merges; without it both reads
        // see the same base index and one descriptor vanishes from the final PUT.
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

        // Primary manifest PUTs: NO `OCI-Subject` => OCI-1.0 => fallback runs.
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

        // A stateful fallback tag endpoint: GET serves whatever the last PUT
        // stored (404 before the first), like a real registry, so the second
        // merge only sees the first descriptor if the merges serialized.
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
        let (a, b) = tokio::join!(
            push_manifest(
                &client,
                &blob_store,
                &metadata_store,
                NAMESPACE,
                &digest_a,
                None,
                Some("v1"),
                bytes_a,
                4,
                None,
            ),
            push_manifest(
                &client,
                &blob_store,
                &metadata_store,
                NAMESPACE,
                &digest_b,
                None,
                Some("v2"),
                bytes_b,
                4,
                None,
            ),
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
            Some("application/vnd.oci.image.manifest.v1+json".to_string()),
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
        metrics_provider::init_for_tests();
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
            Some("application/vnd.oci.image.index.v1+json".to_string()),
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

        metrics_provider::init_for_tests();
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
        // the push, proving the bytes were never streamed.
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
            Some("application/vnd.oci.image.manifest.v1+json".to_string()),
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
        // A sibling references the blob, so a mount is attempted, but the
        // downstream rejects it (its `mount-blob` policy denies it). The pipeline
        // must fall back to a normal upload rather than failing the push.
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
            Some("application/vnd.oci.image.manifest.v1+json".to_string()),
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
        metrics_provider::init_for_tests();
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
            Some("application/vnd.oci.image.manifest.v1+json".to_string()),
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

        let outcome = push_manifest(
            &downstream_client(&mock_server.uri()),
            &blob_store,
            &metadata_store,
            NAMESPACE,
            &manifest_digest,
            Some("application/vnd.oci.image.manifest.v1+json".to_string()),
            Some("v1"),
            manifest_bytes,
            4,
            None,
        )
        .await
        .expect("a converged downstream must skip the PUT and succeed");
        assert_eq!(
            outcome,
            PushOutcome::Converged,
            "a HEAD-matched skip must report Converged, not Pushed, so the metric distinguishes a no-op"
        );

        // No PUT mock exists; `.expect(1)` on the HEAD (verified on drop) proves
        // the probe ran and the absence of an error proves no PUT was attempted.
        drop(mock_server);
    }

    #[tokio::test]
    async fn repeated_layer_digest_uploads_the_blob_once() {
        // A manifest may legally list the same layer digest twice. Without
        // dedup, both entries HEAD-miss concurrently and both run the full
        // upload of the same blob. Every upload mock is pinned to `.expect(1)`,
        // so a second concurrent upload fails the test on drop.
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

        // Blob absent on the downstream: exactly ONE probe and ONE upload.
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

        push_manifest(
            &downstream_client(&mock_server.uri()),
            &blob_store,
            &metadata_store,
            NAMESPACE,
            &manifest_digest,
            None,
            Some("v1"),
            manifest_bytes,
            4,
            None,
        )
        .await
        .expect("a manifest repeating a layer digest must push it once");

        // The `.expect(1)` mocks (verified on MockServer drop) prove the
        // duplicate entry triggered no second probe or upload.
        drop(mock_server);
    }

    #[tokio::test]
    async fn converged_skip_head_sends_standard_accept_headers() {
        // The converged-skip HEAD must advertise the standard OCI + Docker
        // manifest media types: without an `Accept` header a content-negotiating
        // downstream may return a CONVERTED representation whose digest never
        // matches the local one, so the skip never fires and every push
        // re-transfers a converged manifest. The HEAD responder asserts all four
        // values arrived (the scrub reconcile HEAD and the referrers-fallback
        // GET stamp the same `manifest_accept_types()` set).
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

        let outcome = push_manifest(
            &downstream_client(&mock_server.uri()),
            &blob_store,
            &metadata_store,
            NAMESPACE,
            &manifest_digest,
            Some("application/vnd.oci.image.manifest.v1+json".to_string()),
            Some("v1"),
            manifest_bytes,
            4,
            None,
        )
        .await
        .expect("the Accept-stamped HEAD must still drive the converged skip");
        assert_eq!(outcome, PushOutcome::Converged);
        drop(mock_server);
    }

    #[tokio::test]
    async fn converged_subject_manifest_still_pushes_referrers_fallback() {
        // A subject-bearing manifest the downstream ALREADY holds must NOT take
        // the converged HEAD-skip. The original push's primary PUT can land while
        // its referrers-fallback PUT fails transiently; a blanket converged-skip
        // on the next attempt would then never retry the fallback, stranding the
        // referrer while the job reports success. So the primary is re-PUT
        // (idempotent) and, on an OCI-1.0 downstream (no `OCI-Subject` on the PUT
        // response), the referrers fallback is re-pushed.
        metrics_provider::init_for_tests();
        let mock_server = MockServer::start().await;
        let dir = TempDir::new().unwrap();
        let (blob_store, metadata_store, store) = test_blob_store(dir.path().to_str().unwrap());

        let subject = put_blob_direct(&store, b"subject-bytes").await;
        // Config-less subject manifest, so the push has no referenced blob to
        // probe; only the primary manifest and the referrers fallback are pushed.
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

        // The downstream already resolves the tag to THIS digest (the converged
        // state a prior attempt's primary PUT left behind). A subject-bearing
        // manifest bypasses the HEAD-skip, so the HEAD is never consulted; mounting
        // it (no `.expect()`) documents the converged state without requiring it.
        Mock::given(method("HEAD"))
            .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header(DOCKER_CONTENT_DIGEST, manifest_digest.to_string().as_str())
                    .insert_header("Content-Length", manifest_bytes.len().to_string().as_str()),
            )
            .mount(&mock_server)
            .await;

        // The primary PUT is re-issued (idempotent) and returns NO `OCI-Subject`,
        // i.e. an OCI-1.0 downstream that does not auto-index the subject.
        Mock::given(method("PUT"))
            .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
            .respond_with(
                ResponseTemplate::new(201)
                    .insert_header(DOCKER_CONTENT_DIGEST, manifest_digest.to_string().as_str()),
            )
            .expect(1)
            .mount(&mock_server)
            .await;

        // The referrers fallback index is fetched (absent -> 404 -> start fresh)
        // and PUT, proving the fallback is not stranded by the converged primary.
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

        push_manifest(
            &downstream_client(&mock_server.uri()),
            &blob_store,
            &metadata_store,
            NAMESPACE,
            &manifest_digest,
            None,
            Some("v1"),
            manifest_bytes,
            4,
            None,
        )
        .await
        .expect("a converged subject-bearing manifest must re-push the referrers fallback");

        // `.expect(1)` on both the primary PUT and the fallback PUT (verified on
        // drop) proves the converged HEAD-skip was bypassed and the fallback ran.
        drop(mock_server);
    }

    #[tokio::test]
    async fn push_manifest_puts_when_downstream_holds_a_different_digest() {
        // HEAD returns a DIFFERENT digest (tag moved / divergence): the PUT must
        // still run so LWW on the receiver can arbitrate.
        metrics_provider::init_for_tests();
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
            Some("application/vnd.oci.image.manifest.v1+json".to_string()),
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
        // downstream (HEAD 404, the common first-replication case), `is_ok_and`
        // is false, so the manifest PUT still runs. Mount NO HEAD mock, so wiremock
        // answers HEAD with a default 404; mount the PUT with `.expect(1)`.
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

        push_manifest(
            &downstream_client(&mock_server.uri()),
            &blob_store,
            &metadata_store,
            NAMESPACE,
            &manifest_digest,
            Some("application/vnd.oci.image.manifest.v1+json".to_string()),
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
    async fn push_manifest_recovers_content_type_from_the_link_for_a_typeless_body() {
        // Production always passes `None` as the media_type override (the handler
        // reads only the blob body), and a manifest body may legitimately omit a
        // top-level `mediaType`. Without recovering the type from the local
        // revision link, the PUT would carry NO `Content-Type` and the receiver
        // rejects it 400. Assert the PUT instead carries the link's stored type.
        metrics_provider::init_for_tests();
        let mock_server = MockServer::start().await;
        let dir = TempDir::new().unwrap();
        let (blob_store, metadata_store, store) = test_blob_store(dir.path().to_str().unwrap());

        // A body with NO top-level `mediaType`, so the parse surfaces `None`.
        let manifest = json!({ "schemaVersion": 2, "layers": [] });
        let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
        let manifest_digest = put_blob_direct(&store, &manifest_bytes).await;

        // Seed the revision link with the type the original push carried in its
        // `Content-Type` header (what `store_manifest` records on the link).
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

        // The PUT must carry the recovered Content-Type; without the link
        // recovery the header is absent, this matcher never fires, and the
        // `.expect(1)` fails on drop.
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

        push_manifest(
            &downstream_client(&mock_server.uri()),
            &blob_store,
            &metadata_store,
            NAMESPACE,
            &manifest_digest,
            None,
            Some("v1"),
            manifest_bytes,
            4,
            None,
        )
        .await
        .expect("a typeless body must recover its Content-Type from the revision link");

        drop(mock_server);
    }

    #[tokio::test]
    async fn push_index_recovers_typeless_child_content_type_from_link() {
        // The child-recursion counterpart of the test above: a CHILD manifest of
        // an index may itself omit a top-level `mediaType`. The child is pushed
        // by digest (before the parent index), with the production `None`
        // override, so its PUT must carry the Content-Type recovered from the
        // child's revision link. Otherwise the receiver 400s and the index
        // never lands.
        metrics_provider::init_for_tests();
        let mock_server = MockServer::start().await;
        let dir = TempDir::new().unwrap();
        let (blob_store, metadata_store, store) = test_blob_store(dir.path().to_str().unwrap());

        // A typeless child manifest referenced by a (typed) image index.
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

        // Seed ONLY the child's revision link with its stored media type.
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

        // The child PUT (by digest) must carry the recovered Content-Type.
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
        // The parent index PUT (by tag) carries the index body's own mediaType.
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

        push_manifest(
            &downstream_client(&mock_server.uri()),
            &blob_store,
            &metadata_store,
            NAMESPACE,
            &index_digest,
            None,
            Some("v1"),
            index_bytes,
            4,
            None,
        )
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
            Some("application/vnd.oci.image.manifest.v1+json".to_string()),
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
        metrics_provider::init_for_tests();
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
            Some("application/vnd.oci.image.manifest.v1+json".to_string()),
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
        metrics_provider::init_for_tests();
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
    async fn delete_manifest_of_absent_target_is_converged_not_pushed() {
        // A 404 delete (the target is already gone) is a no-op, mirroring the
        // push path's HEAD-converged skip: it must record
        // `outcome="converged"`, not `pushed`, so the metric distinguishes an
        // applied delete from a converged retry/bounce.
        metrics_provider::init_for_tests();
        let mock_server = MockServer::start().await;

        Mock::given(method("DELETE"))
            .and(path(format!("/v2/{NAMESPACE}/manifests/gone")))
            .respond_with(ResponseTemplate::new(404))
            .expect(1)
            .mount(&mock_server)
            .await;

        let outcome = delete_manifest(
            &downstream_client(&mock_server.uri()),
            NAMESPACE,
            &Reference::Tag("gone".to_string()),
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
    async fn delete_manifest_propagates_non_superseded_409_as_error() {
        metrics_provider::init_for_tests();
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
