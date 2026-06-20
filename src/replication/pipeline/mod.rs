//! The replication push pipeline: one code path drives both the event-driven
//! and the scrub-reconcile push of a `(namespace, digest, tag?)` job to a
//! downstream [`RegistryClient`].
//!
//! Idempotency is mandatory (the queue is at-least-once): blobs are HEAD-probed
//! before transfer and child manifests land before the parent index, so a
//! re-run of an already-converged manifest costs a single no-op HEAD.

use std::{collections::HashSet, str::FromStr, sync::Arc, time::Duration};

use futures_util::stream::{self, StreamExt, TryStreamExt};
use serde_json::{Value, json};
use tokio::time::timeout;
use tracing::{debug, info, instrument, warn};

use crate::{
    oci::{Digest, OCI_INDEX_MEDIA_TYPE, OCI_MANIFEST_MEDIA_TYPE, Reference, Tag},
    registry::{
        Error as RegistryError, ParsedManifestDigests,
        blob_ownership::BlobOwnership,
        blob_store::BlobStore,
        metadata_store::{LinkKind, MetadataStore},
        parse_manifest_digests,
    },
    registry_client::{DeleteManifestOutcome, NO_LOCAL_PREFIX, RegistryClient, UploadSession},
    replication::{Error, manifest_accept_types},
};

/// Upper bound on each downstream HTTP call inside the referrers-merge
/// critical section, kept well below the metadata executor lock's 300-second
/// max-hold lease (the tx-engine default) so a hung downstream cannot outlive
/// the lease and let a concurrent merge re-admit a lost update.
const REFERRERS_MERGE_HTTP_TIMEOUT: Duration = Duration::from_mins(1);

/// Outcome of a successful replication push or delete.
///
/// Every arm except [`PushOutcome::Unsupported`] is convergence; the
/// distinction drives metrics, and all arms complete the job (no retry).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PushOutcome {
    /// The downstream accepted and applied the change (a PUT/DELETE was issued).
    Pushed,
    /// The downstream already held this exact digest, so the PUT was skipped.
    Converged,
    /// The downstream already holds a strictly-newer copy (last-writer-wins loss).
    Superseded,
    /// The downstream rejects this delete method (`405`, e.g. tag deletion): the
    /// delete cannot propagate, but retrying cannot help, so the job completes
    /// without converging rather than dead-lettering per deletion event.
    Unsupported,
}

/// Per-push invariants shared across the recursion and the blob fan-out: the
/// borrowed downstream and stores, the concurrency cap, the namespace, and the
/// last-writer-wins source timestamp. The per-manifest varying inputs (digest,
/// media type, tag, body) stay direct arguments to [`push_manifest`].
///
/// Built once at the handler call site; the recursion passes the same context
/// to every child since children push into the same namespace.
pub struct PushContext<'a> {
    pub downstream: &'a RegistryClient,
    pub blob_store: &'a Arc<BlobStore>,
    pub metadata_store: &'a Arc<MetadataStore>,
    pub namespace: &'a str,
    pub max_concurrent_pushes: usize,
    pub source_ts: Option<&'a str>,
}

/// Pushes the manifest at `digest` (and everything it references) to
/// `ctx.downstream`, then binds `tag` to it when set.
///
/// Child manifests land before the parent index, referenced blobs are
/// HEAD-probed and only transferred when absent, and `ctx.source_ts` (the
/// last-writer-wins timestamp header) is stamped on the primary manifest PUT
/// only: the referrers fallback tag is a merged set, not an LWW register (see
/// [`push_referrers_fallback`]).
///
/// # Errors
///
/// Returns [`Error::Registry`] when a local read or downstream operation fails
/// with anything other than an LWW-superseded 409, which converges as
/// [`PushOutcome::Superseded`].
#[instrument(skip(ctx, body))]
pub async fn push_manifest(
    ctx: &PushContext<'_>,
    digest: &Digest,
    media_type: Option<String>,
    tag: Option<&str>,
    body: Vec<u8>,
) -> Result<PushOutcome, Error> {
    let parsed = parse_manifest_digests(&body, media_type.as_ref()).map_err(Error::Registry)?;

    // Pushing by tag binds tag -> digest atomically on the downstream.
    let reference = match tag {
        Some(tag) => Reference::Tag(Tag::new(tag).map_err(|e| {
            Error::Registry(RegistryError::Internal(format!("invalid tag '{tag}': {e}")))
        })?),
        None => Reference::Digest(digest.clone()),
    };
    let location = ctx
        .downstream
        .get_manifest_path(NO_LOCAL_PREFIX, ctx.namespace, &reference);

    // The converged skip runs before child recursion and the blob sweep: a
    // digest-matching HEAD means the downstream validated this manifest's
    // references at PUT time, so its children and blobs are already present
    // (each recursed child still gets its own skip). A subject-bearing
    // manifest must always PUT: only the PUT's `OCI-Subject` response reveals
    // whether the downstream needs the referrers fallback, and a converged
    // primary does not imply the fallback landed.
    if parsed.subject.is_none()
        && ctx
            .downstream
            .head_manifest(&manifest_accept_types(), &location)
            .await
            .is_ok_and(|(_, downstream_digest, _)| &downstream_digest == digest)
    {
        info!(
            namespace = ctx.namespace,
            %digest,
            ?tag,
            "Downstream already holds this manifest; skipping PUT (converged)"
        );
        return Ok(PushOutcome::Converged);
    }

    push_child_manifests(ctx, &parsed).await?;

    push_blobs(ctx, &parsed).await?;

    // Retain a body copy only for the subject-bearing fallback path; the common
    // path moves the body into the PUT.
    let fallback_body = parsed.subject.is_some().then(|| body.clone());

    // A body may legitimately omit `mediaType` while the original push carried
    // it in `Content-Type` (recorded on the revision link), and the receiver
    // rejects a PUT without a `Content-Type`, so fall back to the link's type.
    let effective_media_type = match media_type.or_else(|| parsed.media_type.clone()) {
        Some(media_type) => Some(media_type),
        None => ctx
            .metadata_store
            .read_link(ctx.namespace, &LinkKind::Digest(digest.clone()))
            .await
            .ok()
            .and_then(|link| link.media_type),
    };

    let result = ctx
        .downstream
        .put_manifest(
            &location,
            effective_media_type.as_deref(),
            body,
            ctx.source_ts,
        )
        .await
        .map_err(Error::Registry)?;

    // An LWW loss is convergence: drop the push and skip the referrers fallback.
    if result.superseded {
        info!(
            namespace = ctx.namespace,
            %digest,
            ?tag,
            "Downstream superseded the push (last-writer-wins); treating as converged"
        );
        return Ok(PushOutcome::Superseded);
    }
    // A downstream echoing a digest other than the locally computed one has
    // transformed the manifest body: silent content divergence worth a warn.
    if let Some(echoed) = &result.digest
        && echoed != digest
    {
        warn!(
            namespace = ctx.namespace,
            %digest,
            %echoed,
            ?tag,
            "Downstream echoed a different digest for the pushed manifest body"
        );
    }
    info!(namespace = ctx.namespace, %digest, ?tag, "Pushed manifest to downstream");

    // An OCI-1.0 downstream (no `OCI-Subject` response) does not auto-index the
    // subject, so push the referrers fallback tag.
    if let Some(body) = fallback_body.filter(|_| result.subject.is_none()) {
        push_referrers_fallback(
            ctx.downstream,
            ctx.metadata_store,
            ctx.namespace,
            digest,
            &parsed,
            &body,
        )
        .await?;
    }

    Ok(PushOutcome::Pushed)
}

/// Push every child manifest of an index, overlapping independent children up
/// to `max_concurrent_pushes` so a wide multi-arch index is not serialized one
/// child at a time. The caller awaits this before it pushes the parent, so the
/// parent index never lands before its children.
async fn push_child_manifests(
    ctx: &PushContext<'_>,
    parsed: &ParsedManifestDigests,
) -> Result<(), Error> {
    stream::iter(parsed.manifests.clone())
        .map(|child| async move {
            let child_body = ctx.blob_store.read(&child).await.map_err(|e| {
                Error::Registry(RegistryError::Internal(format!(
                    "failed to read local manifest blob '{child}': {e}"
                )))
            })?;
            Box::pin(push_manifest(ctx, &child, None, None, child_body))
                .await
                .map(|_| ())
        })
        // Config rejects 0; the floor guards direct builder misuse.
        .buffer_unordered(ctx.max_concurrent_pushes.max(1))
        .try_collect::<Vec<()>>()
        .await
        .map(|_| ())
}

/// HEAD-before-PUT every referenced blob; transfer only the absent ones.
async fn push_blobs(ctx: &PushContext<'_>, parsed: &ParsedManifestDigests) -> Result<(), Error> {
    // Dedup: a manifest may legally repeat a digest, and two concurrent pushes
    // of the same absent blob would both HEAD-miss and upload.
    let mut seen = HashSet::new();
    let blobs: Vec<Digest> = parsed
        .config
        .iter()
        .chain(parsed.layers.iter())
        .filter(|digest| seen.insert(*digest))
        .cloned()
        .collect();

    stream::iter(blobs)
        .map(|blob| async move { push_one_blob(ctx, &blob).await })
        // Config rejects 0; the floor guards direct builder misuse.
        .buffer_unordered(ctx.max_concurrent_pushes.max(1))
        .try_collect::<Vec<()>>()
        .await?;

    Ok(())
}

/// Picks a cross-repo blob-mount `from` hint: the lexicographically smallest
/// local namespace, other than the target, that already references the blob.
///
/// A wrong guess just falls back to the full upload, and any blob-index read
/// error yields `None`: a missing optimization must never fail a push.
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
async fn push_one_blob(ctx: &PushContext<'_>, digest: &Digest) -> Result<(), Error> {
    let head_location = ctx
        .downstream
        .get_blob_path(NO_LOCAL_PREFIX, ctx.namespace, digest);
    // Existence-only probe: any 2xx means present (the optional
    // Docker-Content-Digest header is not required, so a converged blob never
    // dead-letters on a minimal downstream); a 404 means absent; a transient
    // failure fails the push so the job retries instead of doing a pointless
    // full upload.
    if ctx
        .downstream
        .blob_exists(&head_location)
        .await
        .map_err(Error::Registry)?
    {
        debug!(namespace = ctx.namespace, %digest, "Blob already present on downstream; skipping");
        return Ok(());
    }

    let start_location = ctx
        .downstream
        .get_uploads_start_path(NO_LOCAL_PREFIX, ctx.namespace);

    // The mount is a pure optimization: a miss opens a session and a policy
    // rejection falls through to a plain upload, so it can never fail the push.
    if let Some(from) = mount_candidate(ctx.metadata_store, ctx.namespace, digest).await {
        match ctx
            .downstream
            .mount_blob(&start_location, digest, Some(&from))
            .await
        {
            Ok(None) => {
                info!(namespace = ctx.namespace, %digest, %from, "Mounted blob cross-repo on downstream (no transfer)");
                return Ok(());
            }
            Ok(Some(session)) => {
                return upload_into_session(ctx, digest, &session).await;
            }
            Err(e) => {
                debug!(namespace = ctx.namespace, %digest, "Cross-repo mount unavailable ({e}); uploading instead");
            }
        }
    }

    let session = ctx
        .downstream
        .start_upload(&start_location)
        .await
        .map_err(Error::Registry)?;
    upload_into_session(ctx, digest, &session).await
}

/// Streams a local blob's bytes into an already-open upload session and
/// finalizes it.
async fn upload_into_session(
    ctx: &PushContext<'_>,
    digest: &Digest,
    session: &UploadSession,
) -> Result<(), Error> {
    let (reader, content_length) = match ctx.blob_store.reader(digest, None).await {
        Ok(reader) => reader,
        Err(e) => {
            // The session is already open; cancel it, like the patch/complete
            // failure paths, so a dying push does not strand it on the downstream.
            cancel_upload_session(ctx.downstream, &session.url).await;
            return Err(Error::Registry(RegistryError::Internal(format!(
                "failed to open local blob '{digest}': {e}"
            ))));
        }
    };
    let patched_url = match ctx
        .downstream
        .patch_upload(
            &session.url,
            session.auth.as_deref(),
            content_length,
            reader,
        )
        .await
    {
        Ok(url) => url,
        Err(e) => {
            cancel_upload_session(ctx.downstream, &session.url).await;
            return Err(Error::Registry(e));
        }
    };
    if let Err(e) = ctx.downstream.complete_upload(&patched_url, digest).await {
        cancel_upload_session(ctx.downstream, &patched_url).await;
        return Err(Error::Registry(e));
    }

    info!(namespace = ctx.namespace, %digest, content_length, "Pushed blob to downstream");
    Ok(())
}

/// Best-effort OCI session cancel after a failed upload step, so the failure
/// does not strand an open session on the downstream until its own GC.
async fn cancel_upload_session(downstream: &RegistryClient, session_url: &str) {
    if let Err(e) = downstream.delete_upload(session_url).await {
        debug!("Failed to cancel downstream upload session ({e}); leaving it to downstream GC");
    }
}

/// Pushes the OCI-1.0 referrers fallback tag index for a subject-bearing
/// manifest the downstream did not auto-index.
///
/// The fallback tag (`<alg>-<hash>` of the subject digest) holds a merged image
/// index of referrer descriptors; its PUT is deliberately timestamp-less, since
/// a set merge must never lose an LWW comparison and drop a descriptor.
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

    // Serialize the GET/merge/PUT: two referrers of the same subject are
    // distinct jobs the queue runs concurrently, and unserialized merges read
    // the same base index and drop the loser's descriptor. The lock lives on
    // the metadata executor, which every drain of this store shares, but cannot
    // cover an unrelated sender registry pushing to the same downstream.
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
/// Deliberately downstream-agnostic: the critical section is two short HTTP
/// calls, so cross-downstream contention never matters in practice.
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
    // The timeout keeps each call below the executor lock's max-hold lease.
    let mut manifests = timeout(
        REFERRERS_MERGE_HTTP_TIMEOUT,
        fetch_fallback_manifests(downstream, location),
    )
    .await
    .map_err(|_| {
        Error::Registry(RegistryError::Internal(format!(
            "referrers fallback GET at '{location}' timed out inside the merge lock"
        )))
    })??;

    // The blob store is content-addressed, so `digest` is already the body's
    // digest; no re-hash or re-parse needed.
    let descriptor = referrer_descriptor(
        digest,
        body.len(),
        parsed.media_type.as_deref(),
        parsed.artifact_type.as_deref(),
    );

    // Dedup by digest so a re-run is idempotent.
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

    // Timestamp-less PUT: the receiver then skips LWW, so the merged index can
    // never come back superseded and silently drop a descriptor. The timeout
    // keeps the call below the executor lock's max-hold lease.
    timeout(
        REFERRERS_MERGE_HTTP_TIMEOUT,
        downstream.put_manifest(location, Some(OCI_INDEX_MEDIA_TYPE), index_body, None),
    )
    .await
    .map_err(|_| {
        Error::Registry(RegistryError::Internal(format!(
            "referrers fallback PUT at '{location}' timed out inside the merge lock"
        )))
    })?
    .map_err(Error::Registry)?;
    Ok(())
}

/// GETs the existing referrers fallback index at `location` and returns its
/// `manifests[]` descriptors.
///
/// Only a `404` yields an empty base; any other error, including a `200` body
/// that is not a parseable image index, propagates so the caller never rebuilds
/// the index from an empty base and drops the subject's sibling referrers.
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

/// Builds the OCI descriptor for a referrer manifest to embed in the fallback
/// index; `media_type` defaults to the OCI image-manifest type when the body
/// declared none.
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

/// Deletes the manifest bound to `reference` on the downstream, stamping
/// `source_ts` for receiver-side last-writer-wins. A digest delete of a referrer
/// also drops its descriptor from the subject's OCI-1.0 referrers fallback index
/// (a no-op on a 1.1 downstream); a tag delete leaves the manifest in place, so
/// the fallback is untouched.
///
/// Returns the [`PushOutcome`]; all but [`PushOutcome::Unsupported`] are
/// convergence.
///
/// # Errors
///
/// Returns [`Error::Registry`] when the delete fails with anything other than
/// a 404, an LWW-superseded 409, or a 405.
#[instrument(skip(downstream, metadata_store))]
pub async fn delete_manifest(
    downstream: &RegistryClient,
    metadata_store: &Arc<MetadataStore>,
    namespace: &str,
    reference: &Reference,
    source_ts: Option<&str>,
) -> Result<PushOutcome, Error> {
    // Capture the subject before the manifest is gone (a tag delete leaves it,
    // so it has no fallback index to maintain).
    let fallback_subject = match reference {
        Reference::Digest(digest) => deleted_referrer_subject(downstream, namespace, digest).await,
        Reference::Tag(_) => None,
    };

    let location = downstream.get_manifest_path(NO_LOCAL_PREFIX, namespace, reference);
    let outcome = downstream
        .delete_manifest(&location, source_ts)
        .await
        .map_err(Error::Registry)?;
    let push_outcome = match outcome {
        DeleteManifestOutcome::Deleted => {
            info!(namespace, %reference, "Deleted manifest on downstream");
            PushOutcome::Pushed
        }
        DeleteManifestOutcome::AlreadyAbsent => {
            info!(
                namespace,
                %reference,
                "Downstream already lacked this manifest; delete is a no-op (converged)"
            );
            PushOutcome::Converged
        }
        DeleteManifestOutcome::Superseded => {
            info!(
                namespace,
                %reference,
                "Downstream superseded the delete (last-writer-wins); treating as converged"
            );
            PushOutcome::Superseded
        }
        DeleteManifestOutcome::Unsupported => {
            warn!(
                namespace,
                %reference,
                "Downstream does not support deleting this reference (405); the delete will not \
                 propagate. Completing the job (retrying cannot help)"
            );
            PushOutcome::Unsupported
        }
    };

    // Drop the gone manifest's descriptor from the subject's fallback index.
    // Best-effort: a retry cannot re-derive the subject once the manifest is
    // gone, so a failure warns rather than churning the whole delete.
    if matches!(push_outcome, PushOutcome::Pushed | PushOutcome::Converged)
        && let (Reference::Digest(digest), Some(subject)) = (reference, &fallback_subject)
        && let Err(e) =
            remove_referrers_fallback(downstream, metadata_store, namespace, subject, digest).await
    {
        warn!(
            namespace,
            %digest,
            %subject,
            "Failed to drop the referrer descriptor from the fallback index: {e}"
        );
    }

    Ok(push_outcome)
}

/// GETs the manifest at `digest` from the downstream and returns its subject
/// when it is a referrer. Best-effort: any failure (absent, unparseable, or no
/// subject) yields `None`, leaving the fallback index untouched.
async fn deleted_referrer_subject(
    downstream: &RegistryClient,
    namespace: &str,
    digest: &Digest,
) -> Option<Digest> {
    let location = downstream.get_manifest_path(
        NO_LOCAL_PREFIX,
        namespace,
        &Reference::Digest(digest.clone()),
    );
    let (_, _, body) = downstream
        .get_manifest(&manifest_accept_types(), &location)
        .await
        .ok()?;
    parse_manifest_digests(&body, None).ok()?.subject
}

/// Drops `referrer`'s descriptor from the subject's OCI-1.0 referrers fallback
/// index, deleting the fallback tag when no referrers remain. Serialized under
/// the same per-subject lock as the push-side merge, so a concurrent add and
/// this removal cannot lose each other's update.
async fn remove_referrers_fallback(
    downstream: &RegistryClient,
    metadata_store: &Arc<MetadataStore>,
    namespace: &str,
    subject: &Digest,
    referrer: &Digest,
) -> Result<(), Error> {
    let fallback_tag = format!("{}-{}", subject.algorithm(), subject.hash());
    let reference = Reference::from_str(&fallback_tag).map_err(|e| {
        Error::Registry(RegistryError::Internal(format!(
            "invalid referrers fallback tag '{fallback_tag}': {e}"
        )))
    })?;
    let location = downstream.get_manifest_path(NO_LOCAL_PREFIX, namespace, &reference);

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
    let result = prune_fallback_descriptor(downstream, &location, referrer).await;
    session.release().await;
    result
}

/// Locked critical section of [`remove_referrers_fallback`]: GET the index, drop
/// the descriptor, then PUT the remainder back, or DELETE the tag when empty.
async fn prune_fallback_descriptor(
    downstream: &RegistryClient,
    location: &str,
    referrer: &Digest,
) -> Result<(), Error> {
    let mut manifests = timeout(
        REFERRERS_MERGE_HTTP_TIMEOUT,
        fetch_fallback_manifests(downstream, location),
    )
    .await
    .map_err(|_| {
        Error::Registry(RegistryError::Internal(format!(
            "referrers fallback GET at '{location}' timed out inside the merge lock"
        )))
    })??;

    let referrer_str = referrer.to_string();
    let before = manifests.len();
    manifests.retain(|m| m.get("digest").and_then(Value::as_str) != Some(referrer_str.as_str()));
    // Descriptor absent (already pruned, or a 1.1 downstream has no fallback tag
    // and the GET returned an empty base): nothing to do.
    if manifests.len() == before {
        return Ok(());
    }

    if manifests.is_empty() {
        // No referrers remain: drop the fallback tag rather than leave an empty
        // index. Timestamp-less, mirroring the merge PUT.
        timeout(
            REFERRERS_MERGE_HTTP_TIMEOUT,
            downstream.delete_manifest(location, None),
        )
        .await
        .map_err(|_| {
            Error::Registry(RegistryError::Internal(format!(
                "referrers fallback DELETE at '{location}' timed out inside the merge lock"
            )))
        })?
        .map_err(Error::Registry)?;
        return Ok(());
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
    timeout(
        REFERRERS_MERGE_HTTP_TIMEOUT,
        downstream.put_manifest(location, Some(OCI_INDEX_MEDIA_TYPE), index_body, None),
    )
    .await
    .map_err(|_| {
        Error::Registry(RegistryError::Internal(format!(
            "referrers fallback PUT at '{location}' timed out inside the merge lock"
        )))
    })?
    .map_err(Error::Registry)?;
    Ok(())
}

#[cfg(test)]
mod tests;
