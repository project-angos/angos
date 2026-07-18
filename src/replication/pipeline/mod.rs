//! The replication push pipeline: one code path drives both the event-driven
//! and the scrub-reconcile push of a `(namespace, digest, tag?)` job to a
//! downstream [`RegistryClient`].
//!
//! Idempotency is mandatory (the queue is at-least-once): blobs are HEAD-probed
//! before transfer and child manifests land before the parent index, so a
//! re-run of an already-converged manifest costs a single no-op HEAD.

use std::{collections::HashSet, sync::Arc};

use futures_util::stream::{self, StreamExt, TryStreamExt};
use tracing::{debug, info, instrument, warn};

use crate::{
    oci::{Digest, MediaType, Namespace, Reference, Tag},
    registry::{
        ParsedManifestDigests,
        blob_ownership::BlobOwnership,
        blob_store::BlobStore,
        metadata_store::{LinkKind, MetadataStore},
        parse_manifest_digests,
    },
    registry_client::{DeleteManifestOutcome, RegistryClient, UploadSession},
    replication::ReplicationDownstream,
    replication::{Error, manifest_accept_types},
};

mod referrers_fallback;

use self::referrers_fallback::{
    deleted_referrer_subject, push_referrers_fallback, remove_referrers_fallback,
};

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
/// borrowed downstream and stores, the namespace, and the last-writer-wins
/// source timestamp. The per-manifest varying inputs (digest, media type, tag,
/// body) stay direct arguments to [`push_manifest`].
///
/// Built once at the handler call site; the recursion passes the same context
/// to every child since children push into the same namespace.
pub struct PushContext<'a> {
    pub downstream: &'a ReplicationDownstream,
    pub blob_store: &'a Arc<BlobStore>,
    pub metadata_store: &'a Arc<MetadataStore>,
    pub namespace: &'a Namespace,
    /// The remote namespace this push targets on the downstream, derived by the
    /// handler via [`ReplicationDownstream::remote`].
    pub downstream_namespace: &'a Namespace,
    pub source_ts: Option<&'a str>,
}

/// Pushes the manifest at `digest` (and everything it references) to
/// `ctx.downstream`'s registry, then binds `tag` to it when set.
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
    media_type: Option<MediaType>,
    tag: Option<&str>,
    body: Vec<u8>,
) -> Result<PushOutcome, Error> {
    let parsed = parse_manifest_digests(&body, media_type.as_ref())
        .map_err(|e| Error::Internal(format!("manifest parse failed: {e}")))?;

    // Pushing by tag binds tag -> digest atomically on the downstream.
    let reference = match tag {
        Some(tag) => Reference::Tag(
            Tag::new(tag).map_err(|e| Error::Internal(format!("invalid tag '{tag}': {e}")))?,
        ),
        None => Reference::Digest(digest.clone()),
    };
    let location = ctx
        .downstream
        .registry_client
        .get_manifest_path(ctx.downstream_namespace.as_ref(), &reference);

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
            .registry_client
            .head_manifest(&manifest_accept_types(), &location)
            .await
            .is_ok_and(|(_, downstream_digest, _)| &downstream_digest == digest)
    {
        info!(
            namespace = %ctx.namespace,
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
        .registry_client
        .put_manifest(
            &location,
            effective_media_type.as_deref(),
            body,
            ctx.source_ts,
        )
        .await?;

    // An LWW loss is convergence: drop the push and skip the referrers fallback.
    if result.superseded {
        info!(
            namespace = %ctx.namespace,
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
            namespace = %ctx.namespace,
            %digest,
            %echoed,
            ?tag,
            "Downstream echoed a different digest for the pushed manifest body"
        );
    }
    info!(namespace = %ctx.namespace, %digest, ?tag, "Pushed manifest to downstream");

    // An OCI-1.0 downstream (no `OCI-Subject` response) does not auto-index the
    // subject, so push the referrers fallback tag.
    if let Some(body) = fallback_body.filter(|_| result.subject.is_none()) {
        push_referrers_fallback(
            &ctx.downstream.registry_client,
            ctx.metadata_store,
            ctx.namespace,
            ctx.downstream_namespace.as_ref(),
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
                Error::Internal(format!("failed to read local manifest blob '{child}': {e}"))
            })?;
            Box::pin(push_manifest(ctx, &child, None, None, child_body))
                .await
                .map(|_| ())
        })
        // Config rejects 0; the floor guards direct builder misuse.
        .buffer_unordered(ctx.downstream.max_concurrent_pushes.max(1))
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
        .buffer_unordered(ctx.downstream.max_concurrent_pushes.max(1))
        .try_collect::<Vec<()>>()
        .await?;

    Ok(())
}

/// Picks a cross-repo blob-mount `from` hint: the smallest sibling namespace
/// referencing the blob, mapped through the same strip/prepend so `from` is the
/// sibling's location on the downstream. A sibling outside this repository (strip
/// fails) or any read error yields `None`, so the mount is skipped and the push
/// falls back to a full upload.
async fn mount_candidate(
    metadata_store: &Arc<MetadataStore>,
    namespace: &Namespace,
    digest: &Digest,
    downstream: &ReplicationDownstream,
) -> Option<String> {
    let sibling = BlobOwnership::new(metadata_store)
        .smallest_referencing_namespace(digest, namespace)
        .await
        .ok()
        .flatten()?;
    downstream.remote(&sibling).ok().map(|m| m.to_string())
}

/// Transfers a single blob to the downstream if it is not already present,
/// attempting a cross-repo mount before a full upload.
async fn push_one_blob(ctx: &PushContext<'_>, digest: &Digest) -> Result<(), Error> {
    let head_location = ctx
        .downstream
        .registry_client
        .get_blob_path(ctx.downstream_namespace.as_ref(), digest);
    // Existence-only probe: any 2xx means present (the optional
    // Docker-Content-Digest header is not required, so a converged blob never
    // dead-letters on a minimal downstream); a 404 means absent; a transient
    // failure fails the push so the job retries instead of doing a pointless
    // full upload.
    if ctx
        .downstream
        .registry_client
        .blob_exists(&head_location)
        .await?
    {
        debug!(namespace = %ctx.namespace, %digest, "Blob already present on downstream; skipping");
        return Ok(());
    }

    let start_location = ctx
        .downstream
        .registry_client
        .get_uploads_start_path(ctx.downstream_namespace.as_ref());

    // The mount is a pure optimization: a miss opens a session and a policy
    // rejection falls through to a plain upload, so it can never fail the push.
    if let Some(from) =
        mount_candidate(ctx.metadata_store, ctx.namespace, digest, ctx.downstream).await
    {
        match ctx
            .downstream
            .registry_client
            .mount_blob(&start_location, digest, Some(&from))
            .await
        {
            Ok(None) => {
                info!(namespace = %ctx.namespace, %digest, %from, "Mounted blob cross-repo on downstream (no transfer)");
                return Ok(());
            }
            Ok(Some(session)) => {
                return upload_into_session(ctx, digest, &session).await;
            }
            Err(e) => {
                debug!(namespace = %ctx.namespace, %digest, "Cross-repo mount unavailable ({e}); uploading instead");
            }
        }
    }

    let session = ctx
        .downstream
        .registry_client
        .start_upload(&start_location)
        .await?;
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
            cancel_upload_session(&ctx.downstream.registry_client, &session.url).await;
            return Err(Error::Internal(format!(
                "failed to open local blob '{digest}': {e}"
            )));
        }
    };
    let patched_url = match ctx
        .downstream
        .registry_client
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
            cancel_upload_session(&ctx.downstream.registry_client, &session.url).await;
            return Err(Error::Client(e));
        }
    };
    if let Err(e) = ctx
        .downstream
        .registry_client
        .complete_upload(&patched_url, digest)
        .await
    {
        cancel_upload_session(&ctx.downstream.registry_client, &patched_url).await;
        return Err(Error::Client(e));
    }

    info!(namespace = %ctx.namespace, %digest, content_length, "Pushed blob to downstream");
    Ok(())
}

/// Best-effort OCI session cancel after a failed upload step, so the failure
/// does not strand an open session on the downstream until its own GC.
async fn cancel_upload_session(downstream: &RegistryClient, session_url: &str) {
    if let Err(e) = downstream.delete_upload(session_url).await {
        debug!("Failed to cancel downstream upload session ({e}); leaving it to downstream GC");
    }
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
    namespace: &Namespace,
    downstream_namespace: &str,
    reference: &Reference,
    source_ts: Option<&str>,
) -> Result<PushOutcome, Error> {
    // Capture the subject before the manifest is gone (a tag delete leaves it,
    // so it has no fallback index to maintain).
    let fallback_subject = match reference {
        Reference::Digest(digest) => {
            deleted_referrer_subject(downstream, downstream_namespace, digest).await
        }
        Reference::Tag(_) => None,
    };

    let location = downstream.get_manifest_path(downstream_namespace, reference);
    let outcome = downstream.delete_manifest(&location, source_ts).await?;
    let push_outcome = match outcome {
        DeleteManifestOutcome::Deleted => {
            info!(namespace = %namespace, %reference, "Deleted manifest on downstream");
            PushOutcome::Pushed
        }
        DeleteManifestOutcome::AlreadyAbsent => {
            info!(
                namespace = %namespace,
                %reference,
                "Downstream already lacked this manifest; delete is a no-op (converged)"
            );
            PushOutcome::Converged
        }
        DeleteManifestOutcome::Superseded => {
            info!(
                namespace = %namespace,
                %reference,
                "Downstream superseded the delete (last-writer-wins); treating as converged"
            );
            PushOutcome::Superseded
        }
        DeleteManifestOutcome::Unsupported => {
            warn!(
                namespace = %namespace,
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
        && let Err(e) = remove_referrers_fallback(
            downstream,
            metadata_store,
            namespace,
            downstream_namespace,
            subject,
            digest,
        )
        .await
    {
        warn!(
            namespace = %namespace,
            %digest,
            %subject,
            "Failed to drop the referrer descriptor from the fallback index: {e}"
        );
    }

    Ok(push_outcome)
}

#[cfg(test)]
mod tests;
