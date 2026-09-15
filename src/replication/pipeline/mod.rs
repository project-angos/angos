//! The replication push pipeline: one code path drives both the event-driven
//! and the scrub-reconcile push of a `(namespace, digest, tag?)` job to a
//! downstream [`RegistryClient`].
//!
//! The queue is at-least-once, so idempotency is mandatory: blobs are
//! HEAD-probed before transfer, child manifests land before the parent index,
//! and a re-run of a converged manifest costs one no-op HEAD.

use std::{collections::HashSet, sync::Arc};

use chrono::{DateTime, Utc};
use futures_util::stream::{self, StreamExt};
use tokio::io::AsyncReadExt;
use tracing::{debug, info, instrument, warn};

use angos_oci::manifest_accept_types;
use angos_oci::request::{
    BlobMount, DeleteManifestRequest, HeadBlobRequest, HeadManifestRequest, MountBlobRequest,
    PutManifestRequest, StartUploadRequest,
};
use angos_oci::response::{DeleteManifestOutcome, PutManifestOutcome};
use angos_oci::{Content, Digest, Manifest, Namespace, Reference, Tag};
use angos_oci_client::{RegistryClient, UploadSession};

use crate::{
    registry::{
        blob_store::BlobStore,
        manifest::DEFAULT_MAX_MANIFEST_SIZE_BYTES,
        metadata_store::{LinkKind, MetadataStore},
    },
    replication::Error,
    replication::ReplicationDownstream,
};

/// Deepest index nesting a replication push follows. Each level holds its
/// manifest body while the children below it push, so an unbounded chain of
/// indexes each naming the next is memory an authenticated pusher controls.
const MAX_INDEX_DEPTH: usize = 8;

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
    /// The downstream rejects this delete method (`405`, e.g. tag deletion):
    /// retrying cannot help, so the job completes without converging instead of
    /// dead-lettering once per deletion event.
    Unsupported,
}

/// What stays fixed across the recursion and the blob fan-out: the borrowed
/// downstream and stores, the namespace, and the last-writer-wins timestamp.
/// Built once at the handler call site and passed unchanged to every child,
/// which pushes into the same namespace.
pub struct PushContext<'a> {
    pub downstream: &'a ReplicationDownstream,
    pub blob_store: &'a Arc<BlobStore>,
    pub metadata_store: &'a Arc<MetadataStore>,
    pub namespace: &'a Namespace,
    /// The remote namespace this push targets, derived by the handler via
    /// [`ReplicationDownstream::remote`].
    pub downstream_namespace: &'a Namespace,
    pub source_ts: Option<DateTime<Utc>>,
    /// Indexes already descended through, bounding the recursion; a caller
    /// starts at zero.
    pub index_depth: usize,
}

/// Pushes the manifest at `digest` (and everything it references) to
/// `ctx.downstream`'s registry, then binds `tag` to it when set.
///
/// Child manifests land before the parent index, referenced blobs are
/// HEAD-probed and only transferred when absent, and `ctx.source_ts` is stamped
/// on the primary manifest PUT only, because the referrers fallback tag is a
/// merged set rather than an LWW register (see [`push_referrers_fallback`]).
///
/// # Errors
///
/// Returns [`Error::InvalidManifest`] when `body` does not parse, and
/// [`Error::Client`] when a local read or downstream call fails with anything
/// other than an LWW-superseded 409.
#[instrument(skip(ctx, body))]
pub async fn push_manifest(
    ctx: &PushContext<'_>,
    digest: &Digest,
    tag: Option<&str>,
    body: Vec<u8>,
) -> Result<PushOutcome, Error> {
    let manifest = Manifest::from_slice(&body)
        .map_err(|e| Error::InvalidManifest(format!("manifest parse failed: {e}")))?;

    // Pushing by tag binds tag -> digest atomically on the downstream.
    let reference = match tag {
        Some(tag) => Reference::Tag(
            Tag::new(tag).map_err(|e| Error::Internal(format!("invalid tag '{tag}': {e}")))?,
        ),
        None => Reference::Digest(digest.clone()),
    };
    // The converged skip runs before the child recursion and the blob sweep: a
    // digest-matching HEAD means the downstream validated this manifest's
    // references at PUT time, so its children and blobs are already present. A
    // subject-bearing manifest must always PUT, since only the PUT's
    // `OCI-Subject` response reveals whether the referrers fallback is needed.
    if manifest.subject.is_none()
        && ctx
            .downstream
            .registry_client
            .head_manifest(HeadManifestRequest {
                namespace: ctx.downstream_namespace.clone(),
                reference: reference.clone(),
                accepted_types: manifest_accept_types(),
            })
            .await
            .is_ok_and(|head| head.digest.as_ref() == Some(digest))
    {
        info!(
            namespace = %ctx.namespace,
            %digest,
            ?tag,
            "Downstream already holds this manifest; skipping PUT (converged)"
        );
        return Ok(PushOutcome::Converged);
    }

    push_child_manifests(ctx, &manifest, digest).await?;

    push_blobs(ctx, &manifest).await?;

    // Only the fallback path needs a second copy; the PUT below moves the body.
    let fallback_body = manifest.subject.is_some().then(|| body.clone());

    // A body may legitimately omit `mediaType` while the original push carried
    // it in `Content-Type` (recorded on the revision link), and the receiver
    // rejects a PUT without one.
    let effective_media_type = match &manifest.media_type {
        Some(media_type) => Some(media_type.clone()),
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
            PutManifestRequest {
                namespace: ctx.downstream_namespace.clone(),
                reference: reference.clone(),
                content_type: effective_media_type.clone(),
                tags: Vec::new(),
                source_ts: ctx.source_ts,
            },
            body,
        )
        .await?;

    // An LWW loss is convergence: drop the push and skip the referrers fallback.
    let PutManifestOutcome::Stored {
        digest: echoed,
        subject,
    } = result
    else {
        info!(
            namespace = %ctx.namespace,
            %digest,
            ?tag,
            "Downstream superseded the push (last-writer-wins); treating as converged"
        );
        return Ok(PushOutcome::Superseded);
    };
    // A different echoed digest means the downstream transformed the body,
    // which is silent content divergence worth a warn.
    if let Some(echoed) = &echoed
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
    if let Some(body) = fallback_body.filter(|_| subject.is_none()) {
        push_referrers_fallback(
            &ctx.downstream.registry_client,
            ctx.namespace,
            ctx.downstream_namespace,
            digest,
            manifest,
            &body,
        )
        .await?;
    }

    Ok(PushOutcome::Pushed)
}

/// Push every child manifest of an index, overlapping independent children up
/// to `max_concurrent_pushes`. The caller awaits this before the parent PUT, so
/// the parent index never lands before its children.
async fn push_child_manifests(
    ctx: &PushContext<'_>,
    manifest: &Manifest,
    digest: &Digest,
) -> Result<(), Error> {
    let Content::Index { manifests } = &manifest.content else {
        return Ok(());
    };
    // Terminal: the nesting is a property of the stored bytes, so a replay
    // reaches the same depth.
    if ctx.index_depth >= MAX_INDEX_DEPTH {
        return Err(Error::InvalidManifest(format!(
            "index '{digest}' nests deeper than {MAX_INDEX_DEPTH} levels"
        )));
    }
    let children: Vec<Digest> = manifests.iter().map(|child| child.digest.clone()).collect();
    let child_ctx = &PushContext {
        index_depth: ctx.index_depth + 1,
        ..*ctx
    };

    let results = stream::iter(children)
        .map(|child| async move {
            // A manifest PUT only checks that a child's bytes exist, never that
            // they parse, so a child may name a layer of any size; the size
            // comes with the stream and is the cheap question, asked first.
            let (mut reader, size) = ctx.blob_store.reader(&child, None).await.map_err(|e| {
                Error::Internal(format!("failed to open local manifest blob '{child}': {e}"))
            })?;
            if size > DEFAULT_MAX_MANIFEST_SIZE_BYTES as u64 {
                return Err(Error::InvalidManifest(format!(
                    "index child '{child}' is {size} bytes, over the \
                     {DEFAULT_MAX_MANIFEST_SIZE_BYTES}-byte manifest limit"
                )));
            }
            let mut child_body = Vec::new();
            reader.read_to_end(&mut child_body).await.map_err(|e| {
                Error::Internal(format!("failed to read local manifest blob '{child}': {e}"))
            })?;
            Box::pin(push_manifest(child_ctx, &child, None, child_body))
                .await
                .map(|_| ())
        })
        // Config rejects 0; the floor guards direct builder misuse.
        .buffer_unordered(ctx.downstream.max_concurrent_pushes.max(1))
        .collect::<Vec<_>>()
        .await;

    // Drained fully before failing, so siblings are not stranded mid-transfer.
    results.into_iter().collect()
}

/// HEAD-before-PUT every referenced blob; transfer only the absent ones.
async fn push_blobs(ctx: &PushContext<'_>, manifest: &Manifest) -> Result<(), Error> {
    let Content::Image { config, layers } = &manifest.content else {
        return Ok(());
    };
    // A manifest may legally repeat a digest, and two concurrent pushes of the
    // same absent blob would both HEAD-miss and upload.
    let mut seen = HashSet::new();
    let blobs: Vec<Digest> = config
        .iter()
        .map(|config| &config.digest)
        .chain(layers.iter().map(|layer| &layer.digest))
        .filter(|digest| seen.insert(*digest))
        .cloned()
        .collect();

    let results = stream::iter(blobs)
        .map(|blob| async move { push_one_blob(ctx, &blob).await })
        // Config rejects 0; the floor guards direct builder misuse.
        .buffer_unordered(ctx.downstream.max_concurrent_pushes.max(1))
        .collect::<Vec<_>>()
        .await;

    // Drained fully before failing, so siblings are not stranded mid-transfer.
    results.into_iter().collect()
}

/// Picks a cross-repo blob-mount `from` hint: the smallest sibling namespace
/// referencing the blob, mapped through the same strip/prepend so `from` is the
/// sibling's location on the downstream. A sibling outside this repository, or
/// any read error, yields `None` and the push falls back to a full upload.
async fn mount_candidate(
    metadata_store: &Arc<MetadataStore>,
    namespace: &Namespace,
    digest: &Digest,
    downstream: &ReplicationDownstream,
) -> Option<Namespace> {
    let sibling = metadata_store
        .smallest_referencing_namespace(digest, namespace)
        .await
        .ok()
        .flatten()?;
    downstream.remote(&sibling).ok()
}

/// Transfers a single blob to the downstream if it is not already present,
/// attempting a cross-repo mount before a full upload.
async fn push_one_blob(ctx: &PushContext<'_>, digest: &Digest) -> Result<(), Error> {
    // Existence-only probe: any 2xx means present, so a converged blob never
    // dead-letters on a downstream that omits `Docker-Content-Digest`. A
    // transient failure fails the push so the job retries rather than starting
    // a pointless full upload.
    if ctx
        .downstream
        .registry_client
        .blob_exists(HeadBlobRequest {
            namespace: ctx.downstream_namespace.clone(),
            digest: digest.clone(),
            accepted_types: Vec::new(),
        })
        .await?
    {
        debug!(namespace = %ctx.namespace, %digest, "Blob already present on downstream; skipping");
        return Ok(());
    }

    // The mount is a pure optimization: a miss opens a session and a policy
    // rejection falls through to a plain upload, so it can never fail the push.
    if let Some(from) =
        mount_candidate(ctx.metadata_store, ctx.namespace, digest, ctx.downstream).await
    {
        match ctx
            .downstream
            .registry_client
            .mount_blob(MountBlobRequest {
                namespace: ctx.downstream_namespace.clone(),
                mount: BlobMount {
                    digest: digest.clone(),
                    from: Some(from.clone()),
                },
            })
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
        .start_upload(StartUploadRequest {
            namespace: ctx.downstream_namespace.clone(),
            digest_algorithm: None,
            target: None,
        })
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
            // The session is already open; cancel it so a dying push does not
            // strand it on the downstream.
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
/// also drops its descriptor from the subject's OCI-1.0 referrers fallback
/// index, while a tag delete leaves the manifest (and the fallback) in place.
///
/// # Errors
///
/// Returns [`Error::Client`] when the delete fails with anything other than a
/// 404, an LWW-superseded 409, or a 405.
#[instrument(skip(downstream))]
pub async fn delete_manifest(
    downstream: &RegistryClient,
    namespace: &Namespace,
    downstream_namespace: &Namespace,
    reference: &Reference,
    subject: Option<&Digest>,
    source_ts: Option<DateTime<Utc>>,
) -> Result<PushOutcome, Error> {
    // The carried subject is preferred because it survives a retry that finds
    // the manifest already gone; the probe covers jobs enqueued without one.
    let fallback_subject = match reference {
        Reference::Digest(digest) => match subject {
            Some(subject) => Some(subject.clone()),
            None => deleted_referrer_subject(downstream, downstream_namespace, digest).await,
        },
        Reference::Tag(_) => None,
    };

    let outcome = downstream
        .delete_manifest(DeleteManifestRequest {
            namespace: downstream_namespace.clone(),
            reference: reference.clone(),
            source_ts,
        })
        .await?;
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

    // Best-effort: the delete itself already landed, so a failed fallback prune
    // warns rather than replaying the whole job.
    if matches!(push_outcome, PushOutcome::Pushed | PushOutcome::Converged)
        && let (Reference::Digest(digest), Some(subject)) = (reference, &fallback_subject)
        && let Err(e) =
            remove_referrers_fallback(downstream, namespace, downstream_namespace, subject, digest)
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
