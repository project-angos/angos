//! The replication push pipeline: one code path drives both the event-driven
//! and the scrub-reconcile push of a `(namespace, digest, tag?)` job to a
//! downstream [`RegistryClient`].
//!
//! The queue is at-least-once, so idempotency is mandatory: blobs are
//! HEAD-probed before transfer, child manifests land before the parent index,
//! and a re-run of a converged manifest costs one no-op HEAD.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, PoisonError};

use chrono::{DateTime, Utc};
use futures_util::future::join;
use futures_util::stream::{self, StreamExt};
use tokio::io::AsyncReadExt;
use tokio::sync::OnceCell;
use tracing::{debug, info, instrument, warn};

use angos_oci::manifest_accept_types;
use angos_oci::request::{
    BlobMount, DeleteManifestRequest, HeadBlobRequest, HeadManifestRequest, MountBlobRequest,
    PutManifestRequest, StartUploadRequest,
};
use angos_oci::response::{DeleteManifestOutcome, PutManifestOutcome};
use angos_oci::{Content, Digest, Manifest, Namespace, Reference, Tag};
use angos_oci_client::{RegistryClient, UploadSession};

use crate::registry::blob_store::BlobStore;
use crate::registry::manifest::DEFAULT_MAX_MANIFEST_SIZE_BYTES;
use crate::registry::metadata_store::{LinkKind, MetadataStore};
use crate::replication::{Error, ReplicationDownstream};

mod referrers_fallback;

use self::referrers_fallback::{
    deleted_referrer_subject, push_referrers_fallback, remove_referrers_fallback,
};

/// Deepest index nesting a replication push follows. Each level holds its
/// manifest body while the children below it push, so an unbounded chain of
/// indexes each naming the next is memory an authenticated pusher controls.
const MAX_INDEX_DEPTH: usize = 8;

/// Child manifests a job has reached, each completed once its push succeeds.
type Pushed = Mutex<HashMap<Digest, Arc<OnceCell<()>>>>;

/// Every arm completes the job; all but [`PushOutcome::Unsupported`] are
/// convergence, and the distinction drives metrics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PushOutcome {
    Pushed,
    /// The downstream already held this exact digest, so the PUT was skipped.
    Converged,
    /// Last-writer-wins loss: the downstream holds a strictly newer copy.
    Superseded,
    /// The downstream rejects this delete method (`405`, e.g. tag deletion):
    /// retrying cannot help, so the job completes without converging instead of
    /// dead-lettering once per deletion event.
    Unsupported,
}

/// What stays fixed across the recursion and the blob fan-out.
pub struct PushContext<'a> {
    pub downstream: &'a ReplicationDownstream,
    pub blob_store: &'a BlobStore,
    pub metadata_store: &'a MetadataStore,
    pub namespace: &'a Namespace,
    pub downstream_namespace: &'a Namespace,
    pub source_ts: Option<DateTime<Utc>>,
    /// Indexes already descended through; a caller starts at zero.
    pub index_depth: usize,
}

/// Pushes the manifest at `digest` and everything it references, then binds
/// `tag` to it when set. `ctx.source_ts` is stamped on the primary manifest PUT
/// only, because the referrers fallback tag is a merged set rather than an LWW
/// register.
pub async fn push_manifest(
    ctx: &PushContext<'_>,
    digest: &Digest,
    tag: Option<&Tag>,
    body: Vec<u8>,
) -> Result<PushOutcome, Error> {
    // One map for the whole job: a manifest several indexes name is pushed once.
    push_reached(ctx, &Pushed::default(), digest, tag, body).await
}

#[instrument(name = "push_manifest", skip(ctx, pushed, body))]
async fn push_reached(
    ctx: &PushContext<'_>,
    pushed: &Pushed,
    digest: &Digest,
    tag: Option<&Tag>,
    body: Vec<u8>,
) -> Result<PushOutcome, Error> {
    let manifest = Manifest::from_slice(&body)
        .map_err(|e| Error::InvalidManifest(format!("manifest parse failed: {e}")))?;

    // Pushing by tag binds tag -> digest atomically on the downstream.
    let reference = match tag {
        Some(tag) => Reference::Tag(tag.clone()),
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

    push_child_manifests(ctx, pushed, &manifest, digest).await?;

    push_blobs(ctx, &manifest).await?;

    let fallback_subject = manifest
        .subject
        .as_ref()
        .map(|subject| subject.digest.clone());
    let size = body.len() as u64;

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
                reference,
                content_type: effective_media_type,
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
    if let Some(fallback_subject) = fallback_subject.filter(|_| subject.is_none()) {
        push_referrers_fallback(
            &ctx.downstream.registry_client,
            ctx.namespace,
            ctx.downstream_namespace,
            &fallback_subject,
            digest,
            manifest,
            size,
        )
        .await?;
    }

    Ok(PushOutcome::Pushed)
}

/// Push every child manifest of an index, so each lands before the index the
/// caller PUTs once this returns. The root index's children push
/// concurrently; deeper levels go one at a time, since each level holds its
/// body while its children push and per-level fan-out would compound.
async fn push_child_manifests(
    ctx: &PushContext<'_>,
    pushed: &Pushed,
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
    let child_ctx = &PushContext {
        index_depth: ctx.index_depth + 1,
        ..*ctx
    };
    let concurrency = if ctx.index_depth == 0 {
        ctx.downstream.max_concurrent_pushes.get()
    } else {
        1
    };

    let mut seen = HashSet::new();
    let children: Vec<Digest> = manifests
        .iter()
        .map(|child| &child.digest)
        .filter(|child| seen.insert(*child))
        .cloned()
        .collect();
    let results = stream::iter(children)
        .map(|child| async move { push_child_once(child_ctx, pushed, &child).await })
        .buffer_unordered(concurrency)
        .collect::<Vec<_>>()
        .await;

    // Drained fully before failing, so siblings are not stranded mid-push.
    results.into_iter().collect()
}

/// Push `child` unless the job already did. A child several indexes name is
/// pushed by the first to reach it while the others wait on that push, so
/// none PUTs before its child lands; a failed push leaves the next to retry.
async fn push_child_once(
    ctx: &PushContext<'_>,
    pushed: &Pushed,
    child: &Digest,
) -> Result<(), Error> {
    let cell = pushed
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .entry(child.clone())
        .or_default()
        .clone();
    cell.get_or_try_init(|| async {
        // A manifest PUT only checks that a child's bytes exist, never that
        // they parse, so a child may name a layer of any size; the size comes
        // with the stream and is the cheap question, asked first.
        let (mut reader, size) = ctx.blob_store.reader(child, None).await.map_err(|e| {
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
        Box::pin(push_reached(ctx, pushed, child, None, child_body)).await?;
        Ok(())
    })
    .await?;
    Ok(())
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
        .buffer_unordered(ctx.downstream.max_concurrent_pushes.get())
        .collect::<Vec<_>>()
        .await;

    // Drained fully before failing, so siblings are not stranded mid-transfer.
    results.into_iter().collect()
}

/// Tries a cross-repo mount before a full upload.
async fn push_one_blob(ctx: &PushContext<'_>, digest: &Digest) -> Result<(), Error> {
    // Existence-only probe: any 2xx means present, so a converged blob never
    // dead-letters on a downstream that omits `Docker-Content-Digest`. A
    // transient failure fails the push so the job retries rather than starting
    // a pointless full upload. The local mount lookup runs alongside it to save
    // a round trip on the upload path.
    let (exists, mount_from) = join(
        ctx.downstream.registry_client.blob_exists(HeadBlobRequest {
            namespace: ctx.downstream_namespace.clone(),
            digest: digest.clone(),
            accepted_types: Vec::new(),
        }),
        // The mount `from` hint is the smallest sibling namespace referencing
        // the blob, mapped to its downstream location; a sibling outside this
        // repository or a read error leaves a full upload.
        async {
            let sibling = ctx
                .metadata_store
                .smallest_referencing_namespace(digest, ctx.namespace)
                .await
                .ok()
                .flatten()?;
            ctx.downstream.remote(&sibling).ok()
        },
    )
    .await;
    if exists? {
        debug!(namespace = %ctx.namespace, %digest, "Blob already present on downstream; skipping");
        return Ok(());
    }

    // The mount is a pure optimization: a miss opens a session and a policy
    // rejection falls through to a plain upload, so it can never fail the push.
    if let Some(from) = mount_from {
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

/// Streams a local blob into an open upload session with the committing `PUT`,
/// cancelling the session on failure so it is not stranded on the downstream.
async fn upload_into_session(
    ctx: &PushContext<'_>,
    digest: &Digest,
    session: &UploadSession,
) -> Result<(), Error> {
    let client = &ctx.downstream.registry_client;
    let upload = async {
        let (reader, content_length) =
            ctx.blob_store.reader(digest, None).await.map_err(|e| {
                Error::Internal(format!("failed to open local blob '{digest}': {e}"))
            })?;
        client
            .put_upload(
                &session.url,
                session.auth.as_deref(),
                digest,
                content_length,
                reader,
            )
            .await?;
        Ok::<_, Error>(content_length)
    };
    match upload.await {
        Ok(content_length) => {
            info!(namespace = %ctx.namespace, %digest, content_length, "Pushed blob to downstream");
            Ok(())
        }
        Err(e) => {
            if let Err(cancel) = client.delete_upload(&session.url).await {
                debug!(
                    "Failed to cancel downstream upload session ({cancel}); leaving it to downstream GC"
                );
            }
            Err(e)
        }
    }
}

/// A digest delete of a referrer also drops its descriptor from the subject's
/// OCI-1.0 referrers fallback index; a tag delete leaves the manifest (and the
/// fallback) in place.
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
