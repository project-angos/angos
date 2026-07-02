pub mod link_plan;
mod parse;
mod response;

use std::collections::HashSet;

use angos_tx_engine::lock::Error as LockError;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures_util::future::join_all;
use parse::manifest_meta_from_body;
pub use parse::{ParsedManifestDigests, parse_and_validate_manifest, parse_manifest_digests};
pub use response::{
    DeleteManifestResponse, GetManifestResponse, HeadManifestResponse, PutManifestResponse,
};
use response::{
    ManifestBody, ManifestMeta, get_manifest_body_headers, get_manifest_redirect_headers,
    head_manifest_headers, put_manifest_headers,
};
use tokio::io::{AsyncRead, AsyncReadExt};
use tracing::{error, instrument, warn};

use crate::{
    event_webhook::event::{Event, EventActor, EventKind},
    metrics_provider::metrics_provider,
    oci::{Digest, Manifest, MediaType, Namespace, Reference, Tag},
    registry::{
        DOCKER_CONTENT_DIGEST, Error, Registry, Repository,
        blob_ownership::BlobOwnership,
        blob_store::Error as BlobStoreError,
        job_store::Queue,
        metadata_store::{
            Error as MetadataStoreError, LinkKind, LinkMetadata, LinkOperation, LinksCommit,
        },
    },
    replication::{
        REPLICATION_DELETE_MANIFEST_KIND, REPLICATION_PUSH_MANIFEST_KIND, ReplicationPushPayload,
        build_envelope,
    },
};

pub const DEFAULT_MAX_MANIFEST_SIZE_BYTES: usize = 5 * 1024 * 1024;

/// Bounded lock-discovery attempts for a manifest push: attempt 1 discovers
/// the tracked grant inserts, attempt 2 commits under their locks, attempt 3
/// absorbs one round of concurrent link churn.
const STORE_MANIFEST_LOCK_ATTEMPTS: u32 = 3;

/// How a manifest push treats descriptors referencing content the target
/// namespace does not own.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReferencePolicy {
    /// Reject the push with `MANIFEST_BLOB_UNKNOWN`.
    Strict,
    /// Store the manifest but skip the granting links for unowned references, so
    /// they stay dangling rather than granting read access to unpushed content.
    Permissive,
    /// Trust every reference as owned; used only by pull-through cache-fill.
    Trusted,
}

fn manifest_event(
    kind: EventKind,
    namespace: &Namespace,
    repository: String,
    digest: Option<String>,
    reference: &Reference,
    actor: Option<EventActor>,
) -> Event {
    Event::new(kind, namespace.clone(), repository)
        .digest(digest)
        .reference(Some(reference.to_string()))
        .actor(actor)
}

fn tag_event(
    kind: EventKind,
    namespace: &Namespace,
    repository: String,
    digest: Option<String>,
    reference: &Reference,
    tag: &Tag,
    actor: Option<EventActor>,
) -> Event {
    Event::new(kind, namespace.clone(), repository)
        .digest(digest)
        .reference(Some(reference.to_string()))
        .tag(Some(tag.to_string()))
        .actor(actor)
}

/// Drop the grant-inserting creates targeting `refused`: the Permissive and
/// Trusted last resort under link churn, storing the manifest without the
/// blocked grants so they stay dangling rather than granting unlocked.
fn drop_refused_grants(ops: &[LinkOperation], refused: &HashSet<Digest>) -> Vec<LinkOperation> {
    ops.iter()
        .filter(|op| {
            !matches!(op, LinkOperation::Create { link, target, referrer: Some(_), .. }
                if link.is_tracked() && refused.contains(target))
        })
        .cloned()
        .collect()
}

impl Registry {
    #[instrument(skip(repository))]
    pub async fn head_manifest(
        &self,
        repository: &Repository,
        accepted_types: &[String],
        namespace: &Namespace,
        reference: Reference,
        is_tag_immutable: bool,
    ) -> Result<HeadManifestResponse, Error> {
        let local = self.head_local_manifest(namespace, &reference).await;

        if !repository.is_pull_through() {
            return local
                .map(|meta| HeadManifestResponse {
                    headers: head_manifest_headers(&meta),
                })
                .map_err(|_| {
                    error!("Failed to head local manifest: {namespace}:{reference}");
                    Error::ManifestUnknown
                });
        }

        if let Ok(meta) = local {
            let use_local = !self
                .needs_upstream_pull_manifest(
                    repository,
                    accepted_types,
                    namespace,
                    &reference,
                    is_tag_immutable,
                    &meta.digest,
                )
                .await?;

            if use_local {
                return Ok(HeadManifestResponse {
                    headers: head_manifest_headers(&meta),
                });
            }
        }

        let body = self
            .get_manifest(
                repository,
                accepted_types,
                namespace,
                reference,
                is_tag_immutable,
            )
            .await?;

        Ok(HeadManifestResponse {
            headers: head_manifest_headers(&ManifestMeta {
                media_type: body.media_type,
                digest: body.digest,
                size: body.content.len() as u64,
            }),
        })
    }

    /// Read a manifest/tag link for a client pull, recording its access time
    /// when pull-time tracking is enabled.
    async fn read_manifest_link(
        &self,
        namespace: &Namespace,
        link: &LinkKind,
    ) -> Result<LinkMetadata, MetadataStoreError> {
        if self.update_pull_time {
            self.metadata_store
                .read_link_recording_access(namespace, link)
                .await
        } else {
            self.metadata_store.read_link(namespace, link).await
        }
    }

    async fn head_local_manifest(
        &self,
        namespace: &Namespace,
        reference: &Reference,
    ) -> Result<ManifestMeta, Error> {
        let blob_link = LinkKind::from_reference(reference);
        let link = self.read_manifest_link(namespace, &blob_link).await?;

        if let Some(media_type) = link.media_type {
            let size = self.blob_store.size(&link.target).await.map_err(|error| {
                error!("Failed to get blob size: {error}");
                Error::ManifestUnknown
            })?;

            return Ok(ManifestMeta {
                media_type: Some(media_type),
                digest: link.target,
                size,
            });
        }

        // Compat: links created before media_type was stored need a full blob
        // read. Removable once all links have been re-pushed.
        let (mut reader, _) =
            self.blob_store
                .reader(&link.target, None)
                .await
                .map_err(|error| {
                    error!("Failed to build blob reader: {error}");
                    Error::ManifestUnknown
                })?;

        let mut manifest_content = Vec::new();
        reader.read_to_end(&mut manifest_content).await?;

        manifest_meta_from_body(&link.target, &manifest_content)
    }

    #[instrument(skip(repository))]
    pub async fn get_manifest(
        &self,
        repository: &Repository,
        accepted_types: &[String],
        namespace: &Namespace,
        reference: Reference,
        is_tag_immutable: bool,
    ) -> Result<ManifestBody, Error> {
        let local = self.get_local_manifest(namespace, &reference).await;

        if !repository.is_pull_through() {
            return local.map_err(|_| {
                error!("Failed to get local manifest: {namespace}:{reference}");
                Error::ManifestUnknown
            });
        }

        if let Ok(manifest) = local {
            let use_local = !self
                .needs_upstream_pull_manifest(
                    repository,
                    accepted_types,
                    namespace,
                    &reference,
                    is_tag_immutable,
                    &manifest.digest,
                )
                .await?;

            if use_local {
                return Ok(manifest);
            }
        }

        let (media_type, digest, content) = repository
            .get_manifest(accepted_types, namespace, &reference)
            .await?;

        self.store_manifest(
            namespace,
            &reference,
            media_type.as_ref(),
            &content,
            &[],
            ReferencePolicy::Trusted,
            None,
        )
        .await?;

        Ok(ManifestBody {
            media_type,
            digest,
            content,
        })
    }

    async fn needs_upstream_pull_manifest(
        &self,
        repository: &Repository,
        accepted_types: &[String],
        namespace: &Namespace,
        reference: &Reference,
        is_tag_immutable: bool,
        local_digest: &Digest,
    ) -> Result<bool, Error> {
        if !repository.is_pull_through()
            || !matches!(reference, Reference::Tag(_))
            || is_tag_immutable
        {
            return Ok(false);
        }

        Ok(!repository
            .is_upstream_digest_match(accepted_types, namespace, reference, local_digest)
            .await?)
    }

    async fn get_local_manifest(
        &self,
        namespace: &Namespace,
        reference: &Reference,
    ) -> Result<ManifestBody, Error> {
        let blob_link = LinkKind::from_reference(reference);
        let link = self.read_manifest_link(namespace, &blob_link).await?;

        let content = self.blob_store.read(&link.target).await?;
        let manifest: Manifest = serde_json::from_slice(&content).map_err(|error| {
            warn!("Failed to deserialize manifest: {error}");
            Error::ManifestInvalid("Failed to deserialize manifest".to_string())
        })?;

        Ok(ManifestBody {
            media_type: link.media_type.or(manifest.media_type),
            digest: link.target,
            content,
        })
    }

    /// Test-only wrapper that stores a manifest without a replication `source_ts`.
    #[cfg(test)]
    #[instrument(skip(body))]
    pub async fn put_manifest(
        &self,
        namespace: &Namespace,
        reference: &Reference,
        content_type: Option<&MediaType>,
        body: &[u8],
    ) -> Result<PutManifestResponse, Error> {
        self.store_manifest(
            namespace,
            reference,
            content_type,
            body,
            &[],
            ReferencePolicy::Strict,
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn store_manifest(
        &self,
        namespace: &Namespace,
        reference: &Reference,
        content_type: Option<&MediaType>,
        body: &[u8],
        created_tags: &[Tag],
        reference_policy: ReferencePolicy,
        created_at: Option<DateTime<Utc>>,
    ) -> Result<PutManifestResponse, Error> {
        let mut manifest = parse_and_validate_manifest(body, content_type)?;
        // A digest reference fixes the algorithm; a tag push lands under the
        // canonical sha256 digest.
        let computed_digest = match reference {
            Reference::Digest(provided) => Digest::from_bytes(provided.algorithm(), body),
            Reference::Tag(_) => Digest::sha256_of_bytes(body),
        };

        if let Reference::Digest(provided_digest) = reference
            && provided_digest != &computed_digest
        {
            warn!(
                "Provided digest does not match computed digest: {provided_digest} != {computed_digest}"
            );
            return Err(Error::ManifestInvalid(
                "Provided digest does not match computed digest".to_string(),
            ));
        }

        let effective_media_type = content_type
            .cloned()
            .or_else(|| manifest.media_type.clone());

        let ops = link_plan::push(
            &mut manifest,
            &computed_digest,
            reference,
            effective_media_type.as_ref(),
            body.len() as u64,
            created_tags,
        );

        let commit = self
            .commit_manifest_links(
                namespace,
                &manifest,
                &ops,
                reference_policy,
                created_at,
                &computed_digest,
                body,
            )
            .await?;

        // Changed-state check from the prior target the commit validated; a
        // missing entry fails open. A by-digest push with `?tag=` also counts its
        // created tag links so new tags replicate even when the digest is present.
        let changed = commit.changed(&LinkKind::from_reference(reference), &computed_digest)
            || created_tags
                .iter()
                .any(|tag| commit.changed(&LinkKind::Tag(tag.clone()), &computed_digest));

        let subject = manifest.subject.map(|s| s.digest);

        Ok(PutManifestResponse {
            headers: put_manifest_headers(
                namespace,
                reference,
                &computed_digest,
                subject.as_ref(),
                created_tags,
            ),
            digest: computed_digest,
            events: Vec::new(),
            changed,
        })
    }

    /// Commit a push's link plan under a bounded lock-discovery loop. Each
    /// attempt takes one sorted all-or-nothing blob-data session over the
    /// manifest digest plus every digest whose tracked grant insert the planner
    /// previously refused; policy validation, the body write, and the link
    /// commit all run under it, so a byte reclaim (which takes the same
    /// per-digest locks) can never interleave: reaper-first means validation
    /// fails `MANIFEST_BLOB_UNKNOWN` and the client re-uploads, push-first
    /// means the reaper's under-lock re-check keeps the bytes.
    #[allow(clippy::too_many_arguments)]
    async fn commit_manifest_links(
        &self,
        namespace: &Namespace,
        manifest: &Manifest,
        ops: &[LinkOperation],
        reference_policy: ReferencePolicy,
        created_at: Option<DateTime<Utc>>,
        computed_digest: &Digest,
        body: &[u8],
    ) -> Result<LinksCommit, Error> {
        let body_bytes = Bytes::copy_from_slice(body);
        // Digests whose blob-data locks the next attempt must hold, discovered
        // from the planner's refused tracked grant inserts.
        let mut needed: HashSet<Digest> = HashSet::new();
        // Grants skipped on the last-resort Permissive/Trusted attempt.
        let mut dropped: HashSet<Digest> = HashSet::new();
        let mut attempt = 0;

        loop {
            attempt += 1;

            let mut lock_digests: Vec<Digest> = Vec::with_capacity(needed.len() + 1);
            lock_digests.push(computed_digest.clone());
            lock_digests.extend(needed.iter().cloned());
            let session = self
                .metadata_store
                .acquire_blob_data_locks(&lock_digests)
                .await?;
            let cancellation = session.cancellation();
            let granted: HashSet<Digest> = lock_digests.into_iter().collect();

            let outcome = async {
                let retained;
                let attempt_ops: &[LinkOperation] = match reference_policy {
                    ReferencePolicy::Strict => {
                        self.validate_manifest_references(namespace, manifest)
                            .await?;
                        ops
                    }
                    ReferencePolicy::Permissive => {
                        retained = self
                            .retain_owned_reference_links(namespace, ops.to_vec())
                            .await?;
                        &retained
                    }
                    ReferencePolicy::Trusted => ops,
                };
                let reduced;
                let attempt_ops = if dropped.is_empty() {
                    attempt_ops
                } else {
                    reduced = drop_refused_grants(attempt_ops, &dropped);
                    &reduced
                };

                // store_manifest links this body, so it must be present at
                // commit; a concurrent delete of the same digest can drop the
                // not-yet-linked bytes between attempts. Re-write when absent,
                // under this attempt's blob-data lock; age-gated reaps never fire
                // within the loop.
                if self.blob_store.size(computed_digest).await.is_err() {
                    self.blob_store
                        .put_blob(computed_digest, body_bytes.clone())
                        .await?;
                }

                // Lock-loss fence: a session whose heartbeat lost the lock must
                // not commit grants a concurrent reaper can no longer see.
                if cancellation.is_cancelled() {
                    return Err(Error::from(LockError::Invalidated));
                }

                self.metadata_store
                    .store_manifest(namespace, attempt_ops, created_at, &granted)
                    .await
                    .map_err(|e| match e {
                        MetadataStoreError::ReplicationSuperseded(message) => {
                            Error::ReplicationSuperseded(message)
                        }
                        e => Error::from(e),
                    })
            }
            .await;
            session.release().await;
            let commit = outcome?;

            if commit.needs_locks.is_empty() {
                return Ok(commit);
            }
            if attempt < STORE_MANIFEST_LOCK_ATTEMPTS {
                needed.extend(commit.needs_locks);
                continue;
            }
            // Attempts exhausted: concurrent create/delete churn on this
            // manifest's tracked links outran the lock discovery.
            match reference_policy {
                // The client heals by re-uploading the referenced blobs and
                // retrying the push.
                ReferencePolicy::Strict => return Err(Error::ManifestBlobUnknown),
                // Skip the churning grants (the policy's drop semantics) in one
                // last attempt; give up when the churn continues.
                ReferencePolicy::Permissive | ReferencePolicy::Trusted => {
                    if !dropped.is_empty() {
                        return Err(Error::MetadataStore(
                            MetadataStoreError::TrackedInsertWithoutLock(commit.needs_locks),
                        ));
                    }
                    dropped.extend(commit.needs_locks);
                }
            }
        }
    }

    async fn validate_manifest_references(
        &self,
        namespace: &Namespace,
        manifest: &Manifest,
    ) -> Result<(), Error> {
        let ownership = BlobOwnership::new(self.metadata_store.as_ref());

        if let Some(config) = &manifest.config {
            self.validate_manifest_reference(namespace, &ownership, &config.digest)
                .await?;
        }

        for layer in &manifest.layers {
            self.validate_manifest_reference(namespace, &ownership, &layer.digest)
                .await?;
        }

        for child in &manifest.manifests {
            self.validate_manifest_reference(namespace, &ownership, &child.digest)
                .await?;
        }

        Ok(())
    }

    async fn validate_manifest_reference(
        &self,
        namespace: &Namespace,
        ownership: &BlobOwnership<'_>,
        digest: &Digest,
    ) -> Result<(), Error> {
        if !ownership.can_read(namespace, digest).await? {
            return Err(Error::ManifestBlobUnknown);
        }

        match self.blob_store.size(digest).await {
            Ok(_) => Ok(()),
            Err(BlobStoreError::BlobNotFound | BlobStoreError::ReferenceNotFound) => {
                Err(Error::ManifestBlobUnknown)
            }
            Err(error) => Err(error.into()),
        }
    }

    /// Drops the config/layer/child-manifest links for descriptors the namespace
    /// does not own, leaving its own digest, tag, and subject back-link, so a
    /// permissive push never grants read access to unpushed content.
    async fn retain_owned_reference_links(
        &self,
        namespace: &Namespace,
        ops: Vec<LinkOperation>,
    ) -> Result<Vec<LinkOperation>, Error> {
        let ownership = BlobOwnership::new(self.metadata_store.as_ref());
        let mut retained = Vec::with_capacity(ops.len());
        for op in ops {
            if let LinkOperation::Create { link, target, .. } = &op
                && link.is_tracked()
                && !ownership.can_read(namespace, target).await?
            {
                continue;
            }
            retained.push(op);
        }
        Ok(retained)
    }

    #[instrument(skip(actor))]
    pub async fn delete_manifest(
        &self,
        actor: Option<EventActor>,
        source_ts: Option<DateTime<Utc>>,
        namespace: &Namespace,
        reference: &Reference,
    ) -> Result<DeleteManifestResponse, Error> {
        // No incoming digest, so a timestamp tie lets the delete proceed.
        self.check_lww_not_superseded(namespace, reference, source_ts, None)
            .await?;

        // A digest delete cascades to every pointing tag; resolve them first for
        // the suppression gate, the LWW guard, and the link plan.
        let pointing_tags = if let Reference::Digest(digest) = reference {
            self.metadata_store
                .find_tags_pointing_at(namespace, digest)
                .await?
        } else {
            Vec::new()
        };

        // No-op suppression: the ref is absent only when the prior link is gone
        // AND no tag points at it; a read error counts as "existed" so a real
        // delete is never suppressed. This pre-commit read a racing write can
        // flip is safe: over-dispatch is idempotent.
        let resolved_repository = self.resolver.resolve(namespace);
        let existed_before = match self
            .prior_link_if_replicated(resolved_repository, namespace, reference)
            .await
        {
            None => false,
            Some(Err(MetadataStoreError::ReferenceNotFound)) => !pointing_tags.is_empty(),
            Some(_) => true,
        };

        let ops = if let Reference::Digest(digest) = reference {
            // A tag re-pointed locally after the delete was authored must not be
            // dropped by the older replicated delete.
            if let Some(source_ts) = source_ts {
                self.check_digest_delete_not_superseded(namespace, &pointing_tags, source_ts)
                    .await?;
            }

            let manifest = self
                .blob_store
                .read(digest)
                .await
                .ok()
                .and_then(|content| Manifest::from_slice(&content).ok());
            link_plan::delete(reference, manifest.as_ref(), &pointing_tags)
        } else {
            link_plan::delete(reference, None, &[])
        };

        // Threading `source_ts` into the transaction makes the deleted tag links
        // part of its validated read set, so a concurrent newer re-put aborts the
        // delete rather than being clobbered.
        if let Reference::Digest(digest) = reference {
            self.delete_manifest_links_and_reclaim(namespace, digest, &ops, source_ts)
                .await?;
        } else {
            self.metadata_store
                .delete_links(namespace, &ops, source_ts)
                .await?;
        }

        let repository = resolved_repository
            .map(|r| r.name.to_string())
            .unwrap_or_default();
        let digest_str = match reference {
            Reference::Digest(d) => Some(d.to_string()),
            Reference::Tag(_) => None,
        };

        let mut events = vec![manifest_event(
            EventKind::ManifestDelete,
            namespace,
            repository.clone(),
            digest_str.clone(),
            reference,
            actor.clone(),
        )];

        if let Some(tag) = reference.as_tag() {
            events.push(tag_event(
                EventKind::TagDelete,
                namespace,
                repository,
                digest_str,
                reference,
                tag,
                actor,
            ));
        }

        // A tag delete's receiver keys off `payload.tag`, so no digest is carried.
        let (tag, dispatch_digest) = match reference {
            Reference::Tag(tag) => (Some(tag), None),
            Reference::Digest(digest) => (None, Some(digest)),
        };
        // Webhook events fired unconditionally; only replication dispatch is gated
        // on a real removal. A replicated delete forwards its author timestamp
        // verbatim so the bounce can't outrank a later recreate.
        if existed_before {
            self.dispatch_replication(
                resolved_repository,
                namespace,
                REPLICATION_DELETE_MANIFEST_KIND,
                tag,
                dispatch_digest,
                source_ts,
            )
            .await;
        }

        Ok(DeleteManifestResponse { events })
    }

    /// Delete a digest manifest's links and reclaim its bytes when the commit
    /// left them unreferenced. Holds the blob-data lock across both so a
    /// concurrent grant is not missed, and fences the byte delete on lock
    /// loss: a lost session means a concurrent grant may have committed
    /// unseen, so the bytes are kept (the sweep reaps true orphans).
    async fn delete_manifest_links_and_reclaim(
        &self,
        namespace: &Namespace,
        digest: &Digest,
        ops: &[LinkOperation],
        source_ts: Option<DateTime<Utc>>,
    ) -> Result<(), Error> {
        let session = self.acquire_blob_data_lock(digest).await?;
        let cancellation = session.cancellation();
        let result = async {
            if self
                .metadata_store
                .delete_manifest(namespace, digest, ops, source_ts)
                .await?
            {
                if cancellation.is_cancelled() {
                    return Err(Error::from(LockError::Invalidated));
                }
                self.blob_store.delete_blob(digest).await?;
            }
            Ok::<_, Error>(())
        }
        .await;
        session.release().await;
        result
    }

    /// Short-circuit a manifest GET into a presigned redirect from link metadata
    /// alone, without reading the blob. `None` when the link lacks a `media_type`
    /// or no presign backend produces a URL.
    async fn try_redirect_via_link(
        &self,
        namespace: &Namespace,
        reference: &Reference,
    ) -> Option<GetManifestResponse> {
        let blob_link = LinkKind::from_reference(reference);
        let link = self.read_manifest_link(namespace, &blob_link).await.ok()?;
        let media_type = link.media_type?;
        let presigned_url = self
            .blob_store
            .presigned_url(&link.target, Some(media_type.as_ref()))
            .await
            .ok()??;

        Some(GetManifestResponse::Redirect {
            headers: get_manifest_redirect_headers(presigned_url, &link.target, Some(media_type)),
        })
    }

    /// Resolve a manifest GET to a presigned redirect or the manifest body. The
    /// redirect fast-path is taken only when the cached target is authoritative;
    /// a mutable tag on a pull-through cache falls through to `get_manifest`.
    #[instrument(skip(self, is_tag_immutable))]
    pub async fn resolve_get_manifest(
        &self,
        namespace: &Namespace,
        reference: Reference,
        mime_types: &[String],
        is_tag_immutable: bool,
    ) -> Result<GetManifestResponse, Error> {
        let repository = self.get_repository_for_namespace(namespace)?;

        let redirect_is_authoritative = !repository.is_pull_through()
            || matches!(reference, Reference::Digest(_))
            || is_tag_immutable;

        if self.enable_manifest_redirect
            && redirect_is_authoritative
            && let Some(resp) = self.try_redirect_via_link(namespace, &reference).await
        {
            return Ok(resp);
        }

        let manifest = self
            .get_manifest(
                repository,
                mime_types,
                namespace,
                reference,
                is_tag_immutable,
            )
            .await?;

        // Compat: when the link-only redirect above fails (no media_type),
        // redirect after reading the full blob. Removable once all links are
        // re-pushed.
        if self.enable_manifest_redirect
            && let Ok(Some(presigned_url)) = self
                .blob_store
                .presigned_url(&manifest.digest, manifest.media_type.as_deref())
                .await
        {
            return Ok(GetManifestResponse::Redirect {
                headers: get_manifest_redirect_headers(
                    presigned_url,
                    &manifest.digest,
                    manifest.media_type,
                ),
            });
        }

        let content_length = manifest.content.len() as u64;
        Ok(GetManifestResponse::Body {
            headers: get_manifest_body_headers(
                manifest.media_type.as_deref(),
                &manifest.digest,
                content_length,
            ),
            content: manifest.content,
        })
    }

    /// LWW guard for a replication-originated tag write: rejects with
    /// [`Error::ReplicationSuperseded`] when the local tag supersedes the incoming
    /// `source_ts` per [`Self::link_supersedes`]. Skipped without a `source_ts`
    /// (client write) and for digest references.
    async fn check_lww_not_superseded(
        &self,
        namespace: &Namespace,
        reference: &Reference,
        source_ts: Option<DateTime<Utc>>,
        incoming_digest: Option<&Digest>,
    ) -> Result<(), Error> {
        let Some(source_ts) = source_ts else {
            return Ok(());
        };
        let Some(tag) = reference.as_tag() else {
            return Ok(());
        };

        if let Some(created_at) = self
            .link_supersedes(
                namespace,
                &LinkKind::Tag(tag.clone()),
                source_ts,
                incoming_digest,
            )
            .await?
        {
            return Err(Error::ReplicationSuperseded(format!(
                "local tag '{tag}' (created {created_at}) is newer than the replicated source ({source_ts})"
            )));
        }

        Ok(())
    }

    /// `Some(created_at)` iff the local link supersedes the incoming write per
    /// [`LinkMetadata::supersedes`]; `None` when it is absent or loses. Non-
    /// `ReferenceNotFound` errors fail closed; reads bypass the link cache.
    async fn link_supersedes(
        &self,
        namespace: &Namespace,
        link: &LinkKind,
        source_ts: DateTime<Utc>,
        incoming_digest: Option<&Digest>,
    ) -> Result<Option<DateTime<Utc>>, Error> {
        let metadata = match self
            .metadata_store
            .read_link_reference(namespace, link)
            .await
        {
            Ok(metadata) => metadata,
            Err(MetadataStoreError::ReferenceNotFound) => return Ok(None),
            Err(err) => return Err(Error::from(err)),
        };
        Ok(metadata.supersedes(source_ts, incoming_digest))
    }

    /// LWW guard for a replication-originated digest delete: a tag re-pointed
    /// locally after the delete was authored rejects the whole delete with
    /// [`Error::ReplicationSuperseded`], preserving the tag and its revision.
    async fn check_digest_delete_not_superseded(
        &self,
        namespace: &Namespace,
        tags: &[LinkKind],
        source_ts: DateTime<Utc>,
    ) -> Result<(), Error> {
        for tag in tags {
            // No incoming digest, so a tie lets the delete proceed.
            if let Some(created_at) = self
                .link_supersedes(namespace, tag, source_ts, None)
                .await?
            {
                return Err(Error::ReplicationSuperseded(format!(
                    "local {tag} (created {created_at}) is newer than the replicated digest delete ({source_ts})"
                )));
            }
        }

        Ok(())
    }

    /// The prior local link for `reference`, read only when an event-enqueuing
    /// downstream matches `namespace` so the replication-off path pays no extra
    /// read. Non-`ReferenceNotFound` errors are surfaced, not collapsed to
    /// "absent"; the read bypasses the link cache.
    async fn prior_link_if_replicated(
        &self,
        repository: Option<&Repository>,
        namespace: &Namespace,
        reference: &Reference,
    ) -> Option<Result<LinkMetadata, MetadataStoreError>> {
        let repository = repository?;

        for downstream in &repository.replication {
            if downstream.enqueues_for(namespace.as_ref()) {
                return Some(
                    self.metadata_store
                        .read_link_reference(namespace, &LinkKind::from_reference(reference))
                        .await,
                );
            }
        }
        None
    }

    /// Read the body stream, store the manifest, and return the domain response.
    /// `tags` are `?tag=` query parameters, applied only for a
    /// `Reference::Digest`; a by-tag push ignores them.
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip(self, body_stream, actor))]
    pub async fn accept_put_manifest<S>(
        &self,
        actor: Option<EventActor>,
        source_ts: Option<DateTime<Utc>>,
        namespace: &Namespace,
        reference: Reference,
        mime_type: MediaType,
        body_stream: S,
        tags: Vec<Tag>,
    ) -> Result<PutManifestResponse, Error>
    where
        S: AsyncRead + Unpin + Send,
    {
        let resolved_repository = self.resolver.resolve(namespace);

        let created_tags: Vec<Tag> = match &reference {
            Reference::Digest(_) => tags,
            Reference::Tag(_) => Vec::new(),
        };

        let limit = self.max_manifest_size_bytes;
        let mut request_body = Vec::new();
        let mut limited_body = body_stream.take(limit as u64 + 1);

        limited_body
            .read_to_end(&mut request_body)
            .await
            .map_err(|_| {
                Error::ManifestInvalid("Unable to retrieve manifest from client query".to_string())
            })?;

        if request_body.len() > limit {
            return Err(Error::ManifestBodyTooLarge { limit });
        }

        // The tie-break compares digests, so only a replicated write pays this
        // extra hash of the body.
        let incoming_digest = source_ts
            .is_some()
            .then(|| Digest::sha256_of_bytes(&request_body));
        self.check_lww_not_superseded(namespace, &reference, source_ts, incoming_digest.as_ref())
            .await?;

        let reference_policy = if self.validate_manifest_references {
            ReferencePolicy::Strict
        } else {
            ReferencePolicy::Permissive
        };
        let mut response = self
            .store_manifest(
                namespace,
                &reference,
                Some(&mime_type),
                &request_body,
                &created_tags,
                reference_policy,
                source_ts,
            )
            .await?;

        let repository = resolved_repository
            .map(|r| r.name.to_string())
            .unwrap_or_default();
        let digest_str = response.headers.get(DOCKER_CONTENT_DIGEST).cloned();

        response.events.push(manifest_event(
            EventKind::ManifestPush,
            namespace,
            repository.clone(),
            digest_str.clone(),
            &reference,
            actor.clone(),
        ));

        if let Some(tag) = reference.as_tag() {
            response.events.push(tag_event(
                EventKind::TagCreate,
                namespace,
                repository.clone(),
                digest_str.clone(),
                &reference,
                tag,
                actor.clone(),
            ));
        }

        // Each `?tag=` created tag gets its own TagCreate event.
        for tag in &created_tags {
            response.events.push(tag_event(
                EventKind::TagCreate,
                namespace,
                repository.clone(),
                digest_str.clone(),
                &reference,
                tag,
                actor.clone(),
            ));
        }

        // No-op suppression: only a write that changed local state is replicated,
        // so a converged replay does not keep a mesh cycle alive.
        if response.changed {
            self.replicate_manifest_push(
                resolved_repository,
                namespace,
                &reference,
                &created_tags,
                &response.digest,
            )
            .await;
        }

        Ok(response)
    }

    /// Replicate a manifest push to every matching downstream: the path tag plus
    /// each `?tag=` created tag, so a by-digest push with tag params converges
    /// identically on every replica.
    async fn replicate_manifest_push(
        &self,
        repository: Option<&Repository>,
        namespace: &Namespace,
        reference: &Reference,
        created_tags: &[Tag],
        digest: &Digest,
    ) {
        let path_tag = reference.as_tag();
        self.dispatch_replication(
            repository,
            namespace,
            REPLICATION_PUSH_MANIFEST_KIND,
            path_tag,
            Some(digest),
            None,
        )
        .await;

        for tag in created_tags {
            self.dispatch_replication(
                repository,
                namespace,
                REPLICATION_PUSH_MANIFEST_KIND,
                Some(tag),
                Some(digest),
                None,
            )
            .await;
        }
    }

    /// Fire-and-forget enqueue of replication push/delete jobs, one per matching
    /// downstream; failures are logged and counted, never failing the client's
    /// write. Invoke only when the write changed local state.
    pub async fn dispatch_replication(
        &self,
        repository: Option<&Repository>,
        namespace: &Namespace,
        kind: &str,
        tag: Option<&Tag>,
        digest: Option<&Digest>,
        source_ts: Option<DateTime<Utc>>,
    ) {
        let Some(repository) = repository else {
            return;
        };

        // Receiver-side LWW timestamp: authoritative for a DELETE; a PUSH
        // re-derives it at execute time. An inbound replicated delete passes its
        // author timestamp verbatim so the bounce can't outrank a later recreate.
        let source_ts = source_ts.unwrap_or_else(Utc::now).to_rfc3339();

        // The per-downstream enqueues run concurrently; this awaits inside the
        // client's PUT/DELETE path, so serial fan-out would add tail latency.
        let dispatches = repository
            .replication
            .iter()
            .filter(|downstream| downstream.enqueues_for(namespace.as_ref()))
            .map(|downstream| {
                let payload = ReplicationPushPayload {
                    downstream: downstream.name.clone(),
                    namespace: namespace.clone(),
                    tag: tag.cloned(),
                    digest: digest.map(ToString::to_string),
                    kind: kind.to_string(),
                    source_ts: Some(source_ts.clone()),
                };
                async move {
                    // Build + enqueue as one fallible step sharing the warn +
                    // metric path.
                    let outcome = match build_envelope(&payload) {
                        Ok(envelope) => self
                            .job_queue
                            .enqueue(envelope)
                            .await
                            .map_err(|e| e.to_string()),
                        Err(e) => Err(e.to_string()),
                    };
                    if let Err(error) = outcome {
                        warn!(
                            "Failed to dispatch replication job for {}: {error}",
                            downstream.name
                        );
                        metrics_provider()
                            .job_queue_enqueue_failures_total
                            .with_label_values(&[Queue::Replication.as_str()])
                            .inc();
                    }
                }
            });
        join_all(dispatches).await;
    }
}

#[cfg(test)]
mod tests;
