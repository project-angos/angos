pub mod link_plan;
mod parse;
mod response;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures_util::future::join_all;
pub use parse::{ParsedManifestDigests, parse_manifest_digests};
use parse::{manifest_meta_from_body, parse_and_validate_manifest};
pub use response::{GetManifestResponse, HeadManifestResponse, PutManifestResponse};
use response::{
    ManifestBody, ManifestMeta, get_manifest_body_headers, get_manifest_redirect_headers,
    head_manifest_headers, put_manifest_headers,
};
use tokio::io::{AsyncRead, AsyncReadExt};
use tracing::{error, instrument, warn};

use crate::{
    event_webhook::event::{Event, EventActor, EventKind},
    jobs::Queue,
    metrics_provider::metrics_provider,
    oci::{Digest, Manifest, MediaType, Namespace, Reference, Tag},
    registry::{
        Error, Registry, Repository,
        blob_ownership::BlobOwnership,
        blob_store::Error as BlobStoreError,
        cache_job_handler::CACHE_ACTOR,
        metadata_store::{
            Error as MetadataStoreError, LinkKind, LinkMetadata, LinkOperation, LinksCommit,
        },
    },
    replication::{
        REPLICATION_DELETE_MANIFEST_KIND, REPLICATION_PUSH_MANIFEST_KIND, ReplicationDownstream,
        ReplicationPushPayload, build_envelope,
    },
};

pub const DEFAULT_MAX_MANIFEST_SIZE_BYTES: usize = 5 * 1024 * 1024;

/// How a manifest push treats descriptors that reference content the target
/// namespace does not already own.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReferencePolicy {
    /// Reject the push with `MANIFEST_BLOB_UNKNOWN`.
    Strict,
    /// Store the manifest but skip the ownership-granting links for unowned
    /// references, so they stay dangling and resolve as unknown on a later pull
    /// instead of handing the namespace read access to content it never pushed.
    Permissive,
    /// Trust every reference as owned. Used only by pull-through cache-fill,
    /// where the referenced content is fetched from the upstream the namespace
    /// mirrors.
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

/// The `ManifestDelete` event a delete emits, plus a `TagDelete` when the
/// reference is a tag.
fn delete_events(
    namespace: &Namespace,
    repository: String,
    digest: Option<String>,
    reference: &Reference,
    actor: Option<EventActor>,
) -> Vec<Event> {
    let mut events = vec![manifest_event(
        EventKind::ManifestDelete,
        namespace,
        repository.clone(),
        digest.clone(),
        reference,
        actor.clone(),
    )];
    if let Some(tag) = reference.as_tag() {
        events.push(tag_event(
            EventKind::TagDelete,
            namespace,
            repository,
            digest,
            reference,
            tag,
            actor,
        ));
    }
    events
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

        // Backward compatibility: links created before media_type was stored require
        // a full blob read. Remove this fallback once all links have been re-pushed.
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

        // The registry is about to gain upstream content, so webhook
        // consumers see the intent like any other write. Best effort: the
        // client operation is the pull, so a delivery failure must not fail it.
        let event = manifest_event(
            EventKind::ManifestPush,
            namespace,
            repository.name.to_string(),
            Some(digest.to_string()),
            &reference,
            Some(EventActor::internal(CACHE_ACTOR)),
        );
        if let Err(error) = self.dispatch_events(&[event]).await {
            warn!("Cache-fill event delivery failed: {error}");
        }

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
        // A digest reference fixes the algorithm to verify against; a tag push has
        // no client-chosen algorithm, so the manifest lands under its canonical
        // sha256 digest.
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

        let mut ops = link_plan::push(
            &mut manifest,
            &computed_digest,
            reference,
            effective_media_type.as_ref(),
            body.len() as u64,
            created_tags,
        );

        match reference_policy {
            ReferencePolicy::Strict => {
                self.validate_manifest_references(namespace, &manifest)
                    .await?;
            }
            ReferencePolicy::Permissive => {
                ops = self.retain_owned_reference_links(namespace, ops).await?;
            }
            ReferencePolicy::Trusted => {}
        }

        // Write the manifest blob-data to the blob store before the link
        // transaction (a link must never point at absent bytes) and hold the
        // blob-data lock across both so a concurrent delete cannot reclaim the
        // blob between the write and the link. A crash or LWW-supersession in
        // between leaves at most an orphan blob, which scrub reclaims.
        let commit: LinksCommit = self
            .metadata_store
            .with_blob_data_lock(&computed_digest, async {
                self.blob_store
                    .put_blob(&computed_digest, Bytes::copy_from_slice(body))
                    .await?;
                self.metadata_store
                    .store_manifest(namespace, &ops, created_at)
                    .await
                    .map_err(|e| match e {
                        MetadataStoreError::ReplicationSuperseded(message) => {
                            Error::ReplicationSuperseded(message)
                        }
                        e => Error::from(e),
                    })
            })
            .await?;

        // Changed-state check from the prior target the committed transaction
        // itself validated; a missing entry fails open so a genuine write is
        // never suppressed. A by-digest push with `?tag=` also counts its created
        // tag links so newly added tags replicate even when the digest is present.
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
            changed,
        })
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

    /// Drops the content-reference links (config, layer, child manifest) for
    /// descriptors the namespace does not already own, leaving the manifest's
    /// own digest, tag, and subject back-link untouched, so a permissive push
    /// never grants read access to a digest the namespace did not upload.
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

    /// Deletes a manifest or tag. The delete's initiator is read off the
    /// `actor`: an internal actor (retention enforcement) mirrors the delete
    /// only to downstreams marked `prune = true`, a client delete to every
    /// matching downstream.
    #[instrument(skip(actor))]
    pub async fn delete_manifest(
        &self,
        actor: Option<EventActor>,
        source_ts: Option<DateTime<Utc>>,
        namespace: &Namespace,
        reference: &Reference,
    ) -> Result<(), Error> {
        let client_initiated = actor.as_ref().is_none_or(EventActor::is_client);
        let resolved_repository = self.resolver.resolve(namespace);

        let repository = resolved_repository
            .map(|r| r.name.to_string())
            .unwrap_or_default();
        let digest_str = match reference {
            Reference::Digest(d) => Some(d.to_string()),
            Reference::Tag(_) => None,
        };
        // Intent-first emission: the events fire before the delete, so a
        // performed delete can never go unnotified; a delete that fails past
        // this point leaves a false-positive notification instead.
        let events = delete_events(namespace, repository, digest_str, reference, actor);
        self.dispatch_events(&events).await?;

        // A delete carries no incoming digest, so a timestamp tie keeps the
        // strictly-greater rule and the delete proceeds.
        self.check_lww_not_superseded(namespace, reference, source_ts, None)
            .await?;

        // A digest delete cascades to every pointing tag; resolve them first
        // for the suppression gate, the LWW guard, and the link plan.
        let pointing_tags = if let Reference::Digest(digest) = reference {
            self.metadata_store
                .find_tags_pointing_at(namespace, digest)
                .await?
        } else {
            Vec::new()
        };

        // No-op suppression: the ref counts as absent only when the prior link
        // is gone AND no tag still points at it; a transient read error counts
        // as "existed" so a real delete is never suppressed. Unlike the put
        // gate's commit-validated `LinksCommit::changed` this is a pre-commit
        // read a racing write can flip, which is safe: over-dispatch is
        // idempotent, and the one suppression race coincides with a concurrent
        // re-put whose own dispatch converges the mesh.
        let existed_before = match self
            .prior_link_if_replicated(resolved_repository, namespace, reference)
            .await
        {
            None => false,
            Some(Err(MetadataStoreError::ReferenceNotFound)) => !pointing_tags.is_empty(),
            Some(_) => true,
        };

        let ops = if let Reference::Digest(digest) = reference {
            // A tag re-pointed locally after the delete was authored must not
            // be dropped by the older replicated delete.
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

        // The pre-commit guards above fail fast; threading `source_ts` into the
        // transaction makes the deleted tag links part of its validated read
        // set, so a concurrent newer re-put aborts the delete rather than being
        // clobbered by an older replicated delete.
        if let Reference::Digest(digest) = reference {
            // The manifest blob-data is content in the blob store. Hold the
            // blob-data lock across the unreferenced-check + reclaim so a
            // concurrent reference grant isn't missed, then reclaim the bytes
            // when the delete left the blob unreferenced.
            self.metadata_store
                .with_blob_data_lock(digest, async {
                    if self
                        .metadata_store
                        .delete_manifest(namespace, digest, &ops, source_ts)
                        .await?
                    {
                        self.blob_store.delete_blob(digest).await?;
                    }
                    Ok::<_, Error>(())
                })
                .await?;
        } else {
            self.metadata_store
                .delete_links(namespace, &ops, source_ts)
                .await?;
        }

        // For a tag delete the receiver keys off `payload.tag`, so no digest
        // is carried.
        let (tag, dispatch_digest) = match reference {
            Reference::Tag(tag) => (Some(tag), None),
            Reference::Digest(digest) => (None, Some(digest)),
        };
        // Webhook events fire unconditionally; only the replication
        // dispatch is gated on a real removal. A replicated delete forwards
        // its author timestamp verbatim so the bounce can never outrank a
        // recreate authored after the original delete.
        if existed_before && let Some(repository) = resolved_repository {
            // An internal delete (retention) mirrors only to authoritative
            // `prune = true` downstreams, so additive downstreams never lose
            // content because of upstream retention.
            let downstreams = repository
                .replication
                .iter()
                .filter(|downstream| client_initiated || downstream.prune);
            self.dispatch_replication_to(
                downstreams,
                namespace,
                REPLICATION_DELETE_MANIFEST_KIND,
                tag,
                dispatch_digest,
                source_ts,
            )
            .await;
        }

        Ok(())
    }

    /// Attempts to short-circuit a manifest GET into a presigned redirect using
    /// only the link metadata (without reading the manifest blob). Returns
    /// `Some(Redirect)` when the link records a `media_type` AND the configured
    /// `PresignedBlobStore` produces a URL; otherwise returns `None` so the caller
    /// falls through to the body-loading path.
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
            digest: link.target,
        })
    }

    /// Resolves a manifest GET request to a presigned redirect URL or the
    /// manifest body, then emits a `manifest.pull` event for the served
    /// digest. The redirect fast-path is taken only when the cached target
    /// is authoritative (not a pull-through cache, a digest reference, or an
    /// immutable tag); mutable tags on a pull-through cache fall through to
    /// `get_manifest` to refresh if upstream has moved.
    #[instrument(skip(self, is_tag_immutable, actor))]
    pub async fn resolve_get_manifest(
        &self,
        actor: Option<EventActor>,
        namespace: &Namespace,
        reference: Reference,
        mime_types: &[String],
        is_tag_immutable: bool,
    ) -> Result<GetManifestResponse, Error> {
        let repository = self.get_repository_for_namespace(namespace)?;
        let repository_name = repository.name.to_string();
        let event_tag = reference.as_tag().map(ToString::to_string);
        let reference_str = reference.to_string();

        let response = self
            .resolve_get_manifest_response(
                repository,
                namespace,
                reference,
                mime_types,
                is_tag_immutable,
            )
            .await?;

        let event = Event::new(EventKind::ManifestPull, namespace.clone(), repository_name)
            .digest(Some(response.digest().to_string()))
            .reference(Some(reference_str))
            .tag(event_tag)
            .actor(actor);
        self.dispatch_events(&[event]).await?;

        Ok(response)
    }

    async fn resolve_get_manifest_response(
        &self,
        repository: &Repository,
        namespace: &Namespace,
        reference: Reference,
        mime_types: &[String],
        is_tag_immutable: bool,
    ) -> Result<GetManifestResponse, Error> {
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

        // Backward compatibility: when the optimized redirect path above fails (link
        // lacks media_type), fall back to redirecting after reading the full blob.
        // Remove this block once all links have been re-pushed.
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
                digest: manifest.digest,
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
            digest: manifest.digest,
        })
    }

    /// Last-writer-wins guard for a replication-originated tag write: rejects
    /// with [`Error::ReplicationSuperseded`] (a distinct 409 the sender records
    /// as convergence, not a retryable conflict) when the local tag strictly
    /// supersedes the incoming `source_ts` per [`Self::link_supersedes`].
    /// Skipped without a `source_ts` (genuine client write) and for digest
    /// references (content-addressed); ordering uses the author's write time,
    /// persisted as `created_at` and propagated verbatim across hops, so
    /// multi-hop ordering is deterministic.
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

    /// `Some(created_at)` iff the local link strictly supersedes the incoming
    /// write per [`LinkMetadata::supersedes`]; `None` when the link is absent
    /// or loses. Read errors other than `ReferenceNotFound` fail closed, and
    /// reads bypass the link cache to avoid its multi-replica staleness.
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

    /// Last-writer-wins guard for a replication-originated digest delete: the
    /// delete cascades to every pointing tag, so a tag re-pointed locally
    /// after the delete was authored rejects the whole delete with
    /// [`Error::ReplicationSuperseded`], preserving the tag and the revision
    /// it still references.
    async fn check_digest_delete_not_superseded(
        &self,
        namespace: &Namespace,
        tags: &[LinkKind],
        source_ts: DateTime<Utc>,
    ) -> Result<(), Error> {
        for tag in tags {
            // No incoming digest, so a timestamp tie lets the delete proceed.
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
    /// downstream matches `namespace` (`None` otherwise) so the replication-off
    /// path pays no extra read. Read errors other than `ReferenceNotFound` are
    /// surfaced rather than collapsed to "absent", and the read bypasses the
    /// link cache, so a hiccup or stale cache never suppresses a real change.
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

    /// Reads the body stream, calls `put_manifest`, and returns the domain response.
    ///
    /// `tags` carries the pre-validated values of `?tag=` query parameters; they
    /// apply only when `reference` is a `Reference::Digest`. A by-tag push
    /// ignores them and creates no extra tags.
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

        // Hashed up front: the intent events fired before the store carry the
        // content digest, and the LWW tie-break compares it on equal timestamps.
        let digest = Digest::sha256_of_bytes(&request_body);

        let repository = resolved_repository
            .map(|r| r.name.to_string())
            .unwrap_or_default();
        let digest_str = Some(digest.to_string());

        let mut events = vec![manifest_event(
            EventKind::ManifestPush,
            namespace,
            repository.clone(),
            digest_str.clone(),
            &reference,
            actor.clone(),
        )];

        if let Some(tag) = reference.as_tag() {
            events.push(tag_event(
                EventKind::TagCreate,
                namespace,
                repository.clone(),
                digest_str.clone(),
                &reference,
                tag,
                actor.clone(),
            ));
        }

        // Each tag created by a `?tag=` query parameter gets its own TagCreate
        // event, mirroring the by-tag push above.
        for tag in &created_tags {
            events.push(tag_event(
                EventKind::TagCreate,
                namespace,
                repository.clone(),
                digest_str.clone(),
                &reference,
                tag,
                actor.clone(),
            ));
        }

        // Intent-first emission: the events fire before the write, so a
        // performed write can never go unnotified; a write that fails past
        // this point leaves a false-positive notification instead.
        self.dispatch_events(&events).await?;

        self.check_lww_not_superseded(namespace, &reference, source_ts, Some(&digest))
            .await?;

        let reference_policy = if self.validate_manifest_references {
            ReferencePolicy::Strict
        } else {
            ReferencePolicy::Permissive
        };
        let response = self
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

        // No-op suppression: re-dispatching a converged replay would keep a
        // mesh cycle alive, so only a write that changed local state (per the
        // committed transaction) is replicated. Webhook events fire
        // unconditionally.
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

    /// Replicates a manifest push to every matching downstream: the path tag
    /// (when the reference is a tag) plus each tag created via a `?tag=` query
    /// parameter, so a by-digest push with tag params converges identically on
    /// every replica.
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
    /// downstream; failures are logged and counted but never fail the client's write.
    /// Callers must only invoke this when the write changed local state, which is
    /// what makes mesh cycles terminate.
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
        self.dispatch_replication_to(
            repository.replication.iter(),
            namespace,
            kind,
            tag,
            digest,
            source_ts,
        )
        .await;
    }

    /// [`Registry::dispatch_replication`] over a caller-selected downstream
    /// set, for dispatches that must not fan out to every downstream (a
    /// retention delete targets only `prune = true` mirrors).
    async fn dispatch_replication_to<'a>(
        &self,
        downstreams: impl Iterator<Item = &'a ReplicationDownstream>,
        namespace: &Namespace,
        kind: &str,
        tag: Option<&Tag>,
        digest: Option<&Digest>,
        source_ts: Option<DateTime<Utc>>,
    ) {
        // Receiver-side last-writer-wins timestamp: authoritative for a DELETE;
        // a PUSH re-derives it at execute time, so a coalesced push never goes
        // stale. An inbound replicated delete passes its author timestamp so it
        // propagates verbatim: re-stamping `now()` would let the bounced delete
        // outrank (and destroy) a recreate that landed in between.
        let source_ts = source_ts.unwrap_or_else(Utc::now).to_rfc3339();

        // The per-downstream enqueues run concurrently: each one is an index
        // GET plus a CAS transaction, and this awaits inside the client's
        // PUT/DELETE response path, so serial fan-out adds tail latency.
        let dispatches = downstreams
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
                    // Build + enqueue as one fallible step so failures share the warn + metric path.
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
