pub mod link_plan;
mod parse;
mod response;

use chrono::{DateTime, Utc};
pub use parse::{ParsedManifestDigests, parse_manifest_digests};
use parse::{manifest_meta_from_body, parse_and_validate_manifest};
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
    oci::{Digest, Manifest, Namespace, Reference},
    registry::{
        DOCKER_CONTENT_DIGEST, Error, Registry, Repository,
        blob_ownership::BlobOwnership,
        blob_store::Error as BlobStoreError,
        metadata_store::{Error as MetadataStoreError, link_kind::LinkKind},
    },
    replication::{REPLICATION_DELETE_MANIFEST_KIND, REPLICATION_PUSH_MANIFEST_KIND},
    util::sha256,
};

pub const DEFAULT_MAX_MANIFEST_SIZE_BYTES: usize = 5 * 1024 * 1024;

fn manifest_event(
    kind: EventKind,
    namespace: &Namespace,
    repository: String,
    digest: Option<String>,
    reference: &Reference,
    actor: Option<EventActor>,
) -> Event {
    Event::new(kind, namespace.to_string(), repository)
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
    tag: &str,
    actor: Option<EventActor>,
) -> Event {
    Event::new(kind, namespace.to_string(), repository)
        .digest(digest)
        .reference(Some(reference.to_string()))
        .tag(Some(tag.to_string()))
        .actor(actor)
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

    async fn head_local_manifest(
        &self,
        namespace: &Namespace,
        reference: &Reference,
    ) -> Result<ManifestMeta, Error> {
        let blob_link = LinkKind::from_reference(reference);
        let link = self
            .metadata_store
            .read_link(namespace, &blob_link, self.update_pull_time)
            .await?;

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

        self.store_manifest(namespace, &reference, media_type.as_ref(), &content, false)
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
        let link = self
            .metadata_store
            .read_link(namespace, &blob_link, self.update_pull_time)
            .await?;

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

    #[instrument(skip(body))]
    pub async fn put_manifest(
        &self,
        namespace: &Namespace,
        reference: &Reference,
        content_type: Option<&String>,
        body: &[u8],
    ) -> Result<PutManifestResponse, Error> {
        self.store_manifest(namespace, reference, content_type, body, true)
            .await
    }

    async fn store_manifest(
        &self,
        namespace: &Namespace,
        reference: &Reference,
        content_type: Option<&String>,
        body: &[u8],
        validate_references: bool,
    ) -> Result<PutManifestResponse, Error> {
        let mut manifest = parse_and_validate_manifest(body, content_type)?;
        let computed_digest = Digest::Sha256(sha256::hex(body).into());

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

        if validate_references {
            self.validate_manifest_references(namespace, &manifest)
                .await?;
        }

        let effective_media_type = content_type
            .cloned()
            .or_else(|| manifest.media_type.clone());

        let ops = link_plan::push(
            &mut manifest,
            &computed_digest,
            reference,
            effective_media_type.as_deref(),
            body.len() as u64,
        );

        self.metadata_store
            .store_manifest(namespace.as_ref(), &computed_digest, body, &ops)
            .await?;

        let subject = manifest.subject.map(|s| s.digest);

        Ok(PutManifestResponse {
            headers: put_manifest_headers(namespace, reference, &computed_digest, subject.as_ref()),
            events: Vec::new(),
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

    #[instrument(skip(actor))]
    pub async fn delete_manifest(
        &self,
        actor: Option<EventActor>,
        source_ts: Option<DateTime<Utc>>,
        namespace: &Namespace,
        reference: &Reference,
    ) -> Result<DeleteManifestResponse, Error> {
        self.check_lww_not_superseded(namespace, reference, source_ts)
            .await?;

        // No-op suppression (loop prevention): record whether the reference
        // existed locally BEFORE the delete. A replicated delete that finds
        // nothing to remove (the ref was already absent) is a converged replay;
        // re-dispatching it would keep a mesh cycle alive. (Client deletes of an
        // absent ref usually 404 before reaching here, but gate defensively.)
        // Only a genuinely-absent ref (ReferenceNotFound) counts as "nothing to
        // delete". A transient read failure must NOT silently suppress a genuine
        // delete, so it is treated as "existed" and falls through to dispatch.
        //
        // The read is gated on an event-enqueuing downstream matching this
        // namespace: with nothing to dispatch the prior state is unused, so the
        // gate short-circuits the `read_link` entirely. `existed_before` is then
        // false, and the `if existed_before { dispatch_replication... }` below
        // correctly skips — no downstream would enqueue anyway.
        let existed_before = self.replicates_on_event(namespace)
            && !matches!(
                self.metadata_store
                    .read_link(namespace, &LinkKind::from_reference(reference), false)
                    .await,
                Err(MetadataStoreError::ReferenceNotFound)
            );

        let ops = if let Reference::Digest(digest) = reference {
            let tags = self
                .metadata_store
                .find_tags_pointing_at(namespace.as_ref(), digest)
                .await?;
            let manifest = self
                .blob_store
                .read(digest)
                .await
                .ok()
                .and_then(|content| Manifest::from_slice(&content).ok());
            link_plan::delete(reference, manifest.as_ref(), &tags)
        } else {
            link_plan::delete(reference, None, &[])
        };

        if let Reference::Digest(digest) = reference {
            self.metadata_store
                .delete_manifest(namespace.as_ref(), digest, &ops)
                .await?;
        } else {
            self.metadata_store
                .update_links(namespace.as_ref(), &ops)
                .await?;
        }

        let repository = self.repository_name_for(namespace);
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

        if let Reference::Tag(tag) = reference {
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

        // For a tag delete the receiver keys off `payload.tag`, so no digest is
        // resolved or carried. For a digest delete the digest IS the reference,
        // so it is dispatched as `Some`.
        let (tag, dispatch_digest) = match reference {
            Reference::Tag(tag) => (Some(tag.as_str()), None),
            Reference::Digest(digest) => (None, Some(digest)),
        };
        // No-op suppression: only re-propagate a delete that actually removed
        // something locally. Webhook events above fire unconditionally; only the
        // replication dispatch is gated.
        if existed_before {
            self.dispatch_replication(
                namespace,
                REPLICATION_DELETE_MANIFEST_KIND,
                tag,
                dispatch_digest,
            )
            .await;
        }

        Ok(DeleteManifestResponse { events })
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
        let link = self
            .metadata_store
            .read_link(namespace, &blob_link, self.update_pull_time)
            .await
            .ok()?;
        let media_type = link.media_type?;
        let presigned_url = self
            .blob_store
            .presigned_url(&link.target, Some(media_type.as_str()))
            .await
            .ok()??;

        Some(GetManifestResponse::Redirect {
            headers: get_manifest_redirect_headers(presigned_url, &link.target, Some(media_type)),
        })
    }

    /// Resolves a manifest GET request to either a presigned redirect URL or the manifest body.
    ///
    /// The redirect fast-path is safe only when the cached target is authoritative:
    /// the repository is not a pull-through cache, the reference is a digest, or the
    /// tag has been declared immutable. For mutable tags on a pull-through cache we
    /// fall through to `get_manifest` to refresh if upstream has moved.
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
            });
        }

        Ok(GetManifestResponse::Body {
            headers: get_manifest_body_headers(manifest.media_type.as_deref(), &manifest.digest),
            content: manifest.content,
        })
    }

    /// Last-writer-wins guard for a replication-originated tag write.
    ///
    /// LWW applies only when the request carries a `source_ts` (i.e. it is a
    /// replication push/delete, not a genuine client write) and the reference is
    /// a tag (digest references are content-addressed, so there is no conflict to
    /// resolve). When the local tag currently resolves to a `created_at` that is
    /// strictly newer than the incoming `source_ts`, the local copy wins and the
    /// write is rejected with [`Error::ReplicationSuperseded`] — a 409 carrying a
    /// distinct OCI code so the sender treats it as convergence rather than a
    /// retryable conflict. A local tag with no recorded `created_at` is treated
    /// as oldest and never blocks the incoming write.
    async fn check_lww_not_superseded(
        &self,
        namespace: &Namespace,
        reference: &Reference,
        source_ts: Option<DateTime<Utc>>,
    ) -> Result<(), Error> {
        let Some(source_ts) = source_ts else {
            return Ok(());
        };
        let Reference::Tag(tag) = reference else {
            return Ok(());
        };

        // Distinguish a genuinely-absent tag from a transient read failure. A
        // missing tag means nothing supersedes the incoming write, so we proceed.
        // Any other error (storage/coordination/deserialization) must NOT silently
        // disable LWW — collapsing those to "no local tag" would let an older
        // replicated write overwrite a strictly-newer local tag during a backend
        // hiccup. Surface them so the replicated write is refused and the sender
        // retries instead of overwriting.
        let local_created_at = match self
            .metadata_store
            .read_link(namespace, &LinkKind::Tag(tag.clone()), false)
            .await
        {
            Ok(link) => link.created_at,
            Err(MetadataStoreError::ReferenceNotFound) => None,
            Err(err) => return Err(Error::from(err)),
        };

        // No local tag, or no recorded creation time: nothing supersedes the
        // incoming write.
        let Some(local_created_at) = local_created_at else {
            return Ok(());
        };

        if local_created_at > source_ts {
            return Err(Error::ReplicationSuperseded(format!(
                "local tag '{tag}' (created {local_created_at}) is newer than the replicated source ({source_ts})"
            )));
        }

        Ok(())
    }

    /// Reads the body stream, calls `put_manifest`, and returns the domain response.
    #[instrument(skip(self, body_stream, actor))]
    pub async fn accept_put_manifest<S>(
        &self,
        actor: Option<EventActor>,
        source_ts: Option<DateTime<Utc>>,
        namespace: &Namespace,
        reference: Reference,
        mime_type: String,
        body_stream: S,
    ) -> Result<PutManifestResponse, Error>
    where
        S: AsyncRead + Unpin + Send,
    {
        self.check_lww_not_superseded(namespace, &reference, source_ts)
            .await?;

        // No-op suppression (loop prevention): capture the PRIOR local state for
        // this reference BEFORE the write so we can decide, after the write,
        // whether local state actually changed. A re-asserted tag (same target)
        // or an already-present revision is a converged replay; re-dispatching it
        // would keep a 3+-node mesh cycle alive even though LWW has nothing to
        // drop. This no-op suppression is now the sole terminator of 3+-node
        // cycles. The result is interpreted per-reference below (tag target moved
        // vs revision newly created).
        //
        // The read is SKIPPED entirely when no event-enqueuing downstream matches
        // this namespace (replication off, or its filter excludes us): the prior
        // state is consumed solely to gate dispatch, so with nothing to dispatch
        // it is pure waste — skipping it restores the v1.2.0 cost for
        // replication-disabled deployments. `None` therefore means "not read"
        // and the post-write dispatch block below does not run.
        let prior_link = if self.replicates_on_event(namespace) {
            Some(
                self.metadata_store
                    .read_link(namespace, &LinkKind::from_reference(&reference), false)
                    .await,
            )
        } else {
            None
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

        let mut response = self
            .put_manifest(namespace, &reference, Some(&mime_type), &request_body)
            .await?;

        let repository = self.repository_name_for(namespace);
        let digest_str = response.headers.get(DOCKER_CONTENT_DIGEST).cloned();

        response.events.push(manifest_event(
            EventKind::ManifestPush,
            namespace,
            repository.clone(),
            digest_str.clone(),
            &reference,
            actor.clone(),
        ));

        if let Reference::Tag(tag) = &reference {
            response.events.push(tag_event(
                EventKind::TagCreate,
                namespace,
                repository,
                digest_str.clone(),
                &reference,
                tag,
                actor,
            ));
        }

        // Enqueue replication for the stored manifest, but ONLY when the write
        // actually changed local state (no-op suppression — see `prior_link`).
        // The `Docker-Content-Digest` header is the canonical stored digest
        // (`digest.to_string()`), so it round-trips back into a `Digest`.
        //
        // Webhook events above are emitted unconditionally; only the replication
        // dispatch is gated, so observers still see every push while the mesh
        // stops re-propagating converged replays.
        if let (Some(prior_link), Some(digest)) = (
            prior_link.as_ref(),
            digest_str.as_deref().and_then(|d| d.parse::<Digest>().ok()),
        ) {
            let (tag, changed) = match &reference {
                // A tag push moves local state when the tag was absent OR pointed
                // at a different digest. A tag re-asserted to the same digest is a
                // converged replay (e.g. an inbound replicated bounce) — skip.
                Reference::Tag(tag) => {
                    // Changed unless the tag already pointed at this exact digest.
                    // A transient read failure (any non-Ok) must not silently
                    // suppress a genuine change, so it falls through to dispatch.
                    let changed = !matches!(prior_link, Ok(link) if link.target == digest);
                    (Some(tag.as_str()), changed)
                }
                // A digest push is content-addressed: it changes local state only
                // when the revision did not already exist locally. A re-push of an
                // already-present revision is a converged replay — skip, otherwise
                // an A<->B digest push loops forever.
                Reference::Digest(_) => {
                    // Changed unless the revision is CONFIRMED present (prior read
                    // returned Ok). A transient read failure (any non-Ok, incl. a
                    // backend hiccup that is not ReferenceNotFound) must not
                    // silently suppress a genuine change after the local write
                    // already succeeded, so it falls through to dispatch — mirroring
                    // the tag and delete arms (err toward dispatching).
                    let changed = prior_link.is_err();
                    (None, changed)
                }
            };

            if changed {
                self.dispatch_replication(
                    namespace,
                    REPLICATION_PUSH_MANIFEST_KIND,
                    tag,
                    Some(&digest),
                )
                .await;
            }
        }

        Ok(response)
    }
}

#[cfg(test)]
mod tests;
