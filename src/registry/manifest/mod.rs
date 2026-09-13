pub mod link_plan;
mod response;

use std::{iter::once, slice};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures_util::future::join_all;
use http::{HeaderMap, Response, StatusCode};
use tokio::io::{AsyncRead, AsyncReadExt};
use tracing::{debug, error, instrument, warn};

use angos_oci::request::{
    DeleteManifestRequest, GetManifestRequest, HeadManifestRequest, PutManifestRequest,
};
use angos_oci::server;
use angos_oci::{Content, Digest, Manifest, MediaRange, MediaType, Namespace, Reference, Tag};

use crate::{
    cache_fill::CACHE_ACTOR,
    event_webhook::event::{Event, EventActor},
    http_response::{ResponseBody, build_response},
    jobs::Queue,
    layer,
    metrics_provider::metrics_provider,
    registry::{
        Error, Registry, Repository,
        blob_store::BlobStore,
        metadata_store::{LinkKind, LinkMetadata, LinkOperation, LinksCommit, ReferencePolicy},
        pull_through_name, record_pull_through, repository_name,
    },
    replication::{ReplicationDownstream, ReplicationJob, ReplicationTarget, build_envelope},
    scan,
};
use response::{GetManifestResponse, ManifestBody, ManifestMeta, PutManifestResponse};

pub const DEFAULT_MAX_MANIFEST_SIZE_BYTES: usize = 5 * 1024 * 1024;

/// The validated inputs to [`Registry::store_manifest`]; the manifest bytes are
/// passed separately.
#[derive(Clone, Copy)]
struct StoreManifest<'a> {
    namespace: &'a Namespace,
    reference: &'a Reference,
    content_type: Option<&'a MediaType>,
    created_tags: &'a [Tag],
    reference_policy: ReferencePolicy,
    created_at: Option<DateTime<Utc>>,
}

/// What a replication job addresses, each variant carrying only the
/// coordinates its job kind uses.
pub enum DispatchTarget<'a> {
    /// The digest is authoritative; the tag is the push's path tag, absent for
    /// a by-digest push.
    Push {
        tag: Option<&'a Tag>,
        digest: &'a Digest,
    },
    /// The receiver keys off the tag, and the manifest itself stays.
    TagDelete { tag: &'a Tag },
    /// Carries the referrer's subject so a retry can still prune the
    /// downstream fallback index.
    DigestDelete {
        digest: &'a Digest,
        subject: Option<&'a Digest>,
    },
}

/// Buffers the manifest body, rejecting a stream longer than `limit` bytes.
/// Reads one byte past the limit so an at-limit body is kept and an over-limit
/// one refused.
async fn read_limited_manifest_body<S>(body_stream: S, limit: usize) -> Result<Vec<u8>, Error>
where
    S: AsyncRead + Unpin + Send,
{
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
    Ok(request_body)
}

/// The manifest stored at `digest`. `Ok(None)` when the blob is absent or its
/// body does not parse; a backend fault propagates instead, so a caller
/// cascading a delete cannot mistake an outage for a manifest with no children.
pub async fn read_manifest(
    blob_store: &BlobStore,
    digest: &Digest,
) -> Result<Option<Manifest>, Error> {
    match blob_store.read(digest).await {
        Ok(body) => Ok(Manifest::from_slice(&body).ok()),
        Err(Error::BlobUnknown) => Ok(None),
        Err(e) => Err(e),
    }
}

/// Whether a pull-through repository serves its local copy of a manifest, and
/// why not when it does not. Both misses cost an upstream fetch, so
/// `angos_pull_through_total` counts them apart.
enum ServeLocal<T> {
    Hit(T),
    /// Nothing local to serve.
    Miss,
    /// A mutable tag the upstream has since re-pointed.
    Refresh,
}

impl<T> ServeLocal<T> {
    fn outcome(&self) -> &'static str {
        match self {
            ServeLocal::Hit(_) => "hit",
            ServeLocal::Miss => "miss",
            ServeLocal::Refresh => "refresh",
        }
    }
}

impl Registry {
    #[instrument(skip(actor))]
    pub async fn head_manifest(
        &self,
        actor: Option<EventActor>,
        request: HeadManifestRequest,
    ) -> Result<Response<ResponseBody>, Error> {
        let client = actor.as_ref().map_or("anonymous", EventActor::audit_name);
        let repository = self.get_repository_for_namespace(&request.namespace).ok();
        let cache_of = pull_through_name(repository);
        let is_tag_immutable = self.is_reference_immutable(repository, &request.reference);
        let local = self
            .head_local_manifest(&request.namespace, &request.reference)
            .await;
        let serveable = self
            .serveable_local(
                &request.namespace,
                &request.reference,
                cache_of.is_some(),
                local,
                async |meta| {
                    self.needs_upstream_pull_manifest(
                        repository,
                        &request.accepted_types,
                        &request.namespace,
                        &request.reference,
                        is_tag_immutable,
                        &meta.digest,
                    )
                    .await
                },
            )
            .await?;
        // Only the hit is counted here: the fall-through below goes through
        // `get_manifest`, which counts the outcome it acts on.
        if let ServeLocal::Hit(meta) = serveable {
            record_pull_through(cache_of, "manifest", "hit");
            self.record_manifest_pull(
                &request.namespace,
                &LinkKind::from_reference(&request.reference),
                client,
            )
            .await;
            return Ok(build_response(
                StatusCode::OK,
                server::manifest_headers(meta.media_type.as_ref(), &meta.digest, meta.size)?,
                ResponseBody::empty(),
            )?);
        }

        let body = self
            .get_manifest(
                repository,
                &request.accepted_types,
                &request.namespace,
                request.reference,
                is_tag_immutable,
                client,
            )
            .await?;

        let size = body.content.len() as u64;

        Ok(build_response(
            StatusCode::OK,
            server::manifest_headers(body.media_type.as_ref(), &body.digest, size)?,
            ResponseBody::empty(),
        )?)
    }

    /// Read a manifest/tag link for a client pull, recording its access time
    /// under `client`'s identity when pull-time tracking is enabled.
    async fn read_manifest_link(
        &self,
        namespace: &Namespace,
        link: &LinkKind,
        client: &str,
    ) -> Result<LinkMetadata, Error> {
        if self.update_pull_time {
            self.metadata_store
                .read_link_recording_access(namespace, link, client)
                .await
        } else {
            self.metadata_store.read_link(namespace, link).await
        }
    }

    /// Record one pull of `link` once the request has committed to serving it,
    /// so a probe that ends up not serving does not count as a pull. Recording
    /// is advisory: a failed stamp must not fail the pull it describes.
    async fn record_manifest_pull(&self, namespace: &Namespace, link: &LinkKind, client: &str) {
        if !self.update_pull_time {
            return;
        }
        if let Err(error) = self
            .metadata_store
            .record_link_access(namespace, link, client)
            .await
        {
            warn!(%error, "Failed to record manifest pull time");
        }
    }

    async fn head_local_manifest(
        &self,
        namespace: &Namespace,
        reference: &Reference,
    ) -> Result<ManifestMeta, Error> {
        let blob_link = LinkKind::from_reference(reference);
        // Stamped by the caller once the metadata is actually served: a HEAD
        // that falls through to an upstream refresh is stamped by that path.
        let link = self.metadata_store.read_link(namespace, &blob_link).await?;
        let revision = self
            .probe_revision_for_tag(namespace, reference, &link.target)
            .await?;

        // A missing body is a genuine 404; a backend fault is not, and must not
        // reach the client as a deleted manifest.
        let size = self
            .blob_store
            .size(&link.target)
            .await
            .map_err(|error| match error {
                Error::NotFound | Error::BlobUnknown => Error::ManifestUnknown,
                other => {
                    error!(
                        "Failed to read manifest body size for {namespace}:{reference}: {other}"
                    );
                    other
                }
            })?;

        Ok(ManifestMeta {
            media_type: revision.map_or(link.media_type, |r| r.media_type),
            digest: link.target,
            size,
        })
    }

    #[instrument(skip(repository))]
    pub async fn get_manifest(
        &self,
        repository: Option<&Repository>,
        accepted_types: &[MediaRange],
        namespace: &Namespace,
        reference: Reference,
        is_tag_immutable: bool,
        client: &str,
    ) -> Result<ManifestBody, Error> {
        let cache_of = pull_through_name(repository);
        let local = self.get_local_manifest(namespace, &reference, client).await;
        let serveable = self
            .serveable_local(
                namespace,
                &reference,
                cache_of.is_some(),
                local,
                async |body| {
                    self.needs_upstream_pull_manifest(
                        repository,
                        accepted_types,
                        namespace,
                        &reference,
                        is_tag_immutable,
                        &body.digest,
                    )
                    .await
                },
            )
            .await?;
        record_pull_through(cache_of, "manifest", serveable.outcome());
        if let ServeLocal::Hit(manifest) = serveable {
            return Ok(manifest);
        }

        let Some(repository) = repository else {
            return Err(Error::ManifestUnknown);
        };
        let fetched = repository
            .get_manifest(accepted_types, namespace, &reference)
            .await?;

        // `Docker-Content-Digest` may be omitted, so hash the body under the
        // algorithm the reference asked for; a tag names none and takes sha256.
        let content = fetched.body;
        let media_type = fetched.media_type;
        let digest = match fetched.digest {
            Some(digest) => digest,
            None => match &reference {
                Reference::Digest(requested) => Digest::from_bytes(requested.algorithm(), &content),
                Reference::Tag(_) => Digest::sha256_of_bytes(&content),
            },
        };

        // Best effort: the client operation is the pull, so a delivery failure
        // must not fail it.
        let event = Event::push_manifest(
            namespace,
            &repository.name,
            &digest,
            &reference,
            Some(&EventActor::internal(CACHE_ACTOR)),
        );
        if let Err(error) = self.dispatch_events(&[event]).await {
            warn!("Cache-fill event delivery failed: {error}");
        }

        let stored = self
            .store_manifest(
                &StoreManifest {
                    namespace,
                    reference: &reference,
                    content_type: media_type.as_ref(),
                    created_tags: &[],
                    reference_policy: ReferencePolicy::Trusted,
                    created_at: None,
                },
                &content,
            )
            .await?;
        // A fill is a write like a push: an image landing in a scanning cache
        // repository gets its scan, a refresh to the same digest does not.
        if stored.changed && stored.scan_subject && repository.scan {
            self.dispatch_scan(namespace, &stored.digest).await;
        }
        if stored.changed && repository.index {
            for layer in &stored.layers {
                self.dispatch_index(namespace, layer).await;
            }
        }

        Ok(ManifestBody {
            media_type,
            digest,
            content,
        })
    }

    /// The serve-local gate shared by manifest HEAD and GET. A non-pull-through
    /// repository always serves local, reporting `Error::ManifestUnknown` when
    /// there is none.
    async fn serveable_local<T>(
        &self,
        namespace: &Namespace,
        reference: &Reference,
        pull_through: bool,
        local: Result<T, Error>,
        needs_upstream: impl AsyncFnOnce(&T) -> Result<bool, Error>,
    ) -> Result<ServeLocal<T>, Error> {
        if !pull_through {
            // Only a genuine miss is a 404; collapsing a backend fault into one
            // makes a storage outage look like deleted images.
            return local.map(ServeLocal::Hit).map_err(|error| match error {
                Error::NotFound | Error::ManifestUnknown => {
                    debug!("No local manifest for {namespace}:{reference}");
                    Error::ManifestUnknown
                }
                other => {
                    error!("Failed to read local manifest {namespace}:{reference}: {other}");
                    other
                }
            });
        }
        let Ok(value) = local else {
            return Ok(ServeLocal::Miss);
        };
        if needs_upstream(&value).await? {
            return Ok(ServeLocal::Refresh);
        }
        Ok(ServeLocal::Hit(value))
    }

    async fn needs_upstream_pull_manifest(
        &self,
        repository: Option<&Repository>,
        accepted_types: &[MediaRange],
        namespace: &Namespace,
        reference: &Reference,
        is_tag_immutable: bool,
        local_digest: &Digest,
    ) -> Result<bool, Error> {
        let upstream = repository.filter(|repository| repository.is_pull_through());
        let Some(repository) = upstream else {
            return Ok(false);
        };
        if !matches!(reference, Reference::Tag(_)) || is_tag_immutable {
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
        client: &str,
    ) -> Result<ManifestBody, Error> {
        let blob_link = LinkKind::from_reference(reference);
        let link = self
            .read_manifest_link(namespace, &blob_link, client)
            .await?;

        // A tag entry can outlive its manifest, whose bytes wait for the
        // collector, so a tag resolving to a deleted revision must read as
        // gone; the probe runs alongside the body read.
        let (revision, content) = tokio::join!(
            self.probe_revision_for_tag(namespace, reference, &link.target),
            self.blob_store.read(&link.target),
        );
        Ok(ManifestBody {
            media_type: revision?.map_or(link.media_type, |r| r.media_type),
            digest: link.target,
            content: content?,
        })
    }

    /// The revision's metadata when `reference` is a tag, which also supplies
    /// the served media type (tag resolution carries none). `None` for a digest
    /// reference, `Err(ManifestUnknown)` when the tag's revision is gone.
    async fn probe_revision_for_tag(
        &self,
        namespace: &Namespace,
        reference: &Reference,
        target: &Digest,
    ) -> Result<Option<LinkMetadata>, Error> {
        if !matches!(reference, Reference::Tag(_)) {
            return Ok(None);
        }
        match self
            .metadata_store
            .read_link_reference(namespace, &LinkKind::Digest(target.clone()))
            .await
        {
            Ok(metadata) => Ok(Some(metadata)),
            Err(Error::NotFound) => Err(Error::ManifestUnknown),
            Err(e) => Err(e),
        }
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
            &StoreManifest {
                namespace,
                reference,
                content_type,
                created_tags: &[],
                reference_policy: ReferencePolicy::Strict,
                created_at: None,
            },
            body,
        )
        .await
    }

    async fn store_manifest(
        &self,
        write: &StoreManifest<'_>,
        body: &[u8],
    ) -> Result<PutManifestResponse, Error> {
        let StoreManifest {
            namespace,
            reference,
            content_type,
            created_tags,
            reference_policy,
            created_at,
        } = *write;
        let mut manifest =
            Manifest::from_pushed(body, content_type).map_err(|e| Error::manifest_invalid(&e))?;
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

        let ops = link_plan::push(
            &mut manifest,
            &computed_digest,
            reference,
            effective_media_type.as_ref(),
            body.len() as u64,
            created_tags,
        );

        // Ownership of the referenced digests is checked by the link
        // planner itself, per `reference_policy`; a strict push only
        // pre-verifies that the referenced bytes exist.
        if reference_policy == ReferencePolicy::Strict {
            self.validate_manifest_references(&manifest).await?;
        }

        // The bytes land before the link waves, so no record ever points at
        // absent bytes; they sit inside the collector's grace period, so no
        // lock is needed and a crash in between leaves at most an orphan blob.
        self.blob_store
            .put_blob(&computed_digest, Bytes::copy_from_slice(body))
            .await?;
        let commit: LinksCommit = self
            .metadata_store
            .store_manifest(namespace, &ops, created_at, reference_policy)
            .await?;

        // Changed-state check from the prior target the commit reported; a
        // missing entry fails open so a genuine write is never suppressed. A
        // by-digest push with `?tag=` also counts its created tag links so
        // newly added tags replicate even when the digest is present.
        let changed = commit.changed(&LinkKind::from_reference(reference), &computed_digest)
            || created_tags
                .iter()
                .any(|tag| commit.changed(&LinkKind::Tag(tag.clone()), &computed_digest));

        let scan_subject = scan::is_scan_subject(&manifest);
        let layers = layer::filesystem_layers(&manifest);

        let subject = manifest.subject.map(|s| s.digest);

        Ok(PutManifestResponse {
            headers: server::put_manifest_headers(
                namespace,
                reference,
                &computed_digest,
                subject.as_ref(),
                created_tags,
            )?,
            digest: computed_digest,
            changed,
            scan_subject,
            layers,
        })
    }

    /// Verifies each referenced blob's bytes exist; ownership is checked by
    /// the link planner's own pre-read.
    async fn validate_manifest_references(&self, manifest: &Manifest) -> Result<(), Error> {
        match &manifest.content {
            Content::Image { config, layers } => {
                if let Some(config) = config {
                    self.validate_manifest_reference(&config.digest).await?;
                }
                for layer in layers {
                    self.validate_manifest_reference(&layer.digest).await?;
                }
            }
            Content::Index { manifests } => {
                for child in manifests {
                    self.validate_manifest_reference(&child.digest).await?;
                }
            }
        }

        Ok(())
    }

    async fn validate_manifest_reference(&self, digest: &Digest) -> Result<(), Error> {
        match self.blob_store.size(digest).await {
            Ok(_) => Ok(()),
            Err(Error::BlobUnknown | Error::NotFound) => Err(Error::ManifestBlobUnknown),
            Err(error) => Err(error),
        }
    }

    /// Serves a client's manifest delete; only this entry point answers `202`,
    /// since the retention sweeper also calls [`Registry::delete_manifest`].
    pub async fn accept_delete_manifest(
        &self,
        actor: Option<EventActor>,
        request: DeleteManifestRequest,
    ) -> Result<Response<ResponseBody>, Error> {
        self.delete_manifest(
            actor,
            request.source_ts,
            &request.namespace,
            &request.reference,
        )
        .await?;

        Ok(build_response(
            StatusCode::ACCEPTED,
            HeaderMap::new(),
            ResponseBody::empty(),
        )?)
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
        let repository = repository_name(resolved_repository);
        // Intent-first emission: the events fire before the delete, so a
        // performed delete can never go unnotified; a delete that fails past
        // this point leaves a false-positive notification instead.
        let events = Event::delete_manifest(namespace, &repository, reference, actor.as_ref());
        self.dispatch_events(&events).await?;

        // Read while the manifest is still here: once gone, neither this job nor
        // its retries can name the subject holding the referrer's descriptor.
        let subject = self
            .referrer_subject(resolved_repository, reference)
            .await?;

        // A digest delete cascades to every pointing tag; the planner applies
        // the LWW gate to a replicated delete.
        let existed_before = self
            .commit_manifest_delete(resolved_repository, namespace, reference, source_ts)
            .await?;

        let target = match reference {
            Reference::Tag(tag) => DispatchTarget::TagDelete { tag },
            Reference::Digest(digest) => DispatchTarget::DigestDelete {
                digest,
                subject: subject.as_ref(),
            },
        };
        // Webhook events fire unconditionally; only the replication
        // dispatch is gated on a real removal. A replicated delete forwards
        // its author timestamp verbatim so the bounce can never outrank a
        // recreate authored after the original delete.
        if existed_before && let Some(repository) = resolved_repository {
            let downstreams = repository
                .replication
                .iter()
                .filter(|downstream| client_initiated || downstream.prune);
            self.dispatch_replication_to(downstreams, namespace, target, source_ts)
                .await;
        }

        Ok(())
    }

    /// Subject of the referrer manifest at `reference`, for the delete job to
    /// carry. Only a replicated delete has a fallback index to prune.
    async fn referrer_subject(
        &self,
        repository: Option<&Repository>,
        reference: &Reference,
    ) -> Result<Option<Digest>, Error> {
        let Reference::Digest(digest) = reference else {
            return Ok(None);
        };
        if repository.is_none_or(|repository| repository.replication.is_empty()) {
            return Ok(None);
        }
        Ok(read_manifest(&self.blob_store, digest)
            .await?
            .and_then(|manifest| manifest.subject)
            .map(|subject| subject.digest))
    }

    /// Whether the reference counted as present before the delete, gating the
    /// replication dispatch; absent only when the prior link is gone AND no tag
    /// still points at it. A transient read error counts as present so a real
    /// delete is never suppressed, and a racing write flipping this pre-read is
    /// safe because over-dispatch is idempotent.
    async fn manifest_delete_existed_before(
        &self,
        resolved_repository: Option<&Repository>,
        namespace: &Namespace,
        reference: &Reference,
        pointing_tags: &[LinkKind],
    ) -> bool {
        match self
            .prior_link_if_replicated(resolved_repository, namespace, reference)
            .await
        {
            None => false,
            Some(Err(Error::NotFound)) => !pointing_tags.is_empty(),
            Some(_) => true,
        }
    }

    /// Builds the link plan for a delete: a digest delete reads the manifest to
    /// cascade its child links, a tag delete drops only the tag link. The
    /// planner's LWW gate rejects the plan when a pointing tag was re-pointed
    /// locally after a replicated delete was authored.
    async fn plan_manifest_delete_ops(
        &self,
        reference: &Reference,
        pointing_tags: &[LinkKind],
    ) -> Result<Vec<LinkOperation>, Error> {
        let Reference::Digest(digest) = reference else {
            return Ok(link_plan::delete(reference, None, &[]));
        };

        let manifest = read_manifest(&self.blob_store, digest).await?;
        Ok(link_plan::delete(
            reference,
            manifest.as_ref(),
            pointing_tags,
        ))
    }

    /// Commits the delete, reporting whether the reference counted as present
    /// beforehand (the replication-dispatch gate). A concurrently pushed tag
    /// appends its own newer entry and wins resolution by timestamp regardless
    /// of interleaving; `source_ts` stamps a replicated delete with the
    /// author's clock so the LWW gate resolves it like any entry.
    async fn commit_manifest_delete(
        &self,
        resolved_repository: Option<&Repository>,
        namespace: &Namespace,
        reference: &Reference,
        source_ts: Option<DateTime<Utc>>,
    ) -> Result<bool, Error> {
        let pointing_tags = match reference {
            Reference::Digest(digest) => {
                self.metadata_store
                    .find_tags_pointing_at(namespace, digest)
                    .await?
            }
            Reference::Tag(_) => Vec::new(),
        };
        let existed_before = self
            .manifest_delete_existed_before(
                resolved_repository,
                namespace,
                reference,
                &pointing_tags,
            )
            .await;
        let ops = self
            .plan_manifest_delete_ops(reference, &pointing_tags)
            .await?;
        // The bytes are the collector's to reclaim once every reference is
        // stale; both delete endpoints answer `202 Accepted` regardless.
        match reference {
            Reference::Digest(_) => {
                self.metadata_store
                    .delete_manifest(namespace, &ops, source_ts)
                    .await?;
            }
            Reference::Tag(_) => {
                self.metadata_store
                    .delete_links(namespace, &ops, source_ts)
                    .await?;
            }
        }
        Ok(existed_before)
    }

    /// Short-circuits a manifest GET into a presigned redirect from the link
    /// metadata alone, without reading the manifest blob. `None` when the link
    /// records no `media_type` or the blob store produces no URL, so the caller
    /// falls through to the body-loading path.
    async fn try_redirect_via_link(
        &self,
        namespace: &Namespace,
        reference: &Reference,
        client: &str,
    ) -> Option<GetManifestResponse> {
        let blob_link = LinkKind::from_reference(reference);
        // Read without stamping: this probe abandons the redirect on a backend
        // that presigns nothing, and an abandoned probe is not a pull.
        let link = self
            .metadata_store
            .read_link(namespace, &blob_link)
            .await
            .ok()?;
        let media_type = link.media_type?;
        let presigned_url = self
            .blob_store
            .presigned_url(&link.target, Some(media_type.as_ref()))
            .await
            .ok()??;
        self.record_manifest_pull(namespace, &blob_link, client)
            .await;

        Some(GetManifestResponse::Redirect {
            headers: server::manifest_redirect_headers(
                &presigned_url,
                &link.target,
                Some(&media_type),
            )
            .ok()?,
            digest: link.target,
        })
    }

    /// Resolves a manifest GET to a presigned redirect or the manifest body,
    /// then emits `manifest.pull` for the served digest. The redirect fast-path
    /// needs the caller's consent (a client opts out with
    /// `X-Angos-No-Redirect`) and an authoritative target, so a mutable tag on
    /// a pull-through cache falls through to `get_manifest` to refresh.
    #[instrument(skip(self, request))]
    pub async fn resolve_get_manifest(
        &self,
        actor: Option<EventActor>,
        request: GetManifestRequest,
        allow_redirect: bool,
    ) -> Result<Response<ResponseBody>, Error> {
        let client = actor.as_ref().map_or("anonymous", EventActor::audit_name);
        let repository = self.get_repository_for_namespace(&request.namespace).ok();
        let repository_name = repository_name(repository);
        let event_reference = request.reference.clone();

        let response = self
            .resolve_get_manifest_response(
                repository,
                &request.namespace,
                request.reference,
                &request.accepted_types,
                allow_redirect,
                client,
            )
            .await?;

        let event = Event::pull_manifest(
            &request.namespace,
            &repository_name,
            response.digest(),
            &event_reference,
            actor.as_ref(),
        );
        self.dispatch_events(&[event]).await?;

        Ok(match response {
            GetManifestResponse::Redirect { headers, .. } => build_response(
                StatusCode::TEMPORARY_REDIRECT,
                headers,
                ResponseBody::empty(),
            )?,
            GetManifestResponse::Body {
                content, headers, ..
            } => build_response(StatusCode::OK, headers, ResponseBody::fixed(content))?,
        })
    }

    async fn resolve_get_manifest_response(
        &self,
        repository: Option<&Repository>,
        namespace: &Namespace,
        reference: Reference,
        mime_types: &[MediaRange],
        allow_redirect: bool,
        client: &str,
    ) -> Result<GetManifestResponse, Error> {
        let is_tag_immutable = self.is_reference_immutable(repository, &reference);
        let redirect_is_authoritative = !repository.is_some_and(Repository::is_pull_through)
            || matches!(reference, Reference::Digest(_))
            || is_tag_immutable;

        if allow_redirect
            && self.enable_manifest_redirect
            && redirect_is_authoritative
            && let Some(resp) = self
                .try_redirect_via_link(namespace, &reference, client)
                .await
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
                client,
            )
            .await?;

        Ok(GetManifestResponse::Body {
            headers: server::manifest_headers(
                manifest.media_type.as_ref(),
                &manifest.digest,
                manifest.content.len() as u64,
            )?,
            digest: manifest.digest,
            content: manifest.content,
        })
    }

    /// Advisory last-writer-wins fast-fail for a replication-originated tag
    /// write, saving the manifest blob write when the local tag already
    /// supersedes the incoming `source_ts`; the authoritative gate in the link
    /// planner applies the same [`LinkMetadata::supersedes`] rule. Skipped
    /// without a `source_ts` and for digest references, and the read bypasses
    /// the link cache and fails closed on errors other than `NotFound`, so
    /// stale state never lets an older write win.
    /// Refuse a push that would move an immutable `tag` to different content.
    ///
    /// Immutability is about overwrites: a tag that does not exist yet has
    /// nothing to protect, and a re-push of the digest it already holds
    /// changes nothing. Like the last-writer-wins gate below, the read bypasses
    /// the link cache and fails closed, so a stale entry can neither refuse a
    /// push that no longer conflicts nor admit one that does.
    async fn refuse_immutable_overwrite(
        &self,
        repository: Option<&Repository>,
        namespace: &Namespace,
        tag: &Tag,
        incoming_digest: &Digest,
    ) -> Result<(), Error> {
        if !self.is_tag_immutable(repository, tag) {
            return Ok(());
        }

        let held = match self
            .metadata_store
            .read_link_reference(namespace, &LinkKind::Tag(tag.clone()))
            .await
        {
            Ok(metadata) => metadata.target,
            Err(Error::NotFound) => return Ok(()),
            Err(err) => return Err(err),
        };
        if held == *incoming_digest {
            return Ok(());
        }

        Err(Error::Conflict(format!(
            "Tag '{tag}' is immutable and cannot be overwritten"
        )))
    }

    async fn check_lww_not_superseded(
        &self,
        namespace: &Namespace,
        reference: &Reference,
        source_ts: Option<DateTime<Utc>>,
        incoming_digest: &Digest,
    ) -> Result<(), Error> {
        let Some(source_ts) = source_ts else {
            return Ok(());
        };
        let Some(tag) = reference.as_tag() else {
            return Ok(());
        };

        let metadata = match self
            .metadata_store
            .read_link_reference(namespace, &LinkKind::Tag(tag.clone()))
            .await
        {
            Ok(metadata) => metadata,
            Err(Error::NotFound) => return Ok(()),
            Err(err) => return Err(err),
        };
        if let Some(created_at) = metadata.supersedes(source_ts, Some(incoming_digest)) {
            return Err(Error::ReplicationSuperseded(format!(
                "local tag '{tag}' (created {created_at}) is newer than the replicated source ({source_ts})"
            )));
        }

        Ok(())
    }

    /// The prior local link for `reference`, read only when an event-enqueuing
    /// downstream matches `namespace` so the replication-off path pays no extra
    /// read. Read errors other than `NotFound` are surfaced rather than
    /// collapsed to "absent", and the read bypasses the link cache, so a hiccup
    /// or stale cache never suppresses a real change.
    async fn prior_link_if_replicated(
        &self,
        repository: Option<&Repository>,
        namespace: &Namespace,
        reference: &Reference,
    ) -> Option<Result<LinkMetadata, Error>> {
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

    /// Reads the body stream and stores the manifest. `tags` carries the
    /// pre-validated `?tag=` query values, which apply only to a by-digest
    /// push.
    #[instrument(
        skip(self, body_stream, request),
        fields(namespace = %request.namespace, reference = %request.reference)
    )]
    pub async fn accept_put_manifest<S>(
        &self,
        actor: Option<EventActor>,
        request: PutManifestRequest,
        body_stream: S,
    ) -> Result<Response<ResponseBody>, Error>
    where
        S: AsyncRead + Unpin + Send,
    {
        let PutManifestRequest {
            namespace,
            reference,
            content_type,
            tags,
            source_ts,
        } = request;
        let resolved_repository = self.resolver.resolve(&namespace);

        let created_tags: Vec<Tag> = match &reference {
            Reference::Digest(_) => tags,
            Reference::Tag(_) => Vec::new(),
        };

        let request_body =
            read_limited_manifest_body(body_stream, self.max_manifest_size_bytes).await?;

        // Hashed up front: the intent events fired before the store carry the
        // content digest, the LWW tie-break compares it on equal timestamps,
        // and the immutability check below needs it to tell an overwrite from
        // a re-push of what the tag already holds.
        let digest = Digest::sha256_of_bytes(&request_body);

        // A by-tag push writes the path tag; a by-digest push writes only the
        // `?tag=` params. Checked before the events, so a refused push emits
        // nothing.
        let written_tags: &[Tag] = match &reference {
            Reference::Tag(tag) => slice::from_ref(tag),
            Reference::Digest(_) => &created_tags,
        };
        for tag in written_tags {
            self.refuse_immutable_overwrite(resolved_repository, &namespace, tag, &digest)
                .await?;
        }

        let repository = resolved_repository
            .map(|r| r.name.to_string())
            .unwrap_or_default();

        // Intent-first emission: a performed write can never go unnotified.
        let events = Event::put_manifest(
            &namespace,
            &repository,
            &digest,
            &reference,
            &created_tags,
            actor.as_ref(),
        );
        self.dispatch_events(&events).await?;

        self.check_lww_not_superseded(&namespace, &reference, source_ts, &digest)
            .await?;

        let reference_policy = if self.validate_manifest_references {
            ReferencePolicy::Strict
        } else {
            ReferencePolicy::Permissive
        };
        let response = self
            .store_manifest(
                &StoreManifest {
                    namespace: &namespace,
                    reference: &reference,
                    content_type: content_type.as_ref(),
                    created_tags: &created_tags,
                    reference_policy,
                    created_at: source_ts,
                },
                &request_body,
            )
            .await?;

        // No-op suppression: re-dispatching a converged replay would keep a
        // mesh cycle alive, so only a write that changed local state (per
        // the commit) is replicated. Webhook events fire unconditionally.
        if response.changed {
            self.replicate_manifest_push(
                resolved_repository,
                &namespace,
                &reference,
                &created_tags,
                &response.digest,
            )
            .await;
            if response.scan_subject && resolved_repository.is_some_and(|r| r.scan) {
                self.dispatch_scan(&namespace, &response.digest).await;
            }
            if resolved_repository.is_some_and(|r| r.index) {
                for layer in &response.layers {
                    self.dispatch_index(&namespace, layer).await;
                }
            }
        }

        Ok(build_response(
            StatusCode::CREATED,
            response.headers,
            ResponseBody::empty(),
        )?)
    }

    /// Fire-and-forget enqueue of the scan job for an image that just landed;
    /// a failure is logged and counted but never fails the client's write.
    async fn dispatch_scan(&self, namespace: &Namespace, digest: &Digest) {
        let payload = scan::ScanImagePayload {
            namespace: namespace.clone(),
            digest: digest.clone(),
            force: false,
        };
        let outcome = match scan::build_envelope(&payload) {
            Ok(envelope) => self
                .job_queue
                .enqueue(envelope)
                .await
                .map_err(|e| e.to_string()),
            Err(e) => Err(e.to_string()),
        };
        if let Err(error) = outcome {
            warn!("Failed to dispatch scan job for {namespace}@{digest}: {error}");
            metrics_provider()
                .job_queue_enqueue_failures_total
                .with_label_values(&[Queue::Scan.as_str()])
                .inc();
        }
    }

    /// Replicates a push for the path tag plus each `?tag=` created tag, so a
    /// by-digest push with tag params converges identically on every replica.
    async fn replicate_manifest_push(
        &self,
        repository: Option<&Repository>,
        namespace: &Namespace,
        reference: &Reference,
        created_tags: &[Tag],
        digest: &Digest,
    ) {
        let tags = once(reference.as_tag()).chain(created_tags.iter().map(Some));
        for tag in tags {
            self.dispatch_replication(
                repository,
                namespace,
                DispatchTarget::Push { tag, digest },
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
        target: DispatchTarget<'_>,
        source_ts: Option<DateTime<Utc>>,
    ) {
        let Some(repository) = repository else {
            return;
        };
        self.dispatch_replication_to(repository.replication.iter(), namespace, target, source_ts)
            .await;
    }

    /// [`Registry::dispatch_replication`] over a caller-selected downstream set,
    /// for dispatches that must not fan out to every downstream (a retention
    /// delete targets only `prune = true` mirrors, so additive downstreams never
    /// lose content to upstream retention).
    async fn dispatch_replication_to<'a>(
        &self,
        downstreams: impl Iterator<Item = &'a ReplicationDownstream>,
        namespace: &Namespace,
        target: DispatchTarget<'_>,
        source_ts: Option<DateTime<Utc>>,
    ) {
        // Receiver-side last-writer-wins timestamp: authoritative for a DELETE;
        // a PUSH re-derives it at execute time, so a coalesced push never goes
        // stale. An inbound replicated delete passes its author timestamp so it
        // propagates verbatim: re-stamping `now()` would let the bounced delete
        // outrank (and destroy) a recreate that landed in between.
        let source_ts = source_ts.unwrap_or_else(Utc::now);
        let (is_push, tag, digest, subject) = match target {
            DispatchTarget::Push { tag, digest } => (true, tag, Some(digest), None),
            DispatchTarget::TagDelete { tag } => (false, Some(tag), None, None),
            DispatchTarget::DigestDelete { digest, subject } => {
                (false, None, Some(digest), subject)
            }
        };

        // The per-downstream enqueues run concurrently: each one is an index
        // GET plus a CAS transaction, and this awaits inside the client's
        // PUT/DELETE response path, so serial fan-out adds tail latency.
        let dispatches = downstreams
            .filter(|downstream| downstream.enqueues_for(namespace.as_ref()))
            .map(|downstream| {
                let job_target = ReplicationTarget {
                    downstream: downstream.name.clone(),
                    namespace: namespace.clone(),
                    tag: tag.cloned(),
                    digest: digest.cloned(),
                    source_ts: Some(source_ts),
                };
                let payload = if is_push {
                    ReplicationJob::Push { target: job_target }
                } else {
                    ReplicationJob::Delete {
                        target: job_target,
                        subject: subject.cloned(),
                    }
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
