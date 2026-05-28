pub mod link_plan;
mod parse;
mod response;

pub use parse::parse_manifest_digests;
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
        DOCKER_CONTENT_DIGEST, Error, Registry, Repository, blob_ownership::BlobOwnership,
        blob_store::Error as BlobStoreError, metadata_store::link_kind::LinkKind,
    },
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
        namespace: &Namespace,
        reference: &Reference,
    ) -> Result<DeleteManifestResponse, Error> {
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
        let presigned = self.presigned_blob_store.as_ref()?;
        let presigned_url = presigned
            .url(&link.target, Some(media_type.as_str()))
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
            && let Some(presigned) = &self.presigned_blob_store
            && let Ok(Some(presigned_url)) = presigned
                .url(&manifest.digest, manifest.media_type.as_deref())
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

    /// Reads the body stream, calls `put_manifest`, and returns the domain response.
    #[instrument(skip(self, body_stream, actor))]
    pub async fn accept_put_manifest<S>(
        &self,
        actor: Option<EventActor>,
        namespace: &Namespace,
        reference: Reference,
        mime_type: String,
        body_stream: S,
    ) -> Result<PutManifestResponse, Error>
    where
        S: AsyncRead + Unpin + Send,
    {
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
                digest_str,
                &reference,
                tag,
                actor,
            ));
        }

        Ok(response)
    }
}

#[cfg(test)]
mod tests;
