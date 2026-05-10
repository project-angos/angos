mod parse;
mod response;

use futures_util::StreamExt;
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
        metadata_store::{MetadataStoreExt, link_kind::LinkKind},
        pagination::collect_all_pages,
    },
};

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
            let needs_upstream_pull = matches!(&reference, Reference::Tag(_))
                && !is_tag_immutable
                && !repository
                    .is_upstream_digest_match(accepted_types, namespace, &reference, &meta.digest)
                    .await?;

            if !needs_upstream_pull {
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
            let needs_upstream_pull = matches!(&reference, Reference::Tag(_))
                && !is_tag_immutable
                && !repository
                    .is_upstream_digest_match(
                        accepted_types,
                        namespace,
                        &reference,
                        &manifest.digest,
                    )
                    .await?;

            if !needs_upstream_pull {
                return Ok(manifest);
            }
        }

        let (media_type, digest, content) = repository
            .get_manifest(accepted_types, namespace, &reference)
            .await?;

        self.put_manifest(namespace, &reference, media_type.as_ref(), &content)
            .await?;

        Ok(ManifestBody {
            media_type,
            digest,
            content,
        })
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
        let manifest = serde_json::from_slice::<Manifest>(&content).map_err(|error| {
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
        let manifest = parse_and_validate_manifest(body, content_type)?;

        let digest = self.blob_store.create(body).await?;

        if let Reference::Digest(provided_digest) = reference
            && provided_digest != &digest
        {
            warn!("Provided digest does not match computed digest: {provided_digest} != {digest}");
            return Err(Error::ManifestInvalid(
                "Provided digest does not match computed digest".to_string(),
            ));
        }

        let mut tx = self.metadata_store.begin_transaction(namespace);

        let effective_media_type = content_type
            .cloned()
            .or_else(|| manifest.media_type.clone());

        tx.create_link(&LinkKind::Digest(digest.clone()), &digest)
            .with_optional_media_type(effective_media_type.as_deref())
            .add();

        if let Reference::Tag(tag) = reference {
            tx.create_link(&LinkKind::Tag(tag.clone()), &digest)
                .with_optional_media_type(effective_media_type.as_deref())
                .add();
        }

        if let Some(subject) = &manifest.subject {
            let referrer_link = LinkKind::Referrer(subject.digest.clone(), digest.clone());
            if let Some(descriptor) = manifest.to_descriptor(digest.clone(), body.len() as u64) {
                tx.create_link(&referrer_link, &digest)
                    .with_descriptor(descriptor)
                    .add();
            } else {
                tx.create_link(&referrer_link, &digest).add();
            }
        }

        if let Some(config) = manifest.config {
            tx.create_link(&LinkKind::Config(config.digest.clone()), &config.digest)
                .with_referrer(&digest)
                .add();
        }

        for layer in manifest.layers {
            tx.create_link(&LinkKind::Layer(layer.digest.clone()), &layer.digest)
                .with_referrer(&digest)
                .add();
        }

        for child in manifest.manifests {
            tx.create_link(
                &LinkKind::Manifest(digest.clone(), child.digest.clone()),
                &child.digest,
            )
            .with_referrer(&digest)
            .add();
        }

        tx.commit().await?;

        let subject = manifest.subject.map(|s| s.digest);

        Ok(PutManifestResponse {
            headers: put_manifest_headers(namespace, reference, &digest, subject.as_ref()),
            events: Vec::new(),
        })
    }

    async fn find_tags_pointing_at(
        &self,
        namespace: &Namespace,
        digest: &Digest,
    ) -> Result<Vec<LinkKind>, Error> {
        let all_tags = collect_all_pages(|marker| async move {
            self.metadata_store.list_tags(namespace, 100, marker).await
        })
        .await?;

        let matching = futures_util::stream::iter(all_tags)
            .map(|tag| async move {
                let result = self
                    .metadata_store
                    .read_link(namespace, &LinkKind::Tag(tag.clone()), false)
                    .await;
                (tag, result)
            })
            .buffer_unordered(20)
            .filter_map(|(tag, result)| async move {
                if let Ok(metadata) = result
                    && &metadata.target == digest
                {
                    Some(LinkKind::Tag(tag))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
            .await;

        Ok(matching)
    }

    #[instrument(skip(actor))]
    pub async fn delete_manifest(
        &self,
        actor: Option<EventActor>,
        namespace: &Namespace,
        reference: &Reference,
    ) -> Result<DeleteManifestResponse, Error> {
        let mut tx = self.metadata_store.begin_transaction(namespace);

        match reference {
            Reference::Tag(tag) => {
                tx.delete_link(&LinkKind::Tag(tag.clone()));
            }
            Reference::Digest(digest) => {
                tx.delete_link(&LinkKind::Digest(digest.clone()));

                for link in self.find_tags_pointing_at(namespace, digest).await? {
                    tx.delete_link(&link);
                }

                if let Ok(content) = self.blob_store.read(digest).await
                    && let Ok(manifest) = Manifest::from_slice(&content)
                {
                    if let Some(subject) = manifest.subject {
                        tx.delete_link(&LinkKind::Referrer(subject.digest, digest.clone()));
                    }

                    if let Some(config) = manifest.config {
                        tx.delete_link_with_referrer(&LinkKind::Config(config.digest), digest);
                    }

                    for layer in manifest.layers {
                        tx.delete_link_with_referrer(&LinkKind::Layer(layer.digest), digest);
                    }

                    for child in manifest.manifests {
                        tx.delete_link_with_referrer(
                            &LinkKind::Manifest(digest.clone(), child.digest),
                            digest,
                        );
                    }
                }
            }
        }

        tx.commit().await?;

        let repository = self.repository_name_for(namespace);
        let digest_str = match reference {
            Reference::Digest(d) => Some(d.to_string()),
            Reference::Tag(_) => None,
        };

        let mut events = Vec::new();
        events.push(
            Event::new(
                EventKind::ManifestDelete,
                namespace.to_string(),
                repository.clone(),
            )
            .digest(digest_str.clone())
            .reference(Some(reference.to_string()))
            .actor(actor.clone()),
        );

        if let Reference::Tag(tag) = reference {
            events.push(
                Event::new(EventKind::TagDelete, namespace.to_string(), repository)
                    .digest(digest_str)
                    .reference(Some(reference.to_string()))
                    .tag(Some(tag.clone()))
                    .actor(actor),
            );
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
        mut body_stream: S,
    ) -> Result<PutManifestResponse, Error>
    where
        S: AsyncRead + Unpin + Send,
    {
        let mut request_body = String::new();

        body_stream
            .read_to_string(&mut request_body)
            .await
            .map_err(|_| {
                Error::ManifestInvalid("Unable to retrieve manifest from client query".to_string())
            })?;
        let request_body = request_body.into_bytes();

        let mut response = self
            .put_manifest(namespace, &reference, Some(&mime_type), &request_body)
            .await?;

        let repository = self.repository_name_for(namespace);
        let digest_str = response.headers.get(DOCKER_CONTENT_DIGEST).cloned();

        response.events.push(
            Event::new(
                EventKind::ManifestPush,
                namespace.to_string(),
                repository.clone(),
            )
            .digest(digest_str.clone())
            .reference(Some(reference.to_string()))
            .actor(actor.clone()),
        );

        if let Reference::Tag(tag) = &reference {
            response.events.push(
                Event::new(EventKind::TagCreate, namespace.to_string(), repository)
                    .digest(digest_str)
                    .reference(Some(reference.to_string()))
                    .tag(Some(tag.clone()))
                    .actor(actor),
            );
        }

        Ok(response)
    }
}

#[cfg(test)]
mod tests;
