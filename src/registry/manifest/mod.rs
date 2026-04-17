use futures_util::StreamExt;
use hyper::{
    Response, StatusCode,
    header::{CONTENT_LENGTH, CONTENT_TYPE, LOCATION},
    http::response,
};
use tokio::io::{AsyncRead, AsyncReadExt};
use tracing::{error, instrument, warn};

use crate::{
    command::server::response_body::ResponseBody,
    oci::{Digest, Manifest, Namespace, Reference},
    registry::{
        DOCKER_CONTENT_DIGEST, Error, OCI_SUBJECT, Registry, Repository,
        metadata_store::{MetadataStoreExt, link_kind::LinkKind},
    },
};

fn manifest_response_builder(digest: &Digest, media_type: Option<&str>) -> response::Builder {
    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(DOCKER_CONTENT_DIGEST, digest.to_string());
    if let Some(media_type) = media_type {
        builder = builder.header(CONTENT_TYPE, media_type);
    }
    builder
}

pub struct GetManifestResponse {
    pub media_type: Option<String>,
    pub digest: Digest,
    pub content: Vec<u8>,
}

pub struct HeadManifestResponse {
    pub media_type: Option<String>,
    pub digest: Digest,
    pub size: usize,
}

pub struct PutManifestResponse {
    pub digest: Digest,
    pub subject: Option<Digest>,
}

pub struct ParsedManifestDigests {
    pub subject: Option<Digest>,
    pub config: Option<Digest>,
    pub layers: Vec<Digest>,
    pub manifests: Vec<Digest>,
}

fn validate_media_type_match(
    manifest: &Manifest,
    content_type: Option<&String>,
) -> Result<(), Error> {
    if content_type.is_some()
        && manifest.media_type.is_some()
        && manifest.media_type.as_ref() != content_type
    {
        warn!(
            "Manifest media type mismatch: {content_type:?} (expected) != {:?} (found)",
            manifest.media_type
        );
        return Err(Error::ManifestInvalid(
            "Expected manifest media type mismatch".to_string(),
        ));
    }
    Ok(())
}

pub fn parse_manifest_digests(
    body: &[u8],
    content_type: Option<&String>,
) -> Result<ParsedManifestDigests, Error> {
    let manifest: Manifest = serde_json::from_slice(body).map_err(|e| {
        warn!("Failed to deserialize manifest: {e}");
        Error::ManifestInvalid(format!("invalid manifest JSON: {e}"))
    })?;

    validate_media_type_match(&manifest, content_type)?;

    let subject = manifest.subject.map(|subject| subject.digest);

    let config = manifest.config.map(|config| config.digest);

    let layers = manifest
        .layers
        .into_iter()
        .map(|layer| layer.digest)
        .collect::<Vec<_>>();

    let manifests = manifest
        .manifests
        .into_iter()
        .map(|m| m.digest)
        .collect::<Vec<_>>();

    Ok(ParsedManifestDigests {
        subject,
        config,
        layers,
        manifests,
    })
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
            return local.map_err(|_| {
                error!("Failed to head local manifest: {namespace}:{reference}");
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

        let res = self
            .get_manifest(
                repository,
                accepted_types,
                namespace,
                reference,
                is_tag_immutable,
            )
            .await?;

        Ok(HeadManifestResponse {
            media_type: res.media_type,
            digest: res.digest,
            size: res.content.len(),
        })
    }

    async fn head_local_manifest(
        &self,
        namespace: &Namespace,
        reference: &Reference,
    ) -> Result<HeadManifestResponse, Error> {
        let blob_link = reference.clone().into();
        let link = self
            .metadata_store
            .read_link(namespace, &blob_link, self.update_pull_time)
            .await?;

        if let Some(media_type) = link.media_type {
            let size = self
                .blob_store
                .get_blob_size(&link.target)
                .await
                .map_err(|error| {
                    error!("Failed to get blob size: {error}");
                    Error::ManifestUnknown
                })?;

            return Ok(HeadManifestResponse {
                media_type: Some(media_type),
                digest: link.target,
                size: usize::try_from(size).unwrap_or(usize::MAX),
            });
        }

        // Backward compatibility: links created before media_type was stored require
        // a full blob read. Remove this fallback once all links have been re-pushed.
        let (mut reader, _) = self
            .blob_store
            .build_blob_reader(&link.target, None)
            .await
            .map_err(|error| {
                error!("Failed to build blob reader: {error}");
                Error::ManifestUnknown
            })?;

        let mut manifest_content = Vec::new();
        reader.read_to_end(&mut manifest_content).await?;

        let manifest = serde_json::from_slice::<Manifest>(&manifest_content)?;
        let size = manifest_content.len();

        Ok(HeadManifestResponse {
            media_type: manifest.media_type,
            digest: link.target,
            size,
        })
    }

    #[instrument(skip(repository))]
    pub async fn get_manifest(
        &self,
        repository: &Repository,
        accepted_types: &[String],
        namespace: &Namespace,
        reference: Reference,
        is_tag_immutable: bool,
    ) -> Result<GetManifestResponse, Error> {
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

        Ok(GetManifestResponse {
            media_type,
            digest,
            content,
        })
    }

    async fn get_local_manifest(
        &self,
        namespace: &Namespace,
        reference: &Reference,
    ) -> Result<GetManifestResponse, Error> {
        let blob_link = reference.clone().into();
        let link = self
            .metadata_store
            .read_link(namespace, &blob_link, self.update_pull_time)
            .await?;

        let content = self.blob_store.read_blob(&link.target).await?;
        let manifest = serde_json::from_slice::<Manifest>(&content).map_err(|error| {
            warn!("Failed to deserialize manifest: {error}");
            Error::ManifestInvalid("Failed to deserialize manifest".to_string())
        })?;

        Ok(GetManifestResponse {
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
        let manifest: Manifest = serde_json::from_slice(body).map_err(|e| {
            warn!("Failed to deserialize manifest: {e}");
            Error::ManifestInvalid(format!("invalid manifest JSON: {e}"))
        })?;

        validate_media_type_match(&manifest, content_type)?;

        let digest = self.blob_store.create_blob(body).await?;

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

        // Backward compatibility: fall back to create_link without media_type when
        // content type is unknown. Once all clients send Content-Type, simplify to
        // always use create_link_with_media_type.
        if let Some(ref mt) = effective_media_type {
            tx.create_link_with_media_type(&LinkKind::Digest(digest.clone()), &digest, mt);
        } else {
            tx.create_link(&LinkKind::Digest(digest.clone()), &digest);
        }

        if let Reference::Tag(tag) = reference {
            if let Some(ref mt) = effective_media_type {
                tx.create_link_with_media_type(&LinkKind::Tag(tag.clone()), &digest, mt);
            } else {
                tx.create_link(&LinkKind::Tag(tag.clone()), &digest);
            }
        }

        if let Some(subject) = &manifest.subject {
            let referrer_link = LinkKind::Referrer(subject.digest.clone(), digest.clone());
            if let Some(descriptor) =
                manifest.to_descriptor(None, digest.clone(), body.len() as u64)
            {
                tx.create_link_with_descriptor(&referrer_link, &digest, descriptor);
            } else {
                tx.create_link(&referrer_link, &digest);
            }
        }

        if let Some(config) = manifest.config {
            tx.create_link_with_referrer(
                &LinkKind::Config(config.digest.clone()),
                &config.digest,
                &digest,
            );
        }

        for layer in manifest.layers {
            tx.create_link_with_referrer(
                &LinkKind::Layer(layer.digest.clone()),
                &layer.digest,
                &digest,
            );
        }

        for child in manifest.manifests {
            tx.create_link_with_referrer(
                &LinkKind::Manifest(digest.clone(), child.digest.clone()),
                &child.digest,
                &digest,
            );
        }

        tx.commit().await?;

        let subject = manifest.subject.map(|s| s.digest.clone());

        Ok(PutManifestResponse { digest, subject })
    }

    #[instrument]
    pub async fn delete_manifest(
        &self,
        namespace: &Namespace,
        reference: &Reference,
    ) -> Result<(), Error> {
        let mut tx = self.metadata_store.begin_transaction(namespace);

        match reference {
            Reference::Tag(tag) => {
                tx.delete_link(&LinkKind::Tag(tag.clone()));
            }
            Reference::Digest(digest) => {
                tx.delete_link(&LinkKind::Digest(digest.clone()));

                // Collect all tags
                let mut all_tags = Vec::new();
                let mut marker = None;
                loop {
                    let (tags, next_marker) = self
                        .metadata_store
                        .list_tags(namespace, 100, marker)
                        .await?;
                    all_tags.extend(tags);
                    if next_marker.is_none() {
                        break;
                    }
                    marker = next_marker;
                }

                // Read all tag links concurrently to find ones pointing to this digest
                let matching_tags: Vec<_> = futures_util::stream::iter(all_tags)
                    .map(|tag| {
                        let tag_link = LinkKind::Tag(tag);
                        async {
                            let result = self
                                .metadata_store
                                .read_link(namespace, &tag_link, false)
                                .await;
                            (tag_link, result)
                        }
                    })
                    .buffer_unordered(20)
                    .filter_map(|(tag_link, result)| async {
                        if let Ok(metadata) = result
                            && &metadata.target == digest
                        {
                            Some(tag_link)
                        } else {
                            None
                        }
                    })
                    .collect()
                    .await;

                for tag_link in matching_tags {
                    tx.delete_link(&tag_link);
                }

                // Delete all links created during put_manifest
                if let Ok(content) = self.blob_store.read_blob(digest).await
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
        Ok(())
    }

    // API Handlers
    #[instrument(skip(self, is_tag_immutable))]
    pub async fn handle_head_manifest(
        &self,
        namespace: &Namespace,
        reference: Reference,
        mime_types: &[String],
        is_tag_immutable: bool,
    ) -> Result<Response<ResponseBody>, Error> {
        let repository = self.get_repository_for_namespace(namespace)?;

        let manifest = self
            .head_manifest(
                repository,
                mime_types,
                namespace,
                reference,
                is_tag_immutable,
            )
            .await?;

        let res = manifest_response_builder(&manifest.digest, manifest.media_type.as_deref())
            .header(CONTENT_LENGTH, manifest.size)
            .body(ResponseBody::empty())?;

        Ok(res)
    }

    #[instrument(skip(self, is_tag_immutable))]
    pub async fn handle_get_manifest(
        &self,
        namespace: &Namespace,
        reference: Reference,
        mime_types: &[String],
        is_tag_immutable: bool,
    ) -> Result<Response<ResponseBody>, Error> {
        let repository = self.get_repository_for_namespace(namespace)?;

        // The redirect fast-path serves the cached manifest link directly,
        // bypassing `get_manifest` and its upstream revalidation. That is
        // only safe when the cached target is authoritative:
        //   * the repository is not a pull-through cache (we own the data),
        //   * the reference is a digest (content-addressable, immutable),
        //   * or the tag has been declared immutable via configuration.
        // For mutable tags on a pull-through cache we must fall through to
        // `get_manifest`, which HEADs the upstream and refreshes the cache
        // when the tag has moved; otherwise clients would be pinned forever
        // to the digest captured on the first pull.
        let redirect_is_authoritative = !repository.is_pull_through()
            || matches!(reference, Reference::Digest(_))
            || is_tag_immutable;

        if self.enable_manifest_redirect
            && redirect_is_authoritative
            && let Ok(link) = {
                let blob_link: LinkKind = reference.clone().into();
                self.metadata_store
                    .read_link(namespace, &blob_link, self.update_pull_time)
                    .await
            }
            && let Some(media_type) = link.media_type
            && let Ok(Some(presigned_url)) = self
                .blob_store
                .get_blob_url(&link.target, Some(media_type.as_str()))
                .await
        {
            return Response::builder()
                .status(StatusCode::TEMPORARY_REDIRECT)
                .header(LOCATION, presigned_url)
                .header(DOCKER_CONTENT_DIGEST, link.target.to_string())
                .header(CONTENT_TYPE, media_type)
                .body(ResponseBody::empty())
                .map_err(Into::into);
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
        // lacks media_type), fall back to reading the full blob via get_manifest then
        // redirecting. Remove this block once all links have been re-pushed.
        if self.enable_manifest_redirect
            && let Ok(Some(presigned_url)) = self
                .blob_store
                .get_blob_url(&manifest.digest, manifest.media_type.as_deref())
                .await
        {
            let mut builder = Response::builder()
                .status(StatusCode::TEMPORARY_REDIRECT)
                .header(LOCATION, presigned_url)
                .header(DOCKER_CONTENT_DIGEST, manifest.digest.to_string());

            if let Some(content_type) = manifest.media_type {
                builder = builder.header(CONTENT_TYPE, content_type);
            }

            return builder.body(ResponseBody::empty()).map_err(Into::into);
        }

        let res = manifest_response_builder(&manifest.digest, manifest.media_type.as_deref())
            .body(ResponseBody::fixed(manifest.content))?;

        Ok(res)
    }

    #[instrument(skip(self, body_stream))]
    pub async fn handle_put_manifest<S>(
        &self,
        namespace: &Namespace,
        reference: Reference,
        mime_type: String,
        mut body_stream: S,
    ) -> Result<Response<ResponseBody>, Error>
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

        let location = format!("/v2/{namespace}/manifests/{reference}");

        let manifest = self
            .put_manifest(namespace, &reference, Some(&mime_type), &request_body)
            .await?;

        let res = match manifest.subject {
            Some(subject) => Response::builder()
                .status(StatusCode::CREATED)
                .header(LOCATION, location)
                .header(DOCKER_CONTENT_DIGEST, manifest.digest.to_string())
                .header(OCI_SUBJECT, subject.to_string())
                .body(ResponseBody::empty())?,
            None => Response::builder()
                .status(StatusCode::CREATED)
                .header(LOCATION, location)
                .header(DOCKER_CONTENT_DIGEST, manifest.digest.to_string())
                .body(ResponseBody::empty())?,
        };

        Ok(res)
    }

    #[instrument(skip(self))]
    pub async fn handle_delete_manifest(
        &self,
        namespace: &Namespace,
        reference: Reference,
    ) -> Result<Response<ResponseBody>, Error> {
        self.delete_manifest(namespace, &reference).await?;

        let res = Response::builder()
            .status(StatusCode::ACCEPTED)
            .body(ResponseBody::empty())?;

        Ok(res)
    }
}

#[cfg(test)]
mod tests;
