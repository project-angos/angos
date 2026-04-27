use std::collections::HashMap;

use futures_util::StreamExt;
use hyper::header::{CONTENT_LENGTH, CONTENT_TYPE, LOCATION};
use tokio::io::{AsyncRead, AsyncReadExt};
use tracing::{error, instrument, warn};

use crate::{
    event_webhook::event::{Event, EventActor, EventKind},
    oci::{Digest, Manifest, Namespace, Reference},
    registry::{
        DOCKER_CONTENT_DIGEST, Error, OCI_SUBJECT, Registry, Repository,
        metadata_store::{MetadataStoreExt, link_kind::LinkKind},
    },
};

pub(crate) struct ManifestMeta {
    pub media_type: Option<String>,
    pub digest: Digest,
    pub size: u64,
}

pub(crate) struct ManifestBody {
    pub media_type: Option<String>,
    pub digest: Digest,
    pub content: Vec<u8>,
}

pub enum GetManifestResponse {
    Redirect {
        headers: HashMap<&'static str, String>,
    },
    Body {
        headers: HashMap<&'static str, String>,
        content: Vec<u8>,
    },
}

pub struct HeadManifestResponse {
    pub headers: HashMap<&'static str, String>,
}

pub struct PutManifestResponse {
    pub headers: HashMap<&'static str, String>,
    pub events: Vec<Event>,
}

pub struct DeleteManifestResponse {
    pub events: Vec<Event>,
}

fn head_manifest_headers(meta: &ManifestMeta) -> HashMap<&'static str, String> {
    let mut headers = HashMap::from([
        (DOCKER_CONTENT_DIGEST, meta.digest.to_string()),
        (CONTENT_LENGTH.as_str(), meta.size.to_string()),
    ]);
    if let Some(media_type) = meta.media_type.clone() {
        headers.insert(CONTENT_TYPE.as_str(), media_type);
    }
    headers
}

fn get_manifest_body_headers(
    media_type: Option<&str>,
    digest: &Digest,
) -> HashMap<&'static str, String> {
    let mut headers = HashMap::from([(DOCKER_CONTENT_DIGEST, digest.to_string())]);
    if let Some(media_type) = media_type {
        headers.insert(CONTENT_TYPE.as_str(), media_type.to_string());
    }
    headers
}

fn get_manifest_redirect_headers(
    url: String,
    digest: &Digest,
    media_type: Option<String>,
) -> HashMap<&'static str, String> {
    let mut headers = HashMap::from([
        (LOCATION.as_str(), url),
        (DOCKER_CONTENT_DIGEST, digest.to_string()),
    ]);
    if let Some(media_type) = media_type {
        headers.insert(CONTENT_TYPE.as_str(), media_type);
    }
    headers
}

fn put_manifest_headers(
    namespace: &Namespace,
    reference: &Reference,
    digest: &Digest,
    subject: Option<&Digest>,
) -> HashMap<&'static str, String> {
    let mut headers = HashMap::from([
        (
            LOCATION.as_str(),
            format!("/v2/{namespace}/manifests/{reference}"),
        ),
        (DOCKER_CONTENT_DIGEST, digest.to_string()),
    ]);
    if let Some(subject) = subject {
        headers.insert(OCI_SUBJECT, subject.to_string());
    }
    headers
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
        let blob_link = reference.clone().into();
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

        let manifest = serde_json::from_slice::<Manifest>(&manifest_content)?;

        Ok(ManifestMeta {
            media_type: manifest.media_type,
            digest: link.target,
            size: manifest_content.len() as u64,
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
        let blob_link = reference.clone().into();
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
        let manifest: Manifest = serde_json::from_slice(body).map_err(|e| {
            warn!("Failed to deserialize manifest: {e}");
            Error::ManifestInvalid(format!("invalid manifest JSON: {e}"))
        })?;

        validate_media_type_match(&manifest, content_type)?;

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

        if let Some(ref mt) = effective_media_type {
            tx.create_link(&LinkKind::Digest(digest.clone()), &digest)
                .with_media_type(mt)
                .add();
        } else {
            tx.create_link(&LinkKind::Digest(digest.clone()), &digest)
                .add();
        }

        if let Reference::Tag(tag) = reference {
            if let Some(ref mt) = effective_media_type {
                tx.create_link(&LinkKind::Tag(tag.clone()), &digest)
                    .with_media_type(mt)
                    .add();
            } else {
                tx.create_link(&LinkKind::Tag(tag.clone()), &digest).add();
            }
        }

        if let Some(subject) = &manifest.subject {
            let referrer_link = LinkKind::Referrer(subject.digest.clone(), digest.clone());
            if let Some(descriptor) =
                manifest.to_descriptor(None, digest.clone(), body.len() as u64)
            {
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
            && let Ok(link) = {
                let blob_link: LinkKind = reference.clone().into();
                self.metadata_store
                    .read_link(namespace, &blob_link, self.update_pull_time)
                    .await
            }
            && let Some(media_type) = link.media_type
            && let Some(presigned) = &self.presigned_blob_store
            && let Ok(Some(presigned_url)) =
                presigned.url(&link.target, Some(media_type.as_str())).await
        {
            return Ok(GetManifestResponse::Redirect {
                headers: get_manifest_redirect_headers(
                    presigned_url,
                    &link.target,
                    Some(media_type),
                ),
            });
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
