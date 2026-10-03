use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap},
    pin::pin,
};

use futures_util::{
    future::{BoxFuture, ready},
    stream::{self, Stream, StreamExt, TryStreamExt},
};
use tracing::{instrument, warn};

use angos_docker_extension_service::{Catalog, CatalogRequest};
use angos_oci::{
    Content, Descriptor, Digest, Manifest, MediaType, Namespace, client,
    request::{GetReferrersRequest, ListTagsRequest},
    response::TagsListResponse,
};
use angos_oci_service::{Referrers, Tags};
use angos_storage::Page;

use crate::registry::{
    Error, Registry, Repository,
    metadata_store::{LinkKind, MetadataStore},
};

/// Whether the catalog names `namespace` as a repository: it holds at least
/// one manifest revision, or one tag that resolves live. A namespace holding
/// nothing but tombstones or in-progress uploads is not a repository.
pub fn holds_manifest_content(
    metadata_store: &MetadataStore,
    namespace: Namespace,
) -> BoxFuture<'_, Result<bool, Error>> {
    Box::pin(async move {
        if metadata_store.any_revision(&namespace).await? {
            return Ok(true);
        }
        let mut tags = pin!(metadata_store.stream_live_tags(&namespace, None));
        Ok(tags.try_next().await?.is_some())
    })
}

/// Whether `referrer` passes a listing's `artifactType` filter.
fn matches_filter(referrer: &Descriptor, artifact_type: Option<&MediaType>) -> bool {
    artifact_type.is_none_or(|filter| referrer.artifact_type.as_ref() == Some(filter))
}

/// The local and described referrer candidates in one digest order, a digest
/// both hold taken as local: a local candidate (`None`) resolves from its
/// record, a described one arrives resolved.
fn merge_candidates<'a>(
    local: impl Stream<Item = Result<Digest, Error>> + Send + 'a,
    described: BTreeMap<Digest, Descriptor>,
) -> impl Stream<Item = Result<(Digest, Option<Descriptor>), Error>> + Send + 'a {
    stream::try_unfold(
        (
            Box::pin(local),
            None::<Digest>,
            described.into_iter().peekable(),
        ),
        |(mut local, mut head, mut described)| async move {
            if head.is_none() {
                head = local.try_next().await?;
            }
            let order = match (&head, described.peek()) {
                (Some(own), Some((other, _))) => other.cmp(own),
                (Some(_), None) => Ordering::Greater,
                (None, Some(_)) => Ordering::Less,
                (None, None) => return Ok(None),
            };
            let next = match order {
                Ordering::Less => described
                    .next()
                    .map(|(other, descriptor)| (other, Some(descriptor))),
                Ordering::Equal => {
                    described.next();
                    head.take().map(|own| (own, None))
                }
                Ordering::Greater => head.take().map(|own| (own, None)),
            };
            Ok(next.map(|candidate| (candidate, (local, head, described))))
        },
    )
}

/// Fan-out for resolving referrer candidates, each an independent manifest read.
pub const REFERRER_RESOLVE_CONCURRENCY: usize = 10;

/// Default page size of every listing that omits `n`, and of the referrers,
/// which take none; the `Link` a paged listing builds echoes it.
pub const DEFAULT_PAGE_SIZE: u16 = 150;

impl Registry {
    /// One page of namespaces the caller may list, advertising the next through
    /// the `Link` header while the listing is not exhausted. `visibility` drops
    /// the entries the caller's access policy hides.
    pub async fn handle_list_catalog(
        &self,
        request: CatalogRequest,
        visibility: &(dyn Fn(&Namespace) -> bool + Sync),
    ) -> Result<Catalog, Error> {
        let n = request.n.unwrap_or(DEFAULT_PAGE_SIZE);
        // The walk drops what the caller may not see before it probes a name,
        // so the page holds `n` visible entries whenever that many remain and
        // its cursor is the last one served.
        let page = self
            .metadata_store
            .list_namespaces(n, request.last, visibility, |namespace| {
                holds_manifest_content(&self.metadata_store, namespace)
            })
            .await?;
        let next = page
            .next_token
            .as_ref()
            .map(|last| format!("/v2/_catalog?n={n}&last={last}"));

        Ok(Catalog {
            repositories: page.items,
            next,
        })
    }

    /// One page of a namespace's tags, advertising the next through the `Link`
    /// header while the listing is not exhausted.
    pub async fn handle_list_tags(&self, request: ListTagsRequest) -> Result<Tags, Error> {
        let n = request.n.unwrap_or(DEFAULT_PAGE_SIZE);
        let page = self
            .metadata_store
            .list_tags(&request.namespace, n, request.last)
            .await?;
        // A namespace holding nothing is unknown, not empty: a client probing
        // existence here must be able to tell the two apart. A repository whose
        // tags were all deleted still holds revisions, so it stays a `200`.
        if page.items.is_empty()
            && !holds_manifest_content(&self.metadata_store, request.namespace.clone()).await?
        {
            return Err(Error::NameUnknown);
        }

        let link = page.next_token.as_ref().map(|last| {
            client::tags_list_path(
                "",
                &ListTagsRequest {
                    namespace: request.namespace.clone(),
                    n: Some(n),
                    last: Some(last.clone()),
                },
            )
        });

        let list = TagsListResponse {
            name: Some(request.namespace.clone()),
            tags: page.items.iter().map(ToString::to_string).collect(),
        };

        Ok(Tags { list, next: link })
    }

    /// One page of a subject's referrers, served as the OCI image index that
    /// carries them.
    #[instrument(skip(request))]
    pub async fn handle_get_referrers(
        &self,
        mut request: GetReferrersRequest,
    ) -> Result<Referrers, Error> {
        let upstream = self
            .get_repository_for_namespace(&request.namespace)
            .ok()
            .filter(|repository| repository.is_pull_through());
        let page = self.list_referrers(upstream, &request).await?;
        let filtered = request.artifact_type.is_some();

        // The request carries its filter into the next page: dropping it would
        // answer a different question halfway through a client's walk.
        let next = page.next_token.map(|last| {
            request.last = Some(last);
            client::referrers_path("", &request)
        });

        let index = Manifest::oci_index(page.items);

        Ok(Referrers {
            index,
            filtered,
            next,
        })
    }

    /// One page of the request's subject referrers as a sorted descriptor list,
    /// where `upstream` is the pull-through repository whose referrers join the
    /// local ones. The page holds a full page of matches whenever that many
    /// remain: candidates resolve until it is filled, so a filter dropping a
    /// long stretch costs reads rather than a short page.
    ///
    /// The local records are read from the cursor and only as far as the page
    /// takes. An upstream's listing has no cursor to resume at, so every page
    /// re-enumerates it in full: walking a pull-through subject costs one
    /// upstream enumeration per page.
    #[instrument(skip(upstream))]
    pub async fn list_referrers(
        &self,
        upstream: Option<&Repository>,
        request: &GetReferrersRequest,
    ) -> Result<Page<Descriptor>, Error> {
        let (namespace, digest) = (&request.namespace, &request.digest);
        let artifact_type = request.artifact_type.as_ref();
        let last = request.last.as_deref();
        // A digest sorts as its string form, so `last` cuts every source alike.
        let above =
            move |candidate: &Digest| last.is_none_or(|last| candidate.to_string().as_str() > last);

        // Referrers no local index knows, both arriving already resolved: an
        // upstream's, and any a pre-API client left under the fallback tag.
        let mut described = self
            .upstream_referrers(upstream, namespace, digest, artifact_type)
            .await;
        for referrer in self.fallback_tag_referrers(namespace, digest).await {
            if matches_filter(&referrer, artifact_type) {
                described.entry(referrer.digest.clone()).or_insert(referrer);
            }
        }
        let described: BTreeMap<Digest, Descriptor> = described
            .into_iter()
            .filter(|(candidate, _)| above(candidate))
            .collect();

        // The records resume past a cursor naming a digest; any other cursor
        // lists them from the start, the filter cutting what it covers.
        let after = last.and_then(|last| last.parse::<Digest>().ok());
        let local = self
            .metadata_store
            .stream_referrer_digests(namespace, digest, after.as_ref())
            .try_filter(move |candidate| ready(above(candidate)));
        let mut candidates = pin!(merge_candidates(local, described).peekable());

        let page_size = usize::from(DEFAULT_PAGE_SIZE);
        let mut referrers: Vec<Descriptor> = Vec::new();
        let mut last_consumed: Option<Digest> = None;
        // A local candidate's artifact type is only known once its descriptor
        // is resolved, so the page fills as they resolve rather than being cut
        // over the candidates first, which would answer short while matches
        // remain. Resolution is batched a page at a time to bound the reads an
        // unmatched stretch costs.
        let more = loop {
            let chunk: Vec<(Digest, Option<Descriptor>)> =
                candidates.as_mut().take(page_size).try_collect().await?;
            if chunk.is_empty() {
                break false;
            }
            let resolved: Vec<(Digest, Option<Descriptor>)> = stream::iter(chunk)
                .map(async |(candidate, described)| {
                    let descriptor = match described {
                        Some(descriptor) => Some(descriptor),
                        None => {
                            self.resolve_referrer_descriptor(
                                namespace,
                                digest,
                                candidate.clone(),
                                artifact_type,
                            )
                            .await
                        }
                    };
                    (candidate, descriptor)
                })
                .buffered(REFERRER_RESOLVE_CONCURRENCY)
                .collect()
                .await;
            let mut resolved = resolved.into_iter();
            for (candidate, descriptor) in resolved.by_ref() {
                last_consumed = Some(candidate);
                referrers.extend(descriptor);
                if referrers.len() == page_size {
                    break;
                }
            }
            if referrers.len() == page_size {
                break resolved.next().is_some() || candidates.as_mut().peek().await.is_some();
            }
        };

        // The cursor names the last candidate consumed rather than the last
        // served, so a stretch the filter dropped is not walked a second time.
        let next_token = last_consumed
            .filter(|_| more)
            .map(|candidate| candidate.to_string());

        Ok(Page {
            items: referrers,
            next_token,
        })
    }

    /// The referrers a pre-API client recorded under the fallback tag
    /// (`sha256-<hex>`), which the spec's "Enabling the Referrers API"
    /// procedure has a registry fold into the listing: a repository imported
    /// from such a registry would otherwise lose them.
    async fn fallback_tag_referrers(
        &self,
        namespace: &Namespace,
        subject: &Digest,
    ) -> Vec<Descriptor> {
        let tag = subject.referrers_fallback_tag();
        let Ok(link) = self
            .metadata_store
            .read_link(namespace, &LinkKind::Tag(tag))
            .await
        else {
            return Vec::new();
        };
        let Ok(body) = self.blob_store.read(&link.target).await else {
            return Vec::new();
        };

        match Manifest::from_slice(&body).map(|index| index.content) {
            Ok(Content::Index { manifests }) => manifests,
            _ => Vec::new(),
        }
    }

    /// The referrers `upstream` holds for `digest`, filtered like the local
    /// ones: nothing fills a referrer index on its own, so a pull-through
    /// namespace listing only what it cached would answer an uncached subject
    /// with nothing. An upstream that cannot be reached yields none, so a
    /// mirror still lists what it holds when its upstream is down.
    async fn upstream_referrers(
        &self,
        upstream: Option<&Repository>,
        namespace: &Namespace,
        digest: &Digest,
        artifact_type: Option<&MediaType>,
    ) -> HashMap<Digest, Descriptor> {
        let Some(repository) = upstream else {
            return HashMap::new();
        };

        match repository.list_referrers(namespace, digest).await {
            Ok(referrers) => referrers
                .into_iter()
                .filter(|referrer| matches_filter(referrer, artifact_type))
                .map(|referrer| (referrer.digest.clone(), referrer))
                .collect(),
            Err(error) => {
                warn!("Upstream referrer listing failed for {namespace}@{digest}: {error}");
                HashMap::new()
            }
        }
    }

    /// One referrer entry as an OCI [`Descriptor`]: the cached link descriptor
    /// when that answers the `artifact_type` filter, else the manifest read
    /// through the blob store, where manifest bodies live.
    pub async fn resolve_referrer_descriptor(
        &self,
        namespace: &Namespace,
        subject_digest: &Digest,
        manifest_digest: Digest,
        artifact_type: Option<&MediaType>,
    ) -> Option<Descriptor> {
        let referrer_link = LinkKind::Referrer {
            subject: subject_digest.clone(),
            referrer: manifest_digest.clone(),
        };

        if let Ok(metadata) = self
            .metadata_store
            .read_link(namespace, &referrer_link)
            .await
            && let Some(desc) = metadata.descriptor
        {
            if matches_filter(&desc, artifact_type) {
                return Some(desc);
            }
            // A cached descriptor carrying its own `artifactType` has answered
            // the filter; one carrying none falls through to the manifest read,
            // where the config `mediaType` fallback can still match.
            if desc.artifact_type.is_some() {
                return None;
            }
        }

        let data = match self.blob_store.read(&manifest_digest).await {
            Ok(data) => data,
            Err(e) => {
                warn!("Failed to read referrer manifest {manifest_digest}: {e}");
                return None;
            }
        };
        let manifest_len = data.len();
        match Manifest::from_slice(&data) {
            Ok(mut manifest) => {
                if !manifest.artifact_type_matches(artifact_type) {
                    return None;
                }
                Some(manifest.take_descriptor(manifest_digest, manifest_len as u64))
            }
            Err(e) => {
                warn!("Failed to parse referrer manifest {manifest_digest}: {e}");
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use futures_util::future::try_join_all;
    use http::{Response, header::LINK};
    use serde_json::json;
    use url::form_urlencoded;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use angos_docker_extension_service::CatalogRequest;
    use angos_oci::{
        Descriptor, Digest, Manifest, MediaType, Namespace, OCI_INDEX_MEDIA_TYPE, Reference, Tag,
        client::next_page_target,
        request::{GetReferrersRequest, ListTagsRequest},
    };
    use angos_storage::{
        Error as StorageError,
        test_util::{HookedStore, StoreHook, StoreOp},
    };
    use angos_transport::ResponseBody;

    use crate::{
        registry::{
            Error,
            content_discovery::{DEFAULT_PAGE_SIZE, Repository},
            keys::NamespaceKeys,
            manifest::DEFAULT_MAX_MANIFEST_SIZE_BYTES,
            metadata_store::{LIST_PAGE, LinkKind, MetadataStore},
            repository::Config,
            test_utils::{
                FSRegistryTestCase, RegistryTestCase, create_link, create_test_blob,
                create_test_registry, for_each_backend, media_type, metadata_store_over,
                put_blob_direct, referrers_request, response_json, seed_links, upload_blob,
            },
        },
        test_fixtures::client::test_client_config,
    };
    /// The repository names a catalog response served.
    async fn catalog(response: Response<ResponseBody>) -> Vec<String> {
        json_strings(response, "repositories").await
    }

    /// The tag names a tags response served.
    async fn tags(response: Response<ResponseBody>) -> Vec<String> {
        json_strings(response, "tags").await
    }

    async fn json_strings(response: Response<ResponseBody>, field: &str) -> Vec<String> {
        response_json(response).await[field]
            .as_array()
            .expect("the listing field must be an array")
            .iter()
            .map(|value| value.as_str().expect("entries are strings").to_string())
            .collect()
    }

    /// The `field` of every object in a response's `array`.
    async fn json_strings_at(
        response: Response<ResponseBody>,
        array: &str,
        field: &str,
    ) -> Vec<String> {
        response_json(response).await[array]
            .as_array()
            .expect("the listing field must be an array")
            .iter()
            .map(|entry| {
                entry[field]
                    .as_str()
                    .expect("entries carry the field")
                    .to_string()
            })
            .collect()
    }

    /// The `last` cursor a client would follow out of a `Link` header, or
    /// `None` once the listing is exhausted. Read through the crate's own
    /// `rel="next"` reader so the test follows the link the way a client does.
    fn next_cursor(response: &Response<ResponseBody>) -> Option<String> {
        let header = response.headers().get(LINK)?.to_str().ok()?;
        let query = next_page_target(header)?.split_once('?')?.1;

        form_urlencoded::parse(query.as_bytes())
            .find(|(name, _)| name == "last")
            .map(|(_, cursor)| cursor.into_owned())
    }

    /// A registry holding nothing serves an empty catalog rather than a miss,
    /// unlike the tag listing: here the caller named the registry itself.
    #[tokio::test]
    async fn list_catalog_entries_serves_an_empty_registry() {
        for_each_backend(async |test_case| {
            let response = test_case
                .registry()
                .handle_list_catalog(
                    CatalogRequest {
                        n: None,
                        last: None,
                    },
                    &|_: &Namespace| true,
                )
                .await
                .expect("an empty registry must serve a catalog, not a miss")
                .into_response()
                .unwrap();

            assert!(next_cursor(&response).is_none());
            assert!(catalog(response).await.is_empty());
        })
        .await;
    }

    #[tokio::test]
    async fn test_list_tag_entries() {
        for_each_backend(async |test_case| {
            let registry = test_case.registry();
            let namespace = Namespace::new("test-repo").unwrap();

            let test_content = b"test content";
            let test_digest = put_blob_direct(&registry.blob_store, test_content).await;
            let ops: Vec<(LinkKind, Digest)> = ["latest", "v1.0", "v2.0"]
                .iter()
                .map(|&tag| (LinkKind::Tag(Tag::new(tag).unwrap()), test_digest.clone()))
                .collect();
            seed_links(&registry.metadata_store, &namespace, &ops)
                .await
                .unwrap();

            let list = async |n: Option<u16>, last: Option<String>| {
                registry
                    .handle_list_tags(ListTagsRequest {
                        namespace: namespace.clone(),
                        n,
                        last,
                    })
                    .await
                    .unwrap()
                    .into_response()
                    .unwrap()
            };

            let all = list(None, None).await;
            assert!(next_cursor(&all).is_none());
            let body = response_json(all).await;
            assert_eq!(body["name"], namespace.as_ref());
            assert_eq!(
                body["tags"].as_array().unwrap().len(),
                3,
                "every tag must be listed"
            );

            let page1 = list(Some(2), None).await;
            let cursor = next_cursor(&page1).expect("a partial page must advertise Link");
            assert_eq!(tags(page1).await.len(), 2);

            let page2 = list(Some(2), Some(cursor)).await;
            assert!(next_cursor(&page2).is_none());
            assert_eq!(tags(page2).await.len(), 1);

            let one = list(Some(1), None).await;
            let cursor = next_cursor(&one).expect("a partial page must advertise Link");
            assert_eq!(tags(one).await.len(), 1);

            let two = list(Some(1), Some(cursor)).await;
            let cursor = next_cursor(&two).expect("a partial page must advertise Link");
            assert_eq!(tags(two).await.len(), 1);

            let three = list(Some(1), Some(cursor)).await;
            assert!(next_cursor(&three).is_none());
            assert_eq!(tags(three).await.len(), 1);

            let after_latest = list(Some(10), Some("latest".to_string())).await;
            assert!(next_cursor(&after_latest).is_none());
            assert_eq!(tags(after_latest).await.len(), 2);
        })
        .await;
    }

    /// A page holds `n` entries the caller may see, not `n` candidates minus
    /// the ones it may not: the visibility filter runs inside the walk, so a
    /// caller who sees one namespace in ten still gets full pages.
    #[tokio::test]
    async fn a_filtered_catalog_page_serves_a_full_page_of_visible_entries() {
        // FS only: this pins the walk's filtering, not backend specifics.
        let test_case = FSRegistryTestCase::new();
        let registry = test_case.registry();
        let digest = put_blob_direct(&registry.blob_store, b"visible").await;
        for i in 0..10 {
            let namespace = Namespace::new(&format!("vis-{i:02}")).unwrap();
            seed_links(
                &registry.metadata_store,
                &namespace,
                &[(LinkKind::Tag(Tag::new("latest").unwrap()), digest.clone())],
            )
            .await
            .unwrap();
        }

        // Every third namespace is visible, so a page of two must walk past
        // the ones it hides instead of serving a short page.
        let visible = |namespace: &Namespace| {
            namespace
                .as_ref()
                .rsplit_once('-')
                .and_then(|(_, i)| i.parse::<u32>().ok())
                .is_some_and(|i| i % 3 == 0)
        };
        let response = registry
            .handle_list_catalog(
                CatalogRequest {
                    n: Some(2),
                    last: None,
                },
                &visible,
            )
            .await
            .unwrap()
            .into_response()
            .unwrap();
        let cursor = next_cursor(&response);
        assert_eq!(catalog(response).await, ["vis-00", "vis-03"]);

        let response = registry
            .handle_list_catalog(
                CatalogRequest {
                    n: Some(2),
                    last: cursor,
                },
                &visible,
            )
            .await
            .unwrap()
            .into_response()
            .unwrap();
        assert_eq!(catalog(response).await, ["vis-06", "vis-09"]);
    }

    #[tokio::test]
    async fn list_catalog_entries_continuation_token_round_trip() {
        // FS only: this pins pagination logic, not backend specifics.
        let test_case = FSRegistryTestCase::new();
        let registry = test_case.registry();

        let namespaces = [
            "alpha/image",
            "beta/image",
            "gamma/image",
            "delta/image",
            "epsilon/image",
        ];

        let blob_content = b"pagination-test-blob";
        let digest = put_blob_direct(&registry.blob_store, blob_content).await;

        for ns_str in &namespaces {
            let ns = Namespace::new(ns_str).unwrap();
            seed_links(
                &registry.metadata_store,
                &ns,
                &[(LinkKind::Tag(Tag::new("latest").unwrap()), digest.clone())],
            )
            .await
            .unwrap();
        }

        let mut all_collected: Vec<String> = Vec::new();
        let mut last: Option<String> = None;

        loop {
            let response = registry
                .handle_list_catalog(CatalogRequest { n: Some(2), last }, &|_: &Namespace| true)
                .await
                .unwrap()
                .into_response()
                .unwrap();
            let cursor = next_cursor(&response);
            all_collected.extend(catalog(response).await);

            match cursor {
                None => break,
                Some(cursor) => last = Some(cursor),
            }
        }

        assert_eq!(
            all_collected.len(),
            namespaces.len(),
            "pagination must visit every namespace exactly once"
        );
        for ns in &namespaces {
            assert!(
                all_collected.iter().any(|got| got == ns),
                "namespace '{ns}' must appear in paginated results"
            );
        }
    }

    // A never-written namespace is unknown, not empty: a client probing
    // existence through this endpoint must be able to tell them apart.
    #[tokio::test]
    async fn list_tag_entries_unknown_namespace_is_not_found() {
        let test_case = FSRegistryTestCase::new();
        let registry = test_case.registry();

        let result = registry
            .handle_list_tags(ListTagsRequest {
                namespace: Namespace::new("no-such-repo/no-such-image").unwrap(),
                n: None,
                last: None,
            })
            .await;

        assert!(
            matches!(result, Err(Error::NameUnknown)),
            "a namespace holding nothing must be unknown, got {result:?}"
        );
    }

    // The other half of the same rule: a namespace whose tags were all deleted
    // still holds a revision, so it is empty rather than missing.
    #[tokio::test]
    async fn list_tag_entries_serves_a_namespace_whose_tags_are_gone() {
        let test_case = FSRegistryTestCase::new();
        let registry = test_case.registry();
        let namespace = Namespace::new("test-repo").unwrap();

        let digest = put_blob_direct(&registry.blob_store, b"revision body").await;
        seed_links(
            &registry.metadata_store,
            &namespace,
            &[(LinkKind::Digest(digest.clone()), digest)],
        )
        .await
        .unwrap();

        let response = registry
            .handle_list_tags(ListTagsRequest {
                namespace,
                n: None,
                last: None,
            })
            .await
            .expect("a namespace holding a revision must be served")
            .into_response()
            .unwrap();

        assert!(next_cursor(&response).is_none());
        assert!(
            tags(response).await.is_empty(),
            "a namespace with no tags must serve an empty list"
        );
    }

    #[tokio::test]
    async fn test_list_referrers_with_manifest() {
        for_each_backend(async |test_case| {
            let registry = test_case.registry();
            let namespace = &Namespace::new("test-repo").unwrap();

            let manifest_content = r#"{"schemaVersion": 2, "mediaType": "application/vnd.docker.distribution.manifest.v2+json"}"#;
            let media_type =
                MediaType::new("application/vnd.docker.distribution.manifest.v2+json").unwrap();

            let (base_manifest_digest, _) =
                create_test_blob(registry, namespace, manifest_content.as_bytes()).await;
            registry
                .put_manifest_direct(
                    namespace,
                    &Reference::Digest(base_manifest_digest.clone()),
                    Some(&media_type),
                    manifest_content.as_bytes(),
                )
                .await
                .unwrap();

            let (referrer_manifest_digest, _) =
                create_test_blob(registry, namespace, manifest_content.as_bytes()).await;
            registry
                .put_manifest_direct(
                    namespace,
                    &Reference::Digest(referrer_manifest_digest.clone()),
                    Some(&media_type),
                    manifest_content.as_bytes(),
                )
                .await
                .unwrap();

            let referrer_link = LinkKind::Referrer { subject: base_manifest_digest.clone(), referrer: referrer_manifest_digest.clone(), };
            seed_links(&registry
                .metadata_store, namespace, &[(
                        referrer_link,
                        referrer_manifest_digest.clone(),
                    )])
                .await
                .unwrap();

            let referrers = registry
                .list_referrers(None, &referrers_request(namespace, &base_manifest_digest))
                .await
                .unwrap();

            assert_eq!(referrers.items.len(), 1);
            assert_eq!(referrers.items[0].digest, referrer_manifest_digest);
        })
        .await;
    }

    // The referrer-resolution tests below run on the split-backend fixture:
    // manifest bodies live in the blob store only, so any resolution path
    // reading them through the metadata store fails here.

    // A digest with no stored blob.
    const HASH_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    fn subject() -> Digest {
        Digest::sha256("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap()
    }

    fn referrer_namespace() -> Namespace {
        Namespace::new("test-repo").unwrap()
    }

    fn descriptor_with(artifact_type: Option<&str>, manifest_digest: &Digest) -> Descriptor {
        Descriptor {
            media_type: media_type("application/vnd.oci.image.manifest.v1+json"),
            digest: manifest_digest.clone(),
            size: 100,
            annotations: HashMap::new(),
            artifact_type: artifact_type.map(media_type),
            platform: None,
        }
    }

    fn manifest_bytes(artifact_type: Option<&str>) -> Vec<u8> {
        let manifest = Manifest {
            schema_version: 2,
            media_type: Some(media_type("application/vnd.oci.image.manifest.v1+json")),
            artifact_type: artifact_type.map(media_type),
            ..Manifest::default()
        };
        serde_json::to_vec(&manifest).expect("serialization must succeed")
    }

    /// Split-backend fixture plus one referrer manifest in the blob store.
    async fn split_case_with_blob(
        blob_artifact_type: Option<&str>,
    ) -> (FSRegistryTestCase, Digest) {
        let case = FSRegistryTestCase::new();
        let digest = upload_blob(
            case.registry(),
            &referrer_namespace(),
            &manifest_bytes(blob_artifact_type),
        )
        .await;
        (case, digest)
    }

    /// A pull-through repository owning `mirror/*`, so a request for
    /// `mirror/app` maps to the upstream's `app`.
    async fn pull_through_repository(upstream: &str) -> Repository {
        let config = Config {
            upstream: vec![test_client_config(upstream)],
            ..Default::default()
        };
        let cache_backend = angos_cache::Config::Memory.to_backend().unwrap();
        Repository::new(
            "mirror",
            &config,
            &cache_backend,
            DEFAULT_MAX_MANIFEST_SIZE_BYTES,
        )
        .await
        .unwrap()
    }

    /// The referrer link `subject() -> manifest`, optionally carrying a cached
    /// descriptor.
    async fn create_referrer_link(
        m: &MetadataStore,
        namespace: &Namespace,
        manifest: &Digest,
        descriptor: Option<Descriptor>,
    ) {
        m.put_referrer(namespace, &subject(), manifest, descriptor.as_ref())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn returns_cached_descriptor_when_no_filter() {
        // The blob is deliberately unparseable, so only the cached descriptor
        // can answer.
        let case = FSRegistryTestCase::new();
        let registry = case.registry();
        let manifest_digest = upload_blob(registry, &referrer_namespace(), b"not json").await;
        let desc = descriptor_with(Some("application/vnd.foo"), &manifest_digest);
        create_referrer_link(
            &registry.metadata_store,
            &referrer_namespace(),
            &manifest_digest,
            Some(desc.clone()),
        )
        .await;

        let result = registry
            .resolve_referrer_descriptor(&referrer_namespace(), &subject(), manifest_digest, None)
            .await;
        assert_eq!(result, Some(desc));
    }

    #[tokio::test]
    async fn returns_cached_descriptor_when_filter_matches() {
        let case = FSRegistryTestCase::new();
        let registry = case.registry();
        let manifest_digest = upload_blob(registry, &referrer_namespace(), b"not json").await;
        let at = media_type("application/vnd.foo");
        let desc = descriptor_with(Some(&at), &manifest_digest);
        create_referrer_link(
            &registry.metadata_store,
            &referrer_namespace(),
            &manifest_digest,
            Some(desc.clone()),
        )
        .await;

        let result = registry
            .resolve_referrer_descriptor(
                &referrer_namespace(),
                &subject(),
                manifest_digest,
                Some(&at),
            )
            .await;
        assert_eq!(result, Some(desc));
    }

    #[tokio::test]
    async fn returns_none_when_cached_descriptor_filter_mismatches() {
        // The stored manifest does match the filter, so a wrong fall-through to
        // the blob would return Some.
        let (case, manifest_digest) = split_case_with_blob(Some("application/vnd.bar")).await;
        let registry = case.registry();
        let desc = descriptor_with(Some("application/vnd.foo"), &manifest_digest);
        create_referrer_link(
            &registry.metadata_store,
            &referrer_namespace(),
            &manifest_digest,
            Some(desc),
        )
        .await;

        let filter = media_type("application/vnd.bar");
        let result = registry
            .resolve_referrer_descriptor(
                &referrer_namespace(),
                &subject(),
                manifest_digest,
                Some(&filter),
            )
            .await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn falls_through_to_blob_when_cached_descriptor_missing_artifact_type() {
        // The filter cannot be evaluated from a cache entry carrying no
        // artifact_type, so the manifest blob decides.
        let (case, manifest_digest) = split_case_with_blob(Some("application/vnd.foo")).await;
        let registry = case.registry();
        let desc = descriptor_with(None, &manifest_digest);
        create_referrer_link(
            &registry.metadata_store,
            &referrer_namespace(),
            &manifest_digest,
            Some(desc),
        )
        .await;

        let at = media_type("application/vnd.foo");
        let result = registry
            .resolve_referrer_descriptor(
                &referrer_namespace(),
                &subject(),
                manifest_digest,
                Some(&at),
            )
            .await;
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn falls_through_to_blob_when_link_is_absent() {
        let (case, manifest_digest) = split_case_with_blob(None).await;
        let registry = case.registry();

        let result = registry
            .resolve_referrer_descriptor(&referrer_namespace(), &subject(), manifest_digest, None)
            .await;
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn falls_through_to_blob_when_link_carries_no_descriptor() {
        let (case, manifest_digest) = split_case_with_blob(None).await;
        let registry = case.registry();
        create_referrer_link(
            &registry.metadata_store,
            &referrer_namespace(),
            &manifest_digest,
            None,
        )
        .await;

        let result = registry
            .resolve_referrer_descriptor(&referrer_namespace(), &subject(), manifest_digest, None)
            .await;
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn returns_blob_descriptor_when_blob_filter_matches() {
        let (case, manifest_digest) = split_case_with_blob(Some("application/vnd.foo")).await;
        let registry = case.registry();

        let at = media_type("application/vnd.foo");
        let result = registry
            .resolve_referrer_descriptor(
                &referrer_namespace(),
                &subject(),
                manifest_digest,
                Some(&at),
            )
            .await;
        assert!(result.is_some());
        assert_eq!(
            result.unwrap().artifact_type.as_deref(),
            Some("application/vnd.foo")
        );
    }

    #[tokio::test]
    async fn returns_none_when_blob_filter_mismatches() {
        let (case, manifest_digest) = split_case_with_blob(Some("application/vnd.foo")).await;
        let registry = case.registry();

        let filter = media_type("application/vnd.bar");
        let result = registry
            .resolve_referrer_descriptor(
                &referrer_namespace(),
                &subject(),
                manifest_digest,
                Some(&filter),
            )
            .await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn returns_none_when_blob_not_found() {
        let case = FSRegistryTestCase::new();
        let registry = case.registry();

        let result = registry
            .resolve_referrer_descriptor(
                &referrer_namespace(),
                &subject(),
                Digest::sha256(HASH_B).unwrap(),
                None,
            )
            .await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn returns_none_when_blob_is_invalid_manifest_json() {
        let case = FSRegistryTestCase::new();
        let registry = case.registry();
        let manifest_digest = upload_blob(registry, &referrer_namespace(), b"not json").await;

        let result = registry
            .resolve_referrer_descriptor(&referrer_namespace(), &subject(), manifest_digest, None)
            .await;
        assert!(result.is_none());
    }

    /// A repository imported from a registry whose clients used the referrers
    /// fallback tag keeps those entries.
    #[tokio::test]
    async fn list_referrers_merges_the_fallback_tag_index() {
        let case = FSRegistryTestCase::new();
        let registry = case.registry();
        let namespace = referrer_namespace();
        let subject = subject();

        let indexed = Digest::sha256_of_bytes(b"referrer the index knows");
        create_referrer_link(
            &registry.metadata_store,
            &namespace,
            &indexed,
            Some(descriptor_with(None, &indexed)),
        )
        .await;

        // The fallback tag an older client would have pushed: an index whose
        // manifests are the subject's referrers.
        let tagged = Digest::sha256_of_bytes(b"referrer only the tag knows");
        let fallback = serde_json::to_vec(&json!({
            "schemaVersion": 2,
            "mediaType": OCI_INDEX_MEDIA_TYPE,
            "manifests": [descriptor_with(None, &tagged)],
        }))
        .unwrap();
        let fallback_digest = upload_blob(registry, &namespace, &fallback).await;
        let tag = subject.referrers_fallback_tag();
        seed_links(
            &registry.metadata_store,
            &namespace,
            &[(LinkKind::Tag(tag), fallback_digest.clone())],
        )
        .await
        .unwrap();

        let page = registry
            .list_referrers(None, &referrers_request(&namespace, &subject))
            .await
            .unwrap();

        let mut served: Vec<Digest> = page.items.into_iter().map(|d| d.digest).collect();
        served.sort();
        let mut expected = vec![indexed, tagged];
        expected.sort();
        assert_eq!(
            served, expected,
            "the listing must hold both the indexed and the fallback-tagged referrer"
        );
    }

    /// A pull-through namespace lists what the upstream holds alongside what it
    /// cached, or an uncached subject would answer with nothing at all.
    #[tokio::test]
    async fn list_referrers_merges_the_upstream_listing() {
        let case = FSRegistryTestCase::new();
        let registry = case.registry();
        let namespace = Namespace::new("mirror/app").unwrap();
        let cached = Digest::sha256_of_bytes(b"cached referrer");
        create_referrer_link(
            &registry.metadata_store,
            &namespace,
            &cached,
            Some(descriptor_with(None, &cached)),
        )
        .await;

        let remote = Digest::sha256_of_bytes(b"upstream referrer");
        let subject = subject();
        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("/v2/app/referrers/{subject}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "schemaVersion": 2,
                "mediaType": OCI_INDEX_MEDIA_TYPE,
                "manifests": [descriptor_with(None, &remote)],
            })))
            .mount(&mock_server)
            .await;
        let repository = pull_through_repository(&mock_server.uri()).await;

        let page = registry
            .list_referrers(Some(&repository), &referrers_request(&namespace, &subject))
            .await
            .unwrap();

        let mut served: Vec<Digest> = page.items.into_iter().map(|d| d.digest).collect();
        served.sort();
        let mut expected = vec![cached, remote];
        expected.sort();
        assert_eq!(
            served, expected,
            "a pull-through listing must hold the cached and the upstream referrer"
        );
    }

    /// An upstream that cannot be reached must not take the cached referrers
    /// down with it.
    #[tokio::test]
    async fn list_referrers_serves_the_cache_when_the_upstream_fails() {
        let case = FSRegistryTestCase::new();
        let registry = case.registry();
        let namespace = Namespace::new("mirror/app").unwrap();
        let cached = Digest::sha256_of_bytes(b"cached referrer");
        create_referrer_link(
            &registry.metadata_store,
            &namespace,
            &cached,
            Some(descriptor_with(None, &cached)),
        )
        .await;
        let repository = pull_through_repository("http://127.0.0.1:1").await;

        let page = registry
            .list_referrers(
                Some(&repository),
                &referrers_request(&namespace, &subject()),
            )
            .await
            .unwrap();

        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].digest, cached);
    }

    // A referrer whose link carries no cached descriptor still resolves through
    // the public listing when blob and metadata stores use separate roots.
    #[tokio::test]
    async fn list_referrers_resolves_manifests_on_split_backends() {
        let (case, manifest_digest) = split_case_with_blob(Some("application/vnd.foo")).await;
        let registry = case.registry();
        create_referrer_link(
            &registry.metadata_store,
            &referrer_namespace(),
            &manifest_digest,
            None,
        )
        .await;

        let referrers = registry
            .list_referrers(None, &referrers_request(&referrer_namespace(), &subject()))
            .await
            .unwrap();
        assert_eq!(referrers.items.len(), 1);
        assert_eq!(referrers.items[0].digest, manifest_digest);
    }

    /// A wide fan-out is served one page at a time: following `Link` visits
    /// every referrer exactly once and no single response carries them all.
    #[tokio::test]
    async fn get_referrers_pages_through_the_fan_out() {
        let case = FSRegistryTestCase::new();
        let registry = case.registry();

        // One past the page size the registry serves: the endpoint takes no
        // page-size parameter, so this is what forces a second page.
        let overflowing = usize::from(DEFAULT_PAGE_SIZE) + 1;
        let mut expected = Vec::new();
        for index in 0..overflowing {
            let digest = Digest::sha256_of_bytes(index.to_le_bytes());
            create_referrer_link(
                &registry.metadata_store,
                &referrer_namespace(),
                &digest,
                Some(descriptor_with(None, &digest)),
            )
            .await;
            expected.push(digest.to_string());
        }
        expected.sort();

        let mut served = Vec::new();
        let mut last = None;
        let mut pages = 0;
        loop {
            let response = registry
                .handle_get_referrers(GetReferrersRequest {
                    namespace: referrer_namespace(),
                    digest: subject(),
                    artifact_type: None,
                    last,
                })
                .await
                .unwrap()
                .into_response()
                .unwrap();
            let cursor = next_cursor(&response);
            let page = json_strings_at(response, "manifests", "digest").await;
            assert!(
                page.len() <= usize::from(DEFAULT_PAGE_SIZE),
                "a page must not exceed the size the registry serves"
            );
            pages += 1;
            served.extend(page);

            match cursor {
                None => break,
                Some(cursor) => last = Some(cursor),
            }
        }

        assert!(pages > 1, "a fan-out past the page size must be paginated");
        assert_eq!(
            served, expected,
            "paging must visit every referrer exactly once, in digest order"
        );
    }

    /// Counts the listings of one subject's referrer records.
    struct CountRecordListings {
        dir: String,
        count: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl StoreHook for CountRecordListings {
        async fn before(&self, op: StoreOp<'_>) -> Result<(), StorageError> {
            if let StoreOp::List { prefix } = op
                && prefix == self.dir
            {
                self.count.fetch_add(1, Ordering::SeqCst);
            }
            Ok(())
        }
    }

    /// A page reads the records from its cursor: with more records than one
    /// listing returns, the last page costs a single listing, where reading
    /// from the start would take two.
    #[tokio::test]
    async fn a_referrers_page_lists_the_records_from_its_cursor() {
        let case = FSRegistryTestCase::new();
        let seeding = case.metadata_store();
        let namespace = referrer_namespace();
        let mut referrers: Vec<Digest> = (0..=usize::from(LIST_PAGE))
            .map(|index| Digest::sha256_of_bytes(index.to_le_bytes()))
            .collect();
        {
            let (seeding, namespace, subject) = (&seeding, &namespace, &subject());
            try_join_all(referrers.iter().map(|referrer| async move {
                let descriptor = descriptor_with(None, referrer);
                seeding
                    .put_referrer(namespace, subject, referrer, Some(&descriptor))
                    .await
            }))
            .await
            .unwrap();
        }
        referrers.sort();

        let count = Arc::new(AtomicUsize::new(0));
        let registry = create_test_registry(
            case.blob_store(),
            metadata_store_over(Arc::new(HookedStore::new(
                seeding.object_store().clone(),
                CountRecordListings {
                    dir: namespace.referrer_record_dir(&subject()),
                    count: count.clone(),
                },
            ))),
        );
        let page_size = usize::from(DEFAULT_PAGE_SIZE);
        let page = registry
            .list_referrers(
                None,
                &GetReferrersRequest {
                    namespace,
                    digest: subject(),
                    artifact_type: None,
                    last: Some(referrers[referrers.len() - page_size - 1].to_string()),
                },
            )
            .await
            .unwrap();

        let served: Vec<&Digest> = page.items.iter().map(|referrer| &referrer.digest).collect();
        let expected: Vec<&Digest> = referrers[referrers.len() - page_size..].iter().collect();
        assert_eq!(served, expected);
        assert_eq!(page.next_token, None);
        assert_eq!(
            count.load(Ordering::SeqCst),
            1,
            "a page must not list the records before its cursor"
        );
    }

    /// A filter that drops a whole page's worth of candidates must not answer
    /// with a short page while matches remain: the walk carries on past them.
    #[tokio::test]
    async fn filtered_referrer_pages_fill_past_what_the_filter_drops() {
        let case = FSRegistryTestCase::new();
        let registry = case.registry();
        let wanted = "application/vnd.wanted";

        // Enough that the dropped stretch alone covers a full page, so a page
        // cut over the candidates would answer with nothing at all.
        let dropped = usize::from(DEFAULT_PAGE_SIZE);
        let matching = 50;
        let mut digests: Vec<Digest> = (0..dropped + matching)
            .map(|index| Digest::sha256_of_bytes(index.to_le_bytes()))
            .collect();
        digests.sort_by_key(ToString::to_string);

        let mut expected = Vec::new();
        for (position, digest) in digests.iter().enumerate() {
            let artifact_type = if position < dropped {
                "application/vnd.other"
            } else {
                expected.push(digest.to_string());
                wanted
            };
            create_referrer_link(
                &registry.metadata_store,
                &referrer_namespace(),
                digest,
                Some(descriptor_with(Some(artifact_type), digest)),
            )
            .await;
        }

        let page = registry
            .list_referrers(
                None,
                &GetReferrersRequest {
                    namespace: referrer_namespace(),
                    digest: subject(),
                    artifact_type: Some(media_type(wanted)),
                    last: None,
                },
            )
            .await
            .unwrap();

        let served: Vec<String> = page.items.iter().map(|d| d.digest.to_string()).collect();
        assert_eq!(
            served, expected,
            "the page must hold every match, not stop at the dropped stretch"
        );
        assert!(
            page.next_token.is_none(),
            "nothing remains after the last match, so no next page is advertised"
        );
    }

    #[tokio::test]
    async fn test_list_referrers() {
        for_each_backend(async |test_case| {
            let registry = test_case.registry();
            let m = registry.metadata_store.clone();
            let namespace = &Namespace::new("test-repo").unwrap();
            let base_digest = put_blob_direct(&test_case.blob_store(), b"base manifest content").await;
            let base_link = LinkKind::Digest(base_digest.clone());

            create_link(&m, namespace, &base_link, &base_digest).await;

            let referrer_content = format!(
                r#"{{
                    "schemaVersion": 2,
                    "mediaType": "application/vnd.oci.image.manifest.v1+json",
                    "subject": {{
                        "mediaType": "application/vnd.oci.image.manifest.v1+json",
                        "digest": "{base_digest}",
                        "size": 123
                    }},
                    "artifactType": "application/vnd.example.test-artifact",
                    "config": {{
                        "mediaType": "application/vnd.oci.image.config.v1+json",
                        "digest": "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                        "size": 7023
                    }},
                    "layers": []
                }}"#
            );

            let referrer_digest = put_blob_direct(&test_case.blob_store(), referrer_content.as_bytes()).await;
            let link = LinkKind::Digest(referrer_digest.clone());

            create_link(&m, namespace, &link, &referrer_digest).await;

            let referrers_link = LinkKind::Referrer {
                subject: base_digest.clone(),
                referrer: referrer_digest.clone(),
            };

            create_link(&m, namespace, &referrers_link, &referrer_digest).await;

            let referrers = registry
                .list_referrers(None, &referrers_request(namespace, &base_digest))
                .await;

            let expected = vec![Descriptor {
                media_type: media_type("application/vnd.oci.image.manifest.v1+json"),
                digest: referrer_digest,
                size: u64::try_from(referrer_content.len()).unwrap(),
                annotations: HashMap::new(),
                artifact_type: Some(media_type("application/vnd.example.test-artifact")),
                platform: None,
            }];

            assert_eq!(referrers.unwrap().items, expected);

            let filtered_referrers = registry
                .list_referrers(
                    None,
                    &GetReferrersRequest {
                        artifact_type: Some(media_type("application/vnd.example.test-artifact")),
                        ..referrers_request(namespace, &base_digest)
                    },
                )
                .await
                .unwrap();

            assert!(!filtered_referrers.items.is_empty());

            let non_matching_referrers = registry
                .list_referrers(
                    None,
                    &GetReferrersRequest {
                        artifact_type: Some(media_type("application/vnd.non-existent")),
                        ..referrers_request(namespace, &base_digest)
                    },
                )
                .await
                .unwrap();

            assert!(non_matching_referrers.items.is_empty());
        })
        .await;
    }

    #[tokio::test]
    async fn test_list_referrers_with_artifact_type_filter() {
        for_each_backend(async |test_case| {
            let registry = test_case.registry();
            let m = registry.metadata_store.clone();
            let namespace = &Namespace::new("test-referrers-filter").unwrap();
            let subject_digest =
                put_blob_direct(&test_case.blob_store(), b"subject manifest for filter test").await;
            let subject_link = LinkKind::Digest(subject_digest.clone());
            create_link(&m, namespace, &subject_link, &subject_digest).await;

            for i in 0..3 {
                let referrer_content = format!(
                    r#"{{
                        "schemaVersion": 2,
                        "mediaType": "application/vnd.oci.image.manifest.v1+json",
                        "subject": {{
                            "mediaType": "application/vnd.oci.image.manifest.v1+json",
                            "digest": "{subject_digest}",
                            "size": 123
                        }},
                        "artifactType": "application/vnd.example.sbom",
                        "config": {{
                            "mediaType": "application/vnd.oci.image.config.v1+json",
                            "digest": "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                            "size": 7023
                        }},
                        "layers": [],
                        "annotations": {{ "sbom-index": "{i}" }}
                    }}"#
                );

                let referrer_digest = put_blob_direct(&test_case.blob_store(), referrer_content.as_bytes()).await;
                let digest_link = LinkKind::Digest(referrer_digest.clone());
                create_link(&m, namespace, &digest_link, &referrer_digest).await;

                let referrer_link = LinkKind::Referrer {
                    subject: subject_digest.clone(),
                    referrer: referrer_digest.clone(),
                };
                create_link(&m, namespace, &referrer_link, &referrer_digest).await;
            }

            for i in 0..2 {
                let referrer_content = format!(
                    r#"{{
                        "schemaVersion": 2,
                        "mediaType": "application/vnd.oci.image.manifest.v1+json",
                        "subject": {{
                            "mediaType": "application/vnd.oci.image.manifest.v1+json",
                            "digest": "{subject_digest}",
                            "size": 123
                        }},
                        "artifactType": "application/vnd.example.signature",
                        "config": {{
                            "mediaType": "application/vnd.oci.image.config.v1+json",
                            "digest": "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                            "size": 7023
                        }},
                        "layers": [],
                        "annotations": {{ "sig-index": "{i}" }}
                    }}"#
                );

                let referrer_digest = put_blob_direct(&test_case.blob_store(), referrer_content.as_bytes()).await;
                let digest_link = LinkKind::Digest(referrer_digest.clone());
                create_link(&m, namespace, &digest_link, &referrer_digest).await;

                let referrer_link = LinkKind::Referrer {
                    subject: subject_digest.clone(),
                    referrer: referrer_digest.clone(),
                };
                create_link(&m, namespace, &referrer_link, &referrer_digest).await;
            }

            let descriptors = registry
                .list_referrers(
                    None,
                    &GetReferrersRequest {
                        artifact_type: Some(media_type("application/vnd.example.sbom")),
                        ..referrers_request(namespace, &subject_digest)
                    },
                )
                .await
                .unwrap();

            assert_eq!(
                descriptors.items.len(),
                3,
                "Expected 3 SBOM referrer descriptors but got {}",
                descriptors.items.len()
            );

            for desc in &descriptors.items {
                assert_eq!(
                    desc.artifact_type.as_deref(),
                    Some("application/vnd.example.sbom"),
                    "All filtered descriptors should have SBOM artifact type"
                );
            }
        })
        .await;
    }

    #[tokio::test]
    async fn test_list_referrers_deterministic_order() {
        for_each_backend(async |test_case| {
            let registry = test_case.registry();
            let m = registry.metadata_store.clone();
            let namespace = &Namespace::new("test-referrers-order").unwrap();
            let subject_digest =
                put_blob_direct(&test_case.blob_store(), b"subject manifest for order test").await;
            let subject_link = LinkKind::Digest(subject_digest.clone());
            create_link(&m, namespace, &subject_link, &subject_digest).await;

            for i in 0..10 {
                let referrer_content = format!(
                    r#"{{
                        "schemaVersion": 2,
                        "mediaType": "application/vnd.oci.image.manifest.v1+json",
                        "subject": {{
                            "mediaType": "application/vnd.oci.image.manifest.v1+json",
                            "digest": "{subject_digest}",
                            "size": 123
                        }},
                        "artifactType": "application/vnd.example.test-artifact",
                        "config": {{
                            "mediaType": "application/vnd.oci.image.config.v1+json",
                            "digest": "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                            "size": 7023
                        }},
                        "layers": [],
                        "annotations": {{ "order-index": "{i}" }}
                    }}"#
                );

                let referrer_digest = put_blob_direct(&test_case.blob_store(), referrer_content.as_bytes()).await;
                let digest_link = LinkKind::Digest(referrer_digest.clone());
                create_link(&m, namespace, &digest_link, &referrer_digest).await;

                let referrer_link = LinkKind::Referrer {
                    subject: subject_digest.clone(),
                    referrer: referrer_digest.clone(),
                };
                create_link(&m, namespace, &referrer_link, &referrer_digest).await;
            }

            let result1 = registry
                .list_referrers(None, &referrers_request(namespace, &subject_digest))
                .await
                .unwrap();
            let result2 = registry
                .list_referrers(None, &referrers_request(namespace, &subject_digest))
                .await
                .unwrap();
            let result3 = registry
                .list_referrers(None, &referrers_request(namespace, &subject_digest))
                .await
                .unwrap();

            assert_eq!(
                result1.items.len(),
                10,
                "Expected 10 referrer descriptors but got {}",
                result1.items.len()
            );
            assert_eq!(
                result1, result2,
                "First and second list_referrers calls should return identical results"
            );
            assert_eq!(
                result2, result3,
                "Second and third list_referrers calls should return identical results"
            );

            for pair in result1.items.windows(2) {
                assert!(
                    pair[0].digest.to_string() <= pair[1].digest.to_string(),
                    "Descriptors should be sorted by digest: {} should come before {}",
                    pair[0].digest,
                    pair[1].digest
                );
            }
        })
        .await;
    }

    #[tokio::test]
    async fn test_list_referrers_with_stored_descriptor() {
        for_each_backend(async |test_case| {
            let registry = test_case.registry();
            let m = registry.metadata_store.clone();
            let namespace = &Namespace::new("test-stored-descriptor").unwrap();

            let base_digest =
                put_blob_direct(&test_case.blob_store(), b"base manifest content").await;
            let base_link = LinkKind::Digest(base_digest.clone());
            create_link(&m, namespace, &base_link, &base_digest).await;

            // The referrer blob is never written, so the descriptor can only come
            // from the stored link metadata.
            let referrer_digest: Digest =
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .parse()
                    .unwrap();

            let descriptor = Descriptor {
                media_type: media_type("application/vnd.oci.image.manifest.v1+json"),
                digest: referrer_digest.clone(),
                size: 1234,
                annotations: HashMap::new(),
                artifact_type: Some(media_type("application/vnd.example.test-artifact")),
                platform: None,
            };

            m.put_referrer(namespace, &base_digest, &referrer_digest, Some(&descriptor))
                .await
                .unwrap();

            let referrers = registry
                .list_referrers(None, &referrers_request(namespace, &base_digest))
                .await
                .unwrap();

            assert_eq!(referrers.items.len(), 1, "Expected 1 referrer descriptor");
            assert_eq!(referrers.items[0], descriptor);

            let filtered = registry
                .list_referrers(
                    None,
                    &GetReferrersRequest {
                        artifact_type: Some(media_type("application/vnd.example.test-artifact")),
                        ..referrers_request(namespace, &base_digest)
                    },
                )
                .await
                .unwrap();
            assert_eq!(filtered.items.len(), 1, "Should match artifact type filter");
            assert_eq!(filtered.items[0], descriptor);

            let non_matching = registry
                .list_referrers(
                    None,
                    &GetReferrersRequest {
                        artifact_type: Some(media_type("application/vnd.non-existent")),
                        ..referrers_request(namespace, &base_digest)
                    },
                )
                .await
                .unwrap();
            assert!(
                non_matching.items.is_empty(),
                "Should return empty for non-matching artifact type"
            );
        })
        .await;
    }
}
