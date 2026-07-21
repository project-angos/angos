use futures_util::stream::TryStreamExt;
use tracing::{instrument, warn};

use crate::{
    oci::{Descriptor, Digest, Manifest, Namespace, Tag},
    registry::{Error, Registry, metadata_store::LinkKind},
};

/// Fan-out for resolving referrer candidates to descriptors: each candidate is
/// an independent manifest read, so a bounded window keeps the listing and the
/// reads overlapped.
const REFERRER_RESOLVE_CONCURRENCY: usize = 10;

/// Default page size applied when a listing request omits `n`. Shared with the
/// HTTP handlers so the pagination `Link` they build echoes the size actually
/// used.
pub const DEFAULT_PAGE_SIZE: u16 = 100;

impl Registry {
    /// Lists namespaces and returns the continuation cursor (the `last` value
    /// for the next page), or `None` when the listing is exhausted. Building the
    /// pagination URL from the cursor is the handler's concern.
    pub async fn list_catalog_entries(
        &self,
        n: Option<u16>,
        last: Option<String>,
    ) -> Result<(Vec<String>, Option<String>), Error> {
        let n = n.unwrap_or(DEFAULT_PAGE_SIZE);
        self.metadata_store.list_namespaces(n, last).await
    }

    /// Lists a namespace's tags and returns the continuation cursor (the `last`
    /// value for the next page), or `None` when the listing is exhausted.
    /// Building the pagination URL from the cursor is the handler's concern.
    pub async fn list_tag_entries(
        &self,
        namespace: &Namespace,
        n: Option<u16>,
        last: Option<String>,
    ) -> Result<(Vec<Tag>, Option<String>), Error> {
        let n = n.unwrap_or(DEFAULT_PAGE_SIZE);
        self.metadata_store.list_tags(namespace, n, last).await
    }

    /// Returns the referrer descriptors for a subject digest along with a flag
    /// indicating whether an `artifactType` filter was applied. Presentation
    /// (image-index body + headers) is the handler's responsibility.
    #[instrument]
    pub async fn get_referrers(
        &self,
        namespace: &Namespace,
        digest: &Digest,
        artifact_type: Option<String>,
    ) -> Result<(Vec<Descriptor>, bool), Error> {
        let filtered = artifact_type.is_some();
        let manifests = self
            .list_referrers(namespace, digest, artifact_type)
            .await?;

        Ok((manifests, filtered))
    }

    /// Resolves every referrer of `digest` in `namespace` to a sorted
    /// descriptor list, filtered by `artifact_type` when given.
    #[instrument]
    pub async fn list_referrers(
        &self,
        namespace: &Namespace,
        digest: &Digest,
        artifact_type: Option<String>,
    ) -> Result<Vec<Descriptor>, Error> {
        let artifact_type = artifact_type.as_ref();

        // Candidate digests stream off the listing while up to
        // `REFERRER_RESOLVE_CONCURRENCY` of them resolve concurrently, so the
        // resolution window spans page boundaries and overlaps the page fetches.
        let mut referrers: Vec<Descriptor> = self
            .metadata_store
            .stream_referrer_digests(namespace, digest)
            .map_ok(|manifest_digest| async move {
                Ok::<_, Error>(
                    self.resolve_referrer_descriptor(
                        namespace,
                        digest,
                        manifest_digest,
                        artifact_type,
                    )
                    .await,
                )
            })
            .try_buffer_unordered(REFERRER_RESOLVE_CONCURRENCY)
            .try_filter_map(|descriptor| async move { Ok(descriptor) })
            .try_collect()
            .await?;

        referrers.sort_by(|a, b| a.digest.cmp(&b.digest));
        Ok(referrers)
    }

    /// Resolves a single referrer entry to an OCI [`Descriptor`], applying an
    /// optional `artifact_type` filter: returns the cached link descriptor
    /// when that suffices, else falls back to reading the manifest through the
    /// blob store, where manifest bodies live.
    async fn resolve_referrer_descriptor(
        &self,
        namespace: &Namespace,
        subject_digest: &Digest,
        manifest_digest: Digest,
        artifact_type: Option<&String>,
    ) -> Option<Descriptor> {
        let referrer_link = LinkKind::Referrer(subject_digest.clone(), manifest_digest.clone());

        if let Ok(metadata) = self
            .metadata_store
            .read_link_reference(namespace, &referrer_link)
            .await
            && let Some(desc) = metadata.descriptor
        {
            match artifact_type {
                Some(at) if desc.artifact_type.as_deref() == Some(at.as_str()) => {
                    return Some(desc);
                }
                None => return Some(desc),
                // Cached descriptor has no artifact_type; fall through to manifest
                // read so the filter can be evaluated against the full manifest data.
                Some(_) if desc.artifact_type.is_none() => {}
                Some(_) => return None,
            }
        }

        match self.blob_store.read(&manifest_digest).await {
            Ok(data) => {
                let manifest_len = data.len();
                match Manifest::from_slice(&data) {
                    Ok(mut manifest) => {
                        if !manifest.artifact_type_matches(artifact_type) {
                            return None;
                        }
                        manifest.take_descriptor(manifest_digest, manifest_len as u64)
                    }
                    Err(e) => {
                        warn!("Failed to parse referrer manifest {manifest_digest}: {e}");
                        None
                    }
                }
            }
            Err(Error::BlobUnknown) => {
                warn!("Referrer manifest blob {manifest_digest} not found, skipping");
                None
            }
            Err(e) => {
                warn!("Failed to read referrer manifest {manifest_digest}: {e}");
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::{
        oci::{Descriptor, Digest, Manifest, MediaType, Namespace, Reference, Tag},
        registry::{
            metadata_store::{LinkKind, LinkOperation, MetadataStore},
            test_utils::{
                FSRegistryTestCase, create_test_blob, for_each_backend, media_type,
                put_blob_direct, upload_blob,
            },
        },
    };

    #[tokio::test]
    async fn test_list_catalog_entries() {
        for_each_backend(async |test_case| {
            let registry = test_case.registry();

            let (namespaces, token) = registry.list_catalog_entries(None, None).await.unwrap();
            assert!(namespaces.is_empty());
            assert!(token.is_none());

            let (namespaces, token) = registry.list_catalog_entries(Some(10), None).await.unwrap();
            assert!(namespaces.is_empty());
            assert!(token.is_none());

            let (namespaces, token) = registry
                .list_catalog_entries(Some(10), Some("test".to_string()))
                .await
                .unwrap();
            assert!(namespaces.is_empty());
            assert!(token.is_none());
        })
        .await;
    }

    #[tokio::test]
    async fn test_list_tag_entries() {
        for_each_backend(async |test_case| {
            let registry = test_case.registry();
            let namespace = &Namespace::new("test-repo").unwrap();

            let test_content = b"test content";
            let test_digest = put_blob_direct(registry.metadata_store.store(), test_content).await;
            let tags = ["latest", "v1.0", "v2.0"];
            let ops: Vec<LinkOperation> = tags
                .iter()
                .map(|&tag| {
                    LinkOperation::create(
                        LinkKind::Tag(Tag::new(tag).unwrap()),
                        test_digest.clone(),
                    )
                })
                .collect();
            registry
                .metadata_store
                .update_links(namespace, &ops)
                .await
                .unwrap();

            let (tags, token) = registry
                .list_tag_entries(namespace, None, None)
                .await
                .unwrap();
            assert_eq!(tags.len(), 3);
            assert!(tags.contains(&Tag::new("latest").unwrap()));
            assert!(tags.contains(&Tag::new("v1.0").unwrap()));
            assert!(tags.contains(&Tag::new("v2.0").unwrap()));
            assert!(token.is_none());

            let (page1, token1) = registry
                .list_tag_entries(namespace, Some(2), None)
                .await
                .unwrap();
            assert_eq!(page1.len(), 2);
            assert!(token1.is_some());

            let last_tag = token1.unwrap();

            let (page2, token2) = registry
                .list_tag_entries(namespace, Some(2), Some(last_tag))
                .await
                .unwrap();
            assert_eq!(page2.len(), 1);
            assert!(token2.is_none());

            let (page1, token1) = registry
                .list_tag_entries(namespace, Some(1), None)
                .await
                .unwrap();
            assert_eq!(page1.len(), 1);
            assert!(token1.is_some());

            let last_tag = token1.unwrap();

            let (page2, token2) = registry
                .list_tag_entries(namespace, Some(1), Some(last_tag))
                .await
                .unwrap();
            assert_eq!(page2.len(), 1);
            assert!(token2.is_some());

            let last_tag = token2.unwrap();

            let (page3, token3) = registry
                .list_tag_entries(namespace, Some(1), Some(last_tag))
                .await
                .unwrap();
            assert_eq!(page3.len(), 1);
            assert!(token3.is_none());

            let (tags, token) = registry
                .list_tag_entries(namespace, Some(10), Some("latest".to_string()))
                .await
                .unwrap();
            assert_eq!(tags.len(), 2);
            assert!(token.is_none());
        })
        .await;
    }

    // list_catalog_entries pagination: write N namespaces then page through them
    // using the returned continuation token, asserting every entry is visited
    // exactly once.
    #[tokio::test]
    async fn list_catalog_entries_continuation_token_round_trip() {
        // Use only the FS backend: this tests pagination logic, not backend specifics.
        let test_case = crate::registry::test_utils::FSRegistryTestCase::new();
        let registry = test_case.registry();

        let namespaces = [
            "alpha/image",
            "beta/image",
            "gamma/image",
            "delta/image",
            "epsilon/image",
        ];

        let blob_content = b"pagination-test-blob";
        let digest = put_blob_direct(registry.metadata_store.store(), blob_content).await;

        for ns_str in &namespaces {
            let ns = Namespace::new(ns_str).unwrap();
            registry
                .metadata_store
                .update_links(
                    &ns,
                    &[LinkOperation::create(
                        LinkKind::Tag(Tag::new("latest").unwrap()),
                        digest.clone(),
                    )],
                )
                .await
                .unwrap();
        }

        // Fetch 2 at a time and collect all namespaces.
        let mut all_collected: Vec<String> = Vec::new();
        let mut last: Option<String> = None;

        loop {
            let (page, token) = registry.list_catalog_entries(Some(2), last).await.unwrap();
            all_collected.extend(page);

            // The token is the continuation cursor: feed it straight back as `last`.
            match token {
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
                all_collected.contains(&ns.to_string()),
                "namespace '{ns}' must appear in paginated results"
            );
        }
    }

    // list_tag_entries for a namespace that has never been written must return
    // an empty tag list and no continuation token.
    #[tokio::test]
    async fn list_tag_entries_unknown_namespace_returns_empty() {
        let test_case = crate::registry::test_utils::FSRegistryTestCase::new();
        let registry = test_case.registry();
        let unknown = Namespace::new("no-such-repo/no-such-image").unwrap();

        let (tags, token) = registry
            .list_tag_entries(&unknown, None, None)
            .await
            .unwrap();

        assert!(tags.is_empty(), "unknown namespace must have no tags");
        assert!(
            token.is_none(),
            "unknown namespace must have no continuation token"
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
                .put_manifest(
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
                .put_manifest(
                    namespace,
                    &Reference::Digest(referrer_manifest_digest.clone()),
                    Some(&media_type),
                    manifest_content.as_bytes(),
                )
                .await
                .unwrap();

            let referrer_link = LinkKind::Referrer(
                base_manifest_digest.clone(),
                referrer_manifest_digest.clone(),
            );
            registry
                .metadata_store
                .update_links(
                    namespace,
                    &[LinkOperation::create(
                        referrer_link,
                        referrer_manifest_digest.clone(),
                    )],
                )
                .await
                .unwrap();

            let referrers = registry
                .list_referrers(namespace, &base_manifest_digest, None)
                .await
                .unwrap();

            assert_eq!(referrers.len(), 1);
            assert_eq!(referrers[0].digest, referrer_manifest_digest);
        })
        .await;
    }

    // The referrer-resolution tests below run on the split-backend fixture:
    // manifest bodies live in the blob store only, so any resolution path
    // reading them through the metadata store fails here.

    // A 64-char lowercase hex string for a digest with no stored blob.
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

    /// Split-backend registry fixture plus one referrer manifest uploaded
    /// through the blob store; the returned digest addresses that blob.
    async fn split_case_with_blob(
        blob_artifact_type: Option<&str>,
    ) -> (FSRegistryTestCase, Digest) {
        let case = FSRegistryTestCase::with_split_backends();
        let digest = upload_blob(
            case.registry(),
            &referrer_namespace(),
            &manifest_bytes(blob_artifact_type),
        )
        .await;
        (case, digest)
    }

    /// Creates the referrer link `subject() -> manifest`, optionally carrying
    /// a cached descriptor.
    async fn create_referrer_link(
        m: &MetadataStore,
        manifest: &Digest,
        descriptor: Option<Descriptor>,
    ) {
        let ops = vec![LinkOperation::Create {
            link: LinkKind::Referrer(subject(), manifest.clone()),
            target: manifest.clone(),
            referrer: None,
            media_type: None,
            descriptor: descriptor.map(Box::new),
        }];
        m.update_links(&referrer_namespace(), &ops).await.unwrap();
    }

    #[tokio::test]
    async fn returns_cached_descriptor_when_no_filter() {
        // The blob is deliberately unparseable: the cached descriptor must
        // answer without any manifest read.
        let case = FSRegistryTestCase::with_split_backends();
        let registry = case.registry();
        let manifest_digest = upload_blob(registry, &referrer_namespace(), b"not json").await;
        let desc = descriptor_with(Some("application/vnd.foo"), &manifest_digest);
        create_referrer_link(
            &registry.metadata_store,
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
        let case = FSRegistryTestCase::with_split_backends();
        let registry = case.registry();
        let manifest_digest = upload_blob(registry, &referrer_namespace(), b"not json").await;
        let at = "application/vnd.foo".to_string();
        let desc = descriptor_with(Some(&at), &manifest_digest);
        create_referrer_link(
            &registry.metadata_store,
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
        // The stored manifest DOES match the filter: a wrong fall-through to
        // the blob would return Some, so this pins the cache-only decision.
        let (case, manifest_digest) = split_case_with_blob(Some("application/vnd.bar")).await;
        let registry = case.registry();
        let desc = descriptor_with(Some("application/vnd.foo"), &manifest_digest);
        create_referrer_link(&registry.metadata_store, &manifest_digest, Some(desc)).await;

        let filter = "application/vnd.bar".to_string();
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
        // Cached descriptor has artifact_type = None; filter is Some(...).
        // The filter cannot be evaluated from the cache entry alone, so the
        // manifest blob decides.
        let (case, manifest_digest) = split_case_with_blob(Some("application/vnd.foo")).await;
        let registry = case.registry();
        let desc = descriptor_with(None, &manifest_digest);
        create_referrer_link(&registry.metadata_store, &manifest_digest, Some(desc)).await;

        let at = "application/vnd.foo".to_string();
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
        create_referrer_link(&registry.metadata_store, &manifest_digest, None).await;

        let result = registry
            .resolve_referrer_descriptor(&referrer_namespace(), &subject(), manifest_digest, None)
            .await;
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn returns_blob_descriptor_when_blob_filter_matches() {
        let (case, manifest_digest) = split_case_with_blob(Some("application/vnd.foo")).await;
        let registry = case.registry();

        let at = "application/vnd.foo".to_string();
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

        let filter = "application/vnd.bar".to_string();
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
        let case = FSRegistryTestCase::with_split_backends();
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
        let case = FSRegistryTestCase::with_split_backends();
        let registry = case.registry();
        let manifest_digest = upload_blob(registry, &referrer_namespace(), b"not json").await;

        let result = registry
            .resolve_referrer_descriptor(&referrer_namespace(), &subject(), manifest_digest, None)
            .await;
        assert!(result.is_none());
    }

    // End-to-end pin of the cross-store contract: a referrer whose link
    // carries no cached descriptor resolves through the public listing even
    // when blob and metadata stores use separate roots.
    #[tokio::test]
    async fn list_referrers_resolves_manifests_on_split_backends() {
        let (case, manifest_digest) = split_case_with_blob(Some("application/vnd.foo")).await;
        let registry = case.registry();
        create_referrer_link(&registry.metadata_store, &manifest_digest, None).await;

        let referrers = registry
            .list_referrers(&referrer_namespace(), &subject(), None)
            .await
            .unwrap();
        assert_eq!(referrers.len(), 1);
        assert_eq!(referrers[0].digest, manifest_digest);
    }
}
