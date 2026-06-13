use serde::Serialize;
use tracing::instrument;

use hyper::header::LINK;

use crate::{
    oci::{Descriptor, Digest, Namespace, OCI_INDEX_MEDIA_TYPE, OCI_MANIFEST_SCHEMA_VERSION},
    registry::{APPLICATION_JSON, Error, HeaderMap, JsonResponse, Registry, ResponseHeaders},
};

const OCI_FILTERS_APPLIED: &str = "OCI-Filters-Applied";

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
struct ReferrerList {
    schema_version: i32,
    media_type: String,
    manifests: Vec<Descriptor>,
}

impl Default for ReferrerList {
    fn default() -> Self {
        ReferrerList {
            schema_version: OCI_MANIFEST_SCHEMA_VERSION,
            media_type: OCI_INDEX_MEDIA_TYPE.to_string(),
            manifests: Vec::new(),
        }
    }
}

#[derive(Serialize)]
struct CatalogBody {
    repositories: Vec<String>,
}

#[derive(Serialize)]
struct TagsBody<'a> {
    name: &'a str,
    tags: Vec<String>,
}

fn referrers_headers(artifact_type_filtered: bool) -> HeaderMap {
    let headers = ResponseHeaders::new().content_type(OCI_INDEX_MEDIA_TYPE);
    if artifact_type_filtered {
        headers
            .with(OCI_FILTERS_APPLIED, "artifactType")
            .into_inner()
    } else {
        headers.into_inner()
    }
}

fn paginated_json_headers(link: Option<&str>) -> HeaderMap {
    let headers = ResponseHeaders::new().content_type(APPLICATION_JSON);
    match link {
        Some(link) => headers
            .with(LINK.as_str(), format!("<{link}>; rel=\"next\""))
            .into_inner(),
        None => headers.into_inner(),
    }
}

impl Registry {
    pub(crate) async fn list_catalog_entries(
        &self,
        n: Option<u16>,
        last: Option<String>,
    ) -> Result<(Vec<String>, Option<String>), Error> {
        let n = n.unwrap_or(100);

        let (namespaces, next_last) = self.metadata_store.list_namespaces(n, last).await?;
        let link = next_last.map(|next_last| format!("/v2/_catalog?n={n}&last={next_last}"));

        Ok((namespaces, link))
    }

    pub(crate) async fn list_tag_entries(
        &self,
        namespace: &Namespace,
        n: Option<u16>,
        last: Option<String>,
    ) -> Result<(Vec<String>, Option<String>), Error> {
        let n = n.unwrap_or(100);

        let (tags, next_last) = self.metadata_store.list_tags(namespace, n, last).await?;
        let link =
            next_last.map(|next_last| format!("/v2/{namespace}/tags/list?n={n}&last={next_last}"));

        Ok((tags, link))
    }

    #[instrument]
    pub async fn get_referrers(
        &self,
        namespace: &Namespace,
        digest: &Digest,
        artifact_type: Option<String>,
    ) -> Result<JsonResponse, Error> {
        let filtered = artifact_type.is_some();
        let manifests = self
            .metadata_store
            .list_referrers(namespace, digest, artifact_type)
            .await?;
        let referrer_list = ReferrerList {
            manifests,
            ..ReferrerList::default()
        };

        Ok(JsonResponse {
            headers: referrers_headers(filtered),
            body: serde_json::to_vec(&referrer_list)?,
        })
    }

    #[instrument]
    pub async fn list_catalog(
        &self,
        n: Option<u16>,
        last: Option<String>,
    ) -> Result<JsonResponse, Error> {
        let (repositories, link) = self.list_catalog_entries(n, last).await?;

        Ok(JsonResponse {
            headers: paginated_json_headers(link.as_deref()),
            body: serde_json::to_vec(&CatalogBody { repositories })?,
        })
    }

    #[instrument]
    pub async fn list_tags(
        &self,
        namespace: &Namespace,
        n: Option<u16>,
        last: Option<String>,
    ) -> Result<JsonResponse, Error> {
        let (tags, link) = self.list_tag_entries(namespace, n, last).await?;

        Ok(JsonResponse {
            headers: paginated_json_headers(link.as_deref()),
            body: serde_json::to_vec(&TagsBody {
                name: namespace.as_ref(),
                tags,
            })?,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        oci::{Namespace, Reference},
        registry::{
            metadata_store::{LinkOperation, link_kind::LinkKind},
            test_utils::{backends, create_test_blob, put_blob_direct},
        },
    };

    #[tokio::test]
    async fn test_list_catalog_entries() {
        for test_case in backends() {
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
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn test_list_tag_entries() {
        for test_case in backends() {
            let registry = test_case.registry();
            let namespace = &Namespace::new("test-repo").unwrap();

            let test_content = b"test content";
            let test_digest = put_blob_direct(registry.metadata_store.store(), test_content).await;
            let tags = ["latest", "v1.0", "v2.0"];
            let ops: Vec<LinkOperation> = tags
                .iter()
                .map(|&tag| {
                    LinkOperation::create(LinkKind::Tag(tag.to_string()), test_digest.clone())
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
            assert!(tags.contains(&"latest".to_string()));
            assert!(tags.contains(&"v1.0".to_string()));
            assert!(tags.contains(&"v2.0".to_string()));
            assert!(token.is_none());

            let (page1, token1) = registry
                .list_tag_entries(namespace, Some(2), None)
                .await
                .unwrap();
            assert_eq!(page1.len(), 2);
            assert!(token1.is_some());

            let last_tag = token1.unwrap().split("last=").nth(1).unwrap().to_string();

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

            let last_tag = token1.unwrap().split("last=").nth(1).unwrap().to_string();

            let (page2, token2) = registry
                .list_tag_entries(namespace, Some(1), Some(last_tag))
                .await
                .unwrap();
            assert_eq!(page2.len(), 1);
            assert!(token2.is_some());

            let last_tag = token2.unwrap().split("last=").nth(1).unwrap().to_string();

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
            test_case.cleanup().await;
        }
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
                        LinkKind::Tag("latest".to_string()),
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

            match token {
                None => break,
                Some(link) => {
                    // The link is a URL fragment; extract the `last=` parameter.
                    last = link.split("last=").nth(1).map(ToString::to_string);
                }
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
        for test_case in backends() {
            let registry = test_case.registry();
            let namespace = &Namespace::new("test-repo").unwrap();

            let manifest_content = r#"{"schemaVersion": 2, "mediaType": "application/vnd.docker.distribution.manifest.v2+json"}"#;
            let media_type = "application/vnd.docker.distribution.manifest.v2+json".to_string();

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
                .metadata_store
                .list_referrers(namespace, &base_manifest_digest, None)
                .await
                .unwrap();

            assert_eq!(referrers.len(), 1);
            assert_eq!(referrers[0].digest, referrer_manifest_digest);
            test_case.cleanup().await;
        }
    }
}
