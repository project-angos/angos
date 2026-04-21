use std::collections::HashMap;

use hyper::header::{CONTENT_TYPE, LINK};
use serde::Serialize;
use tracing::instrument;

use crate::{
    oci::{Descriptor, Digest, Namespace, ReferrerList},
    registry::{Error, JsonResponse, Registry},
};

const OCI_FILTERS_APPLIED: &str = "OCI-Filters-Applied";
const OCI_INDEX_MEDIA_TYPE: &str = "application/vnd.oci.image.index.v1+json";
const APPLICATION_JSON: &str = "application/json";

fn referrers_headers(artifact_type_filtered: bool) -> HashMap<&'static str, String> {
    let mut headers = HashMap::from([(CONTENT_TYPE.as_str(), OCI_INDEX_MEDIA_TYPE.to_string())]);
    if artifact_type_filtered {
        headers.insert(OCI_FILTERS_APPLIED, "artifactType".to_string());
    }
    headers
}

fn paginated_json_headers(link: Option<&str>) -> HashMap<&'static str, String> {
    let mut headers = HashMap::from([(CONTENT_TYPE.as_str(), APPLICATION_JSON.to_string())]);
    if let Some(link) = link {
        headers.insert(LINK.as_str(), format!("<{link}>; rel=\"next\""));
    }
    headers
}

impl Registry {
    pub(crate) async fn list_referrers(
        &self,
        namespace: &Namespace,
        digest: &Digest,
        artifact_type: Option<String>,
    ) -> Result<Vec<Descriptor>, Error> {
        Ok(self
            .metadata_store
            .list_referrers(namespace, digest, artifact_type)
            .await?)
    }

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
        #[derive(Serialize)]
        struct CatalogBody {
            repositories: Vec<String>,
        }

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
        #[derive(Serialize)]
        struct TagsBody<'a> {
            name: &'a str,
            tags: Vec<String>,
        }

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
            metadata_store::{MetadataStoreExt, link_kind::LinkKind},
            test_utils::create_test_blob,
            tests::backends,
        },
    };

    #[tokio::test]
    async fn test_list_referrers() {
        for test_case in backends() {
            let registry = test_case.registry();
            let namespace = &Namespace::new("test-repo").unwrap();
            let digest = "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
                .parse()
                .unwrap();

            let test_content = b"test content";
            let test_digest = registry.blob_store.create_blob(test_content).await.unwrap();
            let tag_link = LinkKind::Tag("latest".to_string());
            let mut tx = registry.metadata_store.begin_transaction(namespace);
            tx.create_link(&tag_link, &test_digest).add();
            tx.commit().await.unwrap();

            let referrers = registry
                .list_referrers(namespace, &digest, None)
                .await
                .unwrap();
            assert!(referrers.is_empty());

            let referrers = registry
                .list_referrers(namespace, &digest, Some("test-type".to_string()))
                .await
                .unwrap();
            assert!(referrers.is_empty());
        }
    }

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
        }
    }

    #[tokio::test]
    async fn test_list_tag_entries() {
        for test_case in backends() {
            let registry = test_case.registry();
            let namespace = &Namespace::new("test-repo").unwrap();

            let test_content = b"test content";
            let test_digest = registry.blob_store.create_blob(test_content).await.unwrap();
            let tags = ["latest", "v1.0", "v2.0"];
            let mut tx = registry.metadata_store.begin_transaction(namespace);
            for tag in tags {
                tx.create_link(&LinkKind::Tag(tag.to_string()), &test_digest)
                    .add();
            }
            tx.commit().await.unwrap();

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
        }
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
            let mut tx = registry.metadata_store.begin_transaction(namespace);
            tx.create_link(&referrer_link, &referrer_manifest_digest)
                .add();
            tx.commit().await.unwrap();

            let referrers = registry
                .list_referrers(namespace, &base_manifest_digest, None)
                .await
                .unwrap();

            assert_eq!(referrers.len(), 1);
            assert_eq!(referrers[0].digest, referrer_manifest_digest);
        }
    }
}
