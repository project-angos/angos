use std::fmt::Debug;

use hyper::{
    Response, StatusCode,
    header::{CONTENT_TYPE, LINK},
};
use serde::Serialize;
use tracing::instrument;

use crate::{
    command::server::response_body::ResponseBody,
    oci::{Descriptor, Digest, Namespace, ReferrerList},
    registry::{Error, Registry},
};

pub const OCI_FILTERS_APPLIED: &str = "OCI-Filters-Applied";

fn paginated_response(
    body: ResponseBody,
    link: Option<&str>,
) -> Result<Response<ResponseBody>, Error> {
    let res = match link {
        Some(link) => Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, "application/json")
            .header(LINK, format!("<{link}>; rel=\"next\""))
            .body(body)?,
        None => Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, "application/json")
            .body(body)?,
    };

    Ok(res)
}

impl Registry {
    #[instrument]
    pub async fn get_referrers(
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

    #[instrument]
    pub async fn list_catalog(
        &self,
        n: Option<u16>,
        last: Option<String>,
    ) -> Result<(Vec<String>, Option<String>), Error> {
        let n = n.unwrap_or(100);

        let (namespaces, next_last) = self.metadata_store.list_namespaces(n, last).await?;
        let link = next_last.map(|next_last| format!("/v2/_catalog?n={n}&last={next_last}"));

        Ok((namespaces, link))
    }

    #[instrument]
    pub async fn list_tags(
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

    // API Handlers
    #[instrument(skip(self))]
    pub async fn handle_get_referrers(
        &self,
        namespace: &Namespace,
        digest: &Digest,
        artifact_type: Option<String>,
    ) -> Result<Response<ResponseBody>, Error> {
        let query_supplied_artifact_type = artifact_type.is_some();

        let manifests = self.get_referrers(namespace, digest, artifact_type).await?;

        let referrer_list = ReferrerList {
            manifests,
            ..ReferrerList::default()
        };
        let referrer_list = serde_json::to_string(&referrer_list)?.into_bytes();

        let res = if query_supplied_artifact_type {
            Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, "application/vnd.oci.image.index.v1+json")
                .header(OCI_FILTERS_APPLIED, "artifactType")
                .body(ResponseBody::fixed(referrer_list))?
        } else {
            Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, "application/vnd.oci.image.index.v1+json")
                .body(ResponseBody::fixed(referrer_list))?
        };

        Ok(res)
    }

    #[instrument(skip(self))]
    pub async fn handle_list_catalog(
        &self,
        n: Option<u16>,
        last: Option<String>,
    ) -> Result<Response<ResponseBody>, Error> {
        #[derive(Serialize, Debug)]
        struct CatalogResponse {
            repositories: Vec<String>,
        }

        let (repositories, link) = self.list_catalog(n, last).await?;

        let catalog = CatalogResponse { repositories };
        let catalog = serde_json::to_string(&catalog)?;

        paginated_response(ResponseBody::fixed(catalog.into_bytes()), link.as_deref())
    }

    #[instrument(skip(self))]
    pub async fn handle_list_tags(
        &self,
        namespace: &Namespace,
        n: Option<u16>,
        last: Option<String>,
    ) -> Result<Response<ResponseBody>, Error> {
        #[derive(Serialize, Debug)]
        struct TagsResponse<'a> {
            name: &'a str,
            tags: Vec<String>,
        }

        let (tags, link) = self.list_tags(namespace, n, last).await?;

        let tag_list = TagsResponse {
            name: namespace,
            tags,
        };
        let tag_list = serde_json::to_string(&tag_list)?;
        paginated_response(ResponseBody::fixed(tag_list.into_bytes()), link.as_deref())
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use futures_util::TryStreamExt;
    use http_body_util::BodyExt;
    use tokio::io::AsyncReadExt;
    use tokio_util::io::StreamReader;

    use super::*;
    use crate::{
        oci::{Namespace, Reference},
        registry::{
            metadata_store::{MetadataStoreExt, link_kind::LinkKind},
            test_utils::create_test_blob,
            tests::backends,
        },
    };

    #[tokio::test]
    async fn test_get_referrers() {
        for test_case in backends() {
            let registry = test_case.registry();
            let namespace = &Namespace::new("test-repo").unwrap();
            let digest = Digest::from_str(
                "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            )
            .unwrap();

            let test_content = b"test content";
            let test_digest = registry.blob_store.create_blob(test_content).await.unwrap();
            let tag_link = LinkKind::Tag("latest".to_string());
            let mut tx = registry.metadata_store.begin_transaction(namespace);
            tx.create_link(&tag_link, &test_digest);
            tx.commit().await.unwrap();

            let referrers = registry
                .get_referrers(namespace, &digest, None)
                .await
                .unwrap();
            assert!(referrers.is_empty());

            let referrers = registry
                .get_referrers(namespace, &digest, Some("test-type".to_string()))
                .await
                .unwrap();
            assert!(referrers.is_empty());
        }
    }

    #[tokio::test]
    async fn test_list_catalog() {
        for test_case in backends() {
            let registry = test_case.registry();

            let (namespaces, token) = registry.list_catalog(None, None).await.unwrap();
            assert!(namespaces.is_empty());
            assert!(token.is_none());

            let (namespaces, token) = registry.list_catalog(Some(10), None).await.unwrap();
            assert!(namespaces.is_empty());
            assert!(token.is_none());

            let (namespaces, token) = registry
                .list_catalog(Some(10), Some("test".to_string()))
                .await
                .unwrap();
            assert!(namespaces.is_empty());
            assert!(token.is_none());
        }
    }

    #[tokio::test]
    async fn test_list_tags() {
        for test_case in backends() {
            let registry = test_case.registry();
            let namespace = &Namespace::new("test-repo").unwrap();

            let test_content = b"test content";
            let test_digest = registry.blob_store.create_blob(test_content).await.unwrap();
            let tags = ["latest", "v1.0", "v2.0"];
            let mut tx = registry.metadata_store.begin_transaction(namespace);
            for tag in tags {
                tx.create_link(&LinkKind::Tag(tag.to_string()), &test_digest);
            }
            tx.commit().await.unwrap();

            let (tags, token) = registry.list_tags(namespace, None, None).await.unwrap();
            assert_eq!(tags.len(), 3);
            assert!(tags.contains(&"latest".to_string()));
            assert!(tags.contains(&"v1.0".to_string()));
            assert!(tags.contains(&"v2.0".to_string()));
            assert!(token.is_none());

            let (page1, token1) = registry.list_tags(namespace, Some(2), None).await.unwrap();
            assert_eq!(page1.len(), 2);
            assert!(token1.is_some());

            let last_tag = token1.unwrap().split("last=").nth(1).unwrap().to_string();

            let (page2, token2) = registry
                .list_tags(namespace, Some(2), Some(last_tag))
                .await
                .unwrap();
            assert_eq!(page2.len(), 1);
            assert!(token2.is_none());

            let (page1, token1) = registry.list_tags(namespace, Some(1), None).await.unwrap();
            assert_eq!(page1.len(), 1);
            assert!(token1.is_some());

            let last_tag = token1.unwrap().split("last=").nth(1).unwrap().to_string();

            let (page2, token2) = registry
                .list_tags(namespace, Some(1), Some(last_tag))
                .await
                .unwrap();
            assert_eq!(page2.len(), 1);
            assert!(token2.is_some());

            let last_tag = token2.unwrap().split("last=").nth(1).unwrap().to_string();

            let (page3, token3) = registry
                .list_tags(namespace, Some(1), Some(last_tag))
                .await
                .unwrap();
            assert_eq!(page3.len(), 1);
            assert!(token3.is_none());

            let (tags, token) = registry
                .list_tags(namespace, Some(10), Some("latest".to_string()))
                .await
                .unwrap();
            assert_eq!(tags.len(), 2);
            assert!(token.is_none());
        }
    }

    #[tokio::test]
    async fn test_handle_get_referrers() {
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
            tx.create_link(&referrer_link, &referrer_manifest_digest);
            tx.commit().await.unwrap();

            let response = registry
                .handle_get_referrers(namespace, &base_manifest_digest, None)
                .await
                .unwrap();

            assert_eq!(response.status(), StatusCode::OK);
            let stream = response.into_data_stream().map_err(std::io::Error::other);
            let mut reader = StreamReader::new(stream);
            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).await.unwrap();
            let referrers: serde_json::Value = serde_json::from_slice(&buf).unwrap();
            let manifests = referrers["manifests"].as_array().unwrap();
            assert_eq!(manifests.len(), 1);
            assert_eq!(
                manifests[0]["digest"].as_str().unwrap(),
                referrer_manifest_digest.to_string()
            );
        }
    }

    #[tokio::test]
    async fn test_handle_list_catalog() {
        for test_case in backends() {
            let registry = test_case.registry();
            let namespaces = ["repo1", "repo2", "repo3"];
            let content = b"test content";

            for namespace in &namespaces {
                let (digest, _) = create_test_blob(registry, namespace, content).await;
                let tag_link = LinkKind::Tag("latest".to_string());
                let mut tx = registry.metadata_store.begin_transaction(namespace);
                tx.create_link(&tag_link, &digest);
                tx.commit().await.unwrap();
            }

            let response = registry.handle_list_catalog(None, None).await.unwrap();

            assert_eq!(response.status(), StatusCode::OK);
            let has_link = response.headers().get("Link").is_some();
            let stream = response.into_data_stream().map_err(std::io::Error::other);
            let mut reader = StreamReader::new(stream);
            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).await.unwrap();
            let catalog: serde_json::Value = serde_json::from_slice(&buf).unwrap();
            let repositories = catalog["repositories"].as_array().unwrap();
            assert_eq!(repositories.len(), namespaces.len());
            for namespace in &namespaces {
                assert!(
                    repositories
                        .iter()
                        .any(|r| r.as_str().unwrap() == *namespace)
                );
            }
            assert!(!has_link);

            let response = registry.handle_list_catalog(Some(2), None).await.unwrap();

            assert_eq!(response.status(), StatusCode::OK);
            let has_link = response.headers().get("Link").is_some();
            let stream = response.into_data_stream().map_err(std::io::Error::other);
            let mut reader = StreamReader::new(stream);
            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).await.unwrap();
            let catalog: serde_json::Value = serde_json::from_slice(&buf).unwrap();
            let repositories = catalog["repositories"].as_array().unwrap();
            assert_eq!(repositories.len(), 2);
            assert!(has_link);
        }
    }

    #[tokio::test]
    async fn test_handle_list_tags() {
        for test_case in backends() {
            let registry = test_case.registry();
            let namespace = &Namespace::new("test-repo").unwrap();
            let content = b"test content";
            let (digest, _) = create_test_blob(registry, namespace, content).await;

            let tags = ["v1", "v2", "latest"];
            let mut tx = registry.metadata_store.begin_transaction(namespace);
            for tag in tags {
                tx.create_link(&LinkKind::Tag(tag.to_string()), &digest);
            }
            tx.commit().await.unwrap();

            let response = registry
                .handle_list_tags(namespace, None, None)
                .await
                .unwrap();

            assert_eq!(response.status(), StatusCode::OK);
            let has_link = response.headers().get("Link").is_some();
            let stream = response.into_data_stream().map_err(std::io::Error::other);
            let mut reader = StreamReader::new(stream);
            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).await.unwrap();

            let tag_list: serde_json::Value = serde_json::from_slice(&buf).unwrap();

            let name = tag_list["name"].as_str().unwrap();
            let returned_tags = tag_list["tags"].as_array().unwrap();

            assert_eq!(name, namespace);
            assert_eq!(returned_tags.len(), tags.len());

            for tag in &tags {
                assert!(returned_tags.iter().any(|t| t.as_str().unwrap() == *tag));
            }
            assert!(!has_link);

            let response = registry
                .handle_list_tags(namespace, Some(2), None)
                .await
                .unwrap();

            assert_eq!(response.status(), StatusCode::OK);
            let has_link = response.headers().get("Link").is_some();
            let stream = response.into_data_stream().map_err(std::io::Error::other);
            let mut reader = StreamReader::new(stream);
            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).await.unwrap();
            let tag_list: serde_json::Value = serde_json::from_slice(&buf).unwrap();

            let name = tag_list["name"].as_str().unwrap();
            let tags = tag_list["tags"].as_array().unwrap();

            assert_eq!(name, namespace);
            assert_eq!(tags.len(), 2);
            assert!(has_link);
        }
    }
}
