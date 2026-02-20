mod error;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
pub use error::Error;
use serde::{Deserialize, Serialize};

use crate::oci::{Descriptor, Digest};

mod config;
pub mod fs;
pub mod link_kind;
mod link_metadata;
mod lock;
pub mod s3;

pub use config::MetadataStoreConfig;
pub use link_metadata::LinkMetadata;
pub use lock::redis::LockConfig;

use crate::registry::metadata_store::link_kind::LinkKind;

#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BlobIndex {
    pub namespace: HashMap<String, HashSet<LinkKind>>,
}

#[derive(Debug, Clone)]
pub enum BlobIndexOperation {
    Insert(LinkKind),
    Remove(LinkKind),
}

#[derive(Debug, Clone)]
pub(crate) enum LinkOperation {
    Create {
        link: LinkKind,
        target: Digest,
        referrer: Option<Digest>,
    },
    Delete {
        link: LinkKind,
        referrer: Option<Digest>,
    },
}

pub struct Transaction {
    store: Arc<dyn MetadataStore + Send + Sync>,
    namespace: String,
    operations: Vec<LinkOperation>,
}

impl Transaction {
    pub fn create_link(&mut self, link: &LinkKind, target: &Digest) {
        self.operations.push(LinkOperation::Create {
            link: link.clone(),
            target: target.clone(),
            referrer: None,
        });
    }

    pub fn create_link_with_referrer(
        &mut self,
        link: &LinkKind,
        target: &Digest,
        referrer: &Digest,
    ) {
        self.operations.push(LinkOperation::Create {
            link: link.clone(),
            target: target.clone(),
            referrer: Some(referrer.clone()),
        });
    }

    pub fn delete_link(&mut self, link: &LinkKind) {
        self.operations.push(LinkOperation::Delete {
            link: link.clone(),
            referrer: None,
        });
    }

    pub fn delete_link_with_referrer(&mut self, link: &LinkKind, referrer: &Digest) {
        self.operations.push(LinkOperation::Delete {
            link: link.clone(),
            referrer: Some(referrer.clone()),
        });
    }

    pub async fn commit(self) -> Result<(), Error> {
        if self.operations.is_empty() {
            return Ok(());
        }
        self.store
            .update_links(&self.namespace, &self.operations)
            .await
    }
}

pub trait MetadataStoreExt {
    fn begin_transaction(&self, namespace: &str) -> Transaction;
}

impl MetadataStoreExt for Arc<dyn MetadataStore + Send + Sync> {
    fn begin_transaction(&self, namespace: &str) -> Transaction {
        Transaction {
            store: self.clone(),
            namespace: namespace.to_string(),
            operations: Vec::new(),
        }
    }
}

#[async_trait]
pub trait MetadataStore: Send + Sync {
    async fn list_namespaces(
        &self,
        n: u16,
        last: Option<String>,
    ) -> Result<(Vec<String>, Option<String>), Error>;

    async fn list_tags(
        &self,
        namespace: &str,
        n: u16,
        last: Option<String>,
    ) -> Result<(Vec<String>, Option<String>), Error>;

    async fn list_referrers(
        &self,
        namespace: &str,
        digest: &Digest,
        artifact_type: Option<String>,
    ) -> Result<Vec<Descriptor>, Error>;

    async fn has_referrers(&self, namespace: &str, subject: &Digest) -> Result<bool, Error>;

    async fn list_revisions(
        &self,
        namespace: &str,
        n: u16,
        continuation_token: Option<String>,
    ) -> Result<(Vec<Digest>, Option<String>), Error>;

    async fn count_manifests(&self, namespace: &str) -> Result<usize, Error>;

    async fn read_blob_index(&self, digest: &Digest) -> Result<BlobIndex, Error>;

    async fn update_blob_index(
        &self,
        namespace: &str,
        digest: &Digest,
        operation: BlobIndexOperation,
    ) -> Result<(), Error>;

    async fn read_link(
        &self,
        namespace: &str,
        link: &LinkKind,
        update_access_time: bool,
    ) -> Result<LinkMetadata, Error>;

    async fn update_links(
        &self,
        namespace: &str,
        operations: &[LinkOperation],
    ) -> Result<(), Error>;
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use chrono::{Duration, Utc};

    use crate::oci::{Descriptor, Digest, Namespace};
    use crate::registry::blob_store::BlobStore;
    use crate::registry::metadata_store::link_kind::LinkKind;
    use crate::registry::metadata_store::{MetadataStore, MetadataStoreExt};
    use crate::registry::tests::backends;

    async fn create_link(
        m: &Arc<dyn MetadataStore + Send + Sync>,
        namespace: &str,
        link: &LinkKind,
        digest: &Digest,
    ) {
        let mut tx = m.begin_transaction(namespace);
        tx.create_link(link, digest);
        tx.commit().await.unwrap();
    }

    async fn delete_link(
        m: &Arc<dyn MetadataStore + Send + Sync>,
        namespace: &str,
        link: &LinkKind,
    ) {
        let mut tx = m.begin_transaction(namespace);
        tx.delete_link(link);
        tx.commit().await.unwrap();
    }

    pub async fn test_datastore_list_namespaces(
        b: Arc<dyn BlobStore>,
        m: Arc<dyn MetadataStore + Send + Sync>,
    ) {
        let namespaces = ["repo1", "repo2", "repo3/nested"];
        let digest = b.create_blob(b"test blob content").await.unwrap();

        for namespace in &namespaces {
            let tag_link = LinkKind::Tag("latest".to_string());
            create_link(&m, namespace, &tag_link, &digest).await;
        }

        // Test listing all namespaces
        let (listed_namespaces, token) = m.list_namespaces(10, None).await.unwrap();
        assert_eq!(listed_namespaces, namespaces);
        assert!(token.is_none() || listed_namespaces.len() >= namespaces.len());

        // Test pagination (2 items per pages)
        let (page1, token1) = m.list_namespaces(2, None).await.unwrap();
        assert_eq!(page1, ["repo1", "repo2"]);
        assert!(token1.is_some());

        let (page2, token2) = m.list_namespaces(2, token1).await.unwrap();
        assert_eq!(page2, ["repo3/nested"]);
        assert!(token2.is_none());

        // Test pagination (1 item per pages)
        let (page1, token1) = m.list_namespaces(1, None).await.unwrap();
        assert_eq!(page1, ["repo1"]);
        assert!(token1.is_some());

        let (page2, token2) = m.list_namespaces(1, token1).await.unwrap();
        assert_eq!(page2, ["repo2"]);

        let (page3, token3) = m.list_namespaces(1, token2).await.unwrap();
        assert_eq!(page3, ["repo3/nested"]);
        assert!(token3.is_none());

        let mut all_namespaces = page1;
        all_namespaces.extend(page2);
        all_namespaces.extend(page3);

        assert_eq!(all_namespaces, namespaces);
    }

    pub async fn test_datastore_list_tags(
        b: Arc<dyn BlobStore>,
        m: Arc<dyn MetadataStore + Send + Sync>,
    ) {
        let namespace = &Namespace::new("test-repo").unwrap();
        let digest = b.create_blob(b"test blob content").await.unwrap();

        let tags = ["latest", "v1.0", "v2.0"];
        for tag in tags {
            let tag_link = LinkKind::Tag(tag.to_string());
            create_link(&m, namespace, &tag_link, &digest).await;
        }

        // Test listing all tags
        let (all_tags, token) = m.list_tags(namespace, 10, None).await.unwrap();
        assert_eq!(all_tags.len(), tags.len());
        for tag in tags {
            assert!(all_tags.contains(&tag.to_string()));
        }
        assert!(token.is_none());

        // Test pagination (2 items per page)
        let (page1, token1) = m.list_tags(namespace, 2, None).await.unwrap();
        assert_eq!(page1.len(), 2);
        assert!(token1.is_some());

        let (page2, token2) = m.list_tags(namespace, 2, token1).await.unwrap();
        assert_eq!(page2.len(), 1);
        assert!(token2.is_none());

        // Test pagination (1 item per page)
        let (page1, token1) = m.list_tags(namespace, 1, None).await.unwrap();
        assert_eq!(page1.len(), 1);
        assert!(token1.is_some());

        let (page2, token2) = m.list_tags(namespace, 1, token1).await.unwrap();
        assert_eq!(page2.len(), 1);
        assert!(token2.is_some());

        let (page3, token3) = m.list_tags(namespace, 1, token2).await.unwrap();
        assert_eq!(page3.len(), 1);
        assert!(token3.is_none());

        // Test tag deletion
        let delete_tag = "v1.0";
        let tag_link = LinkKind::Tag(delete_tag.to_string());
        delete_link(&m, namespace, &tag_link).await;

        let (tags_after_delete, _) = m.list_tags(namespace, 10, None).await.unwrap();
        assert_eq!(tags_after_delete.len(), tags.len() - 1);
        assert!(!tags_after_delete.contains(&delete_tag.to_string()));
    }

    pub async fn test_datastore_list_referrers(
        b: Arc<dyn BlobStore>,
        m: Arc<dyn MetadataStore + Send + Sync>,
    ) {
        let namespace = &Namespace::new("test-repo").unwrap();
        let base_digest = b.create_blob(b"base manifest content").await.unwrap();
        let base_link = LinkKind::Digest(base_digest.clone());

        create_link(&m, namespace, &base_link, &base_digest).await;

        // Create artifacts that reference the base manifest
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

        let referrer_digest = b.create_blob(referrer_content.as_bytes()).await.unwrap();
        let link = LinkKind::Digest(referrer_digest.clone());

        create_link(&m, namespace, &link, &referrer_digest).await;

        // Also add it to the referrers index
        let referrers_link = LinkKind::Referrer(base_digest.clone(), referrer_digest.clone());

        create_link(&m, namespace, &referrers_link, &referrer_digest).await;

        // Test listing referrers
        let referrers = m.list_referrers(namespace, &base_digest, None).await;

        let expected = vec![Descriptor {
            media_type: "application/vnd.oci.image.manifest.v1+json".to_string(),
            digest: referrer_digest,
            size: 754,
            annotations: HashMap::new(),
            artifact_type: Some("application/vnd.example.test-artifact".to_string()),
            platform: None,
        }];

        assert_eq!(Ok(expected), referrers);

        // Test with artifact type filter
        let filtered_referrers = m
            .list_referrers(
                namespace,
                &base_digest,
                Some("application/vnd.example.test-artifact".to_string()),
            )
            .await
            .unwrap();

        assert!(!filtered_referrers.is_empty());

        // Test with non-matching artifact type
        let non_matching_referrers = m
            .list_referrers(
                namespace,
                &base_digest,
                Some("application/vnd.non-existent".to_string()),
            )
            .await
            .unwrap();

        assert!(non_matching_referrers.is_empty());
    }

    pub async fn test_datastore_list_revisions(
        b: Arc<dyn BlobStore>,
        m: Arc<dyn MetadataStore + Send + Sync>,
    ) {
        let namespace = &Namespace::new("test-repo").unwrap();

        let manifest_contents = [
            b"manifest content 1".to_vec(),
            b"manifest content 2".to_vec(),
            b"manifest content 3".to_vec(),
        ];

        let mut digests = Vec::new();
        for content in &manifest_contents {
            let digest = b.create_blob(content).await.unwrap();
            digests.push(digest.clone());

            let digest_link = LinkKind::Digest(digest.clone());
            create_link(&m, namespace, &digest_link, &digest).await;
        }

        // Test listing all revisions
        let (revisions, token) = m.list_revisions(namespace, 10, None).await.unwrap();
        assert_eq!(revisions.len(), digests.len());
        assert!(token.is_none());
        for digest in &digests {
            assert!(revisions.contains(digest));
        }

        // Test pagination (2 items per page)
        let (page1, token1) = m.list_revisions(namespace, 2, None).await.unwrap();
        assert_eq!(page1.len(), 2);
        assert!(token1.is_some());

        let (page2, token2) = m.list_revisions(namespace, 2, token1).await.unwrap();
        assert_eq!(page2.len(), 1);
        assert!(token2.is_none());

        // Test basic pagination (1 item per page)
        let (page1, token1) = m.list_revisions(namespace, 1, None).await.unwrap();
        assert_eq!(page1.len(), 1);
        assert!(token1.is_some());

        let (page2, token2) = m.list_revisions(namespace, 1, token1).await.unwrap();
        assert_eq!(page2.len(), 1);
        assert!(token2.is_some());

        let (page3, token3) = m.list_revisions(namespace, 1, token2).await.unwrap();
        assert_eq!(page3.len(), 1);
        assert!(token3.is_none());
    }

    pub async fn test_datastore_link_operations(
        b: Arc<dyn BlobStore>,
        m: Arc<dyn MetadataStore + Send + Sync>,
    ) {
        let namespace = &Namespace::new("test-namespace").unwrap();
        let digest = b.create_blob(b"test blob content").await.unwrap();

        // Test creating and reading tag link
        let tag = "latest";
        let tag_link = LinkKind::Tag(tag.to_string());

        create_link(&m, namespace, &tag_link, &digest).await;

        let read_digest = m.read_link(namespace, &tag_link, false).await.unwrap();
        assert_eq!(read_digest.target, digest);

        // Test reading reference info
        let ref_info = m.read_link(namespace, &tag_link, false).await.unwrap();
        let created_at = ref_info.created_at.unwrap();
        assert!(Utc::now().signed_duration_since(created_at) < Duration::seconds(1));
    }

    #[tokio::test]
    async fn test_list_namespaces() {
        for test_case in backends() {
            test_datastore_list_namespaces(test_case.blob_store(), test_case.metadata_store())
                .await;
        }
    }

    #[tokio::test]
    async fn test_list_tags() {
        for test_case in backends() {
            test_datastore_list_tags(test_case.blob_store(), test_case.metadata_store()).await;
        }
    }

    #[tokio::test]
    async fn test_list_referrers() {
        for test_case in backends() {
            test_datastore_list_referrers(test_case.blob_store(), test_case.metadata_store()).await;
        }
    }

    #[tokio::test]
    async fn test_list_revisions() {
        for test_case in backends() {
            test_datastore_list_revisions(test_case.blob_store(), test_case.metadata_store()).await;
        }
    }

    #[tokio::test]
    async fn test_link_operations() {
        for test_case in backends() {
            test_datastore_link_operations(test_case.blob_store(), test_case.metadata_store())
                .await;
        }
    }

    pub async fn test_datastore_list_namespaces_deduplication(
        b: Arc<dyn BlobStore>,
        m: Arc<dyn MetadataStore + Send + Sync>,
    ) {
        let namespace = "dedup-repo";
        let digest = b.create_blob(b"dedup test content").await.unwrap();

        // Create multiple link types within the same namespace
        let tag_link = LinkKind::Tag("latest".to_string());
        let digest_link = LinkKind::Digest(digest.clone());
        let layer_link = LinkKind::Layer(digest.clone());
        let config_link = LinkKind::Config(digest.clone());

        create_link(&m, namespace, &tag_link, &digest).await;
        create_link(&m, namespace, &digest_link, &digest).await;
        create_link(&m, namespace, &layer_link, &digest).await;
        create_link(&m, namespace, &config_link, &digest).await;

        // The namespace should appear exactly once despite having multiple object types
        let (namespaces, _) = m.list_namespaces(10, None).await.unwrap();
        let count = namespaces.iter().filter(|n| *n == namespace).count();
        assert_eq!(
            count, 1,
            "Namespace '{namespace}' should appear exactly once but appeared {count} times"
        );
    }

    pub async fn test_datastore_list_namespaces_many_namespaces_pagination(
        b: Arc<dyn BlobStore>,
        m: Arc<dyn MetadataStore + Send + Sync>,
    ) {
        let digest = b.create_blob(b"pagination test content").await.unwrap();

        let namespace_names: Vec<String> = (0..10).map(|i| format!("ns-{i:02}")).collect();

        for ns in &namespace_names {
            let tag_link = LinkKind::Tag("latest".to_string());
            create_link(&m, ns, &tag_link, &digest).await;
        }

        // Paginate with page size 3, collecting all results
        let mut all_namespaces = Vec::new();
        let mut token: Option<String> = None;
        let mut page_count = 0;

        loop {
            let (page, next_token) = m.list_namespaces(3, token).await.unwrap();
            assert!(
                !page.is_empty(),
                "Page {page_count} should not be empty while paginating"
            );
            assert!(
                page.len() <= 3,
                "Page {page_count} returned {} items, expected at most 3",
                page.len()
            );
            all_namespaces.extend(page);
            page_count += 1;
            match next_token {
                Some(t) => token = Some(t),
                None => break,
            }
        }

        assert_eq!(
            all_namespaces, namespace_names,
            "All namespaces should be returned in sorted order across pages"
        );
        assert_eq!(
            page_count, 4,
            "Expected 4 pages (3+3+3+1) but got {page_count}"
        );
    }

    pub async fn test_datastore_list_namespaces_single_item_pages(
        b: Arc<dyn BlobStore>,
        m: Arc<dyn MetadataStore + Send + Sync>,
    ) {
        let digest = b.create_blob(b"single page test content").await.unwrap();

        let namespace_names: Vec<String> = (0..5).map(|i| format!("single-{i:02}")).collect();

        for ns in &namespace_names {
            let tag_link = LinkKind::Tag("v1".to_string());
            create_link(&m, ns, &tag_link, &digest).await;
        }

        // Paginate with page size 1
        let mut all_namespaces = Vec::new();
        let mut token: Option<String> = None;

        for (i, expected_name) in namespace_names.iter().enumerate() {
            let (page, next_token) = m.list_namespaces(1, token).await.unwrap();
            assert_eq!(
                page.len(),
                1,
                "Page {i} should have exactly 1 item but had {}",
                page.len()
            );
            assert_eq!(
                page[0], *expected_name,
                "Page {i} should contain '{expected_name}' but contained '{}'",
                page[0]
            );
            all_namespaces.extend(page);
            token = next_token;
        }

        // After exhausting all items, token should be None
        assert!(
            token.is_none(),
            "Token should be None after all namespaces are consumed"
        );
        assert_eq!(all_namespaces, namespace_names);
    }

    #[tokio::test]
    async fn test_list_namespaces_deduplication() {
        for test_case in backends() {
            test_datastore_list_namespaces_deduplication(
                test_case.blob_store(),
                test_case.metadata_store(),
            )
            .await;
        }
    }

    #[tokio::test]
    async fn test_list_namespaces_many_namespaces_pagination() {
        for test_case in backends() {
            test_datastore_list_namespaces_many_namespaces_pagination(
                test_case.blob_store(),
                test_case.metadata_store(),
            )
            .await;
        }
    }

    #[tokio::test]
    async fn test_list_namespaces_single_item_pages() {
        for test_case in backends() {
            test_datastore_list_namespaces_single_item_pages(
                test_case.blob_store(),
                test_case.metadata_store(),
            )
            .await;
        }
    }

    pub async fn test_update_links(b: Arc<dyn BlobStore>, m: Arc<dyn MetadataStore + Send + Sync>) {
        let namespace = &Namespace::new("test-update-links").unwrap();
        let digest1 = b.create_blob(b"content1").await.unwrap();
        let digest2 = b.create_blob(b"content2").await.unwrap();

        let tag1 = LinkKind::Tag("v1".to_string());
        let tag2 = LinkKind::Tag("v2".to_string());

        let mut tx = m.begin_transaction(namespace);
        tx.create_link(&tag1, &digest1);
        tx.create_link(&tag2, &digest2);
        tx.commit().await.unwrap();

        let meta1 = m.read_link(namespace, &tag1, false).await.unwrap();
        assert_eq!(meta1.target, digest1);
        let meta2 = m.read_link(namespace, &tag2, false).await.unwrap();
        assert_eq!(meta2.target, digest2);

        let mut tx = m.begin_transaction(namespace);
        tx.delete_link(&tag1);
        tx.delete_link(&tag2);
        tx.commit().await.unwrap();

        assert!(m.read_link(namespace, &tag1, false).await.is_err());
        assert!(m.read_link(namespace, &tag2, false).await.is_err());
    }

    #[tokio::test]
    async fn test_update_links_batched() {
        for test_case in backends() {
            test_update_links(test_case.blob_store(), test_case.metadata_store()).await;
        }
    }
}
