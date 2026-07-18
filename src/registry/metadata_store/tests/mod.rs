mod access_time;
mod blob_index;
mod cache;
mod list_namespaces;

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use bytes::Bytes;
use chrono::{Duration, Utc};
use futures_util::TryStreamExt;

use angos_s3_client::Backend as S3HttpBackend;
use angos_storage::{ConditionalStore, ObjectStore, s3::Backend as StorageS3Backend};
use angos_tx_engine::{lock::LockStrategy, store::Store};

use crate::{
    cache::Cache,
    cache::memory::Backend as CacheMemoryBackend,
    metrics_provider,
    oci::{Algorithm, Descriptor, Digest, MediaType, Namespace, Tag},
    registry::{
        Error,
        metadata_store::{LinkKind, LinkMetadata, LinkOperation, MetadataStore},
        path_builder,
        s3_connection::S3ConnectionConfig,
        test_utils::{for_each_backend, media_type, put_blob_direct, s3_test_connection},
    },
};

/// Configuration for a test S3 metadata backend.
#[derive(Clone)]
pub struct TestS3Config {
    pub connection: S3ConnectionConfig,
    pub lock_strategy: LockStrategy,
    pub link_cache_ttl: u64,
    pub access_time_debounce_secs: u64,
}

impl TestS3Config {
    pub fn to_backend(
        &self,
        conditional_operations: bool,
        cache: Option<Arc<Cache>>,
    ) -> Result<MetadataStore, Error> {
        let http = Arc::new(
            S3HttpBackend::new(&self.connection.to_client_config())
                .map_err(|e| Error::Internal(e.to_string()))?,
        );
        let raw_storage = Arc::new(StorageS3Backend::builder(http.clone()).build());
        let object_store: Arc<dyn ObjectStore> = raw_storage.clone();
        let cond_store: Option<Arc<dyn ConditionalStore>> =
            conditional_operations.then_some(raw_storage as Arc<dyn ConditionalStore>);

        let s3_lock_store: Option<Arc<dyn ConditionalStore>> = match &self.lock_strategy {
            LockStrategy::S3(cfg) => {
                let lock_http = S3HttpBackend::new(&self.connection.to_lock_client_config(cfg))
                    .map_err(|e| Error::Internal(e.to_string()))?;
                let lock_backend = StorageS3Backend::builder(Arc::new(lock_http)).build();
                Some(Arc::new(lock_backend))
            }
            _ => None,
        };

        let facade = Arc::new(
            Store::new(
                object_store,
                cond_store,
                self.lock_strategy.clone(),
                s3_lock_store,
            )
            .map_err(|e| Error::Internal(e.to_string()))?,
        );
        let mut builder = MetadataStore::builder(facade)
            .link_cache_ttl(self.link_cache_ttl)
            .access_time_debounce_secs(self.access_time_debounce_secs);

        if let Some(c) = cache {
            builder = builder.cache(c);
        }

        Ok(builder.build())
    }
}

pub fn test_config() -> TestS3Config {
    metrics_provider::init_for_tests();
    TestS3Config {
        connection: s3_test_connection(format!("test-backend-{}", uuid::Uuid::new_v4())),
        lock_strategy: LockStrategy::Memory,
        link_cache_ttl: 30,
        access_time_debounce_secs: 0,
    }
}

pub fn test_backend_with_cache(config: &TestS3Config) -> (MetadataStore, Arc<Cache>) {
    let cache = Arc::new(Cache::Memory(CacheMemoryBackend::new()));
    let backend = config.to_backend(true, Some(cache.clone())).unwrap();
    (backend, cache)
}

/// Debounce applies to lock-coordinated deployments only (CAS stamps access
/// times inline), so this builds without conditional operations.
pub fn test_backend_with_debounce(config: &TestS3Config, debounce_secs: u64) -> MetadataStore {
    let mut cfg = config.clone();
    cfg.access_time_debounce_secs = debounce_secs;
    cfg.to_backend(false, None).unwrap()
}

// Integration tests

async fn create_link(m: &Arc<MetadataStore>, namespace: &str, link: &LinkKind, digest: &Digest) {
    let namespace = Namespace::new(namespace).unwrap();
    m.update_links(
        &namespace,
        &[LinkOperation::create(link.clone(), digest.clone())],
    )
    .await
    .unwrap();
}

async fn delete_link(m: &Arc<MetadataStore>, namespace: &str, link: &LinkKind) {
    let namespace = Namespace::new(namespace).unwrap();
    m.update_links(&namespace, &[LinkOperation::delete(link.clone())])
        .await
        .unwrap();
}

pub async fn test_datastore_list_namespaces(m: Arc<MetadataStore>) {
    let namespaces = ["repo1", "repo2", "repo3/nested"];
    let digest = put_blob_direct(m.store(), b"test blob content").await;

    for namespace in &namespaces {
        let tag_link = LinkKind::Tag(Tag::new("latest").unwrap());
        create_link(&m, namespace, &tag_link, &digest).await;
    }

    let (listed_namespaces, _) = m.list_namespaces(10, None).await.unwrap();
    assert_eq!(listed_namespaces, namespaces);

    let (page1, token1) = m.list_namespaces(2, None).await.unwrap();
    assert_eq!(page1, ["repo1", "repo2"]);
    assert!(token1.is_some());

    let (page2, token2) = m.list_namespaces(2, token1).await.unwrap();
    assert_eq!(page2, ["repo3/nested"]);
    assert!(token2.is_none());

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

pub async fn test_datastore_list_tags(m: Arc<MetadataStore>) {
    let namespace = &Namespace::new("test-repo").unwrap();
    let digest = put_blob_direct(m.store(), b"test blob content").await;

    let tags = ["latest", "v1.0", "v2.0"];
    for tag in tags {
        let tag_link = LinkKind::Tag(Tag::new(tag).unwrap());
        create_link(&m, namespace, &tag_link, &digest).await;
    }

    let (all_tags, token) = m.list_tags(namespace, 10, None).await.unwrap();
    assert_eq!(all_tags.len(), tags.len());
    for tag in tags {
        assert!(all_tags.contains(&Tag::new(tag).unwrap()));
    }
    assert!(token.is_none());

    let (page1, token1) = m.list_tags(namespace, 2, None).await.unwrap();
    assert_eq!(page1.len(), 2);
    assert!(token1.is_some());

    let (page2, token2) = m.list_tags(namespace, 2, token1).await.unwrap();
    assert_eq!(page2.len(), 1);
    assert!(token2.is_none());

    let (page1, token1) = m.list_tags(namespace, 1, None).await.unwrap();
    assert_eq!(page1.len(), 1);
    assert!(token1.is_some());

    let (page2, token2) = m.list_tags(namespace, 1, token1).await.unwrap();
    assert_eq!(page2.len(), 1);
    assert!(token2.is_some());

    let (page3, token3) = m.list_tags(namespace, 1, token2).await.unwrap();
    assert_eq!(page3.len(), 1);
    assert!(token3.is_none());

    let delete_tag = "v1.0";
    let tag_link = LinkKind::Tag(Tag::new(delete_tag).unwrap());
    delete_link(&m, namespace, &tag_link).await;

    let (tags_after_delete, _) = m.list_tags(namespace, 10, None).await.unwrap();
    assert_eq!(tags_after_delete.len(), tags.len() - 1);
    assert!(!tags_after_delete.contains(&Tag::new(delete_tag).unwrap()));
}

pub async fn test_datastore_list_tag_names_includes_malformed(m: Arc<MetadataStore>) {
    let namespace = &Namespace::new("test-repo/raw-tags").unwrap();
    let digest = put_blob_direct(m.store(), b"raw tag test blob").await;

    // A valid tag via the normal link path.
    let valid = LinkKind::Tag(Tag::new("v1.0").unwrap());
    create_link(&m, namespace, &valid, &digest).await;

    // A directory whose name fails the tag grammar (leading '-'), planted by
    // writing a raw `current/link` object so validation is bypassed.
    m.store()
        .object_store()
        .put(
            &format!(
                "{}/current/link",
                path_builder::manifest_tag_dir(namespace, "-bad")
            ),
            Bytes::from_static(
                b"sha256:0000000000000000000000000000000000000000000000000000000000000000",
            ),
        )
        .await
        .unwrap();

    let (raw_names, _) = m.list_tag_names(namespace, 10, None).await.unwrap();
    assert!(raw_names.contains(&"-bad".to_string()));
    assert!(raw_names.contains(&"v1.0".to_string()));

    let (tags, _) = m.list_tags(namespace, 10, None).await.unwrap();
    assert!(tags.contains(&Tag::new("v1.0").unwrap()));
    assert!(
        !tags.iter().any(|t| &**t == "-bad"),
        "list_tags must filter out the malformed name"
    );
}

pub async fn test_datastore_delete_tag_directory_guards_unsafe_names(m: Arc<MetadataStore>) {
    let namespace = &Namespace::new("test-repo/guard-tags").unwrap();

    // Unsafe segment names must be refused without deleting anything.
    let unsafe_name = "a/b";
    assert!(
        matches!(
            m.delete_tag_directory(namespace, unsafe_name).await,
            Err(Error::Internal(_))
        ),
        "expected guard to reject {unsafe_name:?}"
    );

    // Positive case: a safe but grammar-invalid name (leading '-') is the normal
    // invalid-tag scrub target and must be deleted, proving no over-rejection.
    m.store()
        .object_store()
        .put(
            &format!(
                "{}/current/link",
                path_builder::manifest_tag_dir(namespace, "-bad")
            ),
            Bytes::from_static(
                b"sha256:0000000000000000000000000000000000000000000000000000000000000000",
            ),
        )
        .await
        .unwrap();

    let (before, _) = m.list_tag_names(namespace, 10, None).await.unwrap();
    assert!(before.contains(&"-bad".to_string()));

    m.delete_tag_directory(namespace, "-bad").await.unwrap();

    let (after, _) = m.list_tag_names(namespace, 10, None).await.unwrap();
    assert!(
        !after.contains(&"-bad".to_string()),
        "the '-bad' tag directory must be gone after delete"
    );
}

pub async fn test_datastore_list_referrers(m: Arc<MetadataStore>) {
    let namespace = &Namespace::new("test-repo").unwrap();
    let base_digest = put_blob_direct(m.store(), b"base manifest content").await;
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

    let referrer_digest = put_blob_direct(m.store(), referrer_content.as_bytes()).await;
    let link = LinkKind::Digest(referrer_digest.clone());

    create_link(&m, namespace, &link, &referrer_digest).await;

    // Also add it to the referrers index
    let referrers_link = LinkKind::Referrer(base_digest.clone(), referrer_digest.clone());

    create_link(&m, namespace, &referrers_link, &referrer_digest).await;

    let referrers = m.list_referrers(namespace, &base_digest, None).await;

    let expected = vec![Descriptor {
        media_type: media_type("application/vnd.oci.image.manifest.v1+json"),
        digest: referrer_digest,
        size: 694,
        annotations: HashMap::new(),
        artifact_type: Some(media_type("application/vnd.example.test-artifact")),
        platform: None,
    }];

    assert_eq!(referrers.unwrap(), expected);

    let filtered_referrers = m
        .list_referrers(
            namespace,
            &base_digest,
            Some("application/vnd.example.test-artifact".to_string()),
        )
        .await
        .unwrap();

    assert!(!filtered_referrers.is_empty());

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

pub async fn test_datastore_stream_revisions(m: Arc<MetadataStore>) {
    let namespace = &Namespace::new("test-repo").unwrap();

    let manifest_contents = [
        b"manifest content 1".to_vec(),
        b"manifest content 2".to_vec(),
        b"manifest content 3".to_vec(),
    ];

    let mut digests = Vec::new();
    for content in &manifest_contents {
        let digest = put_blob_direct(m.store(), content).await;
        digests.push(digest.clone());

        let digest_link = LinkKind::Digest(digest.clone());
        create_link(&m, namespace, &digest_link, &digest).await;
    }

    let revisions: Vec<Digest> = m.stream_revisions(namespace).try_collect().await.unwrap();
    assert_eq!(revisions.len(), digests.len());
    for digest in &digests {
        assert!(revisions.contains(digest));
    }
}

pub async fn test_datastore_stream_revisions_across_algorithms(m: Arc<MetadataStore>) {
    let namespace = &Namespace::new("multi-algo-repo").unwrap();

    // Two revisions per algorithm. sha256 and sha512 are stored under separate
    // prefixes; the stream must chain them in global sort order (every sha256
    // before every sha512), each exactly once.
    let mut expected = Vec::new();
    for content in [b"a".as_slice(), b"b".as_slice()] {
        for algorithm in [Algorithm::Sha256, Algorithm::Sha512] {
            let digest = Digest::from_bytes(algorithm, content);
            create_link(&m, namespace, &LinkKind::Digest(digest.clone()), &digest).await;
            expected.push(digest);
        }
    }
    expected.sort();

    let all: Vec<Digest> = m.stream_revisions(namespace).try_collect().await.unwrap();
    assert_eq!(all, expected);
}

pub async fn test_datastore_link_operations(m: Arc<MetadataStore>) {
    let namespace = &Namespace::new("test-namespace").unwrap();
    let digest = put_blob_direct(m.store(), b"test blob content").await;

    let tag = "latest";
    let tag_link = LinkKind::Tag(Tag::new(tag).unwrap());

    create_link(&m, namespace, &tag_link, &digest).await;

    let read_digest = m.read_link(namespace, &tag_link).await.unwrap();
    assert_eq!(read_digest.target, digest);

    let ref_info = m.read_link(namespace, &tag_link).await.unwrap();
    let created_at = ref_info.created_at.unwrap();
    assert!(Utc::now().signed_duration_since(created_at) < Duration::seconds(1));
}

#[tokio::test]
async fn test_list_namespaces() {
    for_each_backend(async |test_case| {
        test_datastore_list_namespaces(test_case.metadata_store()).await;
    })
    .await;
}

#[tokio::test]
async fn test_list_tags() {
    for_each_backend(async |test_case| {
        test_datastore_list_tags(test_case.metadata_store()).await;
    })
    .await;
}

#[tokio::test]
async fn test_list_tag_names_includes_malformed() {
    for_each_backend(async |test_case| {
        test_datastore_list_tag_names_includes_malformed(test_case.metadata_store()).await;
    })
    .await;
}

#[tokio::test]
async fn test_delete_tag_directory_guards_unsafe_names() {
    for_each_backend(async |test_case| {
        test_datastore_delete_tag_directory_guards_unsafe_names(test_case.metadata_store()).await;
    })
    .await;
}

#[tokio::test]
async fn test_list_referrers() {
    for_each_backend(async |test_case| {
        test_datastore_list_referrers(test_case.metadata_store()).await;
    })
    .await;
}

#[tokio::test]
async fn test_stream_revisions() {
    for_each_backend(async |test_case| {
        test_datastore_stream_revisions(test_case.metadata_store()).await;
    })
    .await;
}

#[tokio::test]
async fn test_stream_revisions_across_algorithms() {
    for_each_backend(async |test_case| {
        test_datastore_stream_revisions_across_algorithms(test_case.metadata_store()).await;
    })
    .await;
}

#[tokio::test]
async fn test_link_operations() {
    for_each_backend(async |test_case| {
        test_datastore_link_operations(test_case.metadata_store()).await;
    })
    .await;
}

pub async fn test_datastore_list_namespaces_deduplication(m: Arc<MetadataStore>) {
    let namespace = "dedup-repo";
    let digest = put_blob_direct(m.store(), b"dedup test content").await;

    // Create multiple link types within the same namespace
    let tag_link = LinkKind::Tag(Tag::new("latest").unwrap());
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

pub async fn test_datastore_list_namespaces_many_namespaces_pagination(m: Arc<MetadataStore>) {
    let digest = put_blob_direct(m.store(), b"pagination test content").await;

    let namespace_names: Vec<String> = (0..10).map(|i| format!("ns-{i:02}")).collect();

    for ns in &namespace_names {
        let tag_link = LinkKind::Tag(Tag::new("latest").unwrap());
        create_link(&m, ns, &tag_link, &digest).await;
    }

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

pub async fn test_datastore_list_namespaces_single_item_pages(m: Arc<MetadataStore>) {
    let digest = put_blob_direct(m.store(), b"single page test content").await;

    let namespace_names: Vec<String> = (0..5).map(|i| format!("single-{i:02}")).collect();

    for ns in &namespace_names {
        let tag_link = LinkKind::Tag(Tag::new("v1").unwrap());
        create_link(&m, ns, &tag_link, &digest).await;
    }

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

    assert!(
        token.is_none(),
        "Token should be None after all namespaces are consumed"
    );
    assert_eq!(all_namespaces, namespace_names);
}

#[tokio::test]
async fn test_list_namespaces_deduplication() {
    for_each_backend(async |test_case| {
        test_datastore_list_namespaces_deduplication(test_case.metadata_store()).await;
    })
    .await;
}

#[tokio::test]
async fn test_list_namespaces_many_namespaces_pagination() {
    for_each_backend(async |test_case| {
        test_datastore_list_namespaces_many_namespaces_pagination(test_case.metadata_store()).await;
    })
    .await;
}

#[tokio::test]
async fn test_list_namespaces_single_item_pages() {
    for_each_backend(async |test_case| {
        test_datastore_list_namespaces_single_item_pages(test_case.metadata_store()).await;
    })
    .await;
}

pub async fn test_datastore_list_tags_many_tags_pagination(m: Arc<MetadataStore>) {
    let namespace = &Namespace::new("test-repo-name").unwrap();
    let digest = put_blob_direct(m.store(), b"content").await;

    let tag_names: Vec<String> = (0..10).map(|i| format!("tag-{i:02}")).collect();

    for tag in &tag_names {
        create_link(
            &m,
            namespace,
            &LinkKind::Tag(Tag::new(tag).unwrap()),
            &digest,
        )
        .await;
    }

    let mut all_tags = Vec::new();
    let mut token: Option<String> = None;
    let mut page_count = 0;

    loop {
        let (page, next_token) = m.list_tags(namespace, 3, token).await.unwrap();
        assert!(
            !page.is_empty(),
            "Page {page_count} should not be empty while paginating"
        );
        assert!(
            page.len() <= 3,
            "Page {page_count} returned {} items, expected at most 3",
            page.len()
        );
        all_tags.extend(page);
        page_count += 1;
        match next_token {
            Some(t) => token = Some(t),
            None => break,
        }
    }

    let all_tag_names: Vec<&str> = all_tags.iter().map(Tag::as_ref).collect();
    assert_eq!(
        all_tag_names, tag_names,
        "All tags should be returned in sorted order across pages"
    );
    assert_eq!(
        page_count, 4,
        "Expected 4 pages (3+3+3+1) but got {page_count}"
    );
}

pub async fn test_datastore_list_tags_single_item_pages(m: Arc<MetadataStore>) {
    let namespace = &Namespace::new("test-repo-name").unwrap();
    let digest = put_blob_direct(m.store(), b"content").await;

    let tag_names: Vec<String> = (0..5).map(|i| format!("single-tag-{i:02}")).collect();

    for tag in &tag_names {
        create_link(
            &m,
            namespace,
            &LinkKind::Tag(Tag::new(tag).unwrap()),
            &digest,
        )
        .await;
    }

    let mut all_tags = Vec::new();
    let mut token: Option<String> = None;

    for (i, expected_name) in tag_names.iter().enumerate() {
        let (page, next_token) = m.list_tags(namespace, 1, token).await.unwrap();
        assert_eq!(
            page.len(),
            1,
            "Page {i} should have exactly 1 item but had {}",
            page.len()
        );
        assert_eq!(
            page[0].as_ref(),
            expected_name.as_str(),
            "Page {i} should contain '{expected_name}' but contained '{}'",
            page[0]
        );
        all_tags.extend(page);
        token = next_token;
    }

    assert!(
        token.is_none(),
        "Token should be None after all tags are consumed"
    );
    let all_tag_names: Vec<&str> = all_tags.iter().map(Tag::as_ref).collect();
    assert_eq!(all_tag_names, tag_names);
}

#[tokio::test]
async fn test_list_tags_many_tags_pagination() {
    for_each_backend(async |test_case| {
        test_datastore_list_tags_many_tags_pagination(test_case.metadata_store()).await;
    })
    .await;
}

#[tokio::test]
async fn test_list_tags_single_item_pages() {
    for_each_backend(async |test_case| {
        test_datastore_list_tags_single_item_pages(test_case.metadata_store()).await;
    })
    .await;
}

pub async fn test_update_links(m: Arc<MetadataStore>) {
    let namespace = &Namespace::new("test-update-links").unwrap();
    let digest1 = put_blob_direct(m.store(), b"content1").await;
    let digest2 = put_blob_direct(m.store(), b"content2").await;

    let tag1 = LinkKind::Tag(Tag::new("v1").unwrap());
    let tag2 = LinkKind::Tag(Tag::new("v2").unwrap());

    m.update_links(
        namespace,
        &[
            LinkOperation::create(tag1.clone(), digest1.clone()),
            LinkOperation::create(tag2.clone(), digest2.clone()),
        ],
    )
    .await
    .unwrap();

    let meta1 = m.read_link(namespace, &tag1).await.unwrap();
    assert_eq!(meta1.target, digest1);
    let meta2 = m.read_link(namespace, &tag2).await.unwrap();
    assert_eq!(meta2.target, digest2);

    m.update_links(
        namespace,
        &[
            LinkOperation::delete(tag1.clone()),
            LinkOperation::delete(tag2.clone()),
        ],
    )
    .await
    .unwrap();

    assert!(m.read_link(namespace, &tag1).await.is_err());
    assert!(m.read_link(namespace, &tag2).await.is_err());
}

#[tokio::test]
async fn test_update_links_batched() {
    for_each_backend(async |test_case| {
        test_update_links(test_case.metadata_store()).await;
    })
    .await;
}

pub async fn test_datastore_read_link_access_time_update(m: Arc<MetadataStore>) {
    let namespace = &Namespace::new("test-access-time").unwrap();
    let digest = put_blob_direct(m.store(), b"access time test content").await;

    let tag_link = LinkKind::Tag(Tag::new("latest").unwrap());
    create_link(&m, namespace, &tag_link, &digest).await;

    // A tracked read records accessed_at.
    let meta = m
        .read_link_recording_access(namespace, &tag_link)
        .await
        .unwrap();
    assert!(
        meta.accessed_at.is_some(),
        "accessed_at should be set after a recording read"
    );
    let accessed_at = meta.accessed_at.unwrap();
    assert!(
        Utc::now().signed_duration_since(accessed_at) < Duration::seconds(2),
        "accessed_at should be within 2 seconds of now"
    );

    // A plain read still returns the persisted accessed_at.
    let meta_readonly = m.read_link(namespace, &tag_link).await.unwrap();
    assert!(
        meta_readonly.accessed_at.is_some(),
        "accessed_at should still be persisted after read-only read"
    );
}

#[tokio::test]
async fn test_read_link_access_time_update() {
    for_each_backend(async |test_case| {
        test_datastore_read_link_access_time_update(test_case.metadata_store()).await;
    })
    .await;
}

pub async fn test_datastore_read_link_concurrent_readonly(m: Arc<MetadataStore>) {
    let namespace = &Namespace::new("test-concurrent-read").unwrap();
    let digest = put_blob_direct(m.store(), b"concurrent read test content").await;

    let tag_link = LinkKind::Tag(Tag::new("latest").unwrap());
    create_link(&m, namespace, &tag_link, &digest).await;

    let mut handles = Vec::new();
    for _ in 0..20 {
        let m = m.clone();
        let ns = namespace.clone();
        let link = tag_link.clone();
        handles.push(tokio::spawn(async move { m.read_link(&ns, &link).await }));
    }

    let results: Vec<_> = futures_util::future::join_all(handles).await;
    for result in results {
        let meta = result.unwrap().unwrap();
        assert_eq!(meta.target, digest);
    }
}

#[tokio::test]
async fn test_read_link_concurrent_readonly() {
    for_each_backend(async |test_case| {
        test_datastore_read_link_concurrent_readonly(test_case.metadata_store()).await;
    })
    .await;
}

pub async fn test_datastore_list_referrers_parallel_correctness(m: Arc<MetadataStore>) {
    let namespace = &Namespace::new("test-referrers-parallel").unwrap();
    let subject_digest = put_blob_direct(m.store(), b"subject manifest content").await;
    let subject_link = LinkKind::Digest(subject_digest.clone());
    create_link(&m, namespace, &subject_link, &subject_digest).await;

    let mut referrer_digests = Vec::new();
    for i in 0..5 {
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
                "annotations": {{ "index": "{i}" }}
            }}"#
        );

        let referrer_digest = put_blob_direct(m.store(), referrer_content.as_bytes()).await;
        let digest_link = LinkKind::Digest(referrer_digest.clone());
        create_link(&m, namespace, &digest_link, &referrer_digest).await;

        let referrer_link = LinkKind::Referrer(subject_digest.clone(), referrer_digest.clone());
        create_link(&m, namespace, &referrer_link, &referrer_digest).await;

        referrer_digests.push(referrer_digest);
    }

    let descriptors = m
        .list_referrers(namespace, &subject_digest, None)
        .await
        .unwrap();

    assert_eq!(
        descriptors.len(),
        5,
        "Expected 5 referrer descriptors but got {}",
        descriptors.len()
    );

    for pair in descriptors.windows(2) {
        assert!(
            pair[0].digest.to_string() <= pair[1].digest.to_string(),
            "Descriptors should be sorted by digest: {} should come before {}",
            pair[0].digest,
            pair[1].digest
        );
    }
}

pub async fn test_datastore_list_referrers_with_artifact_type_filter(m: Arc<MetadataStore>) {
    let namespace = &Namespace::new("test-referrers-filter").unwrap();
    let subject_digest = put_blob_direct(m.store(), b"subject manifest for filter test").await;
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

        let referrer_digest = put_blob_direct(m.store(), referrer_content.as_bytes()).await;
        let digest_link = LinkKind::Digest(referrer_digest.clone());
        create_link(&m, namespace, &digest_link, &referrer_digest).await;

        let referrer_link = LinkKind::Referrer(subject_digest.clone(), referrer_digest.clone());
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

        let referrer_digest = put_blob_direct(m.store(), referrer_content.as_bytes()).await;
        let digest_link = LinkKind::Digest(referrer_digest.clone());
        create_link(&m, namespace, &digest_link, &referrer_digest).await;

        let referrer_link = LinkKind::Referrer(subject_digest.clone(), referrer_digest.clone());
        create_link(&m, namespace, &referrer_link, &referrer_digest).await;
    }

    let descriptors = m
        .list_referrers(
            namespace,
            &subject_digest,
            Some("application/vnd.example.sbom".to_string()),
        )
        .await
        .unwrap();

    assert_eq!(
        descriptors.len(),
        3,
        "Expected 3 SBOM referrer descriptors but got {}",
        descriptors.len()
    );

    for desc in &descriptors {
        assert_eq!(
            desc.artifact_type.as_deref(),
            Some("application/vnd.example.sbom"),
            "All filtered descriptors should have SBOM artifact type"
        );
    }
}

pub async fn test_datastore_list_referrers_deterministic_order(m: Arc<MetadataStore>) {
    let namespace = &Namespace::new("test-referrers-order").unwrap();
    let subject_digest = put_blob_direct(m.store(), b"subject manifest for order test").await;
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

        let referrer_digest = put_blob_direct(m.store(), referrer_content.as_bytes()).await;
        let digest_link = LinkKind::Digest(referrer_digest.clone());
        create_link(&m, namespace, &digest_link, &referrer_digest).await;

        let referrer_link = LinkKind::Referrer(subject_digest.clone(), referrer_digest.clone());
        create_link(&m, namespace, &referrer_link, &referrer_digest).await;
    }

    let result1 = m
        .list_referrers(namespace, &subject_digest, None)
        .await
        .unwrap();
    let result2 = m
        .list_referrers(namespace, &subject_digest, None)
        .await
        .unwrap();
    let result3 = m
        .list_referrers(namespace, &subject_digest, None)
        .await
        .unwrap();

    assert_eq!(
        result1.len(),
        10,
        "Expected 10 referrer descriptors but got {}",
        result1.len()
    );
    assert_eq!(
        result1, result2,
        "First and second list_referrers calls should return identical results"
    );
    assert_eq!(
        result2, result3,
        "Second and third list_referrers calls should return identical results"
    );

    for pair in result1.windows(2) {
        assert!(
            pair[0].digest.to_string() <= pair[1].digest.to_string(),
            "Descriptors should be sorted by digest: {} should come before {}",
            pair[0].digest,
            pair[1].digest
        );
    }
}

#[tokio::test]
async fn test_list_referrers_parallel_correctness() {
    for_each_backend(async |test_case| {
        test_datastore_list_referrers_parallel_correctness(test_case.metadata_store()).await;
    })
    .await;
}

#[tokio::test]
async fn test_list_referrers_with_artifact_type_filter() {
    for_each_backend(async |test_case| {
        test_datastore_list_referrers_with_artifact_type_filter(test_case.metadata_store()).await;
    })
    .await;
}

#[tokio::test]
async fn test_list_referrers_deterministic_order() {
    for_each_backend(async |test_case| {
        test_datastore_list_referrers_deterministic_order(test_case.metadata_store()).await;
    })
    .await;
}

pub async fn test_datastore_parallel_multiple_creates(m: Arc<MetadataStore>) {
    let namespace = &Namespace::new("parallel-creates-ns").unwrap();

    let mut digests = Vec::new();
    for i in 0..5 {
        let digest = put_blob_direct(m.store(), format!("content-{i}").as_bytes()).await;
        digests.push(digest);
    }

    let ops: Vec<LinkOperation> = digests
        .iter()
        .enumerate()
        .map(|(i, digest)| {
            LinkOperation::create(
                LinkKind::Tag(Tag::try_from(format!("t{}", i + 1)).unwrap()),
                digest.clone(),
            )
        })
        .collect();
    m.update_links(namespace, &ops).await.unwrap();

    for (i, digest) in digests.iter().enumerate() {
        let tag = format!("t{}", i + 1);
        let meta = m
            .read_link(namespace, &LinkKind::Tag(Tag::new(&tag).unwrap()))
            .await
            .unwrap();
        assert_eq!(
            meta.target, *digest,
            "Tag {tag} should point to the correct digest"
        );

        let blob_index = m.read_blob_index(digest).await.unwrap();
        let links = blob_index.namespace.get(namespace);
        assert!(
            links.is_some(),
            "Blob index for digest {digest} should have an entry for namespace {namespace}"
        );
        assert!(
            links
                .unwrap()
                .contains(&LinkKind::Tag(Tag::new(&tag).unwrap())),
            "Blob index for digest {digest} should contain Tag({tag})"
        );
    }

    let (tags, _) = m.list_tags(namespace, 10, None).await.unwrap();
    assert_eq!(tags.len(), 5, "Should have 5 tags but got {}", tags.len());
    for i in 0..5 {
        let tag = format!("t{}", i + 1);
        assert!(
            tags.contains(&Tag::new(&tag).unwrap()),
            "Tags list should contain {tag}"
        );
    }
}

#[tokio::test]
async fn test_parallel_multiple_creates() {
    for_each_backend(async |test_case| {
        test_datastore_parallel_multiple_creates(test_case.metadata_store()).await;
    })
    .await;
}

pub async fn test_datastore_parallel_mixed_create_delete(m: Arc<MetadataStore>) {
    let namespace = &Namespace::new("parallel-mixed-ns").unwrap();

    let digest_a = put_blob_direct(m.store(), b"content-a").await;
    let digest_b = put_blob_direct(m.store(), b"content-b").await;
    let digest_c = put_blob_direct(m.store(), b"content-c").await;

    create_link(
        &m,
        namespace,
        &LinkKind::Tag(Tag::new("v1").unwrap()),
        &digest_a,
    )
    .await;
    create_link(
        &m,
        namespace,
        &LinkKind::Tag(Tag::new("v2").unwrap()),
        &digest_b,
    )
    .await;

    m.update_links(
        namespace,
        &[
            LinkOperation::delete(LinkKind::Tag(Tag::new("v1").unwrap())),
            LinkOperation::create(LinkKind::Tag(Tag::new("v3").unwrap()), digest_c.clone()),
        ],
    )
    .await
    .unwrap();

    let err = m
        .read_link(namespace, &LinkKind::Tag(Tag::new("v1").unwrap()))
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::NotFound),
        "Tag v1 should not exist after deletion but got error: {err:?}"
    );

    let meta_v2 = m
        .read_link(namespace, &LinkKind::Tag(Tag::new("v2").unwrap()))
        .await
        .unwrap();
    assert_eq!(
        meta_v2.target, digest_b,
        "Tag v2 should still point to digest_b"
    );

    let meta_v3 = m
        .read_link(namespace, &LinkKind::Tag(Tag::new("v3").unwrap()))
        .await
        .unwrap();
    assert_eq!(meta_v3.target, digest_c, "Tag v3 should point to digest_c");

    let tag_v1 = LinkKind::Tag(Tag::new("v1").unwrap());
    match m.read_blob_index(&digest_a).await {
        Ok(index_a) => {
            let links_a = index_a.namespace.get(namespace);
            assert!(
                links_a.is_none_or(|s| !s.contains(&tag_v1)),
                "Blob index for digest_a should not contain Tag(v1) after deletion"
            );
        }
        Err(Error::NotFound) => {}
        Err(e) => panic!("Unexpected error reading blob index for digest_a: {e:?}"),
    }

    let index_c = m.read_blob_index(&digest_c).await.unwrap();
    let links_c = index_c
        .namespace
        .get(namespace)
        .expect("Blob index for digest_c should have an entry for namespace");
    assert!(
        links_c.contains(&LinkKind::Tag(Tag::new("v3").unwrap())),
        "Blob index for digest_c should contain Tag(v3)"
    );
}

#[tokio::test]
async fn test_parallel_mixed_create_delete() {
    for_each_backend(async |test_case| {
        test_datastore_parallel_mixed_create_delete(test_case.metadata_store()).await;
    })
    .await;
}

pub async fn test_datastore_parallel_blob_index_correctness(m: Arc<MetadataStore>) {
    let namespace = &Namespace::new("parallel-blob-index-ns").unwrap();

    let mut digests = Vec::new();
    for i in 0..4 {
        let digest = put_blob_direct(m.store(), format!("content-{i}").as_bytes()).await;
        digests.push(digest);
    }

    let ops: Vec<LinkOperation> = digests
        .iter()
        .enumerate()
        .map(|(i, digest)| {
            LinkOperation::create(
                LinkKind::Tag(Tag::try_from(format!("tag-{i}")).unwrap()),
                digest.clone(),
            )
        })
        .collect();
    m.update_links(namespace, &ops).await.unwrap();

    for (i, digest) in digests.iter().enumerate() {
        let expected_tag = LinkKind::Tag(Tag::try_from(format!("tag-{i}")).unwrap());
        let blob_index = m.read_blob_index(digest).await.unwrap();

        let links = blob_index.namespace.get(namespace).unwrap_or_else(|| {
            panic!("Blob index for digest {digest} should have an entry for namespace {namespace}")
        });

        assert!(
            links.contains(&expected_tag),
            "Blob index for digest {digest} should contain tag-{i}"
        );

        for (j, other_digest) in digests.iter().enumerate() {
            if j != i {
                let other_tag = LinkKind::Tag(Tag::try_from(format!("tag-{j}")).unwrap());
                assert!(
                    !links.contains(&other_tag),
                    "Blob index for digest {digest} (tag-{i}) should NOT contain tag-{j}"
                );
                let other_index = m.read_blob_index(other_digest).await.unwrap();
                let other_links = other_index.namespace.get(namespace);
                assert!(
                    other_links.is_some_and(|s| !s.contains(&expected_tag)),
                    "Blob index for digest {other_digest} (tag-{j}) should NOT contain tag-{i}"
                );
            }
        }
    }
}

#[tokio::test]
async fn test_parallel_blob_index_correctness() {
    for_each_backend(async |test_case| {
        test_datastore_parallel_blob_index_correctness(test_case.metadata_store()).await;
    })
    .await;
}

pub async fn test_datastore_tracked_create_with_referrer(m: Arc<MetadataStore>) {
    let namespace = &Namespace::new("tracked-create-referrer-ns").unwrap();

    let digest_layer = put_blob_direct(m.store(), b"layer content").await;
    let digest_manifest = put_blob_direct(m.store(), b"manifest content").await;

    m.update_links(
        namespace,
        &[LinkOperation::create_with_referrer(
            LinkKind::Layer(digest_layer.clone()),
            digest_layer.clone(),
            digest_manifest.clone(),
        )],
    )
    .await
    .unwrap();

    let metadata = m
        .read_link(namespace, &LinkKind::Layer(digest_layer.clone()))
        .await
        .unwrap();
    assert_eq!(
        metadata.target, digest_layer,
        "Link target should be the layer digest"
    );
    assert!(
        metadata.referenced_by.contains(&digest_manifest),
        "referenced_by should contain the manifest digest"
    );

    let blob_index = m.read_blob_index(&digest_layer).await.unwrap();
    let links = blob_index
        .namespace
        .get(namespace)
        .expect("Blob index should have an entry for the namespace");
    assert!(
        links.contains(&LinkKind::Layer(digest_layer.clone())),
        "Blob index should contain the Layer link"
    );
}

#[tokio::test]
async fn test_tracked_create_with_referrer() {
    for_each_backend(async |test_case| {
        test_datastore_tracked_create_with_referrer(test_case.metadata_store()).await;
    })
    .await;
}

pub async fn test_datastore_tracked_delete_with_referrer(m: Arc<MetadataStore>) {
    let namespace = &Namespace::new("tracked-delete-referrer-ns").unwrap();

    let layer_digest = put_blob_direct(m.store(), b"layer for delete test").await;
    let first_manifest_digest = put_blob_direct(m.store(), b"manifest a content").await;
    let second_manifest_digest = put_blob_direct(m.store(), b"manifest b content").await;

    m.update_links(
        namespace,
        &[LinkOperation::create_with_referrer(
            LinkKind::Layer(layer_digest.clone()),
            layer_digest.clone(),
            first_manifest_digest.clone(),
        )],
    )
    .await
    .unwrap();

    m.update_links(
        namespace,
        &[LinkOperation::create_with_referrer(
            LinkKind::Layer(layer_digest.clone()),
            layer_digest.clone(),
            second_manifest_digest.clone(),
        )],
    )
    .await
    .unwrap();

    let metadata = m
        .read_link(namespace, &LinkKind::Layer(layer_digest.clone()))
        .await
        .unwrap();
    assert!(
        metadata.referenced_by.contains(&first_manifest_digest),
        "referenced_by should contain first manifest after both creates"
    );
    assert!(
        metadata.referenced_by.contains(&second_manifest_digest),
        "referenced_by should contain second manifest after both creates"
    );

    m.update_links(
        namespace,
        &[LinkOperation::delete_with_referrer(
            LinkKind::Layer(layer_digest.clone()),
            first_manifest_digest.clone(),
        )],
    )
    .await
    .unwrap();

    let metadata = m
        .read_link(namespace, &LinkKind::Layer(layer_digest.clone()))
        .await
        .unwrap();
    assert!(
        !metadata.referenced_by.contains(&first_manifest_digest),
        "referenced_by should not contain first manifest after deletion"
    );
    assert!(
        metadata.referenced_by.contains(&second_manifest_digest),
        "referenced_by should still contain second manifest"
    );

    let blob_index = m.read_blob_index(&layer_digest).await.unwrap();
    let links = blob_index
        .namespace
        .get(namespace)
        .expect("Blob index should still have an entry for the namespace");
    assert!(
        links.contains(&LinkKind::Layer(layer_digest.clone())),
        "Blob index should still contain the Layer link"
    );
}

#[tokio::test]
async fn test_tracked_delete_with_referrer() {
    for_each_backend(async |test_case| {
        test_datastore_tracked_delete_with_referrer(test_case.metadata_store()).await;
    })
    .await;
}

pub async fn test_datastore_tracked_delete_removes_when_no_referrers(m: Arc<MetadataStore>) {
    let namespace = &Namespace::new("tracked-delete-no-referrers-ns").unwrap();

    let layer_digest = put_blob_direct(m.store(), b"layer for removal test").await;
    let manifest_digest = put_blob_direct(m.store(), b"manifest for removal test").await;

    m.update_links(
        namespace,
        &[LinkOperation::create_with_referrer(
            LinkKind::Layer(layer_digest.clone()),
            layer_digest.clone(),
            manifest_digest.clone(),
        )],
    )
    .await
    .unwrap();

    m.update_links(
        namespace,
        &[LinkOperation::delete_with_referrer(
            LinkKind::Layer(layer_digest.clone()),
            manifest_digest.clone(),
        )],
    )
    .await
    .unwrap();

    let err = m
        .read_link(namespace, &LinkKind::Layer(layer_digest.clone()))
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::NotFound),
        "Link should not exist after all referrers removed, got: {err:?}"
    );

    let layer_link = LinkKind::Layer(layer_digest.clone());
    match m.read_blob_index(&layer_digest).await {
        Ok(index) => {
            let links = index.namespace.get(namespace);
            assert!(
                links.is_none_or(|s| !s.contains(&layer_link)),
                "Blob index should not contain the Layer link after removal"
            );
        }
        Err(Error::NotFound) => {}
        Err(e) => panic!("Unexpected error reading blob index: {e:?}"),
    }
}

#[tokio::test]
async fn test_tracked_delete_removes_when_no_referrers() {
    for_each_backend(async |test_case| {
        test_datastore_tracked_delete_removes_when_no_referrers(test_case.metadata_store()).await;
    })
    .await;
}

pub async fn test_datastore_mixed_tracked_untracked_operations(m: Arc<MetadataStore>) {
    let namespace = &Namespace::new("mixed-tracked-untracked-ns").unwrap();

    let tag_digest = put_blob_direct(m.store(), b"tag content").await;
    let layer_digest = put_blob_direct(m.store(), b"layer content mixed").await;
    let digest_link_digest = put_blob_direct(m.store(), b"digest link content").await;
    let manifest_digest = put_blob_direct(m.store(), b"manifest content mixed").await;

    let ops = [
        LinkOperation::create(LinkKind::Tag(Tag::new("v1").unwrap()), tag_digest.clone()),
        LinkOperation::create_with_referrer(
            LinkKind::Layer(layer_digest.clone()),
            layer_digest.clone(),
            manifest_digest.clone(),
        ),
        LinkOperation::create(
            LinkKind::Digest(digest_link_digest.clone()),
            digest_link_digest.clone(),
        ),
    ];
    m.update_links(namespace, &ops).await.unwrap();

    let tag_meta = m
        .read_link(namespace, &LinkKind::Tag(Tag::new("v1").unwrap()))
        .await
        .unwrap();
    assert_eq!(
        tag_meta.target, tag_digest,
        "Tag v1 should target tag_digest"
    );
    assert!(
        tag_meta.referenced_by.is_empty(),
        "Tag link should have empty referenced_by"
    );

    let layer_meta = m
        .read_link(namespace, &LinkKind::Layer(layer_digest.clone()))
        .await
        .unwrap();
    assert_eq!(
        layer_meta.target, layer_digest,
        "Layer link should target layer_digest"
    );
    assert!(
        layer_meta.referenced_by.contains(&manifest_digest),
        "Layer link referenced_by should contain manifest_digest"
    );

    let digest_meta = m
        .read_link(namespace, &LinkKind::Digest(digest_link_digest.clone()))
        .await
        .unwrap();
    assert_eq!(
        digest_meta.target, digest_link_digest,
        "Digest link should target digest_link_digest"
    );
    assert!(
        digest_meta.referenced_by.is_empty(),
        "Digest link should have empty referenced_by"
    );

    let tag_index = m.read_blob_index(&tag_digest).await.unwrap();
    let tag_links = tag_index
        .namespace
        .get(namespace)
        .expect("Blob index for tag_digest should have namespace entry");
    assert!(
        tag_links.contains(&LinkKind::Tag(Tag::new("v1").unwrap())),
        "Blob index for tag_digest should contain Tag(v1)"
    );

    let layer_index = m.read_blob_index(&layer_digest).await.unwrap();
    let layer_links = layer_index
        .namespace
        .get(namespace)
        .expect("Blob index for layer_digest should have namespace entry");
    assert!(
        layer_links.contains(&LinkKind::Layer(layer_digest.clone())),
        "Blob index for layer_digest should contain the Layer link"
    );

    let digest_index = m.read_blob_index(&digest_link_digest).await.unwrap();
    let digest_links = digest_index
        .namespace
        .get(namespace)
        .expect("Blob index for digest_link_digest should have namespace entry");
    assert!(
        digest_links.contains(&LinkKind::Digest(digest_link_digest.clone())),
        "Blob index for digest_link_digest should contain the Digest link"
    );
}

#[tokio::test]
async fn test_mixed_tracked_untracked_operations() {
    for_each_backend(async |test_case| {
        test_datastore_mixed_tracked_untracked_operations(test_case.metadata_store()).await;
    })
    .await;
}

pub async fn test_datastore_batch_deduplicates_same_digest_operations(m: Arc<MetadataStore>) {
    let namespace = &Namespace::new("batch-dedup-ns").unwrap();
    let digest = put_blob_direct(m.store(), b"dedup content").await;

    let tag_link = LinkKind::Tag(Tag::new("latest").unwrap());
    let digest_link = LinkKind::Digest(digest.clone());

    m.update_links(
        namespace,
        &[
            LinkOperation::create(tag_link.clone(), digest.clone()),
            LinkOperation::create(digest_link.clone(), digest.clone()),
        ],
    )
    .await
    .unwrap();

    let blob_index = m.read_blob_index(&digest).await.unwrap();
    let links = blob_index
        .namespace
        .get(namespace)
        .expect("Blob index should have an entry for the namespace");
    assert!(
        links.contains(&tag_link),
        "Blob index should contain Tag(latest)"
    );
    assert!(
        links.contains(&digest_link),
        "Blob index should contain Digest link"
    );
    assert_eq!(links.len(), 2, "Blob index should have exactly 2 entries");
}

#[tokio::test]
async fn test_batch_deduplicates_same_digest_operations() {
    for_each_backend(async |test_case| {
        test_datastore_batch_deduplicates_same_digest_operations(test_case.metadata_store()).await;
    })
    .await;
}

pub async fn test_datastore_batch_handles_mixed_insert_remove_same_digest(m: Arc<MetadataStore>) {
    let namespace = &Namespace::new("batch-mixed-ns").unwrap();
    let digest = put_blob_direct(m.store(), b"mixed content").await;

    let tag_v1 = LinkKind::Tag(Tag::new("v1").unwrap());
    create_link(&m, namespace, &tag_v1, &digest).await;

    let tag_v2 = LinkKind::Tag(Tag::new("v2").unwrap());
    m.update_links(
        namespace,
        &[
            LinkOperation::delete(tag_v1.clone()),
            LinkOperation::create(tag_v2.clone(), digest.clone()),
        ],
    )
    .await
    .unwrap();

    let blob_index = m.read_blob_index(&digest).await.unwrap();
    let links = blob_index
        .namespace
        .get(namespace)
        .expect("Blob index should have an entry for the namespace");
    assert!(links.contains(&tag_v2), "Blob index should contain Tag(v2)");
    assert!(
        !links.contains(&tag_v1),
        "Blob index should not contain Tag(v1)"
    );
}

#[tokio::test]
async fn test_batch_handles_mixed_insert_remove_same_digest() {
    for_each_backend(async |test_case| {
        test_datastore_batch_handles_mixed_insert_remove_same_digest(test_case.metadata_store())
            .await;
    })
    .await;
}

pub async fn test_datastore_batch_deletes_empty_blob_container(m: Arc<MetadataStore>) {
    let namespace = &Namespace::new("batch-empty-container-ns").unwrap();
    let digest = put_blob_direct(m.store(), b"ephemeral content").await;

    let tag_link = LinkKind::Tag(Tag::new("v1").unwrap());
    create_link(&m, namespace, &tag_link, &digest).await;

    delete_link(&m, namespace, &tag_link).await;

    match m.read_blob_index(&digest).await {
        Ok(index) => {
            let links = index.namespace.get(namespace);
            assert!(
                links.is_none_or(HashSet::is_empty),
                "Blob index should have no entries for the namespace after deletion"
            );
        }
        Err(Error::NotFound) => {}
        Err(e) => panic!("Unexpected error reading blob index: {e:?}"),
    }
}

#[tokio::test]
async fn test_batch_deletes_empty_blob_container() {
    for_each_backend(async |test_case| {
        test_datastore_batch_deletes_empty_blob_container(test_case.metadata_store()).await;
    })
    .await;
}

pub async fn test_datastore_batch_multiple_unique_digests(m: Arc<MetadataStore>) {
    let namespace = &Namespace::new("batch-multi-digest-ns").unwrap();
    let layer1_digest = put_blob_direct(m.store(), b"layer1 data").await;
    let layer2_digest = put_blob_direct(m.store(), b"layer2 data").await;
    let layer3_digest = put_blob_direct(m.store(), b"layer3 data").await;
    let config_digest = put_blob_direct(m.store(), b"config data").await;

    let layer1_link = LinkKind::Layer(layer1_digest.clone());
    let layer2_link = LinkKind::Layer(layer2_digest.clone());
    let layer3_link = LinkKind::Layer(layer3_digest.clone());
    let config_link = LinkKind::Config(config_digest.clone());

    m.update_links(
        namespace,
        &[
            LinkOperation::create(layer1_link.clone(), layer1_digest.clone()),
            LinkOperation::create(layer2_link.clone(), layer2_digest.clone()),
            LinkOperation::create(layer3_link.clone(), layer3_digest.clone()),
            LinkOperation::create(config_link.clone(), config_digest.clone()),
        ],
    )
    .await
    .unwrap();

    for (digest, link) in [
        (&layer1_digest, &layer1_link),
        (&layer2_digest, &layer2_link),
        (&layer3_digest, &layer3_link),
        (&config_digest, &config_link),
    ] {
        let blob_index = m.read_blob_index(digest).await.unwrap();
        let links = blob_index
            .namespace
            .get(namespace)
            .expect("Blob index should have an entry for the namespace");
        assert_eq!(
            links.len(),
            1,
            "Each blob index should have exactly 1 entry"
        );
        assert!(
            links.contains(link),
            "Blob index should contain the expected link"
        );
    }
}

#[tokio::test]
async fn test_batch_multiple_unique_digests() {
    for_each_backend(async |test_case| {
        test_datastore_batch_multiple_unique_digests(test_case.metadata_store()).await;
    })
    .await;
}

pub async fn test_datastore_batch_preserves_existing_blob_index_entries(m: Arc<MetadataStore>) {
    let other_ns = &Namespace::new("other-ns").unwrap();
    let my_ns = &Namespace::new("my-ns").unwrap();
    let digest = put_blob_direct(m.store(), b"shared content").await;

    let other_tag = LinkKind::Tag(Tag::new("stable").unwrap());
    create_link(&m, other_ns, &other_tag, &digest).await;

    let blob_index = m.read_blob_index(&digest).await.unwrap();
    assert!(
        blob_index.namespace.contains_key(other_ns),
        "Blob index should have entry for other-ns"
    );

    let my_tag = LinkKind::Tag(Tag::new("latest").unwrap());
    create_link(&m, my_ns, &my_tag, &digest).await;

    let blob_index = m.read_blob_index(&digest).await.unwrap();
    let other_links = blob_index
        .namespace
        .get(other_ns)
        .expect("Blob index should still have entry for other-ns");
    assert!(
        other_links.contains(&other_tag),
        "other-ns should still contain Tag(stable)"
    );

    let my_links = blob_index
        .namespace
        .get(my_ns)
        .expect("Blob index should have entry for my-ns");
    assert!(
        my_links.contains(&my_tag),
        "my-ns should contain Tag(latest)"
    );
}

#[tokio::test]
async fn test_batch_preserves_existing_blob_index_entries() {
    for_each_backend(async |test_case| {
        test_datastore_batch_preserves_existing_blob_index_entries(test_case.metadata_store())
            .await;
    })
    .await;
}

async fn create_link_with_media_type(
    m: &Arc<MetadataStore>,
    namespace: &str,
    link: &LinkKind,
    digest: &Digest,
    media_type: &str,
) {
    let namespace = Namespace::new(namespace).unwrap();
    m.update_links(
        &namespace,
        &[LinkOperation::create_with_media_type(
            link.clone(),
            digest.clone(),
            Some(MediaType::new(media_type).unwrap()),
        )],
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn test_link_metadata_media_type() {
    for_each_backend(async |test_case| {
        let m = test_case.metadata_store();
        let namespace = Namespace::new("media-type-test").unwrap();
        let digest = put_blob_direct(m.store(), b"test content").await;

        let media_type = "application/vnd.docker.distribution.manifest.v2+json";

        create_link_with_media_type(
            &m,
            &namespace,
            &LinkKind::Digest(digest.clone()),
            &digest,
            media_type,
        )
        .await;

        let link = m
            .read_link(&namespace, &LinkKind::Digest(digest.clone()))
            .await
            .unwrap();
        assert_eq!(link.media_type, Some(MediaType::new(media_type).unwrap()));
        assert_eq!(link.target, digest);
    })
    .await;
}

#[tokio::test]
async fn test_link_without_media_type_has_none() {
    for_each_backend(async |test_case| {
        let m = test_case.metadata_store();
        let namespace = Namespace::new("no-media-type-test").unwrap();
        let digest = put_blob_direct(m.store(), b"test content 2").await;

        create_link(
            &m,
            &namespace,
            &LinkKind::Tag(Tag::new("latest").unwrap()),
            &digest,
        )
        .await;

        let link = m
            .read_link(&namespace, &LinkKind::Tag(Tag::new("latest").unwrap()))
            .await
            .unwrap();
        assert_eq!(link.media_type, None);
        assert_eq!(link.target, digest);
    })
    .await;
}

pub async fn test_datastore_list_referrers_with_stored_descriptor(m: Arc<MetadataStore>) {
    let namespace = &Namespace::new("test-stored-descriptor").unwrap();

    // Create a base manifest blob that the referrers will reference
    let base_digest = put_blob_direct(m.store(), b"base manifest content").await;
    let base_link = LinkKind::Digest(base_digest.clone());
    create_link(&m, namespace, &base_link, &base_digest).await;

    // Build a Descriptor for the referrer WITHOUT creating the referrer blob.
    // If the optimization works, list_referrers should return this descriptor
    // directly from the stored metadata, without needing the blob in the blob store.
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

    let referrer_link = LinkKind::Referrer(base_digest.clone(), referrer_digest.clone());
    m.update_links(
        namespace,
        &[LinkOperation::create_with_descriptor(
            referrer_link.clone(),
            referrer_digest.clone(),
            Box::new(descriptor.clone()),
        )],
    )
    .await
    .unwrap();

    // list_referrers should return the stored descriptor without reading a blob
    let referrers = m
        .list_referrers(namespace, &base_digest, None)
        .await
        .unwrap();

    assert_eq!(referrers.len(), 1, "Expected 1 referrer descriptor");
    assert_eq!(referrers[0], descriptor);

    let filtered = m
        .list_referrers(
            namespace,
            &base_digest,
            Some("application/vnd.example.test-artifact".to_string()),
        )
        .await
        .unwrap();
    assert_eq!(filtered.len(), 1, "Should match artifact type filter");
    assert_eq!(filtered[0], descriptor);

    let non_matching = m
        .list_referrers(
            namespace,
            &base_digest,
            Some("application/vnd.non-existent".to_string()),
        )
        .await
        .unwrap();
    assert!(
        non_matching.is_empty(),
        "Should return empty for non-matching artifact type"
    );
}

#[tokio::test]
async fn test_list_referrers_with_stored_descriptor() {
    for_each_backend(async |test_case| {
        test_datastore_list_referrers_with_stored_descriptor(test_case.metadata_store()).await;
    })
    .await;
}

#[test]
fn test_link_metadata_backward_compat_no_media_type() {
    let json = format!(
        r#"{{"target":"sha256:{}","created_at":"2024-01-01T00:00:00Z"}}"#,
        "a".repeat(64)
    );
    let metadata: LinkMetadata = serde_json::from_slice(json.as_bytes()).unwrap();
    assert_eq!(metadata.media_type, None);
}
