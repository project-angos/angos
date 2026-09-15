mod access_time;
mod blob_index;
mod cache;
mod gc;
mod list_namespaces;
mod tag_entries;

use std::{collections::HashMap, sync::Arc};

use chrono::{Duration, Utc};
use futures_util::TryStreamExt;

use angos_oci::request::GetReferrersRequest;
use angos_oci::{Algorithm, Descriptor, Digest, MediaType, Namespace, Tag};
use angos_s3_client::Backend as S3HttpBackend;
use angos_storage::Page;
use angos_storage::{ObjectStore, s3::Backend as StorageS3Backend};

use crate::{
    metrics_provider,
    registry::{
        Error, Registry,
        metadata_store::{LinkKind, LinkMetadata, LinkOperation, MetadataStore},
        s3_connection::S3ConnectionConfig,
        test_utils::{
            for_each_backend, media_type, put_blob_direct, referrers_request, s3_test_connection,
        },
    },
};
use angos_cache::Cache;
use angos_cache::memory::Backend as CacheMemoryBackend;

#[derive(Clone)]
pub struct TestS3Config {
    pub connection: S3ConnectionConfig,
    pub link_cache_ttl: u64,
}

impl TestS3Config {
    pub fn to_backend(&self, cache: Option<Arc<Cache>>) -> Result<MetadataStore, Error> {
        let http = Arc::new(
            S3HttpBackend::new(&self.connection.to_client_config())
                .map_err(|e| Error::Internal(e.to_string()))?,
        );
        let object_store: Arc<dyn ObjectStore> = Arc::new(StorageS3Backend::builder(http).build());

        let mut builder = MetadataStore::builder(object_store)
            .link_cache_ttl(self.link_cache_ttl)
            .gc_grace_secs(0);

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
        link_cache_ttl: 30,
    }
}

pub fn test_backend_with_cache(config: &TestS3Config) -> (MetadataStore, Arc<Cache>) {
    let cache = Arc::new(Cache::Memory(CacheMemoryBackend::new()));
    let backend = config.to_backend(Some(cache.clone())).unwrap();
    (backend, cache)
}

pub fn test_backend(config: &TestS3Config) -> MetadataStore {
    config.to_backend(None).unwrap()
}

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
    let digest = put_blob_direct(m.object_store(), b"test blob content").await;

    for namespace in &namespaces {
        let tag_link = LinkKind::Tag(Tag::new("latest").unwrap());
        create_link(&m, namespace, &tag_link, &digest).await;
    }

    let listed_namespaces = m.list_namespaces(10, None).await.unwrap().items;
    assert_eq!(listed_namespaces, namespaces);

    let Page {
        items: page1,
        next_token: token1,
    } = m.list_namespaces(2, None).await.unwrap();
    assert_eq!(page1, ["repo1", "repo2"]);
    assert!(token1.is_some());

    let Page {
        items: page2,
        next_token: token2,
    } = m.list_namespaces(2, token1).await.unwrap();
    assert_eq!(page2, ["repo3/nested"]);
    assert!(token2.is_none());

    let Page {
        items: page1,
        next_token: token1,
    } = m.list_namespaces(1, None).await.unwrap();
    assert_eq!(page1, ["repo1"]);
    assert!(token1.is_some());

    let Page {
        items: page2,
        next_token: token2,
    } = m.list_namespaces(1, token1).await.unwrap();
    assert_eq!(page2, ["repo2"]);

    let Page {
        items: page3,
        next_token: token3,
    } = m.list_namespaces(1, token2).await.unwrap();
    assert_eq!(page3, ["repo3/nested"]);
    assert!(token3.is_none());

    let mut all_namespaces = page1;
    all_namespaces.extend(page2);
    all_namespaces.extend(page3);

    assert_eq!(all_namespaces, namespaces);
}

pub async fn test_datastore_list_tags(m: Arc<MetadataStore>) {
    let namespace = &Namespace::new("test-repo").unwrap();
    let digest = put_blob_direct(m.object_store(), b"test blob content").await;

    let tags = ["latest", "v1.0", "v2.0"];
    for tag in tags {
        let tag_link = LinkKind::Tag(Tag::new(tag).unwrap());
        create_link(&m, namespace, &tag_link, &digest).await;
    }

    let Page {
        items: all_tags,
        next_token: token,
    } = m.list_tags(namespace, 10, None).await.unwrap();
    assert_eq!(all_tags.len(), tags.len());
    for tag in tags {
        assert!(all_tags.contains(&Tag::new(tag).unwrap()));
    }
    assert!(token.is_none());

    let Page {
        items: page1,
        next_token: token1,
    } = m.list_tags(namespace, 2, None).await.unwrap();
    assert_eq!(page1.len(), 2);
    assert!(token1.is_some());

    let Page {
        items: page2,
        next_token: token2,
    } = m.list_tags(namespace, 2, token1).await.unwrap();
    assert_eq!(page2.len(), 1);
    assert!(token2.is_none());

    let Page {
        items: page1,
        next_token: token1,
    } = m.list_tags(namespace, 1, None).await.unwrap();
    assert_eq!(page1.len(), 1);
    assert!(token1.is_some());

    let Page {
        items: page2,
        next_token: token2,
    } = m.list_tags(namespace, 1, token1).await.unwrap();
    assert_eq!(page2.len(), 1);
    assert!(token2.is_some());

    let Page {
        items: page3,
        next_token: token3,
    } = m.list_tags(namespace, 1, token2).await.unwrap();
    assert_eq!(page3.len(), 1);
    assert!(token3.is_none());

    let delete_tag = "v1.0";
    let tag_link = LinkKind::Tag(Tag::new(delete_tag).unwrap());
    delete_link(&m, namespace, &tag_link).await;

    let tags_after_delete = m.list_tags(namespace, 10, None).await.unwrap().items;
    assert_eq!(tags_after_delete.len(), tags.len() - 1);
    assert!(!tags_after_delete.contains(&Tag::new(delete_tag).unwrap()));
}

pub async fn test_datastore_list_referrers(registry: &Registry) {
    let m = registry.metadata_store.clone();
    let namespace = &Namespace::new("test-repo").unwrap();
    let base_digest = put_blob_direct(m.object_store(), b"base manifest content").await;
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

    let referrer_digest = put_blob_direct(m.object_store(), referrer_content.as_bytes()).await;
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
        size: 694,
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
        let digest = put_blob_direct(m.object_store(), content).await;
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

    // sha256 and sha512 live under separate prefixes; the stream must chain
    // them in global sort order, each digest exactly once.
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
    let digest = put_blob_direct(m.object_store(), b"test blob content").await;

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
async fn test_list_referrers() {
    for_each_backend(async |test_case| {
        test_datastore_list_referrers(test_case.registry()).await;
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
    let digest = put_blob_direct(m.object_store(), b"dedup test content").await;

    let tag_link = LinkKind::Tag(Tag::new("latest").unwrap());
    let digest_link = LinkKind::Digest(digest.clone());
    let layer_link = LinkKind::Layer(digest.clone());
    let config_link = LinkKind::Config(digest.clone());

    create_link(&m, namespace, &tag_link, &digest).await;
    create_link(&m, namespace, &digest_link, &digest).await;
    create_link(&m, namespace, &layer_link, &digest).await;
    create_link(&m, namespace, &config_link, &digest).await;

    let namespaces = m.list_namespaces(10, None).await.unwrap().items;
    let count = namespaces.iter().filter(|n| *n == namespace).count();
    assert_eq!(
        count, 1,
        "Namespace '{namespace}' should appear exactly once but appeared {count} times"
    );
}

pub async fn test_datastore_list_namespaces_many_namespaces_pagination(m: Arc<MetadataStore>) {
    let digest = put_blob_direct(m.object_store(), b"pagination test content").await;

    let namespace_names: Vec<String> = (0..10).map(|i| format!("ns-{i:02}")).collect();

    for ns in &namespace_names {
        let tag_link = LinkKind::Tag(Tag::new("latest").unwrap());
        create_link(&m, ns, &tag_link, &digest).await;
    }

    let mut all_namespaces = Vec::new();
    let mut token: Option<String> = None;
    let mut page_count = 0;

    loop {
        let Page {
            items: page,
            next_token,
        } = m.list_namespaces(3, token).await.unwrap();
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
        all_namespaces
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
        namespace_names,
        "All namespaces should be returned in sorted order across pages"
    );
    assert_eq!(
        page_count, 4,
        "Expected 4 pages (3+3+3+1) but got {page_count}"
    );
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

pub async fn test_datastore_list_tags_many_tags_pagination(m: Arc<MetadataStore>) {
    let namespace = &Namespace::new("test-repo-name").unwrap();
    let digest = put_blob_direct(m.object_store(), b"content").await;

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
        let Page {
            items: page,
            next_token,
        } = m.list_tags(namespace, 3, token).await.unwrap();
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
    let digest = put_blob_direct(m.object_store(), b"content").await;

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
        let Page {
            items: page,
            next_token,
        } = m.list_tags(namespace, 1, token).await.unwrap();
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
    let digest1 = put_blob_direct(m.object_store(), b"content1").await;
    let digest2 = put_blob_direct(m.object_store(), b"content2").await;

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
    let digest = put_blob_direct(m.object_store(), b"access time test content").await;

    let tag_link = LinkKind::Tag(Tag::new("latest").unwrap());
    create_link(&m, namespace, &tag_link, &digest).await;

    m.read_link_recording_access(namespace, &tag_link, "test-client")
        .await
        .unwrap();

    let stored = m
        .read_tag_access_time(namespace, &Tag::new("latest").unwrap())
        .await
        .unwrap()
        .expect("a recording read must persist the pull");
    assert!(
        Utc::now().signed_duration_since(stored) < Duration::seconds(2),
        "the recorded pull must be within 2 seconds of now"
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
    let digest = put_blob_direct(m.object_store(), b"concurrent read test content").await;

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

pub async fn test_datastore_list_referrers_parallel_correctness(registry: &Registry) {
    let m = registry.metadata_store.clone();
    let namespace = &Namespace::new("test-referrers-parallel").unwrap();
    let subject_digest = put_blob_direct(m.object_store(), b"subject manifest content").await;
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

        let referrer_digest = put_blob_direct(m.object_store(), referrer_content.as_bytes()).await;
        let digest_link = LinkKind::Digest(referrer_digest.clone());
        create_link(&m, namespace, &digest_link, &referrer_digest).await;

        let referrer_link = LinkKind::Referrer {
            subject: subject_digest.clone(),
            referrer: referrer_digest.clone(),
        };
        create_link(&m, namespace, &referrer_link, &referrer_digest).await;

        referrer_digests.push(referrer_digest);
    }

    let descriptors = registry
        .list_referrers(None, &referrers_request(namespace, &subject_digest))
        .await
        .unwrap();

    assert_eq!(
        descriptors.items.len(),
        5,
        "Expected 5 referrer descriptors but got {}",
        descriptors.items.len()
    );

    for pair in descriptors.items.windows(2) {
        assert!(
            pair[0].digest.to_string() <= pair[1].digest.to_string(),
            "Descriptors should be sorted by digest: {} should come before {}",
            pair[0].digest,
            pair[1].digest
        );
    }
}

pub async fn test_datastore_list_referrers_with_artifact_type_filter(registry: &Registry) {
    let m = registry.metadata_store.clone();
    let namespace = &Namespace::new("test-referrers-filter").unwrap();
    let subject_digest =
        put_blob_direct(m.object_store(), b"subject manifest for filter test").await;
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

        let referrer_digest = put_blob_direct(m.object_store(), referrer_content.as_bytes()).await;
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

        let referrer_digest = put_blob_direct(m.object_store(), referrer_content.as_bytes()).await;
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
}

pub async fn test_datastore_list_referrers_deterministic_order(registry: &Registry) {
    let m = registry.metadata_store.clone();
    let namespace = &Namespace::new("test-referrers-order").unwrap();
    let subject_digest =
        put_blob_direct(m.object_store(), b"subject manifest for order test").await;
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

        let referrer_digest = put_blob_direct(m.object_store(), referrer_content.as_bytes()).await;
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
}

#[tokio::test]
async fn test_list_referrers_parallel_correctness() {
    for_each_backend(async |test_case| {
        test_datastore_list_referrers_parallel_correctness(test_case.registry()).await;
    })
    .await;
}

#[tokio::test]
async fn test_list_referrers_with_artifact_type_filter() {
    for_each_backend(async |test_case| {
        test_datastore_list_referrers_with_artifact_type_filter(test_case.registry()).await;
    })
    .await;
}

#[tokio::test]
async fn test_list_referrers_deterministic_order() {
    for_each_backend(async |test_case| {
        test_datastore_list_referrers_deterministic_order(test_case.registry()).await;
    })
    .await;
}

pub async fn test_datastore_parallel_multiple_creates(m: Arc<MetadataStore>) {
    let namespace = &Namespace::new("parallel-creates-ns").unwrap();

    let mut digests = Vec::new();
    for i in 0..5 {
        let digest = put_blob_direct(m.object_store(), format!("content-{i}").as_bytes()).await;
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

    let tags = m.list_tags(namespace, 10, None).await.unwrap().items;
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

    let digest_a = put_blob_direct(m.object_store(), b"content-a").await;
    let digest_b = put_blob_direct(m.object_store(), b"content-b").await;
    let digest_c = put_blob_direct(m.object_store(), b"content-c").await;

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
    let index_a = m.read_blob_index(&digest_a).await.unwrap();
    assert!(
        index_a
            .namespace
            .get(namespace)
            .is_some_and(|links| links.contains(&tag_v1)),
        "the stale entry is the collector's to prune, not the writer's"
    );

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
        let digest = put_blob_direct(m.object_store(), format!("content-{i}").as_bytes()).await;
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

    let digest_layer = put_blob_direct(m.object_store(), b"layer content").await;
    let digest_manifest = put_blob_direct(m.object_store(), b"manifest content").await;

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

    let err = m
        .read_link(namespace, &LinkKind::Layer(digest_layer.clone()))
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::NotFound),
        "a tracked create must not write the legacy link file, got: {err:?}"
    );

    let blob_index = m.read_blob_index(&digest_layer).await.unwrap();
    let links = blob_index
        .namespace
        .get(namespace)
        .expect("Blob index should have an entry for the namespace");
    assert!(
        links.contains(&LinkKind::ReferencedBy(digest_manifest.clone())),
        "Blob index should contain the per-referrer entry"
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

    let layer_digest = put_blob_direct(m.object_store(), b"layer for delete test").await;
    let first_manifest_digest = put_blob_direct(m.object_store(), b"manifest a content").await;
    let second_manifest_digest = put_blob_direct(m.object_store(), b"manifest b content").await;

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

    let blob_index = m.read_blob_index(&layer_digest).await.unwrap();
    let links = blob_index
        .namespace
        .get(namespace)
        .expect("Blob index should have an entry for the namespace");
    assert!(
        links.contains(&LinkKind::ReferencedBy(first_manifest_digest.clone()))
            && links.contains(&LinkKind::ReferencedBy(second_manifest_digest.clone())),
        "both pushes must leave their per-referrer entry"
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

    let result = m
        .read_link(namespace, &LinkKind::Layer(layer_digest.clone()))
        .await;
    assert!(
        matches!(result, Err(Error::NotFound)),
        "no legacy link file may exist after a tracked delete"
    );
    let blob_index = m.read_blob_index(&layer_digest).await.unwrap();
    let links = blob_index
        .namespace
        .get(namespace)
        .expect("Blob index should still have an entry for the namespace");
    assert!(
        links.contains(&LinkKind::ReferencedBy(second_manifest_digest.clone())),
        "the second manifest's per-referrer entry must survive"
    );
}

#[tokio::test]
async fn test_tracked_delete_with_referrer() {
    for_each_backend(async |test_case| {
        test_datastore_tracked_delete_with_referrer(test_case.metadata_store()).await;
    })
    .await;
}

pub async fn test_datastore_duplicated_layer_keeps_other_referrers(m: Arc<MetadataStore>) {
    let namespace = &Namespace::new("tracked-duplicate-layer-ns").unwrap();

    let layer_digest = put_blob_direct(m.object_store(), b"layer listed twice").await;
    let first_manifest_digest = put_blob_direct(m.object_store(), b"manifest c content").await;
    let second_manifest_digest = put_blob_direct(m.object_store(), b"manifest d content").await;

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

    // A manifest listing the same layer twice plans that link twice in one
    // transaction.
    let duplicated_create = vec![
        LinkOperation::create_with_referrer(
            LinkKind::Layer(layer_digest.clone()),
            layer_digest.clone(),
            second_manifest_digest.clone(),
        );
        2
    ];
    m.update_links(namespace, &duplicated_create).await.unwrap();

    let blob_index = m.read_blob_index(&layer_digest).await.unwrap();
    let links = blob_index
        .namespace
        .get(namespace)
        .expect("Blob index should have an entry for the namespace");
    assert!(
        links.contains(&LinkKind::ReferencedBy(first_manifest_digest.clone()))
            && links.contains(&LinkKind::ReferencedBy(second_manifest_digest.clone())),
        "the duplicated push must leave one per-referrer entry per manifest"
    );

    let duplicated_delete = vec![
        LinkOperation::delete_with_referrer(
            LinkKind::Layer(layer_digest.clone()),
            second_manifest_digest.clone(),
        );
        2
    ];
    m.update_links(namespace, &duplicated_delete).await.unwrap();

    let blob_index = m.read_blob_index(&layer_digest).await.unwrap();
    let links = blob_index
        .namespace
        .get(namespace)
        .expect("Blob index should still have an entry for the namespace");
    assert!(
        links.contains(&LinkKind::ReferencedBy(first_manifest_digest.clone())),
        "deleting the duplicating manifest should leave the first manifest's reference"
    );
}

#[tokio::test]
async fn test_duplicated_layer_keeps_other_referrers() {
    for_each_backend(async |test_case| {
        test_datastore_duplicated_layer_keeps_other_referrers(test_case.metadata_store()).await;
    })
    .await;
}

pub async fn test_datastore_tracked_delete_removes_when_no_referrers(m: Arc<MetadataStore>) {
    let namespace = &Namespace::new("tracked-delete-no-referrers-ns").unwrap();

    let layer_digest = put_blob_direct(m.object_store(), b"layer for removal test").await;
    let manifest_digest = put_blob_direct(m.object_store(), b"manifest for removal test").await;

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

    let entry = LinkKind::ReferencedBy(manifest_digest.clone());
    let index = m.read_blob_index(&layer_digest).await.unwrap();
    assert!(
        index
            .namespace
            .get(namespace)
            .is_some_and(|links| links.contains(&entry)),
        "the stale entry is the collector's to prune, not the writer's"
    );
}

#[tokio::test]
async fn test_tracked_delete_removes_when_no_referrers() {
    for_each_backend(async |test_case| {
        test_datastore_tracked_delete_removes_when_no_referrers(test_case.metadata_store()).await;
    })
    .await;
}

pub async fn test_datastore_shared_blob_pin_survives_other_manifest_delete(m: Arc<MetadataStore>) {
    let namespace = &Namespace::new("shared-config-delete-ns").unwrap();

    let config_digest = put_blob_direct(m.object_store(), b"shared config bytes").await;
    let first_manifest = put_blob_direct(m.object_store(), b"first sharing manifest").await;
    let second_manifest = put_blob_direct(m.object_store(), b"second sharing manifest").await;

    for manifest in [&first_manifest, &second_manifest] {
        m.update_links(
            namespace,
            &[
                LinkOperation::create(LinkKind::Digest(manifest.clone()), manifest.clone()),
                LinkOperation::create_with_referrer(
                    LinkKind::Config(config_digest.clone()),
                    config_digest.clone(),
                    manifest.clone(),
                ),
            ],
        )
        .await
        .unwrap();
    }

    m.delete_manifest(
        namespace,
        &[
            LinkOperation::delete(LinkKind::Digest(first_manifest.clone())),
            LinkOperation::delete_with_referrer(
                LinkKind::Config(config_digest.clone()),
                first_manifest.clone(),
            ),
        ],
        None,
    )
    .await
    .unwrap();

    let links = m
        .read_blob_index_namespace(namespace, &config_digest)
        .await
        .unwrap();
    let surviving = LinkKind::ReferencedBy(second_manifest.clone());
    let deleted = LinkKind::ReferencedBy(first_manifest.clone());
    assert!(
        links.contains(&surviving),
        "the surviving manifest's entry must remain"
    );
    assert!(
        m.reference_backed(namespace, &surviving, &config_digest)
            .await
            .unwrap(),
        "the surviving manifest's entry must still be backed"
    );
    assert!(
        !m.reference_backed(namespace, &deleted, &config_digest)
            .await
            .unwrap(),
        "the deleted manifest's entry must read as unbacked"
    );
}

#[tokio::test]
async fn test_shared_blob_pin_survives_other_manifest_delete() {
    for_each_backend(async |test_case| {
        test_datastore_shared_blob_pin_survives_other_manifest_delete(test_case.metadata_store())
            .await;
    })
    .await;
}

pub async fn test_datastore_mixed_tracked_untracked_operations(m: Arc<MetadataStore>) {
    let namespace = &Namespace::new("mixed-tracked-untracked-ns").unwrap();

    let tag_digest = put_blob_direct(m.object_store(), b"tag content").await;
    let layer_digest = put_blob_direct(m.object_store(), b"layer content mixed").await;
    let digest_link_digest = put_blob_direct(m.object_store(), b"digest link content").await;
    let manifest_digest = put_blob_direct(m.object_store(), b"manifest content mixed").await;

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
    let layer_err = m
        .read_link(namespace, &LinkKind::Layer(layer_digest.clone()))
        .await
        .unwrap_err();
    assert!(
        matches!(layer_err, Error::NotFound),
        "a tracked create must not write the legacy link file, got: {layer_err:?}"
    );

    let digest_meta = m
        .read_link(namespace, &LinkKind::Digest(digest_link_digest.clone()))
        .await
        .unwrap();
    assert_eq!(
        digest_meta.target, digest_link_digest,
        "Digest link should target digest_link_digest"
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
        layer_links.contains(&LinkKind::ReferencedBy(manifest_digest.clone())),
        "Blob index for layer_digest should contain the per-referrer entry"
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
    let digest = put_blob_direct(m.object_store(), b"dedup content").await;

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
    let digest = put_blob_direct(m.object_store(), b"mixed content").await;

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
        links.contains(&tag_v1),
        "the superseded entry is the collector's to prune, not the writer's"
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
    let digest = put_blob_direct(m.object_store(), b"ephemeral content").await;

    let tag_link = LinkKind::Tag(Tag::new("v1").unwrap());
    create_link(&m, namespace, &tag_link, &digest).await;

    delete_link(&m, namespace, &tag_link).await;

    match m.read_blob_index(&digest).await {
        Ok(index) => {
            let links = index.namespace.get(namespace);
            assert!(
                links.is_some_and(|links| links.contains(&tag_link)),
                "the stale entry is the collector's to prune, not the writer's"
            );
        }
        Err(Error::NotFound) => panic!("the stale entry must persist for the collector"),
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
    let layer1_digest = put_blob_direct(m.object_store(), b"layer1 data").await;
    let layer2_digest = put_blob_direct(m.object_store(), b"layer2 data").await;
    let layer3_digest = put_blob_direct(m.object_store(), b"layer3 data").await;
    let config_digest = put_blob_direct(m.object_store(), b"config data").await;

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
    let digest = put_blob_direct(m.object_store(), b"shared content").await;

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
            None,
            None,
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
        let digest = put_blob_direct(m.object_store(), b"test content").await;

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
        let digest = put_blob_direct(m.object_store(), b"test content 2").await;

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

pub async fn test_datastore_list_referrers_with_stored_descriptor(registry: &Registry) {
    let m = registry.metadata_store.clone();
    let namespace = &Namespace::new("test-stored-descriptor").unwrap();

    let base_digest = put_blob_direct(m.object_store(), b"base manifest content").await;
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

    let referrer_link = LinkKind::Referrer {
        subject: base_digest.clone(),
        referrer: referrer_digest.clone(),
    };
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
}

#[tokio::test]
async fn test_list_referrers_with_stored_descriptor() {
    for_each_backend(async |test_case| {
        test_datastore_list_referrers_with_stored_descriptor(test_case.registry()).await;
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
