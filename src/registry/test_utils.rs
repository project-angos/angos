use std::{collections::HashMap, io::Cursor, sync::Arc};

use bytes::Bytes;
use bytesize::ByteSize;
use http::{
    Response,
    header::{HeaderName, HeaderValue},
};
use http_body_util::BodyExt;
use serde_json::json;
use tempfile::TempDir;
use uuid::Uuid;

use angos_oci::{
    Digest, MediaRange, MediaType, Namespace, Tag, UploadSessionId,
    header::{DOCKER_CONTENT_DIGEST, DOCKER_UPLOAD_UUID},
    http_range::RequestRange,
    request::{CompleteUploadRequest, GetBlobRequest, GetReferrersRequest},
};
use angos_oci_client::RegistryClient;
use angos_s3_client::{
    Backend as S3HttpBackend,
    test_util::{TEST_ACCESS_KEY, TEST_BUCKET, TEST_REGION, TEST_SECRET_KEY, test_endpoint},
};
use angos_secret::Secret;
use angos_storage::{
    ObjectStore, fs::Backend as StorageFsBackend, s3::Backend as StorageS3Backend,
};
use angos_transport::ResponseBody;

use crate::{
    configuration::{GlobalConfig, RegexPattern},
    jobs::{
        Queue,
        store::{ClaimMode, JobStore},
    },
    metrics_provider,
    policy::{RetentionPolicy, RetentionPolicyConfig, SystemClock},
    registry::{
        Error, Registry, RegistryConfig, Repository,
        blob_store::{self, BlobStore, BlobStoreConfig},
        keys::{DigestKeys, NamespaceKeys},
        manifest::DEFAULT_MAX_MANIFEST_SIZE_BYTES,
        metadata_store::{LinkKind, MetadataStore, Settings},
        repository_resolver::RepositoryResolver,
        s3_connection::S3ConnectionConfig,
    },
    replication::{ReplicationDownstream, ReplicationJob},
};

/// Connection to the live S3 test backend, single-sourced from the s3-client
/// fixtures so credentials, bucket, and the endpoint override live in one place.
pub fn s3_test_connection(key_prefix: String) -> S3ConnectionConfig {
    S3ConnectionConfig {
        access_key_id: Secret::new(TEST_ACCESS_KEY.to_string()),
        secret_key: Secret::new(TEST_SECRET_KEY.to_string()),
        endpoint: test_endpoint(),
        region: TEST_REGION.to_string(),
        bucket: TEST_BUCKET.to_string(),
        key_prefix,
    }
}

/// One object store over a fresh temp directory, shared by a cache-less
/// [`MetadataStore`] and a presign-less [`BlobStore`]. Keep the stack alive for
/// the test's duration: dropping it deletes the directory.
pub struct FsTestStack {
    pub dir: TempDir,
    pub store: Arc<dyn ObjectStore>,
    pub metadata_store: Arc<MetadataStore>,
    pub blob_store: Arc<BlobStore>,
}

pub fn fs_test_stack() -> FsTestStack {
    metrics_provider::init_for_tests();
    let dir = TempDir::new().expect("temp dir for fs test stack");
    let store: Arc<dyn ObjectStore> = Arc::new(StorageFsBackend::builder(dir.path()).build());
    let metadata_store = Arc::new(MetadataStore::new(store.clone(), Settings::default()));
    let blob_store = Arc::new(BlobStore::new(store.clone(), None));
    FsTestStack {
        dir,
        store,
        metadata_store,
        blob_store,
    }
}

/// Resolver over a single named repository.
pub fn single_repo_resolver(name: &str, repository: Repository) -> Arc<RepositoryResolver> {
    let mut repositories = HashMap::new();
    repositories.insert(name.to_string(), repository);
    Arc::new(RepositoryResolver::new(Arc::new(repositories)).expect("test resolver"))
}

/// Run `test` once per registry backend, naming the active one in the captured
/// output and running the case's cleanup afterwards. The single home of the
/// backend matrix: tests never iterate [`backends`] themselves.
pub async fn for_each_backend<F>(test: F)
where
    F: AsyncFn(&dyn RegistryTestCase),
{
    for case in backends() {
        eprintln!("running against the {} backend", case.name());
        test(case.as_ref()).await;
        case.cleanup().await;
    }
}

/// A [`MetadataStore`] over `object`.
pub fn metadata_store_over(object: Arc<dyn ObjectStore>) -> Arc<MetadataStore> {
    // Tests exercise reclamation immediately; the race tests needing the grace
    // protection set their own.
    Arc::new(MetadataStore::new(
        object,
        Settings {
            gc_grace_secs: 0,
            // Tests sleep this out; the real linger would stall every run.
            release_linger_ms: 100,
            ..Settings::default()
        },
    ))
}

/// The queue a test registry enqueues into. Nothing drains it unless the test
/// spawns the loops itself, which is the point: building a registry starts no
/// background work.
pub fn test_job_store(metadata_store: &MetadataStore) -> Arc<JobStore> {
    Arc::new(JobStore::new(
        metadata_store.object_store().clone(),
        "test",
        ClaimMode::Atomic,
    ))
}

pub fn create_test_repositories() -> Arc<HashMap<String, Repository>> {
    metrics_provider::init_for_tests();

    let mut repositories = HashMap::new();
    repositories.insert(
        "test-repo".to_string(),
        repository_with_replication("test-repo", Vec::new()),
    );

    Arc::new(repositories)
}

pub fn create_test_registry(
    blob_store: Arc<BlobStore>,
    metadata_store: Arc<MetadataStore>,
) -> Arc<Registry> {
    create_test_registry_with(blob_store, metadata_store, true)
}

/// Like [`create_test_registry`] but pins whether `accept_put_manifest`
/// enforces manifest-reference validation.
pub fn create_test_registry_with(
    blob_store: Arc<BlobStore>,
    metadata_store: Arc<MetadataStore>,
    validate_manifest_references: bool,
) -> Arc<Registry> {
    let resolver = Arc::new(
        RepositoryResolver::new(create_test_repositories())
            .expect("test repositories must not have overlapping prefixes"),
    );
    let global = GlobalConfig::default();

    let config = RegistryConfig {
        update_pull_time: global.update_pull_time,
        enable_blob_redirect: global.enable_blob_redirect,
        enable_manifest_redirect: global.enable_manifest_redirect,
        max_manifest_size_bytes: global.max_manifest_size_bytes(),
        max_blob_size_bytes: global.max_blob_size_bytes(),
        validate_manifest_references,
        global_immutable_tags: global.immutable_tags,
        global_immutable_tags_exclusions: global.immutable_tags_exclusions.clone(),
        ..RegistryConfig::new(test_job_store(&metadata_store))
    };

    Registry::new(blob_store, metadata_store, resolver, config)
}

/// Like [`create_test_registry`] but records pull times, which the default
/// configuration leaves off.
pub fn create_test_registry_recording_pulls(
    blob_store: Arc<BlobStore>,
    metadata_store: Arc<MetadataStore>,
) -> Arc<Registry> {
    let resolver = Arc::new(
        RepositoryResolver::new(create_test_repositories())
            .expect("test repositories must not have overlapping prefixes"),
    );

    let config = RegistryConfig {
        update_pull_time: true,
        ..RegistryConfig::new(test_job_store(&metadata_store))
    };

    Registry::new(blob_store, metadata_store, resolver, config)
}

/// Parse a media type that tests know to be valid.
pub fn media_type(value: &str) -> MediaType {
    MediaType::new(value).unwrap()
}

/// An unfiltered referrers listing of `subject` at the default page size.
pub fn referrers_request(namespace: &Namespace, subject: &Digest) -> GetReferrersRequest {
    GetReferrersRequest {
        namespace: namespace.clone(),
        digest: subject.clone(),
        artifact_type: None,
        last: None,
    }
}

/// Upload `content` through the full registry upload state machine, returning
/// its SHA-256 digest.
pub async fn upload_blob(registry: &Registry, namespace: &Namespace, content: &[u8]) -> Digest {
    let session_id = UploadSessionId::generate();
    registry
        .blob_store
        .create_upload(namespace, &session_id, None)
        .await
        .unwrap();

    let body = content.to_vec();
    let digest = Digest::sha256_of_bytes(&body);
    registry
        .complete_upload(
            None,
            CompleteUploadRequest {
                namespace: namespace.clone(),
                session_id: session_id.clone(),
                digest: digest.clone(),
                content_range: None,
                content_length: Some(body.len() as u64),
            },
            Cursor::new(body),
        )
        .await
        .unwrap();
    digest
}

/// Seed `content` through the blob store, the one production reads bodies from.
/// Seeding through the metadata store's object store instead only resolves when
/// both share a root, which hides a split-backend bug.
pub async fn put_blob_body(blob_store: &BlobStore, content: &[u8]) -> Digest {
    let digest = Digest::sha256_of_bytes(content);
    blob_store
        .put_blob(&digest, Bytes::copy_from_slice(content))
        .await
        .unwrap();
    digest
}

/// Write `content` at the canonical blob path through the raw `ObjectStore`,
/// with no upload state machine and no namespace.
/// Test seeding: make each `(link, target)` exist, through the same write the
/// repair path uses. Batching is a fixture convenience; the store itself only
/// ever writes one key at a time.
pub async fn seed_links(
    metadata_store: &MetadataStore,
    namespace: &Namespace,
    links: &[(LinkKind, Digest)],
) -> Result<(), Error> {
    for (link, target) in links {
        metadata_store.write_link(namespace, link, target).await?;
    }
    Ok(())
}

/// Test teardown: take each link's records away. A tag loses its entries
/// outright rather than gaining a tombstone, since the tombstone is the delete
/// path's write and a fixture copying it would be a second definition of its
/// format. Every other kind lives as a reference key alone, which only the
/// collector removes.
pub async fn drop_links(
    metadata_store: &MetadataStore,
    namespace: &Namespace,
    links: &[LinkKind],
) -> Result<(), Error> {
    let store = metadata_store.object_store();
    for link in links {
        match link {
            LinkKind::Tag(tag) => store.delete_prefix(&namespace.tag_entry_dir(tag)).await?,
            LinkKind::Digest(digest) => {
                store
                    .delete(&namespace.revision_record_path(digest))
                    .await?;
            }
            LinkKind::Referrer { subject, referrer } => {
                store
                    .delete(&namespace.referrer_record_path(subject, referrer))
                    .await?;
            }
            _ => {}
        }
    }
    Ok(())
}

pub async fn create_link(
    m: &Arc<MetadataStore>,
    namespace: &str,
    link: &LinkKind,
    digest: &Digest,
) {
    let namespace = Namespace::new(namespace).unwrap();
    seed_links(m, &namespace, &[(link.clone(), digest.clone())])
        .await
        .unwrap();
}

/// A metadata store of its own over the live S3 test backend, under a key
/// prefix no other test shares.
pub fn s3_metadata_store() -> Arc<MetadataStore> {
    metrics_provider::init_for_tests();
    let connection = s3_test_connection(format!("test-backend-{}", Uuid::new_v4()));
    let http =
        Arc::new(S3HttpBackend::new(&connection.to_client_config()).expect("s3 http client"));
    metadata_store_over(Arc::new(StorageS3Backend::builder(http).build()))
}

pub async fn put_blob_direct(store: &Arc<dyn ObjectStore>, content: &[u8]) -> Digest {
    let digest = Digest::sha256_of_bytes(content);
    store
        .put(&digest.blob_path(), Bytes::copy_from_slice(content))
        .await
        .unwrap();
    digest
}

/// Fetch a blob the way `resolve_get_blob` does for an explicit `repository`,
/// minus the resolver, the redirect and the event.
pub async fn get_blob(
    registry: &Registry,
    repository: &Repository,
    accepted_types: &[MediaRange],
    namespace: &Namespace,
    digest: &Digest,
    range: Option<RequestRange>,
) -> Result<Response<ResponseBody>, Error> {
    let has_access = registry
        .metadata_store()
        .can_read(namespace, digest)
        .await?;
    let request = GetBlobRequest {
        namespace: namespace.clone(),
        digest: digest.clone(),
        accepted_types: accepted_types.to_vec(),
        range,
    };
    let served = if repository.is_pull_through() {
        registry
            .get_cached_blob(repository, &request, has_access, false)
            .await?
    } else if has_access {
        registry.serve_local_blob(&request, false).await?
    } else {
        return Err(Error::BlobUnknown);
    };
    Ok(served.into_response(registry.blob_stream_frame_size())?)
}

pub async fn create_test_blob(
    registry: &Registry,
    namespace: &Namespace,
    content: &[u8],
) -> (Digest, Repository) {
    let digest = put_blob_body(registry.blob_store.as_ref(), content).await;

    let tag_link = LinkKind::Tag(Tag::new("latest").unwrap());
    let layer_link = LinkKind::ReferencedBy(digest.clone());
    seed_links(
        &registry.metadata_store,
        namespace,
        &[
            (tag_link.clone(), digest.clone()),
            (layer_link.clone(), digest.clone()),
        ],
    )
    .await
    .unwrap();

    let blob_index = registry
        .metadata_store
        .read_blob_index(&digest)
        .await
        .unwrap();
    assert!(blob_index.contains_key(namespace));
    let namespace_links = blob_index.get(namespace).unwrap();
    assert!(namespace_links.contains(&layer_link));

    let repository = repository_with_replication("test-repo", Vec::new());

    (digest, repository)
}

#[async_trait::async_trait(?Send)]
pub trait RegistryTestCase {
    fn name(&self) -> &'static str;
    fn registry(&self) -> &Registry;
    fn blob_store(&self) -> Arc<BlobStore>;
    fn metadata_store(&self) -> Arc<MetadataStore>;
    async fn cleanup(&self) {}
}

pub fn backends() -> Vec<Box<dyn RegistryTestCase>> {
    vec![
        Box::new(FSRegistryTestCase::new()),
        Box::new(S3RegistryTestCase::new()),
    ]
}

pub struct FSRegistryTestCase {
    blob_store: Arc<BlobStore>,
    metadata_store: Arc<MetadataStore>,
    registry: Arc<Registry>,
    temp_dir: TempDir,
}

impl FSRegistryTestCase {
    pub fn new() -> Self {
        let temp_dir = TempDir::new().expect("Failed to create temp dir for FSBackendConfig");
        let path = temp_dir.path().to_path_buf();

        let config = BlobStoreConfig::FS(blob_store::FsBackendConfig {
            root_dir: path.clone(),
            sync_to_disk: false,
        });
        let blob_store = Arc::new(config.build_backend().expect("fs blob backend"));

        let meta_storage: Arc<dyn ObjectStore> =
            Arc::new(StorageFsBackend::builder(&path).sync_to_disk(false).build());
        let metadata_store = metadata_store_over(meta_storage);
        let registry = create_test_registry(blob_store.clone(), metadata_store.clone());

        Self {
            blob_store,
            metadata_store,
            registry,
            temp_dir,
        }
    }

    /// A registry whose `test-repo` freezes every tag but `latest`, for the
    /// immutable-tag write path.
    pub fn with_immutable_tags() -> Self {
        let case = Self::new();
        let mut repository = repository_with_replication("test-repo", Vec::new());
        repository.immutable_tags = true;
        repository.immutable_tags_exclusions =
            vec![RegexPattern::compile("^latest$").expect("test pattern")];

        Self {
            registry: Registry::new(
                case.blob_store.clone(),
                case.metadata_store.clone(),
                single_repo_resolver("test-repo", repository),
                RegistryConfig::new(test_job_store(&case.metadata_store)),
            ),
            ..case
        }
    }

    /// Blob and metadata stores over separate roots, the split-backend topology
    /// a deployment can configure. `blob_path(digest)` then addresses different
    /// physical objects per store, so a manifest written through the metadata
    /// store would be invisible to the blob-store read path.
    pub fn with_split_backends() -> Self {
        let temp_dir = TempDir::new().expect("Failed to create temp dir for split backends");
        let blob_path = temp_dir.path().join("blob");
        let meta_path = temp_dir.path().join("meta");

        let config = BlobStoreConfig::FS(blob_store::FsBackendConfig {
            root_dir: blob_path,
            sync_to_disk: false,
        });
        let blob_store = Arc::new(config.build_backend().expect("fs blob backend"));

        let meta_storage: Arc<dyn ObjectStore> = Arc::new(
            StorageFsBackend::builder(&meta_path)
                .sync_to_disk(false)
                .build(),
        );
        let metadata_store = metadata_store_over(meta_storage);
        let registry = create_test_registry(blob_store.clone(), metadata_store.clone());

        Self {
            blob_store,
            metadata_store,
            registry,
            temp_dir,
        }
    }

    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    pub fn temp_dir(&self) -> &TempDir {
        &self.temp_dir
    }
}

#[async_trait::async_trait(?Send)]
impl RegistryTestCase for FSRegistryTestCase {
    fn name(&self) -> &'static str {
        "fs"
    }

    fn registry(&self) -> &Registry {
        &self.registry
    }

    fn blob_store(&self) -> Arc<BlobStore> {
        self.blob_store.clone()
    }

    fn metadata_store(&self) -> Arc<MetadataStore> {
        self.metadata_store.clone()
    }
}

pub struct S3RegistryTestCase {
    key_prefix: String,
    s3_blob_store: Arc<BlobStore>,
    s3_metadata_store: Arc<MetadataStore>,
    s3_registry: Arc<Registry>,
}

impl S3RegistryTestCase {
    pub fn new() -> Self {
        let key_prefix = format!("test-{}", Uuid::new_v4());

        let connection = s3_test_connection(key_prefix.clone());

        let s3_config = blob_store::S3BackendConfig {
            connection: connection.clone(),
            transport: blob_store::TransportFields {
                multipart_copy_threshold: ByteSize::mib(5),
                multipart_copy_chunk_size: ByteSize::mib(5),
                multipart_part_size: ByteSize::mib(5),
                ..blob_store::TransportFields::default()
            },
        };
        let blob_store = Arc::new(
            BlobStoreConfig::S3(s3_config)
                .build_backend()
                .expect("s3 blob backend"),
        );

        let meta_http =
            Arc::new(S3HttpBackend::new(&connection.to_client_config()).expect("s3 http client"));
        let meta_object_store: Arc<dyn ObjectStore> =
            Arc::new(StorageS3Backend::builder(meta_http).build());
        let metadata_store = metadata_store_over(meta_object_store);

        let registry = create_test_registry(blob_store.clone(), metadata_store.clone());

        Self {
            key_prefix,
            s3_blob_store: blob_store,
            s3_metadata_store: metadata_store,
            s3_registry: registry,
        }
    }
}

#[async_trait::async_trait(?Send)]
impl RegistryTestCase for S3RegistryTestCase {
    fn name(&self) -> &'static str {
        "s3"
    }

    fn registry(&self) -> &Registry {
        &self.s3_registry
    }

    fn blob_store(&self) -> Arc<BlobStore> {
        self.s3_blob_store.clone()
    }

    fn metadata_store(&self) -> Arc<MetadataStore> {
        self.s3_metadata_store.clone()
    }

    async fn cleanup(&self) {
        if let Err(e) = self
            .s3_blob_store
            .object_store()
            .delete_prefix(&self.key_prefix)
            .await
        {
            println!("Warning: Failed to clean up S3RegistryTestCase data: {e:?}");
        }
    }
}

/// A `RegistryClient` pointed at `uri` with a fresh in-memory cache; callers
/// pass a placeholder URI when the client is never dialed.
pub fn downstream_client(uri: &str) -> Arc<RegistryClient> {
    let backend = angos_cache::Config::Memory.to_backend().unwrap();
    Arc::new(RegistryClient::new(
        uri.to_string(),
        reqwest::Client::new(),
        None,
        backend,
        DEFAULT_MAX_MANIFEST_SIZE_BYTES,
    ))
}

/// A test `Repository` named `name` carrying `replication` downstreams. The
/// sole `Repository` test literal lives here, so a new struct field is edited
/// in one place.
pub fn repository_with_replication(
    name: &str,
    replication: Vec<ReplicationDownstream>,
) -> Repository {
    Repository {
        name: Namespace::new(name).unwrap(),
        namespace: None,
        upstreams: Vec::new(),
        replication,
        retention_policy: RetentionPolicy::new(
            &RetentionPolicyConfig::default(),
            Arc::new(SystemClock),
        ),
        immutable_tags: false,
        immutable_tags_exclusions: Vec::new(),
        scan: false,
        index: false,
    }
}

/// A `Repository` carrying exactly one match-all event+reconcile downstream
/// named "eu-region", backed by `client`.
pub fn repository_with_downstream(name: &str, client: Arc<RegistryClient>) -> Repository {
    repository_with_replication(
        name,
        vec![ReplicationDownstream::new(
            "eu-region".to_string(),
            client,
            4,
        )],
    )
}

/// Decode the payload of the sole pending replication job, panicking unless
/// exactly one is pending.
pub async fn sole_pending_payload(job_store: &JobStore) -> ReplicationJob {
    let keys = job_store
        .list_pending(Queue::Replication, 16)
        .await
        .unwrap();
    assert_eq!(
        keys.len(),
        1,
        "expected exactly one pending replication job"
    );
    let envelope = job_store
        .read_pending(Queue::Replication, &keys[0])
        .await
        .unwrap();
    assert_eq!(envelope.queue, Queue::Replication);
    serde_json::from_value(envelope.payload).expect("decode ReplicationJob")
}

/// Seed a config blob, a layer blob, a manifest referencing both, and a `v1`
/// tag link under `namespace`, returning the (manifest, config, layer) digests.
pub async fn seed_manifest(
    store: &Arc<dyn ObjectStore>,
    metadata_store: &MetadataStore,
    namespace: &Namespace,
) -> (Digest, Digest, Digest) {
    let config_bytes = br#"{"config":true}"#.to_vec();
    let layer_bytes = b"layer-bytes".to_vec();

    let config_digest = put_blob_direct(store, &config_bytes).await;
    let layer_digest = put_blob_direct(store, &layer_bytes).await;

    let manifest = json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": config_digest.to_string(),
            "size": config_bytes.len(),
        },
        "layers": [{
            "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
            "digest": layer_digest.to_string(),
            "size": layer_bytes.len(),
        }],
    });
    let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
    let manifest_digest = put_blob_direct(store, &manifest_bytes).await;

    seed_links(
        metadata_store,
        namespace,
        &[
            (
                LinkKind::Tag(Tag::new("v1").unwrap()),
                manifest_digest.clone(),
            ),
            (
                LinkKind::ReferencedBy(manifest_digest.clone()),
                config_digest.clone(),
            ),
            (
                LinkKind::ReferencedBy(manifest_digest.clone()),
                layer_digest.clone(),
            ),
        ],
    )
    .await
    .unwrap();

    (manifest_digest, config_digest, layer_digest)
}

/// The value of `name` on a served response, panicking when absent: a test
/// asserting on a header has already decided the response must carry it.
pub fn response_header<'a>(
    response: &'a Response<ResponseBody>,
    name: &HeaderName,
) -> &'a HeaderValue {
    response
        .headers()
        .get(name)
        .unwrap_or_else(|| panic!("response must carry the {name} header"))
}

/// The `Docker-Content-Digest` a response served.
pub fn response_digest(response: &Response<ResponseBody>) -> Digest {
    response_header(response, &DOCKER_CONTENT_DIGEST)
        .to_str()
        .expect("a digest is printable ASCII")
        .parse()
        .expect("the served digest must parse")
}

/// The `Docker-Upload-UUID` an upload session response served.
pub fn response_session_id(response: &Response<ResponseBody>) -> UploadSessionId {
    UploadSessionId::new(
        response_header(response, &DOCKER_UPLOAD_UUID)
            .to_str()
            .expect("a session id is printable ASCII"),
    )
    .expect("the served session id must parse")
}

pub async fn response_body(response: Response<ResponseBody>) -> Vec<u8> {
    response
        .into_body()
        .collect()
        .await
        .expect("the body must read")
        .to_bytes()
        .to_vec()
}

/// The JSON body a listing response served.
pub async fn response_json(response: Response<ResponseBody>) -> serde_json::Value {
    serde_json::from_slice(&response_body(response).await).expect("the body must be JSON")
}
