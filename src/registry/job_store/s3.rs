use std::{
    io::{Error as IoError, ErrorKind},
    path::Path,
    sync::Arc,
};

use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use serde::{Deserialize, Serialize};
use tracing::debug;
use uuid::Uuid;

use crate::{
    registry::{
        job_store::{Error, JobEnvelope, JobStore, serialize_dead_letter},
        path_builder,
    },
    s3_client,
    secret::Secret,
};

#[derive(Clone, Debug, Deserialize)]
pub struct BackendConfig {
    pub bucket: String,
    #[serde(default = "default_prefix")]
    pub prefix: String,
    pub endpoint: String,
    #[serde(default = "default_region")]
    pub region: String,
    pub access_key_id: Secret<String>,
    pub secret_key: Secret<String>,
}

fn default_prefix() -> String {
    "_jobs".to_string()
}

fn default_region() -> String {
    "us-east-1".to_string()
}

/// Lease file payload stored in S3 at `_jobs/leases/<lock_key_encoded>.json`.
/// Lease expiry is driven by the S3 `Last-Modified` header; `worker_id` is kept
/// for diagnostics only.
#[derive(Serialize, Deserialize)]
struct LeaseFile {
    worker_id: String,
    ttl_secs: u64,
}

/// S3-backed `JobStore`.
///
/// Uses the same CAS primitives as the S3 lock backend:
/// - `put_object_if_not_exists` for atomic lease creation
/// - `put_object_if_match` for lease heartbeat and theft
/// - `delete` / `delete_if_match` for release
pub struct Backend {
    backend: Arc<s3_client::Backend>,
}

impl Backend {
    pub fn new(config: &BackendConfig) -> Result<Self, Error> {
        let backend_config = s3_client::BackendConfig {
            access_key_id: config.access_key_id.clone(),
            secret_key: config.secret_key.clone(),
            endpoint: config.endpoint.clone(),
            bucket: config.bucket.clone(),
            region: config.region.clone(),
            key_prefix: config.prefix.clone(),
            ..Default::default()
        };
        let backend = s3_client::Backend::new(&backend_config)
            .map_err(|e| Error::Initialization(e.to_string()))?;
        Ok(Self {
            backend: Arc::new(backend),
        })
    }

    async fn try_steal_stale_lease(
        &self,
        lock_key: &str,
        worker_id: &str,
        ttl_secs: u64,
    ) -> Result<Option<String>, Error> {
        let key = path_builder::job_lease_path(lock_key);
        let data = serialize_lease(worker_id, ttl_secs)?;

        let (existing_bytes, etag, last_modified) =
            match self.backend.read_with_metadata(&key).await {
                Ok(r) => r,
                Err(e) if e.kind() == ErrorKind::NotFound => {
                    // Vanished between the failed put_object_if_not_exists and
                    // now; retry the initial create.
                    return match self.backend.put_object_if_not_exists(&key, data).await {
                        Ok(etag) => Ok(Some(etag.unwrap_or_else(|| Uuid::new_v4().to_string()))),
                        Err(s3_client::Error::PreconditionFailed) => Ok(None),
                        Err(e) => Err(Error::Storage(format!("retry create lease failed: {e}"))),
                    };
                }
                Err(e) => return Err(Error::Storage(format!("failed to read lease: {e}"))),
            };

        let existing: LeaseFile = serde_json::from_slice(&existing_bytes)
            .map_err(|e| Error::Storage(format!("corrupt lease: {e}")))?;

        let last_modified = last_modified.unwrap_or_else(Utc::now);
        let ttl = ChronoDuration::seconds(existing.ttl_secs.min(3600).cast_signed());
        if Utc::now() <= last_modified + ttl {
            return Ok(None);
        }

        let Some(stale_etag) = etag else {
            return Ok(None);
        };

        debug!(
            lock_key,
            worker_id = existing.worker_id,
            "Stealing stale S3 lease"
        );

        match self
            .backend
            .put_object_if_match(&key, &stale_etag, data)
            .await
        {
            Ok(new_etag) => Ok(Some(new_etag.unwrap_or_else(|| Uuid::new_v4().to_string()))),
            Err(s3_client::Error::PreconditionFailed) => Ok(None),
            Err(e) => Err(Error::Storage(format!("steal lease failed: {e}"))),
        }
    }
}

fn s3_io_to_job_err(e: &IoError) -> Error {
    if e.kind() == ErrorKind::NotFound {
        Error::NotFound
    } else {
        Error::Storage(e.to_string())
    }
}

fn serialize_lease(worker_id: &str, ttl_secs: u64) -> Result<Vec<u8>, Error> {
    serde_json::to_vec(&LeaseFile {
        worker_id: worker_id.to_string(),
        ttl_secs,
    })
    .map_err(|e| Error::Storage(format!("lease serialization failed: {e}")))
}

#[async_trait]
impl JobStore for Backend {
    async fn put_pending(
        &self,
        queue: &str,
        id: &str,
        envelope: &JobEnvelope,
    ) -> Result<(), Error> {
        let key = path_builder::job_pending_path(queue, id);
        let data = serde_json::to_vec(envelope)
            .map_err(|e| Error::Storage(format!("envelope serialization failed: {e}")))?;
        self.backend
            .put_object(&key, data)
            .await
            .map_err(|e| s3_io_to_job_err(&e))
    }

    async fn list_pending(&self, queue: &str, n: u16) -> Result<Vec<String>, Error> {
        let prefix = format!("{}/", path_builder::job_pending_dir(queue));
        let (all_keys, _next_token) = self
            .backend
            .list_objects(&prefix, 1000, None)
            .await
            .map_err(|e| s3_io_to_job_err(&e))?;

        Ok(all_keys
            .into_iter()
            .filter(|name| Path::new(name).extension().is_some_and(|ext| ext == "json"))
            .take(n as usize)
            .filter_map(|name| name.strip_suffix(".json").map(str::to_string))
            .collect())
    }

    async fn read_pending(&self, queue: &str, id: &str) -> Result<JobEnvelope, Error> {
        let key = path_builder::job_pending_path(queue, id);
        let data = self
            .backend
            .read(&key)
            .await
            .map_err(|e| s3_io_to_job_err(&e))?;
        serde_json::from_slice(&data)
            .map_err(|e| Error::Storage(format!("failed to parse envelope: {e}")))
    }

    async fn try_create_lease(
        &self,
        lock_key: &str,
        worker_id: &str,
        ttl_secs: u64,
    ) -> Result<Option<String>, Error> {
        let key = path_builder::job_lease_path(lock_key);
        let data = serialize_lease(worker_id, ttl_secs)?;
        match self.backend.put_object_if_not_exists(&key, data).await {
            Ok(etag) => Ok(Some(etag.unwrap_or_else(|| Uuid::new_v4().to_string()))),
            Err(s3_client::Error::PreconditionFailed) => {
                self.try_steal_stale_lease(lock_key, worker_id, ttl_secs)
                    .await
            }
            Err(e) => Err(Error::Storage(format!("failed to create lease: {e}"))),
        }
    }

    async fn heartbeat_lease(
        &self,
        lock_key: &str,
        token: &str,
        worker_id: &str,
        ttl_secs: u64,
    ) -> Result<String, Error> {
        let key = path_builder::job_lease_path(lock_key);
        let data = serialize_lease(worker_id, ttl_secs)?;
        match self.backend.put_object_if_match(&key, token, data).await {
            Ok(new_etag) => Ok(new_etag.unwrap_or_else(|| token.to_string())),
            Err(s3_client::Error::PreconditionFailed) => Err(Error::Storage(
                "heartbeat failed: lease ownership changed".to_string(),
            )),
            Err(e) => Err(Error::Storage(format!("heartbeat S3 error: {e}"))),
        }
    }

    async fn remove_pending(&self, queue: &str, id: &str) -> Result<(), Error> {
        let key = path_builder::job_pending_path(queue, id);
        match self.backend.delete(&key).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
            Err(e) => Err(Error::Storage(format!("failed to delete pending: {e}"))),
        }
    }

    async fn remove_lease(&self, lock_key: &str, _token: &str) -> Result<(), Error> {
        let key = path_builder::job_lease_path(lock_key);
        match self.backend.delete(&key).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
            Err(e) => Err(Error::Storage(format!("failed to delete lease: {e}"))),
        }
    }

    async fn move_to_failed(
        &self,
        queue: &str,
        id: &str,
        envelope: &JobEnvelope,
        last_error: &str,
    ) -> Result<(), Error> {
        let data = serialize_dead_letter(envelope, last_error)?;
        let failed_key = path_builder::job_failed_path(queue, id);
        self.backend
            .put_object(&failed_key, data)
            .await
            .map_err(|e| s3_io_to_job_err(&e))?;
        self.remove_pending(queue, id).await
    }

    async fn count_pending(&self, queue: &str) -> Result<u64, Error> {
        let prefix = format!("{}/", path_builder::job_pending_dir(queue));
        let mut count: u64 = 0;
        let mut token: Option<String> = None;
        loop {
            let (keys, next) = self
                .backend
                .list_objects(&prefix, 1000, token)
                .await
                .map_err(|e| s3_io_to_job_err(&e))?;
            count += keys
                .iter()
                .filter(|k| Path::new(k).extension().is_some_and(|ext| ext == "json"))
                .count() as u64;
            match next {
                Some(t) => token = Some(t),
                None => return Ok(count),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc, time::Duration};

    use tokio::{io::AsyncReadExt, time::sleep};
    use uuid::Uuid;

    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use crate::{
        cache,
        command::worker::runner::run_once,
        metrics_provider,
        oci::Namespace,
        policy::{AccessMode, AccessPolicyConfig, RetentionPolicyConfig},
        registry::{
            GetBlobResponse, Registry, RegistryConfig, Repository,
            blob_ownership::BlobOwnership,
            blob_store::fs::{Backend as BlobBackend, BackendConfig as BlobBackendConfig},
            cache_job_handler::CacheJobHandler,
            job_store::{
                JobEnvelope, JobQueue, JobStore,
                durable::{DurableJobConsumer, DurableJobQueue},
                s3::{Backend, LeaseFile},
            },
            manifest::DEFAULT_MAX_MANIFEST_SIZE_BYTES,
            metadata_store::{
                LockStrategy,
                fs::{Backend as MetaBackend, BackendConfig as MetaBackendConfig},
            },
            path_builder,
            repository::{Config as RepositoryConfig, RegistryClientConfig},
            repository_resolver::RepositoryResolver,
        },
        s3_client::{Backend as S3Backend, BackendConfig as S3BackendConfig},
        secret::Secret,
        util::sha256,
    };

    const REPO_NAME: &str = "local";
    const NAMESPACE: &str = "local/cache-ns";

    fn make_fs_backends(path: &str) -> (Arc<BlobBackend>, Arc<MetaBackend>) {
        let blob = Arc::new(BlobBackend::new(&BlobBackendConfig {
            root_dir: path.to_string(),
            sync_to_disk: false,
        }));
        let meta = Arc::new(
            MetaBackend::new(&MetaBackendConfig {
                root_dir: path.to_string(),
                sync_to_disk: false,
                lock_strategy: LockStrategy::Memory,
            })
            .expect("fs metadata backend"),
        );
        (blob, meta)
    }

    async fn make_pull_through_repository(upstream_url: &str) -> Repository {
        let auth_cache = cache::Config::Memory
            .to_backend()
            .expect("memory auth cache");
        let config = RepositoryConfig {
            upstream: vec![RegistryClientConfig {
                url: upstream_url.to_string(),
                max_redirect: 5,
                server_ca_bundle: None,
                client_certificate: None,
                client_private_key: None,
                username: None,
                password: None,
            }],
            access_policy: AccessPolicyConfig {
                default: AccessMode::Allow,
                ..AccessPolicyConfig::default()
            },
            retention_policy: RetentionPolicyConfig::default(),
            ..RepositoryConfig::default()
        };
        Repository::new(
            REPO_NAME,
            &config,
            &auth_cache,
            DEFAULT_MAX_MANIFEST_SIZE_BYTES,
        )
        .await
        .expect("repository must build")
    }

    fn make_resolver(repo: Repository) -> Arc<RepositoryResolver> {
        let mut map = HashMap::new();
        map.insert(REPO_NAME.to_string(), repo);
        Arc::new(RepositoryResolver::new(Arc::new(map)).expect("resolver must build"))
    }

    /// Build a raw `Backend` and a matching `s3::Backend` job-store that share
    /// the same key prefix so the test can use `backend.delete_prefix` for
    /// cleanup.
    fn test_store_with_backend(prefix: &str) -> (Arc<Backend>, Arc<S3Backend>) {
        metrics_provider::init_for_tests();
        let config = S3BackendConfig {
            access_key_id: Secret::new("root".to_string()),
            secret_key: Secret::new("roottoor".to_string()),
            endpoint: "http://127.0.0.1:9000".to_string(),
            bucket: "registry".to_string(),
            region: "us-east-1".to_string(),
            key_prefix: format!("{prefix}{}_", Uuid::new_v4()),
            ..Default::default()
        };
        let raw = Arc::new(S3Backend::new(&config).expect("backend"));
        (
            Arc::new(Backend {
                backend: raw.clone(),
            }),
            raw,
        )
    }

    fn make_consumer(store: Arc<Backend>) -> DurableJobConsumer {
        DurableJobConsumer::new(store, 30, "test-worker".to_string())
    }

    #[tokio::test]
    async fn s3_claim_and_complete() {
        let (store, _) = test_store_with_backend("jq_claim_");
        let queue = DurableJobQueue::new(store.clone());
        let consumer = make_consumer(store);

        queue
            .enqueue(
                JobEnvelope::new("cache", "test.noop", "cache.ns:sha256:s3claim", &())
                    .expect("envelope"),
            )
            .await
            .expect("enqueue");

        let claimed = consumer
            .claim_one("cache")
            .await
            .expect("claim_one")
            .expect("Some");
        consumer.complete(claimed).await.expect("complete");
        assert!(
            consumer.claim_one("cache").await.expect("claim").is_none(),
            "queue must be empty after complete"
        );
    }

    #[tokio::test]
    async fn s3_stale_lease_recovery() {
        let (store, _) = test_store_with_backend("jq_stale_");
        let consumer = make_consumer(store.clone());

        let env = JobEnvelope::new("cache", "test.noop", "cache.ns:sha256:s3stale", &())
            .expect("envelope");
        store
            .put_pending("cache", &env.id, &env)
            .await
            .expect("put");

        // 3-second TTL relies on the S3 LastModified header to drive expiry.
        let data = serde_json::to_vec(&LeaseFile {
            worker_id: "dead-worker".to_string(),
            ttl_secs: 3,
        })
        .expect("serialize");
        store
            .backend
            .put_object(&path_builder::job_lease_path(&env.lock_key), data)
            .await
            .expect("write stale lease");

        sleep(Duration::from_secs(4)).await;

        let claimed = consumer
            .claim_one("cache")
            .await
            .expect("claim after stale")
            .expect("Some");
        consumer.complete(claimed).await.expect("complete");
    }

    /// Single-replica durable round-trip using `s3::Backend`. Blob and metadata
    /// stores stay FS-backed in a tempdir; only the job queue uses S3.
    #[tokio::test]
    async fn durable_cache_fill_round_trip_s3() {
        metrics_provider::init_for_tests();

        let blob_content = b"s3 durable cache e2e blob content";
        let digest = sha256::digest(blob_content);

        let mock_server = MockServer::start().await;
        let blob_path = format!("/v2/cache-ns/blobs/{digest}");

        // Mount twice: once for the client-path stream, once for the worker fetch.
        for _ in 0..2 {
            Mock::given(method("GET"))
                .and(path(&blob_path))
                .respond_with(ResponseTemplate::new(200).set_body_bytes(blob_content.as_slice()))
                .mount(&mock_server)
                .await;
        }

        let dir = tempfile::TempDir::new().expect("tempdir");
        let (blob_backend, meta_backend) = make_fs_backends(&dir.path().to_string_lossy());

        let (store, raw_backend) = test_store_with_backend("e2e_rtrip_");
        let durable_queue: Arc<dyn JobQueue> = Arc::new(DurableJobQueue::new(store.clone()));

        let resolver = make_resolver(make_pull_through_repository(&mock_server.uri()).await);

        let registry = Registry::new(
            blob_backend.clone(),
            blob_backend.clone(),
            None,
            meta_backend.clone(),
            resolver.clone(),
            RegistryConfig::default().job_queue(durable_queue),
        )
        .expect("Registry::new");

        let namespace = Namespace::new(NAMESPACE).expect("namespace");
        let repo = resolver.resolve(&namespace).expect("repo in resolver");

        // Cache miss — streams from upstream and enqueues a cache-fill job to S3.
        let response = registry
            .get_blob(repo, &[], &namespace, &digest, None)
            .await
            .expect("get_blob must succeed on pull-through miss");

        match response {
            GetBlobResponse::Reader { mut body, .. } => {
                let mut buf = Vec::new();
                body.read_to_end(&mut buf).await.expect("drain body");
                assert_eq!(buf, blob_content);
            }
            _ => panic!("expected Reader response for pull-through miss"),
        }

        // Exactly one envelope must be pending in S3.
        let pending_count = store
            .count_pending("cache")
            .await
            .expect("count_pending after enqueue");
        assert_eq!(pending_count, 1, "exactly one pending envelope expected");

        // Build a consumer + handler and drive the worker once.
        let consumer = Arc::new(DurableJobConsumer::new(
            store.clone(),
            30,
            "e2e-worker".to_string(),
        ));
        let handler =
            CacheJobHandler::new(resolver.clone(), blob_backend.clone(), meta_backend.clone());

        let found = run_once(&consumer, &handler, "cache")
            .await
            .expect("run_once");
        assert!(found, "worker must find and process the pending job");

        // Blob must now be locally owned.
        assert!(
            BlobOwnership::new(meta_backend.as_ref())
                .can_read(&namespace, &digest)
                .await
                .expect("can_read"),
            "blob must be locally owned after worker run"
        );

        // Pending queue must be empty.
        let remaining = store
            .count_pending("cache")
            .await
            .expect("count_pending after worker");
        assert_eq!(remaining, 0, "pending queue must be empty after completion");

        // Best-effort S3 cleanup so parallel test runs don't see stale state.
        let _ = raw_backend.delete_prefix("_jobs/").await;
    }
}
