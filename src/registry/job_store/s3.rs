use std::{
    io::{Error as IoError, ErrorKind},
    path::Path,
    sync::Arc,
};

use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::{
    registry::{
        job_store::{
            Error, JobEnvelope, JobStore, MAX_REPORTED_PENDING, STORAGE_KEY_PREFIX_LEN,
            make_storage_key, parse_lock_key_index, pending_ready_cutoff_prefix,
            serialize_dead_letter, serialize_lock_key_index,
        },
        path_builder,
    },
    secret::Secret,
};
use angos_s3_client as s3_client;

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
    /// Whether the storage service honors `DELETE` with `If-Match: <etag>`.
    /// Defaults to `true` (AWS S3, R2, GCS, modern `MinIO`). Set to `false` on
    /// endpoints without conditional delete; release then falls back to an
    /// unconditional `DELETE` with a small race window during lease theft.
    #[serde(default = "default_delete_if_match")]
    pub delete_if_match: bool,
}

fn default_prefix() -> String {
    "_jobs".to_string()
}

fn default_region() -> String {
    "us-east-1".to_string()
}

fn default_delete_if_match() -> bool {
    true
}

/// Lease file payload stored in S3 at `_jobs/leases/<lock_key_encoded>.json`.
///
/// Staleness is driven by `refreshed_at` in the body, *not* the S3
/// `Last-Modified` response header. Some S3-compatible endpoints strip
/// `Last-Modified` from `PUT` responses; relying on the body lets us steal
/// stale leases on those services too. `refreshed_at` is rewritten on every
/// successful create/heartbeat/steal — so it tracks the worker-asserted
/// freshness, matching what the heartbeat loop already implicitly relies on.
/// `worker_id` is kept for diagnostics only.
#[derive(Serialize, Deserialize)]
struct LeaseFile {
    /// Wall-clock instant the body was last written. Pre-fix leases without
    /// this field deserialize to `MIN_UTC` (immediately stealable), which is
    /// the safe default during the transitional window.
    #[serde(default)]
    refreshed_at: DateTime<Utc>,
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
    delete_if_match: bool,
}

impl Backend {
    pub fn new(config: &BackendConfig) -> Result<Self, Error> {
        let backend_config = s3_client::BackendConfig {
            access_key_id: config.access_key_id.expose().clone(),
            secret_key: config.secret_key.expose().clone(),
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
            delete_if_match: config.delete_if_match,
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

        // Staleness is driven by the body's `refreshed_at`, not the S3
        // `Last-Modified` header — some S3-compatible endpoints strip
        // `Last-Modified` from `PUT` responses and would otherwise leave
        // crashed leases stuck forever.
        let (existing_bytes, etag) = match self.backend.read_with_etag(&key).await {
            Ok(r) => r,
            Err(e) if e.kind() == ErrorKind::NotFound => {
                // Vanished between the failed put_object_if_not_exists and
                // now; retry the initial create.
                return match self.backend.put_object_if_not_exists(&key, data).await {
                    Ok(etag) => Ok(Some(require_etag(etag)?)),
                    Err(s3_client::Error::PreconditionFailed) => Ok(None),
                    Err(e) => Err(Error::Storage(format!("retry create lease failed: {e}"))),
                };
            }
            Err(e) => return Err(Error::Storage(format!("failed to read lease: {e}"))),
        };

        let existing: LeaseFile = serde_json::from_slice(&existing_bytes)
            .map_err(|e| Error::Storage(format!("corrupt lease: {e}")))?;

        let ttl = ChronoDuration::seconds(existing.ttl_secs.min(3600).cast_signed());
        if Utc::now() <= existing.refreshed_at + ttl {
            return Ok(None);
        }

        let Some(stale_etag) = etag else {
            return Ok(None);
        };

        debug!(
            lock_key,
            worker_id = existing.worker_id,
            refreshed_at = %existing.refreshed_at,
            "Stealing stale S3 lease"
        );

        match self
            .backend
            .put_object_if_match(&key, &stale_etag, data)
            .await
        {
            Ok(new_etag) => Ok(Some(require_etag(new_etag)?)),
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
        refreshed_at: Utc::now(),
        worker_id: worker_id.to_string(),
        ttl_secs,
    })
    .map_err(|e| Error::Storage(format!("lease serialization failed: {e}")))
}

fn require_etag(etag: Option<String>) -> Result<String, Error> {
    etag.ok_or_else(|| Error::Storage("S3 PUT response is missing an ETag".to_string()))
}

#[async_trait]
impl JobStore for Backend {
    async fn put_pending(
        &self,
        queue: &str,
        envelope: &JobEnvelope,
        not_before: DateTime<Utc>,
    ) -> Result<String, Error> {
        let storage_key = make_storage_key(not_before, &envelope.id);
        let key = path_builder::job_pending_path(queue, &storage_key);
        let data = serde_json::to_vec(envelope)
            .map_err(|e| Error::Storage(format!("envelope serialization failed: {e}")))?;
        self.backend
            .put_object(&key, data)
            .await
            .map_err(|e| s3_io_to_job_err(&e))?;
        Ok(storage_key)
    }

    async fn list_pending(&self, queue: &str, n: u16) -> Result<Vec<String>, Error> {
        // S3 LIST returns keys in lexicographic order. Storage keys start
        // with a fixed-width hex unix-millis prefix, so this is also
        // `not_before` order without any extra sort.
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

    async fn read_pending(&self, queue: &str, storage_key: &str) -> Result<JobEnvelope, Error> {
        let key = path_builder::job_pending_path(queue, storage_key);
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
            Ok(etag) => Ok(Some(require_etag(etag)?)),
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

    async fn remove_pending(&self, queue: &str, storage_key: &str) -> Result<(), Error> {
        let key = path_builder::job_pending_path(queue, storage_key);
        match self.backend.delete(&key).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
            Err(e) => Err(Error::Storage(format!("failed to delete pending: {e}"))),
        }
    }

    async fn remove_lease(&self, lock_key: &str, token: &str) -> Result<(), Error> {
        let key = path_builder::job_lease_path(lock_key);
        if self.delete_if_match {
            // ETag mismatch means another worker already stole the lease;
            // treat as success rather than failing the completing worker.
            match self.backend.delete_if_match(&key, token).await {
                Ok(()) | Err(s3_client::Error::PreconditionFailed) => Ok(()),
                Err(e) => Err(Error::Storage(format!("failed to delete lease: {e}"))),
            }
        } else {
            match self.backend.delete(&key).await {
                Ok(()) => Ok(()),
                Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
                Err(e) => Err(Error::Storage(format!("failed to delete lease: {e}"))),
            }
        }
    }

    async fn move_to_failed(
        &self,
        queue: &str,
        storage_key: &str,
        envelope: &JobEnvelope,
        last_error: &str,
    ) -> Result<(), Error> {
        let data = serialize_dead_letter(envelope, last_error)?;
        let failed_key = path_builder::job_failed_path(queue, storage_key);
        self.backend
            .put_object(&failed_key, data)
            .await
            .map_err(|e| s3_io_to_job_err(&e))?;
        self.remove_pending(queue, storage_key).await?;
        // Drop the dedup index only if it still references this storage key —
        // a concurrent retry may have re-pointed it at a fresher envelope.
        self.remove_lock_key_index_if_matches(queue, &envelope.lock_key, storage_key)
            .await
    }

    /// O(1) dedup check backed by the per-`lock_key` index object. If the
    /// index references a `storage_key` that no longer exists (left over by
    /// a crash mid-`complete`/`move_to_failed`), the orphan is deleted and
    /// the check reports no-pending so the next enqueue proceeds.
    async fn find_pending_with_lock_key(&self, queue: &str, lock_key: &str) -> Result<bool, Error> {
        let index_path = path_builder::job_lock_key_index_path(queue, lock_key);
        let data = match self.backend.read(&index_path).await {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(e) => return Err(Error::Storage(format!("read lock-key index: {e}"))),
        };
        let index = parse_lock_key_index(&data)?;

        let pending_key = path_builder::job_pending_path(queue, &index.storage_key);
        match self.backend.object_size(&pending_key).await {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // Orphan: pending vanished but index lingers. Clear it.
                if let Err(remove_err) = self.backend.delete(&index_path).await {
                    warn!(
                        lock_key,
                        error = %remove_err,
                        "Failed to remove orphan lock-key index"
                    );
                }
                Ok(false)
            }
            Err(e) => Err(Error::Storage(format!(
                "HEAD pending via lock-key index: {e}"
            ))),
        }
    }

    async fn remove_lock_key_index_if_matches(
        &self,
        queue: &str,
        lock_key: &str,
        storage_key: &str,
    ) -> Result<(), Error> {
        let index_path = path_builder::job_lock_key_index_path(queue, lock_key);
        // Read body + etag so we can do a conditional delete and avoid a TOCTOU
        // against a concurrent retry that re-pointed the index.
        let (data, etag) = match self.backend.read_with_etag(&index_path).await {
            Ok(r) => r,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => {
                warn!(lock_key, error = %e, "Failed to read lock-key index before remove");
                return Ok(());
            }
        };
        match parse_lock_key_index(&data) {
            Ok(index) if index.storage_key == storage_key => {}
            Ok(_) => return Ok(()), // index points elsewhere; leave it
            Err(_) => {}            // corrupt — fall through and delete
        }

        if let Some(etag) = etag {
            match self.backend.delete_if_match(&index_path, &etag).await {
                Ok(()) | Err(s3_client::Error::PreconditionFailed) => Ok(()),
                Err(e) => {
                    warn!(lock_key, error = %e, "Failed conditional delete of lock-key index");
                    Ok(())
                }
            }
        } else {
            // Endpoint didn't return ETag — fall back to unconditional delete.
            match self.backend.delete(&index_path).await {
                Ok(()) => Ok(()),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(e) => {
                    warn!(lock_key, error = %e, "Failed to delete lock-key index");
                    Ok(())
                }
            }
        }
    }

    async fn try_claim_lock_key(
        &self,
        queue: &str,
        lock_key: &str,
        storage_key: &str,
    ) -> Result<bool, Error> {
        let path = path_builder::job_lock_key_index_path(queue, lock_key);
        let data = serialize_lock_key_index(storage_key)?;
        match self.backend.put_object_if_not_exists(&path, data).await {
            Ok(_) => Ok(true),
            Err(s3_client::Error::PreconditionFailed) => Ok(false),
            Err(e) => Err(Error::Storage(format!("try_claim_lock_key: {e}"))),
        }
    }

    async fn refresh_lock_key_index(
        &self,
        queue: &str,
        lock_key: &str,
        storage_key: &str,
    ) -> Result<(), Error> {
        let path = path_builder::job_lock_key_index_path(queue, lock_key);
        let data = serialize_lock_key_index(storage_key)?;
        self.backend
            .put_object(&path, data)
            .await
            .map_err(|e| s3_io_to_job_err(&e))
    }

    async fn count_pending(&self, queue: &str, ready_horizon_secs: u64) -> Result<u64, Error> {
        let prefix = format!("{}/", path_builder::job_pending_dir(queue));
        let cutoff_prefix = pending_ready_cutoff_prefix(ready_horizon_secs);
        let mut count: u64 = 0;
        let mut token: Option<String> = None;
        loop {
            let (keys, next) = self
                .backend
                .list_objects(&prefix, 1000, token)
                .await
                .map_err(|e| s3_io_to_job_err(&e))?;
            for k in &keys {
                if Path::new(k).extension().is_none_or(|ext| ext != "json") {
                    continue;
                }
                // S3 LIST returns keys in lexicographic order; the storage-key
                // prefix is `not_before` in fixed-width hex, so the first key
                // whose prefix is past the readiness cutoff terminates the
                // count — every later key is even further in the future.
                if let Some(p) = k.get(..STORAGE_KEY_PREFIX_LEN)
                    && p > cutoff_prefix.as_str()
                {
                    return Ok(count.min(MAX_REPORTED_PENDING));
                }
                count += 1;
                if count >= MAX_REPORTED_PENDING {
                    return Ok(MAX_REPORTED_PENDING);
                }
            }
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

    use chrono::Utc;
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
        util::sha256,
    };
    use angos_s3_client::{Backend as S3Backend, BackendConfig as S3BackendConfig};

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
            access_key_id: "root".to_string(),
            secret_key: "roottoor".to_string(),
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
                delete_if_match: true,
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
            .claimed
            .expect("Some");
        consumer.complete(claimed).await.expect("complete");
        assert!(
            consumer
                .claim_one("cache")
                .await
                .expect("claim")
                .claimed
                .is_none(),
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
            .put_pending("cache", &env, Utc::now())
            .await
            .expect("put");

        // 3-second TTL with refreshed_at set 10s ago → body-driven staleness.
        let data = serde_json::to_vec(&LeaseFile {
            refreshed_at: Utc::now() - chrono::Duration::seconds(10),
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
            .claimed
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
            .count_pending("cache", 600)
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
            .count_pending("cache", 600)
            .await
            .expect("count_pending after worker");
        assert_eq!(remaining, 0, "pending queue must be empty after completion");

        // Best-effort S3 cleanup so parallel test runs don't see stale state.
        let _ = raw_backend.delete_prefix("_jobs/").await;
    }
}
