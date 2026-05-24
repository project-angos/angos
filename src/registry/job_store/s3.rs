//! TOML config + factory for S3-backed job queues.
//!
//! Picks the [`JobStore`] + [`LeaseBackend`] pair based on `lock_strategy`:
//!
//! - `LockStrategy::S3(_)` (default): use [`conditional::Backend`] +
//!   [`lease::conditional::Backend`]. Requires `If-None-Match` / `If-Match`
//!   support on the provider.
//! - `LockStrategy::Redis(_)` / `LockStrategy::Memory`: fall back to
//!   [`locked::Backend`] + [`lease::locked::Backend`] over the same S3
//!   `ObjectStore`, with the lock backend providing atomicity. Use these
//!   on S3-compatible providers that don't honour the conditional headers.

use std::sync::Arc;

use serde::{Deserialize, Deserializer};
use tracing::info;

use crate::{
    registry::{
        job_store::{
            Error, JobBackends, JobStore, conditional,
            lease::{self, LeaseBackend},
            locked,
        },
        metadata_store::{
            ConditionalCapabilities, LockConfig, LockStrategy,
            lock::{self, LockBackend, MemoryBackend, s3::S3LockConfig},
        },
        s3_connection::S3ConnectionConfig,
    },
    secret::Secret,
};
use angos_s3_client::Backend as S3HttpBackend;
use angos_storage::{ConditionalStore, ObjectStore, s3::Backend as StorageBackend};

/// S3 job-queue configuration. Field shape matches
/// [`crate::registry::metadata_store::s3::BackendConfig`] so operators see
/// the same keys in both `[metadata_store.s3]` and `[global.job_queue.s3]`.
#[derive(Clone, Debug, PartialEq)]
pub struct BackendConfig {
    pub connection: S3ConnectionConfig,
    /// Coordination strategy. `S3` (default) uses S3 conditional writes
    /// for both [`JobStore`] primitives and lease lifecycle. `Redis` /
    /// `Memory` fall back to `locked::Backend` + `lease::locked::Backend`,
    /// useful on S3-compatible providers without `If-Match` / `If-None-Match`
    /// support. Mirrors the `[metadata_store.s3.lock_strategy]` shape.
    pub lock_strategy: LockStrategy,
    /// Explicitly declared S3 conditional-operation capabilities, matching
    /// the `[metadata_store.s3.capabilities]` shape. Only consulted on the
    /// CAS path (`lock_strategy = "s3"`):
    /// - `put_if_none_match` and `put_if_match` are required and validated
    ///   at startup; defaults are `true` under `S3` strategy.
    /// - `delete_if_match` selects whether lease release uses conditional
    ///   delete (defaults to `true`); set `false` on endpoints that ignore
    ///   `If-Match` on `DELETE`.
    ///
    /// Ignored when `lock_strategy` is `redis` or `memory`.
    pub capabilities: Option<ConditionalCapabilities>,
}

fn default_key_prefix() -> String {
    "_jobs".to_string()
}

fn default_region() -> String {
    "us-east-1".to_string()
}

impl Default for BackendConfig {
    fn default() -> Self {
        Self {
            connection: S3ConnectionConfig {
                key_prefix: default_key_prefix(),
                ..S3ConnectionConfig::default()
            },
            lock_strategy: LockStrategy::S3(S3LockConfig::default()),
            capabilities: None,
        }
    }
}

impl<'de> Deserialize<'de> for BackendConfig {
    // Custom impl because `lock_strategy` needs `lock::resolve_lock_strategy`
    // and `key_prefix` must default to `"_jobs"` (not the empty string that
    // `S3ConnectionConfig` gives) to preserve existing operator deployments.
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Raw {
            access_key_id: Secret<String>,
            secret_key: Secret<String>,
            endpoint: String,
            bucket: String,
            #[serde(default = "default_region")]
            region: String,
            #[serde(default = "default_key_prefix")]
            key_prefix: String,
            #[serde(default)]
            redis: Option<LockConfig>,
            #[serde(default)]
            lock_strategy: Option<LockStrategy>,
            #[serde(default)]
            capabilities: Option<ConditionalCapabilities>,
        }

        let raw = Raw::deserialize(deserializer)?;

        // S3 storage with no explicit strategy defaults to `S3` (CAS); the
        // shared helper handles the `Some/Some` conflict and the
        // `None/Some(redis)` shorthand.
        let lock_strategy = if raw.lock_strategy.is_none() && raw.redis.is_none() {
            LockStrategy::S3(S3LockConfig::default())
        } else {
            lock::resolve_lock_strategy(raw.lock_strategy, raw.redis, true)?
        };

        Ok(BackendConfig {
            connection: S3ConnectionConfig {
                access_key_id: raw.access_key_id,
                secret_key: raw.secret_key,
                endpoint: raw.endpoint,
                bucket: raw.bucket,
                region: raw.region,
                key_prefix: raw.key_prefix,
            },
            lock_strategy,
            capabilities: raw.capabilities,
        })
    }
}

impl BackendConfig {
    /// Build the storage + lease backends for this S3 deployment.
    pub fn to_backends(&self) -> Result<JobBackends, Error> {
        let http = S3HttpBackend::new(&self.connection.to_client_config())
            .map_err(|e| Error::Initialization(e.to_string()))?;
        let storage = Arc::new(
            StorageBackend::builder()
                .client(Arc::new(http))
                .build()
                .map_err(|e| Error::Initialization(e.to_string()))?,
        );

        match &self.lock_strategy {
            LockStrategy::S3(_) => {
                // Under the CAS strategy, default missing capability flags to
                // `true` (the AWS S3 / R2 / GCS / modern MinIO baseline). The
                // metadata-store probe machinery isn't replicated here; if you
                // need auto-detection, set `capabilities` explicitly.
                let caps = self
                    .capabilities
                    .clone()
                    .unwrap_or(ConditionalCapabilities {
                        put_if_none_match: true,
                        put_if_match: true,
                        delete_if_match: true,
                    });
                if !caps.supports_cas() {
                    return Err(Error::Initialization(format!(
                        "S3 lock strategy requires If-None-Match and If-Match support, \
                         but capabilities declare put_if_none_match={}, put_if_match={}. \
                         Use lock_strategy = redis or lock_strategy = memory instead.",
                        caps.put_if_none_match, caps.put_if_match
                    )));
                }
                info!("Using S3 CAS coordination for S3 job queue");
                let conditional_store: Arc<dyn ConditionalStore> = storage;
                let store: Arc<dyn JobStore> = Arc::new(
                    conditional::Backend::builder()
                        .store(conditional_store.clone())
                        .build()?,
                );
                let leases: Arc<dyn LeaseBackend> = Arc::new(
                    lease::conditional::Backend::builder()
                        .store(conditional_store)
                        .delete_if_match(caps.delete_if_match)
                        .build()?,
                );
                Ok(JobBackends { store, leases })
            }
            LockStrategy::Redis(redis_config) => {
                info!("Using Redis lock coordination for S3 job queue");
                let lock_backend = lock::RedisBackend::new(redis_config).map_err(|e| {
                    Error::Initialization(format!("Failed to initialize Redis lock store: {e}"))
                })?;
                build_lock_coordinated(storage, Arc::new(lock_backend))
            }
            LockStrategy::Memory => {
                info!("Using in-memory lock coordination for S3 job queue");
                build_lock_coordinated(storage, Arc::new(MemoryBackend::new()))
            }
        }
    }
}

/// Build the lock-coordinated [`JobBackends`] over an S3 `ObjectStore` and
/// a non-CAS [`LockBackend`].
fn build_lock_coordinated(
    storage: Arc<StorageBackend>,
    lock: Arc<dyn LockBackend + Send + Sync>,
) -> Result<JobBackends, Error> {
    let object_store: Arc<dyn ObjectStore> = storage;
    let store: Arc<dyn JobStore> = Arc::new(
        locked::Backend::builder()
            .store(object_store.clone())
            .lock(lock.clone())
            .build()?,
    );
    let leases: Arc<dyn LeaseBackend> = Arc::new(
        lease::locked::Backend::builder()
            .store(object_store)
            .lock(lock)
            .build()?,
    );
    Ok(JobBackends { store, leases })
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc, time::Duration};

    use bytes::Bytes;
    use chrono::Utc;
    use serde::Serialize;
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
                JobBackends, JobEnvelope, JobQueue, JobStore,
                conditional::Backend,
                durable::{DurableJobConsumer, DurableJobQueue},
                lease::{self, LeaseBackend},
            },
            manifest::DEFAULT_MAX_MANIFEST_SIZE_BYTES,
            metadata_store::{
                Backend as MetaBackend, LockStrategy, fs::BackendConfig as MetaBackendConfig,
            },
            path_builder,
            repository::{Config as RepositoryConfig, RegistryClientConfig},
            repository_resolver::RepositoryResolver,
            s3_connection::S3ConnectionConfig,
        },
        secret::Secret,
        util::sha256,
    };
    use angos_s3_client::Backend as S3HttpBackend;
    use angos_storage::{ConditionalStore, s3::Backend as StorageBackend};

    const REPO_NAME: &str = "local";
    const NAMESPACE: &str = "local/cache-ns";

    /// `[global.job_queue.s3]` round-trip: flat TOML deserialises into a
    /// `BackendConfig` whose `connection` carries the right values.
    #[test]
    fn s3_backend_config_toml_round_trip() {
        let toml = r#"
            access_key_id = "job-key"
            secret_key    = "job-secret"
            endpoint      = "https://jobs.s3.example.com"
            bucket        = "jobs-bucket"
            region        = "ap-southeast-1"
            key_prefix    = "_custom"
        "#;

        let cfg: super::BackendConfig = toml::from_str(toml).expect("deserialize");
        assert_eq!(cfg.connection.access_key_id.expose(), "job-key");
        assert_eq!(cfg.connection.secret_key.expose(), "job-secret");
        assert_eq!(cfg.connection.endpoint, "https://jobs.s3.example.com");
        assert_eq!(cfg.connection.bucket, "jobs-bucket");
        assert_eq!(cfg.connection.region, "ap-southeast-1");
        assert_eq!(cfg.connection.key_prefix, "_custom");
    }

    /// Regression: omitting `key_prefix` in `[global.job_queue.s3]` must
    /// resolve to `"_jobs"` (not the empty `S3ConnectionConfig` default) so
    /// existing operator deployments are not broken.
    #[test]
    fn s3_backend_config_key_prefix_defaults_to_jobs_when_absent() {
        let toml = r#"
            access_key_id = "job-key"
            secret_key    = "job-secret"
            endpoint      = "https://jobs.s3.example.com"
            bucket        = "jobs-bucket"
            region        = "us-east-1"
        "#;

        let cfg: super::BackendConfig = toml::from_str(toml).expect("deserialize");
        assert_eq!(cfg.connection.key_prefix, "_jobs");
    }

    /// Regression: omitting `region` falls back to `default_region()`
    /// (`"us-east-1"`), preserving the behaviour from before consolidation.
    #[test]
    fn s3_backend_config_region_defaults_to_us_east_1_when_absent() {
        let toml = r#"
            access_key_id = "job-key"
            secret_key    = "job-secret"
            endpoint      = "https://jobs.s3.example.com"
            bucket        = "jobs-bucket"
        "#;

        let cfg: super::BackendConfig = toml::from_str(toml).expect("deserialize");
        assert_eq!(cfg.connection.region, "us-east-1");
    }

    /// Mirrors the body produced by `conditional::serialize_lease`. Re-defined
    /// here (rather than re-exported) so test-only fixtures stay test-local.
    #[derive(Serialize)]
    struct LeaseFile {
        refreshed_at: chrono::DateTime<Utc>,
        worker_id: String,
        ttl_secs: u64,
    }

    fn make_fs_backends(path: &str) -> (Arc<BlobBackend>, Arc<MetaBackend>) {
        let blob = Arc::new(
            BlobBackend::new(&BlobBackendConfig {
                root_dir: path.to_string(),
                sync_to_disk: false,
            })
            .unwrap(),
        );
        let meta = Arc::new(
            MetaBackendConfig {
                root_dir: path.to_string(),
                sync_to_disk: false,
                lock_strategy: LockStrategy::Memory,
            }
            .to_backend()
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

    /// Build the storage + lease backends and a sibling raw HTTP client
    /// over the same key prefix so the test can `delete_prefix` for cleanup
    /// and write fixtures directly into S3.
    fn test_backends(prefix: &str) -> (JobBackends, Arc<S3HttpBackend>) {
        metrics_provider::init_for_tests();
        let connection = S3ConnectionConfig {
            access_key_id: Secret::new("root".to_string()),
            secret_key: Secret::new("roottoor".to_string()),
            endpoint: "http://127.0.0.1:9000".to_string(),
            bucket: "registry".to_string(),
            region: "us-east-1".to_string(),
            key_prefix: format!("{prefix}{}_", Uuid::new_v4()),
        };
        let transport = connection.to_client_config();
        let http = Arc::new(S3HttpBackend::new(&transport).expect("s3 client"));
        let storage: Arc<dyn ConditionalStore> = Arc::new(
            StorageBackend::builder()
                .client(http.clone())
                .build()
                .expect("storage backend"),
        );
        let store: Arc<dyn JobStore> = Arc::new(
            Backend::builder()
                .store(storage.clone())
                .build()
                .expect("conditional backend"),
        );
        let leases: Arc<dyn LeaseBackend> = Arc::new(
            lease::conditional::Backend::builder()
                .store(storage)
                .delete_if_match(true)
                .build()
                .expect("lease backend"),
        );
        (JobBackends { store, leases }, http)
    }

    fn make_consumer(backends: &JobBackends) -> DurableJobConsumer {
        DurableJobConsumer::new(
            backends.store.clone(),
            backends.leases.clone(),
            30,
            "test-worker".to_string(),
        )
    }

    #[tokio::test]
    async fn s3_claim_and_complete() {
        let (backends, _) = test_backends("jq_claim_");
        let queue = DurableJobQueue::new(backends.store.clone());
        let consumer = make_consumer(&backends);

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
        let (backends, http) = test_backends("jq_stale_");
        let consumer = make_consumer(&backends);

        let env = JobEnvelope::new("cache", "test.noop", "cache.ns:sha256:s3stale", &())
            .expect("envelope");
        backends
            .store
            .put_pending("cache", &env, Utc::now())
            .await
            .expect("put");

        // 3-second TTL with refreshed_at set 10s ago → body-driven staleness.
        let stale_body = serde_json::to_vec(&LeaseFile {
            refreshed_at: Utc::now() - chrono::Duration::seconds(10),
            worker_id: "dead-worker".to_string(),
            ttl_secs: 3,
        })
        .expect("serialize");
        http.put_object(
            &path_builder::job_lease_path(&env.lock_key),
            Bytes::from(stale_body),
        )
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

    /// Single-replica durable round-trip. Blob and metadata stores stay
    /// FS-backed in a tempdir; only the job queue uses S3.
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

        let (backends, http) = test_backends("e2e_rtrip_");
        let durable_queue: Arc<dyn JobQueue> =
            Arc::new(DurableJobQueue::new(backends.store.clone()));

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
        let pending_count = backends
            .store
            .count_pending("cache", 600)
            .await
            .expect("count_pending after enqueue");
        assert_eq!(pending_count, 1, "exactly one pending envelope expected");

        // Build a consumer + handler and drive the worker once.
        let consumer = Arc::new(DurableJobConsumer::new(
            backends.store.clone(),
            backends.leases.clone(),
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
        let remaining = backends
            .store
            .count_pending("cache", 600)
            .await
            .expect("count_pending after worker");
        assert_eq!(remaining, 0, "pending queue must be empty after completion");

        // Best-effort S3 cleanup so parallel test runs don't see stale state.
        let _ = http.delete_prefix("_jobs/").await;
    }
}
