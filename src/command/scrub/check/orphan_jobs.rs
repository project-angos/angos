//! [`OrphanJobChecker`]: emits a delete action for every job (pending or
//! dead-lettered) on one durable queue whose payload no longer resolves to
//! configured state, so stale config stops churning the queue through retries.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use tracing::{info, warn};

use crate::{
    command::scrub::{action::Action, check::StoreChecker, error::Error, executor::ActionSink},
    registry::{
        Repository,
        cache_job_handler::{CACHE_QUEUE, CacheFetchBlobPayload},
        job_store::{Error as JobStoreError, JobState, JobStore},
        repository_resolver::RepositoryResolver,
    },
    replication::{REPLICATION_QUEUE, ReplicationPushPayload},
};

/// Keyset page size for the pending and failed scans; pages are looped to
/// exhaustion, so this only bounds memory per round-trip.
const PAGE_SIZE: u16 = 256;

/// Which durable queue an [`OrphanJobChecker`] scans, carrying the per-queue
/// orphan classification.
#[derive(Clone, Copy)]
pub enum OrphanQueue {
    /// Replication jobs; orphaned when the downstream or repository is no
    /// longer configured.
    Replication,
    /// Pull-through cache-fill jobs; orphaned when the repository is no longer
    /// configured for pull-through.
    Cache,
}

impl OrphanQueue {
    /// Durable queue name, as addressed by the [`JobStore`].
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            OrphanQueue::Replication => REPLICATION_QUEUE,
            OrphanQueue::Cache => CACHE_QUEUE,
        }
    }

    /// Classifies one raw payload: `Ok(Some(reason))` for an orphan, `Ok(None)`
    /// for a job that still resolves, `Err` for a payload that fails to decode.
    fn classify(
        self,
        resolver: &RepositoryResolver,
        payload: Value,
    ) -> Result<Option<String>, serde_json::Error> {
        match self {
            // A downstream whose mode changed still resolves, so it is not an
            // orphan.
            OrphanQueue::Replication => {
                let payload: ReplicationPushPayload = serde_json::from_value(payload)?;
                let configured = resolver
                    .resolve(&payload.namespace)
                    .is_some_and(|repository| {
                        repository
                            .replication
                            .iter()
                            .any(|d| d.name == payload.downstream)
                    });
                Ok((!configured).then(|| {
                    format!(
                        "downstream '{}' is not configured for '{}'",
                        payload.downstream, payload.namespace
                    )
                }))
            }
            // A blob already stored locally would let such a job complete on
            // drain, but granting a reference to a namespace removed from
            // pull-through config serves nothing, so it still counts as an orphan.
            OrphanQueue::Cache => {
                let payload: CacheFetchBlobPayload = serde_json::from_value(payload)?;
                let configured = resolver
                    .resolve(&payload.namespace)
                    .is_some_and(Repository::is_pull_through);
                Ok((!configured).then(|| {
                    format!(
                        "namespace '{}' is not configured for pull-through",
                        payload.namespace
                    )
                }))
            }
        }
    }
}

/// Scans every pending and dead-lettered job on one queue and emits
/// [`Action::DeleteOrphanJob`] for each whose payload no longer resolves to
/// configured state.
pub struct OrphanJobChecker {
    job_store: Arc<JobStore>,
    resolver: Arc<RepositoryResolver>,
    queue: OrphanQueue,
}

impl OrphanJobChecker {
    /// Construct a checker from its resolved fields: the `job_store` the queue
    /// partitions are read through, the namespace -> repository `resolver`
    /// yielding the configured downstreams and pull-through upstreams, and the
    /// durable `queue` to scan with its orphan classification.
    #[must_use]
    pub fn new(
        job_store: Arc<JobStore>,
        resolver: Arc<RepositoryResolver>,
        queue: OrphanQueue,
    ) -> Self {
        Self {
            job_store,
            resolver,
            queue,
        }
    }

    /// Reads the raw payload for one storage key. Returns `Ok(None)` for a key
    /// that vanished mid-scan (someone else already handled it); decoding is
    /// deferred to [`OrphanQueue::classify`].
    async fn read_payload(
        &self,
        state: JobState,
        storage_key: &str,
    ) -> Result<Option<Value>, Error> {
        let queue = self.queue.name();
        let envelope = match state {
            JobState::Pending => self.job_store.read_pending(queue, storage_key).await,
            JobState::Failed => self
                .job_store
                .read_failed(queue, storage_key)
                .await
                .map(|read| read.envelope),
        };
        match envelope {
            Ok(envelope) => Ok(Some(envelope.payload)),
            Err(JobStoreError::NotFound) => {
                warn!("{queue} job '{storage_key}' vanished mid-scan; skipping");
                Ok(None)
            }
            Err(e) => Err(Error::JobQueue(format!(
                "failed to read {queue} job '{storage_key}': {e}"
            ))),
        }
    }

    /// Scans one queue partition to exhaustion, emitting a delete action per
    /// orphan; returns how many orphans were emitted.
    async fn scan_partition(
        &self,
        state: JobState,
        sink: &mut (dyn ActionSink + Send),
    ) -> Result<u64, Error> {
        let queue = self.queue.name();
        let mut orphans: u64 = 0;
        let mut after: Option<String> = None;
        loop {
            let (keys, next) = match state {
                JobState::Pending => {
                    self.job_store
                        .list_pending_page(queue, PAGE_SIZE, after.as_deref())
                        .await
                }
                JobState::Failed => {
                    self.job_store
                        .list_failed_page(queue, PAGE_SIZE, after.as_deref())
                        .await
                }
            }
            .map_err(|e| Error::JobQueue(format!("failed to list {queue} jobs: {e}")))?;

            for storage_key in keys {
                let Some(payload) = self.read_payload(state, &storage_key).await? else {
                    continue;
                };
                // A payload that fails to decode is skipped: scrub must not
                // delete what it cannot attribute.
                let reason = match self.queue.classify(&self.resolver, payload) {
                    Ok(reason) => reason,
                    Err(e) => {
                        warn!(
                            "{queue} job '{storage_key}' has an undecodable payload; skipping: {e}"
                        );
                        continue;
                    }
                };
                let Some(reason) = reason else {
                    continue;
                };
                orphans += 1;
                sink.apply(Action::DeleteOrphanJob {
                    queue,
                    state,
                    storage_key,
                    reason,
                })
                .await?;
            }

            let Some(cursor) = next else {
                break;
            };
            after = Some(cursor);
        }
        Ok(orphans)
    }
}

#[async_trait]
impl StoreChecker for OrphanJobChecker {
    async fn check_all(&self, sink: &mut (dyn ActionSink + Send)) -> Result<(), Error> {
        let pending = self.scan_partition(JobState::Pending, sink).await?;
        let failed = self.scan_partition(JobState::Failed, sink).await?;
        info!(
            "Found {pending} orphan pending and {failed} orphan dead-lettered {} job(s)",
            self.queue.name()
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use serde_json::json;
    use tempfile::TempDir;

    use angos_storage::{ObjectStore, fs::Backend as StorageFsBackend};
    use angos_tx_engine::store::Store;

    use crate::{
        cache,
        command::scrub::{
            action::Action,
            check::{
                StoreChecker,
                orphan_jobs::{OrphanJobChecker, OrphanQueue},
            },
            executor::{ActionSink, DryRunSink, Executor},
        },
        metrics_provider,
        policy::{RetentionPolicy, RetentionPolicyConfig, SystemClock},
        registry::{
            Repository,
            blob_store::BlobStore,
            cache_job_handler::{CACHE_FETCH_BLOB_KIND, CACHE_QUEUE, CacheFetchBlobPayload},
            job_store::{FailOutcome, JobEnvelope, JobState, JobStore},
            manifest::DEFAULT_MAX_MANIFEST_SIZE_BYTES,
            metadata_store::MetadataStore,
            repository_resolver::RepositoryResolver,
            test_utils::{build_store, build_test_fs_executor},
        },
        registry_client::RegistryClient,
        replication::{
            REPLICATION_PUSH_MANIFEST_KIND, REPLICATION_QUEUE, ReplicationDownstream,
            ReplicationMode, ReplicationPushPayload, build_envelope,
        },
    };

    const NAMESPACE: &str = "nginx";
    const REPO: &str = "nginx";
    /// Repository with no upstreams: configured, but not pull-through.
    const LOCAL_REPO: &str = "local";
    const DOWNSTREAM: &str = "eu-region";
    const REMOVED_DOWNSTREAM: &str = "decommissioned";
    const GHOST_NAMESPACE: &str = "ghost/app";
    const DIGEST: &str = "sha256:1111111111111111111111111111111111111111111111111111111111111111";

    /// FS-backed metadata store so the tests run without S3.
    fn fs_metadata_store() -> (Arc<MetadataStore>, Arc<Store>, TempDir) {
        let dir = TempDir::new().unwrap();
        let root = dir.path().to_str().unwrap();
        let object: Arc<dyn ObjectStore> = Arc::new(StorageFsBackend::builder(root).build());
        let executor = build_test_fs_executor(root, false);
        let store = build_store(object, executor);
        let metadata_store = Arc::new(
            MetadataStore::builder(store.clone())
                .link_cache_ttl(0)
                .access_time_debounce_secs(0)
                .build(),
        );
        (metadata_store, store, dir)
    }

    /// Registry client whose URL is never contacted by these checkers.
    fn client() -> RegistryClient {
        let backend = cache::Config::Memory.to_backend().unwrap();
        RegistryClient::builder(
            "http://127.0.0.1:1".to_string(),
            reqwest::Client::new(),
            backend,
        )
        .max_manifest_size_bytes(DEFAULT_MAX_MANIFEST_SIZE_BYTES)
        .build()
    }

    fn repository(
        name: &str,
        upstreams: Vec<RegistryClient>,
        replication: Vec<ReplicationDownstream>,
    ) -> Repository {
        Repository {
            name: name.to_string(),
            upstreams,
            replication,
            retention_policy: RetentionPolicy::new(
                &RetentionPolicyConfig::default(),
                Arc::new(SystemClock),
            ),
            immutable_tags: false,
            immutable_tags_exclusions: Vec::new(),
        }
    }

    /// Resolver with a pull-through repository whose only downstream is
    /// [`DOWNSTREAM`], plus a configured repository with no upstreams.
    fn resolver() -> Arc<RepositoryResolver> {
        let downstream =
            ReplicationDownstream::builder(DOWNSTREAM.to_string(), Arc::new(client()), 4)
                .mode(ReplicationMode::EventReconcile)
                .prune(false)
                .build();
        let mut repositories = HashMap::new();
        repositories.insert(
            REPO.to_string(),
            repository(REPO, vec![client()], vec![downstream]),
        );
        repositories.insert(
            LOCAL_REPO.to_string(),
            repository(LOCAL_REPO, Vec::new(), Vec::new()),
        );
        Arc::new(RepositoryResolver::new(Arc::new(repositories)).unwrap())
    }

    fn checker(job_store: Arc<JobStore>, queue: OrphanQueue) -> OrphanJobChecker {
        OrphanJobChecker::new(job_store, resolver(), queue)
    }

    fn push_payload(downstream: &str, namespace: &str) -> ReplicationPushPayload {
        ReplicationPushPayload {
            downstream: downstream.to_string(),
            namespace: namespace.to_string(),
            tag: Some("v1".to_string()),
            digest: None,
            kind: REPLICATION_PUSH_MANIFEST_KIND.to_string(),
            source_ts: None,
        }
    }

    fn cache_envelope(namespace: &str) -> JobEnvelope {
        let payload = CacheFetchBlobPayload {
            namespace: namespace.to_string(),
            digest: DIGEST.to_string(),
        };
        JobEnvelope::new(
            CACHE_QUEUE,
            CACHE_FETCH_BLOB_KIND,
            format!("{CACHE_QUEUE}.{namespace}:{DIGEST}"),
            &payload,
        )
        .unwrap()
    }

    async fn enqueue_push(job_store: &JobStore, downstream: &str, namespace: &str) {
        let envelope = build_envelope(&push_payload(downstream, namespace)).unwrap();
        job_store.enqueue(envelope).await.unwrap();
    }

    async fn enqueue_cache_fill(job_store: &JobStore, namespace: &str) {
        job_store.enqueue(cache_envelope(namespace)).await.unwrap();
    }

    /// Enqueues a single-attempt job and fails it once so it dead-letters under
    /// its original storage key.
    async fn dead_letter(job_store: &JobStore, queue: &str, mut envelope: JobEnvelope) {
        envelope.max_attempts = 1;
        job_store.enqueue(envelope).await.unwrap();
        let claimed = job_store
            .claim_one(queue)
            .await
            .unwrap()
            .claimed
            .expect("the enqueued job must be claimable");
        let outcome = job_store.fail(claimed, "simulated failure").await.unwrap();
        assert!(matches!(outcome, FailOutcome::MovedToDeadLetter));
    }

    #[tokio::test]
    async fn orphan_pending_replication_job_emits_delete_action() {
        metrics_provider::init_for_tests();
        let (metadata_store, _store, _dir) = fs_metadata_store();
        let job_store = Arc::new(JobStore::new(metadata_store.store_arc(), "orphan-test"));

        enqueue_push(&job_store, REMOVED_DOWNSTREAM, NAMESPACE).await;
        let keys = job_store.list_pending(REPLICATION_QUEUE, 10).await.unwrap();
        assert_eq!(keys.len(), 1);

        let mut sink: Vec<Action> = Vec::new();
        checker(job_store, OrphanQueue::Replication)
            .check_all(&mut sink)
            .await
            .unwrap();

        assert_eq!(sink.len(), 1, "the orphan job must emit one action");
        assert!(matches!(
            &sink[0],
            Action::DeleteOrphanJob { queue, state, storage_key, reason }
                if *queue == REPLICATION_QUEUE
                    && *state == JobState::Pending
                    && *storage_key == keys[0]
                    && reason == "downstream 'decommissioned' is not configured for 'nginx'"
        ));
    }

    #[tokio::test]
    async fn configured_downstream_job_emits_nothing() {
        metrics_provider::init_for_tests();
        let (metadata_store, _store, _dir) = fs_metadata_store();
        let job_store = Arc::new(JobStore::new(metadata_store.store_arc(), "orphan-test"));

        enqueue_push(&job_store, DOWNSTREAM, NAMESPACE).await;

        let mut sink: Vec<Action> = Vec::new();
        checker(job_store, OrphanQueue::Replication)
            .check_all(&mut sink)
            .await
            .unwrap();

        assert!(
            sink.is_empty(),
            "a job for a configured downstream must not emit an action, got {} action(s)",
            sink.len()
        );
    }

    #[tokio::test]
    async fn unresolvable_namespace_replication_job_emits_delete_action() {
        metrics_provider::init_for_tests();
        let (metadata_store, _store, _dir) = fs_metadata_store();
        let job_store = Arc::new(JobStore::new(metadata_store.store_arc(), "orphan-test"));

        // The downstream name is configured, but only for `nginx`; the payload
        // namespace resolves to no repository at all.
        enqueue_push(&job_store, DOWNSTREAM, GHOST_NAMESPACE).await;

        let mut sink: Vec<Action> = Vec::new();
        checker(job_store, OrphanQueue::Replication)
            .check_all(&mut sink)
            .await
            .unwrap();

        assert_eq!(sink.len(), 1, "an unresolvable namespace is an orphan");
        assert!(matches!(
            &sink[0],
            Action::DeleteOrphanJob { queue, state, reason, .. }
                if *queue == REPLICATION_QUEUE
                    && *state == JobState::Pending
                    && reason.contains(GHOST_NAMESPACE)
        ));
    }

    #[tokio::test]
    async fn orphan_failed_replication_job_emits_action_with_failed_state() {
        metrics_provider::init_for_tests();
        let (metadata_store, _store, _dir) = fs_metadata_store();
        let job_store = Arc::new(JobStore::new(metadata_store.store_arc(), "orphan-test"));

        let envelope = build_envelope(&push_payload(REMOVED_DOWNSTREAM, NAMESPACE)).unwrap();
        dead_letter(&job_store, REPLICATION_QUEUE, envelope).await;

        let mut sink: Vec<Action> = Vec::new();
        checker(job_store, OrphanQueue::Replication)
            .check_all(&mut sink)
            .await
            .unwrap();

        assert_eq!(
            sink.len(),
            1,
            "the dead-lettered orphan must emit one action"
        );
        assert!(matches!(
            &sink[0],
            Action::DeleteOrphanJob { state, reason, .. }
                if *state == JobState::Failed && reason.contains(REMOVED_DOWNSTREAM)
        ));
    }

    #[tokio::test]
    async fn malformed_replication_payload_is_skipped_without_action() {
        metrics_provider::init_for_tests();
        let (metadata_store, _store, _dir) = fs_metadata_store();
        let job_store = Arc::new(JobStore::new(metadata_store.store_arc(), "orphan-test"));

        let envelope = JobEnvelope::new(
            REPLICATION_QUEUE,
            REPLICATION_PUSH_MANIFEST_KIND,
            "replication.push.bogus",
            &json!({ "not": "a payload" }),
        )
        .unwrap();
        job_store.enqueue(envelope).await.unwrap();

        let mut sink: Vec<Action> = Vec::new();
        checker(job_store.clone(), OrphanQueue::Replication)
            .check_all(&mut sink)
            .await
            .unwrap();

        assert!(
            sink.is_empty(),
            "a payload scrub cannot attribute must be skipped, got {} action(s)",
            sink.len()
        );
        assert_eq!(
            job_store.count_pending(REPLICATION_QUEUE, 0).await.unwrap(),
            1,
            "the undecodable job must stay in the queue"
        );
    }

    /// End-to-end shape: checker into executor deletes only the orphan and the
    /// configured job survives.
    #[tokio::test]
    async fn end_to_end_deletes_only_the_orphan_replication_job() {
        metrics_provider::init_for_tests();
        let (metadata_store, store, _dir) = fs_metadata_store();
        let blob_store = Arc::new(BlobStore::new(store.clone()));
        let job_store = Arc::new(JobStore::new(metadata_store.store_arc(), "orphan-test"));

        enqueue_push(&job_store, DOWNSTREAM, NAMESPACE).await;
        enqueue_push(&job_store, REMOVED_DOWNSTREAM, NAMESPACE).await;
        assert_eq!(
            job_store.count_pending(REPLICATION_QUEUE, 0).await.unwrap(),
            2
        );

        let mut executor: Box<dyn ActionSink + Send> =
            Box::new(Executor::new(blob_store, metadata_store, job_store.clone()));
        checker(job_store.clone(), OrphanQueue::Replication)
            .check_all(executor.as_mut())
            .await
            .unwrap();

        let keys = job_store.list_pending(REPLICATION_QUEUE, 10).await.unwrap();
        assert_eq!(keys.len(), 1, "only the orphan job must be deleted");
        let survivor = job_store
            .read_pending(REPLICATION_QUEUE, &keys[0])
            .await
            .unwrap();
        let payload: ReplicationPushPayload = serde_json::from_value(survivor.payload).unwrap();
        assert_eq!(
            payload.downstream, DOWNSTREAM,
            "the configured downstream's job must survive"
        );
    }

    /// Dry-run shape: the checker reads regardless of dry-run, but the
    /// `DryRunSink` applies nothing, so both jobs remain.
    #[tokio::test]
    async fn dry_run_leaves_both_replication_jobs_in_place() {
        metrics_provider::init_for_tests();
        let (metadata_store, _store, _dir) = fs_metadata_store();
        let job_store = Arc::new(JobStore::new(metadata_store.store_arc(), "orphan-test"));

        enqueue_push(&job_store, DOWNSTREAM, NAMESPACE).await;
        enqueue_push(&job_store, REMOVED_DOWNSTREAM, NAMESPACE).await;

        let mut sink = DryRunSink;
        checker(job_store.clone(), OrphanQueue::Replication)
            .check_all(&mut sink)
            .await
            .unwrap();

        assert_eq!(
            job_store.count_pending(REPLICATION_QUEUE, 0).await.unwrap(),
            2,
            "dry-run must leave both jobs in place"
        );
    }

    #[tokio::test]
    async fn orphan_pending_cache_job_with_unresolvable_namespace_emits_delete_action() {
        metrics_provider::init_for_tests();
        let (metadata_store, _store, _dir) = fs_metadata_store();
        let job_store = Arc::new(JobStore::new(metadata_store.store_arc(), "orphan-test"));

        enqueue_cache_fill(&job_store, GHOST_NAMESPACE).await;
        let keys = job_store.list_pending(CACHE_QUEUE, 10).await.unwrap();
        assert_eq!(keys.len(), 1);

        let mut sink: Vec<Action> = Vec::new();
        checker(job_store, OrphanQueue::Cache)
            .check_all(&mut sink)
            .await
            .unwrap();

        assert_eq!(sink.len(), 1, "an unresolvable namespace is an orphan");
        assert!(matches!(
            &sink[0],
            Action::DeleteOrphanJob { queue, state, storage_key, reason }
                if *queue == CACHE_QUEUE
                    && *state == JobState::Pending
                    && *storage_key == keys[0]
                    && reason == "namespace 'ghost/app' is not configured for pull-through"
        ));
    }

    #[tokio::test]
    async fn configured_pull_through_cache_job_emits_nothing() {
        metrics_provider::init_for_tests();
        let (metadata_store, _store, _dir) = fs_metadata_store();
        let job_store = Arc::new(JobStore::new(metadata_store.store_arc(), "orphan-test"));

        enqueue_cache_fill(&job_store, NAMESPACE).await;

        let mut sink: Vec<Action> = Vec::new();
        checker(job_store, OrphanQueue::Cache)
            .check_all(&mut sink)
            .await
            .unwrap();

        assert!(
            sink.is_empty(),
            "a job for a configured pull-through repository must not emit an action, got {} action(s)",
            sink.len()
        );
    }

    #[tokio::test]
    async fn cache_job_for_non_pull_through_repository_emits_delete_action() {
        metrics_provider::init_for_tests();
        let (metadata_store, _store, _dir) = fs_metadata_store();
        let job_store = Arc::new(JobStore::new(metadata_store.store_arc(), "orphan-test"));

        // The namespace resolves to a configured repository, but one with no
        // upstreams: a fill job for it can never fetch anything.
        enqueue_cache_fill(&job_store, LOCAL_REPO).await;

        let mut sink: Vec<Action> = Vec::new();
        checker(job_store, OrphanQueue::Cache)
            .check_all(&mut sink)
            .await
            .unwrap();

        assert_eq!(sink.len(), 1, "a non-pull-through repository is an orphan");
        assert!(matches!(
            &sink[0],
            Action::DeleteOrphanJob { queue, state, reason, .. }
                if *queue == CACHE_QUEUE
                    && *state == JobState::Pending
                    && reason == "namespace 'local' is not configured for pull-through"
        ));
    }

    #[tokio::test]
    async fn orphan_failed_cache_job_emits_action_with_failed_state() {
        metrics_provider::init_for_tests();
        let (metadata_store, _store, _dir) = fs_metadata_store();
        let job_store = Arc::new(JobStore::new(metadata_store.store_arc(), "orphan-test"));

        dead_letter(&job_store, CACHE_QUEUE, cache_envelope(GHOST_NAMESPACE)).await;

        let mut sink: Vec<Action> = Vec::new();
        checker(job_store, OrphanQueue::Cache)
            .check_all(&mut sink)
            .await
            .unwrap();

        assert_eq!(
            sink.len(),
            1,
            "the dead-lettered orphan must emit one action"
        );
        assert!(matches!(
            &sink[0],
            Action::DeleteOrphanJob { queue, state, reason, .. }
                if *queue == CACHE_QUEUE
                    && *state == JobState::Failed
                    && reason.contains(GHOST_NAMESPACE)
        ));
    }

    #[tokio::test]
    async fn malformed_cache_payload_is_skipped_without_action() {
        metrics_provider::init_for_tests();
        let (metadata_store, _store, _dir) = fs_metadata_store();
        let job_store = Arc::new(JobStore::new(metadata_store.store_arc(), "orphan-test"));

        let envelope = JobEnvelope::new(
            CACHE_QUEUE,
            CACHE_FETCH_BLOB_KIND,
            "cache.bogus",
            &json!({ "not": "a payload" }),
        )
        .unwrap();
        job_store.enqueue(envelope).await.unwrap();

        let mut sink: Vec<Action> = Vec::new();
        checker(job_store.clone(), OrphanQueue::Cache)
            .check_all(&mut sink)
            .await
            .unwrap();

        assert!(
            sink.is_empty(),
            "a payload scrub cannot attribute must be skipped, got {} action(s)",
            sink.len()
        );
        assert_eq!(
            job_store.count_pending(CACHE_QUEUE, 0).await.unwrap(),
            1,
            "the undecodable job must stay in the queue"
        );
    }

    /// End-to-end shape: checker into executor deletes only the orphan cache
    /// job and the configured pull-through job survives.
    #[tokio::test]
    async fn end_to_end_deletes_only_the_orphan_cache_job() {
        metrics_provider::init_for_tests();
        let (metadata_store, store, _dir) = fs_metadata_store();
        let blob_store = Arc::new(BlobStore::new(store.clone()));
        let job_store = Arc::new(JobStore::new(metadata_store.store_arc(), "orphan-test"));

        enqueue_cache_fill(&job_store, NAMESPACE).await;
        enqueue_cache_fill(&job_store, GHOST_NAMESPACE).await;
        assert_eq!(job_store.count_pending(CACHE_QUEUE, 0).await.unwrap(), 2);

        let mut executor: Box<dyn ActionSink + Send> =
            Box::new(Executor::new(blob_store, metadata_store, job_store.clone()));
        checker(job_store.clone(), OrphanQueue::Cache)
            .check_all(executor.as_mut())
            .await
            .unwrap();

        let keys = job_store.list_pending(CACHE_QUEUE, 10).await.unwrap();
        assert_eq!(keys.len(), 1, "only the orphan job must be deleted");
        let survivor = job_store.read_pending(CACHE_QUEUE, &keys[0]).await.unwrap();
        let payload: CacheFetchBlobPayload = serde_json::from_value(survivor.payload).unwrap();
        assert_eq!(
            payload.namespace, NAMESPACE,
            "the pull-through repository's job must survive"
        );
    }

    /// Dry-run shape: the checker reads regardless of dry-run, but the
    /// `DryRunSink` applies nothing, so both jobs remain.
    #[tokio::test]
    async fn dry_run_leaves_both_cache_jobs_in_place() {
        metrics_provider::init_for_tests();
        let (metadata_store, _store, _dir) = fs_metadata_store();
        let job_store = Arc::new(JobStore::new(metadata_store.store_arc(), "orphan-test"));

        enqueue_cache_fill(&job_store, NAMESPACE).await;
        enqueue_cache_fill(&job_store, GHOST_NAMESPACE).await;

        let mut sink = DryRunSink;
        checker(job_store.clone(), OrphanQueue::Cache)
            .check_all(&mut sink)
            .await
            .unwrap();

        assert_eq!(
            job_store.count_pending(CACHE_QUEUE, 0).await.unwrap(),
            2,
            "dry-run must leave both jobs in place"
        );
    }
}
