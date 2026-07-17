//! [`OrphanJobChecker`]: emits a delete action for every job (pending or
//! dead-lettered) on one durable queue whose payload no longer resolves to
//! configured state, so stale config stops churning the queue through retries.
//! Runs on every `angos prune`: prune is the config-trusting command.

use std::sync::Arc;

use serde_json::Value;
use tracing::{info, warn};

use crate::{
    command::scrub::{Error, action::Action, executor::ActionSink},
    jobs::store::{Error as JobStoreError, JobStore},
    jobs::{JobState, Queue},
    registry::{
        Repository, cache_job_handler::CacheFetchBlobPayload,
        repository_resolver::RepositoryResolver,
    },
    replication::ReplicationPushPayload,
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
    /// The durable [`Queue`] this orphan scan addresses.
    #[must_use]
    pub fn as_queue(self) -> Queue {
        match self {
            OrphanQueue::Replication => Queue::Replication,
            OrphanQueue::Cache => Queue::Cache,
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
        let queue = self.queue.as_queue();
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
    async fn scan_partition(&self, state: JobState, sink: &dyn ActionSink) -> Result<u64, Error> {
        let queue = self.queue.as_queue();
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

impl OrphanJobChecker {
    /// Scan both partitions of this checker's queue.
    pub async fn check_all(&self, sink: &dyn ActionSink) -> Result<(), Error> {
        let pending = self.scan_partition(JobState::Pending, sink).await?;
        let failed = self.scan_partition(JobState::Failed, sink).await?;
        info!(
            "prune: found {pending} orphan pending and {failed} orphan dead-lettered {} job(s)",
            self.queue.as_queue()
        );
        Ok(())
    }
}

/// Delete queued jobs on both durable queues whose downstream or repository
/// is no longer configured. Always runs with `angos prune`.
pub async fn sweep_orphan_jobs(
    job_store: &Arc<JobStore>,
    resolver: &Arc<RepositoryResolver>,
    sink: &dyn ActionSink,
) -> Result<(), Error> {
    for queue in [OrphanQueue::Replication, OrphanQueue::Cache] {
        OrphanJobChecker::new(job_store.clone(), resolver.clone(), queue)
            .check_all(sink)
            .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use serde_json::json;

    use crate::{
        command::{
            prune::orphan_jobs::{OrphanJobChecker, OrphanQueue},
            scrub::{
                action::Action,
                executor::{ActionSink, DryRunSink, Executor},
            },
        },
        jobs::{
            JobState, Queue,
            store::{FailOutcome, JobEnvelope, JobStore},
        },
        oci::{Namespace, Tag},
        policy::{RetentionPolicy, RetentionPolicyConfig, SystemClock},
        registry::{
            Repository,
            cache_job_handler::{CACHE_FETCH_BLOB_KIND, CacheFetchBlobPayload},
            metadata_store::MetadataStore,
            repository::Upstream,
            repository_resolver::RepositoryResolver,
            test_utils::{FsTestStack, downstream_client, fs_test_stack},
        },
        registry_client::RegistryClient,
        replication::{
            REPLICATION_PUSH_MANIFEST_KIND, ReplicationDownstream, ReplicationMode,
            ReplicationPushPayload, build_envelope,
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

    /// Job store over the shared test store, under this suite's worker id.
    fn orphan_job_store(metadata_store: &MetadataStore) -> Arc<JobStore> {
        Arc::new(JobStore::new(metadata_store.store_arc(), "orphan-test"))
    }

    /// Bare registry client for [`Upstream`]; never dialed by these checkers.
    fn upstream_client() -> RegistryClient {
        Arc::into_inner(downstream_client("http://127.0.0.1:1")).expect("unshared test client")
    }

    fn repository(
        name: &str,
        upstreams: Vec<RegistryClient>,
        replication: Vec<ReplicationDownstream>,
    ) -> Repository {
        // Tests build clients directly; wrap each in a verbatim-mapping Upstream.
        let upstreams = upstreams
            .into_iter()
            .map(|client| Upstream {
                client,
                local_namespace: None,
                target_namespace: None,
            })
            .collect();
        Repository {
            name: Namespace::new(name).unwrap(),
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
        let downstream = ReplicationDownstream::builder(
            DOWNSTREAM.to_string(),
            downstream_client("http://127.0.0.1:1"),
            4,
        )
        .mode(ReplicationMode::EventReconcile)
        .prune(false)
        .build();
        let mut repositories = HashMap::new();
        repositories.insert(
            REPO.to_string(),
            repository(REPO, vec![upstream_client()], vec![downstream]),
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
            namespace: Namespace::new(namespace).unwrap(),
            tag: Some(Tag::new("v1").unwrap()),
            digest: None,
            kind: REPLICATION_PUSH_MANIFEST_KIND.to_string(),
            source_ts: None,
        }
    }

    fn cache_envelope(namespace: &str) -> JobEnvelope {
        let payload = CacheFetchBlobPayload {
            namespace: Namespace::new(namespace).unwrap(),
            digest: DIGEST.to_string(),
        };
        JobEnvelope::new(
            Queue::Cache,
            CACHE_FETCH_BLOB_KIND,
            format!("{}.{namespace}:{DIGEST}", Queue::Cache),
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
    async fn dead_letter(job_store: &JobStore, queue: Queue, mut envelope: JobEnvelope) {
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
        let FsTestStack {
            dir: _dir,
            metadata_store,
            ..
        } = fs_test_stack();
        let job_store = orphan_job_store(&metadata_store);

        enqueue_push(&job_store, REMOVED_DOWNSTREAM, NAMESPACE).await;
        let keys = job_store
            .list_pending(Queue::Replication, 10)
            .await
            .unwrap();
        assert_eq!(keys.len(), 1);

        let sink: std::sync::Mutex<Vec<Action>> = std::sync::Mutex::new(Vec::new());
        checker(job_store, OrphanQueue::Replication)
            .check_all(&sink)
            .await
            .unwrap();

        assert_eq!(
            sink.lock().unwrap().len(),
            1,
            "the orphan job must emit one action"
        );
        assert!(matches!(
            &sink.lock().unwrap()[0],
            Action::DeleteOrphanJob { queue, state, storage_key, reason }
                if *queue == Queue::Replication
                    && *state == JobState::Pending
                    && *storage_key == keys[0]
                    && reason == "downstream 'decommissioned' is not configured for 'nginx'"
        ));
    }

    #[tokio::test]
    async fn configured_downstream_job_emits_nothing() {
        let FsTestStack {
            dir: _dir,
            metadata_store,
            ..
        } = fs_test_stack();
        let job_store = orphan_job_store(&metadata_store);

        enqueue_push(&job_store, DOWNSTREAM, NAMESPACE).await;

        let sink: std::sync::Mutex<Vec<Action>> = std::sync::Mutex::new(Vec::new());
        checker(job_store, OrphanQueue::Replication)
            .check_all(&sink)
            .await
            .unwrap();

        assert!(
            sink.lock().unwrap().is_empty(),
            "a job for a configured downstream must not emit an action, got {} action(s)",
            sink.lock().unwrap().len()
        );
    }

    #[tokio::test]
    async fn unresolvable_namespace_replication_job_emits_delete_action() {
        let FsTestStack {
            dir: _dir,
            metadata_store,
            ..
        } = fs_test_stack();
        let job_store = orphan_job_store(&metadata_store);

        // The downstream name is configured, but only for `nginx`; the payload
        // namespace resolves to no repository at all.
        enqueue_push(&job_store, DOWNSTREAM, GHOST_NAMESPACE).await;

        let sink: std::sync::Mutex<Vec<Action>> = std::sync::Mutex::new(Vec::new());
        checker(job_store, OrphanQueue::Replication)
            .check_all(&sink)
            .await
            .unwrap();

        assert_eq!(
            sink.lock().unwrap().len(),
            1,
            "an unresolvable namespace is an orphan"
        );
        assert!(matches!(
            &sink.lock().unwrap()[0],
            Action::DeleteOrphanJob { queue, state, reason, .. }
                if *queue == Queue::Replication
                    && *state == JobState::Pending
                    && reason.contains(GHOST_NAMESPACE)
        ));
    }

    #[tokio::test]
    async fn orphan_failed_replication_job_emits_action_with_failed_state() {
        let FsTestStack {
            dir: _dir,
            metadata_store,
            ..
        } = fs_test_stack();
        let job_store = orphan_job_store(&metadata_store);

        let envelope = build_envelope(&push_payload(REMOVED_DOWNSTREAM, NAMESPACE)).unwrap();
        dead_letter(&job_store, Queue::Replication, envelope).await;

        let sink: std::sync::Mutex<Vec<Action>> = std::sync::Mutex::new(Vec::new());
        checker(job_store, OrphanQueue::Replication)
            .check_all(&sink)
            .await
            .unwrap();

        assert_eq!(
            sink.lock().unwrap().len(),
            1,
            "the dead-lettered orphan must emit one action"
        );
        assert!(matches!(
            &sink.lock().unwrap()[0],
            Action::DeleteOrphanJob { state, reason, .. }
                if *state == JobState::Failed && reason.contains(REMOVED_DOWNSTREAM)
        ));
    }

    #[tokio::test]
    async fn malformed_replication_payload_is_skipped_without_action() {
        let FsTestStack {
            dir: _dir,
            metadata_store,
            ..
        } = fs_test_stack();
        let job_store = orphan_job_store(&metadata_store);

        let envelope = JobEnvelope::new(
            Queue::Replication,
            REPLICATION_PUSH_MANIFEST_KIND,
            "replication.push.bogus",
            &json!({ "not": "a payload" }),
        )
        .unwrap();
        job_store.enqueue(envelope).await.unwrap();

        let sink: std::sync::Mutex<Vec<Action>> = std::sync::Mutex::new(Vec::new());
        checker(job_store.clone(), OrphanQueue::Replication)
            .check_all(&sink)
            .await
            .unwrap();

        assert!(
            sink.lock().unwrap().is_empty(),
            "a payload scrub cannot attribute must be skipped, got {} action(s)",
            sink.lock().unwrap().len()
        );
        assert_eq!(
            job_store
                .count_pending(Queue::Replication, 0)
                .await
                .unwrap(),
            1,
            "the undecodable job must stay in the queue"
        );
    }

    /// End-to-end shape: checker into executor deletes only the orphan and the
    /// configured job survives.
    #[tokio::test]
    async fn end_to_end_deletes_only_the_orphan_replication_job() {
        let FsTestStack {
            dir: _dir,
            metadata_store,
            blob_store,
            ..
        } = fs_test_stack();
        let job_store = orphan_job_store(&metadata_store);

        enqueue_push(&job_store, DOWNSTREAM, NAMESPACE).await;
        enqueue_push(&job_store, REMOVED_DOWNSTREAM, NAMESPACE).await;
        assert_eq!(
            job_store
                .count_pending(Queue::Replication, 0)
                .await
                .unwrap(),
            2
        );

        let mut executor: Box<dyn ActionSink> =
            Box::new(Executor::new(blob_store, metadata_store, job_store.clone()));
        checker(job_store.clone(), OrphanQueue::Replication)
            .check_all(executor.as_mut())
            .await
            .unwrap();

        let keys = job_store
            .list_pending(Queue::Replication, 10)
            .await
            .unwrap();
        assert_eq!(keys.len(), 1, "only the orphan job must be deleted");
        let survivor = job_store
            .read_pending(Queue::Replication, &keys[0])
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
        let FsTestStack {
            dir: _dir,
            metadata_store,
            ..
        } = fs_test_stack();
        let job_store = orphan_job_store(&metadata_store);

        enqueue_push(&job_store, DOWNSTREAM, NAMESPACE).await;
        enqueue_push(&job_store, REMOVED_DOWNSTREAM, NAMESPACE).await;

        let sink = DryRunSink;
        checker(job_store.clone(), OrphanQueue::Replication)
            .check_all(&sink)
            .await
            .unwrap();

        assert_eq!(
            job_store
                .count_pending(Queue::Replication, 0)
                .await
                .unwrap(),
            2,
            "dry-run must leave both jobs in place"
        );
    }

    #[tokio::test]
    async fn orphan_pending_cache_job_with_unresolvable_namespace_emits_delete_action() {
        let FsTestStack {
            dir: _dir,
            metadata_store,
            ..
        } = fs_test_stack();
        let job_store = orphan_job_store(&metadata_store);

        enqueue_cache_fill(&job_store, GHOST_NAMESPACE).await;
        let keys = job_store.list_pending(Queue::Cache, 10).await.unwrap();
        assert_eq!(keys.len(), 1);

        let sink: std::sync::Mutex<Vec<Action>> = std::sync::Mutex::new(Vec::new());
        checker(job_store, OrphanQueue::Cache)
            .check_all(&sink)
            .await
            .unwrap();

        assert_eq!(
            sink.lock().unwrap().len(),
            1,
            "an unresolvable namespace is an orphan"
        );
        assert!(matches!(
            &sink.lock().unwrap()[0],
            Action::DeleteOrphanJob { queue, state, storage_key, reason }
                if *queue == Queue::Cache
                    && *state == JobState::Pending
                    && *storage_key == keys[0]
                    && reason == "namespace 'ghost/app' is not configured for pull-through"
        ));
    }

    #[tokio::test]
    async fn configured_pull_through_cache_job_emits_nothing() {
        let FsTestStack {
            dir: _dir,
            metadata_store,
            ..
        } = fs_test_stack();
        let job_store = orphan_job_store(&metadata_store);

        enqueue_cache_fill(&job_store, NAMESPACE).await;

        let sink: std::sync::Mutex<Vec<Action>> = std::sync::Mutex::new(Vec::new());
        checker(job_store, OrphanQueue::Cache)
            .check_all(&sink)
            .await
            .unwrap();

        assert!(
            sink.lock().unwrap().is_empty(),
            "a job for a configured pull-through repository must not emit an action, got {} action(s)",
            sink.lock().unwrap().len()
        );
    }

    #[tokio::test]
    async fn cache_job_for_non_pull_through_repository_emits_delete_action() {
        let FsTestStack {
            dir: _dir,
            metadata_store,
            ..
        } = fs_test_stack();
        let job_store = orphan_job_store(&metadata_store);

        // The namespace resolves to a configured repository, but one with no
        // upstreams: a fill job for it can never fetch anything.
        enqueue_cache_fill(&job_store, LOCAL_REPO).await;

        let sink: std::sync::Mutex<Vec<Action>> = std::sync::Mutex::new(Vec::new());
        checker(job_store, OrphanQueue::Cache)
            .check_all(&sink)
            .await
            .unwrap();

        assert_eq!(
            sink.lock().unwrap().len(),
            1,
            "a non-pull-through repository is an orphan"
        );
        assert!(matches!(
            &sink.lock().unwrap()[0],
            Action::DeleteOrphanJob { queue, state, reason, .. }
                if *queue == Queue::Cache
                    && *state == JobState::Pending
                    && reason == "namespace 'local' is not configured for pull-through"
        ));
    }

    #[tokio::test]
    async fn orphan_failed_cache_job_emits_action_with_failed_state() {
        let FsTestStack {
            dir: _dir,
            metadata_store,
            ..
        } = fs_test_stack();
        let job_store = orphan_job_store(&metadata_store);

        dead_letter(&job_store, Queue::Cache, cache_envelope(GHOST_NAMESPACE)).await;

        let sink: std::sync::Mutex<Vec<Action>> = std::sync::Mutex::new(Vec::new());
        checker(job_store, OrphanQueue::Cache)
            .check_all(&sink)
            .await
            .unwrap();

        assert_eq!(
            sink.lock().unwrap().len(),
            1,
            "the dead-lettered orphan must emit one action"
        );
        assert!(matches!(
            &sink.lock().unwrap()[0],
            Action::DeleteOrphanJob { queue, state, reason, .. }
                if *queue == Queue::Cache
                    && *state == JobState::Failed
                    && reason.contains(GHOST_NAMESPACE)
        ));
    }

    #[tokio::test]
    async fn malformed_cache_payload_is_skipped_without_action() {
        let FsTestStack {
            dir: _dir,
            metadata_store,
            ..
        } = fs_test_stack();
        let job_store = orphan_job_store(&metadata_store);

        let envelope = JobEnvelope::new(
            Queue::Cache,
            CACHE_FETCH_BLOB_KIND,
            "cache.bogus",
            &json!({ "not": "a payload" }),
        )
        .unwrap();
        job_store.enqueue(envelope).await.unwrap();

        let sink: std::sync::Mutex<Vec<Action>> = std::sync::Mutex::new(Vec::new());
        checker(job_store.clone(), OrphanQueue::Cache)
            .check_all(&sink)
            .await
            .unwrap();

        assert!(
            sink.lock().unwrap().is_empty(),
            "a payload scrub cannot attribute must be skipped, got {} action(s)",
            sink.lock().unwrap().len()
        );
        assert_eq!(
            job_store.count_pending(Queue::Cache, 0).await.unwrap(),
            1,
            "the undecodable job must stay in the queue"
        );
    }

    /// End-to-end shape: checker into executor deletes only the orphan cache
    /// job and the configured pull-through job survives.
    #[tokio::test]
    async fn end_to_end_deletes_only_the_orphan_cache_job() {
        let FsTestStack {
            dir: _dir,
            metadata_store,
            blob_store,
            ..
        } = fs_test_stack();
        let job_store = orphan_job_store(&metadata_store);

        enqueue_cache_fill(&job_store, NAMESPACE).await;
        enqueue_cache_fill(&job_store, GHOST_NAMESPACE).await;
        assert_eq!(job_store.count_pending(Queue::Cache, 0).await.unwrap(), 2);

        let mut executor: Box<dyn ActionSink> =
            Box::new(Executor::new(blob_store, metadata_store, job_store.clone()));
        checker(job_store.clone(), OrphanQueue::Cache)
            .check_all(executor.as_mut())
            .await
            .unwrap();

        let keys = job_store.list_pending(Queue::Cache, 10).await.unwrap();
        assert_eq!(keys.len(), 1, "only the orphan job must be deleted");
        let survivor = job_store
            .read_pending(Queue::Cache, &keys[0])
            .await
            .unwrap();
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
        let FsTestStack {
            dir: _dir,
            metadata_store,
            ..
        } = fs_test_stack();
        let job_store = orphan_job_store(&metadata_store);

        enqueue_cache_fill(&job_store, NAMESPACE).await;
        enqueue_cache_fill(&job_store, GHOST_NAMESPACE).await;

        let sink = DryRunSink;
        checker(job_store.clone(), OrphanQueue::Cache)
            .check_all(&sink)
            .await
            .unwrap();

        assert_eq!(
            job_store.count_pending(Queue::Cache, 0).await.unwrap(),
            2,
            "dry-run must leave both jobs in place"
        );
    }
}
