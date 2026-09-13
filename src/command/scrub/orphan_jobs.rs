//! [`OrphanJobChecker`]: emits a delete action for every job, pending or
//! dead-lettered, whose payload no longer resolves to configured state, so
//! stale config stops churning the queue through retries.

use std::sync::Arc;

use futures_util::stream::{self, StreamExt, TryStreamExt};
use serde_json::Value;
use tracing::{info, warn};

use crate::{
    cache_fill::CacheFetchBlobPayload,
    command::maintenance::{Error, action::Action, executor::ActionSink},
    jobs::store::{Error as JobStoreError, JobStore},
    jobs::{JobState, Queue},
    registry::{Repository, repository_resolver::RepositoryResolver},
    replication::ReplicationJob,
    scan::ScanImagePayload,
};

/// Keyset page size for the pending and failed scans; pages are looped to
/// exhaustion, so this only bounds memory per round-trip.
const PAGE_SIZE: u16 = 256;

/// Classifies one raw payload of `queue`: `Ok(Some(reason))` for an orphan,
/// `Ok(None)` for a job that still resolves, `Err` for a payload that fails to
/// decode. A replication job is orphaned when its downstream or repository is
/// no longer configured, a cache fill when its repository no longer pulls
/// through.
fn classify(
    queue: Queue,
    resolver: &RepositoryResolver,
    payload: Value,
) -> Result<Option<String>, serde_json::Error> {
    match queue {
        // A downstream whose mode changed still resolves, so it is not an
        // orphan.
        Queue::Replication => {
            let job: ReplicationJob = serde_json::from_value(payload)?;
            let target = job.target();
            let configured = resolver
                .resolve(&target.namespace)
                .is_some_and(|repository| {
                    repository
                        .replication
                        .iter()
                        .any(|d| d.name == target.downstream)
                });
            Ok((!configured).then(|| {
                format!(
                    "downstream '{}' is not configured for '{}'",
                    target.downstream, target.namespace
                )
            }))
        }
        // Such a job could still complete on drain if the blob is already
        // local, but granting a reference to a namespace removed from
        // pull-through config serves nothing.
        Queue::Scan => {
            let payload: ScanImagePayload = serde_json::from_value(payload)?;
            let configured = resolver
                .resolve(&payload.namespace)
                .is_some_and(|repository| repository.scan);
            Ok((!configured).then(|| {
                format!(
                    "namespace '{}' is not configured for scanning",
                    payload.namespace
                )
            }))
        }
        Queue::Index => Ok(None),
        Queue::Cache => {
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

/// Scans every pending and dead-lettered job on one queue and emits
/// [`Action::DeleteOrphanJob`] for each whose payload no longer resolves to
/// configured state.
pub struct OrphanJobChecker {
    job_store: Arc<JobStore>,
    resolver: Arc<RepositoryResolver>,
    queue: Queue,
    payload_read_concurrency: usize,
}

impl OrphanJobChecker {
    /// `payload_read_concurrency` is the scrub command's `--concurrency`.
    #[must_use]
    pub fn new(
        job_store: Arc<JobStore>,
        resolver: Arc<RepositoryResolver>,
        queue: Queue,
        payload_read_concurrency: usize,
    ) -> Self {
        Self {
            job_store,
            resolver,
            queue,
            payload_read_concurrency: payload_read_concurrency.max(1),
        }
    }

    /// Reads the raw payload for one storage key, `Ok(None)` for a key that
    /// vanished mid-scan; decoding is left to [`classify`].
    async fn read_payload(
        &self,
        state: JobState,
        storage_key: &str,
    ) -> Result<Option<Value>, Error> {
        let queue = self.queue;
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
        let queue = self.queue;
        let mut orphans: u64 = 0;
        let mut after: Option<String> = None;
        loop {
            let page = match state {
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

            // Payload reads fan out; classification and the sink stay serial
            // in key order.
            let payloads: Vec<(String, Option<Value>)> = stream::iter(page.items)
                .map(|storage_key| async move {
                    let payload = self.read_payload(state, &storage_key).await?;
                    Ok::<_, Error>((storage_key, payload))
                })
                .buffered(self.payload_read_concurrency)
                .try_collect()
                .await?;

            for (storage_key, payload) in payloads {
                let Some(payload) = payload else {
                    continue;
                };
                // A payload that fails to decode is skipped: scrub must not
                // delete what it cannot attribute.
                let reason = match classify(self.queue, &self.resolver, payload) {
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

            let Some(cursor) = page.next_token else {
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
            "scrub: found {pending} orphan pending and {failed} orphan dead-lettered {} job(s)",
            self.queue
        );
        Ok(())
    }
}

/// Delete queued jobs on both durable queues whose downstream or repository
/// is no longer configured.
pub async fn sweep_orphan_jobs(
    job_store: &Arc<JobStore>,
    resolver: &Arc<RepositoryResolver>,
    sink: &dyn ActionSink,
    concurrency: usize,
) -> Result<(), Error> {
    for queue in [Queue::Replication, Queue::Cache, Queue::Scan, Queue::Index] {
        OrphanJobChecker::new(job_store.clone(), resolver.clone(), queue, concurrency)
            .check_all(sink)
            .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use serde_json::json;

    use angos_oci::{Namespace, Tag};

    use crate::{
        cache_fill::{CACHE_FETCH_BLOB_KIND, CacheFetchBlobPayload},
        command::{
            maintenance::{
                action::Action,
                executor::{ActionSink, DryRunSink, Executor},
            },
            scrub::orphan_jobs::{OrphanJobChecker, classify},
        },
        jobs::{
            JobState, Queue,
            store::{ClaimMode, FailOutcome, JobEnvelope, JobStore},
        },
        policy::{RetentionPolicy, RetentionPolicyConfig, SystemClock},
        registry::{
            Repository,
            metadata_store::MetadataStore,
            repository::Upstream,
            repository_resolver::RepositoryResolver,
            test_utils::{FsTestStack, downstream_client, fs_test_stack},
        },
        registry_client::RegistryClient,
        replication::{
            REPLICATION_PUSH_MANIFEST_KIND, ReplicationDownstream, ReplicationJob,
            ReplicationTarget, build_envelope,
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
        Arc::new(JobStore::new(
            metadata_store.object_store().clone(),
            "orphan-test",
            ClaimMode::Atomic,
        ))
    }

    /// Never dialed by these checkers.
    fn upstream_client() -> RegistryClient {
        Arc::into_inner(downstream_client("http://127.0.0.1:1")).expect("unshared test client")
    }

    fn repository(
        name: &str,
        upstreams: Vec<RegistryClient>,
        replication: Vec<ReplicationDownstream>,
    ) -> Repository {
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
            namespace: None,
            upstreams,
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

    /// Resolver with a pull-through repository whose only downstream is
    /// [`DOWNSTREAM`], plus a configured repository with no upstreams.
    fn resolver() -> Arc<RepositoryResolver> {
        let downstream = ReplicationDownstream::new(
            DOWNSTREAM.to_string(),
            downstream_client("http://127.0.0.1:1"),
            4,
        );
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

    fn checker(job_store: Arc<JobStore>, queue: Queue) -> OrphanJobChecker {
        OrphanJobChecker::new(job_store, resolver(), queue, 16)
    }

    fn push_payload(downstream: &str, namespace: &str) -> ReplicationJob {
        ReplicationJob::Push {
            target: ReplicationTarget {
                downstream: downstream.to_string(),
                namespace: Namespace::new(namespace).unwrap(),
                tag: Some(Tag::new("v1").unwrap()),
                digest: None,
                source_ts: None,
            },
        }
    }

    fn cache_envelope(namespace: &str) -> JobEnvelope {
        let payload = CacheFetchBlobPayload {
            namespace: Namespace::new(namespace).unwrap(),
            digest: DIGEST.parse().unwrap(),
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
        envelope.max_attempts = Some(1);
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

    /// The conformance gate seeds this payload by hand, so decoding it here
    /// keeps the fixture honest about what the queue stores.
    #[test]
    fn the_gate_orphan_fixture_decodes_and_classifies() {
        let payload = json!({
            "downstream": "gate-ghost-downstream",
            "namespace": "conformance/gate",
            "tag": "gate",
            "kind": "replication.push_manifest",
        });
        let reason = classify(Queue::Replication, &resolver(), payload)
            .expect("the seeded payload must decode");
        assert!(
            reason.is_some_and(|r| r.contains("gate-ghost-downstream")),
            "an unconfigured downstream must classify as an orphan"
        );
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
        checker(job_store, Queue::Replication)
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
        checker(job_store, Queue::Replication)
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

        // The downstream name is configured, but only for `nginx`.
        enqueue_push(&job_store, DOWNSTREAM, GHOST_NAMESPACE).await;

        let sink: std::sync::Mutex<Vec<Action>> = std::sync::Mutex::new(Vec::new());
        checker(job_store, Queue::Replication)
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
        checker(job_store, Queue::Replication)
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
        checker(job_store.clone(), Queue::Replication)
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
        checker(job_store.clone(), Queue::Replication)
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
        let payload: ReplicationJob = serde_json::from_value(survivor.payload).unwrap();
        assert_eq!(
            payload.target().downstream,
            DOWNSTREAM,
            "the configured downstream's job must survive"
        );
    }

    /// The checker reads regardless of dry-run, but `DryRunSink` applies
    /// nothing.
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
        checker(job_store.clone(), Queue::Replication)
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
        checker(job_store, Queue::Cache)
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
        checker(job_store, Queue::Cache)
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

        // Configured, but with no upstreams: a fill job can never fetch.
        enqueue_cache_fill(&job_store, LOCAL_REPO).await;

        let sink: std::sync::Mutex<Vec<Action>> = std::sync::Mutex::new(Vec::new());
        checker(job_store, Queue::Cache)
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
        checker(job_store, Queue::Cache)
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
        checker(job_store.clone(), Queue::Cache)
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
        checker(job_store.clone(), Queue::Cache)
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

    /// The checker reads regardless of dry-run, but `DryRunSink` applies
    /// nothing.
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
        checker(job_store.clone(), Queue::Cache)
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
