use std::sync::Arc;
use std::time::Duration;

use tokio::select;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use angos_backoff::Backoff;

use crate::jobs::Queue;
use crate::jobs::store::{
    ClaimOutcome, ClaimedJob, CompleteOutcome, Error, FailOutcome, JobHandler, JobStore,
};

/// Floor on the wait for a backed-off job due at a known instant.
const IDLE_POLL: Duration = Duration::from_secs(1);

/// Wait between polls of an empty queue. Each poll is a `LIST`, so the wait
/// grows while the queue stays empty; this process's enqueues cut it short.
const IDLE_BACKOFF: Backoff =
    Backoff::exponential(Duration::from_secs(1), Duration::from_secs(15)).with_jitter();

/// One in-process claim loop over `queue`, where `handler` is the handler bound
/// to it. Cancellation races the claim and the idle wait only, so a job already
/// claimed runs to completion rather than being interrupted.
pub async fn claim_loop(
    consumer: Arc<JobStore>,
    handler: Arc<dyn JobHandler>,
    queue: Queue,
    shutdown: CancellationToken,
) {
    let mut empty_polls: u32 = 0;
    loop {
        let wait = select! {
            () = shutdown.cancelled() => return,
            outcome = consumer.claim_one(queue) => match outcome {
                // `claim_one` self-throttles on a backend error; nothing to do.
                Err(_) => continue,
                Ok(ClaimOutcome { claimed: Some(claimed), .. }) => {
                    empty_polls = 0;
                    execute_one(consumer.as_ref(), handler.as_ref(), claimed).await;
                    continue;
                }
                Ok(idle) if idle.next_ready.is_some() => idle.idle_sleep(IDLE_POLL),
                Ok(_) => {
                    empty_polls = empty_polls.saturating_add(1);
                    IDLE_BACKOFF.delay(empty_polls - 1)
                }
            },
        };
        select! {
            () = shutdown.cancelled() => return,
            _ = timeout(wait, consumer.waker(queue).notified()) => {}
        }
    }
}

/// Execute one claimed job, racing the handler against the claim's cancellation
/// token, then complete, fail, or abort on claim loss.
pub async fn execute_one(consumer: &JobStore, handler: &dyn JobHandler, claimed: ClaimedJob) {
    let lock_key = claimed.envelope.lock_key.to_string();
    let lock_lost = claimed.lock_lost();

    let handler_result = select! {
        result = handler.execute(&claimed.envelope) => Some(result),
        () = lock_lost.cancelled() => None,
    };

    match handler_result {
        None => warn!(lock_key, "Lock lost during execution; aborting"),
        Some(Ok(())) => match consumer.complete(claimed).await {
            Ok(CompleteOutcome::Completed) => info!(lock_key, "Job completed successfully"),
            Ok(CompleteOutcome::FailedOver(FailOutcome::Retried { next_at })) => {
                warn!(lock_key, %next_at, "Commit failed; job scheduled for retry");
            }
            Ok(CompleteOutcome::FailedOver(FailOutcome::MovedToDeadLetter)) => {
                warn!(lock_key, "Commit failed; job moved to dead-letter");
            }
            Err(e) => error!(lock_key, error = %e, "Failed to complete or fail job"),
        },
        Some(Err(err)) => {
            warn!(lock_key, error = %err, "Job handler returned error");
            let err_msg = err.to_string();
            // Dead-letter a terminal failure now rather than burn the retry
            // budget against an outcome that cannot change.
            let outcome = if matches!(err, Error::Terminal(_)) {
                consumer.fail_terminal(claimed, &err_msg).await
            } else {
                consumer.fail(claimed, &err_msg).await
            };
            match outcome {
                Ok(FailOutcome::Retried { next_at }) => {
                    info!(lock_key, %next_at, "Job scheduled for retry");
                }
                Ok(FailOutcome::MovedToDeadLetter) => {
                    warn!(lock_key, "Job moved to dead-letter");
                }
                Err(e) => error!(lock_key, error = %e, "Failed to record job failure"),
            }
        }
    }
}

/// Drive one claim, execute, and complete/fail cycle. Returns `true` when a job
/// was processed and `false` when no claimable job remains.
pub async fn run_once(
    consumer: &JobStore,
    handler: &dyn JobHandler,
    queue: Queue,
) -> Result<bool, Error> {
    match consumer.claim_one(queue).await?.claimed {
        None => Ok(false),
        Some(claimed) => {
            execute_one(consumer, handler, claimed).await;
            Ok(true)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use async_trait::async_trait;
    use tempfile::TempDir;
    use tokio::spawn;
    use tokio::sync::Barrier;
    use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
    use tokio::task::JoinHandle;
    use tokio::time::{Instant, sleep, timeout};
    use tokio_util::sync::CancellationToken;

    use angos_backoff::Backoff;
    use angos_storage::fs::Backend as StorageFsBackend;
    use angos_storage::test_util::{HookedStore, StoreHook, StoreOp};
    use angos_storage::{Error as StorageError, ObjectStore};

    use crate::jobs::Queue;
    use crate::jobs::runner::{claim_loop, execute_one, run_once};
    use crate::jobs::store::tests::claimed_job;
    use crate::jobs::store::{ClaimMode, Error, JobEnvelope, JobHandler, JobStore};
    use crate::metrics_provider;
    use crate::registry::test_utils::metadata_store_over;

    struct OkHandler;

    #[async_trait]
    impl JobHandler for OkHandler {
        async fn execute(&self, _envelope: &JobEnvelope) -> Result<(), Error> {
            Ok(())
        }
    }

    fn make_store(dir: &TempDir) -> Arc<JobStore> {
        let object: Arc<dyn ObjectStore> = Arc::new(StorageFsBackend::new(dir.path(), false));
        Arc::new(JobStore::new(
            &metadata_store_over(object),
            "test-worker",
            ClaimMode::Atomic,
        ))
    }

    /// Reports each executed job's lock key, after waiting on the barrier when
    /// one is set.
    struct ReportingHandler {
        ran: UnboundedSender<String>,
        barrier: Option<Barrier>,
    }

    #[async_trait]
    impl JobHandler for ReportingHandler {
        async fn execute(&self, envelope: &JobEnvelope) -> Result<(), Error> {
            if let Some(barrier) = &self.barrier {
                barrier.wait().await;
            }
            self.ran
                .send(envelope.lock_key.to_string())
                .expect("test receiver alive");
            Ok(())
        }
    }

    /// Records when each poll of a pending queue starts.
    #[derive(Clone, Default)]
    struct PollLog(Arc<Mutex<Vec<Instant>>>);

    #[async_trait]
    impl StoreHook for PollLog {
        async fn before(&self, op: StoreOp<'_>) -> Result<(), StorageError> {
            if let StoreOp::List { prefix } = op
                && prefix.contains("/pending")
            {
                self.0.lock().expect("poll log").push(Instant::now());
            }
            Ok(())
        }
    }

    fn spawn_loops(
        store: &Arc<JobStore>,
        handler: &Arc<ReportingHandler>,
        count: usize,
    ) -> (CancellationToken, Vec<JoinHandle<()>>) {
        let shutdown = CancellationToken::new();
        let tasks = (0..count)
            .map(|_| {
                spawn(claim_loop(
                    store.clone(),
                    handler.clone(),
                    Queue::Cache,
                    shutdown.clone(),
                ))
            })
            .collect();
        (shutdown, tasks)
    }

    async fn stop(shutdown: &CancellationToken, tasks: Vec<JoinHandle<()>>) {
        shutdown.cancel();
        for task in tasks {
            task.await.expect("loop task");
        }
    }

    /// Long enough for an idle loop's wait to reach the cap.
    const SETTLE: Duration = Duration::from_mins(2);

    // Under paused time the clock moves only once every task is parked, so
    // a job a woken loop claims runs at the instant of the wake.

    #[tokio::test(start_paused = true)]
    async fn local_enqueue_wakes_a_backed_off_loop() {
        metrics_provider::initialize_metrics();
        let dir = TempDir::new().expect("temp dir");
        let store = make_store(&dir);
        let (ran, mut reports) = unbounded_channel();
        let handler = Arc::new(ReportingHandler { ran, barrier: None });
        let (shutdown, tasks) = spawn_loops(&store, &handler, 1);

        sleep(SETTLE).await;
        store
            .enqueue(
                JobEnvelope::new(Queue::Cache, "test.noop", "cache.ns:sha256:woken", &())
                    .expect("envelope"),
            )
            .await
            .expect("enqueue");

        let ran = timeout(Duration::from_millis(1), reports.recv())
            .await
            .expect("a local enqueue must wake the loop, not wait for its next poll");
        assert_eq!(ran.as_deref(), Some("cache.ns:sha256:woken"));
        stop(&shutdown, tasks).await;
    }

    #[tokio::test(start_paused = true)]
    async fn idle_polls_back_off_find_a_peer_job_and_reset_on_a_claim() {
        metrics_provider::initialize_metrics();
        let dir = TempDir::new().expect("temp dir");
        let log = PollLog::default();
        let object: Arc<dyn ObjectStore> = Arc::new(HookedStore::new(
            Arc::new(StorageFsBackend::new(dir.path(), false)),
            log.clone(),
        ));
        let store = Arc::new(JobStore::new(
            &metadata_store_over(object),
            "test-worker",
            ClaimMode::Atomic,
        ));
        // A second store over the same backend stands in for another replica,
        // whose enqueue wakes nothing here.
        let peer = make_store(&dir);
        let (ran, mut reports) = unbounded_channel();
        let handler = Arc::new(ReportingHandler { ran, barrier: None });
        let (shutdown, tasks) = spawn_loops(&store, &handler, 1);

        sleep(SETTLE).await;
        let unjittered = Backoff::exponential(Duration::from_secs(1), Duration::from_secs(15));
        let polls = log.0.lock().expect("poll log").clone();
        for (attempt, pair) in (0..).zip(polls.windows(2)) {
            let gap = pair[1] - pair[0];
            let step = unjittered.delay(attempt);
            assert!(
                (step..step * 2).contains(&gap),
                "poll gap {attempt} was {gap:?}, expected jittered {step:?}"
            );
        }

        peer.enqueue(
            JobEnvelope::new(Queue::Cache, "test.noop", "cache.ns:sha256:peer", &())
                .expect("envelope"),
        )
        .await
        .expect("enqueue");
        let ran = timeout(Duration::from_secs(30), reports.recv())
            .await
            .expect("the next poll must claim a peer's job");
        assert_eq!(ran.as_deref(), Some("cache.ns:sha256:peer"));

        // The claim is followed at once by an empty poll, then the first step.
        let claimed_at = Instant::now();
        sleep(Duration::from_secs(10)).await;
        let next_poll = *log
            .0
            .lock()
            .expect("poll log")
            .iter()
            .find(|&&poll| poll > claimed_at + Duration::from_millis(1))
            .expect("a poll after the claim");
        let next_wait = next_poll - claimed_at;
        assert!(
            (Duration::from_secs(1)..Duration::from_secs(2)).contains(&next_wait),
            "a claim must restart the backoff, next poll came after {next_wait:?}"
        );
        stop(&shutdown, tasks).await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_claim_wakes_a_sibling_so_a_polled_burst_runs_at_full_concurrency() {
        const LOOPS: usize = 3;
        metrics_provider::initialize_metrics();
        let dir = TempDir::new().expect("temp dir");
        let store = make_store(&dir);
        let peer = make_store(&dir);
        let (ran, mut reports) = unbounded_channel();
        let handler = Arc::new(ReportingHandler {
            ran,
            barrier: Some(Barrier::new(LOOPS)),
        });
        let (shutdown, tasks) = spawn_loops(&store, &handler, LOOPS);

        sleep(SETTLE).await;
        for i in 0..LOOPS {
            peer.enqueue(
                JobEnvelope::new(
                    Queue::Cache,
                    "test.noop",
                    &format!("cache.ns:sha256:{i}"),
                    &(),
                )
                .expect("envelope"),
            )
            .await
            .expect("enqueue");
        }
        // One wake, as from a single local enqueue; the claims pass it on.
        store.waker(Queue::Cache).notify_one();

        for _ in 0..LOOPS {
            timeout(Duration::from_millis(1), reports.recv())
                .await
                .expect("every loop must join the burst before its next poll");
        }
        stop(&shutdown, tasks).await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_backed_off_loop_stops_at_shutdown() {
        metrics_provider::initialize_metrics();
        let dir = TempDir::new().expect("temp dir");
        let store = make_store(&dir);
        let (ran, _reports) = unbounded_channel();
        let handler = Arc::new(ReportingHandler { ran, barrier: None });
        let (shutdown, tasks) = spawn_loops(&store, &handler, 1);

        sleep(SETTLE).await;
        timeout(Duration::from_millis(1), stop(&shutdown, tasks))
            .await
            .expect("an idle loop must stop at shutdown, not at the end of its wait");
    }

    #[tokio::test]
    async fn run_once_processes_one_job() {
        metrics_provider::initialize_metrics();
        let dir = TempDir::new().expect("temp dir");
        let store = make_store(&dir);

        store
            .enqueue(
                JobEnvelope::new(Queue::Cache, "test.noop", "cache.ns:sha256:aabbcc", &())
                    .expect("envelope"),
            )
            .await
            .expect("enqueue");

        assert!(
            run_once(&store, &OkHandler, Queue::Cache)
                .await
                .expect("run_once"),
            "queue with one job must return true"
        );
        assert!(
            !run_once(&store, &OkHandler, Queue::Cache)
                .await
                .expect("run_once second call"),
            "queue must be empty after job completes"
        );
    }

    struct TerminalHandler;

    #[async_trait]
    impl JobHandler for TerminalHandler {
        async fn execute(&self, _envelope: &JobEnvelope) -> Result<(), Error> {
            Err(Error::Terminal("downstream forbade the push".to_string()))
        }
    }

    #[tokio::test]
    async fn terminal_error_dead_letters_without_retrying() {
        metrics_provider::initialize_metrics();
        let dir = TempDir::new().expect("temp dir");
        let store = make_store(&dir);

        store
            .enqueue(
                JobEnvelope::new(Queue::Cache, "test.terminal", "cache.ns:sha256:denied", &())
                    .expect("envelope"),
            )
            .await
            .expect("enqueue");

        assert!(
            run_once(&store, &TerminalHandler, Queue::Cache)
                .await
                .expect("run_once"),
            "the terminal job must be processed"
        );

        assert_eq!(
            store
                .count_failed(Queue::Cache)
                .await
                .expect("count_failed"),
            1,
            "a terminal job must be dead-lettered immediately"
        );
        assert_eq!(
            store
                .count_pending(Queue::Cache, 600)
                .await
                .expect("count_pending"),
            0,
            "a terminal job must not be re-queued for retry"
        );
    }

    /// Sleeps for `duration` and records whether it ran to completion.
    struct SleepyHandler {
        duration: Duration,
        completed: Arc<AtomicBool>,
    }

    #[async_trait]
    impl JobHandler for SleepyHandler {
        async fn execute(&self, _envelope: &JobEnvelope) -> Result<(), Error> {
            sleep(self.duration).await;
            self.completed.store(true, Ordering::Release);
            Ok(())
        }
    }

    /// A hand-fired cancellation token pins the runner's `select!` behaviour
    /// without depending on backend timing.
    #[tokio::test]
    async fn execute_one_cancels_handler_when_lock_lost() {
        metrics_provider::initialize_metrics();
        let dir = TempDir::new().expect("temp dir");
        let consumer = make_store(&dir);

        let lost = CancellationToken::new();
        let lost_clone = lost.clone();
        let claimed = claimed_job(
            JobEnvelope::new(Queue::Cache, "test.sleep", "cache.ns:sha256:lost", &())
                .expect("envelope"),
            "00000000-0000-0000-0000-000000000000".to_string(),
            lost,
        );

        let completed = Arc::new(AtomicBool::new(false));
        let handler = SleepyHandler {
            // Longer than the timeout below, so cancellation is the only way
            // `execute_one` returns in time.
            duration: Duration::from_secs(30),
            completed: completed.clone(),
        };

        spawn(async move {
            sleep(Duration::from_millis(100)).await;
            lost_clone.cancel();
        });

        timeout(
            Duration::from_secs(2),
            execute_one(&consumer, &handler, claimed),
        )
        .await
        .expect("execute_one must return after the lock is lost");

        assert!(
            !completed.load(Ordering::Acquire),
            "handler must be cancelled by lock loss before completing its sleep"
        );
    }
}
