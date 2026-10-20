use std::sync::Arc;

use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use angos_backoff::Backoff;

use crate::{
    jobs::Queue,
    jobs::store::{ClaimedJob, CompleteOutcome, Error, FailOutcome, JobHandler, JobStore},
};

/// One in-process claim loop over `queue`. Each poll is a `LIST`, so empty polls
/// grow the wait by `idle`; only this process's enqueues cut it short. A claimed
/// job runs to completion: cancellation races the claim and the wait only.
pub async fn claim_loop(
    consumer: Arc<JobStore>,
    handler: Arc<dyn JobHandler>,
    queue: Queue,
    shutdown: CancellationToken,
    idle: Backoff,
) {
    let mut empty_polls: u32 = 0;
    loop {
        let wait = select! {
            () = shutdown.cancelled() => return,
            outcome = consumer.claim_one(queue) => match outcome {
                // `claim_one` self-throttles on a backend error; nothing to do.
                Err(_) => continue,
                Ok(claim_outcome) => match claim_outcome.claimed {
                    // A backed-off job is due at a known instant; only an empty
                    // queue grows the idle backoff.
                    None if claim_outcome.next_ready.is_some() => {
                        claim_outcome.idle_sleep(idle.step(0))
                    }
                    None => {
                        let wait = idle.delay(empty_polls);
                        empty_polls = empty_polls.saturating_add(1);
                        wait
                    }
                    Some(claimed) => {
                        empty_polls = 0;
                        if claim_outcome.more_ready {
                            consumer.wake_one(queue);
                        }
                        execute_one(consumer.as_ref(), handler.as_ref(), claimed).await;
                        continue;
                    }
                },
            },
        };
        select! {
            () = shutdown.cancelled() => return,
            () = consumer.wait_for_work(queue, wait) => {}
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
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };

    use async_trait::async_trait;
    use tempfile::TempDir;
    use tokio::{
        sync::{
            Barrier,
            mpsc::{UnboundedSender, unbounded_channel},
        },
        time::{Instant, sleep, timeout},
    };

    use tokio_util::sync::CancellationToken;

    use angos_backoff::Backoff;
    use angos_storage::{
        Error as StorageError, ObjectStore,
        fs::Backend as StorageFsBackend,
        test_util::{HookedStore, StoreHook, StoreOp},
    };

    use crate::{
        jobs::Queue,
        jobs::{
            runner::{claim_loop, execute_one, run_once},
            store::{ClaimMode, ClaimedJob, Error, JobEnvelope, JobHandler, JobStore},
        },
        metrics_provider,
        registry::test_utils::metadata_store_over,
    };

    struct OkHandler;

    #[async_trait]
    impl JobHandler for OkHandler {
        async fn execute(&self, _envelope: &JobEnvelope) -> Result<(), Error> {
            Ok(())
        }
    }

    fn make_store(dir: &TempDir) -> Arc<JobStore> {
        let object: Arc<dyn ObjectStore> =
            Arc::new(StorageFsBackend::builder(dir.path().to_str().expect("valid path")).build());
        Arc::new(JobStore::new(
            &metadata_store_over(object),
            "test-worker",
            ClaimMode::Atomic,
        ))
    }

    /// Reports each executed job's lock key.
    struct ReportingHandler(UnboundedSender<String>);

    #[async_trait]
    impl JobHandler for ReportingHandler {
        async fn execute(&self, envelope: &JobEnvelope) -> Result<(), Error> {
            let _ = self.0.send(envelope.lock_key.as_str().to_string());
            Ok(())
        }
    }

    /// Records when each poll of a pending queue starts.
    #[derive(Clone, Default)]
    struct PollLog(Arc<std::sync::Mutex<Vec<Instant>>>);

    impl PollLog {
        fn gaps_ms(&self) -> Vec<u128> {
            let polls = self.0.lock().expect("poll log");
            polls
                .windows(2)
                .map(|w| (w[1] - w[0]).as_millis())
                .collect()
        }
    }

    #[async_trait]
    impl StoreHook for PollLog {
        async fn before(&self, op: StoreOp<'_>) -> Result<(), StorageError> {
            if let StoreOp::List { prefix } = op
                && prefix.contains("/pending/")
            {
                self.0.lock().expect("poll log").push(Instant::now());
            }
            Ok(())
        }
    }

    fn logged_store(dir: &TempDir, log: PollLog) -> Arc<JobStore> {
        let object: Arc<dyn ObjectStore> =
            Arc::new(StorageFsBackend::builder(dir.path().to_str().expect("valid path")).build());
        let hooked: Arc<dyn ObjectStore> = Arc::new(HookedStore::new(object, log));
        Arc::new(JobStore::new(
            &metadata_store_over(hooked),
            "test-worker",
            ClaimMode::Atomic,
        ))
    }

    fn noop_job(lock_key: &str) -> JobEnvelope {
        JobEnvelope::new(Queue::Cache, "test.noop", lock_key, &()).expect("envelope")
    }

    fn long_idle() -> Backoff {
        Backoff::constant(Duration::from_secs(30))
    }

    fn spawn_loop(
        store: &Arc<JobStore>,
        handler: Arc<dyn JobHandler>,
        idle: Backoff,
    ) -> (CancellationToken, tokio::task::JoinHandle<()>) {
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(claim_loop(
            store.clone(),
            handler,
            Queue::Cache,
            shutdown.clone(),
            idle,
        ));
        (shutdown, task)
    }

    // Under paused time the clock only moves once every task, file I/O
    // included, is parked, so a test sleep returns with the loops idle.

    #[tokio::test(start_paused = true)]
    async fn idle_loop_wakes_on_local_enqueue() {
        metrics_provider::init_for_tests();
        let dir = TempDir::new().expect("temp dir");
        let store = make_store(&dir);
        let (tx, mut rx) = unbounded_channel();
        let (shutdown, task) = spawn_loop(&store, Arc::new(ReportingHandler(tx)), long_idle());

        sleep(Duration::from_millis(10)).await;
        store
            .enqueue(noop_job("cache.ns:sha256:woken"))
            .await
            .expect("enqueue");

        let ran = timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("a local enqueue must wake the idle loop before its next poll");
        assert_eq!(ran.as_deref(), Some("cache.ns:sha256:woken"));
        shutdown.cancel();
        task.await.expect("loop task");
    }

    #[tokio::test(start_paused = true)]
    async fn idle_loop_waits_for_its_poll_on_a_peer_enqueue() {
        metrics_provider::init_for_tests();
        let dir = TempDir::new().expect("temp dir");
        let store = make_store(&dir);
        // A second store over the same backend stands in for another replica.
        let peer = make_store(&dir);
        let (tx, mut rx) = unbounded_channel();
        let (shutdown, task) = spawn_loop(&store, Arc::new(ReportingHandler(tx)), long_idle());

        sleep(Duration::from_millis(10)).await;
        peer.enqueue(noop_job("cache.ns:sha256:peer"))
            .await
            .expect("enqueue");

        assert!(
            timeout(Duration::from_secs(29), rx.recv()).await.is_err(),
            "a peer's enqueue must not wake this process's loop"
        );
        let ran = timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("the next poll must claim the peer's job");
        assert_eq!(ran.as_deref(), Some("cache.ns:sha256:peer"));
        shutdown.cancel();
        task.await.expect("loop task");
    }

    #[tokio::test(start_paused = true)]
    async fn idle_backoff_grows_and_resets_on_a_claim() {
        metrics_provider::init_for_tests();
        let dir = TempDir::new().expect("temp dir");
        let log = PollLog::default();
        let store = logged_store(&dir, log.clone());
        let (tx, mut rx) = unbounded_channel();
        let idle = Backoff::exponential(Duration::from_millis(100), Duration::from_millis(400));
        let (shutdown, task) = spawn_loop(&store, Arc::new(ReportingHandler(tx)), idle);

        // Polls at 0, 100, 300, 700 and 1100 ms.
        sleep(Duration::from_millis(1150)).await;
        assert_eq!(log.gaps_ms(), [100, 200, 400, 400]);

        store
            .enqueue(noop_job("cache.ns:sha256:reset"))
            .await
            .expect("enqueue");
        rx.recv().await.expect("the woken loop must run the job");
        let claimed_at = log.0.lock().expect("poll log").len();
        sleep(Duration::from_millis(350)).await;

        // The claiming poll is followed at once by an empty one, then the
        // backoff starts over.
        let gaps = log.gaps_ms();
        assert_eq!(gaps[claimed_at - 1..], [0, 100, 200]);
        shutdown.cancel();
        task.await.expect("loop task");
    }

    /// Holds each job until as many as the barrier counts run at once.
    struct BurstHandler(Arc<Barrier>, UnboundedSender<()>);

    #[async_trait]
    impl JobHandler for BurstHandler {
        async fn execute(&self, _envelope: &JobEnvelope) -> Result<(), Error> {
            self.0.wait().await;
            let _ = self.1.send(());
            Ok(())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_claim_wakes_a_sibling_so_a_burst_runs_at_full_concurrency() {
        const LOOPS: usize = 3;
        metrics_provider::init_for_tests();
        let dir = TempDir::new().expect("temp dir");
        let store = make_store(&dir);
        let peer = make_store(&dir);
        let (tx, mut rx) = unbounded_channel();
        let handler: Arc<dyn JobHandler> =
            Arc::new(BurstHandler(Arc::new(Barrier::new(LOOPS)), tx));
        let loops: Vec<_> = (0..LOOPS)
            .map(|_| spawn_loop(&store, handler.clone(), long_idle()))
            .collect();

        sleep(Duration::from_millis(10)).await;
        for i in 0..LOOPS {
            peer.enqueue(noop_job(&format!("cache.ns:sha256:burst{i}")))
                .await
                .expect("enqueue");
        }
        // One wake, as a single local enqueue would give; the claims must
        // pass it on for every loop to join in.
        store.wake_one(Queue::Cache);

        for _ in 0..LOOPS {
            timeout(Duration::from_secs(1), rx.recv())
                .await
                .expect("every loop must join the burst before its next poll");
        }
        for (shutdown, task) in loops {
            shutdown.cancel();
            task.await.expect("loop task");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn idle_loop_stops_on_shutdown() {
        metrics_provider::init_for_tests();
        let dir = TempDir::new().expect("temp dir");
        let store = make_store(&dir);
        let (shutdown, task) = spawn_loop(&store, Arc::new(OkHandler), long_idle());

        sleep(Duration::from_millis(10)).await;
        shutdown.cancel();
        timeout(Duration::from_secs(1), task)
            .await
            .expect("an idle loop must stop at shutdown, not at the end of its wait")
            .expect("loop task");
    }

    #[tokio::test]
    async fn run_once_returns_false_on_empty_queue() {
        metrics_provider::init_for_tests();
        let dir = TempDir::new().expect("temp dir");
        let store = make_store(&dir);
        let found = run_once(&store, &OkHandler, Queue::Cache)
            .await
            .expect("run_once");
        assert!(!found, "empty queue must return false");
    }

    #[tokio::test]
    async fn run_once_processes_one_job() {
        metrics_provider::init_for_tests();
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
        metrics_provider::init_for_tests();
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
        metrics_provider::init_for_tests();
        let dir = TempDir::new().expect("temp dir");
        let consumer = make_store(&dir);

        let lost = CancellationToken::new();
        let lost_clone = lost.clone();
        let claimed = ClaimedJob::for_test(
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

        tokio::spawn(async move {
            sleep(Duration::from_millis(100)).await;
            lost_clone.cancel();
        });

        let started = Instant::now();
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
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "execute_one must abort on lock loss long before the handler's 30s sleep elapses"
        );
    }
}
