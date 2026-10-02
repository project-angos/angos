use std::{sync::Arc, time::Duration};

use tokio::{select, time::sleep};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::{
    jobs::Queue,
    jobs::store::{ClaimedJob, CompleteOutcome, Error, FailOutcome, JobHandler, JobStore},
};

/// Idle poll interval of an in-process claim loop; production polls once a
/// second so an empty queue does not storm the object store with `LIST`s.
#[cfg(not(test))]
const IDLE_POLL: Duration = Duration::from_secs(1);
#[cfg(test)]
const IDLE_POLL: Duration = Duration::from_millis(10);

/// One in-process claim loop over `queue`, where `handler` is the handler bound
/// to it. Cancellation races only the claim, so a job already claimed runs to
/// completion rather than being interrupted.
pub async fn claim_loop(
    consumer: Arc<JobStore>,
    handler: Arc<dyn JobHandler>,
    queue: Queue,
    shutdown: CancellationToken,
) {
    loop {
        select! {
            () = shutdown.cancelled() => return,
            outcome = consumer.claim_one(queue) => match outcome {
                // `claim_one` self-throttles on a backend error; nothing to do.
                Err(_) => {}
                Ok(claim_outcome) => match claim_outcome.claimed {
                    None => sleep(claim_outcome.idle_sleep(IDLE_POLL)).await,
                    Some(claimed) => {
                        execute_one(consumer.as_ref(), handler.as_ref(), claimed).await;
                    }
                },
            },
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
    use tokio::time::{Instant, sleep, timeout};

    use tokio_util::sync::CancellationToken;

    use angos_storage::{ObjectStore, fs::Backend as StorageFsBackend};

    use crate::{
        jobs::Queue,
        jobs::{
            runner::{execute_one, run_once},
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
