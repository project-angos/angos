use tokio::select;
use tracing::{error, info, warn};

use crate::registry::job_store::{
    ClaimedJob, CompleteOutcome, Error, FailOutcome, JobHandler, JobStore,
};

/// Execute one claimed job: observe the lock session's cancellation
/// token alongside the handler future, then complete, fail (with retry
/// or dead-letter), or abort on lock loss. The heartbeat is internal to
/// the session held in `claimed.session`; it stops automatically when the
/// session is consumed by `complete`/`fail` or dropped on the lock-lost
/// branch.
pub async fn execute_one(consumer: &JobStore, handler: &dyn JobHandler, claimed: ClaimedJob) {
    let lock_key = claimed.envelope.lock_key.clone();
    let lock_lost = claimed.session.cancellation();

    let handler_result = select! {
        result = handler.execute(&claimed.envelope) => Some(result),
        () = lock_lost.cancelled() => None,
    };

    match handler_result {
        None => warn!(lock_key, "Lock lost during execution; aborting"),
        Some(Ok(tx)) => match consumer.complete(claimed, tx).await {
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
            match consumer.fail(claimed, &err_msg).await {
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

/// Drive one full claim → execute → complete/fail cycle. Returns `true` when
/// a job was processed and `false` when no claimable job remains.
pub async fn run_once(
    consumer: &JobStore,
    handler: &dyn JobHandler,
    queue: &str,
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
    use angos_tx_engine::{
        executor::build_executor,
        lock::{LockSession, LockStrategy},
        store::Store,
        transaction::Transaction,
    };

    use crate::{
        command::worker::runner::{execute_one, run_once},
        metrics_provider,
        registry::job_store::{ClaimedJob, Error, JobEnvelope, JobHandler, JobStore},
    };

    struct OkHandler;

    #[async_trait]
    impl JobHandler for OkHandler {
        async fn execute(&self, _envelope: &JobEnvelope) -> Result<Transaction, Error> {
            Ok(Transaction::builder().build())
        }
    }

    fn make_store(dir: &TempDir) -> Arc<JobStore> {
        let object: Arc<dyn ObjectStore> = Arc::new(
            StorageFsBackend::builder()
                .root_dir(dir.path().to_str().expect("valid path"))
                .build()
                .expect("fs storage"),
        );
        let executor = build_executor(
            object.clone(),
            None,
            LockStrategy::Memory,
            None,
            false,
            false,
        )
        .expect("build executor");
        let facade = Arc::new(
            Store::builder()
                .object(object)
                .executor(executor)
                .build()
                .expect("store façade"),
        );
        Arc::new(JobStore::new(facade, "test-worker"))
    }

    #[tokio::test]
    async fn run_once_returns_false_on_empty_queue() {
        metrics_provider::init_for_tests();
        let dir = TempDir::new().expect("temp dir");
        let store = make_store(&dir);
        let found = run_once(&store, &OkHandler, "cache")
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
                JobEnvelope::new("cache", "test.noop", "cache.ns:sha256:aabbcc", &())
                    .expect("envelope"),
            )
            .await
            .expect("enqueue");

        assert!(
            run_once(&store, &OkHandler, "cache")
                .await
                .expect("run_once"),
            "queue with one job must return true"
        );
        assert!(
            !run_once(&store, &OkHandler, "cache")
                .await
                .expect("run_once second call"),
            "queue must be empty after job completes"
        );
    }

    /// Handler that sleeps for `duration` and records whether it ran to
    /// completion. Used to assert that a lost lock cancels the handler
    /// future before its own work finishes.
    struct SleepyHandler {
        duration: Duration,
        completed: Arc<AtomicBool>,
    }

    #[async_trait]
    impl JobHandler for SleepyHandler {
        async fn execute(&self, _envelope: &JobEnvelope) -> Result<Transaction, Error> {
            sleep(self.duration).await;
            self.completed.store(true, Ordering::Release);
            Ok(Transaction::builder().build())
        }
    }

    /// If the session's heartbeat fires its cancellation mid-execution,
    /// `execute_one` drops the handler future before it completes its
    /// own work: the in-flight operation is cancelled. The test
    /// substitutes a hand-built `LockSession` whose cancellation token
    /// we fire ourselves, so it pins the runner's `select!` behaviour
    /// without depending on backend timing.
    #[tokio::test]
    async fn execute_one_cancels_handler_when_lock_lost() {
        metrics_provider::init_for_tests();
        let dir = TempDir::new().expect("temp dir");
        let consumer = make_store(&dir);

        let lost = CancellationToken::new();
        let lost_clone = lost.clone();
        let session = LockSession::with_async_release_and_heartbeat(
            || Box::pin(async {}),
            lost,
            tokio::spawn(async {}),
        );
        let claimed = ClaimedJob {
            envelope: JobEnvelope::new("cache", "test.sleep", "cache.ns:sha256:lost", &())
                .expect("envelope"),
            storage_key: "00000000-0000-0000-0000-000000000000".to_string(),
            session,
        };

        let completed = Arc::new(AtomicBool::new(false));
        let handler = SleepyHandler {
            // Longer than the test timeout below: cancellation is the
            // only way `execute_one` returns in time.
            duration: Duration::from_secs(30),
            completed: completed.clone(),
        };

        // Fire the lost token shortly after `execute_one` starts so
        // the runner's `select!` picks the cancellation arm.
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
