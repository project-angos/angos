use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

#[cfg(test)]
use crate::registry::job_store::Error;
use crate::registry::job_store::{
    JobHandler,
    durable::{ClaimedJob, DurableJobConsumer, FailOutcome},
};

/// Execute one claimed job: spawn the heartbeat, run the handler, then either
/// complete, fail (with retry or dead-letter), or release on lease loss.
pub async fn execute_one(
    consumer: &DurableJobConsumer,
    handler: &dyn JobHandler,
    claimed: ClaimedJob,
) {
    let lease_cancel = CancellationToken::new();
    let hb = consumer.spawn_heartbeat(&claimed, lease_cancel.clone());
    let lock_key = claimed.envelope.lock_key.clone();

    let handler_result = select! {
        result = handler.execute(&claimed.envelope) => Some(result),
        () = lease_cancel.cancelled() => None,
    };
    lease_cancel.cancel();
    let _ = hb.await;

    match handler_result {
        None => warn!(lock_key, "Lease lost during execution; aborting"),
        Some(Ok(())) => match consumer.complete(claimed).await {
            Ok(()) => info!(lock_key, "Job completed successfully"),
            Err(e) => error!(lock_key, error = %e, "Failed to complete job"),
        },
        Some(Err(err)) => {
            warn!(lock_key, error = %err, "Job handler returned error");
            match consumer.fail(claimed, &err).await {
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
/// a job was processed. Used by tests to validate end-to-end mechanics.
#[cfg(test)]
pub async fn run_once(
    consumer: &DurableJobConsumer,
    handler: &dyn JobHandler,
    queue: &str,
) -> Result<bool, Error> {
    match consumer.claim_one(queue).await? {
        None => Ok(false),
        Some(claimed) => {
            execute_one(consumer, handler, claimed).await;
            Ok(true)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use tempfile::TempDir;

    use crate::{
        command::worker::runner::run_once,
        metrics_provider,
        registry::job_store::{
            JobEnvelope, JobHandler, JobQueue,
            durable::{DurableJobConsumer, DurableJobQueue},
            fs::{Backend, BackendConfig},
        },
    };

    struct OkHandler;

    #[async_trait]
    impl JobHandler for OkHandler {
        async fn execute(&self, _envelope: &JobEnvelope) -> Result<(), String> {
            Ok(())
        }
    }

    fn make_store(dir: &TempDir) -> Arc<Backend> {
        Arc::new(Backend::new(&BackendConfig {
            root_dir: dir.path().to_string_lossy().to_string(),
        }))
    }

    fn make_consumer(store: Arc<Backend>) -> Arc<DurableJobConsumer> {
        Arc::new(DurableJobConsumer::new(
            store,
            30,
            "test-worker".to_string(),
        ))
    }

    #[tokio::test]
    async fn run_once_returns_false_on_empty_queue() {
        metrics_provider::init_for_tests();
        let dir = TempDir::new().expect("temp dir");
        let consumer = make_consumer(make_store(&dir));
        let found = run_once(&consumer, &OkHandler, "cache")
            .await
            .expect("run_once");
        assert!(!found, "empty queue must return false");
    }

    #[tokio::test]
    async fn run_once_processes_one_job() {
        metrics_provider::init_for_tests();
        let dir = TempDir::new().expect("temp dir");
        let store = make_store(&dir);
        let queue = DurableJobQueue::new(store.clone());
        let consumer = make_consumer(store);

        queue
            .enqueue(
                JobEnvelope::new("cache", "test.noop", "cache.ns:sha256:aabbcc", &())
                    .expect("envelope"),
            )
            .await
            .expect("enqueue");

        assert!(
            run_once(&consumer, &OkHandler, "cache")
                .await
                .expect("run_once"),
            "queue with one job must return true"
        );
        assert!(
            !run_once(&consumer, &OkHandler, "cache")
                .await
                .expect("run_once second call"),
            "queue must be empty after job completes"
        );
    }
}
