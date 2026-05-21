use std::sync::Arc;

use async_trait::async_trait;

use crate::{
    metrics_provider::metrics_provider,
    registry::{
        job_store::{Error, JobEnvelope, JobHandler, JobQueue},
        task_queue::{Error as TaskError, TaskQueue},
    },
};

/// In-process job queue backed by `TaskQueue`. Translates `JobEnvelope`s into
/// `TaskQueue::submit` calls; preserves dedup and concurrency semantics of the
/// original cache-fill path.
pub struct InProcessJobQueue {
    task_queue: Arc<TaskQueue>,
    handler: Arc<dyn JobHandler>,
}

impl InProcessJobQueue {
    pub fn new(task_queue: Arc<TaskQueue>, handler: Arc<dyn JobHandler>) -> Self {
        Self {
            task_queue,
            handler,
        }
    }
}

#[async_trait]
impl JobQueue for InProcessJobQueue {
    async fn enqueue(&self, envelope: JobEnvelope) -> Result<(), Error> {
        let metrics = metrics_provider();
        let dedup_label = if self.task_queue.is_active(&envelope.lock_key) {
            "hit"
        } else {
            "miss"
        };
        metrics
            .job_queue_enqueued_total
            .with_label_values(&[envelope.queue.as_str(), dedup_label])
            .inc();

        let queue_name = envelope.queue.clone();
        let lock_key = envelope.lock_key.clone();
        let handler = self.handler.clone();
        self.task_queue.submit(&lock_key, async move {
            handler
                .execute(&envelope)
                .await
                .map_err(TaskError::TaskExecution)
        });

        metrics
            .job_queue_pending
            .with_label_values(&[queue_name.as_str()])
            .set(i64::try_from(self.task_queue.active_task_count()).unwrap_or(i64::MAX));

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::Arc,
        thread,
        time::{Duration, Instant},
    };

    use async_trait::async_trait;
    use tokio::sync::oneshot;

    use crate::{
        metrics_provider,
        registry::{
            job_store::{JobEnvelope, JobHandler, JobQueue, in_process::InProcessJobQueue},
            task_queue::TaskQueue,
        },
    };

    struct NoopHandler;

    #[async_trait]
    impl JobHandler for NoopHandler {
        async fn execute(&self, _envelope: &JobEnvelope) -> Result<(), String> {
            Ok(())
        }
    }

    fn make_task_queue(max: usize) -> Arc<TaskQueue> {
        Arc::new(TaskQueue::new(max).expect("TaskQueue::new must succeed"))
    }

    fn dummy_envelope(lock_key: &str) -> JobEnvelope {
        JobEnvelope::new("cache", "test.noop", lock_key, &()).expect("envelope")
    }

    fn make_queue(task_queue: Arc<TaskQueue>) -> InProcessJobQueue {
        InProcessJobQueue::new(task_queue, Arc::new(NoopHandler))
    }

    fn wait_until(predicate: impl Fn() -> bool) -> bool {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if predicate() {
                return true;
            }
            thread::sleep(Duration::from_millis(5));
        }
        false
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn enqueue_submits_to_task_queue() {
        metrics_provider::init_for_tests();
        let tq = make_task_queue(2);
        let queue = make_queue(tq.clone());

        queue
            .enqueue(dummy_envelope("cache.aaa"))
            .await
            .expect("enqueue must succeed");

        assert!(wait_until(|| tq.active_task_count() == 0));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn duplicate_lock_key_is_deduped() {
        metrics_provider::init_for_tests();
        let tq = make_task_queue(8);
        let queue = make_queue(tq.clone());

        let lock_key = "cache.dup";

        let (tx, rx) = oneshot::channel::<()>();
        tq.submit(lock_key, async move {
            let _ = rx.await;
            Ok(())
        });
        assert!(wait_until(|| tq.active_task_count() == 1));

        queue
            .enqueue(dummy_envelope(lock_key))
            .await
            .expect("enqueue must succeed");

        assert_eq!(
            tq.active_task_count(),
            1,
            "duplicate enqueue must not increase active count"
        );

        let _ = tx.send(());
        assert!(wait_until(|| tq.active_task_count() == 0));
    }
}
