use std::{collections::HashSet, fmt, future::Future, sync::Arc};

use parking_lot::Mutex;
use tokio::{runtime::Handle, sync::Semaphore};
use tracing::info;

#[derive(Debug)]
pub enum Error {
    Initialization(String),
    TaskExecution(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Initialization(e) => write!(f, "failed to initialize task queue: {e}"),
            Error::TaskExecution(e) => write!(f, "task execution failed: {e}"),
        }
    }
}

impl std::error::Error for Error {}

pub struct TaskQueue {
    handle: Handle,
    permits: Arc<Semaphore>,
    active_tasks: Arc<Mutex<HashSet<String>>>,
}

impl TaskQueue {
    pub fn new(max_concurrent_jobs: usize) -> Result<Self, Error> {
        if max_concurrent_jobs == 0 {
            return Err(Error::Initialization(
                "max_concurrent_cache_jobs must be greater than 0".to_string(),
            ));
        }

        Ok(Self {
            handle: Handle::try_current()
                .map_err(|error| Error::Initialization(error.to_string()))?,
            permits: Arc::new(Semaphore::new(max_concurrent_jobs)),
            active_tasks: Arc::new(Mutex::new(HashSet::new())),
        })
    }

    pub fn submit<Fut>(&self, reference: &str, fut: Fut)
    where
        Fut: Future<Output = Result<(), Error>> + Send + 'static,
    {
        if !self.active_tasks.lock().insert(reference.to_string()) {
            return;
        }

        info!("Starting task: {reference}");

        let reference = reference.to_string();
        let active_tasks = self.active_tasks.clone();
        let permits = self.permits.clone();
        self.handle.spawn(async move {
            if let Ok(_permit) = permits.acquire_owned().await {
                let _ = fut.await;
            }
            active_tasks.lock().remove(&reference);
        });
    }

    #[cfg(test)]
    fn active_task_count(&self) -> usize {
        self.active_tasks.lock().len()
    }

    #[cfg(test)]
    fn is_active(&self, reference: &str) -> bool {
        self.active_tasks.lock().contains(reference)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use tokio::sync::{Notify, oneshot};

    use super::{Error, TaskQueue};

    fn make_queue() -> TaskQueue {
        TaskQueue::new(2).expect("failed to build TaskQueue")
    }

    // Future that immediately increments the counter and resolves.
    fn instant_task(
        counter: &Arc<AtomicUsize>,
    ) -> impl Future<Output = Result<(), Error>> + Send + 'static {
        let c = counter.clone();
        async move {
            c.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    // Future that increments the counter, then waits for a oneshot signal before resolving.
    fn gated_task(
        counter: &Arc<AtomicUsize>,
        gate: oneshot::Receiver<()>,
    ) -> impl Future<Output = Result<(), Error>> + Send + 'static {
        let c = counter.clone();
        async move {
            c.fetch_add(1, Ordering::SeqCst);
            let _ = gate.await;
            Ok(())
        }
    }

    // Future that increments the counter, then waits for a Notify signal before resolving.
    fn notify_task(
        counter: &Arc<AtomicUsize>,
        gate: Arc<Notify>,
    ) -> impl Future<Output = Result<(), Error>> + Send + 'static {
        let c = counter.clone();
        async move {
            c.fetch_add(1, Ordering::SeqCst);
            gate.notified().await;
            Ok(())
        }
    }

    // Polls until the predicate returns true or the 5-second deadline expires.
    fn wait_until(predicate: impl Fn() -> bool) -> bool {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while std::time::Instant::now() < deadline {
            if predicate() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        false
    }

    // Single task submitted, runs exactly once, and is removed from the active set.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn single_submit_runs_to_completion() {
        let queue = make_queue();
        let counter = Arc::new(AtomicUsize::new(0));

        queue.submit("ref-a", instant_task(&counter));

        assert!(wait_until(|| counter.load(Ordering::SeqCst) == 1));
        assert!(wait_until(|| !queue.is_active("ref-a")));
        assert_eq!(counter.load(Ordering::SeqCst), 1);
        assert_eq!(queue.active_task_count(), 0);
    }

    // Submitting the same reference twice while the first task is still in-flight
    // results in one execution, not two.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn duplicate_submit_is_deduplicated() {
        let queue = make_queue();
        let counter = Arc::new(AtomicUsize::new(0));

        let (tx, rx) = oneshot::channel::<()>();
        queue.submit("ref-dup", gated_task(&counter, rx));

        // Wait until the task is registered as active.
        assert!(wait_until(|| queue.is_active("ref-dup")));

        // Second submit with same reference must be a no-op.
        queue.submit("ref-dup", instant_task(&counter));

        // Release the gate so the first task can finish.
        let _ = tx.send(());

        assert!(wait_until(|| !queue.is_active("ref-dup")));

        // Counter incremented only once (only the first task ran).
        assert_eq!(counter.load(Ordering::SeqCst), 1);
        assert_eq!(queue.active_task_count(), 0);
    }

    // Two submissions with different references both execute independently.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn different_references_run_independently() {
        let queue = make_queue();
        let counter_a = Arc::new(AtomicUsize::new(0));
        let counter_b = Arc::new(AtomicUsize::new(0));

        queue.submit("ref-x", instant_task(&counter_a));
        queue.submit("ref-y", instant_task(&counter_b));

        assert!(wait_until(
            || counter_a.load(Ordering::SeqCst) == 1 && counter_b.load(Ordering::SeqCst) == 1
        ));

        assert_eq!(counter_a.load(Ordering::SeqCst), 1);
        assert_eq!(counter_b.load(Ordering::SeqCst), 1);
        assert_eq!(queue.active_task_count(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn max_concurrent_jobs_limits_running_tasks() {
        let queue = TaskQueue::new(1).expect("failed to build TaskQueue");
        let counter = Arc::new(AtomicUsize::new(0));
        let first_gate = Arc::new(Notify::new());
        let second_gate = Arc::new(Notify::new());

        queue.submit("ref-first", notify_task(&counter, first_gate.clone()));
        queue.submit("ref-second", notify_task(&counter, second_gate.clone()));

        assert!(wait_until(|| counter.load(Ordering::SeqCst) == 1));
        assert_eq!(queue.active_task_count(), 2);

        first_gate.notify_one();
        assert!(wait_until(|| counter.load(Ordering::SeqCst) == 2));

        second_gate.notify_one();
        assert!(wait_until(|| queue.active_task_count() == 0));
    }

    // After a task completes and its reference is removed from the active set,
    // re-submitting the same reference spawns a new task (not the stale one).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn completed_task_cleanup_allows_resubmission() {
        let queue = make_queue();
        let counter = Arc::new(AtomicUsize::new(0));

        // First submission.
        queue.submit("ref-reuse", instant_task(&counter));
        assert!(wait_until(|| counter.load(Ordering::SeqCst) == 1));
        assert!(wait_until(|| !queue.is_active("ref-reuse")));

        // Second submission with the same key; must be accepted as a new task.
        queue.submit("ref-reuse", instant_task(&counter));
        assert!(wait_until(|| counter.load(Ordering::SeqCst) == 2));
        assert!(wait_until(|| !queue.is_active("ref-reuse")));

        assert_eq!(counter.load(Ordering::SeqCst), 2);
        assert_eq!(queue.active_task_count(), 0);
    }

    // N concurrent callers submitting the same reference cause the underlying
    // future to run exactly once; all surplus submissions are dropped silently.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_dedup_runs_future_exactly_once() {
        let queue = Arc::new(make_queue());
        let counter = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(Notify::new());

        // Submit from 16 threads simultaneously while the first task is gated.
        let handles: Vec<_> = (0..16)
            .map(|_| {
                let q = queue.clone();
                let c = counter.clone();
                let g = gate.clone();
                std::thread::spawn(move || {
                    q.submit("ref-concurrent", notify_task(&c, g));
                })
            })
            .collect();

        for h in handles {
            h.join().expect("thread panicked");
        }

        // Verify exactly one task is active.
        assert_eq!(queue.active_task_count(), 1);

        // Release the gate.
        gate.notify_one();

        assert!(wait_until(|| !queue.is_active("ref-concurrent")));

        // The future body ran exactly once.
        assert_eq!(counter.load(Ordering::SeqCst), 1);
        assert_eq!(queue.active_task_count(), 0);
    }

    // Dedup, then resubmit, then dedup again in tight sequence confirms the
    // cleanup path does not race with a new submission for the same key.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cleanup_completion_race_no_deadlock() {
        let queue = make_queue();
        let counter = Arc::new(AtomicUsize::new(0));

        for _ in 0..8 {
            let (tx, rx) = oneshot::channel::<()>();

            // Submit first task (gated).
            queue.submit("ref-race", gated_task(&counter, rx));

            // Attempt duplicate while first is in-flight — must be deduplicated.
            queue.submit("ref-race", instant_task(&counter));

            // Release the first task.
            let _ = tx.send(());

            // Wait for cleanup before next iteration.
            assert!(
                wait_until(|| !queue.is_active("ref-race")),
                "queue stalled on iteration"
            );
        }

        // Each iteration the gated task ran once; duplicates were all suppressed.
        assert_eq!(counter.load(Ordering::SeqCst), 8);
        assert_eq!(queue.active_task_count(), 0);
    }

    // Edge-case reference values (empty, whitespace, non-ASCII) are valid dedup keys.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn edge_case_keys_are_valid_dedup_keys() {
        let queue = make_queue();

        for key in &["", "   ", "日本語"] {
            let counter = Arc::new(AtomicUsize::new(0));

            // First submission: must run.
            queue.submit(key, instant_task(&counter));
            assert!(
                wait_until(|| counter.load(Ordering::SeqCst) == 1),
                "key '{key}' did not complete first submission"
            );
            assert!(
                wait_until(|| !queue.is_active(key)),
                "key '{key}' not removed from active set"
            );

            // Second submission after cleanup: must also run (not deduplicated).
            queue.submit(key, instant_task(&counter));
            assert!(
                wait_until(|| counter.load(Ordering::SeqCst) == 2),
                "key '{key}' did not complete second submission"
            );

            assert_eq!(
                counter.load(Ordering::SeqCst),
                2,
                "key '{key}' wrong execution count"
            );
        }
    }
}
