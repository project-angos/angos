use std::{collections::HashSet, fmt, future::Future, sync::Arc, thread};

use parking_lot::Mutex;
use tokio::runtime::{self, Handle};
use tracing::info;

#[derive(Debug)]
pub enum Error {
    RuntimeBuild(String),
    TaskExecution(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::RuntimeBuild(e) => write!(f, "failed to build Tokio runtime: {e}"),
            Error::TaskExecution(e) => write!(f, "task execution failed: {e}"),
        }
    }
}

impl std::error::Error for Error {}

pub struct TaskQueue {
    handle: Handle,
    active_tasks: Arc<Mutex<HashSet<String>>>,
    _runtime_thread: thread::JoinHandle<()>,
}

impl TaskQueue {
    pub fn new(worker_threads: usize, thread_name: &str) -> Result<Self, Error> {
        let runtime = runtime::Builder::new_multi_thread()
            .worker_threads(worker_threads)
            .thread_name(thread_name)
            .enable_all()
            .build()
            .map_err(|e| Error::RuntimeBuild(e.to_string()))?;

        let handle = runtime.handle().clone();

        let runtime_thread = thread::spawn(move || {
            runtime.block_on(std::future::pending::<()>());
        });

        Ok(Self {
            handle,
            active_tasks: Arc::new(Mutex::new(HashSet::new())),
            _runtime_thread: runtime_thread,
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
        self.handle.spawn(async move {
            let _ = fut.await;
            active_tasks.lock().remove(&reference);
        });
    }
}
