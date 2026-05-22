use std::{sync::Arc, time::Duration};

use arc_swap::ArcSwap;
use argh::FromArgs;
use async_trait::async_trait;
use humantime::Duration as HumanDuration;
use tokio::{
    select,
    sync::Semaphore,
    time::{sleep, timeout},
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{debug, error, warn};
use uuid::Uuid;

use crate::{
    command::{
        bootstrap::{self, Error},
        worker::runner::execute_one,
    },
    configuration::{Configuration, listeners::ServerTlsConfig, watcher::ConfigNotifier},
    registry::{
        cache_job_handler::{CACHE_QUEUE, CacheJobHandler},
        job_store::{self, JobHandler, durable::DurableJobConsumer},
    },
};

fn default_queue() -> String {
    CACHE_QUEUE.to_string()
}

/// Process durable background jobs.
#[derive(FromArgs, PartialEq, Debug)]
#[argh(
    subcommand,
    name = "worker",
    description = "Process durable background jobs"
)]
pub struct Options {
    /// queue to drain (default: "cache")
    #[argh(option, default = "default_queue()")]
    pub queue: String,
    /// idle poll interval when the queue is empty
    #[argh(option, default = "HumanDuration::from(Duration::from_secs(1))")]
    pub poll_interval: HumanDuration,
}

/// Hot-reloadable worker subcommand. `consumer` and `handler` are swapped
/// atomically on configuration reload; in-flight jobs always finish on the
/// components they started with.
pub struct Command {
    inner: ArcSwap<Components>,
    queue: String,
    poll_interval: Duration,
    shutdown: CancellationToken,
    in_flight: TaskTracker,
    permits: Arc<Semaphore>,
}

struct Components {
    consumer: Arc<DurableJobConsumer>,
    handler: Arc<dyn JobHandler>,
}

impl Command {
    pub async fn new(options: &Options, config: &Configuration) -> Result<Self, Error> {
        let concurrency = config.global.max_concurrent_cache_jobs.max(1);
        Ok(Self {
            inner: ArcSwap::from_pointee(Components::build(config).await?),
            queue: options.queue.clone(),
            poll_interval: *options.poll_interval,
            shutdown: CancellationToken::new(),
            in_flight: TaskTracker::new(),
            permits: Arc::new(Semaphore::new(concurrency)),
        })
    }

    /// Cancel the poll loop and wait up to `grace` for in-flight jobs to
    /// finish. Surviving tasks are dropped on process exit; the durable
    /// queue's lease TTL guarantees re-claim by another worker.
    pub async fn shutdown_with_timeout(&self, grace: Duration) {
        self.shutdown.cancel();
        self.in_flight.close();
        if timeout(grace, self.in_flight.wait()).await.is_err() {
            warn!("Worker did not drain in-flight jobs within shutdown grace period");
        }
    }

    pub async fn run(&self) {
        loop {
            let permit = select! {
                () = self.shutdown.cancelled() => {
                    debug!("Worker poll loop stopping");
                    return;
                }
                acquired = self.permits.clone().acquire_owned() => match acquired {
                    Ok(p) => p,
                    Err(_) => return,
                }
            };

            let snapshot = self.inner.load_full();
            select! {
                () = self.shutdown.cancelled() => {
                    debug!("Worker poll loop stopping");
                    return;
                }
                result = snapshot.consumer.claim_one(&self.queue) => match result {
                    Err(e) => {
                        error!("claim_one failed: {e}");
                        sleep(self.poll_interval).await;
                    }
                    Ok(outcome) => match outcome.claimed {
                        None => sleep(outcome.idle_sleep(self.poll_interval)).await,
                        Some(claimed) => {
                            let consumer = snapshot.consumer.clone();
                            let handler = snapshot.handler.clone();
                            self.in_flight.spawn(async move {
                                execute_one(consumer.as_ref(), handler.as_ref(), claimed).await;
                                drop(permit);
                            });
                        }
                    }
                }
            }
        }
    }
}

#[async_trait]
impl ConfigNotifier for Command {
    async fn notify_config_change(&self, config: &Configuration) {
        match Components::build(config).await {
            Ok(components) => self.inner.store(Arc::new(components)),
            Err(e) => error!("Failed to apply worker configuration: {e}"),
        }
    }

    fn notify_tls_config_change(&self, _tls: &ServerTlsConfig) {
        // The worker has no TLS listener.
    }
}

impl Components {
    async fn build(config: &Configuration) -> Result<Self, Error> {
        let auth_cache = bootstrap::auth_cache(&config.cache)?;
        let blob_handles = bootstrap::blob_stores(&config.blob_store, &auth_cache)?;
        let (metadata_store, _) =
            bootstrap::metadata_store(&config.resolve_metadata_config(), &auth_cache).await?;
        let repositories = bootstrap::repositories(
            &config.repository,
            &auth_cache,
            config.global.max_manifest_size_bytes(),
        )
        .await?;

        let jq_config = config.global.job_queue.as_ref().ok_or_else(|| {
            bootstrap::Error::JobQueue(job_store::Error::Initialization(
                "[global.job_queue] is required for the worker subcommand".to_string(),
            ))
        })?;

        let consumer = Arc::new(DurableJobConsumer::new(
            jq_config.to_backend()?,
            jq_config.default_lease_ttl_secs,
            Uuid::new_v4().to_string(),
        ));
        let handler: Arc<dyn JobHandler> = Arc::new(CacheJobHandler::new(
            repositories,
            blob_handles.upload_store,
            metadata_store,
        ));

        Ok(Self { consumer, handler })
    }
}
