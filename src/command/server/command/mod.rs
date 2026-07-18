use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use argh::FromArgs;
use tokio::time::timeout;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::warn;

use crate::{
    command::{
        bootstrap,
        server::{
            ServerContext,
            error::Error,
            listeners::{
                insecure::InsecureListener,
                tls::{ServerTlsConfig, TlsListener},
            },
        },
    },
    configuration::{Configuration, ServerConfig},
    jobs::Queue,
    jobs::store::queue_depth_refresh_loop,
};

mod notifier;
pub mod setup;

pub enum ServiceListener {
    Insecure(InsecureListener),
    Secure(TlsListener),
}

impl ServiceListener {
    async fn serve(&self) -> Result<(), Error> {
        match self {
            Self::Insecure(listener) => listener.serve().await,
            Self::Secure(listener) => listener.serve().await,
        }
    }

    async fn shutdown(&self) {
        match self {
            Self::Insecure(listener) => listener.shutdown().await,
            Self::Secure(listener) => listener.shutdown().await,
        }
    }
}

#[derive(FromArgs, PartialEq, Debug)]
#[argh(
    subcommand,
    name = "server",
    description = "Run the registry listeners"
)]
pub struct Options {}

/// Background tickers (one per drained queue: `cache` and `replication`) that
/// publish the `angos_job_queue_pending` and `angos_job_queue_failed` gauges
/// on `/metrics`.
struct PendingRefreshTask {
    shutdown: CancellationToken,
    tracker: TaskTracker,
}

pub struct Command {
    listener: ServiceListener,
    cached_conditional_operations: Arc<Mutex<Option<bool>>>,
    /// `None` when `[global.job_queue]` is not configured.
    pending_refresh: Option<PendingRefreshTask>,
    /// Cancellation token tied to the transactional-engine recovery loop.
    /// Fired on shutdown to stop it.
    engine_maintenance: CancellationToken,
}

impl Command {
    pub async fn new(config: &Configuration) -> Result<Command, Error> {
        let cached_conditional_operations = Arc::new(Mutex::new(None));
        let engine_maintenance = CancellationToken::new();
        let auth_cache = bootstrap::auth_cache(&config.cache)?;
        let (registry, pending) = setup::build_registry(
            config,
            &auth_cache,
            &cached_conditional_operations,
            Some(engine_maintenance.clone()),
        )
        .await?;
        let context = ServerContext::new(config, &auth_cache, registry)?;

        let listener = match &config.server {
            ServerConfig::Insecure(server_config) => {
                ServiceListener::Insecure(InsecureListener::new(server_config, context))
            }
            ServerConfig::Tls(server_config) => {
                ServiceListener::Secure(TlsListener::new(server_config, context)?)
            }
        };

        let pending_refresh = pending.map(|refresh| {
            let shutdown = CancellationToken::new();
            let tracker = TaskTracker::new();
            // The server reads queue depth off the shared store, so the
            // replication gauges are published here even though `angos worker`
            // drains that queue.
            for queue in [Queue::Cache, Queue::Replication] {
                tracker.spawn(queue_depth_refresh_loop(
                    refresh.store.clone(),
                    queue,
                    refresh.interval,
                    refresh.ready_horizon_secs,
                    shutdown.clone(),
                ));
            }
            PendingRefreshTask { shutdown, tracker }
        });

        Ok(Command {
            listener,
            cached_conditional_operations,
            pending_refresh,
            engine_maintenance,
        })
    }

    pub async fn notify_config_change(&self, config: &Configuration) -> Result<(), Error> {
        let auth_cache = bootstrap::auth_cache(&config.cache)?;
        let (registry, pending) = setup::build_registry(
            config,
            &auth_cache,
            &self.cached_conditional_operations,
            None,
        )
        .await?;

        if pending.is_some() != self.pending_refresh.is_some() {
            warn!(
                "Enabling or disabling [global.job_queue] at runtime is not supported; \
                 restart angos for the new configuration to take effect."
            );
        }

        let context = ServerContext::new(config, &auth_cache, registry)?;

        match (&self.listener, &config.server) {
            (ServiceListener::Insecure(listener), ServerConfig::Insecure(server_config)) => {
                listener.notify_config_change(server_config, context);
            }
            (ServiceListener::Insecure(listener), ServerConfig::Tls(_)) => {
                warn!(
                    "Listener type transition from insecure to TLS is not supported at runtime; \
                     restart the server to apply the new listener configuration. \
                     Non-listener changes will still be applied."
                );
                listener.store_context(context);
            }
            (ServiceListener::Secure(listener), ServerConfig::Tls(server_config)) => {
                listener.notify_config_change(server_config, context)?;
            }
            (ServiceListener::Secure(_), ServerConfig::Insecure(_)) => {
                warn!(
                    "Listener type transition from TLS to insecure is not supported at runtime; \
                     restart the server to apply the new listener configuration."
                );
            }
        }

        Ok(())
    }

    pub fn notify_tls_config_change(&self, server_config: &ServerTlsConfig) -> Result<(), Error> {
        if let ServiceListener::Secure(listener) = &self.listener {
            listener.notify_tls_config_change(server_config)?;
        }

        Ok(())
    }

    #[cfg(test)]
    pub fn as_insecure(&self) -> Option<&InsecureListener> {
        match &self.listener {
            ServiceListener::Insecure(listener) => Some(listener),
            ServiceListener::Secure(_) => None,
        }
    }

    pub async fn shutdown_with_timeout(&self, grace: Duration) {
        self.listener.shutdown().await;

        if let Some(refresh) = &self.pending_refresh {
            refresh.shutdown.cancel();
            refresh.tracker.close();
            if timeout(grace, refresh.tracker.wait()).await.is_err() {
                warn!("Pending-gauge ticker did not stop within shutdown grace period");
            }
        }

        // Stop the transactional-engine maintenance tasks. They drop on their
        // own once cancelled; we do not need to await them under the grace
        // window because their loops bail out immediately on cancellation.
        self.engine_maintenance.cancel();
    }

    pub async fn run(&self) -> Result<(), Error> {
        self.listener.serve().await
    }
}

#[cfg(test)]
mod tests;
