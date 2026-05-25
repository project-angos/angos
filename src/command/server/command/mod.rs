use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use argh::FromArgs;
use tokio::time::timeout;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::warn;

use crate::{
    command::server::{
        ServerContext,
        error::Error,
        listeners::{
            insecure::InsecureListener,
            tls::{ServerTlsConfig, TlsListener},
        },
    },
    configuration::{Configuration, ServerConfig},
    registry::{
        cache_job_handler::CACHE_QUEUE, job_store::durable::pending_refresh_loop,
        metadata_store::ConditionalCapabilities,
    },
};

mod notifier;
pub mod setup;

pub enum ServiceListener {
    Insecure(InsecureListener),
    Secure(TlsListener),
}

#[derive(FromArgs, PartialEq, Debug)]
#[argh(
    subcommand,
    name = "server",
    description = "Run the registry listeners"
)]
pub struct Options {}

/// Background ticker that publishes `angos_job_queue_pending` on `/metrics`.
struct PendingRefreshTask {
    shutdown: CancellationToken,
    tracker: TaskTracker,
}

pub struct Command {
    listener: ServiceListener,
    cached_capabilities: Arc<Mutex<Option<ConditionalCapabilities>>>,
    /// `None` when `[global.job_queue]` is not configured.
    pending_refresh: Option<PendingRefreshTask>,
}

impl Command {
    pub async fn new(config: &Configuration) -> Result<Command, Error> {
        let cached_capabilities = Arc::new(Mutex::new(None));
        let (registry, pending) = setup::build_registry(config, &cached_capabilities).await?;
        let context = ServerContext::new(config, registry)?;

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
            tracker.spawn(pending_refresh_loop(
                refresh.store,
                CACHE_QUEUE.to_string(),
                refresh.interval,
                refresh.ready_horizon_secs,
                shutdown.clone(),
            ));
            PendingRefreshTask { shutdown, tracker }
        });

        Ok(Command {
            listener,
            cached_capabilities,
            pending_refresh,
        })
    }

    pub async fn notify_config_change(&self, config: &Configuration) -> Result<(), Error> {
        let (registry, pending) = setup::build_registry(config, &self.cached_capabilities).await?;

        if pending.is_some() != self.pending_refresh.is_some() {
            warn!(
                "Enabling or disabling [global.job_queue] at runtime is not supported; \
                 restart angos for the new configuration to take effect."
            );
        }

        let context = ServerContext::new(config, registry)?;

        match (&self.listener, &config.server) {
            (ServiceListener::Insecure(listener), ServerConfig::Insecure(_)) => {
                listener.notify_config_change(context);
            }
            (ServiceListener::Insecure(listener), ServerConfig::Tls(_)) => {
                warn!(
                    "Listener type transition from insecure to TLS is not supported at runtime; \
                     restart the server to apply the new listener configuration. \
                     Non-listener changes will still be applied."
                );
                listener.notify_config_change(context);
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
        match &self.listener {
            ServiceListener::Insecure(listener) => listener.shutdown_with_timeout(grace).await,
            ServiceListener::Secure(listener) => listener.shutdown_with_timeout(grace).await,
        }

        if let Some(refresh) = &self.pending_refresh {
            refresh.shutdown.cancel();
            refresh.tracker.close();
            if timeout(grace, refresh.tracker.wait()).await.is_err() {
                warn!("Pending-gauge ticker did not stop within shutdown grace period");
            }
        }
    }

    pub async fn run(&self) -> Result<(), Error> {
        match &self.listener {
            ServiceListener::Insecure(listener) => listener.serve().await?,
            ServiceListener::Secure(listener) => listener.serve().await?,
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests;
