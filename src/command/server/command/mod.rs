use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use argh::FromArgs;
use tracing::warn;

use super::{
    ServerContext,
    listeners::{
        insecure::InsecureListener,
        tls::{ServerTlsConfig, TlsListener},
    },
};
use crate::{
    command::server::error::Error,
    configuration::{Configuration, ServerConfig},
    registry::metadata_store::ConditionalCapabilities,
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

pub struct Command {
    listener: ServiceListener,
    cached_capabilities: Arc<Mutex<Option<ConditionalCapabilities>>>,
}

impl Command {
    pub async fn new(config: &Configuration) -> Result<Command, Error> {
        let cached_capabilities = Arc::new(Mutex::new(None));
        let registry = setup::build_registry(config, &cached_capabilities).await?;
        let context = ServerContext::new(config, registry)?;

        let listener = match &config.server {
            ServerConfig::Insecure(server_config) => {
                ServiceListener::Insecure(InsecureListener::new(server_config, context))
            }
            ServerConfig::Tls(server_config) => {
                ServiceListener::Secure(TlsListener::new(server_config, context)?)
            }
        };

        Ok(Command {
            listener,
            cached_capabilities,
        })
    }

    pub async fn notify_config_change(&self, config: &Configuration) -> Result<(), Error> {
        let registry = setup::build_registry(config, &self.cached_capabilities).await?;
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
    pub fn insecure_listener(&self) -> &InsecureListener {
        match &self.listener {
            ServiceListener::Insecure(listener) => listener,
            ServiceListener::Secure(_) => panic!("Expected insecure listener"),
        }
    }

    pub async fn shutdown_with_timeout(&self, timeout: Duration) {
        match &self.listener {
            ServiceListener::Insecure(listener) => listener.shutdown_with_timeout(timeout).await,
            ServiceListener::Secure(listener) => listener.shutdown_with_timeout(timeout).await,
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
