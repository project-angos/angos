use async_trait::async_trait;
use tracing::error;

use crate::command::server::command::Command;
use crate::configuration::Configuration;
use crate::configuration::listeners::ServerTlsConfig;
use crate::configuration::watcher::ConfigNotifier;

#[async_trait]
impl ConfigNotifier for Command {
    async fn notify_config_change(&self, config: &Configuration) -> bool {
        match self.notify_config_change(config).await {
            Ok(()) => true,
            Err(e) => {
                error!("Failed to apply configuration: {e}");
                false
            }
        }
    }

    fn notify_tls_config_change(&self, tls: &ServerTlsConfig) {
        if let Err(e) = self.notify_tls_config_change(tls) {
            error!("Failed to reload TLS configuration: {e}");
        }
    }
}
