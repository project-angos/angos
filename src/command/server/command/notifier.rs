use async_trait::async_trait;
use tracing::error;

use super::Command;
use crate::configuration::{Configuration, listeners::ServerTlsConfig, watcher::ConfigNotifier};

#[async_trait]
impl ConfigNotifier for Command {
    async fn notify_config_change(&self, config: &Configuration) {
        if let Err(e) = self.notify_config_change(config).await {
            error!("Failed to apply configuration: {e}");
        }
    }

    fn notify_tls_config_change(&self, tls: &ServerTlsConfig) {
        if let Err(e) = self.notify_tls_config_change(tls) {
            error!("Failed to reload TLS configuration: {e}");
        }
    }
}
