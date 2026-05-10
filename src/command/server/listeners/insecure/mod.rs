use std::{net::SocketAddr, sync::Arc, time::Duration};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use tokio::net::TcpStream;

use crate::command::server::{
    ServerContext,
    error::Error,
    listeners::{Connector, HandshakeResult, accept_loop},
};
pub use crate::configuration::listeners::insecure::InsecureListenerConfig;

pub struct InsecureListener {
    binding_address: SocketAddr,
    context: ArcSwap<ServerContext>,
    timeouts: ArcSwap<[Duration; 2]>,
}

struct InsecureConnector;

#[async_trait]
impl Connector for InsecureConnector {
    type Stream = TcpStream;

    async fn handshake(
        &self,
        tcp: TcpStream,
        _remote_address: SocketAddr,
    ) -> Option<HandshakeResult<TcpStream>> {
        Some(HandshakeResult {
            stream: tcp,
            peer_certificate: None,
        })
    }

    fn label(&self) -> &'static str {
        "non-TLS"
    }
}

impl InsecureListener {
    pub fn new(server_config: &InsecureListenerConfig, context: ServerContext) -> Self {
        let binding_address = SocketAddr::new(server_config.bind_address, server_config.port);

        let timeouts = [
            Duration::from_secs(server_config.query_timeout),
            Duration::from_secs(server_config.query_timeout_grace_period),
        ];

        Self {
            binding_address,
            context: ArcSwap::from_pointee(context),
            timeouts: ArcSwap::from_pointee(timeouts),
        }
    }

    pub fn notify_config_change(&self, context: ServerContext) {
        self.context.store(Arc::new(context));
    }

    pub async fn shutdown_with_timeout(&self, timeout: Duration) {
        self.context.load().shutdown_with_timeout(timeout).await;
    }

    #[cfg(test)]
    pub fn current_context(&self) -> arc_swap::Guard<Arc<ServerContext>> {
        self.context.load()
    }

    pub async fn serve(&self) -> Result<(), Error> {
        accept_loop(
            self.binding_address,
            &InsecureConnector,
            &self.context,
            &self.timeouts,
        )
        .await
    }
}

#[cfg(test)]
mod tests;
