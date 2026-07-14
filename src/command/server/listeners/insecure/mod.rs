use std::net::SocketAddr;

use async_trait::async_trait;
use tokio::net::TcpStream;

use crate::command::server::{
    ServerContext,
    listeners::{Connector, HandshakeResult, Listener},
};
pub use crate::configuration::listeners::InsecureListenerConfig;

/// A non-TLS listener: the shared shell over the pass-through connector.
pub type InsecureListener = Listener<InsecureConnector>;

pub struct InsecureConnector;

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
    pub fn new(config: &InsecureListenerConfig, context: ServerContext) -> Self {
        Self::build(&config.base, InsecureConnector, context)
    }

    /// Apply a non-listener config reload: the insecure listener has no
    /// scheme-specific state, so only the server context is swapped.
    pub fn notify_config_change(&self, context: ServerContext) {
        self.store_context(context);
    }
}

#[cfg(test)]
mod tests;
