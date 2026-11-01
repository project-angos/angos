use std::net::SocketAddr;

use async_trait::async_trait;
use tokio::net::TcpStream;

use crate::command::server::ServerContext;
use crate::command::server::listeners::{Connector, HandshakeResult, Listener};
use crate::configuration::listeners::ListenerBaseConfig;
use crate::identity::RequestScheme;

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

    fn scheme(&self) -> RequestScheme {
        RequestScheme::Http
    }
}

impl InsecureListener {
    pub fn new(config: &ListenerBaseConfig, context: ServerContext) -> Self {
        Self::build(config, InsecureConnector, context)
    }

    /// Apply a config reload: refresh the shared-shell timeouts and swap the
    /// server context; the insecure listener has no scheme-specific state.
    pub fn notify_config_change(&self, config: &ListenerBaseConfig, context: ServerContext) {
        self.store_timeouts(config);
        self.store_context(context);
    }
}

#[cfg(test)]
mod tests;
