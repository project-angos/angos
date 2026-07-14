use std::{fmt::Debug, net::SocketAddr, sync::Arc, time::Duration};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use hyper_util::rt::TokioIo;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{TcpListener, TcpStream},
};
use tracing::{debug, info};

use crate::{
    command::server::{ServerContext, error::Error, serve_request},
    configuration::listeners::ListenerBaseConfig,
};

pub mod insecure;
pub mod tls;

pub struct HandshakeResult<S> {
    pub stream: S,
    pub peer_certificate: Option<Vec<u8>>,
}

/// The two per-connection deadlines: the wall-clock request-processing timeout
/// and the grace period allowed for the in-flight request to drain after it
/// fires.
#[derive(Clone, Copy, Debug)]
pub struct RequestTimeouts {
    pub query: Duration,
    pub grace: Duration,
}

impl RequestTimeouts {
    pub fn from_config(base: &ListenerBaseConfig) -> Self {
        Self {
            query: Duration::from_secs(base.query_timeout.get()),
            grace: Duration::from_secs(base.query_timeout_grace_period.get()),
        }
    }
}

#[async_trait]
pub trait Connector: Send + Sync {
    type Stream: Unpin + AsyncWrite + AsyncRead + Send + Debug + 'static;

    async fn handshake(
        &self,
        tcp: TcpStream,
        remote_address: SocketAddr,
    ) -> Option<HandshakeResult<Self::Stream>>;

    fn label(&self) -> &'static str;
}

/// The shared listener shell: a bound address plus a hot-swappable context and
/// timeouts, driving connections through a [`Connector`] that supplies the
/// per-scheme handshake. TLS and non-TLS listeners are the same shell over a
/// different connector, so the shape lives here once.
pub struct Listener<C: Connector> {
    binding_address: SocketAddr,
    connector: C,
    context: ArcSwap<ServerContext>,
    timeouts: ArcSwap<RequestTimeouts>,
}

impl<C: Connector> Listener<C> {
    /// Assemble the shell around `connector`, deriving the bind address and
    /// timeouts from `base`.
    pub fn build(base: &ListenerBaseConfig, connector: C, context: ServerContext) -> Self {
        Self {
            binding_address: SocketAddr::new(base.bind_address, base.port),
            connector,
            context: ArcSwap::from_pointee(context),
            timeouts: ArcSwap::from_pointee(RequestTimeouts::from_config(base)),
        }
    }

    /// The connector, so scheme-specific reloads (the TLS acceptor) reach it.
    pub fn connector(&self) -> &C {
        &self.connector
    }

    /// Swap in a freshly-built server context on a non-listener config reload.
    pub fn store_context(&self, context: ServerContext) {
        self.context.store(Arc::new(context));
    }

    /// Swap in the per-connection timeouts from an updated base config.
    pub fn store_timeouts(&self, base: &ListenerBaseConfig) {
        self.timeouts
            .store(Arc::new(RequestTimeouts::from_config(base)));
    }

    pub async fn shutdown(&self) {
        self.context.load().shutdown().await;
    }

    pub async fn serve(&self) -> Result<(), Error> {
        accept_loop(
            self.binding_address,
            &self.connector,
            &self.context,
            &self.timeouts,
        )
        .await
    }

    #[cfg(test)]
    pub fn current_context(&self) -> arc_swap::Guard<Arc<ServerContext>> {
        self.context.load()
    }
}

pub async fn accept_loop<C: Connector>(
    binding_address: SocketAddr,
    connector: &C,
    context: &ArcSwap<ServerContext>,
    timeouts: &ArcSwap<RequestTimeouts>,
) -> Result<(), Error> {
    info!("Listening on {} ({})", binding_address, connector.label());
    let listener = build_listener(binding_address).await?;

    loop {
        debug!("Waiting for incoming connection");
        let (tcp, remote_address) = accept(&listener).await?;

        let Some(handshake) = connector.handshake(tcp, remote_address).await else {
            continue;
        };

        debug!("Accepted connection from {remote_address}");
        let stream = TokioIo::new(handshake.stream);
        let context = Arc::clone(&context.load());
        let timeouts = Arc::clone(&timeouts.load());

        tokio::spawn(serve_request(
            stream,
            context,
            handshake.peer_certificate,
            timeouts,
            remote_address,
        ));
    }
}

async fn build_listener(binding_address: SocketAddr) -> Result<TcpListener, Error> {
    match TcpListener::bind(binding_address).await {
        Ok(listener) => Ok(listener),
        Err(err) => {
            let msg = format!("Failed to bind to {binding_address}: {err}");
            Err(Error::Initialization(msg))
        }
    }
}

async fn accept(listener: &TcpListener) -> Result<(TcpStream, SocketAddr), Error> {
    match listener.accept().await {
        Ok((stream, remote_address)) => {
            debug!("Accepted connection from {remote_address}");
            Ok((stream, remote_address))
        }
        Err(err) => {
            let msg = format!("Failed to accept incoming connection: {err}");
            Err(Error::Execution(msg))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_build_listener_invalid_port_in_use() {
        let addr = "127.0.0.1:0".parse().unwrap();
        let listener1 = build_listener(addr).await.unwrap();
        let actual_addr = listener1.local_addr().unwrap();

        let result = build_listener(actual_addr).await;

        assert!(result.is_err());
        match result.unwrap_err() {
            Error::Initialization(msg) => {
                assert!(msg.contains("Failed to bind to"));
            }
            _ => panic!("Expected Initialization error"),
        }
    }

    #[tokio::test]
    async fn test_accept_with_connection() {
        use tokio::io::AsyncWriteExt;

        let addr = "127.0.0.1:0".parse().unwrap();
        let listener = build_listener(addr).await.unwrap();
        let local_addr = listener.local_addr().unwrap();

        let connect_handle = tokio::spawn(async move {
            let mut stream = TcpStream::connect(local_addr).await.unwrap();
            stream.write_all(b"test").await.unwrap();
        });

        let result = accept(&listener).await;

        assert!(result.is_ok());
        let (_, remote_addr) = result.unwrap();
        assert!(remote_addr.port() > 0);

        connect_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_build_listener_error_message_format() {
        let addr: SocketAddr = "240.0.0.1:8080".parse().unwrap();
        let result = build_listener(addr).await;

        assert!(result.is_err());
        if let Err(Error::Initialization(msg)) = result {
            assert!(msg.starts_with("Failed to bind to"));
            assert!(msg.contains("240.0.0.1:8080"));
        } else {
            panic!("Expected Initialization error with formatted message");
        }
    }
}
