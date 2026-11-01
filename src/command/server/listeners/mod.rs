use std::fmt::Debug;
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use angos_backoff::Backoff;
use arc_swap::ArcSwap;
#[cfg(test)]
use arc_swap::Guard;
use async_trait::async_trait;
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::select;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, info, warn};

use crate::command::server::error::Error;
use crate::command::server::{ServerContext, serve_request};
use crate::configuration::listeners::ListenerBaseConfig;
use crate::identity::RequestScheme;

pub mod insecure;
pub mod tls;

pub struct HandshakeResult<S> {
    pub stream: S,
    pub peer_certificate: Option<Vec<u8>>,
}

/// The per-connection deadlines: how long the handshake may take, the
/// wall-clock request-processing timeout, and the grace period allowed for the
/// in-flight request to drain after it fires.
#[derive(Clone, Copy, Debug)]
pub struct RequestTimeouts {
    pub handshake: Duration,
    pub query: Duration,
    pub grace: Duration,
}

impl RequestTimeouts {
    pub fn from_config(base: &ListenerBaseConfig) -> Self {
        Self {
            handshake: Duration::from_secs(base.handshake_timeout.get()),
            query: Duration::from_secs(base.query_timeout.get()),
            grace: Duration::from_secs(base.query_timeout_grace_period.get()),
        }
    }
}

/// The connection tasks a listener has spawned, and the token asking them to
/// finish. Spawns are tracked rather than detached so shutdown can wait for
/// the exchanges in flight instead of cutting them with the runtime.
#[derive(Clone, Default)]
pub struct Connections {
    shutdown: CancellationToken,
    tasks: TaskTracker,
}

impl Connections {
    /// Stop accepting, ask every connection to finish its current exchange,
    /// and wait up to `grace` for them.
    pub async fn drain(&self, grace: Duration) {
        self.shutdown.cancel();
        self.tasks.close();
        if timeout(grace, self.tasks.wait()).await.is_err() {
            warn!("Connections did not finish within the shutdown grace period");
        }
    }
}

/// Paced by consecutive accept failures so a listener that cannot hand out
/// descriptors does not spin the loop at full speed until it recovers.
const ACCEPT_BACKOFF: Backoff =
    Backoff::exponential(Duration::from_millis(50), Duration::from_secs(1));

#[async_trait]
pub trait Connector: Send + Sync {
    type Stream: Unpin + AsyncWrite + AsyncRead + Send + Debug + 'static;

    async fn handshake(
        &self,
        tcp: TcpStream,
        remote_address: SocketAddr,
    ) -> Option<HandshakeResult<Self::Stream>>;

    fn label(&self) -> &'static str;

    fn scheme(&self) -> RequestScheme;
}

/// The shared listener shell: a bound address plus a hot-swappable context and
/// timeouts, driving connections through a [`Connector`] that supplies the
/// per-scheme handshake. TLS and non-TLS listeners are the same shell over a
/// different connector, so the shape lives here once.
pub struct Listener<C: Connector> {
    binding_address: SocketAddr,
    connector: Arc<C>,
    context: Arc<ArcSwap<ServerContext>>,
    timeouts: ArcSwap<RequestTimeouts>,
    connections: Connections,
}

impl<C: Connector + 'static> Listener<C> {
    /// Assemble the shell around `connector`, deriving the bind address and
    /// timeouts from `base`.
    pub fn build(base: &ListenerBaseConfig, connector: C, context: ServerContext) -> Self {
        Self {
            binding_address: SocketAddr::new(base.bind_address, base.port),
            connector: Arc::new(connector),
            context: Arc::new(ArcSwap::from_pointee(context)),
            timeouts: ArcSwap::from_pointee(RequestTimeouts::from_config(base)),
            connections: Connections::default(),
        }
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

    /// Stop serving: no new connections, every in-flight exchange gets up to
    /// `grace` to finish, then the webhook deliveries drain as before.
    pub async fn shutdown(&self, grace: Duration) {
        self.connections.drain(grace).await;
        self.context.load().registry.shutdown().await;
    }

    pub async fn serve(&self) -> Result<(), Error> {
        let listener = build_listener(self.binding_address).await?;
        accept_loop(
            listener,
            &self.connector,
            &self.context,
            &self.timeouts,
            &self.connections,
        )
        .await
    }

    #[cfg(test)]
    pub fn current_context(&self) -> Guard<Arc<ServerContext>> {
        self.context.load()
    }

    #[cfg(test)]
    pub fn current_timeouts(&self) -> Guard<Arc<RequestTimeouts>> {
        self.timeouts.load()
    }
}

pub async fn accept_loop<C: Connector + 'static>(
    listener: TcpListener,
    connector: &Arc<C>,
    context: &Arc<ArcSwap<ServerContext>>,
    timeouts: &ArcSwap<RequestTimeouts>,
    connections: &Connections,
) -> Result<(), Error> {
    let binding_address = match listener.local_addr() {
        Ok(address) => address,
        Err(error) => {
            return Err(Error::Initialization(format!(
                "Failed to read the address of a bound listener: {error}"
            )));
        }
    };
    info!("Listening on {} ({})", binding_address, connector.label());
    let mut consecutive_failures = 0;

    loop {
        debug!("Waiting for incoming connection");
        let accepted = select! {
            () = connections.shutdown.cancelled() => {
                info!("Shutting down, no longer accepting on {binding_address}");
                return Ok(());
            }
            accepted = listener.accept() => accepted,
        };
        let (tcp, remote_address) = match accepted {
            Ok((tcp, remote_address)) => {
                consecutive_failures = 0;
                // A dual-stack bind reports an IPv4 peer as `::ffff:a.b.c.d`,
                // which matches no IPv4 `trusted_proxies` entry and reads wrong
                // in the audit log; canonicalise once so every consumer sees
                // the dotted form.
                let ip = remote_address.ip().to_canonical();
                (tcp, SocketAddr::new(ip, remote_address.port()))
            }
            // A client gone between the SYN and the accept costs only its own
            // connection, so the next one is taken immediately.
            Err(error) if error.kind() == ErrorKind::ConnectionAborted => continue,
            // Everything else is transient too often to kill the server for:
            // an exhausted descriptor table clears once connections close.
            Err(error) => {
                warn!("Failed to accept a connection on {binding_address}: {error}");
                sleep(ACCEPT_BACKOFF.delay(consecutive_failures)).await;
                consecutive_failures = consecutive_failures.saturating_add(1);
                continue;
            }
        };

        let connector = Arc::clone(connector);
        // The swap handle, not a snapshot: a config reload swaps the context
        // and every later request on this open keep-alive loads the new one.
        let context = Arc::clone(context);
        let timeouts = Arc::clone(&timeouts.load());
        let shutdown = connections.shutdown.clone();

        // The handshake runs on its own task: awaiting it here would let one
        // stalled client hold up every other connection on this listener. The
        // task is tracked, so shutdown waits for its exchange.
        connections.tasks.spawn(async move {
            let handshake =
                match timeout(timeouts.handshake, connector.handshake(tcp, remote_address)).await {
                    Ok(Some(handshake)) => handshake,
                    Ok(None) => return,
                    Err(_) => {
                        debug!("Handshake from {remote_address} timed out");
                        return;
                    }
                };

            debug!("Accepted connection from {remote_address}");
            serve_request(
                TokioIo::new(handshake.stream),
                context,
                handshake.peer_certificate,
                timeouts,
                remote_address,
                connector.scheme(),
                shutdown,
            )
            .await;
        });
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

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Instant;

    use super::*;
    use crate::command::server::server_context::tests::create_test_server_context;

    #[tokio::test]
    async fn test_build_listener_invalid_port_in_use() {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let listener1 = build_listener(addr).await.unwrap();
        let actual_addr = listener1.local_addr().unwrap();

        let result = build_listener(actual_addr).await;

        assert!(matches!(result, Err(Error::Initialization(_))));
    }

    /// A connector whose handshake never completes, counting the connections
    /// handed to it. Modelling the client that opens a socket and then stalls.
    struct StalledConnector {
        started: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Connector for StalledConnector {
        type Stream = TcpStream;

        async fn handshake(
            &self,
            _tcp: TcpStream,
            _remote_address: SocketAddr,
        ) -> Option<HandshakeResult<TcpStream>> {
            self.started.fetch_add(1, Ordering::SeqCst);
            pending::<()>().await;
            None
        }

        fn label(&self) -> &'static str {
            "stalled"
        }

        fn scheme(&self) -> RequestScheme {
            RequestScheme::Http
        }
    }

    /// Bind a loopback listener and report where it landed. Binding before the
    /// loop is spawned means the backlog holds the connections the test opens,
    /// so it never races the task's start.
    async fn bound_listener() -> (TcpListener, SocketAddr) {
        let listener = build_listener("127.0.0.1:0".parse().unwrap())
            .await
            .expect("a loopback bind must succeed");
        let address = listener
            .local_addr()
            .expect("a bound listener has an address");
        (listener, address)
    }

    /// The regression: connection tasks were detached spawns, so a shutdown
    /// returned at once and the runtime cut every exchange in flight. Shutdown
    /// must wait for them instead, up to its grace.
    #[tokio::test]
    async fn shutdown_waits_for_the_connections_it_spawned() {
        let (listener, address) = bound_listener().await;
        let started = Arc::new(AtomicUsize::new(0));
        let connector = Arc::new(StalledConnector {
            started: Arc::clone(&started),
        });
        let context = Arc::new(ArcSwap::from_pointee(create_test_server_context().await));
        let timeouts =
            ArcSwap::from_pointee(RequestTimeouts::from_config(&ListenerBaseConfig::default()));
        let connections = Connections::default();

        let served = connections.clone();
        let loop_handle = tokio::spawn(async move {
            accept_loop(listener, &connector, &context, &timeouts, &served).await
        });

        let _client = TcpStream::connect(address).await.expect("connect");
        for _ in 0..100 {
            if started.load(Ordering::SeqCst) >= 1 {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            started.load(Ordering::SeqCst),
            1,
            "the connection must start"
        );

        // The stalled handshake never returns, so the whole grace is spent: an
        // untracked spawn would leave nothing to wait for and return at once.
        let grace = Duration::from_millis(300);
        let waited = Instant::now();
        connections.drain(grace).await;

        assert!(
            waited.elapsed() >= grace,
            "shutdown must wait out the grace for a connection still exchanging, waited {:?}",
            waited.elapsed()
        );
        assert!(
            loop_handle
                .await
                .expect("the accept loop must not panic")
                .is_ok(),
            "a cancelled accept loop stops accepting rather than failing"
        );
    }

    /// The regression: awaiting the handshake in the accept loop let one
    /// stalled client hold up every connection behind it.
    #[tokio::test]
    async fn a_stalled_handshake_does_not_block_the_next_connection() {
        let (listener, address) = bound_listener().await;
        let started = Arc::new(AtomicUsize::new(0));
        let connector = Arc::new(StalledConnector {
            started: Arc::clone(&started),
        });
        let context = Arc::new(ArcSwap::from_pointee(create_test_server_context().await));
        let timeouts =
            ArcSwap::from_pointee(RequestTimeouts::from_config(&ListenerBaseConfig::default()));

        let loop_handle = tokio::spawn(async move {
            accept_loop(
                listener,
                &connector,
                &context,
                &timeouts,
                &Connections::default(),
            )
            .await
        });

        // Both clients connect; neither handshake will ever finish.
        let _first = TcpStream::connect(address).await.expect("first connect");
        let _second = TcpStream::connect(address).await.expect("second connect");

        for _ in 0..100 {
            if started.load(Ordering::SeqCst) >= 2 {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }

        assert_eq!(
            started.load(Ordering::SeqCst),
            2,
            "the second connection must reach the handshake while the first is stalled"
        );
        loop_handle.abort();
    }

    /// A handshake that outlives its deadline is dropped, and the loop keeps
    /// serving: the stalled task must not accumulate.
    #[tokio::test]
    async fn a_handshake_past_its_deadline_is_dropped() {
        let (listener, address) = bound_listener().await;
        let started = Arc::new(AtomicUsize::new(0));
        let connector = Arc::new(StalledConnector {
            started: Arc::clone(&started),
        });
        let context = Arc::new(ArcSwap::from_pointee(create_test_server_context().await));
        let timeouts = ArcSwap::from_pointee(RequestTimeouts {
            handshake: Duration::from_millis(20),
            query: Duration::from_secs(1),
            grace: Duration::from_secs(1),
        });

        let loop_handle = tokio::spawn(async move {
            accept_loop(
                listener,
                &connector,
                &context,
                &timeouts,
                &Connections::default(),
            )
            .await
        });

        let client = TcpStream::connect(address).await.expect("connect");
        for _ in 0..100 {
            if started.load(Ordering::SeqCst) == 1 {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }

        // The deadline fires and the connector's task is dropped, closing the
        // socket the client still holds.
        let mut buffer = [0_u8; 1];
        let read = timeout(Duration::from_secs(5), client.readable())
            .await
            .expect("the timed-out connection must be closed");
        assert!(read.is_ok());
        assert_eq!(
            client
                .try_read(&mut buffer)
                .expect("a closed peer reads EOF"),
            0
        );
        loop_handle.abort();
    }
}
