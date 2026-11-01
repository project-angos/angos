use std::net::{IpAddr, Ipv4Addr};
use std::num::NonZeroU64;
use std::path::PathBuf;

use serde::Deserialize;

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct ListenerBaseConfig {
    pub bind_address: IpAddr,
    #[serde(default = "ListenerBaseConfig::default_port")]
    pub port: u16,
    #[serde(default = "ListenerBaseConfig::default_query_timeout")]
    pub query_timeout: NonZeroU64,
    #[serde(default = "ListenerBaseConfig::default_query_timeout_grace_period")]
    pub query_timeout_grace_period: NonZeroU64,
    /// How long a client may take to complete its handshake before the
    /// connection is dropped. A TLS peer that opens a socket and then stalls
    /// otherwise holds a task until it disconnects.
    #[serde(default = "ListenerBaseConfig::default_handshake_timeout")]
    pub handshake_timeout: NonZeroU64,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct TlsListenerConfig {
    #[serde(flatten)]
    pub base: ListenerBaseConfig,
    pub tls: ServerTlsConfig,
}

/// Whether client certificates are accepted or required at the TLS handshake.
///
/// Only meaningful when `client_ca_bundle` is set; ignored otherwise.
/// See `doc/how-to/configure-mtls.md` for the trade-off between modes.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ClientAuth {
    /// Accept both authenticated clients (cert validates against `client_ca_bundle`)
    /// and anonymous clients (no cert presented). Policy can gate on cert identity.
    #[default]
    Optional,
    /// Reject the TLS handshake unless the client presents a cert that validates
    /// against `client_ca_bundle`.
    Required,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(try_from = "ServerTlsConfigFields")]
pub struct ServerTlsConfig {
    pub server_certificate_bundle: PathBuf,
    pub server_private_key: PathBuf,
    pub client_ca_bundle: Option<PathBuf>,
    pub client_auth: ClientAuth,
}

#[derive(Deserialize)]
struct ServerTlsConfigFields {
    server_certificate_bundle: PathBuf,
    server_private_key: PathBuf,
    client_ca_bundle: Option<PathBuf>,
    #[serde(default)]
    client_auth: ClientAuth,
}

impl TryFrom<ServerTlsConfigFields> for ServerTlsConfig {
    type Error = String;

    fn try_from(fields: ServerTlsConfigFields) -> Result<Self, Self::Error> {
        if fields.client_auth == ClientAuth::Required && fields.client_ca_bundle.is_none() {
            return Err(
                "client_auth = \"required\" requires client_ca_bundle to be set".to_string(),
            );
        }
        Ok(Self {
            server_certificate_bundle: fields.server_certificate_bundle,
            server_private_key: fields.server_private_key,
            client_ca_bundle: fields.client_ca_bundle,
            client_auth: fields.client_auth,
        })
    }
}

impl ListenerBaseConfig {
    fn default_bind_address() -> IpAddr {
        IpAddr::from(Ipv4Addr::from([0; 4]))
    }

    fn default_port() -> u16 {
        8000
    }

    const DEFAULT_QUERY_TIMEOUT: NonZeroU64 = NonZeroU64::MIN.saturating_add(3600 - 1);
    const DEFAULT_QUERY_TIMEOUT_GRACE_PERIOD: NonZeroU64 = NonZeroU64::MIN.saturating_add(60 - 1);
    const DEFAULT_HANDSHAKE_TIMEOUT: NonZeroU64 = NonZeroU64::MIN.saturating_add(10 - 1);

    fn default_query_timeout() -> NonZeroU64 {
        Self::DEFAULT_QUERY_TIMEOUT
    }

    fn default_query_timeout_grace_period() -> NonZeroU64 {
        Self::DEFAULT_QUERY_TIMEOUT_GRACE_PERIOD
    }

    fn default_handshake_timeout() -> NonZeroU64 {
        Self::DEFAULT_HANDSHAKE_TIMEOUT
    }
}

impl Default for ListenerBaseConfig {
    fn default() -> Self {
        Self {
            bind_address: Self::default_bind_address(),
            port: Self::default_port(),
            query_timeout: Self::default_query_timeout(),
            query_timeout_grace_period: Self::default_query_timeout_grace_period(),
            handshake_timeout: Self::default_handshake_timeout(),
        }
    }
}
