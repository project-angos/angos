use std::{net::IpAddr, path::PathBuf};

use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
pub struct TlsListenerConfig {
    pub bind_address: IpAddr,
    #[serde(default = "TlsListenerConfig::default_port")]
    pub port: u16,
    #[serde(default = "TlsListenerConfig::default_query_timeout")]
    pub query_timeout: u64,
    #[serde(default = "TlsListenerConfig::default_query_timeout_grace_period")]
    pub query_timeout_grace_period: u64,
    pub tls: ServerTlsConfig,
}

impl TlsListenerConfig {
    fn default_port() -> u16 {
        8000
    }

    fn default_query_timeout() -> u64 {
        3600
    }

    fn default_query_timeout_grace_period() -> u64 {
        60
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct ServerTlsConfig {
    pub server_certificate_bundle: PathBuf,
    pub server_private_key: PathBuf,
    pub client_ca_bundle: Option<PathBuf>,
}
