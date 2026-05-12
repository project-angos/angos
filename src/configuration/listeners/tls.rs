use std::path::PathBuf;

use serde::Deserialize;

use crate::configuration::listeners::ListenerBaseConfig;

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct TlsListenerConfig {
    #[serde(flatten)]
    pub base: ListenerBaseConfig,
    pub tls: ServerTlsConfig,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct ServerTlsConfig {
    pub server_certificate_bundle: PathBuf,
    pub server_private_key: PathBuf,
    pub client_ca_bundle: Option<PathBuf>,
}
