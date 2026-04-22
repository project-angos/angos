use serde::Deserialize;

use crate::configuration::listeners::{insecure::InsecureListenerConfig, tls::TlsListenerConfig};

#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
pub enum ServerConfig {
    Tls(TlsListenerConfig),
    Insecure(InsecureListenerConfig),
}
