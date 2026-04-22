use std::net::{IpAddr, Ipv4Addr};

use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
pub struct InsecureListenerConfig {
    pub bind_address: IpAddr,
    #[serde(default = "InsecureListenerConfig::default_port")]
    pub port: u16,
    #[serde(default = "InsecureListenerConfig::default_query_timeout")]
    pub query_timeout: u64,
    #[serde(default = "InsecureListenerConfig::default_query_timeout_grace_period")]
    pub query_timeout_grace_period: u64,
}

impl InsecureListenerConfig {
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

impl Default for InsecureListenerConfig {
    fn default() -> Self {
        Self {
            bind_address: IpAddr::from(Ipv4Addr::from([0; 4])),
            port: Self::default_port(),
            query_timeout: Self::default_query_timeout(),
            query_timeout_grace_period: Self::default_query_timeout_grace_period(),
        }
    }
}
