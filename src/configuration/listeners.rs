use std::{
    net::{IpAddr, Ipv4Addr},
    num::NonZeroU64,
    path::PathBuf,
};

use serde::{Deserialize, Deserializer, de::Error};

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct ListenerBaseConfig {
    pub bind_address: IpAddr,
    #[serde(default = "ListenerBaseConfig::default_port")]
    pub port: u16,
    #[serde(
        default = "ListenerBaseConfig::default_query_timeout_secs",
        alias = "query_timeout",
        deserialize_with = "deserialize_query_timeout_secs"
    )]
    pub query_timeout_secs: NonZeroU64,
    #[serde(
        default = "ListenerBaseConfig::default_query_timeout_grace_period_secs",
        alias = "query_timeout_grace_period",
        deserialize_with = "deserialize_query_timeout_grace_period_secs"
    )]
    pub query_timeout_grace_period_secs: NonZeroU64,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct InsecureListenerConfig {
    #[serde(flatten)]
    pub base: ListenerBaseConfig,
}

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

impl ListenerBaseConfig {
    fn default_bind_address() -> IpAddr {
        IpAddr::from(Ipv4Addr::from([0; 4]))
    }

    fn default_port() -> u16 {
        8000
    }

    fn default_query_timeout_secs() -> NonZeroU64 {
        NonZeroU64::new(3600).expect("default query timeout must be non-zero")
    }

    fn default_query_timeout_grace_period_secs() -> NonZeroU64 {
        NonZeroU64::new(60).expect("default query timeout grace period must be non-zero")
    }
}

impl Default for ListenerBaseConfig {
    fn default() -> Self {
        Self {
            bind_address: Self::default_bind_address(),
            port: Self::default_port(),
            query_timeout_secs: Self::default_query_timeout_secs(),
            query_timeout_grace_period_secs: Self::default_query_timeout_grace_period_secs(),
        }
    }
}

fn deserialize_query_timeout_secs<'de, D>(deserializer: D) -> Result<NonZeroU64, D::Error>
where
    D: Deserializer<'de>,
{
    let value = u64::deserialize(deserializer)?;
    NonZeroU64::new(value).ok_or_else(|| D::Error::custom("query_timeout_secs must be > 0"))
}

fn deserialize_query_timeout_grace_period_secs<'de, D>(
    deserializer: D,
) -> Result<NonZeroU64, D::Error>
where
    D: Deserializer<'de>,
{
    let value = u64::deserialize(deserializer)?;
    NonZeroU64::new(value)
        .ok_or_else(|| D::Error::custom("query_timeout_grace_period_secs must be > 0"))
}
