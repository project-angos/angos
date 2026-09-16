use std::num::NonZeroUsize;

use serde::Deserialize;

use angos_secret::Secret;

const DEFAULT_MAX_CONCURRENT_SCANS: NonZeroUsize = NonZeroUsize::new(2).unwrap();

fn default_bind_address() -> String {
    "0.0.0.0".to_string()
}

fn default_port() -> u16 {
    8766
}

fn default_max_concurrent_scans() -> NonZeroUsize {
    DEFAULT_MAX_CONCURRENT_SCANS
}

/// The registry the scanner pulls images from: its URL, and the identity the
/// scanner subprocess authenticates with.
#[derive(Clone, Debug, Deserialize)]
pub struct ScannerRegistry {
    pub url: String,
    pub username: Option<String>,
    pub password: Option<Secret<String>>,
}

/// The `[scanner]` section `angos scanner` reads.
#[derive(Clone, Debug, Deserialize)]
pub struct ScannerConfig {
    #[serde(default = "default_bind_address")]
    pub bind_address: String,
    #[serde(default = "default_port")]
    pub port: u16,
    /// Bearer token a scan request must carry. When unset, requests are
    /// accepted unauthenticated.
    pub token: Option<Secret<String>>,
    #[serde(default = "default_max_concurrent_scans")]
    pub max_concurrent_scans: NonZeroUsize,
    pub registry: ScannerRegistry,
}
