use std::num::NonZeroUsize;

use serde::Deserialize;

use angos_mtls_client::BasicAuth;
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
#[serde(try_from = "ScannerRegistryFields")]
pub struct ScannerRegistry {
    pub url: String,
    pub basic_auth: Option<BasicAuth>,
}

#[derive(Deserialize)]
struct ScannerRegistryFields {
    url: String,
    username: Option<String>,
    password: Option<Secret<String>>,
}

impl TryFrom<ScannerRegistryFields> for ScannerRegistry {
    type Error = String;

    fn try_from(fields: ScannerRegistryFields) -> Result<Self, Self::Error> {
        Ok(Self {
            url: fields.url,
            basic_auth: BasicAuth::from_pair(fields.username, fields.password)?,
        })
    }
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

#[cfg(test)]
mod tests {
    use crate::command::scanner::config::ScannerRegistry;

    /// A username without its password would otherwise pull anonymously.
    #[test]
    fn a_half_credential_pair_is_refused() {
        for half in [r#"username = "ci""#, r#"password = "pw""#] {
            let toml = format!("url = \"https://registry.example\"\n{half}");
            assert!(
                toml::from_str::<ScannerRegistry>(&toml).is_err(),
                "{half} alone must be refused"
            );
        }
    }
}
