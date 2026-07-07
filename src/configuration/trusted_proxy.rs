use std::{fmt, net::IpAddr};

use serde::{Deserialize, de};

/// A proxy address or CIDR network allowed to set the forwarded-client
/// headers (`X-Forwarded-For`, `X-Real-IP`).
///
/// Accepts a plain IP (`"10.0.0.1"`) or a CIDR network (`"10.0.0.0/8"`,
/// `"fd00::/8"`). Invalid entries are rejected when the TOML configuration is
/// parsed, so the registry fails fast instead of silently trusting nobody.
#[derive(Clone)]
pub struct TrustedProxy {
    source: String,
    network: IpAddr,
    prefix: u8,
}

impl TrustedProxy {
    /// Parses `source` as an IP address or CIDR network.
    ///
    /// # Errors
    ///
    /// Returns a message describing the invalid address or prefix.
    pub fn parse(source: impl Into<String>) -> Result<Self, String> {
        let source = source.into();
        let (address, prefix) = match source.split_once('/') {
            Some((address, prefix)) => (address, Some(prefix)),
            None => (source.as_str(), None),
        };
        let network: IpAddr = address
            .parse()
            .map_err(|e| format!("invalid trusted proxy '{source}': {e}"))?;
        let max_prefix = if network.is_ipv4() { 32 } else { 128 };
        let prefix = match prefix {
            Some(prefix) => prefix
                .parse::<u8>()
                .ok()
                .filter(|p| *p <= max_prefix)
                .ok_or_else(|| {
                    format!("invalid trusted proxy '{source}': prefix must be 0-{max_prefix}")
                })?,
            None => max_prefix,
        };
        Ok(Self {
            source,
            network,
            prefix,
        })
    }

    /// Returns `true` when `ip` is this address or belongs to this network.
    /// Families never cross-match: an IPv4 network contains no IPv6 address.
    pub fn contains(&self, ip: IpAddr) -> bool {
        match (self.network, ip) {
            (IpAddr::V4(network), IpAddr::V4(ip)) => {
                let shift = 32 - u32::from(self.prefix);
                (network.to_bits() ^ ip.to_bits())
                    .checked_shr(shift)
                    .unwrap_or(0)
                    == 0
            }
            (IpAddr::V6(network), IpAddr::V6(ip)) => {
                let shift = 128 - u32::from(self.prefix);
                (network.to_bits() ^ ip.to_bits())
                    .checked_shr(shift)
                    .unwrap_or(0)
                    == 0
            }
            _ => false,
        }
    }
}

impl fmt::Debug for TrustedProxy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("TrustedProxy").field(&self.source).finish()
    }
}

impl<'de> Deserialize<'de> for TrustedProxy {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let source = String::deserialize(deserializer)?;
        Self::parse(source).map_err(de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    #[test]
    fn plain_ip_matches_itself_only() {
        let proxy = TrustedProxy::parse("10.0.0.1").unwrap();
        assert!(proxy.contains(ip("10.0.0.1")));
        assert!(!proxy.contains(ip("10.0.0.2")));
    }

    #[test]
    fn cidr_matches_network_members() {
        let proxy = TrustedProxy::parse("10.0.0.0/8").unwrap();
        assert!(proxy.contains(ip("10.1.2.3")));
        assert!(!proxy.contains(ip("11.0.0.1")));
    }

    #[test]
    fn zero_prefix_matches_every_address_of_the_family() {
        let proxy = TrustedProxy::parse("0.0.0.0/0").unwrap();
        assert!(proxy.contains(ip("203.0.113.7")));
        assert!(!proxy.contains(ip("::1")), "families must not cross-match");
    }

    #[test]
    fn ipv6_cidr_matches() {
        let proxy = TrustedProxy::parse("fd00::/8").unwrap();
        assert!(proxy.contains(ip("fd12:3456::1")));
        assert!(!proxy.contains(ip("fe80::1")));
        assert!(!proxy.contains(ip("10.0.0.1")));
    }

    #[test]
    fn invalid_entries_are_rejected() {
        assert!(TrustedProxy::parse("not-an-ip").is_err());
        assert!(TrustedProxy::parse("10.0.0.0/33").is_err());
        assert!(TrustedProxy::parse("fd00::/129").is_err());
        assert!(TrustedProxy::parse("10.0.0.0/").is_err());
    }

    #[test]
    fn invalid_entry_fails_at_deserialize() {
        #[derive(Debug, serde::Deserialize)]
        struct Wrapper {
            #[allow(dead_code)]
            proxies: Vec<TrustedProxy>,
        }
        assert!(toml::from_str::<Wrapper>(r#"proxies = ["10.0.0.0/40"]"#).is_err());
        assert!(toml::from_str::<Wrapper>(r#"proxies = ["127.0.0.1", "10.0.0.0/8"]"#).is_ok());
    }
}
