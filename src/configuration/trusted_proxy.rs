use std::fmt;
use std::net::IpAddr;
use std::str::FromStr;

use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer};

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

impl FromStr for TrustedProxy {
    type Err = String;

    /// Parses `source` as an IP address or CIDR network, failing with a
    /// message describing the invalid address or prefix.
    fn from_str(source: &str) -> Result<Self, Self::Err> {
        let (address, prefix) = match source.split_once('/') {
            Some((address, prefix)) => (address, Some(prefix)),
            None => (source, None),
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
            source: source.to_owned(),
            network,
            prefix,
        })
    }
}

impl TrustedProxy {
    /// Returns `true` when `ip` is this address or belongs to this network.
    /// Families never cross-match: an IPv4 network contains no IPv6 address.
    pub fn contains(&self, ip: IpAddr) -> bool {
        let (network, ip, width) = match (self.network, ip) {
            (IpAddr::V4(network), IpAddr::V4(ip)) => {
                (u128::from(network.to_bits()), u128::from(ip.to_bits()), 32)
            }
            (IpAddr::V6(network), IpAddr::V6(ip)) => (network.to_bits(), ip.to_bits(), 128),
            _ => return false,
        };
        (network ^ ip)
            .checked_shr(width - u32::from(self.prefix))
            .unwrap_or(0)
            == 0
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
        D: Deserializer<'de>,
    {
        let source = String::deserialize(deserializer)?;
        source.parse().map_err(DeError::custom)
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
        let proxy: TrustedProxy = "10.0.0.1".parse().unwrap();
        assert!(proxy.contains(ip("10.0.0.1")));
        assert!(!proxy.contains(ip("10.0.0.2")));
    }

    #[test]
    fn cidr_matches_network_members() {
        let proxy: TrustedProxy = "10.0.0.0/8".parse().unwrap();
        assert!(proxy.contains(ip("10.1.2.3")));
        assert!(!proxy.contains(ip("11.0.0.1")));
    }

    #[test]
    fn zero_prefix_matches_every_address_of_the_family() {
        let proxy: TrustedProxy = "0.0.0.0/0".parse().unwrap();
        assert!(proxy.contains(ip("203.0.113.7")));
        assert!(!proxy.contains(ip("::1")), "families must not cross-match");
    }

    #[test]
    fn ipv6_cidr_matches() {
        let proxy: TrustedProxy = "fd00::/8".parse().unwrap();
        assert!(proxy.contains(ip("fd12:3456::1")));
        assert!(!proxy.contains(ip("fe80::1")));
        assert!(!proxy.contains(ip("10.0.0.1")));
    }

    #[test]
    fn invalid_entries_are_rejected() {
        assert!("not-an-ip".parse::<TrustedProxy>().is_err());
        assert!("10.0.0.0/33".parse::<TrustedProxy>().is_err());
        assert!("fd00::/129".parse::<TrustedProxy>().is_err());
        assert!("10.0.0.0/".parse::<TrustedProxy>().is_err());
    }
}
