use std::{
    fmt,
    fmt::{Display, Formatter},
    str::FromStr,
};

use serde::{Deserialize, Serialize};

use crate::oci::Error;

#[derive(Debug, Clone, Ord, Eq, Hash, PartialEq, PartialOrd, Deserialize)]
#[serde(try_from = "String")]
pub enum Digest {
    Sha256(Box<str>),
}

impl Digest {
    pub fn algorithm(&self) -> &str {
        match self {
            Digest::Sha256(_) => "sha256",
        }
    }

    pub fn hash(&self) -> &str {
        match self {
            Digest::Sha256(s) => s,
        }
    }

    pub fn hash_prefix(&self) -> &str {
        match self {
            Digest::Sha256(s) => &s[0..2],
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Digest::Sha256(s) => s,
        }
    }
}

impl FromStr for Digest {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.try_into()
    }
}

impl TryFrom<&str> for Digest {
    type Error = Error;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let (algorithm, hash) = s.split_once(':').ok_or_else(|| {
            Error::InvalidFormat(format!(
                "Digest must be in the format 'algorithm:hash', got '{s}'"
            ))
        })?;

        if algorithm != "sha256" {
            return Err(Error::InvalidFormat(format!(
                "Unsupported digest algorithm '{algorithm}'"
            )));
        }

        // Check that hash is a valid 64 bytes representation
        // As per the image specification, the hash must be a lowercase hex string:
        //
        // "When the algorithm identifier is sha256, the encoded portion MUST match /[a-f0-9]{64}/.
        // Note that [A-F] MUST NOT be used here."
        //
        // REF:
        // - https://github.com/opencontainers/image-spec/blob/v1.0.1/descriptor.md#sha-256
        if hash.len() != 64 || !hash.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
            return Err(Error::InvalidFormat(format!(
                "Invalid sha256 hash '{hash}'"
            )));
        }

        Ok(Digest::Sha256(hash.into()))
    }
}

impl TryFrom<String> for Digest {
    type Error = Error;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::try_from(s.as_str())
    }
}

impl Display for Digest {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.algorithm(), self.hash())
    }
}

impl Serialize for Digest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID_HASH: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    #[test]
    fn test_parse() {
        let digest = Digest::from_str(&format!("sha256:{VALID_HASH}")).unwrap();
        assert_eq!(digest.algorithm(), "sha256");
        assert_eq!(digest.hash(), VALID_HASH);
        assert_eq!(digest.hash_prefix(), "01");
    }

    #[test]
    fn test_parse_invalid() {
        assert!(Digest::from_str("sha256:invalid").is_err());
    }

    #[test]
    fn test_reject_uppercase_algorithm() {
        assert!(Digest::from_str(&format!("SHA256:{VALID_HASH}")).is_err());
    }

    #[test]
    fn test_reject_mixed_case_algorithm() {
        assert!(Digest::from_str(&format!("Sha256:{VALID_HASH}")).is_err());
    }

    #[test]
    fn test_reject_uppercase_hex() {
        assert!(
            Digest::from_str(
                "sha256:0123456789ABCDEF0123456789abcdef0123456789abcdef0123456789abcdef"
            )
            .is_err()
        );
    }

    #[test]
    fn test_reject_mixed_case_hex() {
        assert!(
            Digest::from_str(
                "sha256:0123456789aBcDeF0123456789abcdef0123456789abcdef0123456789abcdef"
            )
            .is_err()
        );
    }

    #[test]
    fn test_display() {
        let digest = Digest::Sha256(VALID_HASH.into());
        assert_eq!(digest.to_string(), format!("sha256:{VALID_HASH}"));
    }
}
