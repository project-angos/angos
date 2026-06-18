use std::{
    fmt,
    fmt::{Display, Formatter},
    str::FromStr,
};

use serde::{Deserialize, Serialize};
use sha2::{Digest as Sha2Digest, Sha256};

use crate::oci::Error;

/// Per the OCI image spec, a sha256 hash is exactly 64 lowercase hex characters
/// (`/[a-f0-9]{64}/`; uppercase MUST NOT be used).
/// REF: <https://github.com/opencontainers/image-spec/blob/v1.0.1/descriptor.md#sha-256>
const SHA256_HEX_LEN: usize = 64;

#[derive(Debug, Clone, Ord, Eq, Hash, PartialEq, PartialOrd, Deserialize)]
#[serde(try_from = "String")]
pub enum Digest {
    Sha256(Sha256Hash),
}

/// Validated sha256 hash payload. Its inner string is private, so a
/// `Digest::Sha256` can only be built through [`Digest`]'s validating
/// constructors. This closes the bypass a public variant field would leave open.
#[derive(Debug, Clone, Ord, Eq, Hash, PartialEq, PartialOrd)]
pub struct Sha256Hash(Box<str>);

impl Sha256Hash {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Digest {
    pub fn algorithm(&self) -> &str {
        match self {
            Digest::Sha256(_) => "sha256",
        }
    }

    pub fn hash(&self) -> &str {
        match self {
            Digest::Sha256(s) => s.as_str(),
        }
    }

    pub fn hash_prefix(&self) -> &str {
        match self {
            Digest::Sha256(s) => &s.as_str()[0..2],
        }
    }

    /// Build a sha256 digest from a bare hash (no `sha256:` prefix), validating
    /// it is exactly 64 lowercase hex characters.
    pub fn sha256(hash: impl Into<Box<str>>) -> Result<Self, Error> {
        let hash = hash.into();
        validate_sha256_hex(&hash)?;
        Ok(Digest::Sha256(Sha256Hash(hash)))
    }

    /// Finalize a sha256 `hasher` into a digest. Infallible: a sha256 finalize
    /// always yields 32 bytes / 64 lowercase hex chars, so no validation runs.
    pub fn from_sha256(hasher: Sha256) -> Self {
        Digest::Sha256(Sha256Hash(hex::encode(hasher.finalize()).into()))
    }

    /// The sha256 digest of `bytes`, computed in one shot.
    pub fn from_bytes(bytes: impl AsRef<[u8]>) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(bytes.as_ref());
        Self::from_sha256(hasher)
    }
}

/// Validate a bare sha256 hash: exactly [`SHA256_HEX_LEN`] lowercase hex chars.
fn validate_sha256_hex(hash: &str) -> Result<(), Error> {
    if hash.len() != SHA256_HEX_LEN || !hash.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
    {
        return Err(Error::InvalidDigest(format!(
            "Invalid sha256 hash '{hash}'"
        )));
    }
    Ok(())
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
            Error::InvalidDigest(format!(
                "Digest must be in the format 'algorithm:hash', got '{s}'"
            ))
        })?;

        if algorithm != "sha256" {
            return Err(Error::InvalidDigest(format!(
                "Unsupported digest algorithm '{algorithm}'"
            )));
        }

        Self::sha256(hash)
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
        let digest = Digest::sha256(VALID_HASH).unwrap();
        assert_eq!(digest.to_string(), format!("sha256:{VALID_HASH}"));
    }

    #[test]
    fn test_rejects_empty_string() {
        let Err(Error::InvalidDigest(msg)) = Digest::from_str("") else {
            panic!("expected error");
        };
        assert!(msg.contains("''"), "msg: {msg}");
    }

    #[test]
    fn test_rejects_no_colon() {
        let Err(Error::InvalidDigest(msg)) = Digest::from_str("sha256nocolon") else {
            panic!("expected error");
        };
        assert!(msg.contains("'sha256nocolon'"), "msg: {msg}");
    }

    #[test]
    fn test_rejects_empty_algorithm() {
        let Err(Error::InvalidDigest(msg)) = Digest::from_str(&format!(":{VALID_HASH}")) else {
            panic!("expected error");
        };
        assert_eq!(msg, "Unsupported digest algorithm ''");
    }

    #[test]
    fn test_rejects_empty_hash() {
        let Err(Error::InvalidDigest(msg)) = Digest::from_str("sha256:") else {
            panic!("expected error");
        };
        assert_eq!(msg, "Invalid sha256 hash ''");
    }

    #[test]
    fn test_rejects_invalid_hex_chars() {
        let invalid_hash = "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz";
        let Err(Error::InvalidDigest(msg)) = Digest::from_str(&format!("sha256:{invalid_hash}"))
        else {
            panic!("expected error");
        };
        assert!(msg.contains(invalid_hash), "msg: {msg}");
    }

    #[test]
    fn test_rejects_hash_too_short() {
        let Err(Error::InvalidDigest(msg)) = Digest::from_str("sha256:abc") else {
            panic!("expected error");
        };
        assert!(msg.contains("'abc'"), "msg: {msg}");
    }

    #[test]
    fn test_rejects_hash_too_long() {
        let long_hash = format!("{VALID_HASH}a");
        let Err(Error::InvalidDigest(msg)) = Digest::from_str(&format!("sha256:{long_hash}"))
        else {
            panic!("expected error");
        };
        assert!(msg.contains(&long_hash), "msg: {msg}");
    }

    #[test]
    fn test_rejects_multiple_colons() {
        let Err(Error::InvalidDigest(msg)) = Digest::from_str(&format!("sha256:abc:{VALID_HASH}"))
        else {
            panic!("expected error");
        };
        assert!(msg.contains(&format!("'abc:{VALID_HASH}'")), "msg: {msg}");
    }

    #[test]
    fn test_rejects_leading_whitespace() {
        let Err(Error::InvalidDigest(msg)) = Digest::from_str(&format!(" sha256:{VALID_HASH}"))
        else {
            panic!("expected error");
        };
        assert!(msg.contains("' sha256'"), "msg: {msg}");
    }

    #[test]
    fn test_rejects_trailing_newline() {
        let hash_with_newline = format!("{VALID_HASH}\n");
        let Err(Error::InvalidDigest(msg)) =
            Digest::from_str(&format!("sha256:{hash_with_newline}"))
        else {
            panic!("expected error");
        };
        assert!(msg.contains(&hash_with_newline), "msg: {msg}");
    }
}
