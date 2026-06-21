use std::{
    fmt,
    fmt::{Display, Formatter},
    str::FromStr,
};

use serde::{Deserialize, Serialize};
use sha2::{Digest as Sha2Digest, Sha256, Sha512};

use crate::oci::Error;

/// OCI digest algorithm: `sha256` (canonical) and `sha512` (optional) are the
/// only two the image-spec registers.
/// REF: <https://github.com/opencontainers/image-spec/blob/v1.0.1/descriptor.md#digests>
#[derive(Debug, Clone, Copy, Ord, Eq, Hash, PartialEq, PartialOrd)]
pub enum Algorithm {
    Sha256,
    Sha512,
}

impl Algorithm {
    pub fn as_str(self) -> &'static str {
        match self {
            Algorithm::Sha256 => "sha256",
            Algorithm::Sha512 => "sha512",
        }
    }

    /// Length of the lowercase-hex encoding of this algorithm's hash
    /// (64 for sha256, 128 for sha512).
    pub fn hex_len(self) -> usize {
        match self {
            Algorithm::Sha256 => 64,
            Algorithm::Sha512 => 128,
        }
    }

    /// Every algorithm angos supports, canonical (sha256) first.
    pub fn supported_algorithms() -> &'static [Algorithm] {
        &[Algorithm::Sha256, Algorithm::Sha512]
    }
}

impl FromStr for Algorithm {
    type Err = Error;

    /// Parse an algorithm name; the single point that rejects unsupported
    /// algorithms.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "sha256" => Ok(Algorithm::Sha256),
            "sha512" => Ok(Algorithm::Sha512),
            other => Err(Error::InvalidDigest(format!(
                "Unsupported digest algorithm '{other}'"
            ))),
        }
    }
}

impl Display for Algorithm {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A validated OCI content digest: an [`Algorithm`] plus its lowercase-hex hash.
/// The private `hash` field forces construction through the validating
/// constructors below.
#[derive(Debug, Clone, Ord, Eq, Hash, PartialEq, PartialOrd, Deserialize)]
#[serde(try_from = "String")]
pub struct Digest {
    algorithm: Algorithm,
    hash: Box<str>,
}

impl Digest {
    pub fn algorithm(&self) -> Algorithm {
        self.algorithm
    }

    pub fn hash(&self) -> &str {
        &self.hash
    }

    pub fn hash_prefix(&self) -> &str {
        &self.hash[0..2]
    }

    /// Build a digest from a bare hash (no `algorithm:` prefix), validating it
    /// is exactly `algorithm.hex_len()` lowercase hex characters.
    pub fn with_algorithm(algorithm: Algorithm, hash: impl Into<Box<str>>) -> Result<Self, Error> {
        let hash = hash.into();
        validate_hex(algorithm, &hash)?;
        Ok(Digest { algorithm, hash })
    }

    /// Build a sha256 digest from a bare hash.
    pub fn sha256(hash: impl Into<Box<str>>) -> Result<Self, Error> {
        Self::with_algorithm(Algorithm::Sha256, hash)
    }

    /// Build a sha512 digest from a bare hash.
    pub fn sha512(hash: impl Into<Box<str>>) -> Result<Self, Error> {
        Self::with_algorithm(Algorithm::Sha512, hash)
    }

    /// Build a digest from a hasher's finalized raw bytes. Infallible: a
    /// finalize always yields the algorithm's exact length.
    pub fn from_finalized(algorithm: Algorithm, raw: impl AsRef<[u8]>) -> Self {
        Digest {
            algorithm,
            hash: hex::encode(raw).into(),
        }
    }

    /// The digest of `bytes` under `algorithm`, computed in one shot.
    pub fn from_bytes(algorithm: Algorithm, bytes: impl AsRef<[u8]>) -> Self {
        match algorithm {
            Algorithm::Sha256 => {
                let mut hasher = Sha256::new();
                hasher.update(bytes.as_ref());
                Self::from_finalized(Algorithm::Sha256, hasher.finalize())
            }
            Algorithm::Sha512 => {
                let mut hasher = Sha512::new();
                hasher.update(bytes.as_ref());
                Self::from_finalized(Algorithm::Sha512, hasher.finalize())
            }
        }
    }

    /// The sha256 digest of `bytes` (the canonical algorithm), computed in one
    /// shot.
    pub fn sha256_of_bytes(bytes: impl AsRef<[u8]>) -> Self {
        Self::from_bytes(Algorithm::Sha256, bytes)
    }
}

/// Validate a bare hash: exactly `algorithm.hex_len()` lowercase hex chars
/// (`/[a-f0-9]+/`; uppercase MUST NOT be used per the OCI image spec).
fn validate_hex(algorithm: Algorithm, hash: &str) -> Result<(), Error> {
    if hash.len() != algorithm.hex_len()
        || !hash.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
    {
        return Err(Error::InvalidDigest(format!(
            "Invalid {} hash '{hash}'",
            algorithm.as_str()
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

        let algorithm = Algorithm::from_str(algorithm)?;
        Self::with_algorithm(algorithm, hash)
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
    const VALID_HASH_512: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    #[test]
    fn test_parse() {
        let digest = Digest::from_str(&format!("sha256:{VALID_HASH}")).unwrap();
        assert_eq!(digest.algorithm(), Algorithm::Sha256);
        assert_eq!(digest.hash(), VALID_HASH);
        assert_eq!(digest.hash_prefix(), "01");
    }

    #[test]
    fn test_parse_sha512() {
        let digest = Digest::from_str(&format!("sha512:{VALID_HASH_512}")).unwrap();
        assert_eq!(digest.algorithm(), Algorithm::Sha512);
        assert_eq!(digest.hash(), VALID_HASH_512);
        assert_eq!(digest.hash_prefix(), "01");
    }

    #[test]
    fn test_algorithm_as_str_and_display() {
        assert_eq!(Algorithm::Sha256.as_str(), "sha256");
        assert_eq!(Algorithm::Sha512.as_str(), "sha512");
        assert_eq!(Algorithm::Sha512.to_string(), "sha512");
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
    fn test_sha512_rejects_sha256_length() {
        // 64 hex chars is a valid sha256 hash but too short for sha512.
        assert!(Digest::from_str(&format!("sha512:{VALID_HASH}")).is_err());
    }

    #[test]
    fn test_sha256_rejects_sha512_length() {
        // 128 hex chars is a valid sha512 hash but too long for sha256.
        assert!(Digest::from_str(&format!("sha256:{VALID_HASH_512}")).is_err());
    }

    #[test]
    fn test_display() {
        let digest = Digest::sha256(VALID_HASH).unwrap();
        assert_eq!(digest.to_string(), format!("sha256:{VALID_HASH}"));
    }

    #[test]
    fn test_sha512_display_round_trip() {
        let digest = Digest::sha512(VALID_HASH_512).unwrap();
        assert_eq!(digest.to_string(), format!("sha512:{VALID_HASH_512}"));
        assert_eq!(Digest::from_str(&digest.to_string()).unwrap(), digest);
    }

    #[test]
    fn test_from_bytes_sha256_matches_known_empty() {
        let digest = Digest::from_bytes(Algorithm::Sha256, b"");
        let expected = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        assert_eq!(digest, Digest::sha256(expected).unwrap());
        assert_eq!(digest, Digest::sha256_of_bytes(b""));
    }

    #[test]
    fn test_from_bytes_sha512_matches_known_empty() {
        let digest = Digest::from_bytes(Algorithm::Sha512, b"");
        let expected = "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e";
        assert_eq!(digest, Digest::sha512(expected).unwrap());
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
