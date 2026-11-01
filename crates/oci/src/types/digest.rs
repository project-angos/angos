use std::fmt::{self, Display, Formatter};
use std::str::FromStr;

use http::header::{HeaderValue, InvalidHeaderValue};
use serde::{Deserialize, Serialize, Serializer};
use sha2::{Digest as Sha2Digest, Sha256, Sha512};

use crate::types::Error;

/// OCI digest algorithm: `sha256` (canonical) and `sha512` (optional) are the
/// only two the image-spec registers.
/// REF: <https://github.com/opencontainers/image-spec/blob/v1.0.1/descriptor.md#digests>
#[derive(Debug, Clone, Copy, Ord, Eq, Hash, PartialEq, PartialOrd, Deserialize, Serialize)]
#[serde(try_from = "String", into = "&'static str")]
pub enum Algorithm {
    Sha256,
    Sha512,
}

impl Algorithm {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Algorithm::Sha256 => "sha256",
            Algorithm::Sha512 => "sha512",
        }
    }

    /// Length of the lowercase-hex encoding of this algorithm's hash
    /// (64 for sha256, 128 for sha512).
    #[must_use]
    pub fn hex_len(self) -> usize {
        match self {
            Algorithm::Sha256 => 64,
            Algorithm::Sha512 => 128,
        }
    }

    /// Every algorithm angos supports, canonical (sha256) first.
    #[must_use]
    pub fn supported_algorithms() -> &'static [Algorithm] {
        &[Algorithm::Sha256, Algorithm::Sha512]
    }
}

impl From<Algorithm> for &'static str {
    fn from(algorithm: Algorithm) -> Self {
        algorithm.as_str()
    }
}

impl TryFrom<String> for Algorithm {
    type Error = Error;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        s.parse()
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
            other => Err(Error::Digest(format!(
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
    #[must_use]
    pub fn algorithm(&self) -> Algorithm {
        self.algorithm
    }

    #[must_use]
    pub fn hash(&self) -> &str {
        &self.hash
    }

    #[must_use]
    pub fn hash_prefix(&self) -> &str {
        &self.hash[0..2]
    }

    /// Build a digest from a bare hash (no `algorithm:` prefix), validating it
    /// is exactly `algorithm.hex_len()` lowercase hex characters.
    ///
    /// # Errors
    ///
    /// Returns an error when `hash` is not the algorithm's exact length in
    /// lowercase hex.
    pub fn with_algorithm(algorithm: Algorithm, hash: &str) -> Result<Self, Error> {
        // Uppercase hex MUST NOT be used per the OCI image spec.
        if hash.len() != algorithm.hex_len()
            || !hash.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
        {
            return Err(Error::Digest(format!(
                "Invalid {} hash '{hash}'",
                algorithm.as_str()
            )));
        }
        Ok(Digest {
            algorithm,
            hash: hash.into(),
        })
    }

    /// Build a sha256 digest from a bare hash.
    ///
    /// # Errors
    ///
    /// Returns an error when `hash` is not 64 lowercase hex characters.
    pub fn sha256(hash: &str) -> Result<Self, Error> {
        Self::with_algorithm(Algorithm::Sha256, hash)
    }

    /// Build a sha512 digest from a bare hash.
    ///
    /// # Errors
    ///
    /// Returns an error when `hash` is not 128 lowercase hex characters.
    pub fn sha512(hash: &str) -> Result<Self, Error> {
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
            Algorithm::Sha256 => Self::from_finalized(algorithm, Sha256::digest(bytes)),
            Algorithm::Sha512 => Self::from_finalized(algorithm, Sha512::digest(bytes)),
        }
    }

    /// The sha256 digest of `bytes` (the canonical algorithm), computed in one
    /// shot.
    pub fn sha256_of_bytes(bytes: impl AsRef<[u8]>) -> Self {
        Self::from_bytes(Algorithm::Sha256, bytes)
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
            Error::Digest(format!(
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
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

/// A digest is its algorithm plus hex, so this only fails if one ever holds
/// bytes no header may carry.
impl TryFrom<&Digest> for HeaderValue {
    type Error = InvalidHeaderValue;

    fn try_from(digest: &Digest) -> Result<Self, Self::Error> {
        HeaderValue::try_from(digest.to_string())
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

    /// The image spec spells both the algorithm and the encoded hash in
    /// lowercase, and fixes the hash length per algorithm.
    #[test]
    fn rejects_what_the_digest_grammar_forbids() {
        let upper_hex = "sha256:0123456789ABCDEF0123456789abcdef0123456789abcdef0123456789abcdef";
        let mixed_hex = "sha256:0123456789aBcDeF0123456789abcdef0123456789abcdef0123456789abcdef";
        for value in [
            "sha256:invalid".to_string(),
            // The algorithm is lowercase, and so is the encoded hash.
            format!("SHA256:{VALID_HASH}"),
            format!("Sha256:{VALID_HASH}"),
            upper_hex.to_string(),
            mixed_hex.to_string(),
            // 64 hex chars is a valid sha256 hash but too short for sha512;
            // 128 is a valid sha512 but too long for sha256.
            format!("sha512:{VALID_HASH}"),
            format!("sha256:{VALID_HASH_512}"),
            // Missing or extra pieces around the `algorithm:hash` split.
            String::new(),
            "sha256nocolon".to_string(),
            format!(":{VALID_HASH}"),
            "sha256:".to_string(),
            format!("sha256:{}", "z".repeat(64)),
            format!("sha256:abc:{VALID_HASH}"),
            format!(" sha256:{VALID_HASH}"),
            format!("sha256:{VALID_HASH}\n"),
        ] {
            assert!(
                Digest::from_str(&value).is_err(),
                "'{value}' is not a digest the image spec defines"
            );
        }
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
}
