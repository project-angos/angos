use std::collections::BTreeMap;

use base64::{Engine, prelude::BASE64_STANDARD};
use sha2::{Digest, Sha256, Sha512, digest::common::hazmat::SerializableState};

use crate::{
    oci::{self, Algorithm},
    registry::blob_store::Error,
};

/// One supported algorithm's live hasher. The mid-stream state can be
/// serialized and restored (for chunked-upload checkpoints), and a finalized
/// hasher yields an [`oci::Digest`].
enum AlgorithmHasher {
    Sha256(Sha256),
    Sha512(Sha512),
}

impl AlgorithmHasher {
    fn new(algorithm: Algorithm) -> Self {
        match algorithm {
            Algorithm::Sha256 => Self::Sha256(Sha256::new()),
            Algorithm::Sha512 => Self::Sha512(Sha512::new()),
        }
    }

    fn from_state(algorithm: Algorithm, state: &[u8]) -> Result<Self, Error> {
        let invalid =
            || Error::HashSerialization("Unable to resume hash state".to_string());
        Ok(match algorithm {
            Algorithm::Sha256 => {
                Self::Sha256(Sha256::deserialize(state.try_into().map_err(|_| invalid())?)?)
            }
            Algorithm::Sha512 => {
                Self::Sha512(Sha512::deserialize(state.try_into().map_err(|_| invalid())?)?)
            }
        })
    }

    fn algorithm(&self) -> Algorithm {
        match self {
            Self::Sha256(_) => Algorithm::Sha256,
            Self::Sha512(_) => Algorithm::Sha512,
        }
    }

    fn update(&mut self, data: &[u8]) {
        match self {
            Self::Sha256(h) => h.update(data),
            Self::Sha512(h) => h.update(data),
        }
    }

    fn serialized_state(&self) -> Vec<u8> {
        match self {
            Self::Sha256(h) => h.serialize().as_slice().to_vec(),
            Self::Sha512(h) => h.serialize().as_slice().to_vec(),
        }
    }

    fn digest(&self) -> oci::Digest {
        match self {
            Self::Sha256(h) => oci::Digest::from_finalized(Algorithm::Sha256, h.clone().finalize()),
            Self::Sha512(h) => oci::Digest::from_finalized(Algorithm::Sha512, h.clone().finalize()),
        }
    }
}

/// Hashes a byte stream under every [`Algorithm::supported_algorithms`] in a
/// single pass, so a chunked upload (whose digest algorithm is only known at
/// the final `PUT`) can be verified without re-reading the assembled blob. All
/// present states are checkpointed together as one JSON map.
pub struct Hasher {
    hashers: Vec<AlgorithmHasher>,
}

impl Hasher {
    pub fn new() -> Self {
        Self {
            hashers: Algorithm::supported_algorithms()
                .iter()
                .map(|&a| AlgorithmHasher::new(a))
                .collect(),
        }
    }

    /// Hash under a single known algorithm, for verify-only paths where the
    /// target algorithm is already known and no checkpoint is persisted.
    pub fn for_algorithm(algorithm: Algorithm) -> Self {
        Self {
            hashers: vec![AlgorithmHasher::new(algorithm)],
        }
    }

    pub fn update(&mut self, data: &[u8]) {
        for hasher in &mut self.hashers {
            hasher.update(data);
        }
    }

    /// Capture every present hasher's resumable state into a [`HashState`].
    pub fn state(&self) -> HashState {
        HashState {
            states: self
                .hashers
                .iter()
                .map(|h| (h.algorithm(), h.serialized_state()))
                .collect(),
        }
    }

    /// The digest for `algorithm`, or an error if this hasher carries no state
    /// for it (a legacy checkpoint resumed without that algorithm).
    pub fn digest(&self, algorithm: Algorithm) -> Result<oci::Digest, Error> {
        self.hashers
            .iter()
            .find(|h| h.algorithm() == algorithm)
            .map(AlgorithmHasher::digest)
            .ok_or(Error::DigestAlgorithmUnavailable(algorithm))
    }
}

impl Default for Hasher {
    fn default() -> Self {
        Self::new()
    }
}

/// The serializable checkpoint of a [`Hasher`]: each algorithm's resumable
/// state, encoded as a JSON map of algorithm name to base64-encoded state.
pub struct HashState {
    states: BTreeMap<Algorithm, Vec<u8>>,
}

impl HashState {
    /// Encode as the JSON checkpoint bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        let map: BTreeMap<&str, String> = self
            .states
            .iter()
            .map(|(algorithm, state)| (algorithm.as_str(), BASE64_STANDARD.encode(state)))
            .collect();
        Ok(serde_json::to_vec(&map)?)
    }

    /// Decode from checkpoint bytes; unknown algorithms are ignored so a newer
    /// build's checkpoint still resumes. A legacy pre-JSON payload (a bare
    /// sha256 state) is not valid JSON, so it is read as a sha256-only checkpoint.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        let Ok(map) = serde_json::from_slice::<BTreeMap<String, String>>(bytes) else {
            return Ok(Self {
                states: BTreeMap::from([(Algorithm::Sha256, bytes.to_vec())]),
            });
        };

        let mut states = BTreeMap::new();
        for (name, encoded) in map {
            let Ok(algorithm) = name.parse::<Algorithm>() else {
                continue;
            };
            let decoded = BASE64_STANDARD
                .decode(&encoded)
                .map_err(|e| Error::HashSerialization(e.to_string()))?;
            states.insert(algorithm, decoded);
        }

        if states.is_empty() {
            return Err(Error::HashSerialization(
                "empty hash checkpoint".to_string(),
            ));
        }
        Ok(Self { states })
    }

    /// Rebuild a live [`Hasher`] from the checkpointed states. A missing
    /// algorithm (e.g. sha512 absent from a legacy checkpoint) is left out
    /// rather than started fresh, which would hash from the wrong offset and
    /// silently corrupt the result; [`Hasher::digest`] then errors if it is
    /// requested.
    pub fn into_hasher(self) -> Result<Hasher, Error> {
        let hashers = self
            .states
            .into_iter()
            .map(|(algorithm, state)| AlgorithmHasher::from_state(algorithm, &state))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Hasher { hashers })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    const EMPTY_SHA512: &str = "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e";

    #[test]
    fn sha256_state_round_trips() {
        let mut hasher = AlgorithmHasher::new(Algorithm::Sha256);
        hasher.update(b"hello world");
        let state = hasher.serialized_state();
        let restored = AlgorithmHasher::from_state(Algorithm::Sha256, &state).unwrap();
        assert_eq!(state, restored.serialized_state());
    }

    #[test]
    fn sha512_state_round_trips() {
        let mut hasher = AlgorithmHasher::new(Algorithm::Sha512);
        hasher.update(b"hello world");
        let state = hasher.serialized_state();
        let restored = AlgorithmHasher::from_state(Algorithm::Sha512, &state).unwrap();
        assert_eq!(state, restored.serialized_state());
    }

    #[test]
    fn digest_matches_known_empty() {
        assert_eq!(
            AlgorithmHasher::new(Algorithm::Sha256).digest(),
            oci::Digest::sha256(EMPTY_SHA256).unwrap()
        );
        assert_eq!(
            AlgorithmHasher::new(Algorithm::Sha512).digest(),
            oci::Digest::sha512(EMPTY_SHA512).unwrap()
        );
    }

    #[test]
    fn hasher_produces_every_supported_digest() {
        let mut hasher = Hasher::new();
        hasher.update(b"");
        assert_eq!(
            hasher.digest(Algorithm::Sha256).unwrap(),
            oci::Digest::sha256(EMPTY_SHA256).unwrap()
        );
        assert_eq!(
            hasher.digest(Algorithm::Sha512).unwrap(),
            oci::Digest::sha512(EMPTY_SHA512).unwrap()
        );
    }

    #[test]
    fn hasher_combined_state_round_trips() {
        let mut hasher = Hasher::new();
        hasher.update(b"some streamed bytes");
        let bytes = hasher.state().to_bytes().unwrap();

        let restored = HashState::from_bytes(&bytes)
            .unwrap()
            .into_hasher()
            .unwrap();
        assert_eq!(
            restored.digest(Algorithm::Sha256).unwrap(),
            hasher.digest(Algorithm::Sha256).unwrap()
        );
        assert_eq!(
            restored.digest(Algorithm::Sha512).unwrap(),
            hasher.digest(Algorithm::Sha512).unwrap()
        );
    }

    #[test]
    fn legacy_sha256_only_checkpoint_resumes_sha256_and_rejects_sha512() {
        // A pre-JSON checkpoint is a bare sha256 state.
        let mut sha256 = AlgorithmHasher::new(Algorithm::Sha256);
        sha256.update(b"legacy bytes");
        let legacy = sha256.serialized_state();

        let restored = HashState::from_bytes(&legacy)
            .unwrap()
            .into_hasher()
            .unwrap();
        assert_eq!(
            restored.digest(Algorithm::Sha256).unwrap(),
            sha256.digest()
        );
        // sha512 was never hashed for the legacy bytes: it must NOT be fabricated,
        // and the error names the unavailable algorithm (mapped to a 4xx upstream)
        // rather than a generic hash-serialization failure.
        assert!(matches!(
            restored.digest(Algorithm::Sha512),
            Err(Error::DigestAlgorithmUnavailable(Algorithm::Sha512))
        ));
    }

    #[test]
    fn from_state_rejects_garbage() {
        assert!(
            HashState::from_bytes(&[0xAB, 0xCD, 0xEF])
                .unwrap()
                .into_hasher()
                .is_err()
        );
    }
}
