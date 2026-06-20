use std::collections::BTreeMap;

use base64::{Engine, prelude::BASE64_STANDARD};
use sha2::{Digest as Sha2Digest, Sha256, Sha512, digest::common::hazmat::SerializableState};

use crate::{
    oci::{self, Algorithm},
    registry::blob_store::Error,
};

/// State-resumable hash logic shared by every supported digest algorithm: the
/// mid-stream state can be serialized and restored (for chunked-upload
/// checkpoints), and a finalized hasher yields an [`oci::Digest`]. The bodies
/// are identical across algorithms, so the impls are macro-generated.
pub trait ResumableHasher: Sized {
    const ALGORITHM: Algorithm;

    /// Serialize the hasher's mid-stream state, resumable via [`Self::from_state`].
    fn serialized_state(&self) -> Vec<u8>;

    /// Restore a hasher from a [`Self::serialized_state`] payload.
    fn from_state(state: &[u8]) -> Result<Self, Error>;

    /// Finalize into the OCI digest for this algorithm.
    fn into_digest(self) -> oci::Digest;
}

macro_rules! impl_resumable_hasher {
    ($ty:ty, $algorithm:expr) => {
        impl ResumableHasher for $ty {
            const ALGORITHM: Algorithm = $algorithm;

            fn serialized_state(&self) -> Vec<u8> {
                self.serialize().as_slice().to_vec()
            }

            fn from_state(state: &[u8]) -> Result<Self, Error> {
                let state = state.try_into().map_err(|_| {
                    Error::HashSerialization("Unable to resume hash state".to_string())
                })?;
                Ok(<$ty>::deserialize(state)?)
            }

            fn into_digest(self) -> oci::Digest {
                oci::Digest::from_finalized(Self::ALGORITHM, self.finalize())
            }
        }
    };
}

impl_resumable_hasher!(Sha256, Algorithm::Sha256);
impl_resumable_hasher!(Sha512, Algorithm::Sha512);

/// Object-safe view over one algorithm's resumable hasher, so a [`Hasher`] can
/// hold a heterogeneous, future-extensible set behind `Box<dyn _>`.
trait AlgorithmHasher: Send {
    fn algorithm(&self) -> Algorithm;
    fn update(&mut self, data: &[u8]);
    fn serialized_state(&self) -> Vec<u8>;
    fn digest(&self) -> oci::Digest;
}

/// Adapts any concrete [`ResumableHasher`] to the object-safe [`AlgorithmHasher`].
struct Adapter<H>(H);

impl<H> AlgorithmHasher for Adapter<H>
where
    H: ResumableHasher + Sha2Digest + Clone + Send,
{
    fn algorithm(&self) -> Algorithm {
        H::ALGORITHM
    }

    fn update(&mut self, data: &[u8]) {
        Sha2Digest::update(&mut self.0, data);
    }

    fn serialized_state(&self) -> Vec<u8> {
        ResumableHasher::serialized_state(&self.0)
    }

    fn digest(&self) -> oci::Digest {
        self.0.clone().into_digest()
    }
}

fn fresh_boxed(algorithm: Algorithm) -> Box<dyn AlgorithmHasher> {
    match algorithm {
        Algorithm::Sha256 => Box::new(Adapter(Sha256::new())),
        Algorithm::Sha512 => Box::new(Adapter(Sha512::new())),
    }
}

fn boxed_from_state(algorithm: Algorithm, state: &[u8]) -> Result<Box<dyn AlgorithmHasher>, Error> {
    Ok(match algorithm {
        Algorithm::Sha256 => Box::new(Adapter(<Sha256 as ResumableHasher>::from_state(state)?)),
        Algorithm::Sha512 => Box::new(Adapter(<Sha512 as ResumableHasher>::from_state(state)?)),
    })
}

/// Hashes a byte stream under every [`Algorithm::supported_algorithms`] in a
/// single pass, so a chunked upload (whose digest algorithm is only known at
/// the final `PUT`) can be verified without re-reading the assembled blob. All
/// present states are checkpointed together as one JSON map.
pub struct Hasher {
    hashers: Vec<Box<dyn AlgorithmHasher>>,
}

impl Hasher {
    pub fn new() -> Self {
        Self {
            hashers: Algorithm::supported_algorithms()
                .iter()
                .map(|&a| fresh_boxed(a))
                .collect(),
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
            .map(|h| h.digest())
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
            .map(|(algorithm, state)| boxed_from_state(algorithm, &state))
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
        let mut hasher = Sha256::new();
        hasher.update(b"hello world");
        let state = hasher.serialized_state();
        let restored = <Sha256 as ResumableHasher>::from_state(&state).unwrap();
        assert_eq!(state, restored.serialized_state());
    }

    #[test]
    fn sha512_state_round_trips() {
        let mut hasher = Sha512::new();
        hasher.update(b"hello world");
        let state = hasher.serialized_state();
        let restored = <Sha512 as ResumableHasher>::from_state(&state).unwrap();
        assert_eq!(state, restored.serialized_state());
    }

    #[test]
    fn into_digest_matches_known_empty() {
        assert_eq!(
            Sha256::new().into_digest(),
            oci::Digest::sha256(EMPTY_SHA256).unwrap()
        );
        assert_eq!(
            Sha512::new().into_digest(),
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
        let mut sha256 = Sha256::new();
        sha256.update(b"legacy bytes");
        let legacy = sha256.serialized_state();

        let restored = HashState::from_bytes(&legacy)
            .unwrap()
            .into_hasher()
            .unwrap();
        assert_eq!(
            restored.digest(Algorithm::Sha256).unwrap(),
            sha256.into_digest()
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
