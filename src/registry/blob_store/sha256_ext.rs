use sha2::{Sha256, digest::common::hazmat::SerializableState};

use crate::{oci, registry::blob_store::Error};

pub trait Sha256Ext {
    fn serialized_state(&self) -> Vec<u8>;
    fn from_state(state: &[u8]) -> Result<Sha256, Error>;
    fn digest(self) -> oci::Digest;
}

impl Sha256Ext for Sha256 {
    fn serialized_state(&self) -> Vec<u8> {
        let state = self.serialize();
        state.as_slice().to_vec()
    }

    fn from_state(state: &[u8]) -> Result<Sha256, Error> {
        let state = state
            .try_into()
            .map_err(|_| Error::HashSerialization("Unable to resume hash state".to_string()))?;
        let hasher = Sha256::deserialize(state)?;

        Ok(hasher)
    }

    fn digest(self) -> oci::Digest {
        oci::Digest::from_sha256(self)
    }
}

#[cfg(test)]
mod tests {
    use sha2::Digest;

    use super::*;

    #[test]
    fn test_hash_serialization() {
        let mut empty_state = Sha256::new();
        empty_state.update(b"hello world");
        let empty_state = empty_state.serialized_state();

        let state = Sha256::from_state(&empty_state).expect("Failed to deserialize hash state");
        let state = state.serialized_state();

        assert_eq!(empty_state, state);
    }

    #[test]
    fn from_state_with_empty_slice_returns_error() {
        let result = Sha256::from_state(&[]);
        assert!(
            result.is_err(),
            "empty byte slice must not deserialize to a valid hasher"
        );
    }

    #[test]
    fn from_state_with_truncated_bytes_returns_error() {
        let mut hasher = Sha256::new();
        hasher.update(b"data");
        let state = hasher.serialized_state();

        // Truncate to half length, no longer a valid state encoding.
        let truncated = &state[..state.len() / 2];
        let result = Sha256::from_state(truncated);
        assert!(
            result.is_err(),
            "truncated state must not deserialize successfully"
        );
    }

    #[test]
    fn from_state_with_extra_bytes_returns_error() {
        let mut hasher = Sha256::new();
        hasher.update(b"data");
        let mut state = hasher.serialized_state();
        // Append a spurious byte: wrong total length.
        state.push(0xFF);

        let result = Sha256::from_state(&state);
        assert!(
            result.is_err(),
            "state with extra bytes must not deserialize successfully"
        );
    }

    #[test]
    fn fresh_hasher_state_round_trips() {
        // A brand-new hasher (no data written) must serialize and deserialize.
        let hasher = Sha256::new();
        let state = hasher.serialized_state();
        let restored = Sha256::from_state(&state).expect("fresh hasher state must round-trip");
        assert_eq!(state, restored.serialized_state());
    }

    #[test]
    fn digest_of_empty_input_matches_known_sha256() {
        // SHA-256 of the empty string is well-known.
        let hasher = Sha256::new();
        let digest = hasher.digest();
        // Known empty-input SHA-256 hex.
        let expected = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        assert_eq!(
            digest,
            crate::oci::Digest::sha256(expected).unwrap(),
            "SHA-256 of empty input must be {expected}"
        );
    }
}
