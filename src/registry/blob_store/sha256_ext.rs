use sha2::{Digest, Sha256, digest::crypto_common::hazmat::SerializableState};

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
        let hash = self.finalize();
        let digest = hex::encode(hash.as_slice());
        oci::Digest::Sha256(digest.into())
    }
}

#[cfg(test)]
mod tests {
    use sha2::Digest;

    use super::*;

    #[tokio::test]
    async fn test_hash_serialization() {
        let mut empty_state = Sha256::new();
        empty_state.update(b"hello world");
        let empty_state = empty_state.serialized_state();

        let state = Sha256::from_state(&empty_state).expect("Failed to deserialize hash state");
        let state = state.serialized_state();

        assert_eq!(empty_state, state);
    }
}
