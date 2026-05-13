use sha2::{Digest as ShaDigest, Sha256};

use crate::oci::Digest;

pub fn hex(input: impl AsRef<[u8]>) -> String {
    hex::encode(Sha256::digest(input.as_ref()))
}

#[cfg(test)]
pub fn digest(input: impl AsRef<[u8]>) -> Digest {
    Digest::Sha256(hex(input).into())
}

pub fn finalize_digest(hasher: Sha256) -> Digest {
    Digest::Sha256(hex::encode(hasher.finalize()).into())
}

pub fn shard_key(value: &str) -> String {
    let hash = Sha256::digest(value.as_bytes());
    format!("{:02x}", hash[0])
}
