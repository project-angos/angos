use tokio::io::{AsyncRead, AsyncReadExt, Take};

use crate::oci::Digest;
use crate::registry::blob_store::hashing_reader::HashingReader;

/// Splits an `AsyncRead` into fixed-size chunks (last may be smaller).
pub struct ChunkedReader<R> {
    inner: R,
    chunk_size: u64,
    finished: bool,
}

impl<R: AsyncRead + Unpin> ChunkedReader<R> {
    pub fn new(inner: R, chunk_size: u64) -> Self {
        Self {
            inner,
            chunk_size,
            finished: false,
        }
    }

    pub fn next_chunk(&mut self) -> Option<Take<&mut R>> {
        if self.finished {
            return None;
        }
        Some((&mut self.inner).take(self.chunk_size))
    }

    pub fn mark_finished(&mut self) {
        self.finished = true;
    }
}

impl<R: AsyncRead + Unpin> ChunkedReader<HashingReader<R>> {
    pub fn serialized_state(&self) -> Vec<u8> {
        self.inner.serialized_state()
    }

    pub fn digest(&self) -> Digest {
        self.inner.digest()
    }
}
