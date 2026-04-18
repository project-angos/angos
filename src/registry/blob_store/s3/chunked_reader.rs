use tokio::io::{AsyncRead, AsyncReadExt, Take};

use crate::{oci::Digest, registry::blob_store::hashing_reader::HashingReader};

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

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use tokio::io::AsyncReadExt;

    use super::ChunkedReader;

    async fn collect_chunks(data: &[u8], chunk_size: u64) -> Vec<Vec<u8>> {
        let cursor = Cursor::new(data.to_vec());
        let mut reader = ChunkedReader::new(cursor, chunk_size);
        let mut chunks = Vec::new();

        while let Some(mut chunk) = reader.next_chunk() {
            let mut buf = Vec::new();
            chunk.read_to_end(&mut buf).await.unwrap();
            if buf.is_empty() {
                reader.mark_finished();
            } else {
                chunks.push(buf);
            }
        }

        chunks
    }

    #[tokio::test]
    async fn test_exact_divisible() {
        let data = b"abcdefghi"; // 9 bytes
        let chunks = collect_chunks(data, 3).await;
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0], b"abc");
        assert_eq!(chunks[1], b"def");
        assert_eq!(chunks[2], b"ghi");
    }

    #[tokio::test]
    async fn test_last_chunk_smaller() {
        let data = b"abcdefghij"; // 10 bytes
        let chunks = collect_chunks(data, 4).await;
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0], b"abcd");
        assert_eq!(chunks[1], b"efgh");
        assert_eq!(chunks[2], b"ij");
    }

    #[tokio::test]
    async fn test_input_smaller_than_chunk() {
        let data = b"abc"; // 3 bytes
        let chunks = collect_chunks(data, 10).await;
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0], b"abc");
    }

    #[tokio::test]
    async fn test_empty_input() {
        let chunks = collect_chunks(b"", 5).await;
        assert!(chunks.is_empty());
    }

    #[tokio::test]
    async fn test_reassembled_chunks_match_original() {
        let data: Vec<u8> = (0u8..=255).collect();
        let chunks = collect_chunks(&data, 30).await;
        let reassembled: Vec<u8> = chunks.into_iter().flatten().collect();
        assert_eq!(reassembled, data);
    }
}
