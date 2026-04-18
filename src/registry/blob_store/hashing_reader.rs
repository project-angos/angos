use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, ReadBuf};

use crate::{oci, registry::blob_store::sha256_ext::Sha256Ext};

pub struct HashingReader<R> {
    inner: R,
    hasher: Sha256,
}

impl<R> HashingReader<R> {
    pub fn with_hasher(inner: R, hasher: Sha256) -> Self {
        Self { inner, hasher }
    }

    pub fn serialized_state(&self) -> Vec<u8> {
        self.hasher.serialized_state()
    }

    pub fn digest(&self) -> oci::Digest {
        self.hasher.clone().digest()
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for HashingReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let pre_len = buf.filled().len();
        let poll = Pin::new(&mut self.inner).poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = &poll {
            let post_len = buf.filled().len();
            let new_data = &buf.filled()[pre_len..post_len];
            self.hasher.update(new_data);
        }
        poll
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use sha2::{Digest as ShaDigest, Sha256};
    use tokio::io::AsyncReadExt;

    use super::*;

    #[tokio::test]
    async fn test_known_payload_produces_correct_digest() {
        let payload = b"hello world";
        let reader = Cursor::new(payload);
        let mut hashing_reader = HashingReader::with_hasher(reader, Sha256::new());

        let mut buf = Vec::new();
        hashing_reader.read_to_end(&mut buf).await.unwrap();

        let digest = hashing_reader.digest();
        assert_eq!(
            digest.to_string(),
            "sha256:b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
    }

    #[tokio::test]
    async fn test_multiple_small_reads_produce_same_digest() {
        let payload = b"hello world";
        let reader = Cursor::new(payload);
        let mut hashing_reader = HashingReader::with_hasher(reader, Sha256::new());

        let mut chunk = [0u8; 3];
        loop {
            let n = hashing_reader.read(&mut chunk).await.unwrap();
            if n == 0 {
                break;
            }
        }

        let digest = hashing_reader.digest();
        assert_eq!(
            digest.to_string(),
            "sha256:b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
    }

    #[tokio::test]
    async fn test_empty_input_produces_correct_digest() {
        let reader = Cursor::new(b"");
        let mut hashing_reader = HashingReader::with_hasher(reader, Sha256::new());

        let mut buf = Vec::new();
        hashing_reader.read_to_end(&mut buf).await.unwrap();

        let digest = hashing_reader.digest();
        assert_eq!(
            digest.to_string(),
            "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[tokio::test]
    async fn test_inner_data_passed_through_unmodified() {
        let payload = b"hello world";
        let reader = Cursor::new(payload);
        let mut hashing_reader = HashingReader::with_hasher(reader, Sha256::new());

        let mut buf = Vec::new();
        hashing_reader.read_to_end(&mut buf).await.unwrap();

        assert_eq!(buf, payload);
    }
}
