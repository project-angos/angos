use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::{Bytes, BytesMut};
use futures_util::stream;
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncRead, AsyncReadExt, ReadBuf},
    sync::mpsc,
    task::JoinHandle,
};

use angos_tx_engine::ByteStream;

use crate::{
    oci,
    registry::blob_store::{Error, sha256_ext::Sha256Ext},
};

const READ_FRAME_SIZE: usize = 1024 * 1024;

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

/// Final hash state surfaced once the hashing reader has been fully drained.
pub struct FinalHashState {
    pub digest: oci::Digest,
    pub serialized: Vec<u8>,
}

/// Drive a [`HashingReader`] in a background task, surfacing its bytes as a
/// [`ByteStream`] for the storage backend to consume and resolving to the
/// final hasher state via the returned join handle once exactly
/// `content_length` bytes have been read.
///
/// Bytes are framed in 1 MiB chunks shipped over an mpsc channel; the body
/// never sits whole in memory regardless of `content_length`.
pub fn hashing_stream<R>(
    reader: HashingReader<R>,
    content_length: u64,
) -> (ByteStream, JoinHandle<Result<FinalHashState, Error>>)
where
    R: AsyncRead + Unpin + Send + 'static,
{
    let (tx, rx) = mpsc::channel::<Result<Bytes, io::Error>>(8);
    let handle = tokio::spawn(async move {
        let mut reader = reader;
        let mut sent: u64 = 0;
        let mut buf = BytesMut::with_capacity(READ_FRAME_SIZE);
        while sent < content_length {
            let want = (content_length - sent).min(READ_FRAME_SIZE as u64);
            buf.clear();
            let n = {
                let mut limited = (&mut reader).take(want);
                limited.read_buf(&mut buf).await
            };
            match n {
                Ok(0) => {
                    let _ = tx
                        .send(Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            format!(
                                "short upload body: expected {content_length} bytes, got {sent}"
                            ),
                        )))
                        .await;
                    return Err(Error::UploadBodySize {
                        expected: content_length,
                        actual: sent,
                    });
                }
                Ok(n) => {
                    sent = sent
                        .checked_add(
                            u64::try_from(n).map_err(|e| Error::InvalidFormat(e.to_string()))?,
                        )
                        .ok_or_else(|| Error::StorageBackend("size overflow".into()))?;
                    if tx.send(Ok(buf.split().freeze())).await.is_err() {
                        break;
                    }
                }
                Err(e) => {
                    let _ = tx.send(Err(io::Error::other(e.to_string()))).await;
                    return Err(Error::UploadBodyRead(e.to_string()));
                }
            }
        }
        drop(tx);
        Ok(FinalHashState {
            digest: reader.digest(),
            serialized: reader.serialized_state(),
        })
    });

    let body: ByteStream = Box::pin(stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    }));
    (body, handle)
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
