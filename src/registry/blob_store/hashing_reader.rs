use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::{Bytes, BytesMut};
use futures_util::stream;
use tokio::{
    io::{AsyncRead, AsyncReadExt, ReadBuf},
    sync::mpsc,
    task::JoinHandle,
};

use angos_tx_engine::ByteStream;

use crate::registry::blob_store::{Error, resumable_hasher::Hasher};

const READ_FRAME_SIZE: usize = 1024 * 1024;

pub struct HashingReader<R> {
    inner: R,
    hasher: Hasher,
}

impl<R> HashingReader<R> {
    pub fn new(inner: R, hasher: Hasher) -> Self {
        Self { inner, hasher }
    }

    /// Consume the reader, yielding the hasher fed by every byte read.
    pub fn into_hasher(self) -> Hasher {
        self.hasher
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

/// Drive a [`HashingReader`] in a background task, surfacing its bytes as a
/// [`ByteStream`] for the backend to consume and resolving (via the returned
/// join handle, once drained) to the [`Hasher`] fed by every byte.
///
/// `Some(len)` reads exactly `len` bytes and errors on a short body, `None`
/// reads to EOF; bytes are framed in 1 MiB chunks over an mpsc channel, so the
/// body never sits whole in memory.
pub fn hashing_stream<R>(
    reader: HashingReader<R>,
    content_length: Option<u64>,
) -> (ByteStream, JoinHandle<Result<Hasher, Error>>)
where
    R: AsyncRead + Unpin + Send + 'static,
{
    let (tx, rx) = mpsc::channel::<Result<Bytes, io::Error>>(8);
    let handle = tokio::spawn(async move {
        let mut reader = reader;
        let mut sent: u64 = 0;
        let mut buf = BytesMut::with_capacity(READ_FRAME_SIZE);
        while content_length.is_none_or(|expected| sent < expected) {
            let want = content_length.map_or(READ_FRAME_SIZE as u64, |expected| {
                (expected - sent).min(READ_FRAME_SIZE as u64)
            });
            buf.clear();
            let n = {
                let mut limited = (&mut reader).take(want);
                limited.read_buf(&mut buf).await
            };
            match n {
                Ok(0) => {
                    if let Some(expected) = content_length {
                        let _ = tx
                            .send(Err(io::Error::new(
                                io::ErrorKind::UnexpectedEof,
                                format!("short upload body: expected {expected} bytes, got {sent}"),
                            )))
                            .await;
                        return Err(Error::UploadBodySize {
                            expected,
                            actual: sent,
                        });
                    }
                    break;
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
        Ok(reader.into_hasher())
    });

    let body: ByteStream = Box::pin(stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    }));
    (body, handle)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use tokio::io::AsyncReadExt;

    use super::*;
    use crate::oci::Algorithm;

    const HELLO_SHA256: &str =
        "sha256:b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9";
    const HELLO_SHA512: &str = "sha512:309ecc489c12d6eb4cc40f50c902f2b4d0ed77ee511a7c7a9bcd3ca86d4cd86f989dd35bc5ff499670da34255b45b0cfd830e81f605dcf7dc5542e93ae9cd76f";

    #[tokio::test]
    async fn test_known_payload_produces_correct_digests() {
        let reader = Cursor::new(b"hello world");
        let mut hashing_reader = HashingReader::new(reader, Hasher::new());

        let mut buf = Vec::new();
        hashing_reader.read_to_end(&mut buf).await.unwrap();

        let hasher = hashing_reader.into_hasher();
        assert_eq!(
            hasher.digest(Algorithm::Sha256).unwrap().to_string(),
            HELLO_SHA256
        );
        assert_eq!(
            hasher.digest(Algorithm::Sha512).unwrap().to_string(),
            HELLO_SHA512
        );
    }

    #[tokio::test]
    async fn test_multiple_small_reads_produce_same_digest() {
        let reader = Cursor::new(b"hello world");
        let mut hashing_reader = HashingReader::new(reader, Hasher::new());

        let mut chunk = [0u8; 3];
        loop {
            let n = hashing_reader.read(&mut chunk).await.unwrap();
            if n == 0 {
                break;
            }
        }

        let hasher = hashing_reader.into_hasher();
        assert_eq!(
            hasher.digest(Algorithm::Sha256).unwrap().to_string(),
            HELLO_SHA256
        );
        assert_eq!(
            hasher.digest(Algorithm::Sha512).unwrap().to_string(),
            HELLO_SHA512
        );
    }

    #[tokio::test]
    async fn test_empty_input_produces_correct_digest() {
        let reader = Cursor::new(b"");
        let mut hashing_reader = HashingReader::new(reader, Hasher::new());

        let mut buf = Vec::new();
        hashing_reader.read_to_end(&mut buf).await.unwrap();

        assert_eq!(
            hashing_reader
                .into_hasher()
                .digest(Algorithm::Sha256)
                .unwrap()
                .to_string(),
            "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[tokio::test]
    async fn test_inner_data_passed_through_unmodified() {
        let payload = b"hello world";
        let reader = Cursor::new(payload);
        let mut hashing_reader = HashingReader::new(reader, Hasher::new());

        let mut buf = Vec::new();
        hashing_reader.read_to_end(&mut buf).await.unwrap();

        assert_eq!(buf, payload);
    }
}
