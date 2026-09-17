//! Hashing an upload as it streams, across chunks.
//!
//! [`Hasher`] is the live digest; its [`HashState`] serializes mid-stream so a
//! chunked upload can resume one, and [`HashingReader`] feeds it every byte a
//! reader yields.

use std::{
    collections::BTreeMap,
    io,
    pin::Pin,
    task::{Context, Poll},
};

use base64::{Engine, prelude::BASE64_STANDARD};
use bytes::{Bytes, BytesMut};
use futures_util::stream;
use sha2::{Digest, Sha256, Sha512, digest::common::hazmat::SerializableState};
use tokio::{
    io::{AsyncRead, AsyncReadExt, ReadBuf},
    sync::mpsc,
    task::JoinHandle,
};

use angos_oci::{Algorithm, Digest as OciDigest};
use angos_storage::ByteStream;

use crate::registry::Error;

/// One supported algorithm's live hasher. The mid-stream state can be
/// serialized and restored (for chunked-upload checkpoints), and a finalized
/// hasher yields an [`OciDigest`].
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
        let invalid = || Error::Internal("Unable to resume hash state".to_string());
        Ok(match algorithm {
            Algorithm::Sha256 => Self::Sha256(Sha256::deserialize(
                state.try_into().map_err(|_| invalid())?,
            )?),
            Algorithm::Sha512 => Self::Sha512(Sha512::deserialize(
                state.try_into().map_err(|_| invalid())?,
            )?),
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

    fn digest(&self) -> OciDigest {
        match self {
            Self::Sha256(h) => OciDigest::from_finalized(Algorithm::Sha256, h.clone().finalize()),
            Self::Sha512(h) => OciDigest::from_finalized(Algorithm::Sha512, h.clone().finalize()),
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

    /// Hash under a single known algorithm: a verify-only read, and a session
    /// whose client named the algorithm it will close the upload with, whose
    /// checkpoints then carry that one hash instead of every supported one.
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
    /// for it (a checkpoint written before that algorithm was supported).
    pub fn digest(&self, algorithm: Algorithm) -> Result<OciDigest, Error> {
        self.hashers
            .iter()
            .find(|h| h.algorithm() == algorithm)
            .map(AlgorithmHasher::digest)
            .ok_or(Error::DigestInvalid)
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

    /// Decode from the JSON checkpoint bytes; unknown algorithms are ignored so
    /// a newer build's checkpoint still resumes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        let map: BTreeMap<String, String> = serde_json::from_slice(bytes)
            .map_err(|e| Error::Internal(format!("invalid hash checkpoint: {e}")))?;

        let mut states = BTreeMap::new();
        for (name, encoded) in map {
            let Ok(algorithm) = name.parse::<Algorithm>() else {
                continue;
            };
            let decoded = BASE64_STANDARD
                .decode(&encoded)
                .map_err(|e| Error::Internal(e.to_string()))?;
            states.insert(algorithm, decoded);
        }

        if states.is_empty() {
            return Err(Error::Internal("empty hash checkpoint".to_string()));
        }
        Ok(Self { states })
    }

    /// Rebuild a live [`Hasher`] from the checkpointed states. A missing
    /// algorithm (e.g. one added after the checkpoint was written) is left out
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

const READ_FRAME_SIZE: usize = 1024 * 1024;

/// The hasher fed by every forwarded byte, plus the count of those bytes.
type HashOutcome = Result<(Hasher, u64), Error>;

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
/// [`ByteStream`] and, through the join handle once drained, the [`Hasher`]
/// fed by every byte and the count of bytes it forwarded. `Some(len)` reads
/// exactly `len` bytes and errors on a short body, `None` reads to EOF; frames
/// go over an mpsc channel, so the body never sits whole in memory.
pub fn hashing_stream<R>(
    reader: HashingReader<R>,
    content_length: Option<u64>,
) -> (ByteStream, JoinHandle<HashOutcome>)
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
                        return Err(Error::RangeNotSatisfiable);
                    }
                    break;
                }
                Ok(n) => {
                    sent = sent
                        .checked_add(u64::try_from(n).map_err(|e| Error::Internal(e.to_string()))?)
                        .ok_or_else(|| Error::Internal("size overflow".into()))?;
                    if tx.send(Ok(buf.split().freeze())).await.is_err() {
                        break;
                    }
                }
                Err(e) => {
                    let _ = tx.send(Err(io::Error::other(e.to_string()))).await;
                    return Err(Error::Io(e));
                }
            }
        }
        drop(tx);
        Ok((reader.into_hasher(), sent))
    });

    let body: ByteStream = Box::pin(stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    }));
    (body, handle)
}

#[cfg(test)]
mod tests {
    use tokio::io::AsyncReadExt;

    use angos_oci::Algorithm;

    use crate::registry::blob_store::hashing::*;

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
            OciDigest::sha256(EMPTY_SHA256).unwrap()
        );
        assert_eq!(
            AlgorithmHasher::new(Algorithm::Sha512).digest(),
            OciDigest::sha512(EMPTY_SHA512).unwrap()
        );
    }

    #[test]
    fn hasher_produces_every_supported_digest() {
        let mut hasher = Hasher::new();
        hasher.update(b"");
        assert_eq!(
            hasher.digest(Algorithm::Sha256).unwrap(),
            OciDigest::sha256(EMPTY_SHA256).unwrap()
        );
        assert_eq!(
            hasher.digest(Algorithm::Sha512).unwrap(),
            OciDigest::sha512(EMPTY_SHA512).unwrap()
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
    fn from_bytes_rejects_non_json_checkpoint() {
        // A non-JSON payload (corrupt, or a pre-JSON bare state) is rejected
        // rather than resumed.
        assert!(HashState::from_bytes(&[0xAB, 0xCD, 0xEF]).is_err());
    }
    use std::io::Cursor;

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
