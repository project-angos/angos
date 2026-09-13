//! Image filesystem exploring. A layer is a tar stream, usually gzipped, with
//! no random access; [`IndexLayerJobHandler`] walks it once and keeps, by the
//! layer digest, a listing of its entries with their offsets and the
//! inflater's checkpoints, so the web UI can browse the merged filesystem and
//! open a file without decoding the whole layer again. A push into an
//! `index = true` repository enqueues one [`INDEX_LAYER_KIND`] job per layer;
//! any other image is indexed the first time someone asks.

use std::{
    io::{self, BufRead, BufReader, Read},
    sync::Arc,
};

use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::STANDARD};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio_util::io::SyncIoBridge;
use tracing::{debug, info};

use angos_oci::{Content, Digest, Manifest};

use crate::{
    jobs::{
        Queue,
        store::{Error, JobEnvelope, JobHandler},
    },
    registry::{
        Error as RegistryError, blob_store::BlobStore, keys::DigestKeys,
        metadata_store::MetadataStore,
    },
};

pub use angos_inflate::{Checkpoint, Inflater};

pub const INDEX_LAYER_KIND: &str = "index.layer";
/// Output between two checkpoints: what opening a file costs at most in
/// decoding, against 32 KiB of stored window per checkpoint.
const CHECKPOINT_EVERY: u64 = 4 * 1024 * 1024;
const GZIP_MAGIC: [u8; 2] = [0x1f, 0x8b];

/// JSON payload of an [`INDEX_LAYER_KIND`] job. The namespace only names who
/// asked: the listing is keyed by the digest and shared like the blob is.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexLayerPayload {
    pub namespace: angos_oci::Namespace,
    pub digest: Digest,
    /// Walk the layer again even when a listing exists.
    #[serde(default)]
    pub force: bool,
}

/// An index job keyed on `index.{digest}`, so every image sharing the layer
/// coalesces on one job.
pub fn build_envelope(payload: &IndexLayerPayload) -> Result<JobEnvelope, Error> {
    JobEnvelope::new(
        Queue::Index,
        INDEX_LAYER_KIND,
        format!("{}.{}", Queue::Index, payload.digest),
        payload,
    )
}

/// Whether a layer media type names a tar stream the indexer can walk: plain
/// or gzipped, in the OCI or Docker spelling. zstd layers are not.
pub fn is_filesystem_layer(media_type: &str) -> bool {
    media_type.contains(".tar") && !media_type.contains("zstd")
}

/// The digests of a plain image manifest's walkable layers; empty for an
/// index, a referrer or an artifact.
pub fn filesystem_layers(manifest: &Manifest) -> Vec<Digest> {
    if manifest.subject.is_some() || manifest.artifact_type.is_some() {
        return Vec::new();
    }
    match &manifest.content {
        Content::Image { layers, .. } => layers
            .iter()
            .filter(|layer| is_filesystem_layer(layer.media_type.as_ref()))
            .map(|layer| layer.digest.clone())
            .collect(),
        Content::Index { .. } => Vec::new(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    File,
    Dir,
    Symlink,
    Hardlink,
    /// `.wh.<name>`: the lower layers' `<name>` is gone; `path` names it.
    Whiteout,
    /// `.wh..wh..opq`: the lower layers' content of `path` is gone.
    Opaque,
    Other,
}

/// One tar entry, with where its data starts in the uncompressed stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    pub path: String,
    pub kind: Kind,
    pub size: u64,
    pub mode: u32,
    pub uid: u64,
    pub gid: u64,
    pub mtime: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub link: Option<String>,
    pub offset: u64,
}

/// A layer's entries in tar order, stored as JSON by the layer digest and
/// served as-is to the web UI.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Listing {
    /// Whether the layer is gzipped, which is when checkpoints exist.
    pub compressed: bool,
    pub uncompressed_size: u64,
    pub entries: Vec<Entry>,
}

/// The inflater's checkpoints, stored next to the listing with each window
/// base64-encoded.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Checkpoints {
    pub items: Vec<StoredCheckpoint>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StoredCheckpoint {
    pub in_offset: u64,
    pub bit: u8,
    pub out_offset: u64,
    pub window: String,
}

impl Checkpoints {
    pub fn from_inflater(checkpoints: Vec<Checkpoint>) -> Self {
        Self {
            items: checkpoints
                .into_iter()
                .map(|checkpoint| StoredCheckpoint {
                    in_offset: checkpoint.in_offset,
                    bit: checkpoint.bit,
                    out_offset: checkpoint.out_offset,
                    window: STANDARD.encode(checkpoint.window),
                })
                .collect(),
        }
    }

    /// The last checkpoint at or before `offset`, the one a read there resumes
    /// from; `None` means decoding from the start.
    pub fn before(&self, offset: u64) -> Option<Checkpoint> {
        self.items
            .iter()
            .filter(|checkpoint| checkpoint.out_offset <= offset)
            .max_by_key(|checkpoint| checkpoint.out_offset)
            .and_then(|checkpoint| {
                Some(Checkpoint {
                    in_offset: checkpoint.in_offset,
                    bit: checkpoint.bit,
                    out_offset: checkpoint.out_offset,
                    window: STANDARD.decode(&checkpoint.window).ok()?,
                })
            })
    }
}

/// The tar stream behind a layer: inflated, or the bytes as they are.
enum Source<R: Read> {
    Gzip(Box<Inflater<BufReader<R>>>),
    Plain(BufReader<R>, u64),
}

impl<R: Read> Read for Source<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Source::Gzip(inflater) => inflater.read(buf),
            Source::Plain(reader, count) => {
                let n = reader.read(buf)?;
                *count += n as u64;
                Ok(n)
            }
        }
    }
}

/// `usr/bin/.wh.sh` is a whiteout of `usr/bin/sh`, `a/.wh..wh..opq` the
/// opaque marker of `a`; the rest keep their kind. Paths lose the `./` and
/// trailing `/` tar spellings so the same file is named the same in every
/// layer.
fn classify(raw: &str, tar_kind: tar::EntryType) -> Option<(String, Kind)> {
    let path = raw.trim_start_matches("./").trim_matches('/');
    if path.is_empty() || path == "." {
        return None;
    }
    let (dir, name) = path.rsplit_once('/').unwrap_or(("", path));
    let join = |name: &str| {
        if dir.is_empty() {
            name.to_string()
        } else {
            format!("{dir}/{name}")
        }
    };
    if name == ".wh..wh..opq" {
        return Some((dir.to_string(), Kind::Opaque));
    }
    if let Some(target) = name.strip_prefix(".wh.") {
        return Some((join(target), Kind::Whiteout));
    }
    let kind = if tar_kind.is_dir() {
        Kind::Dir
    } else if tar_kind.is_symlink() {
        Kind::Symlink
    } else if tar_kind.is_hard_link() {
        Kind::Hardlink
    } else if tar_kind.is_file() || tar_kind == tar::EntryType::Continuous {
        Kind::File
    } else {
        Kind::Other
    };
    Some((path.to_string(), kind))
}

/// Walks one layer's tar stream, from the start, into its listing and the
/// checkpoints taken along the way. Blocking: run it off the async threads.
pub fn index_stream<R: Read>(input: R) -> io::Result<(Listing, Vec<Checkpoint>)> {
    let mut reader = BufReader::new(input);
    let compressed = reader.fill_buf()?.starts_with(&GZIP_MAGIC);
    let mut source = if compressed {
        Source::Gzip(Box::new(Inflater::new(reader, CHECKPOINT_EVERY)?))
    } else {
        Source::Plain(reader, 0)
    };

    let mut entries = Vec::new();
    // Scoped: the archive borrows the source, which is read again below.
    {
        let mut archive = tar::Archive::new(&mut source);
        for entry in archive.entries()? {
            let entry = entry?;
            let header = entry.header();
            let raw = entry.path()?.to_string_lossy().into_owned();
            let Some((path, kind)) = classify(&raw, header.entry_type()) else {
                continue;
            };
            let link = match kind {
                Kind::Symlink | Kind::Hardlink => entry
                    .link_name()?
                    .map(|link| link.to_string_lossy().into_owned()),
                _ => None,
            };
            entries.push(Entry {
                path,
                kind,
                size: header.size()?,
                mode: header.mode()?,
                uid: header.uid()?,
                gid: header.gid()?,
                mtime: header.mtime()?,
                link,
                offset: entry.raw_file_position(),
            });
        }
    }

    let (uncompressed_size, checkpoints) = match source {
        Source::Gzip(inflater) => (inflater.position(), inflater.into_checkpoints()),
        Source::Plain(_, count) => (count, Vec::new()),
    };
    Ok((
        Listing {
            compressed,
            uncompressed_size,
            entries,
        },
        checkpoints,
    ))
}

/// Feeds `size` bytes of a gzipped layer's uncompressed stream, starting at
/// `offset`, to `sink` chunk by chunk. `input` is positioned at the
/// checkpoint's `in_offset`, or at the stream start without one. Blocking.
pub fn extract_gzip<R: Read>(
    input: R,
    checkpoint: Option<&Checkpoint>,
    offset: u64,
    size: u64,
    mut sink: impl FnMut(&[u8]) -> io::Result<()>,
) -> io::Result<()> {
    let mut inflater = match checkpoint {
        Some(checkpoint) => Inflater::resume(BufReader::new(input), checkpoint)?,
        None => Inflater::new(BufReader::new(input), u64::MAX)?,
    };
    inflater.skip(offset - inflater.position())?;
    let mut remaining = size;
    let mut chunk = vec![0u8; 64 * 1024];
    while remaining > 0 {
        let want = usize::try_from(remaining).map_or(chunk.len(), |n| n.min(chunk.len()));
        let read = inflater.read(&mut chunk[..want])?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "layer ends before the file does",
            ));
        }
        sink(&chunk[..read])?;
        remaining -= read as u64;
    }
    Ok(())
}

/// Reads a layer's stored listing; `None` when it was never indexed.
pub async fn read_listing(
    metadata_store: &MetadataStore,
    digest: &Digest,
) -> Result<Option<Listing>, RegistryError> {
    match metadata_store
        .object_store()
        .get(&digest.layer_entries_path())
        .await
    {
        Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
        Err(angos_storage::Error::NotFound) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

pub async fn read_checkpoints(
    metadata_store: &MetadataStore,
    digest: &Digest,
) -> Result<Checkpoints, RegistryError> {
    match metadata_store
        .object_store()
        .get(&digest.layer_checkpoints_path())
        .await
    {
        Ok(bytes) => Ok(serde_json::from_slice(&bytes)?),
        Err(angos_storage::Error::NotFound) => Ok(Checkpoints::default()),
        Err(e) => Err(e.into()),
    }
}

pub struct IndexLayerJobHandler {
    blob_store: Arc<BlobStore>,
    metadata_store: Arc<MetadataStore>,
}

fn job_error(error: &RegistryError) -> Error {
    Error::Execution(error.to_string())
}

impl IndexLayerJobHandler {
    pub fn new(blob_store: Arc<BlobStore>, metadata_store: Arc<MetadataStore>) -> Self {
        Self {
            blob_store,
            metadata_store,
        }
    }

    /// Indexes the layer unless a listing already exists, or again when
    /// `force`; a layer whose bytes are gone has nothing to index and the job
    /// is done.
    pub async fn index(&self, digest: &Digest, force: bool) -> Result<(), Error> {
        let store = self.metadata_store.object_store();
        if !force
            && store
                .exists(&digest.layer_entries_path())
                .await
                .map_err(|e| job_error(&e.into()))?
        {
            return Ok(());
        }
        let (reader, _) = match self.blob_store.reader(digest, None).await {
            Ok(reader) => reader,
            Err(RegistryError::BlobUnknown) => {
                debug!("Index of {digest} skipped: the layer is gone");
                return Ok(());
            }
            Err(e) => return Err(job_error(&e)),
        };
        let (listing, checkpoints) =
            tokio::task::spawn_blocking(move || index_stream(SyncIoBridge::new(reader)))
                .await
                .map_err(|e| Error::Execution(format!("index task failed: {e}")))?
                .map_err(|e| {
                    Error::Execution(format!("layer {digest} is not a tar stream: {e}"))
                })?;
        let entries = listing.entries.len();
        let checkpoints = Checkpoints::from_inflater(checkpoints);
        store
            .put(
                &digest.layer_checkpoints_path(),
                Bytes::from(
                    serde_json::to_vec(&checkpoints)
                        .map_err(|e| Error::Execution(e.to_string()))?,
                ),
            )
            .await
            .map_err(|e| job_error(&e.into()))?;
        // The listing lands last: its presence is what marks the layer indexed.
        store
            .put(
                &digest.layer_entries_path(),
                Bytes::from(
                    serde_json::to_vec(&listing).map_err(|e| Error::Execution(e.to_string()))?,
                ),
            )
            .await
            .map_err(|e| job_error(&e.into()))?;
        info!(
            "Indexed layer {digest}: {entries} entries, {} checkpoints",
            checkpoints.items.len()
        );
        Ok(())
    }
}

#[async_trait]
impl JobHandler for IndexLayerJobHandler {
    async fn execute(&self, envelope: &JobEnvelope) -> Result<(), Error> {
        if envelope.kind != INDEX_LAYER_KIND {
            return Err(Error::Execution(format!(
                "unsupported job kind '{}'; expected '{INDEX_LAYER_KIND}'",
                envelope.kind,
            )));
        }
        let payload: IndexLayerPayload = serde_json::from_value(envelope.payload.clone())
            .map_err(|e| Error::Execution(format!("failed to deserialize job payload: {e}")))?;
        self.index(&payload.digest, payload.force).await
    }
}

#[cfg(test)]
mod tests;
